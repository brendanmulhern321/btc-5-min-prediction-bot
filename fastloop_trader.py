#!/usr/bin/env python3
"""
Simmer FastLoop Trading Skill

Trades Polymarket BTC 5-minute fast markets using CEX price momentum.
Default signal: Binance BTCUSDT candles. Agents can customize signal source.

Usage:
    python fast_trader.py              # Dry run (show opportunities, no trades)
    python fast_trader.py --live       # Execute real trades
    python fast_trader.py --positions  # Show current fast market positions
    python fast_trader.py --quiet      # Only output on trades/errors

Requires:
    SIMMER_API_KEY environment variable (get from simmer.markets/dashboard)
"""

import os
import sys
import json
import math
import argparse
import time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote

# Force line-buffered stdout for non-TTY environments (cron, Docker, OpenClaw)
sys.stdout.reconfigure(line_buffering=True)

# Optional: Trade Journal integration
try:
    from tradejournal import log_trade
    JOURNAL_AVAILABLE = True
except ImportError:
    try:
        from skills.tradejournal import log_trade
        JOURNAL_AVAILABLE = True
    except ImportError:
        JOURNAL_AVAILABLE = False
        def log_trade(*args, **kwargs):
            pass

# =============================================================================
# Configuration (config.json > env vars > defaults)
# =============================================================================

CONFIG_SCHEMA = {
    "entry_threshold": {"default": 0.05, "env": "SIMMER_SPRINT_ENTRY", "type": float,
                        "help": "Min price divergence from 50¢ to trigger trade"},
    "min_momentum_pct": {"default": 0.5, "env": "SIMMER_SPRINT_MOMENTUM", "type": float,
                         "help": "Min BTC % move in lookback window to trigger"},
    "max_position": {"default": 5.0, "env": "SIMMER_SPRINT_MAX_POSITION", "type": float,
                     "help": "Max $ per trade"},
    "signal_source": {"default": "binance", "env": "SIMMER_SPRINT_SIGNAL", "type": str,
                      "help": "Price feed source (binance, kraken, coingecko)"},
    "lookback_minutes": {"default": 5, "env": "SIMMER_SPRINT_LOOKBACK", "type": int,
                         "help": "Minutes of price history for momentum calc"},
    "min_time_remaining": {"default": 60, "env": "SIMMER_SPRINT_MIN_TIME", "type": int,
                           "help": "Skip fast_markets with less than this many seconds remaining"},
    "max_time_remaining": {"default": 86400, "env": "SIMMER_SPRINT_MAX_TIME", "type": int,
                           "help": "Skip fast_markets with more than this many seconds remaining"},
    "asset": {"default": "BTC", "env": "SIMMER_SPRINT_ASSET", "type": str,
              "help": "Asset to trade (BTC, ETH, SOL)"},
    "window": {"default": "5m", "env": "SIMMER_SPRINT_WINDOW", "type": str,
               "help": "Market window duration (5m or 15m)"},
    "volume_confidence": {"default": True, "env": "SIMMER_SPRINT_VOL_CONF", "type": bool,
                          "help": "Weight signal by volume (higher volume = more confident)"},
}

TRADE_SOURCE = "sdk:fastloop"
SMART_SIZING_PCT = 0.30  # 30% of balance per trade
MIN_SHARES_PER_ORDER = 5  # Polymarket minimum

# Asset → Binance symbol mapping
ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}

# Asset → Gamma API search patterns
ASSET_PATTERNS = {
    "BTC": ["bitcoin up or down"],
    "ETH": ["ethereum up or down"],
    "SOL": ["solana up or down"],
}


def _load_config(schema, skill_file, config_filename="config.json"):
    """Load config with priority: config.json > env vars > defaults."""
    from pathlib import Path
    config_path = Path(skill_file).parent / config_filename
    file_cfg = {}
    if config_path.exists():
        try:
            with open(config_path) as f:
                file_cfg = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    result = {}
    for key, spec in schema.items():
        if key in file_cfg:
            result[key] = file_cfg[key]
        elif spec.get("env") and os.environ.get(spec["env"]):
            val = os.environ.get(spec["env"])
            type_fn = spec.get("type", str)
            try:
                if type_fn == bool:
                    result[key] = val.lower() in ("true", "1", "yes")
                else:
                    result[key] = type_fn(val)
            except (ValueError, TypeError):
                result[key] = spec.get("default")
        else:
            result[key] = spec.get("default")
    return result


def _get_config_path(skill_file, config_filename="config.json"):
    from pathlib import Path
    return Path(skill_file).parent / config_filename


def _update_config(updates, skill_file, config_filename="config.json"):
    """Update config.json with new values."""
    from pathlib import Path
    config_path = Path(skill_file).parent / config_filename
    existing = {}
    if config_path.exists():
        try:
            with open(config_path) as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    existing.update(updates)
    with open(config_path, "w") as f:
        json.dump(existing, f, indent=2)
    return existing


# Load config
cfg = _load_config(CONFIG_SCHEMA, __file__)
ENTRY_THRESHOLD = cfg["entry_threshold"]
MIN_MOMENTUM_PCT = cfg["min_momentum_pct"]
MAX_POSITION_USD = cfg["max_position"]
SIGNAL_SOURCE = cfg["signal_source"]
LOOKBACK_MINUTES = cfg["lookback_minutes"]
MIN_TIME_REMAINING = cfg["min_time_remaining"]
MAX_TIME_REMAINING = cfg["max_time_remaining"]
ASSET = cfg["asset"].upper()
WINDOW = cfg["window"]  # "5m" or "15m"
VOLUME_CONFIDENCE = cfg["volume_confidence"]


# =============================================================================
# API Helpers
# =============================================================================

SIMMER_BASE = os.environ.get("SIMMER_API_BASE", "https://api.simmer.markets")


def get_api_key():
    key = os.environ.get("SIMMER_API_KEY")
    if not key:
        print("Error: SIMMER_API_KEY environment variable not set")
        print("Get your API key from: simmer.markets/dashboard → SDK tab")
        sys.exit(1)
    return key


def _api_request(url, method="GET", data=None, headers=None, timeout=30):
    """Make an HTTP request. Returns parsed JSON or None on error."""
    try:
        req_headers = headers or {}
        if "User-Agent" not in req_headers:
            req_headers["User-Agent"] = "simmer-fastloop_market/1.0"
        body = None
        if data:
            body = json.dumps(data).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        req = Request(url, data=body, headers=req_headers, method=method)
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        try:
            error_body = json.loads(e.read().decode("utf-8"))
            return {"error": error_body.get("detail", str(e)), "status_code": e.code}
        except Exception:
            return {"error": str(e), "status_code": e.code}
    except URLError as e:
        return {"error": f"Connection error: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}


def simmer_request(path, method="GET", data=None, api_key=None):
    """Make a Simmer API request."""
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return _api_request(f"{SIMMER_BASE}{path}", method=method, data=data, headers=headers)


# =============================================================================
# Sprint Market Discovery
# =============================================================================

def _is_5m_market(question):
    """Check if question describes a 5-minute market window."""
    import re
    match = re.search(r'(\d{1,2}):(\d{2})(AM|PM)\s*-\s*(\d{1,2}):(\d{2})(AM|PM)', question)
    if not match:
        return False
    h1, m1, p1 = int(match.group(1)), int(match.group(2)), match.group(3)
    h2, m2, p2 = int(match.group(4)), int(match.group(5)), match.group(6)
    t1 = (h1 % 12 + (12 if p1 == "PM" else 0)) * 60 + m1
    t2 = (h2 % 12 + (12 if p2 == "PM" else 0)) * 60 + m2
    diff = t2 - t1
    return 4 <= diff <= 6  # 5 minutes with tolerance


def _import_current_5m_markets(asset, api_key):
    """Construct and import the current and next 5-minute markets by slug."""
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc - timedelta(hours=5)
    markets = []
    asset_lower = asset.lower()

    # Generate current and next 2 windows
    for offset in range(3):
        minute = (now_et.minute // 5) * 5
        window_start = now_et.replace(minute=minute, second=0, microsecond=0) + timedelta(minutes=5 * offset)
        window_end = window_start + timedelta(minutes=5)
        start_unix = int((window_start + timedelta(hours=5)).timestamp())
        slug = f"{asset_lower}-updown-5m-{start_unix}"

        market_id, error = import_fast_market_market(api_key, slug)
        if market_id:
            end_time = (window_end + timedelta(hours=5)).replace(tzinfo=timezone.utc)
            remaining = (end_time - now_utc).total_seconds()
            markets.append({
                "question": f"{asset} Up or Down - {window_start.strftime('%B %d')}, {window_start.strftime('%I:%M%p')}-{window_end.strftime('%I:%M%p')} ET",
                "slug": slug,
                "simmer_market_id": market_id,
                "condition_id": "",
                "end_time": end_time,
                "outcomes": ["Yes", "No"],
                "outcome_prices": json.dumps(["0.5", "0.5"]),
                "fee_rate_bps": 0,
            })
    return markets


def discover_fast_market_markets(asset="BTC", window="5m", api_key=None):
    """Find active fast markets. Checks Simmer first, imports only if needed."""
    patterns = ASSET_PATTERNS.get(asset, ASSET_PATTERNS["BTC"])
    markets = []
    seen_ids = set()
    now_utc = datetime.now(timezone.utc)

    # Step 1: Check Simmer API for already-imported markets
    if api_key:
        result = simmer_request("/api/sdk/markets", api_key=api_key)
        if result and isinstance(result, dict) and "markets" in result:
            for m in result["markets"]:
                q = (m.get("question") or "").lower()
                if not any(p in q for p in patterns):
                    continue
                if "up or down" not in q:
                    continue
                if m.get("status") != "active":
                    continue
                mid = m.get("id", "")
                if mid in seen_ids:
                    continue
                question_raw = m.get("question", "")
                if window == "5m" and not _is_5m_market(question_raw):
                    continue
                end_time = None
                resolves_at = m.get("resolves_at", "")
                if resolves_at:
                    try:
                        resolves_at = resolves_at.replace("Z", "+00:00").replace(" ", "T")
                        end_time = datetime.fromisoformat(resolves_at)
                    except Exception:
                        pass
                if not end_time:
                    end_time = _parse_fast_market_end_time(question_raw)
                yes_price = m.get("external_price_yes", 0.5)
                no_price = 1 - yes_price if yes_price else 0.5
                seen_ids.add(mid)
                markets.append({
                    "question": question_raw,
                    "slug": "",
                    "simmer_market_id": mid,
                    "condition_id": "",
                    "end_time": end_time,
                    "outcomes": ["Yes", "No"],
                    "outcome_prices": json.dumps([str(yes_price), str(no_price)]),
                    "fee_rate_bps": 0,
                })

    # Step 2: If no near market on Simmer, add a placeholder for the current
    # window so the bot can check the signal. Import only happens at trade time.
    has_near_market = any(
        m.get("end_time") and MIN_TIME_REMAINING < (m["end_time"] - now_utc).total_seconds() <= 600
        for m in markets
    )
    if not has_near_market and window == "5m":
        now_et = now_utc - timedelta(hours=5)
        minute = (now_et.minute // 5) * 5
        ws = now_et.replace(minute=minute, second=0, microsecond=0) + timedelta(minutes=5)
        we = ws + timedelta(minutes=5)
        start_unix = int((ws + timedelta(hours=5)).replace(tzinfo=timezone.utc).timestamp())
        end_time = (we + timedelta(hours=5)).replace(tzinfo=timezone.utc)
        slug = f"{asset.lower()}-updown-5m-{start_unix}"
        markets.append({
            "question": f"{asset} Up or Down - {ws.strftime('%B %d')}, {ws.strftime('%I:%M%p')}-{we.strftime('%I:%M%p')} ET",
            "slug": slug,
            "simmer_market_id": "",  # no ID — will import only on trade signal
            "condition_id": "",
            "end_time": end_time,
            "outcomes": ["Yes", "No"],
            "outcome_prices": json.dumps(["0.5", "0.5"]),
            "fee_rate_bps": 0,
        })

    return markets


def _parse_fast_market_end_time(question):
    """Parse end time from fast market question.
    Handles:
      'Bitcoin Up or Down - February 15, 5:30AM-5:35AM ET' → datetime (5m/15m)
      'Bitcoin Up or Down - February 17, 1PM ET' → datetime (hourly)
    """
    import re
    year = datetime.now(timezone.utc).year

    # Try ranged format first: "Month Day, StartTime-EndTime ET"
    pattern = r'(\w+ \d+),.*?-\s*(\d{1,2}:\d{2}(?:AM|PM))\s*ET'
    match = re.search(pattern, question)
    if match:
        try:
            date_str = match.group(1)
            time_str = match.group(2)
            dt = datetime.strptime(f"{date_str} {year} {time_str}", "%B %d %Y %I:%M%p")
            dt = dt.replace(tzinfo=timezone.utc) + timedelta(hours=5)
            return dt
        except Exception:
            pass

    # Try hourly format: "Month Day, TimeAM/PM ET"
    pattern_hourly = r'(\w+ \d+),\s*(\d{1,2}(?::\d{2})?(?:AM|PM))\s*ET'
    match = re.search(pattern_hourly, question)
    if match:
        try:
            date_str = match.group(1)
            time_str = match.group(2)
            # Add :00 if no minutes (e.g., "1PM" -> "1:00PM")
            if ":" not in time_str:
                time_str = time_str[:-2] + ":00" + time_str[-2:]
            dt = datetime.strptime(f"{date_str} {year} {time_str}", "%B %d %Y %I:%M%p")
            dt = dt.replace(tzinfo=timezone.utc) + timedelta(hours=5)
            return dt
        except Exception:
            pass

    return None


def find_best_fast_market(markets):
    """Pick the best fast_market to trade: soonest expiring with enough time remaining."""
    now = datetime.now(timezone.utc)
    candidates = []
    for m in markets:
        end_time = m.get("end_time")
        if not end_time:
            continue
        remaining = (end_time - now).total_seconds()
        if MIN_TIME_REMAINING < remaining <= MAX_TIME_REMAINING:
            candidates.append((remaining, m))

    if not candidates:
        return None
    # Sort by soonest expiring
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# =============================================================================
# CEX Price Signal
# =============================================================================

def get_binance_momentum(symbol="BTCUSDT", lookback_minutes=5):
    """Get price momentum from Binance public API.
    Returns: {momentum_pct, direction, price_now, price_then, avg_volume, candles}
    """
    url = (
        f"https://api.binance.com/api/v3/klines"
        f"?symbol={symbol}&interval=1m&limit={lookback_minutes}"
    )
    result = _api_request(url)
    if not result:
        print("  ⚠️  Binance API returned empty response", file=sys.stderr, flush=True)
        return None
    if isinstance(result, dict) and "error" in result:
        print(f"  ⚠️  Binance API error: {result.get('error')}", file=sys.stderr, flush=True)
        return None

    try:
        # Kline format: [open_time, open, high, low, close, volume, ...]
        candles = result
        if len(candles) < 2:
            return None

        price_then = float(candles[0][1])   # open of oldest candle
        price_now = float(candles[-1][4])    # close of newest candle
        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"

        volumes = [float(c[5]) for c in candles]
        avg_volume = sum(volumes) / len(volumes)
        latest_volume = volumes[-1]

        # Volume ratio: latest vs average (>1 = above average activity)
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0

        return {
            "momentum_pct": momentum_pct,
            "direction": direction,
            "price_now": price_now,
            "price_then": price_then,
            "avg_volume": avg_volume,
            "latest_volume": latest_volume,
            "volume_ratio": volume_ratio,
            "candles": len(candles),
        }
    except (IndexError, ValueError, KeyError):
        return None


def get_coingecko_momentum(asset="bitcoin", lookback_minutes=5):
    """Fallback: get price from CoinGecko (less accurate, ~1-2 min lag)."""
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={asset}&vs_currencies=usd"
    result = _api_request(url)
    if not result:
        print("  ⚠️  CoinGecko API returned empty response", file=sys.stderr, flush=True)
        return None
    if isinstance(result, dict) and result.get("error"):
        print(f"  ⚠️  CoinGecko API error: {result.get('error')}", file=sys.stderr, flush=True)
        return None
    price_now = result.get(asset, {}).get("usd")
    if not price_now:
        return None
    # CoinGecko doesn't give candle data on free tier, so just return current price
    # Agent would need to track history across calls for momentum
    return {
        "momentum_pct": 0,  # Can't calculate without history
        "direction": "neutral",
        "price_now": price_now,
        "price_then": price_now,
        "avg_volume": 0,
        "latest_volume": 0,
        "volume_ratio": 1.0,
        "candles": 0,
    }


COINGECKO_ASSETS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}


def get_kraken_momentum(pair="XXBTZUSD", lookback_minutes=5):
    """Get price momentum from Kraken public API.
    Returns: {momentum_pct, direction, price_now, price_then, avg_volume, candles}
    """
    # Kraken OHLC: interval 1 = 1 minute
    url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1"
    result = _api_request(url)

    if not result:
        print("  ⚠️  Kraken API returned empty response", file=sys.stderr, flush=True)
        return None
    if isinstance(result, dict) and result.get("error"):
        errors = result.get("error", [])
        if errors:
            print(f"  ⚠️  Kraken API error: {', '.join(errors)}", file=sys.stderr, flush=True)
        return None

    try:
        # Kraken response: {"error":[],"result":{"XXBTZUSD":[[time,open,high,low,close,vwap,volume,count],...]}}
        result_data = result.get("result", {})
        # Get the pair data (key might vary slightly)
        candles = None
        for key in result_data:
            if key != "last":
                candles = result_data[key]
                break

        if not candles or len(candles) < lookback_minutes:
            return None

        # Take last N candles
        recent_candles = candles[-lookback_minutes:]
        if len(recent_candles) < 2:
            return None

        # Kraken format: [time, open, high, low, close, vwap, volume, count]
        price_then = float(recent_candles[0][1])   # open of oldest candle
        price_now = float(recent_candles[-1][4])    # close of newest candle
        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"

        volumes = [float(c[6]) for c in recent_candles]
        avg_volume = sum(volumes) / len(volumes)
        latest_volume = volumes[-1]
        volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 1.0

        return {
            "momentum_pct": momentum_pct,
            "direction": direction,
            "price_now": price_now,
            "price_then": price_then,
            "avg_volume": avg_volume,
            "latest_volume": latest_volume,
            "volume_ratio": volume_ratio,
            "candles": len(recent_candles),
        }
    except (IndexError, ValueError, KeyError, TypeError) as e:
        print(f"  ⚠️  Kraken data parsing error: {e}", file=sys.stderr, flush=True)
        return None


KRAKEN_PAIRS = {
    "BTC": "XXBTZUSD",
    "ETH": "XETHZUSD",
    "SOL": "SOLUSD",
}


def get_momentum(asset="BTC", source="binance", lookback=5):
    """Get price momentum from configured source."""
    if source == "binance":
        symbol = ASSET_SYMBOLS.get(asset, "BTCUSDT")
        return get_binance_momentum(symbol, lookback)
    elif source == "kraken":
        pair = KRAKEN_PAIRS.get(asset, "XXBTZUSD")
        return get_kraken_momentum(pair, lookback)
    elif source == "coingecko":
        cg_id = COINGECKO_ASSETS.get(asset, "bitcoin")
        return get_coingecko_momentum(cg_id, lookback)
    else:
        return None


# =============================================================================
# Import & Trade
# =============================================================================

def import_fast_market_market(api_key, slug):
    """Import a fast market to Simmer. Returns market_id or None."""
    url = f"https://polymarket.com/event/{slug}"
    result = simmer_request("/api/sdk/markets/import", method="POST", data={
        "polymarket_url": url,
        "shared": True,
    }, api_key=api_key)

    if not result:
        return None, "No response from import endpoint"

    if result.get("error"):
        return None, result.get("error", "Unknown error")

    status = result.get("status")
    market_id = result.get("market_id")

    if status == "resolved":
        # Market resolved — check alternatives
        alternatives = result.get("active_alternatives", [])
        if alternatives:
            return None, f"Market resolved. Try alternative: {alternatives[0].get('id')}"
        return None, "Market resolved, no alternatives found"

    if status in ("imported", "already_exists"):
        return market_id, None

    return None, f"Unexpected status: {status}"


def get_market_details(api_key, market_id):
    """Fetch market details by ID."""
    result = simmer_request(f"/api/sdk/markets/{market_id}", api_key=api_key)
    if not result or result.get("error"):
        return None
    return result.get("market", result)


def get_portfolio(api_key):
    """Get portfolio summary."""
    return simmer_request("/api/sdk/portfolio", api_key=api_key)


def get_positions(api_key):
    """Get current positions."""
    result = simmer_request("/api/sdk/positions", api_key=api_key)
    if isinstance(result, dict) and "positions" in result:
        return result["positions"]
    if isinstance(result, list):
        return result
    return []


def redeem_resolved_positions(api_key, log_fn=None):
    """Auto-redeem any resolved positions to reclaim balance."""
    if log_fn is None:
        log_fn = print
    positions = get_positions(api_key)
    redeemed = 0
    for pos in positions:
        if not pos.get("redeemable"):
            continue
        market_id = pos.get("market_id", "")
        side = pos.get("redeemable_side", "")
        question = pos.get("question", "Unknown")
        if not market_id or not side:
            continue
        log_fn(f"  Redeeming: {question} ({side})")
        result = simmer_request("/api/sdk/redeem", method="POST", data={
            "market_id": market_id,
            "side": side,
        }, api_key=api_key)
        if result and not result.get("error"):
            log_fn(f"  Redeemed successfully")
            redeemed += 1
        else:
            error = result.get("error", "Unknown") if result else "No response"
            log_fn(f"  Redeem failed: {error}")
    return redeemed


def execute_trade(api_key, market_id, side, amount):
    """Execute a trade on Simmer."""
    return simmer_request("/api/sdk/trade", method="POST", data={
        "market_id": market_id,
        "side": side,
        "amount": amount,
        "venue": "polymarket",
        "source": TRADE_SOURCE,
    }, api_key=api_key)


def calculate_position_size(api_key, max_size, smart_sizing=False):
    """Calculate position size, optionally based on portfolio."""
    if not smart_sizing:
        return max_size
    portfolio = get_portfolio(api_key)
    if not portfolio or portfolio.get("error"):
        return max_size
    balance = portfolio.get("balance_usdc", 0)
    if balance <= 0:
        return max_size
    smart_size = balance * SMART_SIZING_PCT
    return min(smart_size, max_size)


# =============================================================================
# Main Strategy Logic
# =============================================================================

def run_fast_market_strategy(dry_run=True, positions_only=False, show_config=False,
                        smart_sizing=False, quiet=False):
    """Run one cycle of the fast_market trading strategy."""

    def log(msg, force=False):
        """Print unless quiet mode is on. force=True always prints."""
        if not quiet or force:
            print(msg)

    log("⚡ Simmer FastLoop Trading Skill")
    log("=" * 50)

    if dry_run:
        log("\n  [DRY RUN] No trades will be executed. Use --live to enable trading.")

    log(f"\n⚙️  Configuration:")
    log(f"  Asset:            {ASSET}")
    log(f"  Window:           {WINDOW}")
    log(f"  Entry threshold:  {ENTRY_THRESHOLD} (min divergence from 50¢)")
    log(f"  Min momentum:     {MIN_MOMENTUM_PCT}% (min price move)")
    log(f"  Max position:     ${MAX_POSITION_USD:.2f}")
    log(f"  Signal source:    {SIGNAL_SOURCE}")
    log(f"  Lookback:         {LOOKBACK_MINUTES} minutes")
    log(f"  Min time left:    {MIN_TIME_REMAINING}s")
    log(f"  Max time left:    {MAX_TIME_REMAINING}s")
    log(f"  Volume weighting: {'✓' if VOLUME_CONFIDENCE else '✗'}")

    if show_config:
        config_path = _get_config_path(__file__)
        log(f"\n  Config file: {config_path}")
        log(f"\n  To change settings:")
        log(f'    python fast_trader.py --set entry_threshold=0.08')
        log(f'    python fast_trader.py --set asset=ETH')
        log(f'    Or edit config.json directly')
        return

    api_key = get_api_key()

    # Show positions if requested
    if positions_only:
        log("\n📊 Sprint Positions:")
        positions = get_positions(api_key)
        fast_market_positions = [p for p in positions if "up or down" in (p.get("question", "") or "").lower()]
        if not fast_market_positions:
            log("  No open fast market positions")
        else:
            for pos in fast_market_positions:
                log(f"  • {pos.get('question', 'Unknown')[:60]}")
                log(f"    YES: {pos.get('shares_yes', 0):.1f} | NO: {pos.get('shares_no', 0):.1f} | P&L: ${pos.get('pnl', 0):.2f}")
        return

    # Show portfolio if smart sizing
    if smart_sizing:
        log("\n💰 Portfolio:")
        portfolio = get_portfolio(api_key)
        if portfolio and not portfolio.get("error"):
            log(f"  Balance: ${portfolio.get('balance_usdc', 0):.2f}")

    # Step 0: Auto-redeem any resolved positions
    redeemed = redeem_resolved_positions(api_key, log_fn=log)
    if redeemed > 0:
        log(f"  Redeemed {redeemed} resolved position(s)")

    # Step 1: Discover fast markets
    log(f"\n🔍 Discovering {ASSET} fast markets...")
    markets = discover_fast_market_markets(ASSET, WINDOW, api_key=api_key)
    log(f"  Found {len(markets)} active fast markets")

    if not markets:
        log("  No active fast markets found")
        if not quiet:
            print("📊 Summary: No markets available")
        return

    # Step 2: Find best fast_market to trade
    best = find_best_fast_market(markets)
    if not best:
        log(f"  No fast_markets with {MIN_TIME_REMAINING}s-{MAX_TIME_REMAINING}s remaining")
        if not quiet:
            print("📊 Summary: No tradeable fast_markets (none in time window)")
        return

    end_time = best.get("end_time")
    remaining = (end_time - datetime.now(timezone.utc)).total_seconds() if end_time else 0
    log(f"\n🎯 Selected: {best['question']}")
    log(f"  Expires in: {remaining:.0f}s")

    # Parse current market odds
    try:
        prices = json.loads(best.get("outcome_prices", "[]"))
        market_yes_price = float(prices[0]) if prices else 0.5
    except (json.JSONDecodeError, IndexError, ValueError):
        market_yes_price = 0.5
    log(f"  Current YES price: ${market_yes_price:.3f}")

    # Fee info (fast markets charge 10% on winnings)
    fee_rate_bps = best.get("fee_rate_bps", 0)
    fee_rate = fee_rate_bps / 10000  # 1000 bps -> 0.10
    if fee_rate > 0:
        log(f"  Fee rate:         {fee_rate:.0%} (Polymarket fast market fee)")

    # Step 3: Get CEX price momentum
    log(f"\n📈 Fetching {ASSET} price signal ({SIGNAL_SOURCE})...")
    momentum = get_momentum(ASSET, SIGNAL_SOURCE, LOOKBACK_MINUTES)

    if not momentum:
        log("  ❌ Failed to fetch price data", force=True)
        return

    log(f"  Price: ${momentum['price_now']:,.2f} (was ${momentum['price_then']:,.2f})")
    log(f"  Momentum: {momentum['momentum_pct']:+.3f}%")
    log(f"  Direction: {momentum['direction']}")
    if VOLUME_CONFIDENCE:
        log(f"  Volume ratio: {momentum['volume_ratio']:.2f}x avg")

    # Step 4: Decision logic
    log(f"\n🧠 Analyzing...")

    momentum_pct = abs(momentum["momentum_pct"])
    direction = momentum["direction"]

    # Check for existing positions (only allow one position at a time)
    if not dry_run:
        existing_positions = get_positions(api_key)
        if existing_positions:
            fast_market_positions = [p for p in existing_positions
                                    if "up or down" in (p.get("question", "") or "").lower()]
            if fast_market_positions:
                log(f"  ⏸️  Already have {len(fast_market_positions)} active fast market position(s) — skip")
                if not quiet:
                    for pos in fast_market_positions[:1]:  # Show first position
                        print(f"📊 Summary: Existing position in '{pos.get('question', 'Unknown')[:50]}...'")
                return

    # Check minimum momentum
    if momentum_pct < MIN_MOMENTUM_PCT:
        log(f"  ⏸️  Momentum {momentum_pct:.3f}% < minimum {MIN_MOMENTUM_PCT}% — skip")
        if not quiet:
            print(f"📊 Summary: No trade (momentum too weak: {momentum_pct:.3f}%)")
        return

    # Calculate expected fair price based on momentum direction
    # Simple model: strong momentum → higher probability of continuation
    if direction == "up":
        side = "yes"
        divergence = 0.50 + ENTRY_THRESHOLD - market_yes_price
        trade_rationale = f"{ASSET} up {momentum['momentum_pct']:+.3f}% but YES only ${market_yes_price:.3f}"
    else:
        side = "no"
        divergence = market_yes_price - (0.50 - ENTRY_THRESHOLD)
        trade_rationale = f"{ASSET} down {momentum['momentum_pct']:+.3f}% but YES still ${market_yes_price:.3f}"

    # Volume confidence adjustment
    vol_note = ""
    if VOLUME_CONFIDENCE and momentum["volume_ratio"] < 0.5:
        log(f"  ⏸️  Low volume ({momentum['volume_ratio']:.2f}x avg) — weak signal, skip")
        if not quiet:
            print(f"📊 Summary: No trade (low volume)")
        return
    elif VOLUME_CONFIDENCE and momentum["volume_ratio"] > 2.0:
        vol_note = f" 📊 (high volume: {momentum['volume_ratio']:.1f}x avg)"

    # Check divergence threshold
    if divergence <= 0:
        log(f"  ⏸️  Market already priced in: divergence {divergence:.3f} ≤ 0 — skip")
        if not quiet:
            print(f"📊 Summary: No trade (market already priced in)")
        return

    # Fee-aware EV check: require enough divergence to cover fees
    if fee_rate > 0:
        buy_price = market_yes_price if side == "yes" else (1 - market_yes_price)
        win_profit = (1 - buy_price) * (1 - fee_rate)
        breakeven = buy_price / (win_profit + buy_price)
        fee_penalty = breakeven - 0.50  # how much fees shift breakeven above 50%
        min_divergence = fee_penalty + 0.02  # plus buffer
        log(f"  Breakeven:        {breakeven:.1%} win rate (fee-adjusted, min divergence {min_divergence:.3f})")
        if divergence < min_divergence:
            log(f"  ⏸️  Divergence {divergence:.3f} < fee-adjusted minimum {min_divergence:.3f} — skip")
            if not quiet:
                print(f"📊 Summary: No trade (fees eat the edge)")
            return

    # We have a signal!
    position_size = calculate_position_size(api_key, MAX_POSITION_USD, smart_sizing)
    price = market_yes_price if side == "yes" else (1 - market_yes_price)

    # Check minimum order size
    if price > 0:
        min_cost = MIN_SHARES_PER_ORDER * price
        if min_cost > position_size:
            log(f"  ⚠️  Position ${position_size:.2f} too small for {MIN_SHARES_PER_ORDER} shares at ${price:.2f}")
            return

    log(f"  ✅ Signal: {side.upper()} — {trade_rationale}{vol_note}", force=True)
    log(f"  Divergence: {divergence:.3f}", force=True)

    # Step 5: Import & Trade
    market_id = best.get("simmer_market_id", "")
    if market_id:
        log(f"\n🔗 Using Simmer market: {market_id[:16]}...", force=True)
    else:
        log(f"\n🔗 Importing to Simmer...", force=True)
        market_id, import_error = import_fast_market_market(api_key, best["slug"])
        if not market_id:
            log(f"  ❌ Import failed: {import_error}", force=True)
            return
        log(f"  ✅ Market ID: {market_id[:16]}...", force=True)

    if dry_run:
        est_shares = position_size / price if price > 0 else 0
        log(f"  [DRY RUN] Would buy {side.upper()} ${position_size:.2f} (~{est_shares:.1f} shares)", force=True)
    else:
        log(f"  Executing {side.upper()} trade for ${position_size:.2f}...", force=True)
        result = execute_trade(api_key, market_id, side, position_size)

        if result and result.get("success"):
            shares = result.get("shares_bought") or result.get("shares") or 0
            trade_id = result.get("trade_id")
            log(f"  ✅ Bought {shares:.1f} {side.upper()} shares @ ${price:.3f}", force=True)

            # Log to trade journal
            if trade_id and JOURNAL_AVAILABLE:
                confidence = min(0.9, 0.5 + divergence + (momentum_pct / 100))
                log_trade(
                    trade_id=trade_id,
                    source=TRADE_SOURCE,
                    thesis=trade_rationale,
                    confidence=round(confidence, 2),
                    asset=ASSET,
                    momentum_pct=round(momentum["momentum_pct"], 3),
                    volume_ratio=round(momentum["volume_ratio"], 2),
                    signal_source=SIGNAL_SOURCE,
                )
        else:
            error = result.get("error", "Unknown error") if result else "No response"
            log(f"  ❌ Trade failed: {error}", force=True)

    # Summary
    total_trades = 0 if dry_run else (1 if result and result.get("success") else 0)
    show_summary = not quiet or total_trades > 0
    if show_summary:
        print(f"\n📊 Summary:")
        print(f"  Sprint: {best['question'][:50]}")
        print(f"  Signal: {direction} {momentum_pct:.3f}% | YES ${market_yes_price:.3f}")
        print(f"  Action: {'DRY RUN' if dry_run else ('TRADED' if total_trades else 'FAILED')}")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simmer FastLoop Trading Skill")
    parser.add_argument("--live", action="store_true", help="Execute real trades (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="(Default) Show opportunities without trading")
    parser.add_argument("--positions", action="store_true", help="Show current fast market positions")
    parser.add_argument("--config", action="store_true", help="Show current config")
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="Update config (e.g., --set entry_threshold=0.08)")
    parser.add_argument("--smart-sizing", action="store_true", help="Use portfolio-based position sizing")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Only output on trades/errors (ideal for high-frequency runs)")
    parser.add_argument("--loop", type=int, metavar="SECONDS", default=0,
                        help="Run continuously with SECONDS interval between checks (0 = run once)")
    args = parser.parse_args()

    if args.set:
        updates = {}
        for item in args.set:
            if "=" not in item:
                print(f"Invalid --set format: {item}. Use KEY=VALUE")
                sys.exit(1)
            key, val = item.split("=", 1)
            if key in CONFIG_SCHEMA:
                type_fn = CONFIG_SCHEMA[key].get("type", str)
                try:
                    if type_fn == bool:
                        updates[key] = val.lower() in ("true", "1", "yes")
                    else:
                        updates[key] = type_fn(val)
                except ValueError:
                    print(f"Invalid value for {key}: {val}")
                    sys.exit(1)
            else:
                print(f"Unknown config key: {key}")
                print(f"Valid keys: {', '.join(CONFIG_SCHEMA.keys())}")
                sys.exit(1)
        result = _update_config(updates, __file__)
        print(f"✅ Config updated: {json.dumps(updates)}")
        sys.exit(0)

    dry_run = not args.live

    if args.loop > 0:
        # Continuous mode - run every N seconds
        print(f"🔄 Loop mode: running every {args.loop} seconds (Ctrl+C to stop)\n", flush=True)
        while True:
            try:
                run_fast_market_strategy(
                    dry_run=dry_run,
                    positions_only=args.positions,
                    show_config=args.config,
                    smart_sizing=args.smart_sizing,
                    quiet=args.quiet,
                )
                print(f"\n⏳ Sleeping {args.loop} seconds...\n", flush=True)
                time.sleep(args.loop)
            except KeyboardInterrupt:
                print("\n\n👋 Loop stopped by user", flush=True)
                sys.exit(0)
            except Exception as e:
                print(f"\n❌ Error in loop: {e}", file=sys.stderr, flush=True)
                print(f"⏳ Sleeping {args.loop} seconds before retry...\n", flush=True)
                time.sleep(args.loop)
    else:
        # One-shot mode
        run_fast_market_strategy(
            dry_run=dry_run,
            positions_only=args.positions,
            show_config=args.config,
            smart_sizing=args.smart_sizing,
            quiet=args.quiet,
        )
