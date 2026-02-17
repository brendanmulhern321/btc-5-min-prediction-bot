#!/usr/bin/env python3
"""
Simmer FastLoop Trading Skill

Trades Polymarket 5-minute fast markets (BTC, ETH, etc.) using CEX price momentum.
Supports multiple assets with max one position per asset at a time.

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
    "max_buy_price": {"default": 0.05, "env": "SIMMER_SPRINT_MAX_BUY", "type": float,
                      "help": "Never buy contracts priced above this"},
    "signal_source": {"default": "binance", "env": "SIMMER_SPRINT_SIGNAL", "type": str,
                      "help": "Price feed source (binance, kraken, coingecko)"},
    "lookback_minutes": {"default": 5, "env": "SIMMER_SPRINT_LOOKBACK", "type": int,
                         "help": "Minutes of price history for momentum calc"},
    "min_time_remaining": {"default": 60, "env": "SIMMER_SPRINT_MIN_TIME", "type": int,
                           "help": "Skip fast_markets with less than this many seconds remaining"},
    "max_time_remaining": {"default": 86400, "env": "SIMMER_SPRINT_MAX_TIME", "type": int,
                           "help": "Skip fast_markets with more than this many seconds remaining"},
    "assets": {"default": ["BTC"], "env": "SIMMER_SPRINT_ASSETS", "type": list,
               "help": "Assets to trade, e.g. ['BTC', 'ETH']"},
    "asset": {"default": "", "env": "SIMMER_SPRINT_ASSET", "type": str,
              "help": "(Deprecated) Single asset — use 'assets' list instead"},
    "window": {"default": "daily", "env": "SIMMER_SPRINT_WINDOW", "type": str,
               "help": "Market window duration (5m, 15m, 1h, or daily)"},
    "volume_confidence": {"default": True, "env": "SIMMER_SPRINT_VOL_CONF", "type": bool,
                          "help": "Weight signal by volume (higher volume = more confident)"},
}

TRADE_SOURCE = "sdk:fastloop"
SMART_SIZING_PCT = 0.50  # 50% of balance per trade
MIN_SHARES_PER_ORDER = 100  # Minimum shares — go big on cheap contracts
MAX_BUY_PRICE = 0.05  # Never buy contracts priced above this (any asset)

# Asset → Binance symbol mapping
ASSET_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
}

# Discord webhook for trade notifications
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")


def send_discord_notification(message):
    """Send a notification to Discord via webhook."""
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        data = json.dumps({"content": message}).encode("utf-8")
        req = Request(DISCORD_WEBHOOK_URL, data=data, method="POST",
                      headers={"Content-Type": "application/json",
                               "User-Agent": "FastLoop-Bot/1.0"})
        urlopen(req, timeout=10)
    except Exception as e:
        print(f"  ⚠️  Discord notification failed: {e}", file=sys.stderr, flush=True)


# Asset → Gamma API search patterns
ASSET_PATTERNS = {
    "BTC": ["bitcoin above"],
    "ETH": ["ethereum above"],
    "SOL": ["solana above"],
    "XRP": ["xrp above"],
}

# Asset → market window
ASSET_WINDOWS = {
    "BTC": "daily",
    "ETH": "daily",
    "SOL": "daily",
    "XRP": "daily",
}

# Asset → price level increment and slug format for above/below markets
ASSET_LEVEL_CONFIG = {
    "BTC": {"increment": 2000, "unit": "k", "divisor": 1000},    # 66k, 68k, 70k
    "ETH": {"increment": 100,  "unit": "",  "divisor": 1},       # 1900, 2000, 2100
    "SOL": {"increment": 10,   "unit": "",  "divisor": 1},       # 80, 90, 100
    "XRP": {"increment": 0.10, "unit": "pt", "divisor": 1},      # 1pt4, 1pt5, 1pt6
}

# Asset → keyword for matching positions (lowercase)
ASSET_KEYWORDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "XRP": "xrp",
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
                elif type_fn == list:
                    result[key] = [v.strip() for v in val.split(",")]
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
MAX_BUY_PRICE = cfg["max_buy_price"]
SIGNAL_SOURCE = cfg["signal_source"]
LOOKBACK_MINUTES = cfg["lookback_minutes"]
MIN_TIME_REMAINING = cfg["min_time_remaining"]
MAX_TIME_REMAINING = cfg["max_time_remaining"]
# Build ASSETS list: prefer "assets" key, fall back to deprecated "asset" key
_assets_cfg = cfg.get("assets", [])
_asset_single = cfg.get("asset", "")
if _assets_cfg and isinstance(_assets_cfg, list) and len(_assets_cfg) > 0:
    ASSETS = [a.upper() for a in _assets_cfg]
elif _asset_single:
    ASSETS = [_asset_single.upper()]
else:
    ASSETS = ["BTC"]
ASSET = ASSETS[0]  # backward compat: primary asset for logging
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


def _is_15m_market(question):
    """Check if question describes a 15-minute market window."""
    import re
    match = re.search(r'(\d{1,2}):(\d{2})(AM|PM)\s*-\s*(\d{1,2}):(\d{2})(AM|PM)', question)
    if not match:
        return False
    h1, m1, p1 = int(match.group(1)), int(match.group(2)), match.group(3)
    h2, m2, p2 = int(match.group(4)), int(match.group(5)), match.group(6)
    t1 = (h1 % 12 + (12 if p1 == "PM" else 0)) * 60 + m1
    t2 = (h2 % 12 + (12 if p2 == "PM" else 0)) * 60 + m2
    diff = t2 - t1
    return 14 <= diff <= 16  # 15 minutes with tolerance


def _is_hourly_market(question):
    """Check if question describes an hourly market window.
    Hourly markets have format like 'Bitcoin Up or Down - February 17, 4PM ET'
    (single time, no range) or a range spanning ~60 minutes."""
    import re
    # Single time format (no range): "Month Day, TimeAM/PM ET"
    if re.search(r'\d{1,2}(?::\d{2})?(AM|PM)\s*ET', question) and '-' not in question.split(',')[-1].split('ET')[0]:
        return True
    # Range format: check if ~60 minute span
    match = re.search(r'(\d{1,2}):(\d{2})(AM|PM)\s*-\s*(\d{1,2}):(\d{2})(AM|PM)', question)
    if match:
        h1, m1, p1 = int(match.group(1)), int(match.group(2)), match.group(3)
        h2, m2, p2 = int(match.group(4)), int(match.group(5)), match.group(6)
        t1 = (h1 % 12 + (12 if p1 == "PM" else 0)) * 60 + m1
        t2 = (h2 % 12 + (12 if p2 == "PM" else 0)) * 60 + m2
        diff = t2 - t1
        return 55 <= diff <= 65  # ~60 minutes with tolerance
    return False


def _is_above_below_market(question):
    """Check if question describes an above/below price level market."""
    return "above" in question.lower() and "on " in question.lower()


def _matches_window(question, window):
    """Check if a market question matches the desired window duration.
    Returns False for any unrecognized window or non-matching duration."""
    if window == "5m":
        return _is_5m_market(question)
    elif window == "15m":
        return _is_15m_market(question)
    elif window == "1h":
        return _is_hourly_market(question)
    elif window == "daily":
        return _is_above_below_market(question)
    return False  # reject unknown windows


def _parse_above_below_end_time(question):
    """Parse end time from 'Bitcoin above Xk on Month Day' format.
    These resolve at noon ET on the specified date."""
    import re
    match = re.search(r'on\s+(\w+)\s+(\d+)', question, re.IGNORECASE)
    if match:
        try:
            month_str = match.group(1)
            day = int(match.group(2))
            year = datetime.now(timezone.utc).year
            dt = datetime.strptime(f"{month_str} {day} {year} 12:00PM", "%B %d %Y %I:%M%p")
            dt = dt.replace(tzinfo=timezone.utc) + timedelta(hours=5)  # noon ET -> UTC
            return dt
        except Exception:
            pass
    return None


def _parse_price_level(question):
    """Extract price level from above/below question text.
    'Bitcoin above 72k on ...' -> 72000
    'Ethereum above 2200 on ...' -> 2200
    'Solana above 100 on ...' -> 100
    'XRP above 1pt5 on ...' -> 1.5
    """
    import re
    q = question.lower()
    # BTC format: "above 72k"
    match = re.search(r'above\s+(\d+)k\s+on', q)
    if match:
        return int(match.group(1)) * 1000
    # XRP format: "above 1pt5" or "above 0pt8"
    match = re.search(r'above\s+(\d+)pt(\d+)\s+on', q)
    if match:
        return float(f"{match.group(1)}.{match.group(2)}")
    # ETH/SOL format: "above 2200" or "above 100"
    match = re.search(r'above\s+(\d+)\s+on', q)
    if match:
        return float(match.group(1))
    return None


def _build_above_slug(asset, price_level, resolve_date):
    """Build Polymarket slug for an above/below sub-market.
    Returns: 'bitcoin-above-72k-on-february-18'
    """
    cfg = ASSET_LEVEL_CONFIG.get(asset, {})
    name = ASSET_KEYWORDS.get(asset, asset.lower())
    month = resolve_date.strftime('%B').lower()
    day = resolve_date.day

    if cfg.get("unit") == "k":
        # BTC: divide by 1000, format as "72k"
        level_str = f"{int(price_level // 1000)}k"
    elif cfg.get("unit") == "pt":
        # XRP: format as "1pt5", "0pt8"
        whole = int(price_level)
        frac = round((price_level - whole) * 10)
        if frac == 0:
            level_str = str(whole)
        else:
            level_str = f"{whole}pt{frac}"
    else:
        # ETH/SOL: plain number
        level_str = str(int(price_level))

    return f"{name}-above-{level_str}-on-{month}-{day}"


def _generate_price_levels(asset, current_price, num_levels=6):
    """Generate candidate price levels around the current price for an asset.
    Returns list of price levels above and below current price.
    """
    cfg = ASSET_LEVEL_CONFIG.get(asset, {"increment": 1000})
    increment = cfg["increment"]

    # Round current price to nearest increment
    base = round(current_price / increment) * increment
    levels = []
    for offset in range(-num_levels, num_levels + 1):
        level = base + (offset * increment)
        if level > 0:
            levels.append(level)
    return sorted(levels)


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


def discover_above_below_markets(asset, current_price, api_key=None):
    """Find active above/below price level markets for an asset.
    Returns list of sub-markets with price_level field."""
    patterns = ASSET_PATTERNS.get(asset, [f"{ASSET_KEYWORDS.get(asset, asset.lower())} above"])
    markets = []
    seen_levels = set()
    now_utc = datetime.now(timezone.utc)

    # Step 1: Check Simmer for already-imported above/below markets
    if api_key:
        result = simmer_request("/api/sdk/markets", api_key=api_key)
        if result and isinstance(result, dict) and "markets" in result:
            for m in result["markets"]:
                q = (m.get("question") or "").lower()
                if not any(p in q for p in patterns):
                    continue
                if m.get("status") != "active":
                    continue
                price_level = _parse_price_level(m.get("question", ""))
                if price_level is None:
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
                    end_time = _parse_above_below_end_time(m.get("question", ""))
                if end_time and end_time < now_utc:
                    continue  # already resolved
                yes_price = m.get("external_price_yes", 0.5)
                no_price = 1 - yes_price if yes_price else 0.5
                seen_levels.add(price_level)
                markets.append({
                    "question": m.get("question", ""),
                    "slug": "",
                    "simmer_market_id": m.get("id", ""),
                    "condition_id": "",
                    "end_time": end_time,
                    "price_level": price_level,
                    "outcomes": ["Yes", "No"],
                    "outcome_prices": json.dumps([str(yes_price), str(no_price)]),
                    "fee_rate_bps": 0,
                })

    # Step 2: Import promising levels to get real prices
    now_et = now_utc - timedelta(hours=5)
    # Resolve date: today if before noon ET, otherwise tomorrow
    if now_et.hour < 12:
        resolve_date = now_et.date()
    else:
        resolve_date = (now_et + timedelta(days=1)).date()
    # End time = noon ET on resolve date = 5PM UTC
    resolve_dt = datetime(resolve_date.year, resolve_date.month, resolve_date.day,
                          17, 0, 0, tzinfo=timezone.utc)

    levels = _generate_price_levels(asset, current_price, num_levels=6)
    # Sort by distance from current price — import furthest levels first (most likely cheap)
    levels_by_distance = sorted(levels, key=lambda l: abs(l - current_price), reverse=True)

    imported_count = 0
    max_imports = 4  # limit API calls per asset
    for level in levels_by_distance:
        if level in seen_levels:
            continue
        if imported_count >= max_imports:
            break
        slug = _build_above_slug(asset, level, resolve_date)
        name = ASSET_KEYWORDS.get(asset, asset.lower()).title()

        # Try importing to get real price
        if api_key:
            market_id, err = import_fast_market_market(api_key, slug)
            if market_id:
                imported_count += 1
                details = get_market_details(api_key, market_id)
                yes_price = 0.5
                if details:
                    yes_price = details.get("external_price_yes", 0.5)
                    if yes_price is None:
                        try:
                            dp = json.loads(details.get("outcome_prices", "[]"))
                            yes_price = float(dp[0]) if dp else 0.5
                        except (json.JSONDecodeError, IndexError, ValueError):
                            yes_price = 0.5
                no_price = 1 - yes_price
                seen_levels.add(level)
                markets.append({
                    "question": details.get("question", f"{name} above {level} on {resolve_date.strftime('%B')} {resolve_date.day}?"),
                    "slug": slug,
                    "simmer_market_id": market_id,
                    "condition_id": "",
                    "end_time": resolve_dt,
                    "price_level": level,
                    "outcomes": ["Yes", "No"],
                    "outcome_prices": json.dumps([str(yes_price), str(no_price)]),
                    "fee_rate_bps": 0,
                })
                continue

        # Fallback: add as unimported candidate
        markets.append({
            "question": f"{name} above {level} on {resolve_date.strftime('%B')} {resolve_date.day}?",
            "slug": slug,
            "simmer_market_id": "",
            "condition_id": "",
            "end_time": resolve_dt,
            "price_level": level,
            "outcomes": ["Yes", "No"],
            "outcome_prices": json.dumps(["0.5", "0.5"]),
            "fee_rate_bps": 0,
        })

    return markets


def select_best_price_level(markets, direction, current_price, max_buy_price=0.05):
    """Pick the best above/below sub-market based on momentum direction.

    UP momentum: buy YES on levels ABOVE current price where YES <= max_buy_price
                 (betting BTC will rise above that level)
    DOWN momentum: buy NO on levels BELOW current price where NO <= max_buy_price
                   (betting BTC will drop below that level)

    Returns (side, buy_price, market) or None if nothing qualifies.
    """
    candidates = []
    for m in markets:
        level = m.get("price_level")
        if level is None:
            continue
        try:
            prices = json.loads(m.get("outcome_prices", "[]"))
            yes_price = float(prices[0]) if prices else 0.5
        except (json.JSONDecodeError, IndexError, ValueError):
            yes_price = 0.5
        no_price = 1 - yes_price

        if direction == "up" and level > current_price:
            # Buy YES — cheap when level is far above current price
            if yes_price <= max_buy_price and yes_price > 0:
                distance = level - current_price
                candidates.append((distance, "yes", yes_price, m))
        elif direction == "down" and level < current_price:
            # Buy NO — cheap when level is far below current price
            if no_price <= max_buy_price and no_price > 0:
                distance = current_price - level
                candidates.append((distance, "no", no_price, m))

    if not candidates:
        return None
    # Sort by distance ascending — closest level = most likely to hit
    candidates.sort(key=lambda x: x[0])
    best = candidates[0]
    return best[1], best[2], best[3]  # (side, price, market)


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
                if not _matches_window(question_raw, window):
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
                # Skip markets that have already ended
                if end_time and end_time < now_utc:
                    continue
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

    # Step 2: If no near market on Simmer, build candidate slugs (don't import yet).
    # Import is deferred to trade execution to avoid burning rate-limited API calls.
    window_seconds = {"5m": 300, "15m": 900, "1h": 3600}.get(window, 300)
    has_near_market = any(
        m.get("end_time") and MIN_TIME_REMAINING < (m["end_time"] - now_utc).total_seconds() <= window_seconds
        for m in markets
    )
    if not has_near_market and window in ("5m", "15m", "1h"):
        now_ts = int(now_utc.timestamp())
        asset_lower = asset.lower()
        asset_name = ASSET_KEYWORDS.get(asset, asset_lower)  # "bitcoin", "ethereum", etc.
        for offset in range(2):  # current window, then next window
            ws_ts = (now_ts // window_seconds) * window_seconds + (window_seconds * offset)
            we_ts = ws_ts + window_seconds
            ws_utc = datetime.fromtimestamp(ws_ts, tz=timezone.utc)
            we_utc = datetime.fromtimestamp(we_ts, tz=timezone.utc)
            we_end = we_utc
            # Skip if this window already exists
            already = False
            for m in markets:
                m_end = m.get("end_time")
                if m_end and abs((m_end - we_end).total_seconds()) < 60:
                    already = True
                    break
            if already:
                continue
            ws_et = ws_utc - timedelta(hours=5)
            we_et = we_utc - timedelta(hours=5)
            if window == "1h":
                # Hourly slug: "bitcoin-up-or-down-february-17-4pm-et"
                hour_str = ws_et.strftime('%I%p').lstrip('0').lower()  # "4pm", "11am"
                slug = f"{asset_name}-up-or-down-{ws_et.strftime('%B').lower()}-{ws_et.day}-{hour_str}-et"
                question = f"{asset} Up or Down - {ws_et.strftime('%B %d')}, {ws_et.strftime('%I%p').lstrip('0')} ET"
            else:
                slug = f"{asset_lower}-updown-{window}-{ws_ts}"
                question = f"{asset} Up or Down - {ws_et.strftime('%B %d')}, {ws_et.strftime('%I:%M%p')}-{we_et.strftime('%I:%M%p')} ET"
            markets.append({
                "question": question,
                "slug": slug,
                "simmer_market_id": "",
                "condition_id": "",
                "end_time": we_end,
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


def find_best_fast_market(markets, window="5m"):
    """Pick the best fast_market to trade: soonest expiring that is currently in play."""
    now = datetime.now(timezone.utc)
    max_remaining = {"5m": 300, "15m": 900, "1h": 3600}.get(window, 300)
    candidates = []
    for m in markets:
        end_time = m.get("end_time")
        if not end_time:
            continue
        remaining = (end_time - now).total_seconds()
        if MIN_TIME_REMAINING < remaining <= max_remaining:
            candidates.append((remaining, m))

    if not candidates:
        # Debug: show why no markets qualified
        for m in markets:
            end_time = m.get("end_time")
            if end_time:
                remaining = (end_time - now).total_seconds()
                print(f"  DEBUG: {m.get('question', '?')[:50]} | remaining={remaining:.0f}s | need {MIN_TIME_REMAINING}-{max_remaining}s")
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


COINGECKO_ASSETS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana", "XRP": "ripple"}


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

        # Take last N candles for momentum
        recent_candles = candles[-lookback_minutes:]
        if len(recent_candles) < 2:
            return None

        # Kraken format: [time, open, high, low, close, vwap, volume, count]
        price_then = float(recent_candles[0][1])   # open of oldest candle
        price_now = float(recent_candles[-1][4])    # close of newest candle
        momentum_pct = ((price_now - price_then) / price_then) * 100
        direction = "up" if momentum_pct > 0 else "down"

        # Volume: use second-to-last candle (latest is still forming / incomplete)
        # and average over all completed candles for a stable baseline
        completed = candles[:-1]  # exclude incomplete last candle
        all_volumes = [float(c[6]) for c in completed] if completed else [float(c[6]) for c in candles]
        avg_volume = sum(all_volumes) / len(all_volumes) if all_volumes else 0
        latest_volume = float(recent_candles[-2][6]) if len(recent_candles) >= 2 else float(recent_candles[-1][6])
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
    "XRP": "XXRPZUSD",
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


def liquidate_wrong_contracts(api_key, log_fn=None):
    """Auto-sell any positions that are NOT above/below markets (e.g. up/down contracts).
    Skips resolved/expired positions that can't be sold."""
    if log_fn is None:
        log_fn = print
    positions = get_positions(api_key)
    now_utc = datetime.now(timezone.utc)
    liquidated = 0
    for pos in positions:
        q = pos.get("question", "") or ""
        ql = q.lower()
        # Only look at crypto prediction markets
        if "up or down" not in ql and "above" not in ql:
            continue
        if pos.get("redeemable"):
            continue  # let redeem handle these
        # Skip expired/resolved markets — can't sell these
        pos_end = _parse_above_below_end_time(q)
        if not pos_end:
            pos_end = _parse_fast_market_end_time(q)
        if pos_end and pos_end < now_utc:
            continue
        # Keep above/below markets — liquidate everything else (up/down)
        if _is_above_below_market(q):
            continue  # good, this is what we want
        # This is a wrong contract (up/down, 15m, 5m, hourly, etc.) — sell it
        market_id = pos.get("market_id", "")
        shares_yes = pos.get("shares_yes", 0)
        shares_no = pos.get("shares_no", 0)
        side = "no" if shares_no > shares_yes else "yes"
        shares = shares_no if shares_no > shares_yes else shares_yes
        if not market_id or shares < 5:
            continue  # skip if below Polymarket 5-share minimum
        log_fn(f"  ⚠️  Wrong contract detected: {q[:60]}")
        log_fn(f"  Selling {shares:.1f} {side.upper()} shares...")
        result = simmer_request("/api/sdk/trade", method="POST", data={
            "market_id": market_id,
            "side": side,
            "shares": shares,
            "action": "sell",
            "venue": "polymarket",
            "source": TRADE_SOURCE,
        }, api_key=api_key)
        if result and not result.get("error"):
            log_fn(f"  ✅ Liquidated successfully")
            liquidated += 1
            send_discord_notification(
                f"⚠️ **AUTO-LIQUIDATED** wrong contract\n"
                f"Market: {q[:70]}\n"
                f"Sold {shares:.1f} {side.upper()} shares"
            )
        else:
            error = result.get("error", "Unknown") if result else "No response"
            log_fn(f"  ❌ Liquidation failed: {error}")
    return liquidated


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
            # Discord notification — trade exit / resolution
            pnl = pos.get("pnl", None)
            shares_yes = pos.get("shares_yes", 0)
            shares_no = pos.get("shares_no", 0)
            pnl_str = f" | P&L: ${pnl:+.2f}" if pnl is not None else ""
            send_discord_notification(
                f"🏁 **TRADE RESOLVED**\n"
                f"Market: {question}\n"
                f"Result: Redeemed **{side.upper()}** side{pnl_str}\n"
                f"Shares: YES {shares_yes:.1f} | NO {shares_no:.1f}"
            )
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

def _has_active_position_for_asset(asset, positions, now_utc):
    """Check if there's already an active position for this specific asset.
    Returns the matching position (or None)."""
    keywords = [ASSET_KEYWORDS.get(asset, asset.lower()), asset.lower()]
    for p in positions:
        q = (p.get("question", "") or "").lower()
        # Match both "up or down" and "above" market types
        if "up or down" not in q and "above" not in q:
            continue
        if not any(kw in q for kw in keywords):
            continue  # different asset
        if p.get("redeemable"):
            continue
        # Try parsing end time for both formats
        pos_end = _parse_above_below_end_time(p.get("question", ""))
        if not pos_end:
            pos_end = _parse_fast_market_end_time(p.get("question", ""))
        if pos_end and pos_end < now_utc:
            continue  # market already ended
        return p
    return None


def _run_for_asset(asset, api_key, dry_run, smart_sizing, quiet, log):
    """Run one cycle of the above/below trading strategy for a single asset.
    Returns True if a trade was executed/attempted, False otherwise."""

    # Step 0: Check for existing position FIRST (avoid unnecessary API calls)
    if not dry_run:
        existing_positions = get_positions(api_key)
        if existing_positions:
            now_check = datetime.now(timezone.utc)
            existing = _has_active_position_for_asset(asset, existing_positions, now_check)
            if existing:
                log(f"  ⏸️  Already have an active {asset} position — skip")
                if not quiet:
                    print(f"📊 {asset} Summary: Existing position in '{existing.get('question', 'Unknown')[:50]}...'")
                return False

    # Step 1: Get CEX price momentum FIRST (need current price for market discovery)
    log(f"\n📈 Fetching {asset} price signal ({SIGNAL_SOURCE})...")
    momentum = get_momentum(asset, SIGNAL_SOURCE, LOOKBACK_MINUTES)

    if not momentum:
        log("  ❌ Failed to fetch price data", force=True)
        return False

    current_price = momentum['price_now']
    direction = momentum['direction']
    momentum_pct = abs(momentum["momentum_pct"])

    log(f"  Price: ${current_price:,.2f} (was ${momentum['price_then']:,.2f})")
    log(f"  Momentum: {momentum['momentum_pct']:+.3f}%")
    log(f"  Direction: {direction}")
    if VOLUME_CONFIDENCE:
        log(f"  Volume ratio: {momentum['volume_ratio']:.2f}x avg")

    # Check minimum momentum
    if momentum_pct < MIN_MOMENTUM_PCT:
        log(f"  ⏸️  Momentum {momentum_pct:.3f}% < minimum {MIN_MOMENTUM_PCT}% — skip")
        if not quiet:
            print(f"📊 {asset} Summary: No trade (momentum too weak: {momentum_pct:.3f}%)")
        return False

    # Step 2: Discover above/below markets at various price levels
    log(f"\n🔍 Discovering {asset} above/below markets...")
    markets = discover_above_below_markets(asset, current_price, api_key=api_key)
    log(f"  Found {len(markets)} price levels")

    if not markets:
        log("  No above/below markets found")
        return False

    # Step 3: Select the best price level based on direction
    log(f"\n🧠 Analyzing {asset} price levels...")
    selection = select_best_price_level(markets, direction, current_price, MAX_BUY_PRICE)

    if not selection:
        log(f"  ⏸️  No contracts ≤ ${MAX_BUY_PRICE:.2f} in the {direction} direction — skip")
        if not quiet:
            print(f"📊 {asset} Summary: No trade (no cheap contracts for {direction} direction)")
        return False

    side, price, best = selection
    level = best.get("price_level", 0)
    end_time = best.get("end_time")
    remaining = (end_time - datetime.now(timezone.utc)).total_seconds() if end_time else 0

    log(f"\n🎯 Selected: {best['question']}")
    log(f"  Price level: ${level:,.2f}")
    log(f"  Side: {side.upper()} @ ${price:.3f}")
    log(f"  Resolves in: {remaining:.0f}s")

    vol_note = ""
    if VOLUME_CONFIDENCE and momentum["volume_ratio"] > 2.0:
        vol_note = f" 📊 (volume: {momentum['volume_ratio']:.1f}x avg)"

    trade_rationale = f"{asset} {direction} {momentum['momentum_pct']:+.3f}%, {side.upper()} 'above ${level:,.0f}' @ ${price:.3f}"

    # Size position: target 3000 shares of cheap contracts for max payout
    target_shares = 3000
    position_size = round(target_shares * price, 2)
    position_size = min(position_size, MAX_POSITION_USD)
    # Hard floor/ceiling — $1 max per position
    position_size = max(0.10, min(position_size, MAX_POSITION_USD))

    if position_size < 0.01:
        log(f"  ⚠️  Price ${price:.3f} too low to size position")
        return False

    log(f"  ✅ Signal: {side.upper()} — {trade_rationale}{vol_note}", force=True)

    # Re-check market is still current before trading
    if end_time:
        remaining_now = (end_time - datetime.now(timezone.utc)).total_seconds()
        if remaining_now < MIN_TIME_REMAINING:
            log(f"  ⏸️  Market expired during analysis ({remaining_now:.0f}s left < {MIN_TIME_REMAINING}s min) — skip", force=True)
            if not quiet:
                print(f"📊 {asset} Summary: No trade (market no longer current)")
            return False

    # Step 4: Import & Trade
    market_id = best.get("simmer_market_id", "")
    if not market_id:
        # Re-check Simmer for this market before importing
        log(f"\n🔍 Checking Simmer for existing {asset} market...", force=True)
        asset_patterns = ASSET_PATTERNS.get(asset, [asset.lower()])
        existing = simmer_request("/api/sdk/markets", api_key=api_key)
        if existing and isinstance(existing, dict) and "markets" in existing:
            for m in existing["markets"]:
                mq = (m.get("question") or "").lower()
                if m.get("status") != "active" or "above" not in mq:
                    continue
                if not any(p in mq for p in asset_patterns):
                    continue
                # Match by price level
                m_level = _parse_price_level(m.get("question", ""))
                if m_level and m_level == level:
                    market_id = m.get("id", "")
                    break
    if market_id:
        log(f"\n🔗 Using Simmer market: {market_id[:16]}...", force=True)
    else:
        log(f"\n🔗 Importing to Simmer...", force=True)
        market_id, import_error = import_fast_market_market(api_key, best["slug"])
        if not market_id:
            log(f"  ❌ Import failed: {import_error}", force=True)
            return False
        log(f"  ✅ Market ID: {market_id[:16]}...", force=True)

    trade_success = False
    result = None
    if dry_run:
        est_shares = position_size / price if price > 0 else 0
        log(f"  [DRY RUN] Would buy {side.upper()} ${position_size:.2f} (~{est_shares:.1f} shares)", force=True)
    else:
        # Final guard: re-check live market price before submitting order
        latest = get_market_details(api_key, market_id)
        if latest:
            try:
                latest_prices = json.loads(latest.get("outcome_prices", "[]"))
                latest_yes = float(latest_prices[0]) if latest_prices else price
            except (json.JSONDecodeError, IndexError, ValueError):
                latest_yes = price
            latest_buy = latest_yes if side == "yes" else (1 - latest_yes)
            if latest_buy > MAX_BUY_PRICE:
                log(f"  ⏸️  Live price ${latest_buy:.3f} > ${MAX_BUY_PRICE:.2f} — skip", force=True)
                if not quiet:
                    print(f"📊 {asset} Summary: No trade (live buy price ${latest_buy:.3f} too high)")
                return False

        log(f"  Executing {side.upper()} trade for ${position_size:.2f}...", force=True)
        result = execute_trade(api_key, market_id, side, position_size)
        log(f"  API response: {json.dumps(result) if result else 'None'}", force=True)

        # Only count as success if we got real shares back
        shares_got = 0
        if result:
            try:
                shares_got = float(result.get("shares_bought") or result.get("shares") or 0)
            except (ValueError, TypeError):
                shares_got = 0
        has_error = bool(result and result.get("error"))
        trade_success = shares_got > 0 and not has_error

        if trade_success:
            trade_id = result.get("trade_id")
            log(f"  ✅ Bought {shares_got:.1f} {side.upper()} shares @ ${price:.3f}", force=True)

            # Discord notification — trade entry
            send_discord_notification(
                f"🟢 **TRADE ENTRY | {asset}**\n"
                f"Bought {shares_got:.1f} {side.upper()} shares @ ${price:.3f}\n"
                f"Market: {best['question']}\n"
                f"Signal: {direction} {momentum['momentum_pct']:+.3f}% momentum | Volume {momentum['volume_ratio']:.1f}x avg\n"
                f"Size: ${position_size:.2f} | Expires in {remaining:.0f}s"
            )

            # Log to trade journal
            if trade_id and JOURNAL_AVAILABLE:
                confidence = min(0.9, 0.5 + (momentum_pct / 100))
                log_trade(
                    trade_id=trade_id,
                    source=TRADE_SOURCE,
                    thesis=trade_rationale,
                    confidence=round(confidence, 2),
                    asset=asset,
                    momentum_pct=round(momentum["momentum_pct"], 3),
                    volume_ratio=round(momentum["volume_ratio"], 2),
                    signal_source=SIGNAL_SOURCE,
                )
        else:
            error = result.get("error", "Unknown error") if result else "No response"
            log(f"  ❌ Trade failed: {error} (shares={shares_got})", force=True)

    # Summary
    show_summary = not quiet or trade_success
    if show_summary:
        print(f"\n📊 {asset} Summary:")
        print(f"  Sprint: {best['question'][:50]}")
        print(f"  Signal: {direction} {momentum_pct:.3f}% | YES ${market_yes_price:.3f}")
        print(f"  Action: {'DRY RUN' if dry_run else ('TRADED' if trade_success else 'FAILED')}")

    return trade_success


def run_fast_market_strategy(dry_run=True, positions_only=False, show_config=False,
                        smart_sizing=False, quiet=False):
    """Run one cycle of the fast_market trading strategy for all configured assets."""

    def log(msg, force=False):
        """Print unless quiet mode is on. force=True always prints."""
        if not quiet or force:
            print(msg)

    log("⚡ Simmer FastLoop Trading Skill")
    log("=" * 50)

    if dry_run:
        log("\n  [DRY RUN] No trades will be executed. Use --live to enable trading.")

    log(f"\n⚙️  Configuration:")
    log(f"  Assets:           {', '.join(ASSETS)}")
    log(f"  Window:           {WINDOW}")
    log(f"  Entry threshold:  {ENTRY_THRESHOLD} (min divergence from 50¢)")
    log(f"  Min momentum:     {MIN_MOMENTUM_PCT}% (min price move)")
    log(f"  Max position:     ${MAX_POSITION_USD:.2f}")
    log(f"  Max buy price:    ${MAX_BUY_PRICE:.2f}")
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
        log(f'    python fast_trader.py --set assets=BTC,ETH')
        log(f'    Or edit config.json directly')
        return

    api_key = get_api_key()

    # Show positions if requested
    if positions_only:
        log("\n📊 Sprint Positions:")
        positions = get_positions(api_key)
        fast_market_positions = [p for p in positions if "above" in (p.get("question", "") or "").lower() or "up or down" in (p.get("question", "") or "").lower()]
        if not fast_market_positions:
            log("  No open positions")
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

    # Step 0b: Auto-liquidate any wrong contracts (up/down, 15m, 5m, etc.)
    liquidated = liquidate_wrong_contracts(api_key, log_fn=log)
    if liquidated > 0:
        log(f"  Liquidated {liquidated} wrong contract(s)")

    # Run strategy for each configured asset (max 1 position per asset)
    for asset in ASSETS:
        log(f"\n{'─' * 40}")
        log(f"  Processing {asset}...")
        _run_for_asset(asset, api_key, dry_run, smart_sizing, quiet, log)


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
                    elif type_fn == list:
                        updates[key] = [v.strip().upper() for v in val.split(",")]
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
