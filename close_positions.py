#!/usr/bin/env python3
"""Close all open fast market positions on Simmer/Polymarket."""

import os
import sys
import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

SIMMER_BASE = os.environ.get("SIMMER_API_BASE", "https://api.simmer.markets")


def get_api_key():
    key = os.environ.get("SIMMER_API_KEY")
    if not key:
        print("Error: SIMMER_API_KEY environment variable not set")
        sys.exit(1)
    return key


def api_request(path, method="GET", data=None, api_key=None):
    """Make a Simmer API request."""
    url = f"{SIMMER_BASE}{path}"
    headers = {"User-Agent": "simmer-fastloop_market/1.0"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    body = None
    if data:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        req = Request(url, data=body, headers=headers, method=method)
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        try:
            error_body = json.loads(e.read().decode("utf-8"))
            return {"error": error_body.get("detail", str(e)), "status_code": e.code}
        except Exception:
            return {"error": str(e), "status_code": e.code}
    except (URLError, Exception) as e:
        return {"error": str(e)}


def get_positions(api_key):
    result = api_request("/api/sdk/positions", api_key=api_key)
    if isinstance(result, dict) and "positions" in result:
        return result["positions"]
    if isinstance(result, list):
        return result
    return []


def sell_position(api_key, market_id, side, amount):
    """Sell a position by trading the opposite side."""
    result = api_request("/api/sdk/trade", method="POST", data={
        "market_id": market_id,
        "side": side,
        "amount": amount,
        "action": "sell",
        "venue": "polymarket",
        "source": "sdk:close_positions",
    }, api_key=api_key)
    return result


def main():
    api_key = get_api_key()

    print("Fetching positions...")
    positions = get_positions(api_key)

    if not positions:
        print("No open positions found.")
        return

    print(f"Found {len(positions)} position(s):\n")

    for i, pos in enumerate(positions):
        question = pos.get("question", pos.get("title", "Unknown"))
        market_id = pos.get("market_id", pos.get("marketId", ""))
        side = pos.get("side", pos.get("outcome", ""))
        shares = pos.get("shares", pos.get("size", 0))
        avg_price = pos.get("avg_price", pos.get("averagePrice", 0))
        current_value = pos.get("current_value", pos.get("value", 0))

        print(f"  [{i+1}] {question}")
        print(f"      Side: {side} | Shares: {shares} | Avg price: ${avg_price}")
        print(f"      Market ID: {market_id}")
        if current_value:
            print(f"      Current value: ${current_value}")
        print()

    if "--dry-run" in sys.argv:
        print("[DRY RUN] Would close all positions above. Run without --dry-run to execute.")
        return

    print("Closing all positions...\n")

    for pos in positions:
        question = pos.get("question", pos.get("title", "Unknown"))
        market_id = pos.get("market_id", pos.get("marketId", ""))
        side = pos.get("side", pos.get("outcome", ""))
        shares = pos.get("shares", pos.get("size", 0))

        if not market_id or not shares:
            print(f"  Skipping '{question}' — no market_id or shares")
            continue

        print(f"  Closing: {question}")
        print(f"    Selling {shares} {side} shares...")

        result = sell_position(api_key, market_id, side, shares)

        if result and not result.get("error"):
            print(f"    ✅ Closed successfully")
        else:
            error = result.get("error", "Unknown error") if result else "No response"
            print(f"    ❌ Failed: {error}")
        print()

    print("Done. Fetching updated portfolio...")
    portfolio = api_request("/api/sdk/portfolio", api_key=api_key)
    if portfolio and not portfolio.get("error"):
        balance = portfolio.get("balance_usdc", portfolio.get("balance", "?"))
        print(f"  Balance: ${balance}")


if __name__ == "__main__":
    main()
