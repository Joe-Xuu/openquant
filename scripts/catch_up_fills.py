"""
Catch-up script: fetch recent myTrades and backfill any BUY fills
that were missed by the reconcile loop (e.g. due to silent failure).

Usage: python scripts/catch_up_fills.py
"""

import asyncio
import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from execution.exchange_client import ExchangeClient
from core.local_ledger import get_ledger


async def main():
    use_testnet = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
    use_futures = os.getenv("BINANCE_MARKET", "spot").lower() == "futures"

    if use_futures:
        api_key = os.getenv("BINANCE_FUTURES_API_KEY", "") or os.getenv("BINANCE_TESTNET_API_KEY", "")
        api_secret = os.getenv("BINANCE_FUTURES_API_SECRET", "") or os.getenv("BINANCE_TESTNET_API_SECRET", "")
    else:
        api_key = os.getenv("BINANCE_TESTNET_API_KEY" if use_testnet else "BINANCE_API_KEY", "")
        api_secret = os.getenv("BINANCE_TESTNET_API_SECRET" if use_testnet else "BINANCE_API_SECRET", "")

    if not api_key:
        print("ERROR: No API key found in .env")
        return

    market = "futures" if use_futures else "spot"
    client = ExchangeClient(api_key=api_key, api_secret=api_secret, testnet=use_testnet, market=market)
    await client.start()

    ledger = get_ledger()

    # Fetch recent myTrades
    symbol = "DOGEUSDT"
    print(f"Fetching myTrades for {symbol}...")
    trades = await client._signed_request("GET",
        f"{client._api_prefix}/myTrades",
        {"symbol": symbol, "limit": 100},
    )
    print(f"Got {len(trades)} trades")

    # Get existing trade_ids from grid_fills
    existing = set()
    rows = ledger._query("SELECT trade_id FROM grid_fills")
    for r in rows:
        existing.add(r["trade_id"])

    # Process BUY fills not yet in grid_fills
    # Only catch up fills from the last 48 hours — older fills are
    # either already in the CSV import or from a different market regime.
    import time as _time
    cutoff_ts = _time.time() - 48 * 3600

    matched = 0
    skipped = 0
    for trade in trades:
        fill_side = "BUY" if trade.get("isBuyer", False) else "SELL"
        if fill_side != "BUY":
            continue

        fill_time_ms = trade.get("time", 0)
        fill_time_sec = fill_time_ms / 1000.0
        if fill_time_sec < cutoff_ts:
            skipped += 1
            continue  # too old, skip

        trade_id = str(trade.get("id", ""))
        if trade_id in existing:
            skipped += 1
            continue

        # This is a missed BUY fill — backfill it
        fill_price = float(trade.get("price", 0))
        fill_qty = float(trade.get("qty", 0))
        fill_time_ms = trade.get("time", 0)
        fill_time = datetime.fromtimestamp(fill_time_ms / 1000.0, tz=timezone.utc).isoformat()

        # Generate a deterministic trade_id from exchange trade ID
        profit_pct = 1.0  # matches current config
        sell_price = round(fill_price * (1.0 + profit_pct / 100.0), 8)

        inserted = ledger.record_grid_fill(
            trade_id=trade_id,
            symbol=symbol,
            exchange_order_id=str(trade.get("orderId", "")),
            buy_price=fill_price,
            buy_qty=fill_qty,
            buy_time=fill_time,
            profit_pct=profit_pct,
        )

        if inserted:
            matched += 1
            print(f"  BACKFILLED: BUY {fill_qty} @ ${fill_price:.5f} at {fill_time} (trade_id={trade_id})")
        else:
            skipped += 1

    print(f"\nDone: {matched} backfilled, {skipped} already tracked")

    # Show pending fills
    pending = ledger.get_pending_tp_fills(symbol)
    print(f"\nPending TP fills (sell_placed=FALSE): {len(pending)}")
    for p in pending:
        print(f"  {p['buy_time']} | BUY {float(p['buy_qty']):.0f} @ ${float(p['buy_price']):.5f} "
              f"| TP @ ${float(p['sell_price']):.5f}")

    ledger.close()
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
