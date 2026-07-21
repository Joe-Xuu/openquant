"""
Import historical Binance trades from CSV into grid_fills table.

CSV format: 时间,交易对,方向,价格,已执行,金额,手续费
- Only BUY trades are imported (SELL rows are manual, can't reliably match)
- Before 2026-07-13: sell_placed=TRUE, sell_filled=TRUE (mark completed)
- On/after 2026-07-13: sell_placed=FALSE, sell_filled=FALSE (auto-recovery)
"""

import hashlib
import re
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.local_ledger import get_ledger


CSV_DATA = """2026-07-13 22:41:57,DOGEUSDT,BUY,0.07195,21DOGE,1.51095USDT,0.021DOGE,,,
2026-07-13 12:33:40,DOGEUSDT,BUY,0.07174,22DOGE,1.57828USDT,0.022DOGE,,,
2026-07-13 12:11:39,DOGEUSDT,BUY,0.07204,21DOGE,1.51284USDT,0.021DOGE,,,
2026-07-13 12:07:01,DOGEUSDT,BUY,0.07235,21DOGE,1.51935USDT,0.021DOGE,,,
2026-07-13 07:00:14,DOGEUSDT,BUY,0.07266,21DOGE,1.52586USDT,0.021DOGE,,,
2026-07-13 06:32:52,DOGEUSDT,BUY,0.07297,21DOGE,1.53237USDT,0.021DOGE,,,
2026-07-13 06:21:03,DOGEUSDT,BUY,0.07329,21DOGE,1.53909USDT,0.021DOGE,,,
2026-07-08 11:22:36,DOGEUSDT,BUY,0.07257,23DOGE,1.66911USDT,0.023DOGE
2026-07-08 11:22:36,DOGEUSDT,BUY,0.07257,104DOGE,7.54728USDT,0.104DOGE
2026-07-08 11:01:41,DOGEUSDT,BUY,0.07275,64DOGE,4.656USDT,0.064DOGE
2026-07-08 10:37:23,DOGEUSDT,BUY,0.07294,32DOGE,2.33408USDT,0.032DOGE
2026-07-08 10:36:33,DOGEUSDT,BUY,0.07312,16DOGE,1.16992USDT,0.016DOGE
2026-07-07 13:04:56,DOGEUSDT,BUY,0.07495,33DOGE,2.47335USDT,0.033DOGE
2026-07-07 11:31:16,DOGEUSDT,BUY,0.07552,66DOGE,4.98432USDT,0.066DOGE
2026-07-07 10:29:20,DOGEUSDT,BUY,0.0761,131DOGE,9.9691USDT,0.131DOGE
2026-07-06 21:57:42,DOGEUSDT,BUY,0.07529,16DOGE,1.20464USDT,0.016DOGE
2026-07-06 21:03:26,DOGEUSDT,BUY,0.07587,33DOGE,2.50371USDT,0.033DOGE
2026-07-06 14:31:06,DOGEUSDT,BUY,0.07645,65DOGE,4.96925USDT,0.065DOGE
2026-07-06 14:00:27,DOGEUSDT,BUY,0.07703,129DOGE,9.93687USDT,0.129DOGE
2026-07-05 18:39:47,DOGEUSDT,BUY,0.0756,33DOGE,2.4948USDT,0.033DOGE
2026-07-05 11:16:08,DOGEUSDT,BUY,0.07618,65DOGE,4.9517USDT,0.065DOGE
2026-07-05 10:33:18,DOGEUSDT,BUY,0.07676,129DOGE,9.90204USDT,0.129DOGE
2026-07-02 09:41:56,DOGEUSDT,BUY,0.07175,137DOGE,9.82975USDT,0.137DOGE
2026-07-02 08:06:34,DOGEUSDT,BUY,0.07272,135DOGE,9.8172USDT,0.135DOGE
2026-07-01 17:33:55,DOGEUSDT,BUY,0.0709,35DOGE,2.4815USDT,0.035DOGE
2026-07-01 15:37:04,DOGEUSDT,BUY,0.07144,69DOGE,4.92936USDT,0.069DOGE
2026-07-01 15:20:19,DOGEUSDT,BUY,0.07199,137DOGE,9.86263USDT,0.137DOGE
2026-06-30 20:55:37,DOGEUSDT,BUY,0.07169,26DOGE,1.86394USDT,0.026DOGE
2026-06-30 20:54:07,DOGEUSDT,BUY,0.07193,51DOGE,3.66843USDT,0.051DOGE
2026-06-30 13:10:19,DOGEUSDT,BUY,0.07218,102DOGE,7.36236USDT,0.102DOGE
2026-06-29 07:53:22,DOGEUSDT,BUY,0.07247,36DOGE,2.60892USDT,0.036DOGE
2026-06-28 04:33:08,DOGEUSDT,BUY,0.07463,18DOGE,1.34334USDT,0.018DOGE
2026-06-28 03:02:41,DOGEUSDT,BUY,0.07539,35DOGE,2.63865USDT,0.035DOGE
2026-06-26 21:40:19,DOGEUSDT,BUY,0.07324,36DOGE,2.63664USDT,0.036DOGE
2026-06-26 12:01:45,DOGEUSDT,BUY,0.07249,18DOGE,1.30482USDT,0.018DOGE
2026-06-26 11:12:50,DOGEUSDT,BUY,0.07323,36DOGE,2.63628USDT,0.036DOGE
2026-06-25 01:51:49,DOGEUSDT,BUY,0.07376,18DOGE,1.32768USDT,0.018DOGE
2026-06-25 00:33:20,DOGEUSDT,BUY,0.07546,17DOGE,1.28282USDT,0.017DOGE
2026-06-25 00:31:43,DOGEUSDT,BUY,0.07623,34DOGE,2.59182USDT,0.034DOGE
2026-06-24 22:23:27,DOGEUSDT,BUY,0.07716,34DOGE,2.62344USDT,0.034DOGE
2026-06-24 22:06:09,DOGEUSDT,BUY,0.07815,34DOGE,2.6571USDT,0.034DOGE
2026-06-23 23:57:23,DOGEUSDT,BUY,0.07898,33DOGE,2.60634USDT,0.033DOGE
2026-06-23 17:17:34,DOGEUSDT,BUY,0.07876,17DOGE,1.33892USDT,0.017DOGE
2026-06-23 17:15:14,DOGEUSDT,BUY,0.07957,33DOGE,2.62581USDT,0.033DOGE
2026-06-23 15:08:26,DOGEUSDT,BUY,0.08046,21DOGE,1.68966USDT,0.021DOGE
2026-06-23 15:08:01,DOGEUSDT,BUY,0.08128,21DOGE,1.70688USDT,0.021DOGE
2026-06-23 07:28:07,DOGEUSDT,BUY,0.08227,21DOGE,1.72767USDT,0.021DOGE"""

CUTOFF_DATE = "2026-07-13"
PROFIT_PCT = 0.6  # matches settings.json grid_trading.profit_per_grid_pct


def parse_qty(raw: str) -> float:
    """Extract numeric quantity from '21DOGE' or '0.00015BTC'."""
    return float(re.match(r"[\d.]+", raw).group())


def make_trade_id(ts: str, symbol: str, price: str, qty: str) -> str:
    """Generate a deterministic trade_id from row data."""
    seed = f"{ts}|{symbol}|{price}|{qty}"
    return "csv_" + hashlib.sha256(seed.encode()).hexdigest()[:12]


def parse_line(line: str) -> dict | None:
    """Parse one CSV line. Returns dict or None if not a BUY."""
    line = line.strip().rstrip(",")
    if not line:
        return None

    parts = line.split(",")
    if len(parts) < 7:
        return None

    timestamp = parts[0].strip()
    symbol = parts[1].strip()
    side = parts[2].strip()
    price = float(parts[3].strip())
    qty_raw = parts[4].strip()
    qty = parse_qty(qty_raw)
    # notional = parts[5].strip()  # unused
    # fee = parts[6].strip() if len(parts) > 6 else ""  # unused

    if side.upper() != "BUY":
        return None  # only import BUY trades

    trade_id = make_trade_id(timestamp, symbol, parts[3].strip(), parts[4].strip())
    sell_price = round(price * (1.0 + PROFIT_PCT / 100.0), 8)
    is_before_cutoff = timestamp < CUTOFF_DATE

    return {
        "trade_id": trade_id,
        "symbol": symbol,
        "exchange_order_id": "",
        "buy_time": timestamp,
        "buy_price": price,
        "buy_qty": qty,
        "sell_price": sell_price,
        "profit_pct": PROFIT_PCT,
        "sell_placed": is_before_cutoff,
        "sell_filled": is_before_cutoff,
    }


def main():
    ledger = get_ledger()

    # Parse all BUY rows
    rows = []
    skipped = 0
    for line in CSV_DATA.strip().split("\n"):
        row = parse_line(line)
        if row:
            rows.append(row)
        else:
            skipped += 1

    print(f"Parsed {len(rows)} BUY trades ({skipped} SELL/other skipped)")

    # Split by cutoff
    pre = [r for r in rows if r["sell_placed"]]
    post = [r for r in rows if not r["sell_placed"]]
    print(f"  Pre-{CUTOFF_DATE}: {len(pre)} (marked completed)")
    print(f"  {CUTOFF_DATE}+: {len(post)} (pending TP)")

    # Import
    count = ledger.import_grid_fill_bulk(rows)
    print(f"\nInserted: {count} rows (duplicates skipped)")

    # Verify
    all_rows = ledger._query("SELECT COUNT(*) as cnt FROM grid_fills")
    pending = ledger.get_pending_tp_fills()
    print(f"\nVerification:")
    print(f"  Total grid_fills rows: {all_rows[0]['cnt']}")
    print(f"  Pending TP (sell_placed=FALSE): {len(pending)}")
    if pending:
        for p in pending:
            print(f"    - {p['buy_time']} | {p['symbol']} | "
                  f"BUY {p['buy_qty']} @ ${float(p['buy_price']):.5f} | "
                  f"TP @ ${float(p['sell_price']):.5f}")

    # Show BUY value summary
    total_buy_usdt = sum(float(r["buy_price"]) * float(r["buy_qty"]) for r in rows)
    print(f"\n  Total BUY notional: ${total_buy_usdt:.2f} USDT")
    print(f"  Post-cutoff BUY notional: ${sum(float(r['buy_price'])*float(r['buy_qty']) for r in post):.2f} USDT")

    ledger.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
