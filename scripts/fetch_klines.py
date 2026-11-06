"""
================================================================================
FETCH KLINES — Pull historical OHLCV from Binance public REST (no auth needed)
================================================================================

Standalone offline-data fetcher. Uses only the standard library so it runs
without activating the venv or importing any project modules.

The Binance klines endpoint is PUBLIC — no API key required. We hit mainnet
(api.binance.com) because testnet klines are often stale/limited; kline data
is identical between testnet and mainnet anyway (it's market data, not fills).

Output format matches the project's existing convention:
    [{"open": float, "high": float, "low": float, "close": float,
      "volume": float, "timestamp": "YYYY-MM-DDTHH:MM:SS+00:00"}, ...]
sorted oldest → newest.

Usage:
    python scripts/fetch_klines.py DOGEUSDT 5m --bars 5000
    python scripts/fetch_klines.py DOGEUSDT 1h --bars 5000
    python scripts/fetch_klines.py DOGEUSDT 5m --out data/experiment/DOGEUSDT_5m.json
================================================================================
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "https://api.binance.com/api/v3/klines"

# interval string -> milliseconds
_INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def interval_to_ms(interval: str) -> int:
    if interval not in _INTERVAL_MS:
        raise ValueError(f"Unsupported interval {interval!r}. Use one of {sorted(_INTERVAL_MS)}")
    return _INTERVAL_MS[interval]


def fetch_once(symbol: str, interval: str, start_time: int, limit: int) -> list:
    """Fetch one page (up to `limit` bars) starting at start_time (ms)."""
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": str(min(limit, 1000)),
        "startTime": str(start_time),
    }
    url = f"{BASE_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "openquant-experiment"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_bars(symbol: str, interval: str, bars: int = 5000, quiet: bool = False) -> list:
    """
    Fetch the most recent `bars` klines for `symbol` at `interval`, oldest→newest.

    Paginates forward from (now - bars*interval_ms) in chunks of 1000.
    """
    step_ms = interval_to_ms(interval)
    now_ms = int(time.time() * 1000)
    # Align start to the beginning of the current interval so bar boundaries are clean.
    start_ms = now_ms - (now_ms % step_ms) - bars * step_ms

    rows: list = []
    cursor = start_ms
    remaining = bars
    while remaining > 0:
        page = fetch_once(symbol, interval, cursor, remaining)
        if not page:
            break
        rows.extend(page)
        # Advance cursor to just after the last bar we got.
        last_open = int(page[-1][0])
        cursor = last_open + step_ms
        remaining = bars - len(rows)
        if not quiet:
            print(f"  {symbol} {interval}: {len(rows)}/{bars} bars", flush=True)

    if not rows:
        raise RuntimeError(f"No data returned for {symbol} {interval} — check symbol/network.")

    # Binance kline row: [openTime, open, high, low, close, volume, closeTime, ...]
    out = []
    for row in rows:
        out.append(
            {
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "timestamp": datetime.fromtimestamp(
                    int(row[0]) / 1000.0, tz=timezone.utc
                ).isoformat(),
            }
        )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch historical klines from Binance.")
    parser.add_argument("symbol", help="Trading pair, e.g. DOGEUSDT")
    parser.add_argument("interval", help="Kline interval, e.g. 5m / 1h")
    parser.add_argument("--bars", type=int, default=5000, help="Number of bars to fetch")
    parser.add_argument(
        "--out",
        default=None,
        help="Output path. Default: data/experiment/<SYMBOL>_<INTERVAL>.json",
    )
    args = parser.parse_args()

    out_path = args.out or f"data/experiment/{args.symbol}_{args.interval}.json"
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {args.symbol} {args.interval} ({args.bars} bars) ...")
    data = fetch_bars(args.symbol, args.interval, args.bars)

    with open(out_file, "w") as f:
        json.dump(data, f)

    first = data[0]["timestamp"]
    last = data[-1]["timestamp"]
    print(f"Saved {len(data)} bars -> {out_file}")
    print(f"  range: {first} .. {last}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
