# CLAUDE.md

## Quick Commands

```bash
# Activate venv
.venv\Scripts\activate   # Windows PowerShell
source .venv/bin/activate # Unix

# Tests (100+ passing)
python -m pytest tests/ -q

# Run live trading
python main.py

# Run backtest (need data/DOGEUSDT_5m.json or similar)
python backtest.py --symbol DOGEUSDT --capital 12

# Kill stale processes (Windows)
taskkill /F /IM python.exe

# Reset database (if accounts table missing entries)
python -c "import psycopg2; c=psycopg2.connect(host='localhost',port=5432,user='openquant',password='openquant',dbname='openquant'); c.autocommit=True; c.cursor().execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')"
```

## Architecture (Hard Rules)

1. **strategy/ never imports execution/ or core/** — signals flow through main.py as StrategySignal dataclasses
2. **Grid always runs, trend is opportunistic** — parallel, not regime-switched
3. **Reconciliation is poll-based (5s)** — not event-driven, fills detected via GET /myTrades
4. **TP orders are auto-placed by reconciliation**, not by the strategy layer

## Key Files

| File | Role |
|------|------|
| `main.py` | Event bus, main loop (5-min ticks with 1s sleep chunks) |
| `strategy/grid_strategy.py` | Grid computation (geometric, pyramid, adaptive, tracking reference) |
| `strategy/trend_strategy.py` | Trend signals (EMA cross + MACD + ATR stops) |
| `strategy/regime_detector.py` | 5-factor scoring (ADX/vol/momentum/volume/microstructure) |
| `execution/order_manager.py` | Order dispatch, reconciliation loop, TP auto-placement |
| `execution/exchange_client.py` | Binance REST API (time-synced, recvWindow retry) |
| `data/market_data.py` | WebSocket K-line ingestion (combined-stream format) |
| `core/local_ledger.py` | PostgreSQL double-entry ledger |
| `config/settings.json` | Grid + trading parameters |
| `backtest.py` | Backtest engine with dual-engine support |

## Common Issues & Fixes

- **Windows NotImplementedError on start**: add_signal_handler → wrapped in try/except (fixed)
- **System silent after TICK #5**: asyncio.sleep(300) freeze → 1s sleep chunks (fixed)
- **WebSocket data never updating**: combined-stream format not unwrapped → added `msg["data"]` extraction (fixed)
- **recvWindow timestamp errors**: system clock drift → auto-sync server time at startup + retry (fixed)
- **TP infinite retry loop**: failed TP not deduped → `_tp_attempted` set (fixed)
- **"Account has insufficient balance" on SELL**: selling 21 DOGE when only 20.979 available → use actual balance floored to lot size (fixed)
- **POS-DOGEUSDT foreign key error**: missing from chart of accounts → added to CHART_OF_ACCOUNTS (fixed)
- **Grid levels below $1 minimum**: too many levels for $12 capital → capital-based level cap (fixed)
- **Trend fires immediately on startup**: no warmup → 30-tick cooldown (fixed)
- **Grid rebalance cancels TP orders**: cancel_all_orders too broad → selective cancel preserving tp_* prefix (fixed)

## Log Files

- `logs/trading_system.log` — current hour (rotates hourly, keeps 72 files)
- Format: `TIMESTAMP [LEVEL] module: message`
- TICK lines appear every 5 minutes: `TICK #N | State: ACTIVE_GRID | Equity: $X | Positions: N | DD: X% | Signals: N`
- Order summary tables appear every 60 seconds
