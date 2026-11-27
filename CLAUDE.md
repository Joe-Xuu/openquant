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

# Reset database (if accounts table missing entries) — password comes from PG_PASSWORD in .env
python -c "import os, psycopg2; from dotenv import load_dotenv; load_dotenv(); c=psycopg2.connect(host='localhost',port=5432,user='openquant',password=os.environ['PG_PASSWORD'],dbname='openquant'); c.autocommit=True; c.cursor().execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')"
```

## Architecture (Hard Rules)

1. **strategy/ never imports execution/ or core/** — signals flow through main.py as StrategySignal dataclasses
2. **Grid always runs, trend is opportunistic** — parallel, not regime-switched; prefix-isolated (`grid_`/`tp_`/`rebuy_` vs `trend_`/`trend_sl_`)
3. **Dual-path fill detection** — Binance User Data Stream WebSocket (`executionReport`) for real-time fills + 60s REST `GET /myTrades` fallback reconciliation
4. **TP orders are auto-placed by OrderManager**, strictly capped to `min(available_base, net_fill_qty)` and rounded via cached `SymbolInfo`

## Key Files

| File | Role |
|------|------|
| `main.py` | Event bus, main loop (5-min grid ticks, configurable `trend_interval`) |
| `config/schema.py` | Pydantic v2 validated configuration schema (`.env` overlays `settings.json`) |
| `strategy/grid_strategy.py` | Grid computation (geometric, arithmetic, adaptive, tracking reference) |
| `strategy/trend_strategy.py` | Trend signals (EMA + MACD + ATR stops, Spot long-only guard, full precision) |
| `strategy/regime_detector.py` | Price-scale-invariant 5-factor scoring (ADX/vol/momentum/volume/microstructure) |
| `execution/order_manager.py` | Prefix-isolated order dispatch, User Data Stream WS, fallback reconciliation, TP recovery |
| `execution/exchange_client.py` | Binance REST API (TokenBucket rate limiter, `SymbolInfo` cache, `listenKey`, time-sync) |
| `data/market_data.py` | WebSocket K-line ingestion (combined-stream format) |
| `core/local_ledger.py` | PostgreSQL ACID double-entry ledger (dynamic accounts, non-destructive startup reconciliation) |
| `config/settings.json` | Grid + trend + risk + capital allocation parameters |
| `backtest.py` | Conservative event-driven Spot backtest engine (zero look-ahead, shared RiskGuard & SymbolInfo) |

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
