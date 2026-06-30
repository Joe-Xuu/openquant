"""
================================================================================
QUANTITATIVE TRADING SYSTEM — Main Entry Point & Event Bus
================================================================================

Regime-Switching Model: Dynamically routes between Arithmetic/Geometric Grid
Trading (ranging markets) and Trend Following (trending markets).

ARCHITECTURE (Strict Decoupling):
    data/       → Market data ingestion & indicators (Sensors)
    strategy/   → Multi-factor scoring & signal generation (Brain)
    core/       → State machine & local double-entry ledger (Memory)
    risk/       → Independent risk guard (Immune System)
    execution/  → Exchange API routing & concurrency (Hands)

DATA FLOW:
    1. data/       → OHLCV candles streamed via WebSocket
    2. data/       → Compute indicators (EMA, MACD, ADX, ATR, etc.)
    3. strategy/   → RegimeDetector scores market (0=ranging, 1=trending)
    4. strategy/   → GridStrategy OR TrendStrategy generates signal
    5. core/       → StateMachine validates transition is allowed
    6. risk/       → RiskGuard checks drawdown/exposure/position limits
    7. execution/  → OrderManager dispatches to exchange
    8. core/       → LedgerEngine records everything (double-entry)

The Brain NEVER calls the Hands. main.py is the event bus.
================================================================================
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv()

# --- Core ---
from core.local_ledger import LedgerEngine, get_ledger
from core.state_machine import StateTransition, SystemState, TradingStateMachine

# --- Data ---
from data.indicators import compute_all, IndicatorBundle
from data.market_data import MarketDataEngine

# --- Strategy ---
from strategy.signal import SignalAction, StrategySignal
from strategy.grid_strategy import GridConfig, GridStatus, GridStrategy
from strategy.trend_strategy import TrendState, TrendStrategy
from strategy.regime_detector import MarketRegime, RegimeDetector, RegimeResult

# --- Risk ---
from risk.risk_guard import RiskGuard, RiskVerdict, Verdict

# --- Execution ---
from execution.exchange_client import ExchangeClient
from execution.order_manager import OrderManager

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

Path("logs").mkdir(exist_ok=True)

# Rotating log files: new file every hour, keep 72 backups (3 days)
from logging.handlers import TimedRotatingFileHandler

file_handler = TimedRotatingFileHandler(
    filename="logs/trading_system.log",
    when="H",          # rotate hourly
    interval=1,        # every 1 hour
    backupCount=72,    # keep last 72 hours (3 days)
    encoding="utf-8",
)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)-7s] %(name)s: %(message)s"
))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        file_handler,
    ],
)
logger = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Configuration Loader
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config/settings.json") -> dict:
    """Load system configuration from JSON."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Trading System Orchestrator
# ---------------------------------------------------------------------------

class TradingSystem:
    """
    Top-level orchestrator for the entire trading system.

    Wires together all components and runs the main event loop:
        data → indicators → regime → strategy → state → risk → execution → ledger
    """

    def __init__(self, config: dict):
        self.config = config
        self.running = False

        # --- Initialize Core ---
        self.ledger: LedgerEngine = get_ledger()
        self.state_machine = TradingStateMachine()

        # --- Initialize Data ---
        data_cfg = config.get("data", {})
        exchange_cfg = config.get("exchange", {})
        use_testnet = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
        use_futures = os.getenv("BINANCE_MARKET", "spot").lower() == "futures"

        # REST API: testnet for orders, mainnet for... actually testnet for orders
        # WebSocket: ALWAYS use mainnet for market data (free, public, reliable)
        # The testnet WebSocket is frequently unavailable — we get price data
        # from the real market but execute orders on testnet.
        if use_futures:
            rest_url = "https://testnet.binancefuture.com" if use_testnet else "https://fapi.binance.com"
            ws_url = "wss://fstream.binance.com/ws"
        else:
            rest_url = "https://testnet.binance.vision" if use_testnet else "https://api.binance.com"
            ws_url = "wss://stream.binance.com:9443/ws"

        self.data_engine = MarketDataEngine(
            symbols=config.get("trading", {}).get("symbols", ["BTCUSDT"]),
            intervals=data_cfg.get("kline_intervals", ["1m", "5m", "1h"]),
            primary_interval=data_cfg.get("primary_interval", "5m"),
            max_klines_per_request=data_cfg.get("max_klines_per_request", 500),
            ws_reconnect_delay=data_cfg.get("ws_reconnect_delay_seconds", 5),
            testnet=use_testnet,
            rest_base_url=rest_url,
            ws_base_url=ws_url,
        )

        # --- Initialize Strategy ---
        strategy_cfg = config.get("strategy", {})
        grid_cfg = strategy_cfg.get("grid_trading", {})
        trend_cfg = strategy_cfg.get("trend_following", {})
        regime_cfg = strategy_cfg.get("regime_detection", {})
        mf_cfg = strategy_cfg.get("multi_factor", {})

        # Grid uses 70% of capital so total exposure stays within risk limit (80%)
        max_exposure_pct = config.get("risk", {}).get("max_exposure_pct", 80.0) / 100.0
        grid_capital = config.get("trading", {}).get("initial_capital", 10000.0) * 0.85  # 85% for small account

        self.grid_strategy = GridStrategy(
            grid_type=grid_cfg.get("type", "geometric"),
            upper_bound_pct=grid_cfg.get("upper_bound_pct", 5.0),
            lower_bound_pct=grid_cfg.get("lower_bound_pct", 5.0),
            grid_levels=grid_cfg.get("grid_levels", 10),
            profit_per_grid_pct=grid_cfg.get("profit_per_grid_pct", 0.5),
            total_capital=grid_capital,
            rebalance_threshold_pct=grid_cfg.get("rebalance_threshold_pct", 3.0),
        )

        self.trend_strategy = TrendStrategy(
            ema_fast=trend_cfg.get("ema_fast", 12),
            ema_slow=trend_cfg.get("ema_slow", 26),
            macd_signal_period=trend_cfg.get("macd_signal", 9),
            atr_period=trend_cfg.get("atr_period", 14),
            atr_multiplier=trend_cfg.get("atr_multiplier", 2.0),
            trailing_stop_atr_multiplier=trend_cfg.get("trailing_stop_atr_multiplier", 3.0),
        )

        self.regime_detector = RegimeDetector(
            weights=mf_cfg.get("weights"),
            adx_threshold=regime_cfg.get("adx_threshold", 25.0),
            volatility_window=regime_cfg.get("volatility_window", 20),
            volatility_percentile_low=regime_cfg.get("volatility_percentile_low", 25.0),
            volatility_percentile_high=regime_cfg.get("volatility_percentile_high", 75.0),
            lookback_periods=regime_cfg.get("lookback_periods", 100),
        )

        # --- Initialize Risk ---
        risk_cfg = config.get("risk", {})
        # Allow .env overrides for risk parameters
        for key, env_key in [
            ("max_drawdown_pct", "MAX_DRAWDOWN_PCT"),
            ("max_daily_loss_pct", "MAX_DAILY_LOSS_PCT"),
            ("max_position_size_pct", "MAX_POSITION_SIZE_PCT"),
        ]:
            env_val = os.getenv(env_key)
            if env_val is not None:
                risk_cfg[key] = float(env_val)

        self.risk_guard = RiskGuard(
            config=risk_cfg,
            equity_provider=self._get_equity,
            positions_provider=self._get_positions,
            trade_history_provider=self._get_trade_history,
        )

        # --- Initialize Execution ---
        # Load API credentials: env vars take priority over config file
        use_testnet = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
        use_futures = os.getenv("BINANCE_MARKET", "spot").lower() == "futures"

        if use_futures:
            api_key = os.getenv("BINANCE_FUTURES_API_KEY", "") or os.getenv("BINANCE_TESTNET_API_KEY", "")
            api_secret = os.getenv("BINANCE_FUTURES_API_SECRET", "") or os.getenv("BINANCE_TESTNET_API_SECRET", "")
        else:
            api_key = os.getenv(
                "BINANCE_TESTNET_API_KEY" if use_testnet else "BINANCE_API_KEY", ""
            ) or exchange_cfg.get("testnet_api_key" if use_testnet else "api_key", "")
            api_secret = os.getenv(
                "BINANCE_TESTNET_API_SECRET" if use_testnet else "BINANCE_API_SECRET", ""
            ) or exchange_cfg.get("testnet_api_secret" if use_testnet else "api_secret", "")

        if not api_key or api_key.startswith("your_"):
            logger.warning("  API keys not configured! Set them in .env file")
            logger.warning("  cp .env.example .env  →  edit .env with your keys")

        market_type = "futures" if use_futures else "spot"
        logger.info(f"  Market: {market_type} | Testnet: {use_testnet}")

        self.exchange_client = ExchangeClient(
            api_key=api_key,
            api_secret=api_secret,
            testnet=use_testnet,
            market=market_type,
            recv_window=exchange_cfg.get("recv_window", 5000),
            rate_limit_rps=exchange_cfg.get("rate_limit_rps", 10.0),
        )

        self.order_manager = OrderManager(
            exchange_client=self.exchange_client,
            ledger_record_order=self._ledger_record_order,
            ledger_order_open=self._ledger_order_open,
            ledger_update_fill=self._ledger_update_fill,
            ledger_record_trade_open=self._ledger_record_trade_open,
            ledger_record_trade_close=self._ledger_record_trade_close,
            ledger_register_grid=self._ledger_register_grid,
        )

        # --- Per-Symbol State ---
        self._trend_states: Dict[str, TrendState] = {}
        self._grid_configs: Dict[str, GridConfig] = {}
        self._current_regime: Dict[str, MarketRegime] = {}

        # --- Statistics ---
        self._tick_count = 0
        self._signal_count = 0
        self._start_time: Optional[float] = None

    # ------------------------------------------------------------------
    # Ledger Callbacks (bridge between execution and core)
    # ------------------------------------------------------------------

    async def _get_equity_async(self) -> float:
        """Return current total equity from exchange (live balances × prices)."""
        try:
            balances = await self.exchange_client.get_balances()
            symbols = self.config.get("trading", {}).get("symbols", ["DOGEUSDT"])
            equity = 0.0
            for b in balances:
                if b.asset == "USDT":
                    equity += b.total
                else:
                    # Look up price for this asset
                    sym = f"{b.asset}USDT"
                    try:
                        px = await self.exchange_client.get_ticker_price(sym)
                        equity += b.total * px
                    except Exception:
                        pass  # skip assets we can't price
            return equity if equity > 0 else self.ledger.get_total_equity()
        except Exception:
            return self.ledger.get_total_equity()

    def _get_equity(self) -> float:
        """Sync wrapper: return equity from ledger (updated by async fetch)."""
        return self.ledger.get_total_equity()

    def _get_positions(self) -> List[Dict]:
        """Return open positions from the ledger."""
        positions = self.ledger.get_all_positions()
        return [
            {
                "symbol": p.symbol,
                "quantity": p.quantity,
                "avg_entry_price": p.avg_entry_price,
                "current_price": p.current_mark_price or p.avg_entry_price,
                "unrealized_pnl": p.unrealized_pnl or 0.0,
            }
            for p in positions
        ]

    def _get_trade_history(self, symbol: str) -> List[Dict]:
        """Return recent trades for risk calculations."""
        trades = self.ledger.get_trade_history(symbol=symbol, limit=50)
        return [
            {
                "trade_id": t.trade_id,
                "symbol": t.symbol,
                "pnl_realized": t.pnl_realized or 0.0,
                "status": t.status,
                "entry_time": t.entry_time,
            }
            for t in trades
        ]

    def _ledger_record_order(self, trade_id, symbol, side, order_type, quantity, price=None, **kw):
        """Record an order in the ledger (before API dispatch)."""
        return self.ledger.record_order(
            trade_id=trade_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
        )

    def _ledger_order_open(self, order_id, exchange_order_id):
        """Update order status to OPEN after exchange confirms placement."""
        self.ledger.update_order_open(order_id, exchange_order_id)

    def _ledger_update_fill(self, order_id, exchange_order_id, filled_quantity, fill_price, fee=0.0):
        """Update order fill in the ledger."""
        self.ledger.update_order_fill(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            filled_quantity=filled_quantity,
            fill_price=fill_price,
            fee=fee,
        )

    def _ledger_register_grid(self, trade_id, symbol):
        """Register a grid as a trade container (no journal entries)."""
        return self.ledger.register_grid_trade(trade_id, symbol)

    def _ledger_record_trade_open(self, trade_id, symbol, side, quantity, price, fee=0.0, slippage=0.0):
        """Record trade open in the ledger."""
        self.ledger.record_trade_open(
            trade_id=trade_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            fee=fee,
            slippage=slippage,
        )

    def _ledger_record_trade_close(self, trade_id, exit_price, fee=0.0, slippage=0.0):
        """Record trade close in the ledger."""
        return self.ledger.record_trade_close(
            trade_id=trade_id,
            exit_price=exit_price,
            fee=fee,
            slippage=slippage,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Start all components and enter the main event loop."""
        logger.info("=" * 60)
        logger.info(" QUANT TRADING SYSTEM v1.0 — Regime-Switching Model")
        logger.info("=" * 60)
        self._start_time = time.time()

        # Start exchange client FIRST (needed for sync)
        await self.exchange_client.start()

        # ---- FULL STARTUP SYNC: exchange balances → ledger ----
        # Every restart, pull real balances from Binance and seed the ledger.
        try:
            balances = await self.exchange_client.get_balances()
            symbols = self.config["trading"]["symbols"]
            usdt_bal = sum(b.total for b in balances if b.asset == "USDT")
            total_equity = usdt_bal

            for sym in symbols:
                base = sym.replace("USDT", "")
                try:
                    px = await self.exchange_client.get_ticker_price(sym)
                except Exception:
                    px = 0.0
                qty = sum(b.total for b in balances if b.asset == base)
                total_equity += qty * px
                logger.info(f"  Exchange: {qty:.4f} {base} @ ${px:.5f} = ${qty*px:.2f}")

            logger.info(f"  Exchange: ${usdt_bal:.2f} USDT")
            logger.info(f"  Total equity from exchange: ${total_equity:.2f}")

            # Reset ledger to match exchange reality
            import psycopg2
            conn = psycopg2.connect(self.ledger._dsn)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("DELETE FROM orders")
                cur.execute("DELETE FROM trades")
                cur.execute("DELETE FROM journal_lines")
                cur.execute("DELETE FROM journal_entries")
                cur.execute("DELETE FROM positions")
            conn.close()

            # Seed correct capital
            self.ledger.record_initial_capital(total_equity)
            self.config.setdefault("trading", {})["initial_capital"] = total_equity
            self.grid_strategy.total_capital = total_equity * 0.85
            logger.info(f"  Ledger synced to exchange: ${total_equity:.2f}")

        except Exception as e:
            logger.warning(f"  Could not sync with exchange: {e}. Using ledger as-is.")
            total_equity = self.ledger.get_total_equity()
            if total_equity <= 0:
                total_equity = self.config.get("trading", {}).get("initial_capital", 10000.0)
                self.ledger.record_initial_capital(total_equity)
                self.grid_strategy.total_capital = total_equity * 0.85

        # Start order manager
        await self.order_manager.start()

        # Startup cleanup: cancel stale orders. Do NOT reconcile here —
        # the background reconciliation loop runs every 5s and will
        # handle fills AFTER the grid is deployed (grid_active=True).
        logger.info("Running startup cleanup...")
        for symbol in self.config.get("trading", {}).get("symbols", []):
            try:
                await self.exchange_client.cancel_all_orders(symbol)
                logger.info(f"  Cleared old orders for {symbol}")
            except Exception as e:
                logger.debug(f"  No old orders to clear for {symbol}: {e}")
        logger.info("Startup cleanup complete")

        # Start data engine (fetches historical + opens WebSocket)
        self.state_machine.transition(StateTransition.DATA_READY)
        await self.data_engine.start()

        # Wait for data warm-up
        logger.info("Warming up indicators...")
        await asyncio.sleep(5)  # Allow some candles to accumulate

        self.running = True
        logger.info("Trading system started. Entering main loop.")

        # --- Main Event Loop ---
        await self._main_loop()

    async def stop(self):
        """Gracefully shut down all components."""
        logger.info("Shutting down...")
        self.running = False
        self.state_machine.shutdown()

        # Save shutdown timestamp for next startup reconciliation
        self.ledger.set_metadata("last_shutdown", datetime.now(timezone.utc).isoformat())
        # NOTE: We do NOT cancel open orders — they survive restarts.
        # The next startup will reconcile fills and resume grid state.

        await self.order_manager.stop()
        await self.data_engine.stop()

        # Final snapshot
        self.ledger.take_snapshot()

        # Summary
        elapsed = time.time() - self._start_time if self._start_time else 0
        logger.info(f"Session summary: {self._tick_count} ticks, "
                     f"{self._signal_count} signals, {elapsed:.0f}s runtime")
        stats = self.ledger.get_trade_statistics()
        logger.info(f"Trade stats: {stats}")
        await self.exchange_client.close()
        self.ledger.close()
        logger.info("Trading system stopped.")

    # ------------------------------------------------------------------
    # Main Event Loop
    # ------------------------------------------------------------------

    async def _main_loop(self):
        """
        Core tick loop — the central nervous system.

        Each tick for each symbol:
            1. Get latest OHLCV data
            2. Compute indicators
            3. Score regime
            4. Generate strategy signal
            5. Validate state machine transition
            6. Validate risk
            7. Dispatch to execution
            8. Record in ledger
        """
        symbols = self.config.get("trading", {}).get("symbols", ["DOGEUSDT"])
        self.grid_capital_pct = 0.60
        tick_interval = self._get_tick_interval_seconds()
        self._last_tick_time = time.time()

        # Watchdog: independent task warns if main loop stalls
        watchdog_task = asyncio.create_task(self._watchdog(tick_interval))

        while self.running:
            try:
                for symbol in symbols:
                    try:
                        await asyncio.wait_for(
                            self._process_symbol_tick(symbol),
                            timeout=30.0,
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"Tick timed out for {symbol} after 30s — skipping")
                    except Exception as e:
                        logger.exception(f"Error in tick for {symbol}: {e}")

                self._tick_count += 1
                self._last_tick_time = time.time()

                # Minutely order summary: clean table every 60s
                now = time.time()
                if not hasattr(self, '_last_summary_time'):
                    self._last_summary_time = 0.0
                if now - self._last_summary_time >= 60:
                    self._last_summary_time = now
                    await self._print_order_summary()

                # Log every tick for full visibility
                await self._log_status_async()

                if self._tick_count % 60 == 0:
                    self.ledger.take_snapshot()
                    self.risk_guard.update_peak_equity()

                # Split sleep into 1-second chunks (Windows event-loop stability)
                remaining = tick_interval
                while remaining > 0 and self.running:
                    await asyncio.sleep(1.0)
                    remaining -= 1.0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in main loop: {e}")
                remaining = tick_interval
                while remaining > 0 and self.running:
                    await asyncio.sleep(1.0)
                    remaining -= 1.0

    async def _process_symbol_tick(self, symbol: str):
        """
        Process one tick for one symbol: data → indicators → regime → signal.
        """
        # 0. Data freshness — warn if WebSocket data is stale, but continue
        data_age = self.data_engine.get_data_age(symbol)
        max_stale = self._get_tick_interval_seconds() * 3
        grace_period = self._get_tick_interval_seconds() * 3
        runtime = time.time() - self._start_time if self._start_time else 0
        if runtime > grace_period and data_age > max_stale:
            age_str = f"{data_age:.0f}s" if data_age != float("inf") else "never"
            logger.warning(
                f"WebSocket data stale for {symbol}: {age_str} (limit={max_stale:.0f}s). "
                f"Proceeding with cached data."
            )

        # 1. Get latest OHLCV data
        ohlcv = await self.data_engine.get_ohlcv(symbol)
        if len(ohlcv) < 30:
            return  # Not enough data for reliable indicators

        # 2. Compute indicators
        indicators = compute_all(symbol, ohlcv)

        # Skip if indicators are stale
        if indicators.atr <= 0 or indicators.close <= 0:
            return

        # 3. Score regime
        regime_result = self.regime_detector.detect(
            ohlcv=ohlcv,
            current_regime=self._current_regime.get(symbol, MarketRegime.RANGING),
            adx=indicators.adx,
            plus_di=indicators.plus_di,
            minus_di=indicators.minus_di,
            ema_fast=indicators.ema_fast,
            ema_slow=indicators.ema_slow,
            macd_hist=indicators.macd_hist,
            atr=indicators.atr,
            volume=indicators.volume,
        )

        # Update current regime
        self._current_regime[symbol] = regime_result.regime

        # 4. Generate strategy signals — grid ALWAYS, trend INDEPENDENTLY
        grid_signal: Optional[StrategySignal] = None
        trend_signal: Optional[StrategySignal] = None
        is_ranging = regime_result.is_ranging

        # ---- GRID (always running) ----
        current_grid = self._grid_configs.get(symbol)

        # Stop-loss check first
        if current_grid is not None:
            if self.grid_strategy.check_stop_loss(indicators.close, current_grid):
                logger.error(
                    f"⛔ GRID STOP-LOSS triggered for {symbol}! "
                    f"Price {indicators.close:.5f} <= {current_grid.stop_loss_price:.5f}"
                )
                grid_signal = self.grid_strategy.generate_stop_signal(
                    current_grid, reason="stop_loss"
                )
                self._grid_configs.pop(symbol, None)
                self.state_machine.emergency_stop()

        # ---- Trend gate: pause new grid buys in strong trends ----
        # When the market is clearly trending, the grid keeps chasing price
        # downhill, draining USDT into a falling knife. Pause new grid entries
        # and let trend strategy handle the directional move. Existing TP
        # orders are preserved by the order manager's rebalance logic.
        strong_trend = regime_result.is_trending and regime_result.confidence > 0.80
        was_trending = getattr(self, '_trend_paused_grid', False)
        if strong_trend and not was_trending and current_grid is not None:
            logger.warning(
                f"⚠ Strong trend detected (conf={regime_result.confidence:.2f}). "
                f"Pausing new grid buys — trend strategy takes over."
            )
            self._trend_paused_grid = True
        elif not strong_trend and was_trending:
            logger.info("✓ Trend subsided — resuming grid")
            self._trend_paused_grid = False
            current_grid = None  # force fresh deployment

        if grid_signal is None:
            if strong_trend and current_grid is not None:
                # Grid frozen — don't deploy, don't rebalance. Let existing
                # positions ride with their TP orders on the exchange.
                pass
            elif current_grid is None:
                ref_price = self.grid_strategy.compute_rebalance_price(ohlcv)
                current_grid = self.grid_strategy.compute_grid(
                    reference_price=ref_price, symbol=symbol,
                    atr=indicators.atr, is_ranging=is_ranging,
                )
                self._grid_configs[symbol] = current_grid
                grid_signal = self.grid_strategy.generate_signal(current_grid)
                logger.info(
                    f" Grid deployed: {symbol} {len(current_grid.levels)}lvls "
                    f"@ {ref_price:.5f} sl={current_grid.stop_loss_price:.5f}"
                )
                self.state_machine.activate_grid()

            elif self.grid_strategy.check_rebalance(indicators.close, current_grid):
                ref_price = self.grid_strategy.compute_rebalance_price(ohlcv)
                new_grid = self.grid_strategy.compute_grid(
                    reference_price=ref_price, symbol=symbol,
                    atr=indicators.atr, is_ranging=is_ranging,
                )
                self._grid_configs[symbol] = new_grid
                grid_signal = self.grid_strategy.generate_signal(new_grid)
                logger.info(
                    f" Grid rebalanced: {symbol} @ {ref_price:.5f} "
                    f"({len(new_grid.levels)}lvls)"
                )

        # ---- TREND (independently evaluated, only on strong signals) ----
        # Startup cooldown: no trend trades for first 30 ticks (~2.5h) to let
        # indicators stabilize and prevent insane position sizing at launch.
        trend_cooldown = self._tick_count < 30
        if regime_result.is_trending and regime_result.confidence > 0.80 and not trend_cooldown:
            trend_state = self._trend_states.get(symbol, TrendState.flat(symbol))
            raw_trend = self.trend_strategy.evaluate(
                symbol=symbol, current_price=indicators.close,
                ema_fast_val=indicators.ema_fast, ema_slow_val=indicators.ema_slow,
                macd_hist=indicators.macd_hist, atr=indicators.atr,
                current_state=trend_state,
                capital=await self._get_equity_async(),
            )
            if raw_trend is not None:
                # Only accept ENTRY when flat, or EXIT when in position.
                # Ignore "hold" signals that keep the same state.
                if raw_trend.action == SignalAction.START_TREND and not trend_state.is_active:
                    trend_signal = raw_trend
                elif raw_trend.action == SignalAction.STOP_TREND and trend_state.is_active:
                    trend_signal = raw_trend

        # 5. Route signals — trend takes priority if both fire
        if trend_signal is not None:
            await self._route_signal(trend_signal)
        if grid_signal is not None:
            await self._route_signal(grid_signal)

    async def _route_signal(self, signal: StrategySignal):
        """
        Route a signal through the risk guard and (if approved) to execution.

        This is the "event bus" — the only place where strategy output
        meets execution input.
        """
        self._signal_count += 1

        # Skip expired signals
        if signal.is_expired():
            logger.debug(f"Discarded expired signal: {signal}")
            return

        # Check circuit breaker for volatility spikes
        # (would be computed from indicators in production)

        # --- Risk Check ---
        verdict = self.risk_guard.check_signal(signal)

        if verdict.verdict == Verdict.BLOCKED:
            logger.warning(f" SIGNAL BLOCKED by risk: {verdict.reason}")
            return

        # --- Dispatch ---
        modified_params = verdict.modified_params if verdict.verdict == Verdict.MODIFIED else None
        if modified_params:
            logger.info(f"Signal modified by risk: {verdict.reason}")

        try:
            tracked_orders = await self.order_manager.dispatch_signal(signal, modified_params)
            logger.info(
                f"Dispatched signal {signal.signal_id}: "
                f"{signal.action.value} {signal.symbol} → {len(tracked_orders)} orders"
            )
            # Grid is truly active only after successful dispatch (not just signal generation)
            if signal.action == SignalAction.START_GRID and len(tracked_orders) > 0:
                self.order_manager.grid_active = True
                self.order_manager.grid_deployed_at = time.time()

            # Update strategy state based on dispatch result
            if signal.action == SignalAction.START_TREND:
                fill_price = signal.metadata.get("entry_price", 0.0)
                old_state = self._trend_states.get(signal.symbol, TrendState.flat(signal.symbol))
                new_state = self.trend_strategy.update_state(old_state, signal, fill_price)
                self._trend_states[signal.symbol] = new_state

            elif signal.action == SignalAction.STOP_TREND:
                self._trend_states[signal.symbol] = TrendState.flat(signal.symbol)

            elif signal.action == SignalAction.STOP_GRID:
                if signal.symbol in self._grid_configs:
                    del self._grid_configs[signal.symbol]

            # Update circuit breaker with trade results
            if signal.action == SignalAction.STOP_TREND:
                pnl = signal.metadata.get("pnl_pct", 0.0)
                self.risk_guard.register_trade_result(pnl)

        except Exception as e:
            logger.error(f"Failed to dispatch signal {signal.signal_id}: {e}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _watchdog(self, tick_interval: float) -> None:
        """Independent watchdog: logs if main loop hasn't ticked for 2× interval."""
        while self.running:
            remaining = tick_interval * 2
            while remaining > 0 and self.running:
                await asyncio.sleep(1.0)
                remaining -= 1.0
            if not self.running:
                break
            elapsed = time.time() - self._last_tick_time
            if elapsed > tick_interval * 2:
                logger.error(
                    f"WATCHDOG: Main loop stalled! {elapsed:.0f}s since last tick "
                    f"(tick_interval={tick_interval}s, tick_count={self._tick_count})"
                )

    def _get_tick_interval_seconds(self) -> float:
        """Convert primary interval string to seconds."""
        interval = self.config.get("data", {}).get("primary_interval", "5m")
        unit = interval[-1]
        value = int(interval[:-1])
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return value * multipliers.get(unit, 60)

    async def _print_order_summary(self):
        """Print a clean minutely summary: open orders table + real P&L."""
        try:
            # Fetch open orders and account from exchange
            orders = await self.exchange_client.get_open_orders()
            balances = await self.exchange_client.get_balances()

            buys = []
            sells = []
            for o in orders:
                if o.get("side") == "BUY":
                    buys.append((float(o["price"]), float(o["origQty"])))
                else:
                    sells.append((float(o["price"]), float(o["origQty"])))

            buys.sort(key=lambda x: x[0], reverse=True)
            sells.sort(key=lambda x: x[0])

            # Real equity from exchange
            usdt = sum(b.total for b in balances if b.asset == "USDT")
            equity = usdt
            for sym in self.config["trading"]["symbols"]:
                try:
                    px = await self.exchange_client.get_ticker_price(sym)
                except Exception:
                    px = 0
                base = sym.replace("USDT", "")
                qty = sum(b.total for b in balances if b.asset == base)
                equity += qty * px

            pnl = equity - float(self.config["trading"].get("initial_capital", 12))
            pnl_pct = pnl / float(self.config["trading"].get("initial_capital", 12)) * 100

            # Build table
            lines = []
            lines.append("")
            lines.append("┌──────────────────────────────────────────────────┐")
            lines.append("│              ORDER SUMMARY (60s)                 │")
            lines.append("├─────────────────┬────────────────────────────────┤")
            lines.append(f"│ {'BUY orders':<15} │ {'SELL orders':<30} │")
            lines.append("├─────────────────┼────────────────────────────────┤")
            max_rows = max(len(buys), len(sells), 1)
            for i in range(max_rows):
                buy_str = f"{buys[i][1]:.0f} @ {buys[i][0]:.5f}" if i < len(buys) else ""
                sell_str = f"{sells[i][1]:.0f} @ {sells[i][0]:.5f}" if i < len(sells) else ""
                lines.append(f"│ {buy_str:<15} │ {sell_str:<30} │")
            lines.append("├─────────────────┴────────────────────────────────┤")
            lines.append(f"│ Equity: ${equity:.4f}  |  P&L: {pnl:+.4f} ({pnl_pct:+.2f}%)  │")
            lines.append("└──────────────────────────────────────────────────┘")
            for line in lines:
                logger.info(line)
        except Exception as e:
            logger.debug(f"Order summary failed: {e}")

    async def _log_status_async(self):
        """Log current system status with live exchange equity."""
        sm = self.state_machine.snapshot()
        equity = await self._get_equity_async()
        positions = len(self.ledger.get_all_positions())
        risk = self.risk_guard.get_risk_summary()

        logger.info(
            f"TICK #{self._tick_count} | State: {sm.state} | "
            f"Equity: ${equity:,.2f} | Positions: {positions} | "
            f"DD: {risk['drawdown_pct']:.1f}% | "
            f"Signals: {self._signal_count}"
        )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

async def main():
    """Application entry point."""
    config = load_config()
    system = TradingSystem(config)

    # Graceful shutdown on signals (Unix only; Windows raises NotImplementedError)
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(system.stop()))
    except NotImplementedError:
        # Windows: asyncio signal handlers not supported. Ctrl+C still works.
        pass

    try:
        await system.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
    finally:
        await system.stop()


if __name__ == "__main__":
    asyncio.run(main())
