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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/trading_system.log", encoding="utf-8"),
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
        db_path = config.get("database", {}).get("path", "data/trading_ledger.db")
        self.ledger: LedgerEngine = get_ledger(db_path)
        self.state_machine = TradingStateMachine()

        # --- Initialize Data ---
        data_cfg = config.get("data", {})
        exchange_cfg = config.get("exchange", {})
        self.data_engine = MarketDataEngine(
            symbols=config.get("trading", {}).get("symbols", ["BTCUSDT"]),
            intervals=data_cfg.get("kline_intervals", ["1m", "5m", "1h"]),
            primary_interval=data_cfg.get("primary_interval", "5m"),
            max_klines_per_request=data_cfg.get("max_klines_per_request", 500),
            ws_reconnect_delay=data_cfg.get("ws_reconnect_delay_seconds", 5),
            testnet=exchange_cfg.get("testnet", True),
            rest_base_url=exchange_cfg.get(
                "testnet_rest_base_url" if exchange_cfg.get("testnet") else "rest_base_url",
                "https://testnet.binance.vision",
            ),
            ws_base_url=exchange_cfg.get(
                "testnet_ws_base_url" if exchange_cfg.get("testnet") else "ws_base_url",
                "wss://testnet.binance.vision/ws",
            ),
        )

        # --- Initialize Strategy ---
        strategy_cfg = config.get("strategy", {})
        grid_cfg = strategy_cfg.get("grid_trading", {})
        trend_cfg = strategy_cfg.get("trend_following", {})
        regime_cfg = strategy_cfg.get("regime_detection", {})
        mf_cfg = strategy_cfg.get("multi_factor", {})

        self.grid_strategy = GridStrategy(
            grid_type=grid_cfg.get("type", "geometric"),
            upper_bound_pct=grid_cfg.get("upper_bound_pct", 5.0),
            lower_bound_pct=grid_cfg.get("lower_bound_pct", 5.0),
            grid_levels=grid_cfg.get("grid_levels", 10),
            profit_per_grid_pct=grid_cfg.get("profit_per_grid_pct", 0.5),
            total_capital=config.get("trading", {}).get("initial_capital", 10000.0),
            rebalance_threshold_pct=grid_cfg.get("rebalance_threshold_pct", 1.0),
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
        self.risk_guard = RiskGuard(
            config=risk_cfg,
            equity_provider=self._get_equity,
            positions_provider=self._get_positions,
            trade_history_provider=self._get_trade_history,
        )

        # --- Initialize Execution ---
        # Load API credentials: env vars take priority over config file
        use_testnet = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
        api_key = os.getenv(
            "BINANCE_TESTNET_API_KEY" if use_testnet else "BINANCE_API_KEY", ""
        ) or api_cfg.get("testnet_api_key" if use_testnet else "api_key", "")
        api_secret = os.getenv(
            "BINANCE_TESTNET_API_SECRET" if use_testnet else "BINANCE_API_SECRET", ""
        ) or api_cfg.get("testnet_api_secret" if use_testnet else "api_secret", "")

        if not api_key or api_key.startswith("your_"):
            logger.warning("  API keys not configured! Set them in .env file")
            logger.warning("  cp .env.example .env  →  edit .env with your keys")

        self.exchange_client = ExchangeClient(
            api_key=api_key,
            api_secret=api_secret,
            testnet=use_testnet,
            recv_window=api_cfg.get("recv_window", 5000),
            rate_limit_rps=api_cfg.get("rate_limit_rps", 10.0),
        )

        self.order_manager = OrderManager(
            exchange_client=self.exchange_client,
            ledger_record_order=self._ledger_record_order,
            ledger_update_fill=self._ledger_update_fill,
            ledger_record_trade_open=self._ledger_record_trade_open,
            ledger_record_trade_close=self._ledger_record_trade_close,
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

    def _get_equity(self) -> float:
        """Return current total equity from the ledger."""
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

    def _ledger_update_fill(self, order_id, exchange_order_id, filled_quantity, fill_price, fee=0.0):
        """Update order fill in the ledger."""
        self.ledger.update_order_fill(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            filled_quantity=filled_quantity,
            fill_price=fill_price,
            fee=fee,
        )

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

        # Seed ledger with initial capital if empty
        current_equity = self.ledger.get_total_equity()
        if current_equity <= 0:
            initial_cap = self.config.get("trading", {}).get("initial_capital", 10000.0)
            self.ledger.record_initial_capital(initial_cap)
            logger.info(f"Seeded ledger with initial capital: ${initial_cap:,.2f}")

        # Start exchange client
        await self.exchange_client.start()

        # Start order manager
        await self.order_manager.start()

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

        # Cancel all open orders
        for symbol in self.config.get("trading", {}).get("symbols", []):
            try:
                await self.exchange_client.cancel_all_orders(symbol)
                logger.info(f"Cancelled all orders for {symbol}")
            except Exception:
                pass

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
        symbols = self.config.get("trading", {}).get("symbols", ["BTCUSDT"])
        tick_interval = self._get_tick_interval_seconds()

        while self.running:
            try:
                for symbol in symbols:
                    await self._process_symbol_tick(symbol)

                self._tick_count += 1

                # Periodic tasks
                if self._tick_count % 10 == 0:
                    self._log_status()

                if self._tick_count % 60 == 0:
                    self.ledger.take_snapshot()
                    self.risk_guard.update_peak_equity()

                await asyncio.sleep(tick_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in main loop: {e}")
                await asyncio.sleep(tick_interval)

    async def _process_symbol_tick(self, symbol: str):
        """
        Process one tick for one symbol: data → indicators → regime → signal.
        """
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

        # 4. Generate strategy signal based on regime
        signal: Optional[StrategySignal] = None

        if regime_result.is_trending and regime_result.confidence > 0.6:
            # Trend mode
            if not self.state_machine.state == SystemState.ACTIVE_TREND:
                if self.state_machine.activate_trend():
                    logger.info(f" Switching to TREND mode ({symbol}, score={regime_result.score:.3f})")

            trend_state = self._trend_states.get(symbol, TrendState.flat(symbol))
            signal = self.trend_strategy.evaluate(
                symbol=symbol,
                current_price=indicators.close,
                ema_fast_val=indicators.ema_fast,
                ema_slow_val=indicators.ema_slow,
                macd_hist=indicators.macd_hist,
                atr=indicators.atr,
                current_state=trend_state,
                capital=self.ledger.get_total_equity(),
            )

        elif regime_result.is_ranging and regime_result.confidence > 0.6:
            # Grid mode
            if not self.state_machine.state == SystemState.ACTIVE_GRID:
                if self.state_machine.activate_grid():
                    logger.info(f" Switching to GRID mode ({symbol}, score={regime_result.score:.3f})")

            current_grid = self._grid_configs.get(symbol)
            if current_grid is None:
                # Start a new grid
                ref_price = self.grid_strategy.compute_rebalance_price(ohlcv)
                current_grid = self.grid_strategy.compute_grid(
                    reference_price=ref_price,
                    symbol=symbol,
                    atr=indicators.atr,
                )
                self._grid_configs[symbol] = current_grid
                signal = self.grid_strategy.generate_signal(current_grid)

            elif self.grid_strategy.check_rebalance(indicators.close, current_grid):
                # Grid needs rebalancing
                logger.info(f"Rebalancing grid for {symbol} at {indicators.close}")
                ref_price = self.grid_strategy.compute_rebalance_price(ohlcv)
                new_grid = self.grid_strategy.compute_grid(
                    reference_price=ref_price,
                    symbol=symbol,
                    atr=indicators.atr,
                )
                self._grid_configs[symbol] = new_grid
                signal = self.grid_strategy.generate_signal(new_grid)

        # 5. No signal? Check if we need to stop active strategy
        if signal is None and regime_result.switched:
            prev_mode = "trend" if regime_result.is_ranging else "grid"
            logger.info(f"Regime switch detected — closing {prev_mode} position for {symbol}")
            if prev_mode == "trend":
                ts = self._trend_states.get(symbol)
                if ts and ts.is_active:
                    signal = StrategySignal(
                        action=SignalAction.STOP_TREND,
                        symbol=symbol,
                        score=1.0,
                        metadata={"direction": ts.direction.value, "exit_reason": "regime_switch"},
                    )
            else:
                gc = self._grid_configs.get(symbol)
                if gc:
                    signal = self.grid_strategy.generate_stop_signal(gc, "regime_switch")

        # 6. Route signal through risk → execution
        if signal is not None:
            await self._route_signal(signal)

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

    def _get_tick_interval_seconds(self) -> float:
        """Convert primary interval string to seconds."""
        interval = self.config.get("data", {}).get("primary_interval", "5m")
        unit = interval[-1]
        value = int(interval[:-1])
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return value * multipliers.get(unit, 60)

    def _log_status(self):
        """Log current system status (periodic heartbeat)."""
        sm = self.state_machine.snapshot()
        equity = self.ledger.get_total_equity()
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

    # Graceful shutdown on signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(system.stop()))

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
