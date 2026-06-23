"""
================================================================================
BACKTEST ENGINE — Full-Pipeline Historical Simulation
================================================================================

Simulates the complete trading system on historical data:
    OHLCV → Indicators → Regime Detection → Grid/Trend Strategy
    → Simulated Execution → Performance Metrics

Tests three strategies side-by-side:
    1. GRID-ONLY: Always use grid trading (baseline for ranging markets)
    2. TREND-ONLY: Always use trend following (baseline for trending markets)
    3. REGIME-SWITCH: Dynamically switch based on five-factor scoring

USAGE:
    python backtest.py
    python backtest.py --symbol BTCUSDT --capital 10000
================================================================================
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.indicators import compute_all, IndicatorBundle
from strategy.signal import SignalAction, StrategySignal
from strategy.grid_strategy import GridConfig, GridLevel, GridStatus, GridStrategy, LevelSide, LevelStatus
from strategy.trend_strategy import TrendDirection, TrendState, TrendStrategy
from strategy.regime_detector import MarketRegime, RegimeDetector, RegimeResult


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class BacktestConfig:
    """Backtest parameters."""
    symbol: str = "BTCUSDT"
    interval: str = "5m"
    initial_capital: float = 10000.0
    fee_pct: float = 0.1  # 0.1% per trade
    slippage_pct: float = 0.05  # 0.05% slippage
    max_position_pct: float = 25.0  # max single position as % of equity
    warmup_candles: int = 200
    confidence_threshold_trend: float = 0.92  # min confidence to switch to trend
    min_strategy_bars: int = 96  # min bars (~8h) before allowing strategy switch  # candles needed for indicator warmup
    lookback_candles: int = 500  # max candles for indicator calculation (O(n) optimization)
    grid_upper_pct: float = 5.0
    grid_lower_pct: float = 5.0
    grid_levels: int = 10
    grid_profit_pct: float = 0.5  # 0.5% profit per grid level

    @property
    def fee_decimal(self) -> float:
        return self.fee_pct / 100.0

    @property
    def slippage_decimal(self) -> float:
        return self.slippage_pct / 100.0


# ============================================================================
# Trade Records
# ============================================================================

@dataclass
class Trade:
    """A single completed trade."""
    entry_time: str
    exit_time: str
    symbol: str
    side: str
    strategy: str  # "grid", "trend", "regime_switch"
    entry_price: float
    exit_price: float
    quantity: float
    pnl: float
    pnl_pct: float
    fee: float
    exit_reason: str = ""


@dataclass
class EquityPoint:
    """One point on the equity curve."""
    timestamp: str
    equity: float
    position_value: float
    cash: float
    regime: str = ""
    regime_score: float = 0.0


# ============================================================================
# Performance Metrics
# ============================================================================

@dataclass
class PerformanceReport:
    """Complete performance report for one strategy run."""
    symbol: str
    strategy: str
    initial_capital: float
    final_equity: float
    total_return_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate_pct: float
    total_pnl: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    max_drawdown_pct: float
    max_drawdown_duration_hours: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    avg_trades_per_day: float
    avg_holding_hours: float
    regime_switches: int = 0
    pct_time_grid: float = 0.0
    pct_time_trend: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "strategy": self.strategy,
            "initial_capital": self.initial_capital,
            "final_equity": round(self.final_equity, 2),
            "total_return_pct": round(self.total_return_pct, 2),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate_pct": round(self.win_rate_pct, 2),
            "total_pnl": round(self.total_pnl, 2),
            "avg_win": round(self.avg_win, 2),
            "avg_loss": round(self.avg_loss, 2),
            "profit_factor": round(self.profit_factor, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "sortino_ratio": round(self.sortino_ratio, 2),
            "calmar_ratio": round(self.calmar_ratio, 2),
            "avg_trades_per_day": round(self.avg_trades_per_day, 1),
            "regime_switches": self.regime_switches,
            "pct_time_grid": round(self.pct_time_grid, 1),
            "pct_time_trend": round(self.pct_time_trend, 1),
        }

    def summary(self) -> str:
        lines = [
            f"  Return: {self.total_return_pct:+.2f}%  |  Sharpe: {self.sharpe_ratio:.2f}",
            f"  Trades: {self.total_trades}  |  Win Rate: {self.win_rate_pct:.1f}%",
            f"  Profit Factor: {self.profit_factor:.2f}  |  Max DD: {self.max_drawdown_pct:.1f}%",
            f"  Calmar: {self.calmar_ratio:.2f}  |  Sortino: {self.sortino_ratio:.2f}",
        ]
        if self.regime_switches > 0:
            lines.append(
                f"  Regime: {self.regime_switches} switches, "
                f"Grid {self.pct_time_grid:.0f}% / Trend {self.pct_time_trend:.0f}%"
            )
        return "\n".join(lines)


# ============================================================================
# Backtest Engine
# ============================================================================

class BacktestEngine:
    """
    Full-pipeline backtest simulator.

    Walks through historical candles one-by-one, running the complete
    strategy pipeline and tracking performance.
    """

    def __init__(self, config: BacktestConfig):
        self.cfg = config
        self.cash = config.initial_capital
        self.position_qty = 0.0          # absolute quantity (always positive)
        self.position_entry_price = 0.0   # average entry price
        self.position_direction = "FLAT"  # "LONG", "SHORT", or "FLAT"
        self.short_margin_locked = 0.0    # collateral locked for short position
        self.trades: List[Trade] = []
        self.equity_curve: List[EquityPoint] = []
        self._trade_id_counter = 0

        # Strategy engines
        self.grid = GridStrategy(
            grid_type="geometric",
            upper_bound_pct=config.grid_upper_pct,
            lower_bound_pct=config.grid_lower_pct,
            grid_levels=config.grid_levels,
            profit_per_grid_pct=config.grid_profit_pct,
            total_capital=config.initial_capital,
        )
        self.trend = TrendStrategy(
            ema_fast=12, ema_slow=26, macd_signal_period=9,
            atr_period=14, atr_multiplier=2.0,
            trailing_stop_atr_multiplier=3.0,
            risk_per_trade_pct=1.0,
        )
        self.regime_detector = RegimeDetector(
            adx_threshold=25, volatility_window=20,
            volatility_percentile_low=25, volatility_percentile_high=75,
            lookback_periods=100,
        )

        # Per-run state
        self._current_regime = MarketRegime.RANGING
        self._trend_state = TrendState.flat(config.symbol)
        self._last_switch_bar = 0
        self._grid_config: Optional[GridConfig] = None
        self._active_grid_levels: Dict[int, Dict] = {}  # level_index → {side, price, qty, tp}
        self._filled_grid_levels: Dict[int, Dict] = {}  # filled buy/sell waiting for TP
        self._regime_history: List[Tuple[str, float]] = []

    # ------------------------------------------------------------------
    # Main Entry Point
    # ------------------------------------------------------------------

    def run(self, ohlcv: List[Dict], strategy_mode: str = "regime_switch") -> PerformanceReport:
        """
        Run backtest over historical OHLCV data.

        Args:
            ohlcv: List of OHLCV dicts (oldest first).
            strategy_mode: "grid_only", "trend_only", or "regime_switch".

        Returns:
            PerformanceReport with full statistics.
        """
        self._reset_state()

        L = self.cfg.lookback_candles

        for i in range(self.cfg.warmup_candles, len(ohlcv)):
            start = max(0, i - L)
            window = ohlcv[start:i + 1]
            current_bar = ohlcv[i]

            # Compute indicators
            indicators = compute_all(self.cfg.symbol, window)

            # Determine regime
            if strategy_mode == "grid_only":
                regime_result = self._force_grid_regime(indicators)
            elif strategy_mode == "trend_only":
                regime_result = self._force_trend_regime(indicators)
            else:
                raw_regime = self.regime_detector.detect(
                    ohlcv=window,
                    current_regime=self._current_regime,
                    adx=indicators.adx, plus_di=indicators.plus_di,
                    minus_di=indicators.minus_di,
                    ema_fast=indicators.ema_fast, ema_slow=indicators.ema_slow,
                    macd_hist=indicators.macd_hist, atr=indicators.atr,
                    volume=indicators.volume,
                )
                raw_score = raw_regime.score
                raw_confidence = raw_regime.confidence

                # ---- Strategy lock & confidence filter ----
                bars_since_switch = i - self._last_switch_bar
                locked = bars_since_switch < self.cfg.min_strategy_bars

                switched_this_tick = False
                if raw_regime.switched and not locked:
                    if raw_regime.regime == MarketRegime.TRENDING:
                        if raw_regime.confidence >= self.cfg.confidence_threshold_trend:
                            self._current_regime = raw_regime.regime
                            self._last_switch_bar = i
                            switched_this_tick = True
                            self._regime_history.append((current_bar["timestamp"], raw_regime.score))
                            # Grid persists — trend trades overlay on top, never cancel grid
                    else:
                        self._current_regime = raw_regime.regime
                        self._last_switch_bar = i
                        switched_this_tick = True
                        self._regime_history.append((current_bar["timestamp"], raw_regime.score))

                regime_result = RegimeResult(
                    regime=self._current_regime,
                    score=raw_score,
                    factor_scores=None,
                    confidence=raw_confidence,
                    switched=switched_this_tick,
                )

            # Simulate fills on grid levels
            self._check_grid_fills(current_bar)

            # Generate and execute signals
            self._process_tick(current_bar, indicators, regime_result, strategy_mode)

            # Mark-to-market equity
            self._record_equity(current_bar, indicators, regime_result)

        # Close any remaining position at end of backtest
        if self.position_qty > 0:
            last_price = ohlcv[-1]["close"]
            self._close_position(last_price, ohlcv[-1]["timestamp"], "end_of_backtest",
                               strategy_mode)
        # Unlock any remaining margin
        if self.short_margin_locked > 0:
            self.cash += self.short_margin_locked
            self.short_margin_locked = 0.0

        return self._calculate_performance(strategy_mode)

    def _reset_state(self):
        """Reset all state for a new run."""
        self.cash = self.cfg.initial_capital
        self.position_qty = 0.0
        self.position_entry_price = 0.0
        self.position_direction = "FLAT"
        self.short_margin_locked = 0.0
        self.trades = []
        self.equity_curve = []
        self._current_regime = MarketRegime.RANGING
        self._trend_state = TrendState.flat(self.cfg.symbol)
        self._last_switch_bar = 0
        self._grid_config = None
        self._active_grid_levels = {}
        self._filled_grid_levels = {}
        self._regime_history = []

    # ------------------------------------------------------------------
    # Regime Overrides
    # ------------------------------------------------------------------

    def _force_grid_regime(self, indicators: IndicatorBundle) -> RegimeResult:
        return RegimeResult(
            regime=MarketRegime.RANGING, score=0.0,
            factor_scores=None, confidence=1.0, switched=False,
        )

    def _force_trend_regime(self, indicators: IndicatorBundle) -> RegimeResult:
        return RegimeResult(
            regime=MarketRegime.TRENDING, score=1.0,
            factor_scores=None, confidence=1.0, switched=False,
        )

    # ------------------------------------------------------------------
    # Per-Tick Processing
    # ------------------------------------------------------------------

    def _process_tick(
        self, bar: Dict, indicators: IndicatorBundle,
        regime: RegimeResult, strategy_mode: str,
    ):
        """Run one tick of the strategy."""
        price = bar["close"]

        if strategy_mode == "grid_only":
            self._run_grid_strategy(bar, indicators, strategy_mode)
        elif strategy_mode == "trend_only":
            self._run_trend_strategy(bar, indicators, strategy_mode)
        else:
            # DUAL ENGINE: grid always runs. Trend only fires on strong ENTRY/EXIT.
            self._run_grid_strategy(bar, indicators, strategy_mode)

            if regime.is_trending and regime.confidence >= self.cfg.confidence_threshold_trend:
                trend_signal = self._evaluate_trend(bar, indicators)
                if trend_signal is not None:
                    if trend_signal.action == SignalAction.START_TREND and self.position_qty == 0:
                        self._execute_trend_trade(trend_signal, bar, indicators)
                    elif trend_signal.action == SignalAction.STOP_TREND and self.position_qty > 0:
                        self._execute_trend_trade(trend_signal, bar, indicators)

    def _evaluate_trend(self, bar: Dict, indicators: IndicatorBundle):
        """Evaluate trend strategy without modifying state. Returns signal or None."""
        return self.trend.evaluate(
            symbol=self.cfg.symbol,
            current_price=bar["close"],
            ema_fast_val=indicators.ema_fast,
            ema_slow_val=indicators.ema_slow,
            macd_hist=indicators.macd_hist,
            atr=indicators.atr,
            current_state=self._trend_state,
            capital=self._get_equity(),
        )

    def _execute_trend_trade(self, signal, bar: Dict, indicators: IndicatorBundle):
        """Execute a trend strategy signal (ENTRY or EXIT only)."""
        price = bar["close"]
        if signal.action == SignalAction.START_TREND:
            direction = TrendDirection(signal.metadata["direction"])
            target_qty = signal.metadata.get("position_size", 0)
            if self.position_qty > 0:
                needs_reverse = (
                    (direction == TrendDirection.LONG and self.position_direction == "SHORT") or
                    (direction == TrendDirection.SHORT and self.position_direction == "LONG")
                )
                if needs_reverse:
                    self._close_position(price, bar["timestamp"], "trend_reversal", "regime_switch")
                    new_side = "BUY" if direction == TrendDirection.LONG else "SELL"
                    self._open_position(price, bar["timestamp"], new_side, target_qty, "regime_switch")
            else:
                side = "BUY" if direction == TrendDirection.LONG else "SELL"
                self._open_position(price, bar["timestamp"], side, target_qty, "regime_switch")
            self._trend_state = self.trend.update_state(
                self._trend_state, signal, price, bar["timestamp"])
        elif signal.action == SignalAction.STOP_TREND:
            if self.position_qty > 0:
                self._close_position(price, bar["timestamp"], "trend_exit", "regime_switch")
                self._trend_state = TrendState.flat(self.cfg.symbol)

    def _run_grid_strategy(self, bar: Dict, indicators: IndicatorBundle, mode: str):
        """Execute grid strategy logic."""
        price = bar["close"]

        # Initialize grid if needed
        if self._grid_config is None:
            ref_price = self.grid.compute_rebalance_price(
                [bar], window=1,
            ) if price > 0 else price
            self._grid_config = self.grid.compute_grid(
                reference_price=ref_price,
                symbol=self.cfg.symbol,
                atr=indicators.atr,
            )
            self._arm_grid_levels()

        # Check rebalance
        elif self.grid.check_rebalance(price, self._grid_config):
            # Cancel unfilled levels, keep filled ones tracking
            self._cancel_unfilled_levels()
            ref_price = price
            self._grid_config = self.grid.compute_grid(
                reference_price=ref_price,
                symbol=self.cfg.symbol,
                atr=indicators.atr,
            )
            self._arm_grid_levels()

    def _run_trend_strategy(self, bar: Dict, indicators: IndicatorBundle, mode: str):
        """Execute trend strategy logic."""
        price = bar["close"]

        # If we have an active grid, close it
        if self._grid_config is not None:
            self._cancel_unfilled_levels()
            self._grid_config = None

        signal = self.trend.evaluate(
            symbol=self.cfg.symbol,
            current_price=price,
            ema_fast_val=indicators.ema_fast,
            ema_slow_val=indicators.ema_slow,
            macd_hist=indicators.macd_hist,
            atr=indicators.atr,
            current_state=self._trend_state,
            capital=self._get_equity(),
        )

        if signal is None:
            return

        if signal.action == SignalAction.START_TREND:
            direction = TrendDirection(signal.metadata["direction"])
            target_qty = signal.metadata.get("position_size", 0)

            if self.position_qty > 0:
                # Already in a position — act if reversing (LONG→SHORT or SHORT→LONG)
                needs_reverse = (
                    (direction == TrendDirection.LONG and self.position_direction == "SHORT") or
                    (direction == TrendDirection.SHORT and self.position_direction == "LONG")
                )
                if needs_reverse:
                    self._close_position(price, bar["timestamp"], "trend_reversal", mode)
                    new_side = "BUY" if direction == TrendDirection.LONG else "SELL"
                    self._open_position(price, bar["timestamp"], new_side, target_qty, mode)
                # Same direction → ignore (already in position)
            else:
                side = "BUY" if direction == TrendDirection.LONG else "SELL"
                self._open_position(price, bar["timestamp"], side, target_qty, mode)

            self._trend_state = self.trend.update_state(
                self._trend_state, signal, price, bar["timestamp"],
            )

        elif signal.action == SignalAction.STOP_TREND:
            if self.position_qty > 0:
                self._close_position(price, bar["timestamp"],
                                    signal.metadata.get("exit_reason", "signal"), mode)
            self._trend_state = TrendState.flat(self.cfg.symbol)

    # ------------------------------------------------------------------
    # Grid Fill Simulation
    # ------------------------------------------------------------------

    def _arm_grid_levels(self):
        """Place all grid limit orders (simulated)."""
        if self._grid_config is None:
            return
        self._active_grid_levels = {}
        for lvl in self._grid_config.levels:
            self._active_grid_levels[lvl.level_index] = {
                "side": lvl.side.value,
                "price": lvl.price,
                "quantity": lvl.quantity,
                "tp_price": lvl.take_profit_price,
            }

    def _cancel_unfilled_levels(self):
        """Cancel all unfilled grid orders."""
        self._active_grid_levels = {}

    def _check_grid_fills(self, bar: Dict):
        """Check if any active grid levels were hit this candle."""
        # Grid is long-only — don't interfere with short positions
        if self.position_direction == "SHORT":
            return
        high, low = bar["high"], bar["low"]

        filled_indices = []
        for idx, level in list(self._active_grid_levels.items()):
            if level["side"] == "BUY" and low <= level["price"] <= high:
                # Buy order filled
                fill_price = level["price"]
                cost = fill_price * level["quantity"]
                fee = cost * self.cfg.fee_decimal
                if self.cash >= cost + fee:
                    self.cash -= cost + fee
                    self.position_qty += level["quantity"]
                    # Update avg entry price
                    if self.position_qty > 0:
                        old_value = (self.position_qty - level["quantity"]) * self.position_entry_price
                        new_value = old_value + cost
                        self.position_entry_price = new_value / self.position_qty
                    self._filled_grid_levels[idx] = {
                        "side": "BUY",
                        "entry_price": fill_price,
                        "quantity": level["quantity"],
                        "tp_price": level["tp_price"],
                    }
                    filled_indices.append(idx)

            elif level["side"] == "SELL" and low <= level["price"] <= high:
                # Sell order filled (using existing position)
                if self.position_qty >= level["quantity"]:
                    fill_price = level["price"]
                    revenue = fill_price * level["quantity"]
                    fee = revenue * self.cfg.fee_decimal
                    self.cash += revenue - fee
                    self.position_qty -= level["quantity"]
                    # Record trade
                    pnl = (fill_price - self.position_entry_price) * level["quantity"] - fee
                    pnl_pct = (fill_price / self.position_entry_price - 1) * 100
                    self.trades.append(Trade(
                        entry_time=bar["timestamp"], exit_time=bar["timestamp"],
                        symbol=self.cfg.symbol, side="SELL", strategy=self._current_mode_name,
                        entry_price=self.position_entry_price, exit_price=fill_price,
                        quantity=level["quantity"], pnl=pnl, pnl_pct=pnl_pct, fee=fee,
                        exit_reason="grid_tp",
                    ))
                    if self.position_qty < 0.001:
                        self.position_qty = 0
                        self.position_entry_price = 0
                    filled_indices.append(idx)

        for idx in filled_indices:
            del self._active_grid_levels[idx]

        # Check TP on filled levels
        filled_tp_indices = []
        for idx, fill in list(self._filled_grid_levels.items()):
            if fill["side"] == "BUY" and high >= fill["tp_price"]:
                # TP hit — sell
                tp_price = fill["tp_price"]
                revenue = tp_price * fill["quantity"]
                fee = revenue * self.cfg.fee_decimal
                pnl = (tp_price - fill["entry_price"]) * fill["quantity"] - fee
                pnl_pct = (tp_price / fill["entry_price"] - 1) * 100
                self.cash += revenue - fee
                self.position_qty -= fill["quantity"]
                if self.position_qty < 0.001:
                    self.position_qty = 0
                    self.position_entry_price = 0
                self.trades.append(Trade(
                    entry_time=bar["timestamp"], exit_time=bar["timestamp"],
                    symbol=self.cfg.symbol, side="SELL", strategy=self._current_mode_name,
                    entry_price=fill["entry_price"], exit_price=tp_price,
                    quantity=fill["quantity"], pnl=pnl, pnl_pct=pnl_pct, fee=fee,
                    exit_reason="grid_tp",
                ))
                filled_tp_indices.append(idx)

        for idx in filled_tp_indices:
            del self._filled_grid_levels[idx]

    # ------------------------------------------------------------------
    # Position Management
    # ------------------------------------------------------------------

    def _open_position(self, price: float, timestamp: str, side: str, quantity: float, mode: str):
        """
        Open a new position. Handles both LONG and SHORT.

        LONG:  Buy asset with cash. Cash decreases, position is positive.
               P&L = exit_price - entry_price.

        SHORT: Sell borrowed asset, receive cash, lock margin (110% of notional).
               Cash increases on open, but margin is locked.
               P&L = entry_price - exit_price.
        """
        if quantity <= 0 or price <= 0:
            return

        # Cap position size: max_position_pct% of current equity
        equity = self._get_equity()
        max_notional = equity * (self.cfg.max_position_pct / 100.0)
        notional = price * quantity
        if notional > max_notional:
            quantity = max_notional / price
            notional = max_notional
        fee = notional * self.cfg.fee_decimal
        slippage = notional * self.cfg.slippage_decimal

        if side.upper() == "BUY":
            # ---- LONG: buy with cash ----
            total_cost = notional + fee + slippage
            if total_cost > self.cash:
                quantity = self.cash / (price * (1 + self.cfg.fee_decimal + self.cfg.slippage_decimal))
                notional = price * quantity
                fee = notional * self.cfg.fee_decimal
                slippage = notional * self.cfg.slippage_decimal
                total_cost = notional + fee + slippage

            self.cash -= total_cost
            self.position_qty = quantity
            self.position_direction = "LONG"
            self.position_entry_price = price
            self.short_margin_locked = 0.0

        else:
            # ---- SHORT: sell borrowed asset, lock margin ----
            # Margin requirement: 110% of notional (conservative)
            margin_required = notional * 1.10
            available = self.cash
            if margin_required > available:
                # Scale down
                quantity = available / (price * 1.10 * (1 + self.cfg.fee_decimal + self.cfg.slippage_decimal))
                notional = price * quantity
                fee = notional * self.cfg.fee_decimal
                slippage = notional * self.cfg.slippage_decimal

            # Receive cash from sale, lock margin
            self.cash += notional  # receive sale proceeds
            self.cash -= fee + slippage  # pay fees
            self.short_margin_locked = notional * 1.10  # lock margin
            self.position_qty = quantity
            self.position_direction = "SHORT"
            self.position_entry_price = price

    def _close_position(self, price: float, timestamp: str, reason: str, mode: str):
        """
        Close current position. Handles both LONG and SHORT.

        LONG close:  Sell asset → receive cash. P&L = exit - entry.
        SHORT close: Buy back asset → return it, unlock margin. P&L = entry - exit.
        """
        if self.position_qty <= 0:
            return
        notional = price * self.position_qty
        fee = notional * self.cfg.fee_decimal
        slippage = notional * self.cfg.slippage_decimal

        if self.position_direction == "LONG":
            # Sell the asset
            net_revenue = notional - fee - slippage
            pnl = net_revenue - (self.position_entry_price * self.position_qty)
            pnl_pct = (price / self.position_entry_price - 1) * 100
            trade_side = "SELL"

            self.cash += net_revenue

        else:
            # SHORT: Buy back to cover
            buyback_cost = notional + fee + slippage
            pnl = (self.position_entry_price * self.position_qty) - notional - fee - slippage
            pnl_pct = (self.position_entry_price / price - 1) * 100 if price > 0 else 0
            trade_side = "BUY"

            self.cash -= buyback_cost
            self.cash += self.short_margin_locked  # Unlock margin

        self.trades.append(Trade(
            entry_time="", exit_time=timestamp,
            symbol=self.cfg.symbol,
            side=trade_side,
            strategy=mode,
            entry_price=self.position_entry_price,
            exit_price=price,
            quantity=self.position_qty,
            pnl=pnl,
            pnl_pct=pnl_pct,
            fee=fee,
            exit_reason=reason,
        ))

        self.position_qty = 0.0
        self.position_entry_price = 0.0
        self.position_direction = "FLAT"
        self.short_margin_locked = 0.0

    # ------------------------------------------------------------------
    # Equity Tracking
    # ------------------------------------------------------------------

    @property
    def _current_mode_name(self) -> str:
        """Helper for trade tagging."""
        return "backtest"

    def _get_equity(self) -> float:
        """
        Current total equity.
        LONG: equity = cash + position_value
        SHORT: equity = cash + short_margin_locked - position_liability
               (unrealized P&L is built into this)
        """
        pos_value = self.position_qty * self.position_entry_price
        if self.position_direction == "SHORT":
            return self.cash + self.short_margin_locked - pos_value
        else:
            return self.cash + pos_value

    def _record_equity(self, bar: Dict, indicators: IndicatorBundle, regime: RegimeResult):
        """Record equity point with correct short position valuation."""
        pos_value = self.position_qty * bar["close"]
        if self.position_direction == "SHORT":
            # For shorts: equity rises as price drops
            equity = self.cash + self.short_margin_locked - pos_value
        else:
            equity = self.cash + pos_value

        self.equity_curve.append(EquityPoint(
            timestamp=bar["timestamp"],
            equity=equity,
            position_value=pos_value,
            cash=self.cash,
            regime=regime.regime.value,
            regime_score=regime.score,
        ))

    # ------------------------------------------------------------------
    # Performance Calculation
    # ------------------------------------------------------------------

    def _calculate_performance(self, strategy_name: str) -> PerformanceReport:
        """Compute all performance metrics from trade history and equity curve."""
        initial = self.cfg.initial_capital
        final = self.equity_curve[-1].equity if self.equity_curve else initial

        # Basic stats
        total_return = (final / initial - 1) * 100
        wins = [t for t in self.trades if t.pnl > 0]
        losses = [t for t in self.trades if t.pnl <= 0]
        total_pnl = sum(t.pnl for t in self.trades)
        win_rate = len(wins) / len(self.trades) * 100 if self.trades else 0
        avg_win = sum(t.pnl for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t.pnl for t in losses) / len(losses) if losses else 0
        gross_profit = sum(t.pnl for t in wins)
        gross_loss = abs(sum(t.pnl for t in losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Sharpe ratio (from equity curve returns)
        returns = []
        for i in range(1, len(self.equity_curve)):
            prev_eq = self.equity_curve[i - 1].equity
            if prev_eq > 0:
                returns.append((self.equity_curve[i].equity / prev_eq) - 1)
        if returns and len(returns) > 1:
            mean_ret = sum(returns) / len(returns)
            std_ret = (sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)) ** 0.5
            # Annualize: 5-min candles → 12*24*365 = 105120 periods/year
            sharpe = (mean_ret / std_ret * (105120 ** 0.5)) if std_ret > 0 else 0.0
            # Sortino: only downside deviation
            downside = [r for r in returns if r < 0]
            if downside and len(downside) > 1:
                mean_down = sum(downside) / len(downside)
                std_down = (sum((r - mean_down) ** 2 for r in downside) / (len(downside) - 1)) ** 0.5
                sortino = (mean_ret / std_down * (105120 ** 0.5)) if std_down > 0 else 0.0
            else:
                sortino = 0.0
        else:
            sharpe, sortino = 0.0, 0.0

        # Max drawdown
        peak = initial
        max_dd = 0.0
        dd_start = None
        max_dd_duration = 0.0
        for pt in self.equity_curve:
            if pt.equity > peak:
                peak = pt.equity
                dd_start = None
            dd = (peak - pt.equity) / peak * 100
            if dd > max_dd:
                max_dd = dd
            if dd > 0 and dd_start is None:
                dd_start = pt.timestamp

        # Calmar ratio
        calmar = abs(total_return) / max_dd if max_dd > 0 else 0.0

        # Trade frequency & holding time
        if self.trades:
            first_ts = datetime.fromisoformat(self.equity_curve[0].timestamp)
            last_ts = datetime.fromisoformat(self.equity_curve[-1].timestamp)
            days = (last_ts - first_ts).total_seconds() / 86400
            avg_trades_per_day = len(self.trades) / days if days > 0 else 0
        else:
            avg_trades_per_day = 0

        # Regime stats
        grid_ticks = sum(1 for pt in self.equity_curve if pt.regime == "RANGING")
        trend_ticks = sum(1 for pt in self.equity_curve if pt.regime == "TRENDING")
        total_ticks = len(self.equity_curve)
        pct_grid = grid_ticks / total_ticks * 100 if total_ticks > 0 else 0
        pct_trend = trend_ticks / total_ticks * 100 if total_ticks > 0 else 0

        return PerformanceReport(
            symbol=self.cfg.symbol,
            strategy=strategy_name,
            initial_capital=initial,
            final_equity=final,
            total_return_pct=total_return,
            total_trades=len(self.trades),
            winning_trades=len(wins),
            losing_trades=len(losses),
            win_rate_pct=win_rate,
            total_pnl=total_pnl,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            max_drawdown_pct=max_dd,
            max_drawdown_duration_hours=max_dd_duration,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            avg_trades_per_day=avg_trades_per_day,
            avg_holding_hours=0,
            regime_switches=len([1 for i in range(1, len(self._regime_history))
                                 if self._regime_history[i][0] != self._regime_history[i-1][0]]),
            pct_time_grid=pct_grid,
            pct_time_trend=pct_trend,
        )


# ============================================================================
# Main Runner
# ============================================================================

def load_ohlcv(filepath: str) -> List[Dict]:
    """Load OHLCV data from JSON file."""
    with open(filepath, "r") as f:
        data = json.load(f)
    return data


def run_comparison(symbols: List[str], capital: float = 10000.0, data_file: str = "data/sampled_10k.json"):
    """Run full comparison across symbols and strategies using sampled data."""
    strategies = ["grid_only", "trend_only", "regime_switch"]
    all_reports: Dict[str, Dict[str, PerformanceReport]] = {}

    # Load combined sampled data
    if os.path.exists(data_file):
        with open(data_file, "r") as f:
            all_ohlcv = json.load(f)
        print(f"Using sampled data: {data_file} "
              f"({sum(len(v) for v in all_ohlcv.values())} total candles)")
    else:
        # Fallback: load individual files
        all_ohlcv = {}
        for symbol in symbols:
            fp = f"data/{symbol}_5m.json"
            if os.path.exists(fp):
                all_ohlcv[symbol] = load_ohlcv(fp)

    for symbol in symbols:
        if symbol not in all_ohlcv:
            print(f"  Data not found for {symbol}, skipping")
            continue

        ohlcv = all_ohlcv[symbol]
        print(f"  Loaded {len(ohlcv)} candles")
        print(f"  Range: {ohlcv[0]['timestamp'][:10]} → {ohlcv[-1]['timestamp'][:10]}")
        print(f"  Price: ${ohlcv[0]['close']:.2f} → ${ohlcv[-1]['close']:.2f}")
        pct_change = (ohlcv[-1]['close'] / ohlcv[0]['close'] - 1) * 100
        print(f"  B&H: {pct_change:+.2f}%")

        symbol_reports = {}
        for strategy in strategies:
            cfg = BacktestConfig(symbol=symbol, initial_capital=capital)
            engine = BacktestEngine(cfg)
            report = engine.run(ohlcv, strategy_mode=strategy)
            symbol_reports[strategy] = report
            strategy_label = {"grid_only": "GRID ONLY", "trend_only": "TREND ONLY",
                            "regime_switch": "REGIME SWITCH"}[strategy]
            print(f"\n  [{strategy_label}]")
            print(report.summary())

        all_reports[symbol] = symbol_reports

    # =========================================================================
    # Final Comparison Table
    # =========================================================================
    print(f"\n\n{'='*90}")
    print("  FINAL COMPARISON")
    print(f"{'='*90}")

    for metric_name, fmt in [
        ("total_return_pct", "Return %"),
        ("sharpe_ratio", "Sharpe"),
        ("max_drawdown_pct", "Max DD %"),
        ("win_rate_pct", "Win Rate %"),
        ("profit_factor", "Profit Factor"),
        ("calmar_ratio", "Calmar"),
        ("total_trades", "Trades"),
    ]:
        print(f"\n  --- {fmt} ---")
        header = f"  {'Symbol':<12}"
        for s in strategies:
            header += f"  {s.replace('_',' ').title():>16}"
        print(header)
        print("  " + "-" * 60)
        for symbol in symbols:
            if symbol not in all_reports:
                continue
            row = f"  {symbol:<12}"
            for strategy in strategies:
                val = getattr(all_reports[symbol][strategy], metric_name)
                if isinstance(val, float):
                    row += f"  {val:>16.2f}"
                else:
                    row += f"  {str(val):>16}"
            print(row)

    # Winner determination
    print(f"\n  --- WINNER ---")
    print(f"  {'Symbol':<12}  {'Best Strategy':<20}  {'Return':>10}  {'Sharpe':>8}")
    print("  " + "-" * 55)
    for symbol in symbols:
        if symbol not in all_reports:
            continue
        best = max(strategies, key=lambda s: all_reports[symbol][s].sharpe_ratio)
        r = all_reports[symbol][best]
        print(f"  {symbol:<12}  {best.replace('_',' ').title():<20}  "
              f"{r.total_return_pct:>+9.2f}%  {r.sharpe_ratio:>8.2f}")

    return all_reports


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenQuant Backtest Engine")
    parser.add_argument("--symbol", type=str, default=None,
                       help="Single symbol to backtest (default: all)")
    parser.add_argument("--capital", type=float, default=10000.0,
                       help="Initial capital in USDT")
    args = parser.parse_args()

    symbols = [args.symbol] if args.symbol else ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    run_comparison(symbols, capital=args.capital)
