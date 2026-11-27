"""
================================================================================
TREND FOLLOWING STRATEGY — EMA Crossover + MACD + ATR-Based Risk Management
================================================================================

Generates directional trade signals when the regime detector classifies the
market as "trending" (regime_score > 0.7).

ENTRY LOGIC:
    LONG:  EMA-12 crosses ABOVE EMA-26 AND MACD histogram > 0
    SHORT: EMA-12 crosses BELOW EMA-26 AND MACD histogram < 0

EXIT LOGIC:
    - Stop-loss: entry_price ± (ATR × atr_multiplier)
    - Trailing stop: follows price at distance (ATR × trailing_multiplier)
    - Signal reversal: opposite entry conditions trigger an exit + reverse

POSITION SIZING:
    position_size = capital × risk_per_trade_pct / (stop_distance_in_pct × price)
    This is a volatility-adjusted Kelly-style sizing — we risk a fixed % of
    capital per trade, scaled by how far away the stop is.

DECOUPLING NOTE:
    This module NEVER imports from execution/ or core/. It produces
    StrategySignal objects consumed by main.py.
================================================================================
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from strategy.signal import SignalAction, SignalBuilder, StrategySignal


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TrendDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class TrendSignalType(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    REVERSE = "REVERSE"  # Exit current + enter opposite
    ADJUST_STOP = "ADJUST_STOP"


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class TrendState:
    """
    Current state of the trend-following strategy for one symbol.

    Tracks the active position (if any) and the parameters that define
    when to enter, exit, and adjust stops.
    """

    symbol: str
    direction: TrendDirection = TrendDirection.FLAT
    entry_price: float = 0.0
    position_size: float = 0.0
    stop_loss_price: float = 0.0
    trailing_stop_price: float = 0.0
    entry_time: Optional[str] = None
    last_signal: Optional[str] = None

    # Cached indicator values at entry (for audit trail)
    entry_ema_fast: float = 0.0
    entry_ema_slow: float = 0.0
    entry_macd_hist: float = 0.0
    entry_atr: float = 0.0
    highest_price: float = 0.0  # Peak favorable price since entry (LONG Chandelier stop)
    lowest_price: float = 0.0   # Trough favorable price since entry (SHORT Chandelier stop)

    @property
    def is_active(self) -> bool:
        return self.direction != TrendDirection.FLAT

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "direction": self.direction.value,
            "entry_price": self.entry_price,
            "position_size": self.position_size,
            "stop_loss_price": self.stop_loss_price,
            "trailing_stop_price": self.trailing_stop_price,
            "entry_time": self.entry_time,
            "entry_ema_fast": self.entry_ema_fast,
            "entry_ema_slow": self.entry_ema_slow,
            "entry_macd_hist": self.entry_macd_hist,
            "entry_atr": self.entry_atr,
            "highest_price": self.highest_price,
            "lowest_price": self.lowest_price,
        }

    @classmethod
    def flat(cls, symbol: str) -> TrendState:
        """Create a flat (no position) state for a symbol."""
        return cls(symbol=symbol, direction=TrendDirection.FLAT)


# ---------------------------------------------------------------------------
# TrendStrategy — The Trend Engine
# ---------------------------------------------------------------------------

class TrendStrategy:
    """
    Compute trend-following entry/exit signals with Chandelier trailing stops
    and optional ADX / MACD / macro-trend quality filters.
    """

    def __init__(
        self,
        ema_fast: int = 12,
        ema_slow: int = 26,
        macd_signal_period: int = 9,
        atr_period: int = 14,
        atr_multiplier: float = 2.5,
        trailing_stop_atr_multiplier: float = 3.5,
        risk_per_trade_pct: float = 1.5,
        min_risk_reward_ratio: float = 1.5,
        strategy_id: str = "trend_v2",
        allow_short: bool = False,
        min_adx: float = 20.0,
        require_macd_confirm: bool = True,
        exit_on_ema_cross: bool = False,
    ):
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.macd_signal_period = macd_signal_period
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.trailing_stop_atr_multiplier = trailing_stop_atr_multiplier
        self.risk_per_trade_pct = risk_per_trade_pct
        self.min_risk_reward_ratio = min_risk_reward_ratio
        self.strategy_id = strategy_id
        self.allow_short = allow_short
        self.min_adx = min_adx
        self.require_macd_confirm = require_macd_confirm
        self.exit_on_ema_cross = exit_on_ema_cross

    # ------------------------------------------------------------------
    # Core Signal Evaluation
    # ------------------------------------------------------------------

    def evaluate(
        self,
        symbol: str,
        current_price: float,
        ema_fast_val: float,
        ema_slow_val: float,
        macd_hist: float,
        atr: float,
        current_state: TrendState,
        capital: float = 10000.0,
        adx: Optional[float] = None,
        macro_bullish: Optional[bool] = None,
        macro_bearish: Optional[bool] = None,
    ) -> Optional[StrategySignal]:
        """
        Evaluate whether to enter, exit, or adjust a trend position.
        """
        if atr <= 0 or current_price <= 0:
            return None

        ema_bullish = ema_fast_val > ema_slow_val
        macd_bullish = macd_hist > 0

        ema_pct_diff = abs(ema_fast_val - ema_slow_val) / current_price * 100 if current_price > 0 else 0
        macd_negligible = abs(macd_hist) < atr * 0.01
        is_flat = ema_pct_diff < 0.05 and macd_negligible

        if self.require_macd_confirm:
            strong_bullish = ema_bullish and macd_bullish and not is_flat
            strong_bearish = (not ema_bullish) and (not macd_bullish) and not is_flat
        else:
            strong_bullish = ema_bullish and not is_flat
            strong_bearish = (not ema_bullish) and not is_flat

        _entry_score = 0.85 if (ema_bullish == macd_bullish) else 0.70

        if not current_state.is_active:
            # Quality gates for new entries
            if adx is not None and adx < self.min_adx:
                return None
            if strong_bullish and macro_bullish is False:
                return None
            if strong_bearish and macro_bearish is False:
                return None
            return self._evaluate_entry(
                symbol, current_price, atr, strong_bullish, strong_bearish,
                ema_fast_val, ema_slow_val, macd_hist, capital, _entry_score,
            )
        else:
            return self._evaluate_exit_or_adjust(
                symbol, current_price, atr, strong_bullish, strong_bearish,
                current_state, ema_fast_val, ema_slow_val, macd_hist, capital,
                macro_bullish=macro_bullish,
            )

    def _evaluate_entry(
        self,
        symbol: str,
        price: float,
        atr: float,
        strong_bullish: bool,
        strong_bearish: bool,
        ema_fast_val: float,
        ema_slow_val: float,
        macd_hist: float,
        capital: float,
        entry_score: float = 0.70,
    ) -> Optional[StrategySignal]:
        if strong_bullish:
            direction = TrendDirection.LONG
            stop_loss = price - self.atr_multiplier * atr
            trailing_stop = price - self.trailing_stop_atr_multiplier * atr
        elif strong_bearish and self.allow_short:
            direction = TrendDirection.SHORT
            stop_loss = price + self.atr_multiplier * atr
            trailing_stop = price + self.trailing_stop_atr_multiplier * atr
        else:
            return None

        stop_distance_pct = abs(price - stop_loss) / price * 100
        if stop_distance_pct < 0.1:
            return None

        risk_amount = capital * (self.risk_per_trade_pct / 100.0)
        position_size = risk_amount / abs(price - stop_loss) if abs(price - stop_loss) > 0 else 0.0

        if position_size <= 0:
            return None

        return (
            SignalBuilder(SignalAction.START_TREND, symbol, entry_score)
            .with_metadata("direction", direction.value)
            .with_metadata("entry_price", round(price, 8))
            .with_metadata("position_size", round(position_size, 8))
            .with_metadata("stop_loss", round(stop_loss, 8))
            .with_metadata("trailing_stop", round(trailing_stop, 8))
            .with_metadata("atr", round(atr, 8))
            .with_metadata("ema_fast", round(ema_fast_val, 8))
            .with_metadata("ema_slow", round(ema_slow_val, 8))
            .with_metadata("macd_hist", round(macd_hist, 8))
            .with_metadata("risk_per_trade_pct", self.risk_per_trade_pct)
            .with_metadata("entry_reason", "ema_cross + macd_confirmation")
            .with_strategy(self.strategy_id)
            .with_ttl(30.0)
            .build()
        )

    def _evaluate_exit_or_adjust(
        self,
        symbol: str,
        price: float,
        atr: float,
        strong_bullish: bool,
        strong_bearish: bool,
        state: TrendState,
        ema_fast_val: float,
        ema_slow_val: float,
        macd_hist: float,
        capital: float,
        macro_bullish: Optional[bool] = None,
    ) -> Optional[StrategySignal]:
        """
        Check exit/adjust conditions when a position is active.

        EXIT priority:
            1. Stop-loss or Chandelier trailing stop hit -> immediate exit
            2. Chandelier trailing stop ratchet (based on highest favorable price) -> MODIFY_POSITION
            3. Optional EMA reversal exit (only if exit_on_ema_cross=True or macro turned bearish)
        """
        if state.direction == TrendDirection.LONG:
            effective_stop = max(state.stop_loss_price, state.trailing_stop_price)
            if effective_stop > 0 and price <= effective_stop:
                reason = "trailing_stop_hit" if state.trailing_stop_price > state.stop_loss_price else "stop_loss_hit"
                return self._generate_exit_signal(symbol, state, price, reason)

            # Chandelier trailing stop anchored to highest price since entry
            peak = max(state.highest_price or state.entry_price, price)
            new_trail = peak - self.trailing_stop_atr_multiplier * atr
            if new_trail > state.trailing_stop_price and new_trail > state.stop_loss_price:
                return self._generate_adjust_stop_signal(
                    symbol, state, price, new_trail, atr, highest_price=peak
                )

        elif state.direction == TrendDirection.SHORT:
            candidates = [p for p in (state.stop_loss_price, state.trailing_stop_price) if p > 0]
            effective_stop = min(candidates) if candidates else 0.0
            if effective_stop > 0 and price >= effective_stop:
                reason = "trailing_stop_hit" if (
                    state.trailing_stop_price > 0 and state.trailing_stop_price < state.stop_loss_price
                ) else "stop_loss_hit"
                return self._generate_exit_signal(symbol, state, price, reason)

            # Chandelier trailing stop anchored to lowest price since entry
            trough = min(state.lowest_price or state.entry_price, price)
            new_trail = trough + self.trailing_stop_atr_multiplier * atr
            if (state.trailing_stop_price <= 0 or new_trail < state.trailing_stop_price) and new_trail < state.stop_loss_price:
                return self._generate_adjust_stop_signal(
                    symbol, state, price, new_trail, atr, lowest_price=trough
                )

        # Only exit on EMA cross if explicitly enabled (lets Chandelier stop ride big bull trends)
        if self.exit_on_ema_cross:
            if state.direction == TrendDirection.LONG and strong_bearish:
                if self.allow_short:
                    return self._generate_reverse_signal(
                        symbol, state, price, atr, TrendDirection.SHORT,
                        ema_fast_val, ema_slow_val, macd_hist, capital,
                    )
                return self._generate_exit_signal(symbol, state, price, "trend_reversal")
            elif state.direction == TrendDirection.SHORT and strong_bullish:
                return self._generate_reverse_signal(
                    symbol, state, price, atr, TrendDirection.LONG,
                    ema_fast_val, ema_slow_val, macd_hist, capital,
                )

        return None

    # ------------------------------------------------------------------
    # Signal Factory Methods
    # ------------------------------------------------------------------

    def _generate_exit_signal(
        self, symbol: str, state: TrendState, price: float, reason: str
    ) -> StrategySignal:
        """Generate a STOP_TREND signal to close the position."""
        pnl_pct = 0.0
        if state.entry_price > 0:
            if state.direction == TrendDirection.LONG:
                pnl_pct = (price - state.entry_price) / state.entry_price * 100
            else:
                pnl_pct = (state.entry_price - price) / state.entry_price * 100

        return (
            SignalBuilder(SignalAction.STOP_TREND, symbol, 1.0)
            .with_metadata("direction", state.direction.value)
            .with_metadata("position_size", round(state.position_size, 8))
            .with_metadata("exit_price", round(price, 8))
            .with_metadata("exit_reason", reason)
            .with_metadata("pnl_pct", round(pnl_pct, 4))
            .with_strategy(self.strategy_id)
            .with_ttl(15.0)
            .build()
        )

    def _generate_reverse_signal(
        self,
        symbol: str,
        state: TrendState,
        price: float,
        atr: float,
        new_direction: TrendDirection,
        ema_fast_val: float,
        ema_slow_val: float,
        macd_hist: float,
        capital: float,
    ) -> StrategySignal:
        """
        Generate a signal that exits the current position AND enters the
        opposite direction.
        """
        pnl_pct = 0.0
        if state.entry_price > 0:
            if state.direction == TrendDirection.LONG:
                pnl_pct = (price - state.entry_price) / state.entry_price * 100
            else:
                pnl_pct = (state.entry_price - price) / state.entry_price * 100

        if new_direction == TrendDirection.LONG:
            stop_loss = price - self.atr_multiplier * atr
            trailing_stop = price - self.trailing_stop_atr_multiplier * atr
        else:
            stop_loss = price + self.atr_multiplier * atr
            trailing_stop = price + self.trailing_stop_atr_multiplier * atr

        risk_amount = capital * (self.risk_per_trade_pct / 100.0)
        new_size = risk_amount / abs(price - stop_loss) if abs(price - stop_loss) > 0 else 0.0

        return (
            SignalBuilder(SignalAction.START_TREND, symbol, 0.90)
            .with_metadata("direction", new_direction.value)
            .with_metadata("entry_price", round(price, 8))
            .with_metadata("position_size", round(new_size, 8))
            .with_metadata("stop_loss", round(stop_loss, 8))
            .with_metadata("trailing_stop", round(trailing_stop, 8))
            .with_metadata("atr", round(atr, 8))
            .with_metadata("exit_previous_pnl_pct", round(pnl_pct, 4))
            .with_metadata("entry_reason", "trend_reversal")
            .with_strategy(self.strategy_id)
            .with_ttl(30.0)
            .build()
        )

    def _generate_adjust_stop_signal(
        self,
        symbol: str,
        state: TrendState,
        price: float,
        new_trailing_stop: float,
        atr: float,
        highest_price: float = 0.0,
        lowest_price: float = 0.0,
    ) -> StrategySignal:
        """Generate a signal to update the trailing stop."""
        low_val = min(p for p in (lowest_price, price) if p > 0)
        return (
            SignalBuilder(SignalAction.MODIFY_POSITION, symbol, 0.9)
            .with_metadata("action_type", "update_trailing_stop")
            .with_metadata("direction", state.direction.value)
            .with_metadata("position_size", round(state.position_size, 8))
            .with_metadata("old_trailing_stop", round(state.trailing_stop_price, 8))
            .with_metadata("new_trailing_stop", round(new_trailing_stop, 8))
            .with_metadata("highest_price", round(max(highest_price, price), 8))
            .with_metadata("lowest_price", round(low_val, 8))
            .with_metadata("current_price", round(price, 8))
            .with_metadata("atr", round(atr, 8))
            .with_strategy(self.strategy_id)
            .with_ttl(15.0)
            .build()
        )

    # ------------------------------------------------------------------
    # State Management
    # ------------------------------------------------------------------

    def update_state(
        self,
        old_state: TrendState,
        signal: StrategySignal,
        fill_price: float,
        fill_time: Optional[str] = None,
    ) -> TrendState:
        if fill_time is None:
            fill_time = datetime.now(timezone.utc).isoformat()

        if signal.action == SignalAction.START_TREND:
            meta = signal.metadata
            direction = TrendDirection(meta["direction"])

            return TrendState(
                symbol=old_state.symbol,
                direction=direction,
                entry_price=fill_price,
                position_size=meta.get("position_size", 0.0),
                stop_loss_price=meta.get("stop_loss", 0.0),
                trailing_stop_price=meta.get("trailing_stop", 0.0),
                entry_time=fill_time,
                last_signal=signal.signal_id,
                entry_ema_fast=meta.get("ema_fast", 0.0),
                entry_ema_slow=meta.get("ema_slow", 0.0),
                entry_macd_hist=meta.get("macd_hist", 0.0),
                entry_atr=meta.get("atr", 0.0),
                highest_price=fill_price,
                lowest_price=fill_price,
            )

        elif signal.action in (SignalAction.STOP_TREND, SignalAction.CLOSE_ALL):
            return TrendState.flat(old_state.symbol)

        elif signal.action == SignalAction.MODIFY_POSITION:
            if signal.metadata.get("action_type") == "update_trailing_stop":
                new_high = max(
                    old_state.highest_price,
                    signal.metadata.get("highest_price", 0.0),
                    fill_price,
                )
                low_candidates = [
                    v for v in (
                        old_state.lowest_price,
                        signal.metadata.get("lowest_price", 0.0),
                        fill_price,
                    ) if v > 0
                ]
                new_low = min(low_candidates) if low_candidates else fill_price
                return TrendState(
                    symbol=old_state.symbol,
                    direction=old_state.direction,
                    entry_price=old_state.entry_price,
                    position_size=old_state.position_size,
                    stop_loss_price=old_state.stop_loss_price,
                    trailing_stop_price=signal.metadata["new_trailing_stop"],
                    entry_time=old_state.entry_time,
                    last_signal=signal.signal_id,
                    entry_ema_fast=old_state.entry_ema_fast,
                    entry_ema_slow=old_state.entry_ema_slow,
                    entry_macd_hist=old_state.entry_macd_hist,
                    entry_atr=old_state.entry_atr,
                    highest_price=new_high,
                    lowest_price=new_low,
                )

        return old_state

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def compute_position_size(
        self,
        capital: float,
        entry_price: float,
        stop_price: float,
    ) -> float:
        """
        Compute volatility-adjusted position size.

        Risk-based sizing: we want to lose at most risk_per_trade_pct of
        capital if the stop is hit.

        Formula:
            risk_amount = capital * risk_per_trade_pct
            position_size = risk_amount / |entry - stop|

        Args:
            capital: Total available capital.
            entry_price: Planned entry price.
            stop_price: Stop-loss price.

        Returns:
            Position size in base currency units.
        """
        risk_amount = capital * (self.risk_per_trade_pct / 100.0)
        stop_distance = abs(entry_price - stop_price)

        if stop_distance <= 0:
            return 0.0

        return risk_amount / stop_distance

    def __repr__(self) -> str:
        return (
            f"TrendStrategy(EMA={self.ema_fast}/{self.ema_slow}, "
            f"ATR_stop={self.atr_multiplier}×, trail={self.trailing_stop_atr_multiplier}×, "
            f"risk={self.risk_per_trade_pct}%/trade)"
        )
