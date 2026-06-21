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
    Compute trend-following entry/exit signals.

    This is a PURE COMPUTATION engine. It receives indicator values and
    returns StrategySignal objects. It performs NO I/O.

    USAGE:
        engine = TrendStrategy(
            ema_fast=12, ema_slow=26, macd_signal=9,
            atr_period=14, atr_multiplier=2.0, trailing_stop_atr_multiplier=3.0,
        )

        # On each tick:
        signal = engine.evaluate(
            symbol="BTCUSDT",
            current_price=97500.0,
            ema_fast=97400.0,
            ema_slow=97100.0,
            macd_hist=150.0,
            atr=1200.0,
            current_state=state,
        )
        if signal:
            state = engine.update_state(state, signal, current_price)
    """

    def __init__(
        self,
        ema_fast: int = 12,
        ema_slow: int = 26,
        macd_signal_period: int = 9,
        atr_period: int = 14,
        atr_multiplier: float = 2.0,
        trailing_stop_atr_multiplier: float = 3.0,
        risk_per_trade_pct: float = 1.0,
        min_risk_reward_ratio: float = 1.5,
        strategy_id: str = "trend_v1",
    ):
        """
        Initialize the trend-following strategy.

        Args:
            ema_fast: Fast EMA period (default 12).
            ema_slow: Slow EMA period (default 26).
            macd_signal_period: MACD signal line period (default 9).
            atr_period: ATR lookback period (default 14).
            atr_multiplier: Stop-loss distance in ATR multiples (default 2.0).
            trailing_stop_atr_multiplier: Trailing stop distance (default 3.0).
            risk_per_trade_pct: % of capital to risk per trade (default 1.0%).
            min_risk_reward_ratio: Minimum reward/risk for entry (default 1.5).
            strategy_id: Identifier for signal tagging.
        """
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.macd_signal_period = macd_signal_period
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.trailing_stop_atr_multiplier = trailing_stop_atr_multiplier
        self.risk_per_trade_pct = risk_per_trade_pct
        self.min_risk_reward_ratio = min_risk_reward_ratio
        self.strategy_id = strategy_id

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
    ) -> Optional[StrategySignal]:
        """
        Evaluate whether to enter, exit, or adjust a trend position.

        This is the MAIN entry point. Call on every tick with the latest
        indicator values.

        Args:
            symbol: Trading pair.
            current_price: Latest mid-price.
            ema_fast_val: Current fast EMA value.
            ema_slow_val: Current slow EMA value.
            macd_hist: Current MACD histogram value.
            atr: Current ATR value.
            current_state: Current TrendState for this symbol.
            capital: Available capital for position sizing.

        Returns:
            A StrategySignal if action is required, None otherwise.
        """
        if atr <= 0 or current_price <= 0:
            return None

        # --- Determine market bias ---
        # EMA alignment indicates direction
        ema_bullish = ema_fast_val > ema_slow_val
        # MACD histogram confirms momentum
        macd_bullish = macd_hist > 0

        # ---- Flat-market detection ----
        # When EMAs are essentially equal AND MACD is near zero, the market
        # has no clear direction — don't enter.
        # Threshold: EMA difference < 0.05% of price AND |MACD hist| < ATR * 0.01
        ema_pct_diff = abs(ema_fast_val - ema_slow_val) / current_price * 100 if current_price > 0 else 0
        macd_negligible = abs(macd_hist) < atr * 0.01
        is_flat = ema_pct_diff < 0.05 and macd_negligible

        # Strong signals: both indicators agree AND market is not flat
        strong_bullish = ema_bullish and macd_bullish and not is_flat
        strong_bearish = (not ema_bullish) and (not macd_bullish) and not is_flat

        # --- State-dependent logic ---
        if not current_state.is_active:
            return self._evaluate_entry(
                symbol, current_price, atr, strong_bullish, strong_bearish,
                ema_fast_val, ema_slow_val, macd_hist, capital,
            )
        else:
            return self._evaluate_exit_or_adjust(
                symbol, current_price, atr, strong_bullish, strong_bearish,
                current_state, ema_fast_val, ema_slow_val, macd_hist, capital,
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
    ) -> Optional[StrategySignal]:
        """
        Check entry conditions when flat (no position).

        LONG entry:  strong_bullish (EMA crossover up + MACD > 0)
        SHORT entry: strong_bearish (EMA crossover down + MACD < 0)
        """
        if strong_bullish:
            direction = TrendDirection.LONG
            stop_loss = price - self.atr_multiplier * atr
            trailing_stop = price - self.trailing_stop_atr_multiplier * atr
        elif strong_bearish:
            direction = TrendDirection.SHORT
            stop_loss = price + self.atr_multiplier * atr
            trailing_stop = price + self.trailing_stop_atr_multiplier * atr
        else:
            return None  # No clear direction

        # --- Risk/reward check ---
        stop_distance_pct = abs(price - stop_loss) / price * 100
        if stop_distance_pct < 0.1:
            return None  # Stop too close — market noise will trigger it

        # --- Position sizing (Kelly-style, volatility-adjusted) ---
        risk_amount = capital * (self.risk_per_trade_pct / 100.0)
        position_size = risk_amount / abs(price - stop_loss) if abs(price - stop_loss) > 0 else 0.0

        if position_size <= 0:
            return None

        score = 0.85 if (strong_bullish and macd_hist > 0) or (strong_bearish and macd_hist < 0) else 0.70

        return (
            SignalBuilder(SignalAction.START_TREND, symbol, score)
            .with_metadata("direction", direction.value)
            .with_metadata("entry_price", price)
            .with_metadata("position_size", round(position_size, 8))
            .with_metadata("stop_loss", round(stop_loss, 2))
            .with_metadata("trailing_stop", round(trailing_stop, 2))
            .with_metadata("atr", round(atr, 2))
            .with_metadata("ema_fast", round(ema_fast_val, 2))
            .with_metadata("ema_slow", round(ema_slow_val, 2))
            .with_metadata("macd_hist", round(macd_hist, 2))
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
    ) -> Optional[StrategySignal]:
        """
        Check exit/adjust conditions when a position is active.

        EXIT conditions (in priority order):
            1. Price hits stop-loss → immediate exit
            2. Trend reversal (opposite strong signal) → exit + reverse
            3. EMA cross against position → exit
        """
        # --- Check stop-loss hit ---
        if state.direction == TrendDirection.LONG:
            if price <= state.stop_loss_price:
                return self._generate_exit_signal(symbol, state, price, "stop_loss_hit")
            # Update trailing stop (ratchet up only)
            new_trail = price - self.trailing_stop_atr_multiplier * atr
            if new_trail > state.trailing_stop_price:
                return self._generate_adjust_stop_signal(
                    symbol, state, price, new_trail, atr
                )

        elif state.direction == TrendDirection.SHORT:
            if price >= state.stop_loss_price:
                return self._generate_exit_signal(symbol, state, price, "stop_loss_hit")
            # Update trailing stop (ratchet down only)
            new_trail = price + self.trailing_stop_atr_multiplier * atr
            if new_trail < state.trailing_stop_price:
                return self._generate_adjust_stop_signal(
                    symbol, state, price, new_trail, atr
                )

        # --- Check trend reversal ---
        if state.direction == TrendDirection.LONG and strong_bearish:
            # Exit long and enter short
            return self._generate_reverse_signal(
                symbol, state, price, atr, TrendDirection.SHORT,
                ema_fast_val, ema_slow_val, macd_hist, capital,
            )
        elif state.direction == TrendDirection.SHORT and strong_bullish:
            # Exit short and enter long
            return self._generate_reverse_signal(
                symbol, state, price, atr, TrendDirection.LONG,
                ema_fast_val, ema_slow_val, macd_hist, capital,
            )

        # --- Check EMA cross against position ---
        ema_cross_down = ema_fast_val < ema_slow_val
        ema_cross_up = ema_fast_val > ema_slow_val

        if state.direction == TrendDirection.LONG and ema_cross_down and macd_hist < 0:
            return self._generate_exit_signal(symbol, state, price, "ema_cross_against")
        elif state.direction == TrendDirection.SHORT and ema_cross_up and macd_hist > 0:
            return self._generate_exit_signal(symbol, state, price, "ema_cross_against")

        return None  # Hold position, no action needed

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
            .with_metadata("exit_price", price)
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

        This is rarer than a simple exit — it requires strong conviction
        in the opposite direction.
        """
        # Compute exit PnL
        pnl_pct = 0.0
        if state.entry_price > 0:
            if state.direction == TrendDirection.LONG:
                pnl_pct = (price - state.entry_price) / state.entry_price * 100
            else:
                pnl_pct = (state.entry_price - price) / state.entry_price * 100

        # Compute new entry parameters
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
            .with_metadata("entry_price", price)
            .with_metadata("position_size", round(new_size, 8))
            .with_metadata("stop_loss", round(stop_loss, 2))
            .with_metadata("trailing_stop", round(trailing_stop, 2))
            .with_metadata("atr", round(atr, 2))
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
    ) -> StrategySignal:
        """Generate a signal to update the trailing stop."""
        return (
            SignalBuilder(SignalAction.MODIFY_POSITION, symbol, 0.9)
            .with_metadata("action_type", "update_trailing_stop")
            .with_metadata("direction", state.direction.value)
            .with_metadata("old_trailing_stop", round(state.trailing_stop_price, 2))
            .with_metadata("new_trailing_stop", round(new_trailing_stop, 2))
            .with_metadata("current_price", price)
            .with_metadata("atr", round(atr, 2))
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
        """
        Compute the new TrendState after a signal is executed.

        The strategy module does NOT track state internally — it's a pure
        function. The caller (main.py) manages state and passes it in.

        Args:
            old_state: The state before the signal was executed.
            signal: The signal that was executed.
            fill_price: The actual fill price from the exchange.
            fill_time: ISO-8601 timestamp of the fill.

        Returns:
            The new TrendState after applying the signal.
        """
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
            )

        elif signal.action in (SignalAction.STOP_TREND, SignalAction.CLOSE_ALL):
            return TrendState.flat(old_state.symbol)

        elif signal.action == SignalAction.MODIFY_POSITION:
            if signal.metadata.get("action_type") == "update_trailing_stop":
                new_state = TrendState(
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
                )
                return new_state

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
