"""
================================================================================
RISK GUARD — Independent Pre-Execution Safety Layer
================================================================================

Wraps ALL outgoing orders. If any order violates dynamically-computed limits
(max drawdown, daily loss, position size, exposure), it is HARD-BLOCKED.

The risk guard is the "immune system" — it sits between the event bus and
the execution layer, and is the FINAL gatekeeper before any order reaches
the exchange.

CHECKS (in priority order):
    1. CIRCUIT BREAKER — Is trading paused due to consecutive losses or
       volatility spike? → Block everything except CLOSE_ALL.
    2. DRAWDOWN — Has total equity drawdown exceeded max_drawdown_pct?
       → Block all new positions.
    3. DAILY LOSS — Has today's PnL exceeded max_daily_loss_pct?
       → Block all new positions for the rest of the day.
    4. POSITION SIZE — Would this order make a single position exceed
       max_position_size_pct of equity?
    5. TOTAL EXPOSURE — Would this increase total exposure beyond
       max_exposure_pct?
    6. ORDER VALUE — Is the order notional too small (dust) or too large?

RETURN FORMAT:
    RiskVerdict: {approved: bool, reason: str, modified_params: Optional[dict]}
    If approved=False, the order is HARD-BLOCKED.
    If modified_params is set, the execution layer MUST use those instead.

DECOUPLING NOTE:
    RiskGuard reads from the ledger (via a callback) and receives signals
    from the event bus. It does NOT import execution/ — it only produces
    verdicts.
================================================================================
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

class Verdict(str, Enum):
    APPROVED = "APPROVED"
    BLOCKED = "BLOCKED"
    MODIFIED = "MODIFIED"  # Approved with parameter changes


@dataclass
class RiskVerdict:
    """The output of a risk check."""
    verdict: Verdict
    reason: str = ""
    modified_params: Optional[Dict[str, Any]] = None

    @property
    def is_approved(self) -> bool:
        return self.verdict in (Verdict.APPROVED, Verdict.MODIFIED)

    @property
    def is_blocked(self) -> bool:
        return self.verdict == Verdict.BLOCKED

    @classmethod
    def approve(cls) -> RiskVerdict:
        return cls(verdict=Verdict.APPROVED, reason="passed_all_checks")

    @classmethod
    def block(cls, reason: str) -> RiskVerdict:
        return cls(verdict=Verdict.BLOCKED, reason=reason)

    @classmethod
    def modify(cls, params: Dict[str, Any], reason: str) -> RiskVerdict:
        return cls(verdict=Verdict.MODIFIED, reason=reason, modified_params=params)


@dataclass
class CircuitBreakerState:
    """Tracks the state of the circuit breaker."""
    triggered: bool = False
    trigger_reason: str = ""
    triggered_at: Optional[str] = None
    cooldown_minutes: int = 30
    consecutive_losses: int = 0
    max_consecutive_losses: int = 5
    daily_loss_so_far: float = 0.0
    volatility_spike_detected: bool = False


# ---------------------------------------------------------------------------
# RiskGuard
# ---------------------------------------------------------------------------

class RiskGuard:
    """
    Pre-execution risk management engine.

    Must be initialized with callback functions that provide current
    equity, positions, and trade history from the ledger. This keeps
    the risk module decoupled from the ledger implementation.

    USAGE:
        guard = RiskGuard(config["risk"], get_equity, get_positions, get_trade_history)
        verdict = guard.check_signal(signal)
        if verdict.is_approved:
            execution.dispatch(signal, verdict.modified_params)
    """

    def __init__(
        self,
        config: Dict[str, Any],
        equity_provider: Callable[[], float],
        positions_provider: Callable[[], List[Dict[str, Any]]],
        trade_history_provider: Callable[[str], List[Dict[str, Any]]],
    ):
        """
        Initialize the risk guard.

        Args:
            config: Risk configuration dict (from settings.json → "risk").
            equity_provider: Callable that returns current total equity.
            positions_provider: Callable that returns list of open positions.
            trade_history_provider: Callable that returns recent trades (for
                                    drawdown/circuit breaker calculation).
                                    Args: symbol (or "ALL").
        """
        self.max_drawdown_pct = config.get("max_drawdown_pct", 15.0)
        self.max_daily_loss_pct = config.get("max_daily_loss_pct", 5.0)
        self.max_position_size_pct = config.get("max_position_size_pct", 20.0)
        self.max_leverage = config.get("max_leverage", 1.0)
        self.max_exposure_pct = config.get("max_exposure_pct", 80.0)
        self.stop_loss_pct = config.get("stop_loss_pct", 2.0)
        self.trailing_stop_pct = config.get("trailing_stop_pct", 1.5)

        cb_config = config.get("circuit_breaker", {})
        self.cb_consecutive_losses = cb_config.get("consecutive_losses", 5)
        self.cb_volatility_multiplier = cb_config.get("volatility_spike_multiplier", 3.0)
        self.cb_cooldown_minutes = cb_config.get("cooldown_minutes", 30)

        # Dependency injection: ledger access
        self._equity_provider = equity_provider
        self._positions_provider = positions_provider
        self._trade_history_provider = trade_history_provider

        # State tracking
        self._circuit_breaker = CircuitBreakerState(
            max_consecutive_losses=self.cb_consecutive_losses,
            cooldown_minutes=self.cb_cooldown_minutes,
        )
        self._peak_equity: float = equity_provider()
        self._initial_capital: float = equity_provider()
        self._today_pnl: float = 0.0
        self._today_date: str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # Main Check Pipeline
    # ------------------------------------------------------------------

    def check_signal(self, signal: StrategySignal) -> RiskVerdict:
        """
        Run the full risk check pipeline on a strategy signal.

        Called by main.py for every signal before it reaches execution.

        Args:
            signal: The StrategySignal to validate.

        Returns:
            RiskVerdict (APPROVED, BLOCKED, or MODIFIED).
        """
        # Emergency signals ALWAYS pass (CLOSE_ALL, etc.)
        if signal.is_emergency():
            return RiskVerdict.approve()

        # Neutral signals pass (no action to take)
        if signal.action == SignalAction.NEUTRAL:
            return RiskVerdict.approve()

        # --- Pipeline of checks ---
        # Each check returns None if OK, or a RiskVerdict if blocked/modified.

        verdict = self._check_circuit_breaker()
        if verdict and verdict.is_blocked:
            return verdict

        verdict = self._check_daily_loss()
        if verdict and verdict.is_blocked:
            return verdict

        verdict = self._check_drawdown()
        if verdict and verdict.is_blocked:
            return verdict

        verdict = self._check_position_size(signal)
        if verdict and verdict.is_blocked:
            return verdict

        verdict = self._check_exposure(signal)
        if verdict and verdict.is_blocked:
            return verdict

        verdict = self._check_order_value(signal)
        if verdict:
            return verdict  # May be MODIFIED (reduce size)

        return RiskVerdict.approve()

    # ------------------------------------------------------------------
    # Check 1: Circuit Breaker
    # ------------------------------------------------------------------

    def _check_circuit_breaker(self) -> Optional[RiskVerdict]:
        """
        Check if the circuit breaker is tripped.

        The circuit breaker pauses trading when:
            - N consecutive losing trades (configurable, default 5).
            - Extreme volatility spike detected.

        When tripped, all new positions are blocked for cooldown_minutes.
        CLOSE_ALL signals still pass through.
        """
        cb = self._circuit_breaker

        if not cb.triggered:
            return None  # Circuit is closed

        # Check if cooldown has elapsed
        if cb.triggered_at:
            triggered_dt = datetime.fromisoformat(cb.triggered_at)
            cooldown_seconds = cb.cooldown_minutes * 60
            elapsed = (datetime.now(timezone.utc) - triggered_dt).total_seconds()
            if elapsed >= cooldown_seconds:
                # Reset circuit breaker
                cb.triggered = False
                cb.consecutive_losses = 0
                cb.trigger_reason = ""
                return None

        return RiskVerdict.block(
            f"CIRCUIT_BREAKER_TRIPPED: {cb.trigger_reason}. "
            f"Resumes at {cb.triggered_at} + {cb.cooldown_minutes}min"
        )

    # ------------------------------------------------------------------
    # Check 2: Daily Loss Limit
    # ------------------------------------------------------------------

    def _check_daily_loss(self) -> Optional[RiskVerdict]:
        """
        Check if daily loss limit has been exceeded.

        Resets at midnight UTC.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._today_date:
            self._today_date = today
            self._today_pnl = 0.0

        current_equity = self._equity_provider()
        daily_pnl = current_equity - self._initial_capital

        if self._initial_capital > 0:
            daily_loss_pct = -daily_pnl / self._initial_capital * 100
        else:
            daily_loss_pct = 0.0

        if daily_loss_pct >= self.max_daily_loss_pct:
            return RiskVerdict.block(
                f"DAILY_LOSS_LIMIT: -{daily_loss_pct:.2f}% >= "
                f"-{self.max_daily_loss_pct}% max. Paused until tomorrow."
            )

        return None

    # ------------------------------------------------------------------
    # Check 3: Max Drawdown
    # ------------------------------------------------------------------

    def _check_drawdown(self) -> Optional[RiskVerdict]:
        """
        Check if current drawdown exceeds the maximum allowed.
        """
        current_equity = self._equity_provider()

        # Update peak equity
        if current_equity > self._peak_equity:
            self._peak_equity = current_equity

        if self._peak_equity > 0:
            drawdown_pct = (self._peak_equity - current_equity) / self._peak_equity * 100
        else:
            drawdown_pct = 0.0

        if drawdown_pct >= self.max_drawdown_pct:
            return RiskVerdict.block(
                f"MAX_DRAWDOWN: {drawdown_pct:.2f}% >= {self.max_drawdown_pct}%. "
                f"Peak: {self._peak_equity:.2f}, Current: {current_equity:.2f}"
            )

        return None

    # ------------------------------------------------------------------
    # Check 4: Position Size
    # ------------------------------------------------------------------

    def _check_position_size(self, signal: StrategySignal) -> Optional[RiskVerdict]:
        """
        Check that a single position does not exceed max_position_size_pct.
        """
        if signal.action not in (SignalAction.START_GRID, SignalAction.START_TREND):
            return None

        current_equity = self._equity_provider()
        metadata = signal.metadata

        # Estimated notional value of this signal's position
        if signal.is_grid_signal():
            total_capital = metadata.get("total_capital", 0)
            position_notional = total_capital
        elif signal.is_trend_signal():
            entry_price = metadata.get("entry_price", 0)
            position_size = metadata.get("position_size", 0)
            position_notional = entry_price * position_size
        else:
            return None

        if current_equity > 0:
            position_pct = position_notional / current_equity * 100
        else:
            position_pct = 0.0

        if position_pct > self.max_position_size_pct:
            # Modify: reduce position size to max allowed
            scale_factor = self.max_position_size_pct / position_pct
            modified = dict(metadata)

            if signal.is_grid_signal():
                modified["total_capital"] = metadata.get("total_capital", 0) * scale_factor
                # Also scale individual level quantities
                if "levels" in modified:
                    modified["levels"] = [
                        {**lvl, "quantity": lvl.get("quantity", 0) * scale_factor}
                        for lvl in modified["levels"]
                    ]
            elif signal.is_trend_signal():
                modified["position_size"] = metadata.get("position_size", 0) * scale_factor

            return RiskVerdict.modify(
                modified,
                f"POSITION_SIZE_REDUCED: {position_pct:.1f}% → "
                f"{self.max_position_size_pct}% of equity",
            )

        return None

    # ------------------------------------------------------------------
    # Check 5: Total Exposure
    # ------------------------------------------------------------------

    def _check_exposure(self, signal: StrategySignal) -> Optional[RiskVerdict]:
        """
        Check that total market exposure does not exceed max_exposure_pct.
        """
        current_equity = self._equity_provider()
        positions = self._positions_provider()

        # Current exposure: use avg entry price (not stale mark price from ledger)
        current_exposure = sum(
            abs(pos.get("quantity", 0)) * pos.get("avg_entry_price", 0)
            for pos in positions if pos.get("symbol") == signal.symbol
        )

        # New exposure from this signal
        metadata = signal.metadata
        if signal.is_grid_signal():
            # Only BUY levels create new exposure (use USDT).
            # SELL levels use existing inventory, not new capital.
            levels = metadata.get("levels", [])
            new_exposure = sum(
                lvl["price"] * lvl["quantity"]
                for lvl in levels if lvl.get("side") == "BUY"
            )
        elif signal.is_trend_signal():
            new_exposure = metadata.get("entry_price", 0) * metadata.get("position_size", 0)
        else:
            new_exposure = 0.0

        total_exposure = current_exposure + new_exposure

        if current_equity > 0:
            exposure_pct = total_exposure / current_equity * 100
        else:
            exposure_pct = 0.0

        if exposure_pct > self.max_exposure_pct:
            return RiskVerdict.block(
                f"MAX_EXPOSURE: {exposure_pct:.1f}% >= {self.max_exposure_pct}%. "
                f"Current: {current_exposure:.2f}, New: {new_exposure:.2f}"
            )

        return None

    # ------------------------------------------------------------------
    # Check 6: Order Value (Dust Protection)
    # ------------------------------------------------------------------

    def _check_order_value(self, signal: StrategySignal) -> Optional[RiskVerdict]:
        """
        Check that the order notional is above dust threshold and below
        reasonable maximum.
        """
        metadata = signal.metadata

        # Grid dust filtering is handled per-level by order_manager ($1 minimum).
        # Blocking the entire grid signal because one level is small prevents
        # all other valid levels from being placed.
        if signal.is_trend_signal():
            entry_price = metadata.get("entry_price", 0)
            position_size = metadata.get("position_size", 0)
            notional = entry_price * position_size
            if notional < 1.0:
                return RiskVerdict.block(
                    f"DUST_ORDER: notional={notional:.2f} < $1.00 minimum"
                )

        return None

    # ------------------------------------------------------------------
    # Circuit Breaker Triggers
    # ------------------------------------------------------------------

    def register_trade_result(self, pnl: float) -> None:
        """
        Update circuit breaker state after a trade closes.

        Called by main.py after recording trade close in ledger.

        Args:
            pnl: Realized PnL of the closed trade (positive = win).
        """
        cb = self._circuit_breaker

        if pnl <= 0:
            cb.consecutive_losses += 1
        else:
            cb.consecutive_losses = 0

        if cb.consecutive_losses >= cb.max_consecutive_losses:
            cb.triggered = True
            cb.trigger_reason = f"{cb.consecutive_losses} consecutive losses"
            cb.triggered_at = datetime.now(timezone.utc).isoformat()

    def register_volatility_spike(self, current_vol: float, historical_vol: float) -> None:
        """
        Check for volatility spikes and trip circuit breaker if detected.

        Args:
            current_vol: Current volatility (e.g., ATR/price %).
            historical_vol: Average historical volatility.
        """
        if historical_vol <= 0:
            return

        ratio = current_vol / historical_vol
        if ratio >= self.cb_volatility_multiplier:
            self._circuit_breaker.triggered = True
            self._circuit_breaker.trigger_reason = (
                f"Volatility spike: {ratio:.1f}× average (current={current_vol:.4f}, "
                f"avg={historical_vol:.4f})"
            )
            self._circuit_breaker.triggered_at = datetime.now(timezone.utc).isoformat()

    def update_peak_equity(self) -> None:
        """Update the peak equity tracker (called periodically)."""
        current = self._equity_provider()
        if current > self._peak_equity:
            self._peak_equity = current

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def get_risk_summary(self) -> Dict[str, Any]:
        """Return a human-readable summary of current risk state."""
        current_equity = self._equity_provider()
        positions = self._positions_provider()
        drawdown = (
            (self._peak_equity - current_equity) / self._peak_equity * 100
            if self._peak_equity > 0
            else 0.0
        )
        exposure = sum(
            abs(p.get("quantity", 0)) * p.get("current_price", p.get("avg_entry_price", 0))
            for p in positions
        )
        exposure_pct = exposure / current_equity * 100 if current_equity > 0 else 0.0

        return {
            "current_equity": round(current_equity, 2),
            "peak_equity": round(self._peak_equity, 2),
            "drawdown_pct": round(drawdown, 2),
            "max_drawdown_pct": self.max_drawdown_pct,
            "daily_loss_pct_max": self.max_daily_loss_pct,
            "total_exposure": round(exposure, 2),
            "exposure_pct": round(exposure_pct, 2),
            "max_exposure_pct": self.max_exposure_pct,
            "open_positions": len(positions),
            "circuit_breaker_triggered": self._circuit_breaker.triggered,
            "circuit_breaker_reason": self._circuit_breaker.trigger_reason,
            "consecutive_losses": self._circuit_breaker.consecutive_losses,
        }

    def __repr__(self) -> str:
        return (
            f"RiskGuard(DD<={self.max_drawdown_pct}%, "
            f"DailyLoss<={self.max_daily_loss_pct}%, "
            f"PosSize<={self.max_position_size_pct}%, "
            f"Exposure<={self.max_exposure_pct}%)"
        )


# Needed for type annotation of signal parameter
from strategy.signal import SignalAction, StrategySignal
