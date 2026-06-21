"""
================================================================================
TRADING STATE MACHINE — System-Wide Trading Mode Controller
================================================================================

Manages the high-level trading state for the entire system. This is separate
from the strategy-specific state (TrendState, GridConfig) — it controls
WHICH strategy is active and WHEN to switch.

STATES:
    IDLE         — System initialized, waiting for data warm-up.
    WARMING_UP   — Indicators being computed (need N candles of history).
    ACTIVE_GRID  — Grid trading strategy is running.
    ACTIVE_TREND — Trend following strategy is running.
    PAUSED       — Trading paused (circuit breaker, daily loss limit, etc.).
    EMERGENCY    — Emergency close-all in progress.

TRANSITIONS:
    IDLE → WARMING_UP (data engine started)
    WARMING_UP → ACTIVE_GRID or ACTIVE_TREND (regime detected)
    ACTIVE_GRID ↔ ACTIVE_TREND (regime switch)
    Any → PAUSED (risk guard blocks)
    PAUSED → WARMING_UP (cooldown elapsed, re-evaluate)
    Any → EMERGENCY (CLOSE_ALL signal)
    EMERGENCY → IDLE (all positions closed)

This state machine is consulted by main.py before routing any signal
to execution.
================================================================================
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("core.state_machine")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SystemState(str, Enum):
    IDLE = "IDLE"
    WARMING_UP = "WARMING_UP"
    ACTIVE_GRID = "ACTIVE_GRID"
    ACTIVE_TREND = "ACTIVE_TREND"
    PAUSED = "PAUSED"
    EMERGENCY = "EMERGENCY"


class StateTransition(str, Enum):
    """Valid state transition triggers."""
    DATA_READY = "DATA_READY"
    REGIME_RANGING = "REGIME_RANGING"
    REGIME_TRENDING = "REGIME_TRENDING"
    RISK_PAUSE = "RISK_PAUSE"
    RISK_RESUME = "RISK_RESUME"
    EMERGENCY_TRIGGER = "EMERGENCY_TRIGGER"
    EMERGENCY_RESOLVED = "EMERGENCY_RESOLVED"
    SHUTDOWN = "SHUTDOWN"


# ---------------------------------------------------------------------------
# Valid Transitions Map
# ---------------------------------------------------------------------------

# Each state maps to a dict of {trigger: next_state}
TRANSITION_MAP: Dict[SystemState, Dict[StateTransition, SystemState]] = {
    SystemState.IDLE: {
        StateTransition.DATA_READY: SystemState.WARMING_UP,
    },
    SystemState.WARMING_UP: {
        StateTransition.REGIME_RANGING: SystemState.ACTIVE_GRID,
        StateTransition.REGIME_TRENDING: SystemState.ACTIVE_TREND,
        StateTransition.EMERGENCY_TRIGGER: SystemState.EMERGENCY,
        StateTransition.SHUTDOWN: SystemState.IDLE,
    },
    SystemState.ACTIVE_GRID: {
        StateTransition.REGIME_TRENDING: SystemState.ACTIVE_TREND,
        StateTransition.RISK_PAUSE: SystemState.PAUSED,
        StateTransition.EMERGENCY_TRIGGER: SystemState.EMERGENCY,
        StateTransition.SHUTDOWN: SystemState.IDLE,
    },
    SystemState.ACTIVE_TREND: {
        StateTransition.REGIME_RANGING: SystemState.ACTIVE_GRID,
        StateTransition.RISK_PAUSE: SystemState.PAUSED,
        StateTransition.EMERGENCY_TRIGGER: SystemState.EMERGENCY,
        StateTransition.SHUTDOWN: SystemState.IDLE,
    },
    SystemState.PAUSED: {
        StateTransition.RISK_RESUME: SystemState.WARMING_UP,
        StateTransition.EMERGENCY_TRIGGER: SystemState.EMERGENCY,
        StateTransition.SHUTDOWN: SystemState.IDLE,
    },
    SystemState.EMERGENCY: {
        StateTransition.EMERGENCY_RESOLVED: SystemState.IDLE,
        StateTransition.SHUTDOWN: SystemState.IDLE,
    },
}


# ---------------------------------------------------------------------------
# State Machine
# ---------------------------------------------------------------------------

@dataclass
class StateMachineSnapshot:
    """Serializable snapshot of the state machine for logging."""
    state: str
    previous_state: str
    since: str
    transition_count: int
    metadata: Dict[str, Any]


class TradingStateMachine:
    """
    Manages the system-wide trading state and enforces valid transitions.

    USAGE:
        sm = TradingStateMachine()
        sm.transition(StateTransition.DATA_READY, metadata={"symbols": 2})
        if sm.state == SystemState.ACTIVE_GRID:
            # Route to grid strategy
    """

    def __init__(self, initial_state: SystemState = SystemState.IDLE):
        self._state = initial_state
        self._previous_state = initial_state
        self._state_since = datetime.now(timezone.utc)
        self._transition_count = 0
        self._metadata: Dict[str, Any] = {}
        self._history: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def state(self) -> SystemState:
        return self._state

    @property
    def previous_state(self) -> SystemState:
        return self._previous_state

    @property
    def is_active(self) -> bool:
        """True if the system is actively trading (grid or trend)."""
        return self._state in (SystemState.ACTIVE_GRID, SystemState.ACTIVE_TREND)

    @property
    def can_trade(self) -> bool:
        """True if trading is allowed (not paused, emergency, or idle)."""
        return self._state in (
            SystemState.WARMING_UP,
            SystemState.ACTIVE_GRID,
            SystemState.ACTIVE_TREND,
        )

    @property
    def state_duration_seconds(self) -> float:
        """How long the system has been in the current state."""
        return (datetime.now(timezone.utc) - self._state_since).total_seconds()

    # ------------------------------------------------------------------
    # State Transition
    # ------------------------------------------------------------------

    def transition(
        self,
        trigger: StateTransition,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Attempt a state transition.

        Args:
            trigger: The event triggering the transition.
            metadata: Optional context for the transition (logged).

        Returns:
            True if the transition was valid and executed.

        Raises:
            ValueError: If the transition is invalid for the current state.
        """
        valid_transitions = TRANSITION_MAP.get(self._state, {})
        if trigger not in valid_transitions:
            logger.warning(
                f"INVALID TRANSITION: {self._state.value} → {trigger.value} "
                f"(valid: {list(valid_transitions.keys())})"
            )
            return False

        new_state = valid_transitions[trigger]
        old_state = self._state

        self._previous_state = old_state
        self._state = new_state
        self._state_since = datetime.now(timezone.utc)
        self._transition_count += 1
        self._metadata = metadata or {}

        # Log the transition
        entry = {
            "from": old_state.value,
            "to": new_state.value,
            "trigger": trigger.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": self._metadata,
        }
        self._history.append(entry)

        logger.info(
            f"STATE TRANSITION #{self._transition_count}: "
            f"{old_state.value} → {new_state.value} "
            f"(trigger: {trigger.value})"
        )

        return True

    # ------------------------------------------------------------------
    # Convenience Methods
    # ------------------------------------------------------------------

    def activate_grid(self) -> bool:
        """Transition to ACTIVE_GRID (e.g., when regime is ranging)."""
        return self.transition(StateTransition.REGIME_RANGING)

    def activate_trend(self) -> bool:
        """Transition to ACTIVE_TREND (e.g., when regime is trending)."""
        return self.transition(StateTransition.REGIME_TRENDING)

    def pause(self, reason: str = "") -> bool:
        """Pause trading (e.g., risk guard triggered)."""
        return self.transition(StateTransition.RISK_PAUSE, {"reason": reason})

    def resume(self) -> bool:
        """Resume trading after pause."""
        return self.transition(StateTransition.RISK_RESUME)

    def emergency(self, reason: str = "") -> bool:
        """Trigger emergency stop."""
        return self.transition(StateTransition.EMERGENCY_TRIGGER, {"reason": reason})

    def emergency_resolved(self) -> bool:
        """Mark emergency as resolved."""
        return self.transition(StateTransition.EMERGENCY_RESOLVED)

    def shutdown(self) -> bool:
        """Graceful shutdown."""
        return self.transition(StateTransition.SHUTDOWN)

    # ------------------------------------------------------------------
    # Snapshot & History
    # ------------------------------------------------------------------

    def snapshot(self) -> StateMachineSnapshot:
        """Return a serializable snapshot of current state."""
        return StateMachineSnapshot(
            state=self._state.value,
            previous_state=self._previous_state.value,
            since=self._state_since.isoformat(),
            transition_count=self._transition_count,
            metadata=self._metadata.copy(),
        )

    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return the last N state transitions."""
        return self._history[-limit:]

    def __repr__(self) -> str:
        return (
            f"TradingStateMachine(state={self._state.value}, "
            f"transitions={self._transition_count}, "
            f"duration={self.state_duration_seconds:.0f}s)"
        )
