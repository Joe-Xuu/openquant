"""
================================================================================
STRATEGY SIGNAL — Standardized, Immutable Inter-Module Contract
================================================================================

Every strategy (Grid, Trend, composite) emits a StrategySignal. This is the
ONLY data structure allowed to cross from strategy/ to the event bus (main.py).
The signal is frozen (immutable) to prevent downstream modules from tampering
with the brain's output — risk/ and execution/ may READ it, but never MUTATE it.

DESIGN PRINCIPLES:
    1. Immutability: Frozen dataclass — once created, never changed.
    2. Expiry: Every signal carries an `expires_at` timestamp. Stale signals
       (e.g., a grid config computed 30s ago during a volatility spike) are
       discarded by the event bus before reaching execution.
    3. Serialization: `to_dict()` / `from_dict()` for JSON-safe event bus
       transport and audit logging.
    4. Semantic helpers: `is_grid_signal()`, `is_trend_signal()`, etc. so
       the event bus can route without inspecting raw strings.

SIGNAL LIFECYCLE:
    strategy/  →  Signal created (immutable, timestamped)
       ↓
    main.py    →  Reads action, routes to risk/
       ↓
    risk/      →  Reads metadata, decides PASS/BLOCK/MODIFY
       ↓
    execution/ →  Reads parameters, dispatches to exchange
       ↓
    main.py    →  Records outcome in core/local_ledger.py
================================================================================
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Signal Action Enum
# ---------------------------------------------------------------------------

class SignalAction(str, Enum):
    """
    All possible actions a strategy can request.

    Naming convention: VERB_TARGET — the verb describes what the execution
    layer should DO, the target describes WHAT it applies to.

    GRID actions:
        START_GRID  — Deploy a fresh grid of limit orders.
        STOP_GRID   — Cancel all orders for a grid and close any open positions.
        PAUSE_GRID  — Cancel orders but keep tracking the grid (price out of range).
        RESUME_GRID — Re-arm a paused grid at a new reference price.

    TREND actions:
        START_TREND — Enter a directional position with stop-loss.
        STOP_TREND  — Close the trend position (either TP hit or manual exit).

    UNIVERSAL actions:
        NEUTRAL     — Do nothing this tick (regime uncertain or transitioning).
        CLOSE_ALL   — Emergency: close all positions and cancel all orders.
    """

    # Grid lifecycle
    START_GRID = "START_GRID"
    STOP_GRID = "STOP_GRID"
    PAUSE_GRID = "PAUSE_GRID"
    RESUME_GRID = "RESUME_GRID"

    # Trend lifecycle
    START_TREND = "START_TREND"
    STOP_TREND = "STOP_TREND"

    # Universal
    NEUTRAL = "NEUTRAL"
    CLOSE_ALL = "CLOSE_ALL"

    # Future extensibility
    MODIFY_POSITION = "MODIFY_POSITION"

    @classmethod
    def grid_actions(cls) -> set[SignalAction]:
        """Return the set of grid-related actions (for routing)."""
        return {cls.START_GRID, cls.STOP_GRID, cls.PAUSE_GRID, cls.RESUME_GRID}

    @classmethod
    def trend_actions(cls) -> set[SignalAction]:
        """Return the set of trend-related actions (for routing)."""
        return {cls.START_TREND, cls.STOP_TREND}

    @classmethod
    def emergency_actions(cls) -> set[SignalAction]:
        """Actions that bypass normal risk checks (used by circuit breaker)."""
        return {cls.CLOSE_ALL}


# ---------------------------------------------------------------------------
# StrategySignal — The Standardized Output
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StrategySignal:
    """
    Immutable signal emitted by any strategy module.

    Attributes:
        action:      What the execution layer should do.
        symbol:      Trading pair (e.g., "BTCUSDT").
        score:       Confidence score [0.0, 1.0] from the regime detector or
                     strategy. 0 = no confidence, 1 = maximum confidence.
        metadata:    Strategy-specific parameters (grid levels, stop prices,
                     position size, etc.). Free-form dict — validated by the
                     execution module based on `action`.
        timestamp:   ISO-8601 UTC timestamp of signal creation.
        expires_at:  ISO-8601 UTC timestamp after which this signal is stale.
                     Defaults to 30 seconds from creation.
        signal_id:   Unique deterministic ID for idempotency. Two signals with
                     the same (action, symbol, metadata_hash) produce the same
                     signal_id — prevents duplicate processing on replay.
        strategy_id: Identifier for the strategy that generated this signal
                     (e.g., "grid_v1", "trend_ema_cross").
    """

    action: SignalAction
    symbol: str
    score: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    expires_at: str = field(default="")
    signal_id: str = field(default="")
    strategy_id: str = ""

    # ------------------------------------------------------------------
    # Post-initialization (via __post_init__ or object.__setattr__)
    # ------------------------------------------------------------------

    def __post_init__(self):
        """
        Validate and finalize the signal after construction.

        Uses object.__setattr__ to bypass frozen=True for computed fields.
        """
        # --- Validate score range ---
        if not (0.0 <= self.score <= 1.0):
            raise ValueError(f"score must be in [0.0, 1.0], got {self.score}")

        # --- Validate symbol is non-empty ---
        if not self.symbol or not self.symbol.strip():
            raise ValueError("symbol must be a non-empty trading pair")

        # --- Compute expires_at if not provided ---
        if not self.expires_at:
            # Parse timestamp to compute expiry
            ts = datetime.fromisoformat(self.timestamp)
            expiry = ts.timestamp() + 30.0  # 30-second default TTL
            object.__setattr__(
                self,
                "expires_at",
                datetime.fromtimestamp(expiry, tz=timezone.utc).isoformat(),
            )

        # --- Compute deterministic signal_id if not provided ---
        if not self.signal_id:
            object.__setattr__(self, "signal_id", self._compute_signal_id())

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------

    def _compute_signal_id(self) -> str:
        """
        Generate a deterministic signal ID from (action, symbol, metadata).

        Two signals with identical parameters will produce the same ID,
        enabling the event bus to deduplicate. Uses SHA-256 truncated to 16 hex
        chars — sufficient collision resistance for a single trading session.
        """
        canonical = json.dumps(
            {
                "action": self.action.value,
                "symbol": self.symbol,
                "metadata": self.metadata,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Expiry
    # ------------------------------------------------------------------

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """
        Check if this signal has expired.

        Args:
            now: Current time. Defaults to datetime.now(timezone.utc).

        Returns:
            True if the signal is stale and should be discarded.
        """
        if now is None:
            now = datetime.now(timezone.utc)
        expiry = datetime.fromisoformat(self.expires_at)
        return now >= expiry

    def seconds_until_expiry(self, now: Optional[datetime] = None) -> float:
        """Return seconds remaining before expiry (negative if expired)."""
        if now is None:
            now = datetime.now(timezone.utc)
        expiry = datetime.fromisoformat(self.expires_at)
        return (expiry - now).total_seconds()

    # ------------------------------------------------------------------
    # Routing Helpers
    # ------------------------------------------------------------------

    def is_grid_signal(self) -> bool:
        """True if this signal relates to grid trading."""
        return self.action in SignalAction.grid_actions()

    def is_trend_signal(self) -> bool:
        """True if this signal relates to trend following."""
        return self.action in SignalAction.trend_actions()

    def is_emergency(self) -> bool:
        """True if this is an emergency action that should bypass risk checks."""
        return self.action in SignalAction.emergency_actions()

    @property
    def requires_order(self) -> bool:
        """
        True if this signal requires placing/cancelling orders.

        NEUTRAL signals (no action) return False — the event bus can skip
        the execution module entirely for these ticks.
        """
        return self.action not in (SignalAction.NEUTRAL,)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to a JSON-safe dictionary for event bus transport.

        All values are JSON-serializable primitives (str, float, int, dict, list).
        """
        return {
            "action": self.action.value,
            "symbol": self.symbol,
            "score": self.score,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "expires_at": self.expires_at,
            "signal_id": self.signal_id,
            "strategy_id": self.strategy_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> StrategySignal:
        """
        Deserialize from a dictionary (e.g., received over the event bus).

        Args:
            data: Dictionary with at minimum 'action', 'symbol', 'score'.

        Returns:
            A validated StrategySignal instance.

        Raises:
            KeyError: If required fields are missing.
            ValueError: If action is not a valid SignalAction.
        """
        return cls(
            action=SignalAction(data["action"]),
            symbol=data["symbol"],
            score=float(data["score"]),
            metadata=data.get("metadata", {}),
            timestamp=data.get("timestamp", ""),
            expires_at=data.get("expires_at", ""),
            signal_id=data.get("signal_id", ""),
            strategy_id=data.get("strategy_id", ""),
        )

    def to_json(self) -> str:
        """Serialize to a JSON string (for logging, message queues, etc.)."""
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, json_str: str) -> StrategySignal:
        """Deserialize from a JSON string."""
        return cls.from_dict(json.loads(json_str))

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"StrategySignal(action={self.action.value}, symbol={self.symbol}, "
            f"score={self.score:.3f}, id={self.signal_id}, "
            f"expires_in={self.seconds_until_expiry():.0f}s)"
        )


# ---------------------------------------------------------------------------
# Signal Builder — Convenience Factory
# ---------------------------------------------------------------------------

class SignalBuilder:
    """
    Fluent builder for StrategySignal to reduce boilerplate in strategy code.

    Usage:
        signal = (
            SignalBuilder(SignalAction.START_GRID, "BTCUSDT", 0.85)
            .with_metadata("grid_type", "geometric")
            .with_metadata("grid_levels", 10)
            .with_strategy("grid_v1")
            .with_ttl(60)
            .build()
        )
    """

    def __init__(self, action: SignalAction, symbol: str, score: float):
        self._action = action
        self._symbol = symbol
        self._score = score
        self._metadata: Dict[str, Any] = {}
        self._strategy_id = ""
        self._ttl_seconds = 30

    def with_metadata(self, key: str, value: Any) -> SignalBuilder:
        """Add a key-value pair to the signal metadata."""
        self._metadata[key] = value
        return self

    def with_metadata_bulk(self, kv: Dict[str, Any]) -> SignalBuilder:
        """Merge a dict into the signal metadata."""
        self._metadata.update(kv)
        return self

    def with_strategy(self, strategy_id: str) -> SignalBuilder:
        """Set the strategy identifier."""
        self._strategy_id = strategy_id
        return self

    def with_ttl(self, seconds: float) -> SignalBuilder:
        """
        Set the time-to-live in seconds.

        Grid signals typically need longer TTLs (60-120s) because order
        placement may take time. Trend signals use shorter TTLs (15-30s)
        because entries are time-sensitive.
        """
        self._ttl_seconds = seconds
        return self

    def build(self) -> StrategySignal:
        """Construct the immutable StrategySignal."""
        now = datetime.now(timezone.utc)
        return StrategySignal(
            action=self._action,
            symbol=self._symbol,
            score=self._score,
            metadata=self._metadata.copy(),
            timestamp=now.isoformat(),
            expires_at=datetime.fromtimestamp(
                now.timestamp() + self._ttl_seconds, tz=timezone.utc
            ).isoformat(),
            strategy_id=self._strategy_id,
        )
