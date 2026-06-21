"""
================================================================================
GRID STRATEGY — Geometric & Arithmetic Grid Trading Engine
================================================================================

A grid strategy places a ladder of limit buy orders below the reference price
and a ladder of limit sell orders above it. When price oscillates within a
range, the grid captures small profits on each round-trip (buy low, sell high).

TWO GRID TYPES:
    ARITHMETIC  — Fixed dollar/price step between levels.
                  Best for: stable coins, low-volatility pairs, tight ranges.
                  Example: BTC at $100,000 with $500 steps.
                  Level i: ref ± i * step

    GEOMETRIC   — Fixed percentage step between levels.
                  Best for: volatile assets with log-normal price distribution.
                  Example: BTC with 0.5% spacing.
                  Level i: ref * (1 + rate)^(±i)

WHY GEOMETRIC IS DEFAULT:
    BTC price is log-normally distributed. A $500 step at BTC=$100,000 is a
    0.5% move. The same $500 at BTC=$50,000 is a 1.0% move — the grid density
    changes with price! Geometric spacing maintains constant percentage density,
    which is what matters for profit capture and risk exposure.

LIFECYCLE STATE MACHINE:
    ACTIVE      — Orders placed, grid capturing spread.
    PAUSED      — Price moved outside grid range; orders cancelled, monitoring.
    REBALANCING — New reference price computed; old orders cancelled, new ones
                  being placed. Transitions to ACTIVE on completion.
    CLOSED      — Grid terminated; all positions closed, no further action.

DECOUPLING NOTE:
    This module NEVER imports from execution/ or core/. It produces
    StrategySignal objects that the event bus (main.py) routes downstream.
================================================================================
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple

from strategy.signal import SignalAction, SignalBuilder, StrategySignal


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class GridType(str, Enum):
    """The spacing mode for grid levels."""
    ARITHMETIC = "arithmetic"
    GEOMETRIC = "geometric"


class GridStatus(str, Enum):
    """Lifecycle states for a grid configuration."""
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    REBALANCING = "REBALANCING"
    CLOSED = "CLOSED"


class LevelSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class LevelStatus(str, Enum):
    """Per-level status within a grid."""
    ARMED = "ARMED"       # Limit order placed, waiting for fill
    FILLED = "FILLED"     # Order filled, waiting for take-profit
    CANCELLED = "CANCELLED"
    TP_PLACED = "TP_PLACED"  # Take-profit order placed


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class GridLevel:
    """
    A single rung in the grid ladder.

    For a BUY level at price P:
        - We place a limit BUY at P.
        - When filled, we place a limit SELL at take_profit_price (P + profit%).
        - Net profit = take_profit_price - P - 2*fee (after round-trip).

    For a SELL level at price P (short grid, if enabled):
        - We place a limit SELL at P.
        - When filled, we place a limit BUY at take_profit_price (P - profit%).
    """

    level_index: int
    side: LevelSide
    price: float
    quantity: float
    take_profit_price: float
    status: LevelStatus = LevelStatus.ARMED

    @property
    def notional_value(self) -> float:
        """The quote-currency value of this level if filled."""
        return self.price * self.quantity

    @property
    def expected_profit(self) -> float:
        """Gross profit if this level completes a round-trip (before fees)."""
        if self.side == LevelSide.BUY:
            return self.quantity * (self.take_profit_price - self.price)
        else:
            return self.quantity * (self.price - self.take_profit_price)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "level_index": self.level_index,
            "side": self.side.value,
            "price": self.price,
            "quantity": self.quantity,
            "take_profit_price": self.take_profit_price,
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> GridLevel:
        return cls(
            level_index=data["level_index"],
            side=LevelSide(data["side"]),
            price=data["price"],
            quantity=data["quantity"],
            take_profit_price=data["take_profit_price"],
            status=LevelStatus(data.get("status", "ARMED")),
        )


@dataclass
class GridConfig:
    """
    Complete grid configuration — the output of compute_grid().

    This is the "grid state" — it captures everything the execution layer
    needs to place and manage orders. It is serializable for persistence
    across restarts.
    """

    grid_id: str
    symbol: str
    grid_type: GridType
    reference_price: float
    upper_bound: float
    lower_bound: float
    levels: List[GridLevel]
    profit_per_grid_pct: float
    total_capital: float
    status: GridStatus = GridStatus.ACTIVE
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def buy_levels(self) -> List[GridLevel]:
        """Return only the BUY-side levels, sorted by price descending (best first)."""
        return sorted(
            [lvl for lvl in self.levels if lvl.side == LevelSide.BUY],
            key=lambda x: x.price,
            reverse=True,
        )

    @property
    def sell_levels(self) -> List[GridLevel]:
        """Return only the SELL-side levels, sorted by price ascending (best first)."""
        return sorted(
            [lvl for lvl in self.levels if lvl.side == LevelSide.SELL],
            key=lambda x: x.price,
        )

    @property
    def armed_levels(self) -> List[GridLevel]:
        """Levels that currently have active orders."""
        return [lvl for lvl in self.levels if lvl.status == LevelStatus.ARMED]

    @property
    def filled_levels(self) -> List[GridLevel]:
        """Levels where the entry order has been filled."""
        return [lvl for lvl in self.levels if lvl.status == LevelStatus.FILLED]

    @property
    def total_buy_notional(self) -> float:
        """Total quote currency allocated to buy orders."""
        return sum(lvl.notional_value for lvl in self.buy_levels)

    @property
    def total_sell_notional(self) -> float:
        """Total quote currency allocated to sell orders (base qty * price)."""
        return sum(lvl.notional_value for lvl in self.sell_levels)

    @property
    def max_potential_profit_per_cycle(self) -> float:
        """
        Maximum gross profit if every level completes one round-trip.
        Assumes each buy fills and sells at its take-profit, and each
        sell fills and buys back at its take-profit.
        """
        return sum(lvl.expected_profit for lvl in self.levels)

    @property
    def grid_density(self) -> float:
        """Average percentage spacing between adjacent levels (diagnostic)."""
        if not self.levels:
            return 0.0
        prices = sorted([lvl.price for lvl in self.levels])
        if len(prices) < 2:
            return 0.0
        gaps = [(prices[i] - prices[i - 1]) / prices[i - 1] * 100 for i in range(1, len(prices))]
        return sum(gaps) / len(gaps)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "grid_id": self.grid_id,
            "symbol": self.symbol,
            "grid_type": self.grid_type.value,
            "reference_price": self.reference_price,
            "upper_bound": self.upper_bound,
            "lower_bound": self.lower_bound,
            "levels": [lvl.to_dict() for lvl in self.levels],
            "profit_per_grid_pct": self.profit_per_grid_pct,
            "total_capital": self.total_capital,
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> GridConfig:
        return cls(
            grid_id=data["grid_id"],
            symbol=data["symbol"],
            grid_type=GridType(data["grid_type"]),
            reference_price=data["reference_price"],
            upper_bound=data["upper_bound"],
            lower_bound=data["lower_bound"],
            levels=[GridLevel.from_dict(lvl) for lvl in data["levels"]],
            profit_per_grid_pct=data["profit_per_grid_pct"],
            total_capital=data["total_capital"],
            status=GridStatus(data.get("status", "ACTIVE")),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
        )


# ---------------------------------------------------------------------------
# GridStrategy — The Grid Engine
# ---------------------------------------------------------------------------

class GridStrategy:
    """
    Compute and manage grid trading configurations.

    This is a PURE COMPUTATION engine. It receives parameters and prices,
    and returns GridConfig and StrategySignal objects. It performs NO I/O,
    makes NO API calls, and has NO knowledge of the execution layer.

    USAGE:
        engine = GridStrategy(
            grid_type="geometric",
            upper_bound_pct=5.0,
            lower_bound_pct=5.0,
            grid_levels=10,
            profit_per_grid_pct=0.5,
            total_capital=10000.0,
        )

        # On each tick:
        grid = engine.compute_grid(reference_price=97000.0, symbol="BTCUSDT")
        signal = engine.generate_signal(grid)

        # Check if rebalancing is needed:
        if engine.check_rebalance(current_price=98000.0, grid=grid):
            new_grid = engine.compute_grid(reference_price=98000.0, symbol="BTCUSDT")
    """

    def __init__(
        self,
        grid_type: Literal["arithmetic", "geometric"] = "geometric",
        upper_bound_pct: float = 5.0,
        lower_bound_pct: float = 5.0,
        grid_levels: int = 10,
        profit_per_grid_pct: float = 0.5,
        total_capital: float = 10000.0,
        rebalance_threshold_pct: float = 1.0,
        strategy_id: str = "grid_v1",
        min_profit_after_fees_pct: float = 0.1,
        estimated_fee_pct: float = 0.1,
    ):
        """
        Initialize the grid strategy engine.

        Args:
            grid_type: "arithmetic" for fixed-dollar steps, "geometric" for
                       percentage-based steps.
            upper_bound_pct: Percentage ABOVE reference price for the highest
                             sell level (e.g., 5.0 = +5%).
            lower_bound_pct: Percentage BELOW reference price for the lowest
                             buy level (e.g., 5.0 = -5%).
            grid_levels: Number of levels PER SIDE. Total orders = 2 * grid_levels.
            profit_per_grid_pct: Target profit per round-trip, as percentage
                                 of the entry price (e.g., 0.5 = 0.5%).
            total_capital: Total quote currency allocated to this grid.
            rebalance_threshold_pct: If price moves more than this % from the
                                     reference, trigger a grid recomputation.
            strategy_id: Identifier for signal tagging.
            min_profit_after_fees_pct: Minimum net profit after estimated fees.
                                       If a level can't achieve this, raise error.
            estimated_fee_pct: Estimated round-trip fee (e.g., 0.1% for 2 * 0.05%).
        """
        if grid_type not in ("arithmetic", "geometric"):
            raise ValueError(f"grid_type must be 'arithmetic' or 'geometric', got '{grid_type}'")

        if grid_levels < 1:
            raise ValueError(f"grid_levels must be >= 1, got {grid_levels}")

        if upper_bound_pct <= 0 or lower_bound_pct <= 0:
            raise ValueError("bounds must be positive percentages")

        if profit_per_grid_pct <= 0:
            raise ValueError(f"profit_per_grid_pct must be positive, got {profit_per_grid_pct}")

        # Check: can we actually make money after fees?
        if profit_per_grid_pct <= estimated_fee_pct + min_profit_after_fees_pct:
            raise ValueError(
                f"profit_per_grid_pct ({profit_per_grid_pct}%) is too low: "
                f"must exceed estimated_fee_pct ({estimated_fee_pct}%) + "
                f"min_profit_after_fees_pct ({min_profit_after_fees_pct}%) = "
                f"{estimated_fee_pct + min_profit_after_fees_pct}%"
            )

        self.grid_type = GridType(grid_type)
        self.upper_bound_pct = upper_bound_pct
        self.lower_bound_pct = lower_bound_pct
        self.grid_levels = grid_levels
        self.profit_per_grid_pct = profit_per_grid_pct
        self.total_capital = total_capital
        self.rebalance_threshold_pct = rebalance_threshold_pct
        self.strategy_id = strategy_id
        self.min_profit_after_fees_pct = min_profit_after_fees_pct
        self.estimated_fee_pct = estimated_fee_pct

    # ------------------------------------------------------------------
    # Grid Computation
    # ------------------------------------------------------------------

    def compute_grid(
        self,
        reference_price: float,
        symbol: str = "BTCUSDT",
        atr: Optional[float] = None,
        atr_multiplier: float = 2.0,
    ) -> GridConfig:
        """
        Compute the complete grid layout around a reference price.

        This is the MAIN entry point. Call this when:
            - Starting a new grid
            - Rebalancing an existing grid
            - Price has moved outside the rebalance threshold

        Args:
            reference_price: The center price for the grid (typically mid-price
                            or VWAP of recent candles).
            symbol: Trading pair (e.g., "BTCUSDT").
            atr: Optional Average True Range for dynamic bound scaling.
                 If provided, bounds are expanded to max(base_pct, atr_based_pct).
            atr_multiplier: How many ATRs to use for bound calculation.

        Returns:
            A complete GridConfig ready for signal generation.

        Raises:
            ValueError: If reference_price <= 0 or bounds produce overlapping levels.
        """
        if reference_price <= 0:
            raise ValueError(f"reference_price must be positive, got {reference_price}")

        # --- Dynamic bound adjustment based on volatility ---
        upper_pct = self.upper_bound_pct
        lower_pct = self.lower_bound_pct

        if atr is not None and atr > 0:
            atr_pct = (atr * atr_multiplier / reference_price) * 100
            upper_pct = max(upper_pct, atr_pct)
            lower_pct = max(lower_pct, atr_pct)

        # --- Compute bounds ---
        upper_bound = reference_price * (1.0 + upper_pct / 100.0)
        lower_bound = reference_price * (1.0 - lower_pct / 100.0)

        # --- Compute levels ---
        if self.grid_type == GridType.GEOMETRIC:
            levels = self._compute_geometric_levels(reference_price, upper_bound, lower_bound)
        else:
            levels = self._compute_arithmetic_levels(reference_price, upper_bound, lower_bound)

        # --- Validate ---
        self._validate_grid_levels(levels, reference_price, upper_bound, lower_bound)

        # --- Generate deterministic grid ID ---
        grid_id = self._compute_grid_id(symbol, reference_price, upper_bound, lower_bound)

        return GridConfig(
            grid_id=grid_id,
            symbol=symbol,
            grid_type=self.grid_type,
            reference_price=reference_price,
            upper_bound=upper_bound,
            lower_bound=lower_bound,
            levels=levels,
            profit_per_grid_pct=self.profit_per_grid_pct,
            total_capital=self.total_capital,
            status=GridStatus.ACTIVE,
        )

    def _compute_geometric_levels(
        self,
        reference_price: float,
        upper_bound: float,
        lower_bound: float,
    ) -> List[GridLevel]:
        """
        Build geometric grid levels (constant percentage spacing).

        Geometric spacing: each level is a constant MULTIPLIER from the previous.
            Sell level i: ref_price * k_sell^i    for i = 1..N
            Buy level i:  ref_price / k_buy^i     for i = 1..N

        where:
            k_sell = (upper_bound / ref_price) ^ (1 / grid_levels)
            k_buy  = (ref_price / lower_bound) ^ (1 / grid_levels)

        This ensures that %-distance between adjacent levels is constant,
        which matches the log-normal behavior of crypto prices.
        """
        levels = []

        # Growth factors: solve for k such that k^N spans the full range
        k_sell = (upper_bound / reference_price) ** (1.0 / self.grid_levels)
        k_buy = (reference_price / lower_bound) ** (1.0 / self.grid_levels)

        # Per-level capital allocation (equal split)
        capital_per_level = self.total_capital / (2 * self.grid_levels)

        # --- Sell levels (above reference) ---
        for i in range(1, self.grid_levels + 1):
            price = reference_price * (k_sell ** i)
            quantity = capital_per_level / price
            tp_price = price * (1.0 - self.profit_per_grid_pct / 100.0)

            levels.append(GridLevel(
                level_index=i,
                side=LevelSide.SELL,
                price=round(price, 8),
                quantity=round(quantity, 8),
                take_profit_price=round(tp_price, 8),
            ))

        # --- Buy levels (below reference) ---
        for i in range(1, self.grid_levels + 1):
            price = reference_price / (k_buy ** i)
            quantity = capital_per_level / price
            tp_price = price * (1.0 + self.profit_per_grid_pct / 100.0)

            levels.append(GridLevel(
                level_index=self.grid_levels + i,  # offset to avoid collision with sell indices
                side=LevelSide.BUY,
                price=round(price, 8),
                quantity=round(quantity, 8),
                take_profit_price=round(tp_price, 8),
            ))

        return levels

    def _compute_arithmetic_levels(
        self,
        reference_price: float,
        upper_bound: float,
        lower_bound: float,
    ) -> List[GridLevel]:
        """
        Build arithmetic grid levels (constant dollar-step spacing).

        Arithmetic spacing: each level is a fixed PRICE STEP from the previous.
            Sell level i: ref_price + i * step_sell    for i = 1..N
            Buy level i:  ref_price - i * step_buy     for i = 1..N

        where:
            step_sell = (upper_bound - ref_price) / grid_levels
            step_buy  = (ref_price - lower_bound) / grid_levels
        """
        levels = []

        step_sell = (upper_bound - reference_price) / self.grid_levels
        step_buy = (reference_price - lower_bound) / self.grid_levels

        capital_per_level = self.total_capital / (2 * self.grid_levels)

        # --- Sell levels ---
        for i in range(1, self.grid_levels + 1):
            price = reference_price + i * step_sell
            quantity = capital_per_level / price
            tp_price = price * (1.0 - self.profit_per_grid_pct / 100.0)

            levels.append(GridLevel(
                level_index=i,
                side=LevelSide.SELL,
                price=round(price, 8),
                quantity=round(quantity, 8),
                take_profit_price=round(tp_price, 8),
            ))

        # --- Buy levels ---
        for i in range(1, self.grid_levels + 1):
            price = reference_price - i * step_buy
            quantity = capital_per_level / price
            tp_price = price * (1.0 + self.profit_per_grid_pct / 100.0)

            levels.append(GridLevel(
                level_index=self.grid_levels + i,
                side=LevelSide.BUY,
                price=round(price, 8),
                quantity=round(quantity, 8),
                take_profit_price=round(tp_price, 8),
            ))

        return levels

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_grid_levels(
        self,
        levels: List[GridLevel],
        reference_price: float,
        upper_bound: float,
        lower_bound: float,
    ) -> None:
        """
        Validate grid integrity before returning the config.

        Checks:
            1. No buy level price >= reference_price (would cross the center).
            2. No sell level price <= reference_price.
            3. All prices within [lower_bound, upper_bound].
            4. No buy level price >= any sell level price (overlap = guaranteed
               loss from instant cross-fill, paying fees for nothing).
            5. Take-profit prices are sensible (buy TP > entry, sell TP < entry).
            6. Expected profit per level exceeds minimum after estimated fees.
        """
        buy_levels = [lvl for lvl in levels if lvl.side == LevelSide.BUY]
        sell_levels = [lvl for lvl in levels if lvl.side == LevelSide.SELL]

        # Check 1 & 2: side consistency
        for lvl in buy_levels:
            if lvl.price >= reference_price:
                raise ValueError(
                    f"Buy level {lvl.level_index} price {lvl.price} >= "
                    f"reference {reference_price}. Buy levels must be below reference."
                )

        for lvl in sell_levels:
            if lvl.price <= reference_price:
                raise ValueError(
                    f"Sell level {lvl.level_index} price {lvl.price} <= "
                    f"reference {reference_price}. Sell levels must be above reference."
                )

        # Check 3: bounds compliance
        for lvl in levels:
            if not (lower_bound * 0.999 <= lvl.price <= upper_bound * 1.001):
                raise ValueError(
                    f"Level {lvl.level_index} price {lvl.price} outside bounds "
                    f"[{lower_bound}, {upper_bound}]"
                )

        # Check 4: CRITICAL — no buy-sell overlap
        # If max(buy prices) >= min(sell prices), those orders would cross-fill
        # instantly, resulting in a guaranteed loss equal to 2× fees + spread.
        if buy_levels and sell_levels:
            max_buy = max(lvl.price for lvl in buy_levels)
            min_sell = min(lvl.price for lvl in sell_levels)
            if max_buy >= min_sell:
                raise ValueError(
                    f"GRID OVERLAP DETECTED: highest buy ({max_buy}) >= "
                    f"lowest sell ({min_sell}). This grid would immediately "
                    f"cross-fill and lose {2 * self.estimated_fee_pct}% in fees. "
                    f"Reduce grid_levels or increase bounds."
                )

        # Check 5: Take-profit direction
        for lvl in buy_levels:
            if lvl.take_profit_price <= lvl.price:
                raise ValueError(
                    f"Buy level {lvl.level_index}: take_profit ({lvl.take_profit_price}) "
                    f"must be > entry price ({lvl.price})"
                )

        for lvl in sell_levels:
            if lvl.take_profit_price >= lvl.price:
                raise ValueError(
                    f"Sell level {lvl.level_index}: take_profit ({lvl.take_profit_price}) "
                    f"must be < entry price ({lvl.price})"
                )

        # Check 6: Minimum profit after fees
        for lvl in levels:
            gross_profit_pct = abs(lvl.take_profit_price - lvl.price) / lvl.price * 100
            net_profit_pct = gross_profit_pct - self.estimated_fee_pct
            if net_profit_pct < self.min_profit_after_fees_pct:
                raise ValueError(
                    f"Level {lvl.level_index}: net profit after fees "
                    f"({net_profit_pct:.4f}%) is below minimum "
                    f"({self.min_profit_after_fees_pct}%). "
                    f"Increase profit_per_grid_pct or reduce estimated fees."
                )

    # ------------------------------------------------------------------
    # Rebalancing Logic
    # ------------------------------------------------------------------

    def check_rebalance(self, current_price: float, grid: GridConfig) -> bool:
        """
        Determine if the grid should be rebalanced.

        A grid should rebalance when the current price has moved far enough
        from the original reference that the grid is no longer capturing
        the price action efficiently (e.g., price is now outside the grid,
        or all levels on one side have been filled).

        Args:
            current_price: Latest mid-price.
            grid: The current active grid configuration.

        Returns:
            True if the grid should be cancelled and recomputed.
        """
        # Case 1: Price moved outside the grid range entirely
        if current_price >= grid.upper_bound or current_price <= grid.lower_bound:
            return True

        # Case 2: Price deviated from reference beyond the rebalance threshold
        deviation_pct = abs(current_price - grid.reference_price) / grid.reference_price * 100
        if deviation_pct > self.rebalance_threshold_pct:
            return True

        # Case 3: All levels on one side are filled (grid is "exhausted")
        buy_filled = all(
            lvl.status == LevelStatus.FILLED
            for lvl in grid.levels
            if lvl.side == LevelSide.BUY
        )
        sell_filled = all(
            lvl.status == LevelStatus.FILLED
            for lvl in grid.levels
            if lvl.side == LevelSide.SELL
        )
        if buy_filled or sell_filled:
            return True

        return False

    def compute_rebalance_price(
        self,
        ohlcv: List[Dict[str, float]],
        window: int = 20,
    ) -> float:
        """
        Compute the optimal reference price for rebalancing.

        Uses a simple moving average of mid-prices over the recent window
        to avoid placing the new grid at an extreme price.

        In production, this could use VWAP, EMA, or volume profile POC.

        Args:
            ohlcv: List of dicts with keys 'high', 'low', 'close'.
                   Most recent last.
            window: Number of candles to average over.

        Returns:
            Recommended reference price for the new grid.
        """
        if not ohlcv:
            raise ValueError("ohlcv data is empty")

        recent = ohlcv[-window:] if len(ohlcv) > window else ohlcv
        mids = [(c["high"] + c["low"] + c["close"]) / 3.0 for c in recent]
        return sum(mids) / len(mids)

    # ------------------------------------------------------------------
    # Signal Generation
    # ------------------------------------------------------------------

    def generate_signal(self, grid: GridConfig, ttl_seconds: float = 60.0) -> StrategySignal:
        """
        Generate a START_GRID signal from a GridConfig.

        The signal carries the full grid specification in its metadata,
        enabling the execution layer to place all orders without needing
        to call back into the strategy module.

        Args:
            grid: The grid configuration to signal.
            ttl_seconds: Signal time-to-live. Grid signals use longer TTLs
                         because order placement may be rate-limited.

        Returns:
            An immutable StrategySignal ready for the event bus.
        """
        return (
            SignalBuilder(SignalAction.START_GRID, grid.symbol, score=1.0)
            .with_metadata("grid_id", grid.grid_id)
            .with_metadata("grid_type", grid.grid_type.value)
            .with_metadata("reference_price", grid.reference_price)
            .with_metadata("upper_bound", grid.upper_bound)
            .with_metadata("lower_bound", grid.lower_bound)
            .with_metadata("profit_per_grid_pct", grid.profit_per_grid_pct)
            .with_metadata("levels", [lvl.to_dict() for lvl in grid.levels])
            .with_metadata("total_capital", grid.total_capital)
            .with_strategy(self.strategy_id)
            .with_ttl(ttl_seconds)
            .build()
        )

    def generate_stop_signal(self, grid: GridConfig, reason: str = "") -> StrategySignal:
        """
        Generate a STOP_GRID signal to cancel all orders and close positions.

        Args:
            grid: The grid to stop.
            reason: Human-readable reason for stopping (for audit log).

        Returns:
            A STOP_GRID StrategySignal.
        """
        return (
            SignalBuilder(SignalAction.STOP_GRID, grid.symbol, score=1.0)
            .with_metadata("grid_id", grid.grid_id)
            .with_metadata("reason", reason)
            .with_strategy(self.strategy_id)
            .with_ttl(30.0)
            .build()
        )

    def generate_pause_signal(self, grid: GridConfig) -> StrategySignal:
        """Generate a PAUSE_GRID signal (price out of range)."""
        return (
            SignalBuilder(SignalAction.PAUSE_GRID, grid.symbol, score=0.8)
            .with_metadata("grid_id", grid.grid_id)
            .with_strategy(self.strategy_id)
            .with_ttl(30.0)
            .build()
        )

    # ------------------------------------------------------------------
    # Dynamic Bound Optimization
    # ------------------------------------------------------------------

    def optimize_bounds(
        self,
        ohlcv: List[Dict[str, float]],
        atr_period: int = 14,
        percentile_low: float = 10.0,
        percentile_high: float = 90.0,
    ) -> Tuple[float, float]:
        """
        Compute optimal grid bounds from recent price action.

        Uses price range percentiles to find natural support/resistance levels,
        then expands them by ATR to account for noise.

        Args:
            ohlcv: List of OHLCV dicts with 'high', 'low', 'close'.
                   Most recent last.
            atr_period: Period for ATR calculation.
            percentile_low: Percentile for lower bound (e.g., 10th).
            percentile_high: Percentile for upper bound (e.g., 90th).

        Returns:
            Tuple of (optimal_lower_pct, optimal_upper_pct) relative to
            the most recent close price.
        """
        if len(ohlcv) < atr_period:
            # Not enough data — use configured defaults
            return self.lower_bound_pct, self.upper_bound_pct

        closes = [c["close"] for c in ohlcv]
        current_price = closes[-1]

        # Compute ATR
        atr = self._compute_atr(ohlcv, atr_period)

        # Find natural range from percentiles
        sorted_closes = sorted(closes)
        idx_low = max(0, int(len(sorted_closes) * percentile_low / 100))
        idx_high = min(len(sorted_closes) - 1, int(len(sorted_closes) * percentile_high / 100))
        range_low = sorted_closes[idx_low]
        range_high = sorted_closes[idx_high]

        # Expand by ATR to avoid whipsaw at the edges
        atr_buffer = atr * 1.5
        lower_bound = min(range_low, current_price) - atr_buffer
        upper_bound = max(range_high, current_price) + atr_buffer

        # Convert to percentages relative to current price
        lower_pct = (current_price - lower_bound) / current_price * 100
        upper_pct = (upper_bound - current_price) / current_price * 100

        # Floor at configured minimums
        lower_pct = max(lower_pct, self.lower_bound_pct)
        upper_pct = max(upper_pct, self.upper_bound_pct)

        return round(lower_pct, 4), round(upper_pct, 4)

    @staticmethod
    def _compute_atr(ohlcv: List[Dict[str, float]], period: int = 14) -> float:
        """
        Compute Average True Range from OHLCV data.

        True Range = max(high - low, |high - prev_close|, |low - prev_close|)
        ATR = SMA(TR, period)  (simplified; Wilder's smoothing uses EMA)
        """
        if len(ohlcv) < 2:
            return 0.0

        tr_values = []
        for i in range(1, len(ohlcv)):
            high = ohlcv[i]["high"]
            low = ohlcv[i]["low"]
            prev_close = ohlcv[i - 1]["close"]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)

        if len(tr_values) < period:
            period = len(tr_values)

        return sum(tr_values[-period:]) / period

    # ------------------------------------------------------------------
    # Grid ID (Idempotency)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_grid_id(
        symbol: str, reference_price: float, upper_bound: float, lower_bound: float
    ) -> str:
        """
        Generate a deterministic grid ID.

        Same inputs → same ID. This enables the event bus to detect duplicate
        START_GRID signals after a restart and skip re-placing identical orders.
        """
        seed = f"{symbol}|{reference_price:.2f}|{upper_bound:.2f}|{lower_bound:.2f}"
        return hashlib.sha256(seed.encode()).hexdigest()[:12]

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def estimate_grid_performance(
        self, grid: GridConfig, volatility_pct: float, trades_per_day: int = 10
    ) -> Dict[str, Any]:
        """
        Estimate grid performance metrics.

        Args:
            grid: The grid configuration.
            volatility_pct: Expected daily volatility as percentage.
            trades_per_day: Estimated number of round-trips per day.

        Returns:
            Dict with estimated daily_pnl, monthly_pnl, annualized_return, etc.
        """
        avg_profit_per_trade = grid.max_potential_profit_per_cycle / len(grid.levels)
        daily_pnl = avg_profit_per_trade * trades_per_day
        monthly_pnl = daily_pnl * 30
        annualized_return = (monthly_pnl * 12) / self.total_capital * 100

        return {
            "avg_profit_per_trade": round(avg_profit_per_trade, 4),
            "estimated_daily_trades": trades_per_day,
            "estimated_daily_pnl": round(daily_pnl, 4),
            "estimated_monthly_pnl": round(monthly_pnl, 4),
            "annualized_return_pct": round(annualized_return, 2),
            "capital_utilization_pct": round(
                (grid.total_buy_notional + grid.total_sell_notional)
                / self.total_capital * 100,
                2,
            ),
            "grid_density_pct": round(grid.grid_density, 4),
            "break_even_after_trades": max(1, math.ceil(
                self.estimated_fee_pct / self.profit_per_grid_pct * 2
            )),
        }

    def __repr__(self) -> str:
        return (
            f"GridStrategy(type={self.grid_type.value}, "
            f"levels={self.grid_levels}×2, "
            f"bounds=±{self.upper_bound_pct}%/±{self.lower_bound_pct}%, "
            f"profit={self.profit_per_grid_pct}%/level, "
            f"capital={self.total_capital})"
        )
