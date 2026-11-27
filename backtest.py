"""
================================================================================
BACKTEST ENGINE — Conservative, Event-Driven Binance Spot Simulator
================================================================================

Simulates the exact production pipeline on historical data with ZERO look-ahead
bias and strict accounting conservation:
    OHLCV → Indicators (5m + resampled trend_interval) → RegimeDetector
    → GridStrategy + TrendStrategy → RiskGuard → SymbolInfo Precision
    → Conservative Order Matching (next-bar fills only) → Performance Report

KEY GUARANTEES:
    1. No same-candle look-ahead: orders placed on bar t fill no earlier than
       bar t+1; a grid BUY filled on bar t cannot TP-sell on bar t.
    2. Inventory & capital conservation: Grid lots and Trend positions are
       tracked in separate ledgers; Spot mode never sells unowned inventory.
    3. Real RiskGuard + SymbolInfo tick/lot/min_notional enforcement.
    4. Accurate signed Calmar, drawdown duration, and holding-time metrics.

USAGE:
    python backtest.py --symbol DOGEUSDT --capital 100 --trend-interval 15m
    python backtest.py --symbol BTCUSDT --capital 1000
================================================================================
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.indicators import compute_all, compute_all_series, ema as compute_ema, IndicatorBundle
from execution.exchange_client import SymbolInfo
from risk.risk_guard import RiskGuard, Verdict
from strategy.grid_strategy import GridConfig, GridStrategy
from strategy.regime_detector import (
    MacroTrend,
    MarketRegime,
    RegimeDetector,
    RegimeResult,
    classify_macro_trend,
)
from strategy.signal import SignalAction, StrategySignal
from strategy.trend_strategy import TrendDirection, TrendState, TrendStrategy


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class BacktestConfig:
    """Backtest parameters aligned with live Binance Spot settings."""
    symbol: str = "DOGEUSDT"
    market: str = "spot"         # "spot" or "futures"
    leverage: float = 1.0        # Isolated margin leverage for futures (>=1.0; ignored in spot)
    funding_rate_8h_pct: float = 0.01  # Conservative 8h funding drag % in futures
    interval: str = "5m"
    trend_interval: str = "1h"   # "5m", "15m", "1h"
    initial_capital: float = 100.0
    grid_capital_pct: float = 0.45
    trend_capital_pct: float = 0.50
    fee_pct: float = 0.10        # 0.10% per side (Binance Spot standard; use 0.05% for Futures)
    slippage_pct: float = 0.05   # 0.05% on market orders
    max_position_pct: float = 75.0
    max_exposure_pct: float = 100.0
    max_drawdown_pct: float = 15.0
    max_daily_loss_pct: float = 5.0
    warmup_candles: int = 100
    confidence_threshold_trend: float = 0.80
    lookback_candles: int = 300
    grid_upper_pct: float = 6.0
    grid_lower_pct: float = 6.0
    grid_levels: int = 5
    grid_profit_pct: float = 1.8
    grid_rebalance_pct: float = 5.0
    grid_stop_loss_pct: float = 8.0
    spot_mode: bool = True
    liquidate_grid_on_stop_loss: bool = False
    # Macro 1h trend filter (EMA50 vs EMA200)
    use_macro_filter: bool = True
    macro_ema_fast: int = 50
    macro_ema_slow: int = 200
    pause_grid_in_macro_bear: bool = True
    close_grid_on_macro_bear: bool = True
    macro_bear_exit_depth_pct: float = 6.0  # 0 = exit immediately on BEAR; >0 = only liquidate if close <= EMA200*(1-depth%)
    grid_lot_stop_loss_pct: float = 0.0  # Optional per-lot hard stop-loss % (0 = disabled)
    # Trend v2 parameters (Chandelier trailing stop + ADX/MACD quality filters)
    trend_atr_mult: float = 2.5
    trend_trail_mult: float = 4.0
    trend_risk_pct: float = 2.5
    trend_min_adx: float = 20.0
    trend_require_macd: bool = True
    trend_exit_on_ema_cross: bool = False
    trend_require_ema200_rising: bool = True
    trend_gate_by_regime: bool = False  # When False, 1h macro + ADX/MACD gate trend directly
    allow_short: bool = False    # Spot default: long-only; True in Futures
    consecutive_buy_limit: int = 5
    enforce_risk_guard: bool = True

    @property
    def effective_leverage(self) -> float:
        return max(1.0, self.leverage) if self.market == "futures" else 1.0

    @property
    def fee_decimal(self) -> float:
        return self.fee_pct / 100.0

    @property
    def slippage_decimal(self) -> float:
        return self.slippage_pct / 100.0


# ============================================================================
# Trade & Position Records
# ============================================================================

@dataclass
class Trade:
    """A single completed round-trip trade."""
    entry_time: str
    exit_time: str
    symbol: str
    side: str
    strategy: str
    entry_price: float
    exit_price: float
    quantity: float
    pnl: float
    pnl_pct: float
    fee: float
    exit_reason: str = ""


@dataclass
class EquityPoint:
    """One mark-to-market point on the equity curve."""
    timestamp: str
    equity: float
    position_value: float
    cash: float
    regime: str = ""
    regime_score: float = 0.0


@dataclass
class GridBuyOrder:
    """An open limit BUY order resting on the book."""
    price: float
    quantity: float
    tp_price: float
    armed_bar: int


@dataclass
class GridLot:
    """A filled grid BUY lot waiting for its TP SELL to execute."""
    entry_price: float
    quantity: float
    tp_price: float
    entry_fee: float
    entry_time: str
    entry_bar: int


@dataclass
class TrendPosition:
    """Active directional trend position (strictly isolated from grid lots)."""
    direction: str  # "LONG" or "SHORT"
    quantity: float
    entry_price: float
    entry_fee: float
    stop_loss: float
    trailing_stop: float
    entry_time: str
    entry_bar: int
    margin_locked: float = 0.0
    liquidation_price: float = 0.0
    funding_paid: float = 0.0


# ============================================================================
# Performance Report
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
            "final_equity": round(self.final_equity, 4),
            "total_return_pct": round(self.total_return_pct, 2),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate_pct": round(self.win_rate_pct, 2),
            "total_pnl": round(self.total_pnl, 4),
            "avg_win": round(self.avg_win, 4),
            "avg_loss": round(self.avg_loss, 4),
            "profit_factor": round(self.profit_factor, 2) if math.isfinite(self.profit_factor) else None,
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "max_drawdown_duration_hours": round(self.max_drawdown_duration_hours, 1),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "sortino_ratio": round(self.sortino_ratio, 2),
            "calmar_ratio": round(self.calmar_ratio, 2),
            "avg_trades_per_day": round(self.avg_trades_per_day, 2),
            "avg_holding_hours": round(self.avg_holding_hours, 2),
            "regime_switches": self.regime_switches,
            "pct_time_grid": round(self.pct_time_grid, 1),
            "pct_time_trend": round(self.pct_time_trend, 1),
        }

    def summary(self) -> str:
        pf_str = f"{self.profit_factor:.2f}" if math.isfinite(self.profit_factor) else "inf"
        lines = [
            f"  Return: {self.total_return_pct:+.2f}%  |  Final: ${self.final_equity:.2f}  |  Sharpe: {self.sharpe_ratio:.2f}",
            f"  Trades: {self.total_trades}  |  Win Rate: {self.win_rate_pct:.1f}%  |  Avg Hold: {self.avg_holding_hours:.1f}h",
            f"  Profit Factor: {pf_str}  |  Max DD: {self.max_drawdown_pct:.2f}% ({self.max_drawdown_duration_hours:.0f}h)",
            f"  Calmar: {self.calmar_ratio:.2f}  |  Sortino: {self.sortino_ratio:.2f}",
        ]
        if self.regime_switches > 0:
            lines.append(
                f"  Regime: {self.regime_switches} switches, "
                f"Ranging {self.pct_time_grid:.0f}% / Trending {self.pct_time_trend:.0f}%"
            )
        return "\n".join(lines)


# ============================================================================
# OHLCV Resampling Helper (Zero Look-Ahead)
# ============================================================================

def _interval_to_minutes(interval: str) -> int:
    unit = interval[-1].lower()
    val = int(interval[:-1])
    if unit == "m":
        return val
    if unit == "h":
        return val * 60
    if unit == "d":
        return val * 1440
    return 5


def resample_ohlcv(ohlcv: List[Dict[str, Any]], factor: int) -> List[Dict[str, Any]]:
    """Aggregate every `factor` consecutive bars into higher-timeframe candles."""
    if factor <= 1 or not ohlcv:
        return ohlcv
    out: List[Dict[str, Any]] = []
    n = len(ohlcv)
    for start in range(0, n, factor):
        chunk = ohlcv[start:start + factor]
        if len(chunk) < factor:
            # Include forming candle at the end for latest mark without future data
            pass
        out.append({
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(c.get("volume", 0.0) for c in chunk),
            "timestamp": chunk[-1]["timestamp"],
        })
    return out


# ============================================================================
# Backtest Engine
# ============================================================================

class BacktestEngine:
    """
    Conservative event-driven backtest engine sharing production strategy,
    regime, risk, and exchange precision components.
    """

    def __init__(self, config: BacktestConfig, symbol_info: Optional[SymbolInfo] = None):
        self.cfg = config
        self.symbol_info = symbol_info or SymbolInfo.default_for(config.symbol)
        self._resample_factor = max(
            1,
            _interval_to_minutes(config.trend_interval) // max(1, _interval_to_minutes(config.interval)),
        )

        # Strategy engines (same classes as live main.py)
        self.grid = GridStrategy(
            grid_type="geometric",
            upper_bound_pct=config.grid_upper_pct,
            lower_bound_pct=config.grid_lower_pct,
            grid_levels=config.grid_levels,
            profit_per_grid_pct=config.grid_profit_pct,
            total_capital=config.initial_capital * config.grid_capital_pct,
            rebalance_threshold_pct=config.grid_rebalance_pct,
            spot_mode=config.spot_mode,
        )
        self.grid.grid_stop_loss_pct = config.grid_stop_loss_pct

        self.trend = TrendStrategy(
            ema_fast=12,
            ema_slow=26,
            macd_signal_period=9,
            atr_period=14,
            atr_multiplier=config.trend_atr_mult,
            trailing_stop_atr_multiplier=config.trend_trail_mult,
            risk_per_trade_pct=config.trend_risk_pct,
            allow_short=config.allow_short,
            min_adx=config.trend_min_adx,
            require_macd_confirm=config.trend_require_macd,
            exit_on_ema_cross=config.trend_exit_on_ema_cross,
        )

        self.regime_detector = RegimeDetector(
            adx_threshold=25.0,
            volatility_window=20,
            volatility_percentile_low=25.0,
            volatility_percentile_high=75.0,
            lookback_periods=100,
        )

        self._reset_state()

    def _reset_state(self) -> None:
        self.cash: float = self.cfg.initial_capital
        self._last_close: float = 0.0
        self.grid_buy_orders: List[GridBuyOrder] = []
        self.grid_lots: List[GridLot] = []
        self.trend_pos: Optional[TrendPosition] = None
        self.trades: List[Trade] = []
        self.equity_curve: List[EquityPoint] = []

        self._current_regime = MarketRegime.RANGING
        self._trend_state = TrendState.flat(self.cfg.symbol)
        self._grid_config: Optional[GridConfig] = None
        self._consecutive_buys: int = 0
        self._trend_paused_grid: bool = False
        self._regime_switches: int = 0
        self._grid_retry_after_bar: int = 0
        self._current_bar_dt: datetime = datetime(2020, 1, 1, tzinfo=timezone.utc)

        lev = self.cfg.effective_leverage
        self.risk_guard = RiskGuard(
            config={
                "max_drawdown_pct": self.cfg.max_drawdown_pct,
                "max_daily_loss_pct": self.cfg.max_daily_loss_pct,
                "max_position_size_pct": self.cfg.max_position_pct * lev,
                "max_exposure_pct": self.cfg.max_exposure_pct * lev,
                "max_leverage": lev,
                "min_order_notional": float(self.symbol_info.min_notional),
            },
            equity_provider=self._mark_to_market_equity,
            positions_provider=self._risk_positions,
            trade_history_provider=lambda s: [],
            clock=lambda: self._current_bar_dt,
        )

    # ------------------------------------------------------------------
    # Accounting (Strict Conservation)
    # ------------------------------------------------------------------

    def _mark_to_market_equity(self, mark_price: Optional[float] = None) -> float:
        px = mark_price if mark_price is not None else self._last_close
        grid_val = sum(lot.quantity * px for lot in self.grid_lots)
        trend_val = 0.0
        if self.trend_pos is not None:
            if self.trend_pos.direction == "LONG":
                if self.trend_pos.margin_locked > 0:
                    unrealized = (px - self.trend_pos.entry_price) * self.trend_pos.quantity
                    trend_val = max(0.0, self.trend_pos.margin_locked + unrealized)
                else:
                    trend_val = self.trend_pos.quantity * px
            else:
                # Short: isolated margin_locked + unrealized PnL
                unrealized = (self.trend_pos.entry_price - px) * self.trend_pos.quantity
                trend_val = max(0.0, self.trend_pos.margin_locked + unrealized)
        return self.cash + grid_val + trend_val

    def _risk_positions(self) -> List[Dict[str, Any]]:
        positions: List[Dict[str, Any]] = []
        grid_qty = sum(l.quantity for l in self.grid_lots)
        if grid_qty > 0:
            avg = sum(l.quantity * l.entry_price for l in self.grid_lots) / grid_qty
            positions.append({
                "symbol": self.cfg.symbol,
                "quantity": grid_qty,
                "avg_entry_price": avg,
                "current_price": self._last_close or avg,
            })
        if self.trend_pos is not None:
            positions.append({
                "symbol": self.cfg.symbol,
                "quantity": self.trend_pos.quantity,
                "avg_entry_price": self.trend_pos.entry_price,
                "current_price": self._last_close or self.trend_pos.entry_price,
            })
        return positions

    # ------------------------------------------------------------------
    # Main Simulation Loop
    # ------------------------------------------------------------------

    def run(self, ohlcv: List[Dict[str, Any]], strategy_mode: str = "dual_engine") -> PerformanceReport:
        """
        Run backtest over historical OHLCV bars (oldest first).

        Args:
            ohlcv: List of OHLCV dicts.
            strategy_mode: "grid_only", "trend_only", or "dual_engine" (also accepts "regime_switch").
        """
        self._reset_state()
        if strategy_mode == "regime_switch":
            strategy_mode = "dual_engine"

        n = len(ohlcv)
        if n <= self.cfg.warmup_candles:
            return self._calculate_performance(strategy_mode)

        # Strictly causal O(N) indicator precomputation (index i depends ONLY on bars 0..i)
        primary_indicators = compute_all_series(self.cfg.symbol, ohlcv)
        if self._resample_factor > 1:
            htf_bars = resample_ohlcv(ohlcv, self._resample_factor)
            htf_indicators = compute_all_series(self.cfg.symbol, htf_bars)
        else:
            htf_indicators = primary_indicators

        # Causal 1h macro EMA50 / EMA200 precomputation
        macro_factor = max(1, 60 // max(1, _interval_to_minutes(self.cfg.interval)))
        bars_1h = resample_ohlcv(ohlcv, macro_factor) if macro_factor > 1 else ohlcv
        closes_1h = [b["close"] for b in bars_1h]
        macro_ef = compute_ema(closes_1h, self.cfg.macro_ema_fast)
        macro_es = compute_ema(closes_1h, self.cfg.macro_ema_slow)

        for i in range(self.cfg.warmup_candles, n):
            bar = ohlcv[i]
            self._last_close = bar["close"]
            self._current_bar_dt = datetime.fromisoformat(bar["timestamp"])

            # 1. Process resting orders & intrabar stops on bar i (orders armed on bar < i)
            self._match_grid_orders(bar, bar_idx=i, mode=strategy_mode)
            self._check_trend_intrabar_stop(bar, bar_idx=i, mode=strategy_mode)

            # Update TrendState peak/trough price intrabar for Chandelier stop accuracy
            if self.trend_pos is not None:
                if self.trend_pos.direction == "LONG":
                    if bar["high"] > self._trend_state.highest_price:
                        self._trend_state.highest_price = bar["high"]
                else:
                    if self._trend_state.lowest_price <= 0 or bar["low"] < self._trend_state.lowest_price:
                        self._trend_state.lowest_price = bar["low"]

                # Conservative 8h funding rate deduction in Futures mode (every 96 5m bars)
                if (
                    self.cfg.market == "futures"
                    and self.cfg.funding_rate_8h_pct > 0
                    and i > self.trend_pos.entry_bar
                    and (i - self.trend_pos.entry_bar) % 96 == 0
                ):
                    notional = self.trend_pos.quantity * bar["close"]
                    funding = notional * (self.cfg.funding_rate_8h_pct / 100.0)
                    self.cash -= funding
                    self.trend_pos.funding_paid += funding

            # 2. Fetch causal indicators at bar i (zero future data)
            indicators = primary_indicators[i]
            if indicators.atr <= 0 or indicators.close <= 0:
                self._record_equity(bar, self._current_regime, 0.5)
                continue

            # Small slice for RegimeDetector volatility percentile & 3-bar microstructure
            reg_win = ohlcv[max(0, i - 25):i + 1]

            # 3. Regime detection
            if strategy_mode == "grid_only":
                regime_result = RegimeResult(
                    regime=MarketRegime.RANGING, score=0.0,
                    factor_scores=None, confidence=1.0, switched=False,
                )
            elif strategy_mode == "trend_only":
                regime_result = RegimeResult(
                    regime=MarketRegime.TRENDING, score=1.0,
                    factor_scores=None, confidence=1.0, switched=False,
                )
            else:
                regime_result = self.regime_detector.detect(
                    ohlcv=reg_win,
                    current_regime=self._current_regime,
                    adx=indicators.adx,
                    plus_di=indicators.plus_di,
                    minus_di=indicators.minus_di,
                    ema_fast=indicators.ema_fast,
                    ema_slow=indicators.ema_slow,
                    macd_hist=indicators.macd_hist,
                    atr=indicators.atr,
                    volume=indicators.volume,
                )
                if regime_result.switched:
                    self._regime_switches += 1
                self._current_regime = regime_result.regime

            # Strictly causal HTF indicator: only use last COMPLETED higher-timeframe bar
            if self._resample_factor > 1:
                htf_idx = (i // self._resample_factor) - (0 if (i + 1) % self._resample_factor == 0 else 1)
                t_ind = htf_indicators[htf_idx] if htf_idx >= 26 else None
            else:
                t_ind = indicators

            # Strictly causal 1h macro trend classification
            self._cur_macro_es: float = 0.0
            self._cur_macro_es_rising: bool = True
            self._cur_macro_es_falling: bool = True
            if self.cfg.use_macro_filter:
                m_idx = (i // macro_factor) - (0 if (i + 1) % macro_factor == 0 else 1)
                if m_idx >= self.cfg.macro_ema_fast:
                    self._cur_macro_es = macro_es[m_idx]
                    if m_idx >= 24:
                        self._cur_macro_es_rising = macro_es[m_idx] > macro_es[m_idx - 24]
                        self._cur_macro_es_falling = macro_es[m_idx] < macro_es[m_idx - 24]
                    macro_trend = classify_macro_trend(
                        bar["close"], macro_ef[m_idx], macro_es[m_idx]
                    )
                else:
                    macro_trend = MacroTrend.NEUTRAL
            else:
                macro_trend = MacroTrend.NEUTRAL

            # 4. Evaluate strategies at bar i close (new orders rest for bar i+1+)
            self._process_bar_strategies(
                bar=bar,
                bar_idx=i,
                window=reg_win,
                indicators=indicators,
                trend_indicators=t_ind,
                regime=regime_result,
                macro_trend=macro_trend,
                strategy_mode=strategy_mode,
            )

            # 5. Mark-to-market equity
            self._record_equity(bar, regime_result.regime, regime_result.score)

        # End-of-backtest liquidation at final close
        final_bar = ohlcv[-1]
        self._last_close = final_bar["close"]
        self._liquidate_all(final_bar["close"], final_bar["timestamp"], "end_of_backtest", strategy_mode)
        if self.equity_curve:
            eq = self._mark_to_market_equity(final_bar["close"])
            self.equity_curve[-1].equity = eq
            self.equity_curve[-1].cash = self.cash
            self.equity_curve[-1].position_value = 0.0

        return self._calculate_performance(strategy_mode)

    # ------------------------------------------------------------------
    # Conservative Order Matching (Next-Bar Fills Only)
    # ------------------------------------------------------------------

    def _match_grid_orders(self, bar: Dict[str, Any], bar_idx: int, mode: str) -> None:
        """
        Match resting grid orders conservatively:
          1) First check TP SELLs on lots entered on bar < bar_idx.
          2) Then check resting BUY orders armed on bar < bar_idx.
        A lot bought on bar_idx CANNOT sell on the same bar_idx.
        """
        high, low = bar["high"], bar["low"]
        ts = bar["timestamp"]

        # Step 1: Check TP sells & optional per-lot stop-loss on existing lots (entered on earlier bars)
        remaining_lots: List[GridLot] = []
        new_rebuys: List[GridBuyOrder] = []
        lot_sl_mult = (1.0 - self.cfg.grid_lot_stop_loss_pct / 100.0) if self.cfg.grid_lot_stop_loss_pct > 0 else 0.0
        for lot in self.grid_lots:
            if lot.entry_bar < bar_idx and lot_sl_mult > 0 and low <= lot.entry_price * lot_sl_mult:
                sl_px = min(bar["open"], lot.entry_price * lot_sl_mult) * (1.0 - self.cfg.slippage_decimal)
                revenue = sl_px * lot.quantity
                exit_fee = revenue * self.cfg.fee_decimal
                self.cash += revenue - exit_fee
                pnl = (sl_px - lot.entry_price) * lot.quantity - lot.entry_fee - exit_fee
                pnl_pct = (pnl / (lot.entry_price * lot.quantity)) * 100.0 if lot.entry_price > 0 else 0.0
                self.trades.append(Trade(
                    entry_time=lot.entry_time,
                    exit_time=ts,
                    symbol=self.cfg.symbol,
                    side="SELL",
                    strategy=f"{mode}:grid",
                    entry_price=lot.entry_price,
                    exit_price=sl_px,
                    quantity=lot.quantity,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    fee=lot.entry_fee + exit_fee,
                    exit_reason="grid_lot_sl",
                ))
                continue
            if lot.entry_bar < bar_idx and high >= lot.tp_price:
                tp_px = lot.tp_price
                revenue = tp_px * lot.quantity
                exit_fee = revenue * self.cfg.fee_decimal
                self.cash += revenue - exit_fee
                pnl = (tp_px - lot.entry_price) * lot.quantity - lot.entry_fee - exit_fee
                pnl_pct = (pnl / (lot.entry_price * lot.quantity)) * 100.0 if lot.entry_price > 0 else 0.0
                self.trades.append(Trade(
                    entry_time=lot.entry_time,
                    exit_time=ts,
                    symbol=self.cfg.symbol,
                    side="SELL",
                    strategy=f"{mode}:grid",
                    entry_price=lot.entry_price,
                    exit_price=tp_px,
                    quantity=lot.quantity,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    fee=lot.entry_fee + exit_fee,
                    exit_reason="grid_tp",
                ))
                self._consecutive_buys = 0
                # Re-arm BUY at original entry price for bar_idx+1+ (grid cycling)
                new_rebuys.append(GridBuyOrder(
                    price=lot.entry_price,
                    quantity=lot.quantity,
                    tp_price=lot.tp_price,
                    armed_bar=bar_idx,
                ))
            else:
                remaining_lots.append(lot)
        self.grid_lots = remaining_lots

        # Step 2: Check resting BUY orders (armed on earlier bars)
        remaining_buys: List[GridBuyOrder] = []
        for order in self.grid_buy_orders:
            if order.armed_bar < bar_idx and low <= order.price <= high:
                cost = order.price * order.quantity
                fee = cost * self.cfg.fee_decimal
                if self.cash >= cost + fee:
                    self.cash -= cost + fee
                    self.grid_lots.append(GridLot(
                        entry_price=order.price,
                        quantity=order.quantity,
                        tp_price=order.tp_price,
                        entry_fee=fee,
                        entry_time=ts,
                        entry_bar=bar_idx,
                    ))
                    self._consecutive_buys += 1
                    continue
            remaining_buys.append(order)

        self.grid_buy_orders = remaining_buys + new_rebuys

    def _check_trend_intrabar_stop(self, bar: Dict[str, Any], bar_idx: int, mode: str) -> None:
        """Check if exchange-side stop-loss / trailing stop was hit intrabar."""
        if self.trend_pos is None or self.trend_pos.entry_bar >= bar_idx:
            return
        pos = self.trend_pos
        if pos.direction == "LONG":
            eff_stop = max(pos.stop_loss, pos.trailing_stop)
            if eff_stop > 0 and bar["low"] <= eff_stop:
                exit_px = min(bar["open"], eff_stop)  # gap-down conservative fill
                reason = "trailing_stop_hit" if pos.trailing_stop > pos.stop_loss else "stop_loss_hit"
                self._close_trend_position(exit_px, bar["timestamp"], reason, mode)
        else:
            candidates = [p for p in (pos.stop_loss, pos.trailing_stop) if p > 0]
            eff_stop = min(candidates) if candidates else 0.0
            if eff_stop > 0 and bar["high"] >= eff_stop:
                exit_px = max(bar["open"], eff_stop)
                reason = "trailing_stop_hit" if (
                    pos.trailing_stop > 0 and pos.trailing_stop < pos.stop_loss
                ) else "stop_loss_hit"
                self._close_trend_position(exit_px, bar["timestamp"], reason, mode)

    # ------------------------------------------------------------------
    # Strategy Evaluation (Mirrors main.py)
    # ------------------------------------------------------------------

    def _process_bar_strategies(
        self,
        bar: Dict[str, Any],
        bar_idx: int,
        window: List[Dict[str, Any]],
        indicators: IndicatorBundle,
        trend_indicators: Optional[IndicatorBundle],
        regime: RegimeResult,
        macro_trend: MacroTrend,
        strategy_mode: str,
    ) -> None:
        live_eq = self._mark_to_market_equity(bar["close"])
        grid_inv = sum(l.quantity * bar["close"] for l in self.grid_lots)
        grid_budget_pct = 1.0 if strategy_mode == "grid_only" else self.cfg.grid_capital_pct
        avail_grid_cap = max(0.0, live_eq * grid_budget_pct - grid_inv)
        self.grid.total_capital = avail_grid_cap

        # ---- GRID ----
        if strategy_mode in ("grid_only", "dual_engine"):
            self._eval_grid(bar, bar_idx, window, indicators, regime, macro_trend, strategy_mode)

        # ---- TREND ----
        if strategy_mode in ("trend_only", "dual_engine"):
            self._eval_trend(bar, bar_idx, trend_indicators, regime, macro_trend, strategy_mode, live_eq)

    def _eval_grid(
        self,
        bar: Dict[str, Any],
        bar_idx: int,
        window: List[Dict[str, Any]],
        indicators: IndicatorBundle,
        regime: RegimeResult,
        macro_trend: MacroTrend,
        mode: str,
    ) -> None:
        close = bar["close"]

        # Macro bear filter: cancel resting BUY orders & optionally close underwater bags
        if self.cfg.use_macro_filter and self.cfg.pause_grid_in_macro_bear and macro_trend == MacroTrend.BEAR:
            if self.grid_buy_orders:
                self.grid_buy_orders.clear()
            if self.cfg.close_grid_on_macro_bear and self.grid_lots:
                depth = self.cfg.macro_bear_exit_depth_pct
                should_liq = (
                    depth <= 0.0
                    or (self._cur_macro_es > 0 and close <= self._cur_macro_es * (1.0 - depth / 100.0))
                )
                if should_liq:
                    self._liquidate_grid_lots(close, bar["timestamp"], "macro_bear_exit", mode)
            self._grid_config = None
            return

        # Grid stop-loss check
        if self._grid_config is not None and self.grid.check_stop_loss(close, self._grid_config):
            self.grid_buy_orders.clear()
            if self.cfg.liquidate_grid_on_stop_loss:
                self._liquidate_grid_lots(close, bar["timestamp"], "grid_stop_loss", mode)
            self._grid_config = None
            self._grid_retry_after_bar = bar_idx + 24  # 2h cooldown after stop-loss
            return

        strong_trend = (
            mode == "dual_engine"
            and regime.is_trending
            and regime.confidence > self.cfg.confidence_threshold_trend
        )
        cb_triggered = self._consecutive_buys >= self.cfg.consecutive_buy_limit

        if strong_trend and not self._trend_paused_grid and self._grid_config is not None:
            self._trend_paused_grid = True
        elif not strong_trend and self._trend_paused_grid:
            self._trend_paused_grid = False
            self._grid_config = None

        if (strong_trend or cb_triggered) and self._grid_config is not None:
            return

        if bar_idx < self._grid_retry_after_bar:
            return

        # Don't attempt deploy if remaining grid cash is below min_notional
        min_notional = float(self.symbol_info.min_notional)
        if self.grid.total_capital < min_notional * 1.15:
            return

        needs_deploy = self._grid_config is None
        needs_rebalance = (
            self._grid_config is not None
            and (self.grid.check_rebalance(close, self._grid_config) or bar_idx % 144 == 0)
        )

        if needs_deploy or needs_rebalance:
            ref_price = self.grid.compute_rebalance_price(window)
            new_grid = self.grid.compute_grid(
                reference_price=ref_price,
                symbol=self.cfg.symbol,
                atr=indicators.atr,
                is_ranging=regime.is_ranging,
                min_notional=min_notional,
            )
            signal = self.grid.generate_signal(new_grid)
            meta = dict(signal.metadata)

            if self.cfg.enforce_risk_guard:
                verdict = self.risk_guard.check_signal(signal)
                if verdict.is_blocked:
                    self._grid_retry_after_bar = bar_idx + 12  # wait 1h before retrying
                    return
                if verdict.verdict == Verdict.MODIFIED and verdict.modified_params:
                    meta.update(verdict.modified_params)

            # Cancel unfilled BUY orders (preserve filled grid_lots waiting for TP)
            self.grid_buy_orders = []
            for lvl in meta.get("levels", []):
                if lvl.get("side") != "BUY":
                    continue
                px = self.symbol_info.round_price(lvl["price"])
                qty = self.symbol_info.floor_qty(lvl["quantity"])
                tp = self.symbol_info.round_price(lvl["take_profit_price"])
                if self.symbol_info.is_valid_order(px, qty) and tp > px:
                    self.grid_buy_orders.append(
                        GridBuyOrder(price=px, quantity=qty, tp_price=tp, armed_bar=bar_idx)
                    )
            self._grid_config = new_grid

    def _eval_trend(
        self,
        bar: Dict[str, Any],
        bar_idx: int,
        t_ind: Optional[IndicatorBundle],
        regime: RegimeResult,
        macro_trend: MacroTrend,
        mode: str,
        live_eq: float,
    ) -> None:
        if t_ind is None or t_ind.atr <= 0 or t_ind.close <= 0:
            return

        # Active trend positions are ALWAYS monitored for exits/trailing stops!
        if self._trend_state.is_active:
            should_eval = True
        elif self.cfg.trend_gate_by_regime and mode == "dual_engine":
            should_eval = regime.is_trending and regime.confidence >= self.cfg.confidence_threshold_trend
        else:
            should_eval = True

        if not should_eval:
            return

        macro_bullish: Optional[bool] = None
        macro_bearish: Optional[bool] = None
        if self.cfg.use_macro_filter:
            macro_bullish = (macro_trend == MacroTrend.BULL)
            macro_bearish = (macro_trend == MacroTrend.BEAR)
            if self.cfg.trend_require_ema200_rising:
                if not self._cur_macro_es_rising:
                    macro_bullish = False
                if not self._cur_macro_es_falling:
                    macro_bearish = False

        lev = self.cfg.effective_leverage
        trend_cap = live_eq * (1.0 if mode == "trend_only" else self.cfg.trend_capital_pct)
        sig = self.trend.evaluate(
            symbol=self.cfg.symbol,
            current_price=bar["close"],
            ema_fast_val=t_ind.ema_fast,
            ema_slow_val=t_ind.ema_slow,
            macd_hist=t_ind.macd_hist,
            atr=t_ind.atr,
            current_state=self._trend_state,
            capital=trend_cap * lev,
            adx=t_ind.adx,
            macro_bullish=macro_bullish,
            macro_bearish=macro_bearish,
        )
        if sig is None:
            return

        meta = dict(sig.metadata)
        if self.cfg.enforce_risk_guard:
            verdict = self.risk_guard.check_signal(sig)
            if verdict.is_blocked:
                return
            if verdict.verdict == Verdict.MODIFIED and verdict.modified_params:
                meta.update(verdict.modified_params)

        if sig.action == SignalAction.START_TREND and not self._trend_state.is_active:
            direction = meta["direction"]
            # Cap trend notional to trend_cap * leverage so it stays within its margin bucket
            px = bar["close"]
            max_bucket_qty = (trend_cap * lev) / (px * (1.0 + self.cfg.fee_decimal * lev + self.cfg.slippage_decimal))
            qty = self.symbol_info.floor_qty(min(meta.get("position_size", 0.0), max_bucket_qty))
            if self.symbol_info.is_valid_order(px, qty):
                opened = self._open_trend_position(
                    direction=direction,
                    price=px,
                    qty=qty,
                    stop_loss=meta.get("stop_loss", 0.0),
                    trailing_stop=meta.get("trailing_stop", 0.0),
                    timestamp=bar["timestamp"],
                    bar_idx=bar_idx,
                )
                if opened:
                    self._trend_state = self.trend.update_state(
                        self._trend_state, sig, px, bar["timestamp"]
                    )
                    self._trend_state.position_size = self.trend_pos.quantity

        elif sig.action == SignalAction.MODIFY_POSITION and self.trend_pos is not None:
            new_trail = meta.get("new_trailing_stop", 0.0)
            if new_trail > 0:
                self.trend_pos.trailing_stop = new_trail
                self._trend_state = self.trend.update_state(
                    self._trend_state, sig, bar["close"], bar["timestamp"]
                )

        elif sig.action == SignalAction.STOP_TREND and self.trend_pos is not None:
            self._close_trend_position(
                bar["close"], bar["timestamp"], meta.get("exit_reason", "signal"), mode
            )

    # ------------------------------------------------------------------
    # Trend Position Execution (Proper Cash & Margin Accounting)
    # ------------------------------------------------------------------

    def _open_trend_position(
        self,
        direction: str,
        price: float,
        qty: float,
        stop_loss: float,
        trailing_stop: float,
        timestamp: str,
        bar_idx: int,
    ) -> bool:
        if qty <= 0 or price <= 0:
            return False

        is_futures = (self.cfg.market == "futures")
        lev = self.cfg.effective_leverage

        if direction == "LONG":
            fill_px = price * (1.0 + self.cfg.slippage_decimal)
            # Required cash per unit = (fill_px / lev) + fill_px * fee
            cash_per_unit = fill_px * ((1.0 / lev) + self.cfg.fee_decimal)
            max_affordable = self.cash / cash_per_unit if cash_per_unit > 0 else 0.0
            qty = self.symbol_info.floor_qty(min(qty, max_affordable))
            if not self.symbol_info.is_valid_order(fill_px, qty):
                return False
            notional = fill_px * qty
            fee = notional * self.cfg.fee_decimal
            margin = (notional / lev) if is_futures else 0.0
            liq_px = (fill_px * (1.0 - 0.90 / lev)) if (is_futures and lev > 1.0) else 0.0
            self.cash -= (margin if is_futures else notional) + fee
            self.trend_pos = TrendPosition(
                direction="LONG",
                quantity=qty,
                entry_price=fill_px,
                entry_fee=fee,
                stop_loss=stop_loss,
                trailing_stop=trailing_stop,
                entry_time=timestamp,
                entry_bar=bar_idx,
                margin_locked=margin,
                liquidation_price=liq_px,
            )
            return True

        elif direction == "SHORT" and self.cfg.allow_short:
            fill_px = price * (1.0 - self.cfg.slippage_decimal)
            cash_per_unit = fill_px * ((1.0 / lev) + self.cfg.fee_decimal)
            max_affordable = self.cash / cash_per_unit if cash_per_unit > 0 else 0.0
            qty = self.symbol_info.floor_qty(min(qty, max_affordable))
            if not self.symbol_info.is_valid_order(fill_px, qty):
                return False
            notional = fill_px * qty
            fee = notional * self.cfg.fee_decimal
            margin = notional / lev
            liq_px = (fill_px * (1.0 + 0.90 / lev)) if lev > 1.0 else (fill_px * 1.90)
            self.cash -= margin + fee
            self.trend_pos = TrendPosition(
                direction="SHORT",
                quantity=qty,
                entry_price=fill_px,
                entry_fee=fee,
                stop_loss=stop_loss,
                trailing_stop=trailing_stop,
                entry_time=timestamp,
                entry_bar=bar_idx,
                margin_locked=margin,
                liquidation_price=liq_px,
            )
            return True

        return False

    def _close_trend_position(self, price: float, timestamp: str, reason: str, mode: str) -> None:
        if self.trend_pos is None:
            return
        pos = self.trend_pos
        if pos.direction == "LONG":
            fill_px = price * (1.0 - self.cfg.slippage_decimal)
            revenue = fill_px * pos.quantity
            exit_fee = revenue * self.cfg.fee_decimal
            gross_pnl = (fill_px - pos.entry_price) * pos.quantity
            if pos.margin_locked > 0:
                returned = max(0.0, pos.margin_locked + gross_pnl - exit_fee)
                self.cash += returned
                pnl = (returned - pos.margin_locked) - pos.entry_fee - pos.funding_paid
            else:
                self.cash += revenue - exit_fee
                pnl = gross_pnl - pos.entry_fee - exit_fee - pos.funding_paid
            pnl_pct = (pnl / (pos.margin_locked or (pos.entry_price * pos.quantity))) * 100.0 if pos.entry_price > 0 else 0.0
            trade_side = "SELL"
        else:
            fill_px = price * (1.0 + self.cfg.slippage_decimal)
            buyback = fill_px * pos.quantity
            exit_fee = buyback * self.cfg.fee_decimal
            gross_pnl = (pos.entry_price - fill_px) * pos.quantity
            returned = max(0.0, pos.margin_locked + gross_pnl - exit_fee)
            self.cash += returned
            pnl = (returned - pos.margin_locked) - pos.entry_fee - pos.funding_paid
            pnl_pct = (pnl / (pos.margin_locked or (pos.entry_price * pos.quantity))) * 100.0 if pos.entry_price > 0 else 0.0
            trade_side = "BUY"

        self.trades.append(Trade(
            entry_time=pos.entry_time,
            exit_time=timestamp,
            symbol=self.cfg.symbol,
            side=trade_side,
            strategy=f"{mode}:trend",
            entry_price=pos.entry_price,
            exit_price=fill_px,
            quantity=pos.quantity,
            pnl=pnl,
            pnl_pct=pnl_pct,
            fee=pos.entry_fee + exit_fee + pos.funding_paid,
            exit_reason=reason,
        ))
        self.risk_guard.register_trade_result(pnl)
        self.trend_pos = None
        self._trend_state = TrendState.flat(self.cfg.symbol)

    def _liquidate_grid_lots(self, price: float, timestamp: str, reason: str, mode: str) -> None:
        fill_px = price * (1.0 - self.cfg.slippage_decimal)
        for lot in self.grid_lots:
            revenue = fill_px * lot.quantity
            exit_fee = revenue * self.cfg.fee_decimal
            self.cash += revenue - exit_fee
            pnl = (fill_px - lot.entry_price) * lot.quantity - lot.entry_fee - exit_fee
            pnl_pct = (pnl / (lot.entry_price * lot.quantity)) * 100.0 if lot.entry_price > 0 else 0.0
            self.trades.append(Trade(
                entry_time=lot.entry_time,
                exit_time=timestamp,
                symbol=self.cfg.symbol,
                side="SELL",
                strategy=f"{mode}:grid",
                entry_price=lot.entry_price,
                exit_price=fill_px,
                quantity=lot.quantity,
                pnl=pnl,
                pnl_pct=pnl_pct,
                fee=lot.entry_fee + exit_fee,
                exit_reason=reason,
            ))
        self.grid_lots.clear()

    def _liquidate_all(self, price: float, timestamp: str, reason: str, mode: str) -> None:
        self.grid_buy_orders.clear()
        self._liquidate_grid_lots(price, timestamp, reason, mode)
        self._close_trend_position(price, timestamp, reason, mode)

    def _record_equity(self, bar: Dict[str, Any], regime: MarketRegime, score: float) -> None:
        eq = self._mark_to_market_equity(bar["close"])
        self.equity_curve.append(EquityPoint(
            timestamp=bar["timestamp"],
            equity=eq,
            position_value=eq - self.cash,
            cash=self.cash,
            regime=regime.value,
            regime_score=score,
        ))

    # ------------------------------------------------------------------
    # Performance Metrics (Corrected)
    # ------------------------------------------------------------------

    def _calculate_performance(self, strategy_name: str) -> PerformanceReport:
        initial = self.cfg.initial_capital
        final = self.equity_curve[-1].equity if self.equity_curve else initial
        total_return = (final / initial - 1.0) * 100.0

        wins = [t for t in self.trades if t.pnl > 0]
        losses = [t for t in self.trades if t.pnl <= 0]
        total_pnl = sum(t.pnl for t in self.trades)
        win_rate = len(wins) / len(self.trades) * 100.0 if self.trades else 0.0
        avg_win = sum(t.pnl for t in wins) / len(wins) if wins else 0.0
        avg_loss = sum(t.pnl for t in losses) / len(losses) if losses else 0.0
        gross_profit = sum(t.pnl for t in wins)
        gross_loss = abs(sum(t.pnl for t in losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

        # Bar-to-bar returns for Sharpe & Sortino
        periods_per_year = (365 * 24 * 60) / max(1, _interval_to_minutes(self.cfg.interval))
        returns = []
        for i in range(1, len(self.equity_curve)):
            prev_eq = self.equity_curve[i - 1].equity
            if prev_eq > 0:
                returns.append((self.equity_curve[i].equity / prev_eq) - 1.0)

        if len(returns) > 1:
            mean_ret = sum(returns) / len(returns)
            var = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
            std_ret = var ** 0.5
            sharpe = (mean_ret / std_ret * (periods_per_year ** 0.5)) if std_ret > 0 else 0.0

            downside = [r for r in returns if r < 0]
            if len(downside) > 1:
                down_var = sum(r ** 2 for r in downside) / len(downside)
                std_down = down_var ** 0.5
                sortino = (mean_ret / std_down * (periods_per_year ** 0.5)) if std_down > 0 else 0.0
            else:
                sortino = 0.0
        else:
            sharpe, sortino = 0.0, 0.0

        # Max drawdown & duration (hours)
        peak = initial
        max_dd = 0.0
        dd_start_ts: Optional[datetime] = None
        max_dd_duration_hours = 0.0
        for pt in self.equity_curve:
            pt_ts = datetime.fromisoformat(pt.timestamp)
            if pt.equity >= peak:
                peak = pt.equity
                dd_start_ts = None
            else:
                dd = (peak - pt.equity) / peak * 100.0
                if dd > max_dd:
                    max_dd = dd
                if dd_start_ts is None:
                    dd_start_ts = pt_ts
                dur_h = (pt_ts - dd_start_ts).total_seconds() / 3600.0
                if dur_h > max_dd_duration_hours:
                    max_dd_duration_hours = dur_h

        # Signed Calmar ratio (negative return -> negative Calmar!)
        calmar = (total_return / max_dd) if max_dd > 0 else 0.0

        # Trade frequency & average holding hours
        avg_trades_per_day = 0.0
        avg_holding_hours = 0.0
        if self.equity_curve and len(self.equity_curve) >= 2:
            first_ts = datetime.fromisoformat(self.equity_curve[0].timestamp)
            last_ts = datetime.fromisoformat(self.equity_curve[-1].timestamp)
            days = (last_ts - first_ts).total_seconds() / 86400.0
            if days > 0:
                avg_trades_per_day = len(self.trades) / days

        hold_hours = []
        for t in self.trades:
            if t.entry_time and t.exit_time:
                try:
                    dt = (
                        datetime.fromisoformat(t.exit_time) - datetime.fromisoformat(t.entry_time)
                    ).total_seconds() / 3600.0
                    hold_hours.append(max(0.0, dt))
                except Exception:
                    pass
        if hold_hours:
            avg_holding_hours = sum(hold_hours) / len(hold_hours)

        grid_ticks = sum(1 for pt in self.equity_curve if pt.regime == "RANGING")
        trend_ticks = sum(1 for pt in self.equity_curve if pt.regime == "TRENDING")
        total_ticks = len(self.equity_curve)
        pct_grid = grid_ticks / total_ticks * 100.0 if total_ticks > 0 else 0.0
        pct_trend = trend_ticks / total_ticks * 100.0 if total_ticks > 0 else 0.0

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
            max_drawdown_duration_hours=max_dd_duration_hours,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            avg_trades_per_day=avg_trades_per_day,
            avg_holding_hours=avg_holding_hours,
            regime_switches=self._regime_switches,
            pct_time_grid=pct_grid,
            pct_time_trend=pct_trend,
        )


# ============================================================================
# CLI Runner
# ============================================================================

def load_ohlcv(filepath: str) -> List[Dict[str, Any]]:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def run_comparison(
    symbols: List[str],
    capital: float = 100.0,
    trend_interval: str = "15m",
    data_file: str = "data/sampled_10k.json",
) -> Dict[str, Dict[str, PerformanceReport]]:
    strategies = ["grid_only", "trend_only", "dual_engine"]
    all_reports: Dict[str, Dict[str, PerformanceReport]] = {}

    all_ohlcv: Dict[str, List[Dict[str, Any]]] = {}
    for sym in symbols:
        fp = f"data/{sym}_5m.json"
        if os.path.exists(fp):
            all_ohlcv[sym] = load_ohlcv(fp)
    if not all_ohlcv and os.path.exists(data_file):
        with open(data_file, "r", encoding="utf-8") as f:
            all_ohlcv = json.load(f)

    for symbol in symbols:
        if symbol not in all_ohlcv:
            print(f"  Data not found for {symbol}, skipping")
            continue

        ohlcv = all_ohlcv[symbol]
        pct_change = (ohlcv[-1]["close"] / ohlcv[0]["close"] - 1.0) * 100.0
        print(f"\n{'='*70}")
        print(f"  {symbol} | {len(ohlcv)} bars | Capital: ${capital:.2f} | Trend TF: {trend_interval}")
        print(f"  Range: {ohlcv[0]['timestamp'][:10]} → {ohlcv[-1]['timestamp'][:10]} | B&H: {pct_change:+.2f}%")
        print(f"{'='*70}")

        symbol_reports: Dict[str, PerformanceReport] = {}
        for strategy in strategies:
            cfg = BacktestConfig(
                symbol=symbol,
                initial_capital=capital,
                trend_interval=trend_interval,
            )
            engine = BacktestEngine(cfg)
            report = engine.run(ohlcv, strategy_mode=strategy)
            symbol_reports[strategy] = report
            print(f"\n  [{strategy.upper()}]")
            print(report.summary())

        all_reports[symbol] = symbol_reports

    return all_reports


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenQuant Conservative Spot Backtest Engine")
    parser.add_argument("--symbol", type=str, default="DOGEUSDT", help="Symbol to backtest")
    parser.add_argument("--capital", type=float, default=100.0, help="Initial capital in USDT")
    parser.add_argument("--trend-interval", type=str, default="15m", help="Trend timeframe (5m, 15m, 1h)")
    args = parser.parse_args()

    run_comparison([args.symbol], capital=args.capital, trend_interval=args.trend_interval)
