"""
Tests for the conservative event-driven BacktestEngine:
    - Zero same-candle look-ahead (BUY on bar t cannot TP-sell on bar t)
    - Strict cash/inventory accounting conservation
    - Monotonic downtrend cannot magically print money in Spot mode
    - Multi-timeframe trend_interval (5m vs 15m) & RegimeDetector scale invariance
"""

import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, ".")

from backtest import BacktestConfig, BacktestEngine
from strategy.regime_detector import MarketRegime, RegimeDetector
from strategy.trend_strategy import TrendState, TrendStrategy


def _make_series(prices, spread_pct=0.002, start_dt=None):
    if start_dt is None:
        start_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = []
    for i, p in enumerate(prices):
        ts = (start_dt + timedelta(minutes=5 * i)).isoformat()
        bars.append({
            "open": p,
            "high": p * (1.0 + spread_pct),
            "low": p * (1.0 - spread_pct),
            "close": p,
            "volume": 1000.0,
            "timestamp": ts,
        })
    return bars


class TestBacktestEngineInvariants:
    def test_strict_accounting_conservation_on_oscillating_market(self):
        """After end-of-backtest liquidation, final_equity == initial_capital + sum(trade.pnl)."""
        prices = [0.10] * 110
        for i in range(120):
            # Oscillate between 0.095 and 0.105
            prices.append(0.10 + (0.004 if i % 4 < 2 else -0.004))
        bars = _make_series(prices, spread_pct=0.005)

        cfg = BacktestConfig(
            symbol="DOGEUSDT",
            initial_capital=100.0,
            warmup_candles=100,
            grid_levels=4,
            grid_profit_pct=1.0,
        )
        engine = BacktestEngine(cfg)
        report = engine.run(bars, strategy_mode="grid_only")

        expected_final = cfg.initial_capital + sum(t.pnl for t in engine.trades)
        assert abs(report.final_equity - expected_final) < 1e-4
        assert abs(engine.cash - report.final_equity) < 1e-4

    def test_no_same_candle_lookahead_fill(self):
        """A wide single candle touching both BUY and TP prices must NOT complete a round-trip on the same bar."""
        prices = [0.10] * 105
        bars = _make_series(prices, spread_pct=0.001)
        # Bar 105: huge wick down to 0.090 and up to 0.115 in the same 5m bar
        bars.append({
            "open": 0.10, "high": 0.115, "low": 0.090, "close": 0.10,
            "volume": 5000.0,
            "timestamp": (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=5 * 105)).isoformat(),
        })
        cfg = BacktestConfig(symbol="DOGEUSDT", initial_capital=100.0, warmup_candles=100)
        engine = BacktestEngine(cfg)
        engine.run(bars, strategy_mode="grid_only")

        # Any grid_tp trade MUST have entry_time < exit_time (never same-bar round-trip)
        tp_trades = [t for t in engine.trades if t.exit_reason == "grid_tp"]
        for t in tp_trades:
            assert t.entry_time < t.exit_time

    def test_monotonic_crash_does_not_print_money(self):
        """In a straight -40% crash, Spot grid must NOT report positive returns or negative Calmar sign bugs."""
        prices = [1.0] * 105 + [1.0 - 0.40 * (i / 100.0) for i in range(1, 101)]
        bars = _make_series(prices, spread_pct=0.002)

        cfg = BacktestConfig(symbol="DOGEUSDT", initial_capital=100.0, warmup_candles=100)
        engine = BacktestEngine(cfg)
        report = engine.run(bars, strategy_mode="grid_only")

        assert report.total_return_pct <= 0.0
        if report.max_drawdown_pct > 0:
            assert report.calmar_ratio <= 0.0

    def test_regime_detector_price_scale_invariance(self):
        """RegimeDetector must output identical scores for BTC ($100,000) and DOGE ($0.10) with same shape."""
        prices_btc = [100000.0 * (1.0 + 0.002 * i) for i in range(60)]
        bars_btc = _make_series(prices_btc, spread_pct=0.003)
        bars_doge = [
            {**b, "open": b["open"] * 1e-6, "high": b["high"] * 1e-6,
             "low": b["low"] * 1e-6, "close": b["close"] * 1e-6}
            for b in bars_btc
        ]
        det = RegimeDetector()
        res_btc = det.detect(bars_btc, current_regime=MarketRegime.RANGING)
        res_doge = det.detect(bars_doge, current_regime=MarketRegime.RANGING)
        assert abs(res_btc.score - res_doge.score) < 1e-4

    def test_trend_strategy_subcent_precision_and_spot_long_only(self):
        """DOGE ($0.08) stop-loss must keep sub-cent precision and Spot mode must block SHORT."""
        ts = TrendStrategy(allow_short=False)
        state = TrendState.flat("DOGEUSDT")

        # Bullish entry on $0.08 DOGE with ATR=0.0005 -> SL = 0.0790 (NOT rounded to 0.08!)
        sig = ts.evaluate("DOGEUSDT", 0.08, 0.0805, 0.0795, 0.0002, 0.0005, state, capital=100.0)
        assert sig is not None
        assert sig.metadata["stop_loss"] < 0.08
        assert abs(sig.metadata["stop_loss"] - 0.079) < 1e-6

        # Bearish market in Spot mode -> None (no naked short)
        sig_bear = ts.evaluate("DOGEUSDT", 0.08, 0.0790, 0.0805, -0.0002, 0.0005, state, capital=100.0)
        assert sig_bear is None
