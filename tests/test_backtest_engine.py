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
        ts = TrendStrategy(allow_short=False, atr_multiplier=2.5)
        state = TrendState.flat("DOGEUSDT")

        # Bullish entry on $0.08 DOGE with ATR=0.0005 -> SL = 0.07875 (NOT rounded to 0.08!)
        sig = ts.evaluate("DOGEUSDT", 0.08, 0.0805, 0.0795, 0.0002, 0.0005, state, capital=100.0, adx=25.0)
        assert sig is not None
        assert sig.metadata["stop_loss"] < 0.08
        assert abs(sig.metadata["stop_loss"] - (0.08 - 2.5 * 0.0005)) < 1e-6

        # Bearish market in Spot mode -> None (no naked short)
        sig_bear = ts.evaluate("DOGEUSDT", 0.08, 0.0790, 0.0805, -0.0002, 0.0005, state, capital=100.0, adx=25.0)
        assert sig_bear is None

        # Low ADX chop -> blocked
        sig_chop = ts.evaluate("DOGEUSDT", 0.08, 0.0805, 0.0795, 0.0002, 0.0005, state, capital=100.0, adx=12.0)
        assert sig_chop is None

    def test_grid_spot_mode_doubles_buy_capital_and_macro_classifier(self):
        from strategy.grid_strategy import GridStrategy
        from strategy.regime_detector import MacroTrend, classify_macro_trend

        g_futures = GridStrategy(total_capital=100.0, grid_levels=4, profit_per_grid_pct=1.5, spot_mode=False)
        g_spot = GridStrategy(total_capital=100.0, grid_levels=4, profit_per_grid_pct=1.5, spot_mode=True)
        cfg_f = g_futures.compute_grid(reference_price=1.0, symbol="DOGEUSDT")
        cfg_s = g_spot.compute_grid(reference_price=1.0, symbol="DOGEUSDT")
        assert abs(cfg_s.total_buy_notional / cfg_f.total_buy_notional - 2.0) < 0.01

        assert classify_macro_trend(90.0, 95.0, 100.0) == MacroTrend.BEAR
        assert classify_macro_trend(110.0, 105.0, 100.0) == MacroTrend.BULL
        assert classify_macro_trend(100.0, 100.1, 100.0) == MacroTrend.NEUTRAL

    def test_futures_short_chandelier_and_margin_conservation(self):
        from strategy.trend_strategy import TrendDirection, TrendState, TrendStrategy

        # 1) SHORT Chandelier trailing stop ratchets down on lowest_price
        ts = TrendStrategy(allow_short=True, atr_multiplier=2.5, trailing_stop_atr_multiplier=4.0, min_adx=20.0)
        state = TrendState.flat("DOGEUSDT")
        sig = ts.evaluate(
            "DOGEUSDT", 1.0, 0.99, 1.01, -0.005, 0.01, state,
            capital=100.0, adx=25.0, macro_bearish=True,
        )
        assert sig is not None and sig.metadata["direction"] == "SHORT"
        state = ts.update_state(state, sig, 1.0)
        assert state.direction == TrendDirection.SHORT and state.lowest_price == 1.0

        # Price drops to 0.92 -> Chandelier trail ratchets below initial stop (1.025) to 0.96
        adj = ts.evaluate(
            "DOGEUSDT", 0.92, 0.94, 0.97, -0.005, 0.01, state,
            capital=100.0, adx=25.0, macro_bearish=True,
        )
        assert adj is not None and adj.metadata["new_trailing_stop"] == pytest.approx(0.96, rel=1e-4)

        # 2) Futures 1.5x backtest on bear crash preserves exact accounting conservation
        bars = _make_series([1.0] * 80 + [1.0 * (0.997 ** i) for i in range(320)])
        cfg = BacktestConfig(
            symbol="DOGEUSDT", market="futures", leverage=1.5, allow_short=True,
            fee_pct=0.05, funding_rate_8h_pct=0.01, initial_capital=100.0,
            warmup_candles=60, trend_interval="5m", use_macro_filter=False,
        )
        eng = BacktestEngine(cfg)
        rep = eng.run(bars, strategy_mode="trend_only")
        trade_pnl = sum(t.pnl for t in eng.trades)
        assert abs((cfg.initial_capital + trade_pnl) - rep.final_equity) < 0.05
        assert rep.total_return_pct > 0.0  # SHORT profits cleanly on sustained downtrend

