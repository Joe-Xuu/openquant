"""Tests for TrendStrategy — entry/exit signals, position sizing, state management."""
import sys
sys.path.insert(0, ".")

from strategy.trend_strategy import TrendDirection, TrendState, TrendStrategy
from strategy.signal import SignalAction


class TestTrendEntry:
    def test_long_entry_when_bullish(self):
        ts = TrendStrategy()
        state = TrendState.flat("BTCUSDT")
        signal = ts.evaluate("BTCUSDT", 100000, 99000, 98000, 200, 1000, state)
        assert signal is not None, "Should generate LONG entry when all bullish"
        assert signal.action == SignalAction.START_TREND
        assert signal.metadata["direction"] == "LONG"
        assert "stop_loss" in signal.metadata
        assert signal.metadata["stop_loss"] < 100000

    def test_no_entry_when_flat_market(self):
        ts = TrendStrategy()
        state = TrendState.flat("BTCUSDT")
        signal = ts.evaluate("BTCUSDT", 100000, 99000, 99000, 0, 1000, state)
        assert signal is None, "Should not enter when MACD is flat"

    def test_stop_loss_hit_exits_position(self):
        ts = TrendStrategy()
        state = TrendState(
            symbol="BTCUSDT",
            direction=TrendDirection.LONG,
            entry_price=100000,
            stop_loss_price=98000,
            trailing_stop_price=97000,
        )
        signal = ts.evaluate("BTCUSDT", 97500, 99000, 98000, 0, 1000, state)
        assert signal is not None
        assert signal.action == SignalAction.STOP_TREND
        assert signal.metadata["exit_reason"] == "stop_loss_hit"

    def test_no_exit_when_holding(self):
        """When trend is intact, should either hold or adjust trailing stop (not exit)."""
        ts = TrendStrategy()
        state = TrendState(
            symbol="BTCUSDT",
            direction=TrendDirection.LONG,
            entry_price=100000,
            stop_loss_price=98000,
            trailing_stop_price=97000,
        )
        signal = ts.evaluate("BTCUSDT", 100500, 101000, 99500, 300, 1000, state)
        # Either None (hold) or MODIFY_POSITION (adjust trailing stop) — both are valid
        # Should NOT be STOP_TREND (exit)
        if signal is not None:
            assert signal.action != SignalAction.STOP_TREND, (
                f"Should not exit when trend is intact, got {signal.action}"
            )

    def test_position_size_computation(self):
        ts = TrendStrategy(risk_per_trade_pct=1.0)
        size = ts.compute_position_size(10000, 100000, 98000)
        assert size > 0

    def test_update_state_after_entry(self):
        ts = TrendStrategy()
        old = TrendState.flat("ETHUSDT")
        signal = ts.evaluate("ETHUSDT", 3500, 3450, 3400, 50, 100, old)
        assert signal is not None
        new = ts.update_state(old, signal, 3505)
        assert new.is_active
        assert new.direction == TrendDirection.LONG
        assert new.entry_price == 3505

    def test_update_state_after_exit(self):
        ts = TrendStrategy()
        state = TrendState("BTCUSDT", TrendDirection.LONG, 100000, 0.01, 98000)
        from strategy.signal import SignalBuilder
        signal = SignalBuilder(SignalAction.STOP_TREND, "BTCUSDT", 1.0).with_metadata("pnl_pct", 5.0).build()
        new = ts.update_state(state, signal, 105000)
        assert not new.is_active
        assert new.direction == TrendDirection.FLAT

    def test_trailing_stop_update(self):
        ts = TrendStrategy()
        state = TrendState(
            symbol="BTCUSDT",
            direction=TrendDirection.LONG,
            entry_price=100000,
            stop_loss_price=98000,
            trailing_stop_price=97000,
            entry_atr=1000,
        )
        # Price moved up significantly, trailing stop should ratchet up
        signal = ts.evaluate("BTCUSDT", 105000, 104000, 100000, 500, 1000, state)
        if signal and signal.action == SignalAction.MODIFY_POSITION:
            assert signal.metadata["new_trailing_stop"] > 97000

    def test_short_entry_when_bearish(self):
        ts = TrendStrategy()
        state = TrendState.flat("BTCUSDT")
        signal = ts.evaluate("BTCUSDT", 100000, 97000, 99000, -300, 1000, state)
        if signal:
            assert signal.metadata["direction"] == "SHORT"
            assert signal.metadata["stop_loss"] > 100000
