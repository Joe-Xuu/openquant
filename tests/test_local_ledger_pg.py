"""
Integration tests for LedgerEngine against a REAL PostgreSQL database
(openquant_test). Verifies ACID double-entry invariants, fee/slippage line
consolidation, dynamic symbol accounts, non-destructive equity reconciliation,
mark-price updates, and grid_fills persistence.
"""

import os
import sys

import psycopg2
import pytest

sys.path.insert(0, ".")

from core.local_ledger import LedgerEngine


TEST_DSN = os.getenv(
    "OPENQUANT_TEST_DSN",
    "host=localhost port=5432 user=openquant password=openquant_test dbname=openquant_test",
)


def _pg_available() -> bool:
    try:
        conn = psycopg2.connect(TEST_DSN)
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL test DB not reachable")


@pytest.fixture
def ledger():
    # Clean schema before each test for isolation
    conn = psycopg2.connect(TEST_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    conn.close()

    eng = LedgerEngine(dsn=TEST_DSN)
    yield eng
    eng.close()
    eng._pool.closeall()


class TestLocalLedgerPostgres:
    def test_double_entry_profit_increases_equity_with_fees(self, ledger):
        """Profitable trade with entry+exit fees must increase equity & keep trial balance."""
        ledger.record_initial_capital(100.0)
        assert abs(ledger.get_total_equity() - 100.0) < 1e-6

        tid = ledger.record_trade_open(
            symbol="DOGEUSDT", side="BUY", quantity=100.0, price=0.10, fee=0.01, slippage=0.005
        )
        # Paid $10 for DOGE + $0.01 fee + $0.005 slippage -> cash = 89.985, equity = 99.985
        assert abs(ledger.get_account_balance("CASH-USDT") - 89.985) < 1e-6
        assert abs(ledger.get_account_balance("POS-DOGEUSDT") - 10.0) < 1e-6
        assert abs(ledger.get_total_equity() - 99.985) < 1e-6

        pnl, pnl_pct = ledger.record_trade_close(tid, exit_price=0.12, fee=0.012, slippage=0.005)
        # Gross PnL = +2.00; total costs = 0.01 + 0.005 + 0.012 + 0.005 = 0.032; net = +1.968
        assert abs(pnl - 1.968) < 1e-6
        assert abs(ledger.get_account_balance("POS-DOGEUSDT")) < 1e-6
        assert abs(ledger.get_account_balance("CASH-USDT") - 101.968) < 1e-6
        assert abs(ledger.get_total_equity() - 101.968) < 1e-6

        integrity = ledger.verify_ledger_integrity()
        assert integrity["balanced"] is True

    def test_dynamic_symbol_accounts_no_fk_error(self, ledger):
        """Trading a symbol not hardcoded in CHART_OF_ACCOUNTS (e.g. SOLUSDT) must succeed."""
        ledger.record_initial_capital(500.0)
        tid = ledger.record_trade_open(symbol="SOLUSDT", side="BUY", quantity=1.0, price=150.0, fee=0.15)
        ledger.update_mark_prices({"SOLUSDT": 160.0})
        pos = ledger.get_position("SOLUSDT")
        assert pos is not None
        assert abs(pos.current_mark_price - 160.0) < 1e-6
        assert abs(pos.unrealized_pnl - 10.0) < 1e-6

        pnl, _ = ledger.record_trade_close(tid, exit_price=160.0, fee=0.16)
        assert abs(pnl - (10.0 - 0.31)) < 1e-6
        assert ledger.verify_ledger_integrity()["balanced"] is True

    def test_reconcile_equity_preserves_history(self, ledger):
        """Startup equity reconciliation must adjust equity without wiping trade history."""
        ledger.record_initial_capital(100.0)
        tid = ledger.record_trade_open(symbol="DOGEUSDT", side="BUY", quantity=50.0, price=0.10)
        ledger.record_trade_close(tid, exit_price=0.11)
        assert len(ledger.get_trade_history()) == 1

        # External deposit of $50 on exchange -> equity now $150.50
        ledger.reconcile_equity(150.50)
        assert abs(ledger.get_total_equity() - 150.50) < 1e-4
        assert len(ledger.get_trade_history()) == 1  # History preserved!
        assert ledger.verify_ledger_integrity()["balanced"] is True

    def test_grid_fills_persistence_cycle(self, ledger):
        """Verify BUY fill -> sell_placed -> reset -> sell_filled persistence."""
        inserted = ledger.record_grid_fill(
            trade_id="t_1001", symbol="DOGEUSDT", exchange_order_id="o_1",
            buy_price=0.08, buy_qty=100.0, buy_time="2026-01-01T00:00:00+00:00",
            profit_pct=1.0,
        )
        assert inserted is True
        # Duplicate trade_id is idempotent
        assert ledger.record_grid_fill(
            trade_id="t_1001", symbol="DOGEUSDT", exchange_order_id="o_1",
            buy_price=0.08, buy_qty=100.0, buy_time="2026-01-01T00:00:00+00:00",
            profit_pct=1.0,
        ) is False

        ledger.grid_fill_set_sell_placed("t_1001", "ex_tp_1", "tp_t_1001")
        assert "tp_t_1001" in ledger.get_tp_attempted_tags()

        ledger.grid_fill_reset_sell_placed("t_1001")
        assert "tp_t_1001" not in ledger.get_tp_attempted_tags()
        pending = ledger.get_pending_tp_fills("DOGEUSDT")
        assert len(pending) == 1 and pending[0]["sell_placed"] is False

        ledger.grid_fill_set_sell_placed("t_1001", "ex_tp_2", "tp_t_1001")
        ledger.grid_fill_set_sell_filled("t_1001", 0.0808, "2026-01-01T01:00:00+00:00")
        assert len(ledger.get_pending_tp_fills("DOGEUSDT")) == 0
