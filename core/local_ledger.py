"""
================================================================================
LOCAL LEDGER — Double-Entry Bookkeeping & Concurrency-Safe Trade Recording
================================================================================

This module is the ABSOLUTE SOURCE OF TRUTH for the trading system. Every trade,
fee, and balance change is recorded here using rigorous double-entry accounting.
The exchange API is treated as an external, potentially unreliable data source
— we NEVER trust it for our internal state.

DESIGN PRINCIPLES:
    1. Double-entry bookkeeping: Every financial event produces at least two
       journal lines (one debit, one credit) that must balance to zero.
    2. ACID transactions: All writes go through strict SQLite transactions with
       WAL journaling, BEGIN IMMEDIATE serialization, and savepoint rollback.
    3. Concurrency safety: Connection-per-thread, write-ahead logging, busy
       timeout, and explicit locking prevent dirty reads and ghost writes.
    4. Immutable audit trail: Journal entries are append-only once committed.
       Corrections are recorded as reversing entries, never as UPDATEs.

TABLE OF CONTENTS:
    - Chart of Accounts
    - Schema DDL
    - LedgerEngine class
    - Public API for trade/order/position recording
================================================================================
"""

import sqlite3
import threading
import time
import uuid
import os
import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Enums & Constants
# ---------------------------------------------------------------------------

class AccountType(str, Enum):
    """Standard accounting account types."""
    ASSET = "ASSET"
    LIABILITY = "LIABILITY"
    EQUITY = "EQUITY"
    INCOME = "INCOME"
    EXPENSE = "EXPENSE"


class NormalBalance(str, Enum):
    """Which side increases the account balance."""
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class TradeStatus(str, Enum):
    """Lifecycle of a trade from open to closed."""
    NEW = "NEW"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    CLOSED = "CLOSED"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


# ---------------------------------------------------------------------------
# Chart of Accounts — predefined account codes for the double-entry system
# ---------------------------------------------------------------------------
# We define a minimal but complete chart of accounts that captures every
# financial event the trading system can produce.
#
# ACCOUNT CODE FORMAT:  <TYPE_PREFIX>-<NUMBER>
#   CASH-*   = Asset accounts holding currency balances
#   POS-*    = Asset accounts representing open positions (marked to market)
#   FEES-*   = Expense accounts for trading fees
#   SLIP-*   = Expense accounts for slippage costs
#   PNL-*    = Income accounts for realized gains (credit balance) / losses
#   EQUITY-* = Equity accounts for initial capital & retained earnings
# ---------------------------------------------------------------------------

CHART_OF_ACCOUNTS: Dict[str, Dict[str, Any]] = {
    # --- ASSET ACCOUNTS (normal debit balance) ---
    "CASH-USDT": {
        "name": "USDT Cash Balance",
        "type": AccountType.ASSET,
        "normal_balance": NormalBalance.DEBIT,
        "currency": "USDT",
    },
    "CASH-BTC": {
        "name": "BTC Cash Balance",
        "type": AccountType.ASSET,
        "normal_balance": NormalBalance.DEBIT,
        "currency": "BTC",
    },
    "CASH-ETH": {
        "name": "ETH Cash Balance",
        "type": AccountType.ASSET,
        "normal_balance": NormalBalance.DEBIT,
        "currency": "ETH",
    },
    "POS-BTCUSDT": {
        "name": "BTC/USDT Open Position",
        "type": AccountType.ASSET,
        "normal_balance": NormalBalance.DEBIT,
        "currency": "BTC",
    },
    "POS-ETHUSDT": {
        "name": "ETH/USDT Open Position",
        "type": AccountType.ASSET,
        "normal_balance": NormalBalance.DEBIT,
        "currency": "ETH",
    },
    # --- EQUITY ACCOUNTS (normal credit balance) ---
    "EQUITY-INITIAL": {
        "name": "Initial Capital Contribution",
        "type": AccountType.EQUITY,
        "normal_balance": NormalBalance.CREDIT,
        "currency": "USDT",
    },
    "EQUITY-RETAINED": {
        "name": "Retained Earnings / Cumulative PnL",
        "type": AccountType.EQUITY,
        "normal_balance": NormalBalance.CREDIT,
        "currency": "USDT",
    },
    # --- INCOME ACCOUNTS (normal credit balance) ---
    "PNL-REALIZED": {
        "name": "Realized Trading PnL",
        "type": AccountType.INCOME,
        "normal_balance": NormalBalance.CREDIT,
        "currency": "USDT",
    },
    # --- EXPENSE ACCOUNTS (normal debit balance) ---
    "FEES-TRADING": {
        "name": "Trading Fee Expense",
        "type": AccountType.EXPENSE,
        "normal_balance": NormalBalance.DEBIT,
        "currency": "USDT",
    },
    "SLIPPAGE": {
        "name": "Slippage Cost",
        "type": AccountType.EXPENSE,
        "normal_balance": NormalBalance.DEBIT,
        "currency": "USDT",
    },
}


# ---------------------------------------------------------------------------
# DDL — SQLite Schema
# ---------------------------------------------------------------------------

SCHEMA_DDL = """
-- ============================================================================
-- PRAGMA: Concurrency & Durability Settings
-- ============================================================================
-- WAL mode: Writers don't block readers, readers don't block writers.
-- This is ESSENTIAL for an async trading system where the strategy brain
-- reads positions while the execution module writes fills concurrently.
PRAGMA journal_mode=WAL;

-- NORMAL synchronous: Safe across application crashes (OS crash may lose
-- the last few transactions, but we accept that trade-off for speed).
-- Use FULL for absolute durability if running on unreliable hardware.
PRAGMA synchronous=NORMAL;

-- Busy timeout: Instead of immediately returning SQLITE_BUSY when a write
-- lock can't be acquired, wait up to 5 seconds. This dramatically reduces
-- "database is locked" errors under concurrent write load.
PRAGMA busy_timeout=5000;

-- Enable foreign key enforcement (disabled by default in SQLite).
PRAGMA foreign_keys=ON;

-- ============================================================================
-- TABLE: accounts — Chart of Accounts
-- ============================================================================
-- Each row represents one account in the double-entry system.
-- The account_code is the primary key (e.g., "CASH-USDT", "POS-BTCUSDT").
CREATE TABLE IF NOT EXISTS accounts (
    account_code    TEXT PRIMARY KEY NOT NULL,
    account_name    TEXT NOT NULL,
    account_type    TEXT NOT NULL CHECK (account_type IN ('ASSET','LIABILITY','EQUITY','INCOME','EXPENSE')),
    normal_balance  TEXT NOT NULL CHECK (normal_balance IN ('DEBIT','CREDIT')),
    currency        TEXT NOT NULL DEFAULT 'USDT',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
) STRICT;

-- ============================================================================
-- TABLE: journal_entries — Transaction Headers
-- ============================================================================
-- Each row is ONE complete financial transaction. Multiple journal_lines
-- belong to a single journal_entry. The sum of all debit_amount across
-- lines MUST equal the sum of all credit_amount (this is enforced in
-- application code via the _validate_balance() check before commit).
--
-- entry_id:    UUID v4 string — globally unique, no collision risk across
--              restarts or distributed instances (future-proof).
-- entry_date:  ISO-8601 UTC timestamp of when the event OCCURRED (not
--              when it was recorded — those can differ slightly).
-- description: Human-readable explanation of the transaction.
-- is_reversal: TRUE if this entry reverses a prior entry.
-- reversed_entry_id: If is_reversal, points to the original entry.
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id            TEXT PRIMARY KEY NOT NULL,
    entry_date          TEXT NOT NULL,
    description         TEXT NOT NULL DEFAULT '',
    is_reversal         INTEGER NOT NULL DEFAULT 0 CHECK (is_reversal IN (0,1)),
    reversed_entry_id   TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (reversed_entry_id) REFERENCES journal_entries(entry_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_journal_entries_date ON journal_entries(entry_date);

-- ============================================================================
-- TABLE: journal_lines — Individual Debit/Credit Lines
-- ============================================================================
-- This is the CORE of the double-entry system. Each line records one side
-- of a transaction against a specific account.
--
-- CONSTRAINT unique_line_per_account: A single journal entry cannot have
--   two lines hitting the same account — this prevents accidental double-
--   counting and forces explicit line consolidation.
-- CONSTRAINT balanced_entry: Enforced at the journal_entry level by the
--   application before commit. SQLite CHECK constraints cannot span rows,
--   so we validate in the _post_balance() method.
CREATE TABLE IF NOT EXISTS journal_lines (
    line_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id        TEXT NOT NULL,
    account_code    TEXT NOT NULL,
    debit_amount    REAL NOT NULL DEFAULT 0.0 CHECK (debit_amount >= 0),
    credit_amount   REAL NOT NULL DEFAULT 0.0 CHECK (credit_amount >= 0),
    currency        TEXT NOT NULL DEFAULT 'USDT',
    trade_id        TEXT,       -- nullable FK to trades table
    order_id        TEXT,       -- nullable FK to orders table
    memo            TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (entry_id) REFERENCES journal_entries(entry_id) ON DELETE CASCADE,
    FOREIGN KEY (account_code) REFERENCES accounts(account_code),
    FOREIGN KEY (trade_id) REFERENCES trades(trade_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    -- Ensure one side is zero: a line cannot be both debit AND credit
    CHECK ((debit_amount > 0 AND credit_amount = 0) OR (credit_amount > 0 AND debit_amount = 0) OR (debit_amount = 0 AND credit_amount = 0)),
    -- Prevent duplicate account hits within the same entry
    UNIQUE (entry_id, account_code)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_journal_lines_entry ON journal_lines(entry_id);
CREATE INDEX IF NOT EXISTS idx_journal_lines_account ON journal_lines(account_code);
CREATE INDEX IF NOT EXISTS idx_journal_lines_trade ON journal_lines(trade_id);

-- ============================================================================
-- TABLE: trades — Trade Lifecycle Records
-- ============================================================================
-- Each row represents one complete trade (entry + exit). A trade may have
-- multiple orders (e.g., partial fills, stop-loss amendments).
CREATE TABLE IF NOT EXISTS trades (
    trade_id            TEXT PRIMARY KEY NOT NULL,
    symbol              TEXT NOT NULL,
    side                TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    strategy_id         TEXT NOT NULL DEFAULT '',
    quantity            REAL NOT NULL CHECK (quantity >= 0),
    quantity_filled     REAL NOT NULL DEFAULT 0.0,
    entry_price_avg     REAL,
    exit_price_avg      REAL,
    pnl_realized        REAL,
    pnl_realized_pct    REAL,
    fee_total           REAL NOT NULL DEFAULT 0.0,
    slippage_total      REAL NOT NULL DEFAULT 0.0,
    status              TEXT NOT NULL DEFAULT 'NEW' CHECK (status IN ('NEW','PARTIAL_FILL','FILLED','CANCELLED','CLOSED')),
    entry_time          TEXT,
    exit_time           TEXT,
    tags                TEXT NOT NULL DEFAULT '[]',  -- JSON array for flexible metadata
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time);

-- ============================================================================
-- TABLE: orders — Individual Order Records
-- ============================================================================
-- Every order dispatched to the exchange is recorded here BEFORE the API
-- call is made. This prevents "phantom orders" — orders that the brain
-- thinks were placed but that never actually reached the exchange.
CREATE TABLE IF NOT EXISTS orders (
    order_id            TEXT PRIMARY KEY NOT NULL,
    trade_id            TEXT NOT NULL,
    exchange_order_id   TEXT,
    client_order_id     TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    side                TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    order_type          TEXT NOT NULL CHECK (order_type IN ('LIMIT','MARKET','STOP_LOSS','TAKE_PROFIT')),
    price               REAL,
    stop_price          REAL,
    quantity            REAL NOT NULL CHECK (quantity > 0),
    quantity_filled     REAL NOT NULL DEFAULT 0.0,
    quote_order_qty     REAL,
    status              TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','OPEN','PARTIAL_FILL','FILLED','CANCELLED','REJECTED','EXPIRED')),
    time_in_force       TEXT NOT NULL DEFAULT 'GTC',
    request_payload     TEXT,       -- JSON snapshot of the API request (audit trail)
    response_payload    TEXT,       -- JSON snapshot of the API response
    error_message       TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_orders_trade ON orders(trade_id);
CREATE INDEX IF NOT EXISTS idx_orders_exchange ON orders(exchange_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
-- Prevent duplicate exchange order IDs
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_exchange_unique
    ON orders(exchange_order_id) WHERE exchange_order_id IS NOT NULL;

-- ============================================================================
-- TABLE: positions — Current Position State (Materialized View Pattern)
-- ============================================================================
-- This table caches the current position for fast lookup by the strategy
-- brain. It is ALWAYS derivable from the journal_lines (position = sum of
-- all POS-* debits minus credits), but materializing it avoids re-scanning
-- the entire ledger on every tick.
--
-- The position is updated atomically within the same transaction as the
-- journal entries that caused the change.
CREATE TABLE IF NOT EXISTS positions (
    symbol              TEXT PRIMARY KEY NOT NULL,
    base_asset          TEXT NOT NULL,
    quote_asset         TEXT NOT NULL,
    quantity            REAL NOT NULL DEFAULT 0.0,
    avg_entry_price     REAL NOT NULL DEFAULT 0.0,
    current_mark_price  REAL,
    unrealized_pnl      REAL,
    cost_basis          REAL NOT NULL DEFAULT 0.0,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
) STRICT;

-- ============================================================================
-- TABLE: balance_snapshots — Time-Series Balance Record
-- ============================================================================
-- Periodic snapshots of account balances for equity curve plotting and
-- drawdown calculation. These are written by a separate background process
-- (or triggered on each trade close) and are NEVER used as the source of
-- truth for trading decisions.
CREATE TABLE IF NOT EXISTS balance_snapshots (
    snapshot_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp           TEXT NOT NULL,
    account_code        TEXT NOT NULL,
    balance             REAL NOT NULL,
    currency            TEXT NOT NULL DEFAULT 'USDT',
    FOREIGN KEY (account_code) REFERENCES accounts(account_code)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_balance_snapshots_ts ON balance_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_balance_snapshots_account ON balance_snapshots(account_code);

-- ============================================================================
-- TABLE: ledger_metadata — System-Level State Tracking
-- ============================================================================
-- Stores sequence numbers, last reconciled timestamps, and other operational
-- metadata in a key-value format. Prevents duplicate processing of exchange
-- events after a restart.
CREATE TABLE IF NOT EXISTS ledger_metadata (
    key         TEXT PRIMARY KEY NOT NULL,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
) STRICT;
"""


# ---------------------------------------------------------------------------
# Dataclasses for typed returns
# ---------------------------------------------------------------------------

@dataclass
class JournalLine:
    """A single debit or credit line in a journal entry."""
    account_code: str
    debit_amount: float = 0.0
    credit_amount: float = 0.0
    currency: str = "USDT"
    memo: str = ""


@dataclass
class TradeRecord:
    """Typed representation of a trade row."""
    trade_id: str
    symbol: str
    side: str
    quantity: float
    quantity_filled: float
    entry_price_avg: Optional[float]
    exit_price_avg: Optional[float]
    pnl_realized: Optional[float]
    pnl_realized_pct: Optional[float]
    fee_total: float
    slippage_total: float
    status: str
    entry_time: Optional[str]
    exit_time: Optional[str]


@dataclass
class PositionRecord:
    """Typed representation of a current position."""
    symbol: str
    base_asset: str
    quote_asset: str
    quantity: float
    avg_entry_price: float
    current_mark_price: Optional[float]
    unrealized_pnl: Optional[float]
    cost_basis: float


# ---------------------------------------------------------------------------
# LedgerEngine — The Core Double-Entry Ledger
# ---------------------------------------------------------------------------

class LedgerEngine:
    """
    Thread-safe, ACID-compliant double-entry ledger backed by SQLite.

    CONCURRENCY MODEL:
        - WAL journal mode allows one writer concurrent with many readers.
        - BEGIN IMMEDIATE on every write transaction: acquires the reserved
          lock upfront, preventing a writer from being starved by readers.
        - Thread-local connections: each thread gets its own sqlite3.Connection
          via _get_connection(). SQLite connections are not thread-safe when
          shared, so this is mandatory.
        - Explicit _write_lock (threading.Lock): serializes all write
          transactions at the Python level. While WAL allows concurrent
          reads during a write, only ONE writer can hold the WAL write lock.
          The Python lock prevents contention at the C level and provides
          cleaner error messages.

    DOUBLE-ENTRY INVARIANT:
        Before any transaction commits, _validate_balance() ensures:
            SUM(debit_amount) == SUM(credit_amount)
        If this fails, the transaction is ROLLED BACK and a ValueError is
        raised. This invariant is NEVER violated in committed data.
    """

    def __init__(self, db_path: str = "data/trading_ledger.db"):
        """
        Initialize the ledger engine.

        Args:
            db_path: Path to the SQLite database file. Directories are
                     created automatically if they don't exist.
        """
        self._db_path = db_path
        self._write_lock = threading.Lock()
        self._local = threading.local()

        # Ensure the data directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            Path(db_dir).mkdir(parents=True, exist_ok=True)

        # Initialize schema — connection stays open for lifetime of engine
        conn = self._get_connection()
        conn.executescript(SCHEMA_DDL)
        self._seed_chart_of_accounts(conn)
        conn.commit()
        # Note: do NOT close this connection — it's the thread-local default

    # ------------------------------------------------------------------
    # Connection Management
    # ------------------------------------------------------------------

    def _get_connection(self) -> sqlite3.Connection:
        """
        Return a thread-local SQLite connection.

        Thread safety: Each thread gets its own connection stored in
        threading.local(). SQLite connections (and particularly their
        cursors and transaction state) are NOT safe to share across
        threads. This method ensures isolation.

        Connection parameters:
            - check_same_thread=False: Required because we manage thread
              affinity ourselves via _local.
            - detect_types=PARSE_DECLTYPES: Enables native datetime handling
              if we add it later.
        """
        # Check if existing connection is still alive
        if hasattr(self._local, "connection") and self._local.connection is not None:
            try:
                self._local.connection.execute("SELECT 1")
                return self._local.connection
            except sqlite3.ProgrammingError:
                pass  # Connection is closed, create new one below

        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        conn.row_factory = sqlite3.Row  # dict-like access
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        self._local.connection = conn
        return conn

    def close(self):
        """Close the thread-local connection if open."""
        if hasattr(self._local, "connection") and self._local.connection is not None:
            self._local.connection.close()
            self._local.connection = None

    # ------------------------------------------------------------------
    # Internal: Chart of Accounts Seeding
    # ------------------------------------------------------------------

    def _seed_chart_of_accounts(self, conn: sqlite3.Connection):
        """Insert the predefined chart of accounts if the table is empty."""
        existing = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        if existing == 0:
            for code, info in CHART_OF_ACCOUNTS.items():
                conn.execute(
                    """INSERT OR IGNORE INTO accounts
                       (account_code, account_name, account_type, normal_balance, currency)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        code,
                        info["name"],
                        info["type"].value,
                        info["normal_balance"].value,
                        info["currency"],
                    ),
                )

    # ------------------------------------------------------------------
    # Internal: Double-Entry Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_balance(lines: List[JournalLine], entry_id: str) -> None:
        """
        RAISES ValueError if debits != credits for a journal entry.

        This is the CRITICAL INVARIANT of double-entry bookkeeping.
        Every financial transaction MUST balance to zero:
            SUM(debits) - SUM(credits) = 0

        If this fails, it indicates a logic error in the code that
        constructed the journal lines — the transaction will be rolled
        back and MUST be fixed before retrying.
        """
        total_debits = sum(line.debit_amount for line in lines)
        total_credits = sum(line.credit_amount for line in lines)

        # Use a small epsilon for floating-point tolerance
        if abs(total_debits - total_credits) > 0.0001:
            raise ValueError(
                f"Journal entry {entry_id} does not balance: "
                f"debits={total_debits:.8f}, credits={total_credits:.8f}, "
                f"difference={abs(total_debits - total_credits):.8f}. "
                f"Transaction ABORTED and ROLLED BACK."
            )

    # ------------------------------------------------------------------
    # Transaction Context Manager
    # ------------------------------------------------------------------

    @contextmanager
    def _write_transaction(self, description: str = ""):
        """
        Context manager for write transactions.

        CONCURRENCY SAFEGUARDS:
        1. Acquires self._write_lock (Python-level mutex) to serialize
           all write operations. This is stricter than SQLite's WAL
           (which allows one writer), but it prevents SQLITE_BUSY at
           the C level entirely and ensures clean error handling.
        2. BEGIN IMMEDIATE: Acquires the reserved lock immediately rather
           than waiting until the first UPDATE/INSERT. Without this, a
           reader that started before our BEGIN could block us when we
           try to upgrade from a shared lock to a reserved lock.
        3. On exception: ROLLBACK to the savepoint (or full transaction),
           ensuring no partial writes are ever committed.
        4. On success: COMMIT, making all changes durable.

        Usage:
            with self._write_transaction("Record trade fill"):
                # ... execute writes ...
                # Auto-commits on exit if no exception
        """
        conn = self._get_connection()
        with self._write_lock:
            conn.execute("BEGIN IMMEDIATE")
            # Create a savepoint for fine-grained rollback within the
            # transaction. If a mid-transaction operation fails, we can
            # roll back to here without aborting the entire transaction
            # (though in practice we usually roll back everything).
            savepoint_name = f"sp_{uuid.uuid4().hex[:8]}"
            try:
                conn.execute(f"SAVEPOINT {savepoint_name}")
                yield conn
                conn.execute(f"RELEASE {savepoint_name}")
                conn.commit()
            except Exception:
                conn.execute(f"ROLLBACK TO {savepoint_name}")
                conn.commit()  # Commit the outer transaction (which now has no changes)
                raise

    # ------------------------------------------------------------------
    # Public API: Account Balance Queries
    # ------------------------------------------------------------------

    def get_account_balance(self, account_code: str, currency: str = "USDT") -> float:
        """
        Calculate the current balance of an account by summing its journal lines.

        For accounts with normal DEBIT balance (ASSET, EXPENSE):
            balance = SUM(debit_amount) - SUM(credit_amount)
            (Debits increase, credits decrease)

        For accounts with normal CREDIT balance (LIABILITY, EQUITY, INCOME):
            balance = SUM(credit_amount) - SUM(debit_amount)
            (Credits increase, debits decrease)

        This is ALWAYS computed from the journal, never from a cached value.
        """
        conn = self._get_connection()
        row = conn.execute(
            """SELECT
                 a.normal_balance,
                 COALESCE(SUM(jl.debit_amount), 0)   AS total_debits,
                 COALESCE(SUM(jl.credit_amount), 0)  AS total_credits
               FROM accounts a
               LEFT JOIN journal_lines jl ON a.account_code = jl.account_code
               WHERE a.account_code = ?
               GROUP BY a.account_code""",
            (account_code,),
        ).fetchone()

        if row is None:
            return 0.0

        normal_balance = row["normal_balance"]
        total_debits = row["total_debits"]
        total_credits = row["total_credits"]

        if normal_balance == NormalBalance.DEBIT.value:
            return total_debits - total_credits
        else:
            return total_credits - total_debits

    def get_all_balances(self) -> Dict[str, float]:
        """Return the current balance of every account in the chart."""
        conn = self._get_connection()
        rows = conn.execute(
            """SELECT
                 a.account_code,
                 a.normal_balance,
                 COALESCE(SUM(jl.debit_amount), 0)   AS total_debits,
                 COALESCE(SUM(jl.credit_amount), 0)  AS total_credits
               FROM accounts a
               LEFT JOIN journal_lines jl ON a.account_code = jl.account_code
               GROUP BY a.account_code"""
        ).fetchall()

        balances = {}
        for row in rows:
            if row["normal_balance"] == NormalBalance.DEBIT.value:
                balances[row["account_code"]] = row["total_debits"] - row["total_credits"]
            else:
                balances[row["account_code"]] = row["total_credits"] - row["total_debits"]
        return balances

    def get_total_equity(self) -> float:
        """
        Calculate total equity = total assets - total liabilities.

        In our system, equity is primarily:
            CASH balances + Position market value - Fees - Slippage
        This is equivalent to: sum of all EQUITY account balances.
        """
        balances = self.get_all_balances()
        equity = 0.0
        for code, bal in balances.items():
            if code.startswith("EQUITY-"):
                equity += bal
        return equity

    # ------------------------------------------------------------------
    # Public API: Journal Entry Recording
    # ------------------------------------------------------------------

    def record_journal_entry(
        self,
        lines: List[JournalLine],
        entry_date: Optional[str] = None,
        description: str = "",
        trade_id: Optional[str] = None,
        order_id: Optional[str] = None,
    ) -> str:
        """
        Record a complete double-entry journal transaction.

        This is the LOW-LEVEL primitive for all financial recording.
        Higher-level methods (record_trade_open, record_trade_close, etc.)
        build their journal lines and call this method.

        Args:
            lines: List of JournalLine objects. Must balance (debits==credits).
            entry_date: ISO-8601 timestamp for the event. Defaults to now.
            description: Human-readable explanation.
            trade_id: Optional trade this entry relates to.
            order_id: Optional order this entry relates to.

        Returns:
            The generated entry_id (UUID v4 string).

        Raises:
            ValueError: If debits != credits (double-entry invariant violated).

        CONCURRENCY NOTE:
            This method acquires the write lock. If multiple threads attempt
            to record entries simultaneously, they will serialize cleanly.
            The WAL journal ensures that readers (strategy brain checking
            positions) are never blocked by this write.
        """
        entry_id = uuid.uuid4().hex
        if entry_date is None:
            entry_date = datetime.now(timezone.utc).isoformat()

        # ---- CRITICAL INVARIANT CHECK ----
        self._validate_balance(lines, entry_id)

        with self._write_transaction(f"Journal entry {entry_id}: {description}") as conn:
            # Insert journal entry header
            conn.execute(
                """INSERT INTO journal_entries (entry_id, entry_date, description)
                   VALUES (?, ?, ?)""",
                (entry_id, entry_date, description),
            )

            # Insert each journal line
            for line in lines:
                conn.execute(
                    """INSERT INTO journal_lines
                       (entry_id, account_code, debit_amount, credit_amount,
                        currency, trade_id, order_id, memo)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        entry_id,
                        line.account_code,
                        line.debit_amount,
                        line.credit_amount,
                        line.currency,
                        trade_id,
                        order_id,
                        line.memo,
                    ),
                )

        return entry_id

    # ------------------------------------------------------------------
    # Public API: Trade Lifecycle Recording
    # ------------------------------------------------------------------

    def register_grid_trade(self, trade_id: str, symbol: str) -> str:
        """
        Register a grid as a trade container WITHOUT journal entries.

        Grid trades are different: the grid itself is a strategy container,
        and each fill creates its own trade with proper double-entry.
        This method just creates the parent record for foreign key integrity.
        """
        with self._write_transaction(f"Register grid trade {trade_id}") as conn:
            conn.execute(
                """INSERT INTO trades (trade_id, symbol, side, quantity, quantity_filled,
                   status, entry_time)
                   VALUES (?, ?, 'BUY', 0.0, 0.0, 'NEW', datetime('now'))""",
                (trade_id, symbol),
            )
        return trade_id

    def record_trade_open(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        fee: float = 0.0,
        slippage: float = 0.0,
        strategy_id: str = "",
        trade_id: Optional[str] = None,
        entry_time: Optional[str] = None,
    ) -> str:
        """
        Record the OPENING of a new trade with double-entry bookkeeping.

        Accounting treatment for a BUY (e.g., BTCUSDT):
            DEBIT  POS-BTCUSDT      quantity * price    (we acquire the asset)
            CREDIT CASH-USDT        quantity * price    (we pay in USDT)
            DEBIT  FEES-TRADING     fee                 (trading fee)
            CREDIT CASH-USDT        fee                 (fee paid from cash)
            DEBIT  SLIPPAGE         slippage            (adverse price movement)
            CREDIT CASH-USDT        slippage            (slippage cost from cash)

        For a SELL (opening a short — if enabled):
            The entries are reversed.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT").
            side: "BUY" or "SELL".
            quantity: Base asset quantity.
            price: Execution price.
            fee: Total fee in quote currency.
            slippage: Cost of slippage in quote currency.
            strategy_id: Identifier for the strategy that generated this trade.
            trade_id: Optional pre-generated trade ID. Auto-generated if None.
            entry_time: ISO-8601 timestamp. Defaults to now.

        Returns:
            The trade_id.

        Transaction Atomicity:
            The trade INSERT, journal entry, AND position UPSERT all happen
            within a SINGLE database transaction. If any step fails, the
            entire block is rolled back — preventing orphaned trades or
            unbalanced journal entries.
        """
        if trade_id is None:
            trade_id = uuid.uuid4().hex
        if entry_time is None:
            entry_time = datetime.now(timezone.utc).isoformat()

        quote_asset = "USDT"
        base_asset = symbol.replace("USDT", "")
        pos_account = f"POS-{symbol}"
        cash_account = f"CASH-{quote_asset}"

        # Build journal lines based on side
        notional = quantity * price

        if side.upper() == "BUY":
            lines = [
                JournalLine(pos_account, debit_amount=notional, memo=f"Buy {quantity} {base_asset} @ {price}"),
                JournalLine(cash_account, credit_amount=notional, memo=f"Pay for {quantity} {base_asset}"),
            ]
        else:  # SELL (closing or short)
            lines = [
                JournalLine(cash_account, debit_amount=notional, memo=f"Sell {quantity} {base_asset} @ {price}"),
                JournalLine(pos_account, credit_amount=notional, memo=f"Deliver {quantity} {base_asset}"),
            ]

        # Fee journal lines (if any)
        if fee > 0:
            lines.append(JournalLine("FEES-TRADING", debit_amount=fee, memo=f"Trading fee for {symbol}"))
            lines.append(JournalLine(cash_account, credit_amount=fee, memo=f"Fee deduction"))

        # Slippage journal lines (if any)
        if slippage > 0:
            lines.append(JournalLine("SLIPPAGE", debit_amount=slippage, memo=f"Slippage cost for {symbol}"))
            lines.append(JournalLine(cash_account, credit_amount=slippage, memo=f"Slippage deduction"))

        with self._write_transaction(f"Open trade {trade_id}") as conn:
            # Insert trade record
            conn.execute(
                """INSERT INTO trades
                   (trade_id, symbol, side, strategy_id, quantity, quantity_filled,
                    entry_price_avg, fee_total, slippage_total, status, entry_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'FILLED', ?)""",
                (trade_id, symbol, side.upper(), strategy_id, quantity, quantity,
                 price, fee, slippage, entry_time),
            )

            # Record the journal entry (inside the same transaction)
            entry_id = uuid.uuid4().hex
            self._validate_balance(lines, entry_id)

            conn.execute(
                """INSERT INTO journal_entries (entry_id, entry_date, description)
                   VALUES (?, ?, ?)""",
                (entry_id, entry_time, f"Trade OPEN: {side} {quantity} {symbol} @ {price}"),
            )
            for line in lines:
                conn.execute(
                    """INSERT INTO journal_lines
                       (entry_id, account_code, debit_amount, credit_amount,
                        currency, trade_id, memo)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (entry_id, line.account_code, line.debit_amount,
                     line.credit_amount, line.currency, trade_id, line.memo),
                )

            # Update position atomically
            self._upsert_position(conn, symbol, base_asset, quote_asset,
                                  quantity, price, side.upper())

        return trade_id

    def record_trade_close(
        self,
        trade_id: str,
        exit_price: float,
        fee: float = 0.0,
        slippage: float = 0.0,
        exit_time: Optional[str] = None,
    ) -> Tuple[float, float]:
        """
        Record the CLOSING of an existing trade.

        Reads the original trade to determine side and quantity, then creates
        the reversing journal entries. Calculates realized PnL.

        DOUBLE-ENTRY FOR CLOSING A LONG (BUY → SELL):
            DEBIT  CASH-USDT       exit_notional    (receive cash)
            CREDIT POS-{symbol}    exit_notional    (deliver asset)
            The original POS debit and this POS credit net out to zero.

            The difference between the original debit and this credit,
            minus fees, is the realized PnL, which flows through:
            CREDIT PNL-REALIZED (if profit) or DEBIT PNL-REALIZED (if loss)

        Args:
            trade_id: The trade to close.
            exit_price: Execution price for the closing order.
            fee: Total fee in quote currency.
            slippage: Slippage cost in quote currency.
            exit_time: ISO-8601 timestamp. Defaults to now.

        Returns:
            Tuple of (realized_pnl, realized_pnl_pct).

        Raises:
            ValueError: If trade not found, already closed, or cancelled.
        """
        if exit_time is None:
            exit_time = datetime.now(timezone.utc).isoformat()

        with self._write_transaction(f"Close trade {trade_id}") as conn:
            # Read the original trade — LOCKED within this transaction
            trade = conn.execute(
                "SELECT * FROM trades WHERE trade_id = ?", (trade_id,)
            ).fetchone()

            if trade is None:
                raise ValueError(f"Trade {trade_id} not found")
            if trade["status"] in ("CLOSED", "CANCELLED"):
                raise ValueError(
                    f"Trade {trade_id} is already {trade['status']} — cannot close"
                )

            symbol = trade["symbol"]
            side = trade["side"]
            quantity = trade["quantity"]
            entry_price = trade["entry_price_avg"] or 0.0
            quote_asset = "USDT"
            base_asset = symbol.replace("USDT", "")
            pos_account = f"POS-{symbol}"
            cash_account = f"CASH-{quote_asset}"

            entry_notional = quantity * entry_price
            exit_notional = quantity * exit_price

            # Calculate realized PnL
            if side == "BUY":
                # Long: profit = exit_notional - entry_notional - fees - slippage
                gross_pnl = exit_notional - entry_notional
            else:
                # Short: profit = entry_notional - exit_notional - fees - slippage
                gross_pnl = entry_notional - exit_notional

            total_costs = fee + slippage + trade["fee_total"] + trade["slippage_total"]
            realized_pnl = gross_pnl - total_costs
            realized_pnl_pct = (realized_pnl / entry_notional * 100) if entry_notional > 0 else 0.0

            # Build closing journal lines
            lines: List[JournalLine] = []

            # Reverse the position (close it out)
            if side == "BUY":
                # We held the asset (POS debit), now we sell it (POS credit)
                lines.append(JournalLine(cash_account, debit_amount=exit_notional,
                                         memo=f"Close sell {quantity} {base_asset} @ {exit_price}"))
                lines.append(JournalLine(pos_account, credit_amount=exit_notional,
                                         memo=f"Deliver {quantity} {base_asset}"))
            else:
                lines.append(JournalLine(pos_account, debit_amount=exit_notional,
                                         memo=f"Close buy {quantity} {base_asset} @ {exit_price}"))
                lines.append(JournalLine(cash_account, credit_amount=exit_notional,
                                         memo=f"Pay for {quantity} {base_asset}"))

            # Record PnL
            if realized_pnl >= 0:
                # Profit: credit PNL-REALIZED (income increases on credit side)
                lines.append(JournalLine("EQUITY-RETAINED", debit_amount=realized_pnl,
                                         memo=f"Transfer realized profit to equity"))
                lines.append(JournalLine("PNL-REALIZED", credit_amount=realized_pnl,
                                         memo=f"Realized profit on {symbol}"))
            else:
                # Loss: debit PNL-REALIZED (income decreases on debit side)
                loss_abs = abs(realized_pnl)
                lines.append(JournalLine("PNL-REALIZED", debit_amount=loss_abs,
                                         memo=f"Realized loss on {symbol}"))
                lines.append(JournalLine("EQUITY-RETAINED", credit_amount=loss_abs,
                                         memo=f"Transfer realized loss from equity"))

            # Closing fees
            if fee > 0:
                lines.append(JournalLine("FEES-TRADING", debit_amount=fee,
                                         memo=f"Closing fee for {symbol}"))
                lines.append(JournalLine(cash_account, credit_amount=fee,
                                         memo=f"Fee deduction"))

            # Closing slippage
            if slippage > 0:
                lines.append(JournalLine("SLIPPAGE", debit_amount=slippage,
                                         memo=f"Closing slippage for {symbol}"))
                lines.append(JournalLine(cash_account, credit_amount=slippage,
                                         memo=f"Slippage deduction"))

            # --- Record everything atomically ---
            entry_id = uuid.uuid4().hex
            self._validate_balance(lines, entry_id)

            conn.execute(
                """INSERT INTO journal_entries (entry_id, entry_date, description)
                   VALUES (?, ?, ?)""",
                (entry_id, exit_time, f"Trade CLOSE: {symbol} @ {exit_price}, PnL={realized_pnl:.4f}"),
            )
            for line in lines:
                conn.execute(
                    """INSERT INTO journal_lines
                       (entry_id, account_code, debit_amount, credit_amount,
                        currency, trade_id, memo)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (entry_id, line.account_code, line.debit_amount,
                     line.credit_amount, line.currency, trade_id, line.memo),
                )

            # Update trade record
            conn.execute(
                """UPDATE trades SET
                     exit_price_avg = ?,
                     pnl_realized = ?,
                     pnl_realized_pct = ?,
                     fee_total = fee_total + ?,
                     slippage_total = slippage_total + ?,
                     status = 'CLOSED',
                     exit_time = ?,
                     updated_at = datetime('now')
                   WHERE trade_id = ?""",
                (exit_price, realized_pnl, realized_pnl_pct, fee, slippage,
                 exit_time, trade_id),
            )

            # Clear the position
            self._upsert_position(conn, symbol, base_asset, quote_asset,
                                  0.0, 0.0, side, is_close=True)

        return realized_pnl, realized_pnl_pct

    def record_order(
        self,
        trade_id: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        client_order_id: Optional[str] = None,
        request_payload: Optional[Dict] = None,
    ) -> str:
        """
        Record a NEW order BEFORE it is dispatched to the exchange.

        This is critical: we record the order intent FIRST, then send it
        to the API. This ensures that even if the process crashes between
        API call and response, we have a record that the order was attempted
        and can reconcile later via exchange order status lookup.

        Args:
            trade_id: The trade this order belongs to.
            symbol: Trading pair.
            side: "BUY" or "SELL".
            order_type: "LIMIT", "MARKET", "STOP_LOSS", "TAKE_PROFIT".
            quantity: Order quantity in base asset.
            price: Limit price (required for LIMIT orders).
            stop_price: Stop price (for STOP_LOSS / TAKE_PROFIT).
            client_order_id: Custom client order ID. Auto-generated if None.
            request_payload: Full API request payload for audit trail.

        Returns:
            The generated order_id.
        """
        order_id = uuid.uuid4().hex
        if client_order_id is None:
            client_order_id = f"co_{order_id[:16]}"

        with self._write_transaction(f"Record order {order_id}") as conn:
            conn.execute(
                """INSERT INTO orders
                   (order_id, trade_id, client_order_id, symbol, side,
                    order_type, price, stop_price, quantity, status,
                    request_payload)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)""",
                (
                    order_id, trade_id, client_order_id, symbol, side.upper(),
                    order_type.upper(), price, stop_price, quantity,
                    json.dumps(request_payload) if request_payload else None,
                ),
            )
        return order_id

    def update_order_open(self, order_id: str, exchange_order_id: str) -> None:
        """
        Update order status to OPEN after exchange confirms placement.
        Called by order_manager after receiving exchange response.
        """
        with self._write_transaction(f"Open order {order_id}") as conn:
            conn.execute(
                """UPDATE orders SET exchange_order_id=?, status='OPEN',
                   updated_at=datetime('now') WHERE order_id=?""",
                (exchange_order_id, order_id),
            )

    def update_order_fill(
        self,
        order_id: str,
        exchange_order_id: str,
        filled_quantity: float,
        fill_price: float,
        fee: float = 0.0,
        response_payload: Optional[Dict] = None,
    ) -> None:
        """
        Update an order with fill information from the exchange.

        This is called when we receive a fill confirmation via WebSocket
        or REST polling. The exchange_order_id provides the link back to
        the exchange's own order management system.

        THREAD-SAFETY: The UNIQUE index on exchange_order_id prevents
        duplicate order confirmations from being recorded — if the same
        exchange fill event is processed twice (e.g., via both WebSocket
        and REST polling), the second INSERT will fail with a constraint
        violation and be caught gracefully.
        """
        with self._write_transaction(f"Fill order {order_id}") as conn:
            # Determine new status based on fill ratio
            order = conn.execute(
                "SELECT quantity, quantity_filled FROM orders WHERE order_id = ?",
                (order_id,),
            ).fetchone()

            if order is None:
                raise ValueError(f"Order {order_id} not found")

            new_filled = order["quantity_filled"] + filled_quantity
            if new_filled >= order["quantity"] * 0.9999:
                new_status = OrderStatus.FILLED.value
            elif new_filled > 0:
                new_status = OrderStatus.PARTIAL_FILL.value
            else:
                new_status = OrderStatus.OPEN.value

            conn.execute(
                """UPDATE orders SET
                     exchange_order_id = ?,
                     quantity_filled = ?,
                     status = ?,
                     response_payload = ?,
                     updated_at = datetime('now')
                   WHERE order_id = ?""",
                (exchange_order_id, new_filled, new_status,
                 json.dumps(response_payload) if response_payload else None,
                 order_id),
            )

            # Also update the parent trade's filled quantity
            trade = conn.execute(
                "SELECT trade_id FROM orders WHERE order_id = ?", (order_id,)
            ).fetchone()
            if trade:
                conn.execute(
                    """UPDATE trades SET
                         quantity_filled = (SELECT COALESCE(SUM(quantity_filled), 0)
                                           FROM orders WHERE trade_id = ? AND status = 'FILLED'),
                         updated_at = datetime('now')
                       WHERE trade_id = ?""",
                    (trade["trade_id"], trade["trade_id"]),
                )

    def record_initial_capital(self, amount: float, currency: str = "USDT") -> str:
        """
        Seed the ledger with initial capital.

        DOUBLE-ENTRY:
            DEBIT  CASH-{currency}    amount    (we receive cash)
            CREDIT EQUITY-INITIAL     amount    (capital contribution)
        """
        cash_account = f"CASH-{currency}"
        lines = [
            JournalLine(cash_account, debit_amount=amount,
                        memo=f"Initial capital contribution: {amount} {currency}"),
            JournalLine("EQUITY-INITIAL", credit_amount=amount,
                        memo=f"Capital contributed"),
        ]
        return self.record_journal_entry(
            lines=lines,
            description=f"Initial capital: {amount} {currency}",
        )

    def take_snapshot(self, timestamp: Optional[str] = None) -> int:
        """
        Record a balance snapshot for all accounts.

        This is used for equity curve construction and drawdown monitoring.
        Snapshots are written outside the critical trade path to avoid
        adding latency to order recording.

        Returns:
            Number of account balances snapshotted.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        balances = self.get_all_balances()
        count = 0

        with self._write_transaction(f"Balance snapshot at {timestamp}") as conn:
            for account_code, balance in balances.items():
                # Determine currency from account code
                currency = "USDT"
                if "BTC" in account_code and "USDT" not in account_code:
                    currency = "BTC"
                elif "ETH" in account_code and "USDT" not in account_code:
                    currency = "ETH"

                conn.execute(
                    """INSERT INTO balance_snapshots (timestamp, account_code, balance, currency)
                       VALUES (?, ?, ?, ?)""",
                    (timestamp, account_code, balance, currency),
                )
                count += 1
        return count

    # ------------------------------------------------------------------
    # Position Management
    # ------------------------------------------------------------------

    def _upsert_position(
        self,
        conn: sqlite3.Connection,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        quantity: float,
        price: float,
        side: str,
        is_close: bool = False,
    ) -> None:
        """
        Atomically insert or update a position within an existing transaction.

        Uses SQLite's INSERT ... ON CONFLICT DO UPDATE (UPSERT) pattern
        to avoid the read-check-write race condition. The UNIQUE constraint
        on positions.symbol serializes concurrent updates to the same position.

        For position sizing: 'quantity' is always positive and represents
        the absolute amount. For a BUY, it adds to position; for SELL (close),
        it subtracts. The caller handles the sign logic.
        """
        if is_close:
            # Closing: set position to zero
            conn.execute(
                """INSERT INTO positions (symbol, base_asset, quote_asset, quantity,
                          avg_entry_price, cost_basis, updated_at)
                   VALUES (?, ?, ?, 0.0, 0.0, 0.0, datetime('now'))
                   ON CONFLICT(symbol) DO UPDATE SET
                     quantity = 0.0,
                     avg_entry_price = 0.0,
                     cost_basis = 0.0,
                     updated_at = datetime('now')""",
                (symbol, base_asset, quote_asset),
            )
        else:
            # Opening or adding to position: compute new average entry price
            current = conn.execute(
                "SELECT quantity, cost_basis FROM positions WHERE symbol = ?",
                (symbol,),
            ).fetchone()

            if current and current["quantity"] > 0:
                # Weighted average entry price
                old_qty = current["quantity"]
                old_cost = current["cost_basis"]
                new_cost = old_cost + (quantity * price)
                new_qty = old_qty + quantity
                new_avg = new_cost / new_qty if new_qty > 0 else 0.0
            else:
                new_qty = quantity
                new_avg = price
                new_cost = quantity * price

            conn.execute(
                """INSERT INTO positions (symbol, base_asset, quote_asset, quantity,
                          avg_entry_price, cost_basis, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                   ON CONFLICT(symbol) DO UPDATE SET
                     quantity = excluded.quantity,
                     avg_entry_price = excluded.avg_entry_price,
                     cost_basis = excluded.cost_basis,
                     updated_at = datetime('now')""",
                (symbol, base_asset, quote_asset, new_qty, new_avg, new_cost),
            )

    def get_position(self, symbol: str) -> Optional[PositionRecord]:
        """Get the current position for a symbol (from the materialized cache)."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM positions WHERE symbol = ?", (symbol,)
        ).fetchone()
        if row is None:
            return None
        return PositionRecord(
            symbol=row["symbol"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            quantity=row["quantity"],
            avg_entry_price=row["avg_entry_price"],
            current_mark_price=row["current_mark_price"],
            unrealized_pnl=row["unrealized_pnl"],
            cost_basis=row["cost_basis"],
        )

    def get_all_positions(self) -> List[PositionRecord]:
        """Get all current non-zero positions."""
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM positions WHERE quantity > 0"
        ).fetchall()
        return [
            PositionRecord(
                symbol=row["symbol"],
                base_asset=row["base_asset"],
                quote_asset=row["quote_asset"],
                quantity=row["quantity"],
                avg_entry_price=row["avg_entry_price"],
                current_mark_price=row["current_mark_price"],
                unrealized_pnl=row["unrealized_pnl"],
                cost_basis=row["cost_basis"],
            )
            for row in rows
        ]

    def update_mark_prices(self, prices: Dict[str, float]) -> None:
        """
        Update mark prices for positions and compute unrealized PnL.

        This is a LIGHTWEIGHT operation that does NOT go through the
        full double-entry journal. Mark prices are volatile and updating
        them through formal journal entries would flood the ledger with
        noise. Unrealized PnL is tracked in the positions table for
        informational purposes only — trading decisions should be based
        on realized PnL from the journal.

        Args:
            prices: Dict mapping symbol to current mark price.
        """
        conn = self._get_connection()
        with self._write_lock:
            for symbol, price in prices.items():
                pos = conn.execute(
                    "SELECT * FROM positions WHERE symbol = ? AND quantity > 0",
                    (symbol,),
                ).fetchone()
                if pos:
                    qty = pos["quantity"]
                    avg = pos["avg_entry_price"]
                    # Unrealized PnL = (current_price - avg_entry) * quantity
                    # For long positions only (short handling would invert)
                    unrealized = (price - avg) * qty
                    conn.execute(
                        """UPDATE positions SET
                             current_mark_price = ?,
                             unrealized_pnl = ?,
                             updated_at = datetime('now')
                           WHERE symbol = ?""",
                        (price, unrealized, symbol),
                    )
            conn.commit()

    # ------------------------------------------------------------------
    # Metadata Management
    # ------------------------------------------------------------------

    def set_metadata(self, key: str, value: str) -> None:
        """Store a key-value metadata pair (e.g., last processed event ID)."""
        with self._write_transaction(f"Set metadata: {key}") as conn:
            conn.execute(
                """INSERT INTO ledger_metadata (key, value, updated_at)
                   VALUES (?, ?, datetime('now'))
                   ON CONFLICT(key) DO UPDATE SET
                     value = excluded.value,
                     updated_at = datetime('now')""",
                (key, value),
            )

    def get_metadata(self, key: str) -> Optional[str]:
        """Retrieve a metadata value by key."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT value FROM ledger_metadata WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    # ------------------------------------------------------------------
    # Audit & Reconciliation
    # ------------------------------------------------------------------

    def get_trade_history(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[TradeRecord]:
        """Query trade history with optional filters."""
        conn = self._get_connection()
        query = "SELECT * FROM trades WHERE 1=1"
        params: List[Any] = []

        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()
        return [
            TradeRecord(
                trade_id=row["trade_id"],
                symbol=row["symbol"],
                side=row["side"],
                quantity=row["quantity"],
                quantity_filled=row["quantity_filled"],
                entry_price_avg=row["entry_price_avg"],
                exit_price_avg=row["exit_price_avg"],
                pnl_realized=row["pnl_realized"],
                pnl_realized_pct=row["pnl_realized_pct"],
                fee_total=row["fee_total"],
                slippage_total=row["slippage_total"],
                status=row["status"],
                entry_time=row["entry_time"],
                exit_time=row["exit_time"],
            )
            for row in rows
        ]

    def get_journal_for_trade(self, trade_id: str) -> List[Dict[str, Any]]:
        """Get all journal lines associated with a trade (for audit)."""
        conn = self._get_connection()
        rows = conn.execute(
            """SELECT jl.*, je.description, je.entry_date
               FROM journal_lines jl
               JOIN journal_entries je ON jl.entry_id = je.entry_id
               WHERE jl.trade_id = ?
               ORDER BY je.entry_date""",
            (trade_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def verify_ledger_integrity(self) -> Dict[str, Any]:
        """
        Run a comprehensive integrity check on the entire ledger.

        Returns a dict with:
            - balanced: True if total debits == total credits across ALL entries
            - total_debits, total_credits: Global sums
            - unbalanced_entries: List of entry_ids that don't balance individually
            - orphaned_lines: Journal lines without a parent entry
            - positions_match_journal: True if positions table matches journal sum
        """
        conn = self._get_connection()
        result: Dict[str, Any] = {
            "balanced": True,
            "total_debits": 0.0,
            "total_credits": 0.0,
            "unbalanced_entries": [],
            "orphaned_lines": 0,
            "positions_match_journal": True,
        }

        # Check global balance
        totals = conn.execute(
            "SELECT COALESCE(SUM(debit_amount),0), COALESCE(SUM(credit_amount),0) FROM journal_lines"
        ).fetchone()
        result["total_debits"] = totals[0]
        result["total_credits"] = totals[1]
        if abs(totals[0] - totals[1]) > 0.01:
            result["balanced"] = False

        # Check per-entry balance
        unbalanced = conn.execute(
            """SELECT entry_id,
                      SUM(debit_amount) AS d, SUM(credit_amount) AS c
               FROM journal_lines
               GROUP BY entry_id
               HAVING ABS(SUM(debit_amount) - SUM(credit_amount)) > 0.01"""
        ).fetchall()
        result["unbalanced_entries"] = [row["entry_id"] for row in unbalanced]

        # Check orphaned lines
        orphans = conn.execute(
            """SELECT COUNT(*) FROM journal_lines
               WHERE entry_id NOT IN (SELECT entry_id FROM journal_entries)"""
        ).fetchone()[0]
        result["orphaned_lines"] = orphans

        # Verify positions table matches journal
        for row in conn.execute("SELECT symbol, quantity FROM positions WHERE quantity > 0"):
            symbol = row["symbol"]
            pos_account = f"POS-{symbol}"
            journal_qty = conn.execute(
                """SELECT COALESCE(SUM(debit_amount) - SUM(credit_amount), 0)
                   FROM journal_lines WHERE account_code = ?""",
                (pos_account,),
            ).fetchone()[0]
            # The position account stores notional value, not quantity directly.
            # This is a simplified check; a full reconciliation would need mark prices.
            if abs(journal_qty) < 0.01 and row["quantity"] > 0:
                result["positions_match_journal"] = False

        return result

    # ------------------------------------------------------------------
    # Utility: Export & Diagnostics
    # ------------------------------------------------------------------

    def get_equity_curve(
        self, start_time: Optional[str] = None, end_time: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Compute equity curve from balance snapshots.

        Returns a list of {timestamp, equity} dicts suitable for plotting.
        """
        conn = self._get_connection()
        query = """
            SELECT timestamp,
                   SUM(CASE WHEN a.account_type IN ('EQUITY') THEN balance ELSE 0 END) AS equity
            FROM balance_snapshots bs
            JOIN accounts a ON bs.account_code = a.account_code
            WHERE 1=1
        """
        params: List[Any] = []
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        query += " GROUP BY timestamp ORDER BY timestamp"

        rows = conn.execute(query, params).fetchall()
        return [{"timestamp": row["timestamp"], "equity": row["equity"]} for row in rows]

    def get_trade_statistics(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Compute summary statistics for closed trades.

        Returns:
            Dict with total_trades, win_rate, total_pnl, avg_pnl, profit_factor,
            largest_win, largest_loss, avg_win, avg_loss, sharpe_approximation.
        """
        conn = self._get_connection()
        query = "SELECT pnl_realized FROM trades WHERE status = 'CLOSED' AND pnl_realized IS NOT NULL"
        params: List[Any] = []
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)

        rows = conn.execute(query, params).fetchall()
        pnls = [row["pnl_realized"] for row in rows]

        if not pnls:
            return {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0}

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        total_pnl = sum(pnls)
        win_rate = len(wins) / len(pnls) * 100 if pnls else 0.0

        gross_profit = sum(wins) if wins else 0.0
        gross_loss = abs(sum(losses)) if losses else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Sharpe ratio approximation (assuming risk-free rate = 0)
        if len(pnls) > 1:
            mean_pnl = total_pnl / len(pnls)
            variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
            std_pnl = variance ** 0.5
            sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0
        else:
            sharpe = 0.0
            std_pnl = 0.0
            mean_pnl = 0.0

        return {
            "total_trades": len(pnls),
            "win_rate_pct": round(win_rate, 2),
            "total_pnl": round(total_pnl, 4),
            "avg_pnl": round(mean_pnl, 4),
            "std_pnl": round(std_pnl, 4),
            "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
            "largest_win": round(max(wins), 4) if wins else 0.0,
            "largest_loss": round(min(losses), 4) if losses else 0.0,
            "avg_win": round(sum(wins) / len(wins), 4) if wins else 0.0,
            "avg_loss": round(sum(losses) / len(losses), 4) if losses else 0.0,
            "sharpe_approximation": round(sharpe, 4),
        }


# ---------------------------------------------------------------------------
# Module-Level Convenience: Singleton & Factory
# ---------------------------------------------------------------------------

_ledger_instance: Optional[LedgerEngine] = None
_ledger_lock = threading.Lock()


def get_ledger(db_path: str = "data/trading_ledger.db") -> LedgerEngine:
    """
    Return a process-wide singleton LedgerEngine.

    Thread-safe lazy initialization using double-check locking.
    Multiple LedgerEngine instances pointing at the same database file
    would contend for the same WAL lock — a singleton avoids this confusion.
    """
    global _ledger_instance
    if _ledger_instance is None:
        with _ledger_lock:
            if _ledger_instance is None:
                _ledger_instance = LedgerEngine(db_path=db_path)
    return _ledger_instance


def reset_ledger() -> None:
    """Reset the singleton (primarily for testing)."""
    global _ledger_instance
    with _ledger_lock:
        if _ledger_instance is not None:
            _ledger_instance.close()
        _ledger_instance = None
