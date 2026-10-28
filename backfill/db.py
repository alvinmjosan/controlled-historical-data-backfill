"""
Database schema creation and connection helpers for the backfill pipeline.

All CREATE TABLE statements are idempotent (IF NOT EXISTS).
New columns are added via ALTER TABLE migration so existing warehouse.db files
are not broken when the schema evolves.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path


# ---------------------------------------------------------------------------
# DDL statements
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    # Reference: customers
    """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id   TEXT PRIMARY KEY,
        customer_name TEXT NOT NULL,
        country       TEXT NOT NULL,
        loaded_at     TEXT NOT NULL
    )
    """,
    # Reference: products
    """
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id   TEXT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category     TEXT NOT NULL,
        loaded_at    TEXT NOT NULL
    )
    """,
    # Fact: one row per order (latest event_ts wins)
    """
    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id       TEXT PRIMARY KEY,
        customer_id    TEXT NOT NULL,
        product_id     TEXT NOT NULL,
        quantity       INTEGER NOT NULL,
        unit_price     REAL NOT NULL,
        total_amount   REAL NOT NULL,
        status         TEXT NOT NULL,
        event_ts       TEXT NOT NULL,
        partition_date TEXT NOT NULL,
        run_id         TEXT NOT NULL,
        source_file    TEXT NOT NULL,
        loaded_at      TEXT NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_id)  REFERENCES dim_products(product_id)
    )
    """,
    # History: append-only; PK = order_id + event_ts + status
    """
    CREATE TABLE IF NOT EXISTS order_status_history (
        order_id    TEXT NOT NULL,
        status      TEXT NOT NULL,
        event_ts    TEXT NOT NULL,
        run_id      TEXT NOT NULL,
        source_file TEXT NOT NULL,
        PRIMARY KEY (order_id, event_ts, status)
    )
    """,
    # Quarantine: one row per rejected source row
    """
    CREATE TABLE IF NOT EXISTS rejected_records (
        reject_id      TEXT NOT NULL,
        run_id         TEXT NOT NULL,
        partition_date TEXT NOT NULL,
        source_file    TEXT NOT NULL,
        row_number     INTEGER NOT NULL,
        raw_record     TEXT NOT NULL,
        reject_reason  TEXT NOT NULL,
        rejected_at    TEXT NOT NULL,
        PRIMARY KEY (reject_id)
    )
    """,
    # Run-level summary
    """
    CREATE TABLE IF NOT EXISTS backfill_runs (
        run_id              TEXT PRIMARY KEY,
        started_at          TEXT NOT NULL,
        ended_at            TEXT,
        status              TEXT NOT NULL DEFAULT 'RUNNING',
        params              TEXT,
        partitions_total    INTEGER DEFAULT 0,
        partitions_success  INTEGER DEFAULT 0,
        partitions_failed   INTEGER DEFAULT 0,
        rows_in             INTEGER DEFAULT 0,
        rows_out            INTEGER DEFAULT 0,
        rows_rejected       INTEGER DEFAULT 0,
        late_arriving       INTEGER DEFAULT 0,
        failure_reason      TEXT
    )
    """,
    # Per-partition progress tracking
    """
    CREATE TABLE IF NOT EXISTS backfill_partitions (
        run_id             TEXT NOT NULL,
        partition_date     TEXT NOT NULL,
        status             TEXT NOT NULL DEFAULT 'PENDING',
        rows_in            INTEGER DEFAULT 0,
        rows_out           INTEGER DEFAULT 0,
        rows_history_only  INTEGER DEFAULT 0,
        rows_rejected      INTEGER DEFAULT 0,
        late_arriving      INTEGER DEFAULT 0,
        attempts           INTEGER DEFAULT 0,
        error              TEXT,
        started_at         TEXT,
        ended_at           TEXT,
        PRIMARY KEY (run_id, partition_date)
    )
    """,
]

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fact_orders_partition ON fact_orders(partition_date)",
    "CREATE INDEX IF NOT EXISTS idx_rejected_partition    ON rejected_records(partition_date)",
    "CREATE INDEX IF NOT EXISTS idx_history_order         ON order_status_history(order_id)",
]

# Backward-compat migrations: ALTER TABLE to add new columns to existing databases.
# SQLite raises "duplicate column name" if the column already exists; we catch and ignore that.
_MIGRATIONS = [
    ("backfill_runs",       "ALTER TABLE backfill_runs ADD COLUMN late_arriving INTEGER DEFAULT 0"),
    ("backfill_partitions", "ALTER TABLE backfill_partitions ADD COLUMN rows_history_only INTEGER DEFAULT 0"),
    ("backfill_partitions", "ALTER TABLE backfill_partitions ADD COLUMN late_arriving INTEGER DEFAULT 0"),
]


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """
    Return a SQLite connection with WAL journal mode and foreign-key enforcement.

    The caller is responsible for closing the connection.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    """
    Create all tables and indexes if they do not already exist.

    Also runs backward-compat ALTER TABLE migrations so existing databases
    gain new columns without being dropped and recreated.
    """
    with conn:
        for ddl in _DDL_STATEMENTS:
            conn.execute(ddl)
        for idx in _INDEXES:
            conn.execute(idx)

    # Backward-compat: add new columns to existing tables (silently skip if already present).
    for _table, migration_sql in _MIGRATIONS:
        try:
            with conn:
                conn.execute(migration_sql)
        except Exception:
            pass  # Column already exists — safe to ignore
