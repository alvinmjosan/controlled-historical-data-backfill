"""
Load module: idempotent partition load inside a single SQLite transaction.

Strategy per partition:
1. DELETE existing fact_orders rows for partition_date.
2. INSERT fresh fact_orders rows (one per order_id, latest event_ts via UPSERT).
3. INSERT OR IGNORE into order_status_history (append-only, all events).
4. DELETE + re-INSERT rejected_records for this partition_date.
   NOTE: Delete by partition_date ONLY (not partition_date + run_id) so that
   a rerun with a new run_id replaces the old rejected rows rather than
   accumulating them indefinitely.
5. Rollback the entire transaction on any error.

Reference data (dim_customers, dim_products) uses INSERT OR REPLACE (upsert).

Row accounting:
    rows_in        = total rows read from CSV
    rows_out       = all valid rows written (fact_orders + history-only)
    rows_history_only = valid rows that were NOT the latest per order_id
                        (written to order_status_history only, not fact_orders)
    rows_rejected  = rows that failed validation
    rows_in == rows_out + rows_rejected  (reconciliation invariant)
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reference data upserts
# ---------------------------------------------------------------------------

def load_customers(conn: sqlite3.Connection, customers: list[dict]) -> int:
    """
    Upsert customers into dim_customers.

    Returns the number of rows inserted/replaced.
    """
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (c["customer_id"], c["customer_name"], c["country"], now)
        for c in customers
    ]
    with conn:
        conn.executemany(
            "INSERT OR REPLACE INTO dim_customers (customer_id, customer_name, country, loaded_at) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
    return len(rows)


def load_products(conn: sqlite3.Connection, products: list[dict]) -> int:
    """
    Upsert products into dim_products.

    Returns the number of rows inserted/replaced.
    """
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (p["product_id"], p["product_name"], p["category"], now)
        for p in products
    ]
    with conn:
        conn.executemany(
            "INSERT OR REPLACE INTO dim_products (product_id, product_name, category, loaded_at) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
    return len(rows)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_fact_data(fact_rows: list[dict]) -> list[tuple]:
    return [
        (
            r["order_id"],
            r["customer_id"],
            r["product_id"],
            r["quantity"],
            r["unit_price"],
            r["total_amount"],
            r["status"],
            r["event_ts"],
            r["partition_date"],
            r["run_id"],
            r["source_file"],
            r["loaded_at"],
        )
        for r in fact_rows
    ]


def _build_history_data(transformed_rows: list[dict]) -> list[tuple]:
    return [
        (
            r["order_id"],
            r["status"],
            r["event_ts"],
            r["run_id"],
            r["source_file"],
        )
        for r in transformed_rows
    ]


def _build_reject_data(rejected_rows: list[dict]) -> list[tuple]:
    return [
        (
            r["reject_id"],
            r["run_id"],
            r["partition_date"],
            r["source_file"],
            r["row_number"],
            r["raw_record"],
            r["reject_reason"],
            r["rejected_at"],
        )
        for r in rejected_rows
    ]


# ---------------------------------------------------------------------------
# Partition load (idempotent) — batch mode (executemany, one transaction)
# ---------------------------------------------------------------------------

def load_partition(
    conn: sqlite3.Connection,
    partition_date: str,
    transformed_rows: list[dict],   # all valid events (written to history)
    fact_rows: list[dict],           # one per order_id (latest event wins → fact_orders)
    rejected_rows: list[dict],
    run_id: str,
    source_file: str,
) -> tuple[int, int, int]:
    """
    Idempotently load one date partition in a single transaction (batch/executemany).

    Steps:
    1. DELETE existing fact_orders for partition_date.
    2. UPSERT fact_orders rows (latest event_ts per order_id wins).
    3. INSERT OR IGNORE order_status_history (all valid events, append-only).
    4. DELETE rejected_records for partition_date (only), then re-insert.
       Deleting by partition_date only — not run_id — ensures reruns with new
       run_ids replace old rejected rows rather than accumulating them.

    Returns (fact_count, history_only_count, rejected_count).
    Raises on any error (caller should catch and handle status).
    """
    try:
        with conn:
            # --- Step 1: delete stale fact rows for this partition ---
            conn.execute(
                "DELETE FROM fact_orders WHERE partition_date = ?",
                (partition_date,),
            )

            # --- Step 2: upsert fact_orders (latest event_ts wins) ---
            fact_data = _build_fact_data(fact_rows)
            conn.executemany(
                """
                INSERT INTO fact_orders
                    (order_id, customer_id, product_id, quantity, unit_price,
                     total_amount, status, event_ts, partition_date, run_id,
                     source_file, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    customer_id    = excluded.customer_id,
                    product_id     = excluded.product_id,
                    quantity       = excluded.quantity,
                    unit_price     = excluded.unit_price,
                    total_amount   = excluded.total_amount,
                    status         = excluded.status,
                    event_ts       = excluded.event_ts,
                    partition_date = excluded.partition_date,
                    run_id         = excluded.run_id,
                    source_file    = excluded.source_file,
                    loaded_at      = excluded.loaded_at
                WHERE excluded.event_ts >= fact_orders.event_ts
                """,
                fact_data,
            )

            # --- Step 3: append all valid events to order_status_history ---
            history_data = _build_history_data(transformed_rows)
            conn.executemany(
                "INSERT OR IGNORE INTO order_status_history "
                "(order_id, status, event_ts, run_id, source_file) "
                "VALUES (?, ?, ?, ?, ?)",
                history_data,
            )

            # --- Step 4: delete (by partition_date only) + re-insert rejected records ---
            # Delete by partition_date only so reruns with new run_ids replace old rejects.
            conn.execute(
                "DELETE FROM rejected_records WHERE partition_date = ?",
                (partition_date,),
            )
            if rejected_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO rejected_records "
                    "(reject_id, run_id, partition_date, source_file, row_number, "
                    " raw_record, reject_reason, rejected_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    _build_reject_data(rejected_rows),
                )

        fact_count = len(fact_rows)
        history_only_count = len(transformed_rows) - fact_count
        rejected_count = len(rejected_rows)

        logger.info(
            "partition=%s loaded: fact=%d history_only=%d rejected=%d",
            partition_date,
            fact_count,
            history_only_count,
            rejected_count,
        )
        return fact_count, history_only_count, rejected_count

    except Exception:
        logger.exception("Failed to load partition=%s", partition_date)
        raise


# ---------------------------------------------------------------------------
# Partition load — row-by-row mode (for benchmark comparison only)
# ---------------------------------------------------------------------------

def load_partition_row_by_row(
    conn: sqlite3.Connection,
    partition_date: str,
    transformed_rows: list[dict],
    fact_rows: list[dict],
    rejected_rows: list[dict],
    run_id: str,
    source_file: str,
) -> tuple[int, int, int]:
    """
    Row-by-row INSERT mode for benchmark comparison against the batch mode.

    Each INSERT is committed individually (no executemany, no single transaction).
    This is intentionally slow to demonstrate the cost of per-row commits in SQLite.
    NOT for production use.

    Returns (fact_count, history_only_count, rejected_count).
    """
    try:
        # Step 1: delete stale fact rows
        conn.execute(
            "DELETE FROM fact_orders WHERE partition_date = ?",
            (partition_date,),
        )
        conn.commit()

        # Step 2: insert fact rows one-by-one, each with its own commit
        for row in _build_fact_data(fact_rows):
            conn.execute(
                """
                INSERT INTO fact_orders
                    (order_id, customer_id, product_id, quantity, unit_price,
                     total_amount, status, event_ts, partition_date, run_id,
                     source_file, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    customer_id    = excluded.customer_id,
                    product_id     = excluded.product_id,
                    quantity       = excluded.quantity,
                    unit_price     = excluded.unit_price,
                    total_amount   = excluded.total_amount,
                    status         = excluded.status,
                    event_ts       = excluded.event_ts,
                    partition_date = excluded.partition_date,
                    run_id         = excluded.run_id,
                    source_file    = excluded.source_file,
                    loaded_at      = excluded.loaded_at
                WHERE excluded.event_ts >= fact_orders.event_ts
                """,
                row,
            )
            conn.commit()  # per-row commit — intentionally expensive

        # Step 3: history rows one-by-one
        for row in _build_history_data(transformed_rows):
            conn.execute(
                "INSERT OR IGNORE INTO order_status_history "
                "(order_id, status, event_ts, run_id, source_file) "
                "VALUES (?, ?, ?, ?, ?)",
                row,
            )
            conn.commit()

        # Step 4: rejected records one-by-one
        conn.execute(
            "DELETE FROM rejected_records WHERE partition_date = ?",
            (partition_date,),
        )
        conn.commit()
        for row in _build_reject_data(rejected_rows):
            conn.execute(
                "INSERT OR REPLACE INTO rejected_records "
                "(reject_id, run_id, partition_date, source_file, row_number, "
                " raw_record, reject_reason, rejected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
            conn.commit()

        fact_count = len(fact_rows)
        history_only_count = len(transformed_rows) - fact_count
        rejected_count = len(rejected_rows)
        return fact_count, history_only_count, rejected_count

    except Exception:
        logger.exception("Failed to load partition=%s (row-by-row)", partition_date)
        raise
