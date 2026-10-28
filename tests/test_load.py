"""
Tests for load.py: idempotent partition loading, history append, FK integrity.

Covers:
- Test 1 (happy path): valid rows loaded, counts correct, total_amount correct
- Test 10 (idempotency): rerun produces identical counts, no duplicates
- Test 6 (late-arriving): older event does not overwrite newer fact row
- Test 3/4/5 (reject path): rejects stored and retrievable
- Test: rejected records are replaced (not duplicated) on rerun with new run_id
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from backfill.db import get_connection, create_schema
from backfill.extract import read_customers, read_products
from backfill.load import load_customers, load_products, load_partition
from backfill.transform import transform_row, resolve_latest_per_order
from backfill.validate import validate_rows
from tests.conftest import (
    SAMPLE_CUSTOMERS, SAMPLE_PRODUCTS,
    VALID_CUSTOMER_IDS, VALID_PRODUCT_IDS,
    make_valid_row,
)

PARTITION_DATE = "2025-01-01"
RUN_ID = "run_test_load"
SOURCE_FILE = "orders_2025-01-01.csv"
NOW_TS = datetime(2025, 1, 1, 12, tzinfo=timezone.utc).isoformat()


def _build_fact_rows(raw_rows: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Run full validate + transform pipeline on raw rows."""
    numbered = [{**r, "_row_number": i} for i, r in enumerate(raw_rows, 1)]
    valid, rejected, _ = validate_rows(
        rows=numbered,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )
    transformed = [transform_row(r, PARTITION_DATE, RUN_ID, SOURCE_FILE, NOW_TS) for r in valid]
    fact = resolve_latest_per_order(transformed)
    return transformed, fact, rejected


# ---------------------------------------------------------------------------
# Test 1: Happy path
# ---------------------------------------------------------------------------

def test_happy_path_load(db_conn):
    """Valid rows load into fact_orders with correct counts and total_amount."""
    raw = [
        make_valid_row("ORD-001", quantity=2, unit_price=10.0),
        make_valid_row("ORD-002", quantity=3, unit_price=5.0),
    ]
    transformed, fact, rejected = _build_fact_rows(raw)

    fact_count, history_only, rows_rejected = load_partition(
        db_conn, PARTITION_DATE, transformed, fact, rejected, RUN_ID, SOURCE_FILE
    )

    # 2 valid rows, no status updates → history_only=0, fact_count=2
    assert fact_count == 2
    assert history_only == 0
    assert rows_rejected == 0

    # Verify fact_orders content
    rows = db_conn.execute("SELECT * FROM fact_orders ORDER BY order_id").fetchall()
    assert len(rows) == 2
    assert rows[0]["order_id"] == "ORD-001"
    assert rows[0]["total_amount"] == 20.0
    assert rows[1]["order_id"] == "ORD-002"
    assert rows[1]["total_amount"] == 15.0


# ---------------------------------------------------------------------------
# Test 10: Idempotency
# ---------------------------------------------------------------------------

def test_idempotent_load(db_conn):
    """Running the same load twice produces identical counts, no duplicates."""
    raw = [
        make_valid_row("ORD-001"),
        make_valid_row("ORD-002"),
    ]
    transformed, fact, rejected = _build_fact_rows(raw)

    # First load
    fc1, ho1, rj1 = load_partition(
        db_conn, PARTITION_DATE, transformed, fact, rejected, RUN_ID, SOURCE_FILE
    )
    # Second load (same data)
    fc2, ho2, rj2 = load_partition(
        db_conn, PARTITION_DATE, transformed, fact, rejected, RUN_ID, SOURCE_FILE
    )

    assert fc1 == fc2
    assert rj1 == rj2

    # No duplicate rows in fact_orders
    count = db_conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    assert count == 2


# ---------------------------------------------------------------------------
# Test 6: Late-arriving event does not overwrite newer fact row
# ---------------------------------------------------------------------------

def test_late_arriving_does_not_overwrite_newer_fact(db_conn):
    """
    Scenario: order ORD-001 with event_ts=14:00 is loaded first (newer).
    Then a late-arriving event with event_ts=08:00 is loaded.
    fact_orders should still show the 14:00 (shipped) row.
    Both events should be in order_status_history.
    """
    newer_ts = "2025-01-01T14:00:00+00:00"
    older_ts = "2025-01-01T08:00:00+00:00"

    newer_raw = [make_valid_row("ORD-001", status="shipped", event_ts=newer_ts)]
    t1, f1, r1 = _build_fact_rows(newer_raw)
    load_partition(db_conn, PARTITION_DATE, t1, f1, r1, RUN_ID, SOURCE_FILE)

    # Second partition: older event arrives late (08:00, placed)
    older_raw = [make_valid_row("ORD-001", status="placed", event_ts=older_ts)]
    numbered = [{**r, "_row_number": i} for i, r in enumerate(older_raw, 1)]
    valid, rejected, _ = validate_rows(
        rows=numbered,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file="orders_2025-01-02.csv",
        partition_date="2025-01-02",
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )
    transformed2 = [transform_row(r, "2025-01-02", RUN_ID, "orders_2025-01-02.csv", NOW_TS) for r in valid]
    fact2 = resolve_latest_per_order(transformed2)
    load_partition(db_conn, "2025-01-02", transformed2, fact2, rejected, RUN_ID, "orders_2025-01-02.csv")

    # fact_orders should still show the NEWER (shipped/14:00) row
    fact_row = db_conn.execute(
        "SELECT status, event_ts FROM fact_orders WHERE order_id = 'ORD-001'"
    ).fetchone()
    assert fact_row is not None
    assert fact_row["status"] == "shipped"
    assert "14:00:00" in fact_row["event_ts"]

    # Both events should be in order_status_history
    history = db_conn.execute(
        "SELECT status FROM order_status_history WHERE order_id = 'ORD-001' ORDER BY event_ts"
    ).fetchall()
    statuses = [h["status"] for h in history]
    assert "placed" in statuses
    assert "shipped" in statuses


# ---------------------------------------------------------------------------
# Test: Rejected records are stored and traceable
# ---------------------------------------------------------------------------

def test_rejected_records_stored(db_conn):
    """Rejected rows are written to rejected_records with all required fields."""
    raw = [
        make_valid_row("ORD-GOOD"),
        make_valid_row("ORD-BAD", customer_id="C9999"),  # invalid ref
    ]
    numbered = [{**r, "_row_number": i} for i, r in enumerate(raw, 1)]
    valid, rejected, _ = validate_rows(
        rows=numbered,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )
    transformed = [transform_row(r, PARTITION_DATE, RUN_ID, SOURCE_FILE, NOW_TS) for r in valid]
    fact = resolve_latest_per_order(transformed)
    _, _, rows_rejected = load_partition(db_conn, PARTITION_DATE, transformed, fact, rejected, RUN_ID, SOURCE_FILE)

    assert rows_rejected == 1

    rej = db_conn.execute("SELECT * FROM rejected_records").fetchall()
    assert len(rej) == 1
    assert rej[0]["reject_reason"] == "invalid_reference_customer"
    assert rej[0]["run_id"] == RUN_ID
    assert rej[0]["source_file"] == SOURCE_FILE
    assert rej[0]["row_number"] is not None
    assert rej[0]["raw_record"] is not None


# ---------------------------------------------------------------------------
# Test: order_status_history is append-only
# ---------------------------------------------------------------------------

def test_status_history_append_only(db_conn):
    """Loading the same partition twice does not duplicate history rows."""
    ts1 = "2025-01-01T08:00:00+00:00"
    ts2 = "2025-01-01T12:00:00+00:00"
    raw = [
        make_valid_row("ORD-001", status="placed",  event_ts=ts1),
        make_valid_row("ORD-001", status="shipped", event_ts=ts2),
    ]
    transformed, fact, rejected = _build_fact_rows(raw)

    load_partition(db_conn, PARTITION_DATE, transformed, fact, rejected, RUN_ID, SOURCE_FILE)
    load_partition(db_conn, PARTITION_DATE, transformed, fact, rejected, RUN_ID, SOURCE_FILE)

    history = db_conn.execute(
        "SELECT COUNT(*) AS cnt FROM order_status_history WHERE order_id = 'ORD-001'"
    ).fetchone()["cnt"]
    assert history == 2  # NOT 4


# ---------------------------------------------------------------------------
# Test: empty batch loads as success with 0 rows
# ---------------------------------------------------------------------------

def test_empty_batch_load(db_conn):
    """Empty batch (no valid rows, no rejects) loads successfully with 0 counts."""
    fact_count, history_only, rows_rejected = load_partition(
        db_conn, PARTITION_DATE, [], [], [], RUN_ID, SOURCE_FILE
    )
    assert fact_count == 0
    assert history_only == 0
    assert rows_rejected == 0
    count = db_conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    assert count == 0


# ---------------------------------------------------------------------------
# Test: history_only rows are counted correctly
# ---------------------------------------------------------------------------

def test_history_only_rows_counted(db_conn):
    """
    An order with 3 status updates in one partition produces:
    - fact_count = 1 (latest event_ts only)
    - history_only = 2 (older events, in order_status_history only)
    """
    ts1 = "2025-01-01T08:00:00+00:00"
    ts2 = "2025-01-01T12:00:00+00:00"
    ts3 = "2025-01-01T18:00:00+00:00"
    raw = [
        make_valid_row("ORD-001", status="placed",    event_ts=ts1),
        make_valid_row("ORD-001", status="paid",      event_ts=ts2),
        make_valid_row("ORD-001", status="delivered", event_ts=ts3),
    ]
    transformed, fact, rejected = _build_fact_rows(raw)

    fact_count, history_only, rows_rejected = load_partition(
        db_conn, PARTITION_DATE, transformed, fact, rejected, RUN_ID, SOURCE_FILE
    )

    assert fact_count == 1       # only latest (delivered, ts3) in fact_orders
    assert history_only == 2     # placed + paid go to history only
    assert rows_rejected == 0

    # fact_orders has 1 row: the delivered event
    fact_row = db_conn.execute("SELECT status FROM fact_orders WHERE order_id='ORD-001'").fetchone()
    assert fact_row["status"] == "delivered"

    # All 3 events in order_status_history
    history_count = db_conn.execute(
        "SELECT COUNT(*) FROM order_status_history WHERE order_id='ORD-001'"
    ).fetchone()[0]
    assert history_count == 3


# ---------------------------------------------------------------------------
# Test: reject records are replaced on rerun (Bug 1 fix: DELETE by partition_date only)
# ---------------------------------------------------------------------------

def test_reject_records_idempotent(db_conn):
    """
    Re-running the same partition (even with a different run_id) gives identical
    rejected_records count — no accumulation across reruns.

    This validates the Bug 1 fix: DELETE by partition_date only, not partition_date+run_id.
    """
    raw = [make_valid_row("ORD-BAD", customer_id="C9999")]
    numbered = [{**r, "_row_number": 1} for r in raw]
    _, rejected_first, _ = validate_rows(
        rows=numbered,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id="run_first",
        now_ts=NOW_TS,
    )
    _, rejected_second, _ = validate_rows(
        rows=numbered,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id="run_second",
        now_ts=NOW_TS,
    )

    # Run 1
    load_partition(db_conn, PARTITION_DATE, [], [], rejected_first, "run_first", SOURCE_FILE)
    count_after_run1 = db_conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]
    assert count_after_run1 == 1

    # Run 2 (new run_id — old rejects should be REPLACED, not accumulated)
    load_partition(db_conn, PARTITION_DATE, [], [], rejected_second, "run_second", SOURCE_FILE)
    count_after_run2 = db_conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]
    assert count_after_run2 == 1  # NOT 2
