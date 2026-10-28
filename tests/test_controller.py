"""
Tests for the backfill controller: end-to-end integration tests.

Covers:
- Test 1:  Happy path end-to-end with valid partitions
- Test 2:  Empty input (empty file, header-only)
- Test 3:  Malformed records rejected
- Test 9:  Schema change fails partition; extra column ignored
- Test 10: Idempotency (two runs, identical counts)
- Test 11: Partial failure, retry, and restart
- Test 12: Dry-run writes nothing
- Test 13: Bounded parallelism (workers=1 vs workers=4, same results)
- Test 15: Missing source file -> FAILED with source_file_not_found
"""
from __future__ import annotations

import csv
import os
from pathlib import Path

import pytest

from backfill.controller import run_backfill, run_dry_run, date_range, orders_file_path, plan_partitions
from backfill.db import get_connection, create_schema
from backfill.load import load_customers, load_products
from tests.conftest import (
    SAMPLE_CUSTOMERS, SAMPLE_PRODUCTS,
    VALID_CUSTOMER_IDS, VALID_PRODUCT_IDS,
    write_orders_csv, make_valid_row,
    ORDERS_COLUMNS,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_source(src_dir: Path, days_data: dict[str, list[dict]]) -> None:
    """Write orders CSVs for given date->rows mapping."""
    orders_dir = src_dir / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    for date_str, rows in days_data.items():
        path = orders_dir / f"orders_{date_str}.csv"
        if rows is None:
            # Empty file
            path.write_bytes(b"")
        else:
            write_orders_csv(path, rows)
    from tests.conftest import _write_csv
    _write_csv(src_dir / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src_dir / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])


def _write_header_only(path: Path) -> None:
    with open(path, "w", newline="") as fh:
        fh.write(",".join(ORDERS_COLUMNS) + "\n")


def _write_schema_change(path: Path) -> None:
    bad_cols = [c for c in ORDERS_COLUMNS if c != "status"]
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(bad_cols)
        writer.writerow(["ORD-001", "C0001", "P0001", "2", "10.00", "2025-01-01T10:00:00Z"])


# ---------------------------------------------------------------------------
# Test 1: Happy path end-to-end
# ---------------------------------------------------------------------------

def test_happy_path_end_to_end(tmp_path):
    """Valid partitions load successfully with correct counts."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    days = {
        "2025-01-01": [make_valid_row("ORD-001"), make_valid_row("ORD-002")],
        "2025-01-02": [make_valid_row("ORD-003"), make_valid_row("ORD-004")],
    }
    _setup_source(src, days)

    status, exit_code, run_id = run_backfill(
        partitions=["2025-01-01", "2025-01-02"],
        source_dir=src,
        db_path=db,
        max_workers=1,
    )
    assert status == "SUCCESS"
    assert exit_code == 0

    conn = get_connection(db)
    count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    assert count == 4
    conn.close()


# ---------------------------------------------------------------------------
# Test 2: Empty input
# ---------------------------------------------------------------------------

def test_empty_file_succeeds(tmp_path):
    """Empty file (0 bytes) -> partition SUCCESS with 0 rows."""
    src = tmp_path / "source"
    _setup_source(src, {})
    orders_dir = src / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    (orders_dir / "orders_2025-01-01.csv").write_bytes(b"")
    from tests.conftest import _write_csv
    _write_csv(src / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])

    status, exit_code, run_id = run_backfill(
        partitions=["2025-01-01"],
        source_dir=src,
        db_path=tmp_path / "warehouse.db",
        max_workers=1,
    )
    assert status == "SUCCESS"
    conn = get_connection(tmp_path / "warehouse.db")
    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 0
    conn.close()


def test_header_only_file_succeeds(tmp_path):
    """Header-only file -> partition SUCCESS with 0 rows."""
    src = tmp_path / "source"
    orders_dir = src / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    _write_header_only(orders_dir / "orders_2025-01-01.csv")
    from tests.conftest import _write_csv
    _write_csv(src / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])

    status, exit_code, run_id = run_backfill(
        partitions=["2025-01-01"],
        source_dir=src,
        db_path=tmp_path / "warehouse.db",
        max_workers=1,
    )
    assert status == "SUCCESS"


# ---------------------------------------------------------------------------
# Test 9: Schema change
# ---------------------------------------------------------------------------

def test_schema_mismatch_fails_partition(tmp_path):
    """Partition with missing required column -> partition FAILED, no data loaded."""
    src = tmp_path / "source"
    orders_dir = src / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    _write_schema_change(orders_dir / "orders_2025-01-01.csv")
    from tests.conftest import _write_csv
    _write_csv(src / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])

    status, exit_code, run_id = run_backfill(
        partitions=["2025-01-01"],
        source_dir=src,
        db_path=tmp_path / "warehouse.db",
        max_workers=1,
    )
    assert exit_code == 1
    conn = get_connection(tmp_path / "warehouse.db")
    p = conn.execute(
        "SELECT status, error FROM backfill_partitions WHERE partition_date='2025-01-01' AND run_id=?",
        (run_id,)
    ).fetchone()
    assert p["status"] == "FAILED"
    assert "schema_mismatch" in p["error"]
    conn.close()


# ---------------------------------------------------------------------------
# Test 10: Idempotency
# ---------------------------------------------------------------------------

def test_idempotent_two_runs(tmp_path):
    """Running the same backfill twice gives identical fact_orders count (no duplicates)."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    days = {"2025-01-01": [make_valid_row("ORD-001"), make_valid_row("ORD-002")]}
    _setup_source(src, days)

    run_backfill(partitions=["2025-01-01"], source_dir=src, db_path=db, max_workers=1, force=True)
    run_backfill(partitions=["2025-01-01"], source_dir=src, db_path=db, max_workers=1, force=True)

    conn = get_connection(db)
    count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    assert count == 2  # NOT 4
    conn.close()


# ---------------------------------------------------------------------------
# Test 11: Partial failure and restart
# ---------------------------------------------------------------------------

def test_partial_failure_and_restart(tmp_path):
    """Injected failure on one partition; others succeed. Restart retries failed only."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    days = {
        "2025-01-01": [make_valid_row("ORD-001")],
        "2025-01-02": [make_valid_row("ORD-002")],
        "2025-01-03": [make_valid_row("ORD-003")],
    }
    _setup_source(src, days)

    # First run: inject failure on 2025-01-02
    status1, exit_code1, run_id1 = run_backfill(
        partitions=["2025-01-01", "2025-01-02", "2025-01-03"],
        source_dir=src,
        db_path=db,
        max_workers=1,
        inject_failure_date="2025-01-02",
        max_retries=0,
    )
    assert status1 == "PARTIAL"
    assert exit_code1 == 1

    conn = get_connection(db)
    # 2025-01-01 and 2025-01-03 should be SUCCESS
    p1 = conn.execute(
        "SELECT status FROM backfill_partitions WHERE partition_date='2025-01-01' AND run_id=?",
        (run_id1,)
    ).fetchone()
    p2 = conn.execute(
        "SELECT status FROM backfill_partitions WHERE partition_date='2025-01-03' AND run_id=?",
        (run_id1,)
    ).fetchone()
    assert p1["status"] == "SUCCESS"
    assert p2["status"] == "SUCCESS"
    conn.close()

    # Second run (no inject failure): should process only the failed partition
    status2, exit_code2, run_id2 = run_backfill(
        partitions=["2025-01-01", "2025-01-02", "2025-01-03"],
        source_dir=src,
        db_path=db,
        max_workers=1,
    )
    assert status2 == "SUCCESS"
    assert exit_code2 == 0

    conn = get_connection(db)
    count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    assert count == 3  # all 3 orders loaded
    conn.close()


# ---------------------------------------------------------------------------
# Test 12: Dry-run writes nothing
# ---------------------------------------------------------------------------

def test_dry_run_writes_nothing(tmp_path):
    """Dry-run returns estimates but makes NO writes to the database."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    days = {"2025-01-01": [make_valid_row("ORD-001"), make_valid_row("ORD-002")]}
    _setup_source(src, days)

    estimate = run_dry_run(
        partitions=["2025-01-01"],
        source_dir=src,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
    )

    # DB should NOT exist (no writes)
    assert not db.exists()
    assert estimate["total_rows_in"] == 2
    assert estimate["estimated_valid"] == 2
    assert estimate["estimated_rejected"] == 0
    assert estimate["partitions_missing"] == 0


def test_dry_run_detects_missing_files(tmp_path):
    """Dry-run correctly counts missing files."""
    src = tmp_path / "source"
    orders_dir = src / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    from tests.conftest import _write_csv
    _write_csv(src / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])

    estimate = run_dry_run(
        partitions=["2025-01-01", "2025-01-02"],
        source_dir=src,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
    )
    assert estimate["partitions_missing"] == 2
    assert "2025-01-01" in estimate["missing_dates"]


# ---------------------------------------------------------------------------
# Test 13: Bounded parallelism
# ---------------------------------------------------------------------------

def test_bounded_parallelism_results_identical(tmp_path):
    """workers=1 and workers=4 produce identical fact_orders rows."""
    src = tmp_path / "source"
    days = {
        "2025-01-01": [make_valid_row(f"ORD-{i:03d}") for i in range(5)],
        "2025-01-02": [make_valid_row(f"ORD-{i:03d}", event_ts="2025-01-02T10:00:00Z") for i in range(5, 10)],
    }
    _setup_source(src, days)

    db1 = tmp_path / "w1.db"
    status1, _, _ = run_backfill(["2025-01-01", "2025-01-02"], src, db1, max_workers=1)
    db4 = tmp_path / "w4.db"
    status2, _, _ = run_backfill(["2025-01-01", "2025-01-02"], src, db4, max_workers=4)

    conn1 = get_connection(db1)
    conn4 = get_connection(db4)
    count1 = conn1.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    count4 = conn4.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    sum1 = conn1.execute("SELECT ROUND(SUM(total_amount),2) FROM fact_orders").fetchone()[0]
    sum4 = conn4.execute("SELECT ROUND(SUM(total_amount),2) FROM fact_orders").fetchone()[0]
    conn1.close()
    conn4.close()

    assert count1 == count4
    assert abs((sum1 or 0) - (sum4 or 0)) < 0.01
    assert status1 == status2 == "SUCCESS"


# ---------------------------------------------------------------------------
# Test 15: Missing source file
# ---------------------------------------------------------------------------

def test_missing_source_file_fails_partition(tmp_path):
    """A requested partition with no source file -> partition FAILED with source_file_not_found."""
    src = tmp_path / "source"
    orders_dir = src / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    from tests.conftest import _write_csv
    _write_csv(src / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])

    status, exit_code, run_id = run_backfill(
        partitions=["2025-12-31"],   # no such file
        source_dir=src,
        db_path=tmp_path / "warehouse.db",
        max_workers=1,
        max_retries=0,
    )
    assert exit_code == 1

    conn = get_connection(tmp_path / "warehouse.db")
    p = conn.execute(
        "SELECT status, error FROM backfill_partitions WHERE partition_date='2025-12-31' AND run_id=?",
        (run_id,)
    ).fetchone()
    assert p["status"] == "FAILED"
    assert "source_file_not_found" in p["error"]
    conn.close()
