"""
Tests for reconcile.py: all reconciliation checks.

Test 14: rows_in == rows_out + rows_rejected, PK uniqueness, FK integrity,
          total_amount reconciliation, reject traceability.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backfill.controller import run_backfill
from backfill.db import get_connection
from backfill.reconcile import run_reconcile
from tests.conftest import (
    SAMPLE_CUSTOMERS, SAMPLE_PRODUCTS,
    write_orders_csv, make_valid_row,
)


def _setup_source(src_dir, days_data):
    from tests.conftest import _write_csv
    orders_dir = src_dir / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    for date_str, rows in days_data.items():
        path = orders_dir / f"orders_{date_str}.csv"
        write_orders_csv(path, rows)
    _write_csv(src_dir / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src_dir / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])


# ---------------------------------------------------------------------------
# Test 14: Reconciliation report passes
# ---------------------------------------------------------------------------

def test_reconciliation_passes(tmp_path):
    """All reconciliation checks pass after a clean run."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    output = tmp_path / "output"

    days = {
        "2025-01-01": [
            make_valid_row("ORD-001", quantity=2, unit_price=10.0),
            make_valid_row("ORD-002", quantity=1, unit_price=5.0),
            make_valid_row("ORD-003", customer_id="C9999"),  # will be rejected
        ],
        "2025-01-02": [
            make_valid_row("ORD-004", quantity=3, unit_price=20.0),
        ],
    }
    _setup_source(src, days)

    _, _, run_id = run_backfill(
        partitions=["2025-01-01", "2025-01-02"],
        source_dir=src,
        db_path=db,
        max_workers=1,
        run_id="run_recon_test",
    )

    report = run_reconcile(db_path=db, run_id="run_recon_test", output_dir=output)

    assert report["passed"] is True
    assert report["checks"]["fact_orders_pk_unique"]["passed"] is True
    assert report["checks"]["referential_integrity"]["passed"] is True
    assert report["checks"]["total_amount_reconciliation"]["passed"] is True
    assert report["checks"]["reject_traceability"]["passed"] is True


def test_reconciliation_row_balance(tmp_path):
    """rows_in == rows_out + rows_rejected for each partition."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    output = tmp_path / "output"

    # Mix of valid and invalid rows
    rows_01 = [
        make_valid_row("ORD-001"),
        make_valid_row("ORD-002", customer_id="C9999"),  # reject
        make_valid_row("ORD-003", unit_price=-1),        # reject
    ]
    _setup_source(src, {"2025-01-01": rows_01})

    _, _, run_id = run_backfill(
        partitions=["2025-01-01"],
        source_dir=src,
        db_path=db,
        max_workers=1,
    )

    report = run_reconcile(db_path=db, run_id=run_id, output_dir=output)
    detail = report["checks"]["partition_row_balance"]["details"][0]

    assert detail["rows_in"] == 3
    assert detail["rows_out"] == 1
    assert detail["rows_rejected"] == 2
    assert detail["balanced"] is True


def test_reconciliation_report_written(tmp_path):
    """Reconciliation report JSON is written to the output directory."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    output = tmp_path / "output"

    _setup_source(src, {"2025-01-01": [make_valid_row("ORD-001")]})
    _, _, run_id = run_backfill(["2025-01-01"], src, db, max_workers=1)

    run_reconcile(db_path=db, run_id=run_id, output_dir=output)

    assert (output / "reconciliation_report.json").exists()


def test_reconciliation_idempotent(tmp_path):
    """Running reconcile twice produces the same results."""
    src = tmp_path / "source"
    db = tmp_path / "warehouse.db"
    output = tmp_path / "output"

    _setup_source(src, {"2025-01-01": [make_valid_row("ORD-001")]})
    _, _, run_id = run_backfill(["2025-01-01"], src, db, max_workers=1)

    report1 = run_reconcile(db_path=db, run_id=run_id, output_dir=output)
    report2 = run_reconcile(db_path=db, run_id=run_id, output_dir=output)

    assert report1["checks"]["fact_orders_pk_unique"] == report2["checks"]["fact_orders_pk_unique"]
