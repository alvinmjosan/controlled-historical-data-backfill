"""
Tests for extract.py: CSV reading, schema validation, empty/missing files.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pytest

from backfill.extract import read_partition, SchemaError
from tests.conftest import write_orders_csv, make_valid_row, ORDERS_COLUMNS


def test_read_partition_valid(tmp_path):
    """Happy path: valid CSV with correct header returns rows and no error."""
    path = tmp_path / "orders_2025-01-01.csv"
    rows = [make_valid_row("ORD-001"), make_valid_row("ORD-002")]
    write_orders_csv(path, rows)

    result, error = read_partition(path)
    assert error is None
    assert len(result) == 2
    assert result[0]["order_id"] == "ORD-001"
    assert result[0]["_row_number"] == 1
    assert result[1]["_row_number"] == 2


def test_read_partition_empty_file(tmp_path):
    """Empty file (0 bytes) returns [] with no error (valid empty batch)."""
    path = tmp_path / "orders_2025-01-01.csv"
    path.write_bytes(b"")

    result, error = read_partition(path)
    assert error is None
    assert result == []


def test_read_partition_header_only(tmp_path):
    """Header-only file (no data rows) returns [] with no error."""
    path = tmp_path / "orders_2025-01-01.csv"
    with open(path, "w", newline="") as fh:
        fh.write(",".join(ORDERS_COLUMNS) + "\n")

    result, error = read_partition(path)
    assert error is None
    assert result == []


def test_read_partition_missing_file(tmp_path):
    """Missing file returns ([], 'source_file_not_found')."""
    path = tmp_path / "orders_2025-01-99.csv"

    result, error = read_partition(path)
    assert result == []
    assert error == "source_file_not_found"


def test_read_partition_schema_mismatch_missing_column(tmp_path):
    """Schema mismatch (missing required column) returns error string."""
    path = tmp_path / "orders_2025-01-01.csv"
    bad_cols = [c for c in ORDERS_COLUMNS if c != "status"]
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(bad_cols)
        writer.writerow(["ORD-001", "C0001", "P0001", "1", "10.00", "2025-01-01T10:00:00Z"])

    result, error = read_partition(path)
    assert result == []
    assert "schema_mismatch" in error
    assert "status" in error


def test_read_partition_extra_column_ignored(tmp_path, capsys):
    """Extra unknown column is ignored with a warning; rows still returned."""
    path = tmp_path / "orders_2025-01-01.csv"
    extra_cols = ORDERS_COLUMNS + ["extra_col"]
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=extra_cols)
        writer.writeheader()
        row = make_valid_row()
        row["extra_col"] = "ignored_value"
        writer.writerow(row)

    result, error = read_partition(path)
    assert error is None
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Malformed row detection via the real CSV reader path
# (these cover the gap that the earlier bug slipped through)
# ---------------------------------------------------------------------------

def test_short_row_flagged_malformed_and_rejected(tmp_path):
    """
    A row with fewer fields than the header (e.g. BAD_ROW,missing_cols)
    must come out of read_partition with _malformed=True and then be
    rejected as malformed_row by validate_rows.

    csv.DictReader fills the missing keys with None; extract.py detects
    those None values before stripping them and sets the sentinel.
    """
    from backfill.validate import validate_rows
    from tests.conftest import VALID_CUSTOMER_IDS, VALID_PRODUCT_IDS
    from datetime import datetime, timezone

    path = tmp_path / "orders_2025-01-01.csv"
    # Write a valid header (7 cols) then a 2-field data row — exactly like
    # the real BAD_ROW,missing_cols row in data/sample.
    path.write_text(
        "order_id,customer_id,product_id,quantity,unit_price,status,event_ts\n"
        "BAD_ROW,missing_cols\n",
        encoding="utf-8",
    )

    rows, error = read_partition(path)
    assert error is None, f"unexpected schema error: {error}"
    assert len(rows) == 1

    bad_row = rows[0]
    # The sentinel must be set by extract.py before None values were stripped
    assert bad_row.get("_malformed") is True, (
        "extract.py should have set _malformed=True for a short row"
    )

    # Feed into validate_rows — must be rejected as malformed_row, not
    # missing_required_field:product_id (the old incorrect label)
    now_ts = datetime.now(timezone.utc).isoformat()
    valid, rejected, _ = validate_rows(
        rows=rows,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file="orders_2025-01-01.csv",
        partition_date="2025-01-01",
        run_id="run_test",
        now_ts=now_ts,
    )
    assert len(valid) == 0
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "malformed_row", (
        f"expected malformed_row, got {rejected[0]['reject_reason']!r}"
    )


def test_long_row_flagged_malformed_and_rejected(tmp_path):
    """
    A row with MORE fields than the header must also be rejected as malformed_row.

    csv.DictReader stores the extra values under the key None in raw_row.
    The sentinel check (any(v is None for v in raw_row.values())) does NOT
    catch this case — None is a key, not a value.  extract.py therefore also
    checks for None in raw_row (i.e. None in raw_row), which is True when
    DictReader uses None as the overflow key.
    """
    from backfill.validate import validate_rows
    from tests.conftest import VALID_CUSTOMER_IDS, VALID_PRODUCT_IDS
    from datetime import datetime, timezone

    path = tmp_path / "orders_2025-01-01.csv"
    # 7-column header, 9-field data row (2 extra values)
    path.write_text(
        "order_id,customer_id,product_id,quantity,unit_price,status,event_ts\n"
        "ORD-LONG,C0001,P0001,1,10.00,placed,2025-01-01T10:00:00+00:00,extra1,extra2\n",
        encoding="utf-8",
    )

    rows, error = read_partition(path)
    assert error is None, f"unexpected schema error: {error}"
    assert len(rows) == 1

    bad_row = rows[0]
    assert bad_row.get("_malformed") is True, (
        "extract.py should have set _malformed=True for a long (over-field) row"
    )

    now_ts = datetime.now(timezone.utc).isoformat()
    valid, rejected, _ = validate_rows(
        rows=rows,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file="orders_2025-01-01.csv",
        partition_date="2025-01-01",
        run_id="run_test",
        now_ts=now_ts,
    )
    assert len(valid) == 0
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "malformed_row", (
        f"expected malformed_row, got {rejected[0]['reject_reason']!r}"
    )
