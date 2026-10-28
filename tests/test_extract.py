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
