"""
Extract module: read source CSV partitions and validate the header schema.

Responsibilities:
- Open the CSV file and detect encoding issues.
- Validate that all required columns are present (fail partition on mismatch).
- Warn on extra/unknown columns (but continue).
- Handle empty file and header-only file gracefully (return [] with success).
- Yield rows as plain dicts for downstream processing.
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Iterator

from backfill.config import (
    ORDERS_REQUIRED_COLUMNS,
    CUSTOMERS_REQUIRED_COLUMNS,
    PRODUCTS_REQUIRED_COLUMNS,
)

logger = logging.getLogger(__name__)


class SchemaError(Exception):
    """Raised when a partition file has missing required columns."""

    pass


def _check_header(
    header: list[str],
    required_columns: list[str],
    source_file: str,
) -> None:
    """
    Validate CSV header against required columns.

    Raises SchemaError if any required column is absent.
    Logs a warning for extra unknown columns.
    """
    header_set = set(h.strip() for h in header)
    required_set = set(required_columns)

    missing = sorted(required_set - header_set)
    if missing:
        raise SchemaError(f"schema_mismatch: missing columns {missing}")

    extra = sorted(header_set - required_set)
    if extra:
        logger.warning("source_file=%s extra columns ignored: %s", source_file, extra)


def read_partition(
    file_path: str | Path,
) -> tuple[list[dict], str | None]:
    """
    Read an orders CSV partition and return (rows, schema_error).

    Returns:
        (rows, None)          on success (rows may be empty for empty/header-only files).
        ([], error_message)   when the header schema is invalid.

    Each row dict has string keys matching the CSV header columns.
    Row dicts include a special '_row_number' key (1-indexed, excluding header).
    """
    path = Path(file_path)
    source_file = path.name

    if not path.exists():
        return [], "source_file_not_found"

    try:
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)

            # Empty file (no header at all)
            if reader.fieldnames is None:
                logger.info("source_file=%s is completely empty (0 bytes)", source_file)
                return [], None

            try:
                _check_header(list(reader.fieldnames), ORDERS_REQUIRED_COLUMNS, source_file)
            except SchemaError as exc:
                return [], str(exc)

            rows: list[dict] = []
            for row_number, raw_row in enumerate(reader, start=1):
                # csv.DictReader signals malformed rows in two ways:
                #   short row: fills missing header columns with None VALUES
                #   long row:  stores overflow values under the None KEY
                # Both are detected here, before None→"" stripping, so that
                # validate.py Rule 1 can reject them as malformed_row.
                is_malformed = (
                    any(v is None for v in raw_row.values())  # short row
                    or None in raw_row                         # long row
                )
                # Skip the None key (overflow bucket for long rows) and
                # convert None values (missing fields in short rows) to "".
                row = {
                    k.strip(): (v.strip() if v is not None else "")
                    for k, v in raw_row.items()
                    if k is not None
                }
                row["_row_number"] = row_number
                if is_malformed:
                    row["_malformed"] = True
                rows.append(row)

        if not rows:
            logger.info("source_file=%s is header-only (0 data rows)", source_file)

        return rows, None

    except (UnicodeDecodeError, csv.Error) as exc:
        logger.error("source_file=%s failed to parse: %s", source_file, exc)
        return [], f"parse_error: {exc}"


def read_reference_csv(
    file_path: str | Path,
    required_columns: list[str],
) -> list[dict]:
    """
    Read a reference CSV (customers or products).

    Returns list of row dicts. Raises FileNotFoundError or SchemaError on problems.
    """
    path = Path(file_path)
    source_file = path.name

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            return []
        _check_header(list(reader.fieldnames), required_columns, source_file)
        return [
            {k.strip(): (v.strip() if v is not None else "") for k, v in row.items()}
            for row in reader
        ]


def read_customers(customers_path: str | Path) -> list[dict]:
    """Read customers reference CSV."""
    return read_reference_csv(customers_path, CUSTOMERS_REQUIRED_COLUMNS)


def read_products(products_path: str | Path) -> list[dict]:
    """Read products reference CSV."""
    return read_reference_csv(products_path, PRODUCTS_REQUIRED_COLUMNS)
