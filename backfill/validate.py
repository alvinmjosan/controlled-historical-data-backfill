"""
Validate module: row-level business rule checks and duplicate detection.

Each failing row produces a reject dict with a clear reject_reason.
Valid rows are returned for downstream transformation.

Rules applied in order:
1. Malformed row (wrong column count / missing _row_number sentinel)
2. Required fields not null/empty
3. quantity is positive integer
4. unit_price is number >= 0
5. status in VALID_STATUSES
6. event_ts parses as ISO-8601
7. customer_id exists in dim_customers
8. product_id exists in dim_products
9. Exact duplicate (order_id + event_ts + status) -> keep first, reject rest
   NOTE: same order_id with different status/event_ts is NOT a duplicate (status update).
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from backfill.config import ORDERS_REQUIRED_COLUMNS, VALID_STATUSES

logger = logging.getLogger(__name__)


def _make_reject_id(source_file: str, row_number: int) -> str:
    """Deterministic SHA-256 hash of source_file + row_number."""
    raw = f"{source_file}:{row_number}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _raw_record(row: dict) -> str:
    """Serialize the raw row (excluding internal keys) to a JSON string."""
    clean = {k: v for k, v in row.items() if not k.startswith("_")}
    return json.dumps(clean, ensure_ascii=False)


def _make_reject(
    row: dict,
    source_file: str,
    partition_date: str,
    reason: str,
    run_id: str,
    now_ts: str,
) -> dict:
    """Build a reject record dict from a row and a reason string."""
    row_number = row.get("_row_number", 0)
    return {
        "reject_id": _make_reject_id(source_file, row_number),
        "run_id": run_id,
        "partition_date": partition_date,
        "source_file": source_file,
        "row_number": row_number,
        "raw_record": _raw_record(row),
        "reject_reason": reason,
        "rejected_at": now_ts,
    }


def _parse_iso8601(ts_str: str) -> datetime | None:
    """
    Try to parse ts_str as an ISO-8601 datetime.

    Returns a timezone-aware datetime on success, None on failure.
    """
    # Try common formats including with/without timezone offset
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def validate_rows(
    rows: list[dict],
    valid_customers: set[str],
    valid_products: set[str],
    source_file: str,
    partition_date: str,
    run_id: str,
    now_ts: str,
) -> tuple[list[dict], list[dict], int]:
    """
    Validate a list of raw CSV row dicts.

    Returns:
        (valid_rows, rejected_rows, late_arriving_count)

    valid_rows: rows that passed all checks, augmented with parsed values.
    rejected_rows: reject dicts ready for the rejected_records table.
    late_arriving_count: rows where event_ts.date() != partition_date.
    """
    valid_rows: list[dict] = []
    rejected_rows: list[dict] = []
    late_arriving_count: int = 0

    # Seen set for exact duplicate detection: (order_id, event_ts, status)
    seen_events: set[tuple[str, str, str]] = set()

    for row in rows:
        # ----------------------------------------------------------------
        # Rule 1: malformed row (wrong column count)
        # ----------------------------------------------------------------
        present_data_cols = [k for k in row if not k.startswith("_")]
        if len(present_data_cols) < len(ORDERS_REQUIRED_COLUMNS):
            rejected_rows.append(
                _make_reject(row, source_file, partition_date, "malformed_row", run_id, now_ts)
            )
            continue

        # ----------------------------------------------------------------
        # Rule 2: required fields not null/empty
        # ----------------------------------------------------------------
        missing_field: str | None = None
        for col in ORDERS_REQUIRED_COLUMNS:
            val = row.get(col, "")
            if val is None or str(val).strip() == "":
                missing_field = col
                break
        if missing_field:
            rejected_rows.append(
                _make_reject(
                    row,
                    source_file,
                    partition_date,
                    f"missing_required_field:{missing_field}",
                    run_id,
                    now_ts,
                )
            )
            continue

        # ----------------------------------------------------------------
        # Rule 3: quantity is positive integer
        # ----------------------------------------------------------------
        try:
            qty = int(row["quantity"])
            if qty < 1:
                raise ValueError("quantity < 1")
        except (ValueError, TypeError):
            rejected_rows.append(
                _make_reject(row, source_file, partition_date, "invalid_quantity", run_id, now_ts)
            )
            continue

        # ----------------------------------------------------------------
        # Rule 4: unit_price is number >= 0
        # ----------------------------------------------------------------
        try:
            price = float(row["unit_price"])
            if price < 0:
                raise ValueError("unit_price < 0")
        except (ValueError, TypeError):
            rejected_rows.append(
                _make_reject(
                    row, source_file, partition_date, "invalid_unit_price", run_id, now_ts
                )
            )
            continue

        # ----------------------------------------------------------------
        # Rule 5: status in VALID_STATUSES
        # ----------------------------------------------------------------
        if row["status"].strip().lower() not in VALID_STATUSES:
            rejected_rows.append(
                _make_reject(row, source_file, partition_date, "invalid_status", run_id, now_ts)
            )
            continue

        # ----------------------------------------------------------------
        # Rule 6: event_ts parses as ISO-8601
        # ----------------------------------------------------------------
        parsed_dt = _parse_iso8601(row["event_ts"].strip())
        if parsed_dt is None:
            rejected_rows.append(
                _make_reject(
                    row, source_file, partition_date, "invalid_event_ts", run_id, now_ts
                )
            )
            continue

        # ----------------------------------------------------------------
        # Rule 7: customer_id reference check
        # ----------------------------------------------------------------
        if row["customer_id"] not in valid_customers:
            rejected_rows.append(
                _make_reject(
                    row,
                    source_file,
                    partition_date,
                    "invalid_reference_customer",
                    run_id,
                    now_ts,
                )
            )
            continue

        # ----------------------------------------------------------------
        # Rule 8: product_id reference check
        # ----------------------------------------------------------------
        if row["product_id"] not in valid_products:
            rejected_rows.append(
                _make_reject(
                    row,
                    source_file,
                    partition_date,
                    "invalid_reference_product",
                    run_id,
                    now_ts,
                )
            )
            continue

        # ----------------------------------------------------------------
        # Rule 9: exact duplicate detection (order_id + event_ts + status)
        # NOTE: same order_id with different status/event_ts is a status update, NOT a duplicate.
        # ----------------------------------------------------------------
        event_key = (row["order_id"], row["event_ts"].strip(), row["status"].strip().lower())
        if event_key in seen_events:
            rejected_rows.append(
                _make_reject(
                    row, source_file, partition_date, "duplicate_event", run_id, now_ts
                )
            )
            continue
        seen_events.add(event_key)

        # ----------------------------------------------------------------
        # Late-arriving detection (event_ts.date() != partition_date)
        # ----------------------------------------------------------------
        event_date_str = parsed_dt.date().isoformat()
        if event_date_str != partition_date:
            late_arriving_count += 1
            logger.debug(
                "late_arriving: order_id=%s event_date=%s partition_date=%s",
                row["order_id"],
                event_date_str,
                partition_date,
            )

        # Augment with parsed values for transform
        row["_parsed_dt"] = parsed_dt
        row["_qty"] = qty
        row["_price"] = price
        valid_rows.append(row)

    return valid_rows, rejected_rows, late_arriving_count
