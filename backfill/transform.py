"""
Transform module: derive computed fields and normalize data for loading.

Responsibilities:
- Compute total_amount = round(quantity * unit_price, 2).
- Normalize event_ts to ISO-8601 UTC string.
- Attach run metadata (run_id, source_file, partition_date, loaded_at).
- Flag late-arriving rows (already counted in validate; here just preserved).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _to_utc_iso(dt: datetime) -> str:
    """Convert a datetime (aware or naive) to an ISO-8601 UTC string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def transform_row(
    row: dict,
    partition_date: str,
    run_id: str,
    source_file: str,
    loaded_at: str,
) -> dict:
    """
    Transform a validated row into a fact_orders-ready dict.

    Expects '_parsed_dt', '_qty', '_price' augmented by validate_rows.
    Returns a dict with all fact_orders columns.
    """
    parsed_dt: datetime = row["_parsed_dt"]
    qty: int = row["_qty"]
    price: float = row["_price"]

    total_amount = round(qty * price, 2)
    event_ts_utc = _to_utc_iso(parsed_dt)
    status_normalized = row["status"].strip().lower()

    return {
        "order_id": row["order_id"].strip(),
        "customer_id": row["customer_id"].strip(),
        "product_id": row["product_id"].strip(),
        "quantity": qty,
        "unit_price": price,
        "total_amount": total_amount,
        "status": status_normalized,
        "event_ts": event_ts_utc,
        "partition_date": partition_date,
        "run_id": run_id,
        "source_file": source_file,
        "loaded_at": loaded_at,
    }


def transform_rows(
    valid_rows: list[dict],
    partition_date: str,
    run_id: str,
    source_file: str,
    loaded_at: str,
) -> list[dict]:
    """
    Transform a list of validated rows into fact_orders-ready dicts.

    Returns a list of transformed row dicts.
    """
    return [
        transform_row(row, partition_date, run_id, source_file, loaded_at)
        for row in valid_rows
    ]


def resolve_latest_per_order(transformed_rows: list[dict]) -> list[dict]:
    """
    For each order_id, keep only the row with the latest event_ts.

    All rows (including older ones) will be written to order_status_history.
    Only the latest per order_id goes into fact_orders.
    Returns the de-duplicated list (one entry per order_id, latest event).
    """
    latest: dict[str, dict] = {}
    for row in transformed_rows:
        oid = row["order_id"]
        if oid not in latest:
            latest[oid] = row
        else:
            # Compare event_ts strings (ISO-8601 sorts lexicographically)
            if row["event_ts"] > latest[oid]["event_ts"]:
                latest[oid] = row
    return list(latest.values())
