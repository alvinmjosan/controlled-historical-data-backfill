"""
Tests for transform.py: total_amount computation, UTC normalization, latest-per-order.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backfill.transform import transform_row, transform_rows, resolve_latest_per_order


def _make_validated_row(
    order_id="ORD-001",
    customer_id="C0001",
    product_id="P0001",
    quantity=2,
    unit_price=10.0,
    status="placed",
    event_ts="2025-01-01T10:00:00+00:00",
):
    """Build a row dict as it would appear after validate_rows."""
    from backfill.validate import _parse_iso8601
    dt = _parse_iso8601(event_ts)
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "status": status,
        "event_ts": event_ts,
        "_row_number": 1,
        "_parsed_dt": dt,
        "_qty": quantity,
        "_price": unit_price,
    }


def test_total_amount_computed_correctly():
    row = _make_validated_row(quantity=3, unit_price=12.50)
    result = transform_row(row, "2025-01-01", "run_001", "orders_2025-01-01.csv", "2025-01-01T12:00:00+00:00")
    assert result["total_amount"] == round(3 * 12.50, 2)


def test_total_amount_rounded_to_2dp():
    """Floating point edge case: ensure rounding is applied."""
    row = _make_validated_row(quantity=3, unit_price=0.10)
    result = transform_row(row, "2025-01-01", "run_001", "orders_2025-01-01.csv", "2025-01-01T12:00:00+00:00")
    assert result["total_amount"] == 0.30


def test_unit_price_zero_total_amount_zero():
    """unit_price=0 -> total_amount=0 (free item)."""
    row = _make_validated_row(quantity=5, unit_price=0.0)
    result = transform_row(row, "2025-01-01", "run_001", "orders_2025-01-01.csv", "2025-01-01T12:00:00+00:00")
    assert result["total_amount"] == 0.0


def test_event_ts_normalized_to_utc():
    """event_ts is normalized to UTC ISO-8601 regardless of input timezone."""
    row = _make_validated_row(event_ts="2025-01-01T12:00:00+05:30")
    result = transform_row(row, "2025-01-01", "run_001", "orders_2025-01-01.csv", "2025-01-01T12:00:00+00:00")
    # +05:30 -> UTC = 2025-01-01T06:30:00
    assert "06:30:00" in result["event_ts"]
    assert result["event_ts"].endswith("+00:00")


def test_status_normalized_to_lowercase():
    row = _make_validated_row(status="PLACED")
    result = transform_row(row, "2025-01-01", "run_001", "orders_2025-01-01.csv", "2025-01-01T12:00:00+00:00")
    assert result["status"] == "placed"


def test_metadata_attached():
    """run_id, source_file, partition_date, loaded_at are set."""
    row = _make_validated_row()
    result = transform_row(row, "2025-01-01", "run_XYZ", "orders_2025-01-01.csv", "2025-01-01T12:00:00+00:00")
    assert result["run_id"] == "run_XYZ"
    assert result["source_file"] == "orders_2025-01-01.csv"
    assert result["partition_date"] == "2025-01-01"
    assert result["loaded_at"] == "2025-01-01T12:00:00+00:00"


def test_resolve_latest_per_order_keeps_newest():
    """resolve_latest_per_order keeps the row with the latest event_ts per order_id."""
    rows = [
        {**_make_validated_row(order_id="ORD-001", event_ts="2025-01-01T08:00:00+00:00"),
         "total_amount": 10.0, "partition_date": "2025-01-01", "run_id": "r1",
         "source_file": "f.csv", "loaded_at": "ts"},
        {**_make_validated_row(order_id="ORD-001", event_ts="2025-01-01T14:00:00+00:00"),
         "total_amount": 10.0, "partition_date": "2025-01-01", "run_id": "r1",
         "source_file": "f.csv", "loaded_at": "ts"},
    ]
    # Need proper transform output
    from backfill.transform import transform_row
    t_rows = [
        transform_row(
            _make_validated_row("ORD-001", event_ts="2025-01-01T08:00:00+00:00", status="placed"),
            "2025-01-01", "r1", "f.csv", "ts"
        ),
        transform_row(
            _make_validated_row("ORD-001", event_ts="2025-01-01T14:00:00+00:00", status="shipped"),
            "2025-01-01", "r1", "f.csv", "ts"
        ),
    ]
    latest = resolve_latest_per_order(t_rows)
    assert len(latest) == 1
    assert latest[0]["status"] == "shipped"
    assert "14:00:00" in latest[0]["event_ts"]


def test_resolve_latest_per_order_different_orders():
    """Different order_ids -> all rows kept."""
    from backfill.transform import transform_row
    t_rows = [
        transform_row(_make_validated_row("ORD-001"), "2025-01-01", "r1", "f.csv", "ts"),
        transform_row(_make_validated_row("ORD-002"), "2025-01-01", "r1", "f.csv", "ts"),
    ]
    latest = resolve_latest_per_order(t_rows)
    assert len(latest) == 2
