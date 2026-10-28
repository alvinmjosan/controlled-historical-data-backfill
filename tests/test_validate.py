"""
Tests for validate.py: all business rule checks.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backfill.validate import validate_rows
from tests.conftest import VALID_CUSTOMER_IDS, VALID_PRODUCT_IDS, make_valid_row

PARTITION_DATE = "2025-01-01"
SOURCE_FILE = "orders_2025-01-01.csv"
RUN_ID = "run_test"
NOW_TS = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc).isoformat()


def _validate(rows):
    """Convenience wrapper with default args."""
    # Add _row_number if not present
    numbered = []
    for i, r in enumerate(rows, start=1):
        row = dict(r)
        row.setdefault("_row_number", i)
        numbered.append(row)
    return validate_rows(
        rows=numbered,
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )


# ---------------------------------------------------------------------------
# Test 1: Happy path
# ---------------------------------------------------------------------------

def test_valid_row_passes():
    """A fully valid row passes all checks."""
    rows = [make_valid_row()]
    valid, rejected, late = _validate(rows)
    assert len(valid) == 1
    assert len(rejected) == 0
    assert late == 0


def test_multiple_valid_rows():
    """Multiple valid rows all pass."""
    rows = [make_valid_row(order_id=f"ORD-{i:03d}") for i in range(10)]
    valid, rejected, late = _validate(rows)
    assert len(valid) == 10
    assert len(rejected) == 0


# ---------------------------------------------------------------------------
# Test 2: Missing required fields
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", [
    "order_id", "customer_id", "product_id", "quantity", "unit_price", "status", "event_ts"
])
def test_missing_required_field(field):
    """Each required field being empty triggers missing_required_field:<name>."""
    row = make_valid_row()
    row[field] = ""
    row["_row_number"] = 1
    valid, rejected, late = validate_rows(
        rows=[row],
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )
    assert len(valid) == 0
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == f"missing_required_field:{field}"


# ---------------------------------------------------------------------------
# Test 3: Quantity validation
# ---------------------------------------------------------------------------

def test_invalid_quantity_zero():
    rows = [make_valid_row(quantity=0)]
    valid, rejected, _ = _validate(rows)
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_quantity"


def test_invalid_quantity_negative():
    rows = [make_valid_row(quantity=-5)]
    valid, rejected, _ = _validate(rows)
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_quantity"


def test_invalid_quantity_string():
    row = make_valid_row()
    row["quantity"] = "abc"
    valid, rejected, _ = _validate([row])
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_quantity"


def test_valid_quantity_one():
    """quantity=1 is valid (boundary)."""
    rows = [make_valid_row(quantity=1)]
    valid, rejected, _ = _validate(rows)
    assert len(valid) == 1
    assert len(rejected) == 0


# ---------------------------------------------------------------------------
# Test 4: Unit price validation
# ---------------------------------------------------------------------------

def test_invalid_unit_price_negative():
    rows = [make_valid_row(unit_price=-1.0)]
    valid, rejected, _ = _validate(rows)
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_unit_price"


def test_valid_unit_price_zero():
    """unit_price=0.0 is valid (free item)."""
    rows = [make_valid_row(unit_price=0.0)]
    valid, rejected, _ = _validate(rows)
    assert len(valid) == 1
    assert len(rejected) == 0


def test_invalid_unit_price_string():
    row = make_valid_row()
    row["unit_price"] = "free"
    valid, rejected, _ = _validate([row])
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_unit_price"


# ---------------------------------------------------------------------------
# Test 5: Status validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["placed", "paid", "shipped", "delivered", "cancelled"])
def test_valid_statuses(status):
    rows = [make_valid_row(status=status)]
    valid, rejected, _ = _validate(rows)
    assert len(valid) == 1
    assert len(rejected) == 0


@pytest.mark.parametrize("status", ["returned", "pending", "refunded", "RETURNED"])
def test_invalid_status(status):
    """Statuses not in the valid set are rejected as invalid_status.
    Note: case-insensitive comparison is NOT applied; exact lowercase match is required
    except where validate.py normalizes input. 'PLACED' would pass because
    we lower-case the status before checking — that is intentional behavior.
    """
    row = make_valid_row(status=status)
    valid, rejected, _ = _validate([row])
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_status"


def test_cancelled_status_is_valid():
    """cancelled status is explicitly valid per business rules."""
    rows = [make_valid_row(status="cancelled")]
    valid, rejected, _ = _validate(rows)
    assert len(valid) == 1


# ---------------------------------------------------------------------------
# Test 6: event_ts validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ts", [
    "2025-01-01T10:00:00+00:00",
    "2025-01-01T10:00:00Z",
    "2025-01-01T10:00:00",
    "2025-01-01 10:00:00",
])
def test_valid_event_ts_formats(ts):
    rows = [make_valid_row(event_ts=ts)]
    valid, rejected, _ = _validate(rows)
    assert len(valid) == 1


@pytest.mark.parametrize("ts", [
    "not-a-date",
    "01/01/2025",
    "2025/01/01",
    "2025-13-01T00:00:00",
])
def test_invalid_event_ts(ts):
    row = make_valid_row(event_ts=ts)
    valid, rejected, _ = _validate([row])
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_event_ts"


# ---------------------------------------------------------------------------
# Test 7: Reference key checks
# ---------------------------------------------------------------------------

def test_invalid_customer_reference():
    rows = [make_valid_row(customer_id="C9999")]  # non-existent
    valid, rejected, _ = _validate(rows)
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_reference_customer"


def test_invalid_product_reference():
    rows = [make_valid_row(product_id="P9999")]  # non-existent
    valid, rejected, _ = _validate(rows)
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "invalid_reference_product"


# ---------------------------------------------------------------------------
# Test 8: Duplicate detection
# ---------------------------------------------------------------------------

def test_exact_duplicate_rejected():
    """Exact duplicate (order_id + event_ts + status) — keep first, reject second."""
    ts = "2025-01-01T10:00:00+00:00"
    row1 = make_valid_row(order_id="ORD-DUP", event_ts=ts)
    row2 = make_valid_row(order_id="ORD-DUP", event_ts=ts)  # exact copy
    valid, rejected, _ = _validate([row1, row2])
    assert len(valid) == 1
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "duplicate_event"


def test_status_update_not_duplicate():
    """Same order_id but different status/event_ts is NOT a duplicate."""
    ts1 = "2025-01-01T08:00:00+00:00"
    ts2 = "2025-01-01T10:00:00+00:00"
    ts3 = "2025-01-01T14:00:00+00:00"
    rows = [
        make_valid_row(order_id="ORD-001", status="placed",    event_ts=ts1),
        make_valid_row(order_id="ORD-001", status="paid",      event_ts=ts2),
        make_valid_row(order_id="ORD-001", status="delivered", event_ts=ts3),
    ]
    valid, rejected, _ = _validate(rows)
    assert len(valid) == 3
    assert len(rejected) == 0


# ---------------------------------------------------------------------------
# Test 9: Late-arriving detection
# ---------------------------------------------------------------------------

def test_late_arriving_counted():
    """A row whose event_ts date differs from partition_date is counted as late-arriving."""
    ts_yesterday = "2024-12-31T23:59:00+00:00"  # previous day
    rows = [make_valid_row(event_ts=ts_yesterday)]
    valid, rejected, late = validate_rows(
        rows=[{**rows[0], "_row_number": 1}],
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date="2025-01-01",
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )
    assert len(valid) == 1   # row is accepted
    assert len(rejected) == 0
    assert late == 1          # but flagged as late


def test_on_time_row_not_counted_as_late():
    rows = [make_valid_row(event_ts="2025-01-01T12:00:00+00:00")]
    valid, rejected, late = validate_rows(
        rows=[{**rows[0], "_row_number": 1}],
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date="2025-01-01",
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )
    assert late == 0


# ---------------------------------------------------------------------------
# Test 10: Malformed row
# ---------------------------------------------------------------------------

def test_malformed_row_too_few_columns():
    """A row with too few data columns is rejected as malformed_row."""
    row = {"order_id": "ORD-001", "_row_number": 1}  # only 1 real column
    valid, rejected, _ = validate_rows(
        rows=[row],
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id=RUN_ID,
        now_ts=NOW_TS,
    )
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "malformed_row"


# ---------------------------------------------------------------------------
# Test 11: Reject ID determinism
# ---------------------------------------------------------------------------

def test_reject_id_is_deterministic():
    """Same source_file + row_number always produces the same reject_id."""
    row = make_valid_row(customer_id="C9999")
    row["_row_number"] = 5
    _, rejected1, _ = validate_rows(
        rows=[row],
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id="run_A",
        now_ts=NOW_TS,
    )
    _, rejected2, _ = validate_rows(
        rows=[row],
        valid_customers=VALID_CUSTOMER_IDS,
        valid_products=VALID_PRODUCT_IDS,
        source_file=SOURCE_FILE,
        partition_date=PARTITION_DATE,
        run_id="run_B",
        now_ts=NOW_TS,
    )
    assert rejected1[0]["reject_id"] == rejected2[0]["reject_id"]
