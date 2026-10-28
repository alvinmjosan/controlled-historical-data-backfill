"""
Integration test: validate that the actual pipeline output against data/sample/
matches the expected labels in data/expected/expected_sample_results.json.

This is the 'keep expected-answer labels separate from input data' check required
by the problem statement (Step 7 / Validation Rules).

Skipped automatically if data/sample/ or data/expected/ are missing (e.g. fresh clone
before running `python scripts/generate_data.py`).
"""
from __future__ import annotations

import json
import os
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
SAMPLE_DIR   = PROJECT_ROOT / "data" / "sample"
EXPECTED_FILE = PROJECT_ROOT / "data" / "expected" / "expected_sample_results.json"
DB_PATH = None   # resolved per-test in tmp_path


# ---------------------------------------------------------------------------
# Skip guard: sample data must exist
# ---------------------------------------------------------------------------

def _sample_data_present() -> bool:
    return (
        SAMPLE_DIR.exists()
        and EXPECTED_FILE.exists()
        and (SAMPLE_DIR / "customers.csv").exists()
        and (SAMPLE_DIR / "products.csv").exists()
        and (SAMPLE_DIR / "orders").exists()
    )


pytestmark = pytest.mark.skipif(
    not _sample_data_present(),
    reason="data/sample/ or data/expected/ not present — run: python scripts/generate_data.py",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_expected() -> dict:
    with open(EXPECTED_FILE, encoding="utf-8") as fh:
        return json.load(fh)


def _run_sample_pipeline(db_path: Path) -> tuple[str, str]:
    """Run the full backfill against data/sample and return (final_status, run_id)."""
    from backfill.controller import run_backfill, discover_partitions
    partitions = discover_partitions(SAMPLE_DIR)
    assert partitions, "No order partitions found in data/sample/orders/"
    status, _, run_id = run_backfill(
        partitions=partitions,
        source_dir=SAMPLE_DIR,
        db_path=db_path,
        max_workers=1,
        max_retries=0,
        force=True,
    )
    return status, run_id


def _get_reject_map(db_path: Path, run_id: str) -> dict[str, str]:
    """Return {order_id: reject_reason} for all rejected rows in this run."""
    from backfill.db import get_connection
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT source_file, row_number, raw_record, reject_reason "
        "FROM rejected_records WHERE run_id = ?",
        (run_id,),
    ).fetchall()
    conn.close()
    result: dict[str, str] = {}
    for row in rows:
        try:
            raw = json.loads(row["raw_record"])
            oid = raw.get("order_id", "")
            if oid:
                result[oid] = row["reject_reason"]
        except (json.JSONDecodeError, KeyError):
            pass
    return result


def _get_partition_statuses(db_path: Path, run_id: str) -> dict[str, str]:
    """Return {partition_date: status} for this run."""
    from backfill.db import get_connection
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT partition_date, status, error FROM backfill_partitions WHERE run_id = ?",
        (run_id,),
    ).fetchall()
    conn.close()
    return {r["partition_date"]: r["status"] for r in rows}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_expected_labels_file_is_valid():
    """data/expected/expected_sample_results.json must be valid JSON with a labels list."""
    data = _load_expected()
    assert "labels" in data, "Expected file missing 'labels' key"
    assert isinstance(data["labels"], list), "'labels' must be a list"
    assert len(data["labels"]) > 0, "Expected labels list is empty"


def test_reject_reasons_match_expected_labels(tmp_path):
    """
    For every row labelled with an expected_reject_reason in the expected file,
    confirm the pipeline actually rejected that row with the same reason.
    """
    db_path = tmp_path / "warehouse.db"
    status, run_id = _run_sample_pipeline(db_path)

    expected = _load_expected()
    reject_map = _get_reject_map(db_path, run_id)

    mismatches = []
    for label in expected["labels"]:
        expected_reason = label.get("expected_reject_reason")
        order_id = label.get("order_id")
        if not expected_reason or not order_id:
            continue   # partition-level cases (schema_mismatch, empty_file) checked separately

        actual_reason = reject_map.get(order_id)
        if actual_reason != expected_reason:
            mismatches.append(
                f"order_id={order_id!r}: expected {expected_reason!r}, got {actual_reason!r}"
            )

    assert not mismatches, (
        f"{len(mismatches)} reject-reason mismatch(es):\n" + "\n".join(mismatches)
    )


def test_schema_mismatch_partition_fails(tmp_path):
    """
    The sample dataset has orders_2025-01-03.csv with a missing 'status' column.
    The expected labels say that partition must FAIL with schema_mismatch.
    """
    db_path = tmp_path / "warehouse.db"
    _, run_id = _run_sample_pipeline(db_path)

    expected = _load_expected()
    partition_statuses = _get_partition_statuses(db_path, run_id)

    for label in expected["labels"]:
        if label.get("case") == "schema_mismatch":
            # Derive the partition_date from the filename
            fname = label.get("file", "")
            date_str = fname.replace("orders_", "").replace(".csv", "")
            actual_status = partition_statuses.get(date_str)
            expected_status = label.get("expected_partition_status", "FAILED")
            assert actual_status == expected_status, (
                f"Partition {date_str}: expected status={expected_status!r}, "
                f"got {actual_status!r}"
            )


def test_empty_file_partition_succeeds_with_zero_rows(tmp_path):
    """
    The sample dataset has orders_2025-01-06.csv as a 0-byte empty file.
    The expected labels say that partition must succeed with rows_out=0.
    """
    db_path = tmp_path / "warehouse.db"
    _, run_id = _run_sample_pipeline(db_path)

    expected = _load_expected()
    partition_statuses = _get_partition_statuses(db_path, run_id)

    for label in expected["labels"]:
        if label.get("case") == "empty_file":
            fname = label.get("file", "")
            date_str = fname.replace("orders_", "").replace(".csv", "")
            actual_status = partition_statuses.get(date_str)
            assert actual_status == "SUCCESS", (
                f"Empty-file partition {date_str}: expected SUCCESS, got {actual_status!r}"
            )
            # Verify zero fact rows for that date
            from backfill.db import get_connection
            conn = get_connection(db_path)
            count = conn.execute(
                "SELECT COUNT(*) FROM fact_orders WHERE partition_date = ?",
                (date_str,),
            ).fetchone()[0]
            conn.close()
            assert count == 0, (
                f"Empty-file partition {date_str}: expected 0 fact rows, got {count}"
            )


def test_all_reject_reasons_are_labelled(tmp_path):
    """
    Every row that the pipeline rejects must have a matching label in expected_sample_results.json.
    This ensures no surprise rejects appear that were not anticipated.
    """
    db_path = tmp_path / "warehouse.db"
    _, run_id = _run_sample_pipeline(db_path)

    expected = _load_expected()
    reject_map = _get_reject_map(db_path, run_id)

    labelled_order_ids = {
        lbl["order_id"]
        for lbl in expected["labels"]
        if "order_id" in lbl and "expected_reject_reason" in lbl
    }

    unlabelled = [oid for oid in reject_map if oid not in labelled_order_ids]
    assert not unlabelled, (
        f"{len(unlabelled)} rejected order_id(s) have no expected label: {unlabelled[:5]}"
    )


def test_late_arriving_rows_are_accepted(tmp_path):
    """
    Late-arriving rows (event_ts date != partition_date) must be accepted (not rejected).
    They are labelled in the expected file with case='late_arriving'.
    """
    db_path = tmp_path / "warehouse.db"
    _, run_id = _run_sample_pipeline(db_path)

    expected = _load_expected()
    reject_map = _get_reject_map(db_path, run_id)

    for label in expected["labels"]:
        if label.get("case") == "late_arriving":
            oid = label.get("order_id")
            if oid:
                assert oid not in reject_map, (
                    f"Late-arriving order {oid!r} was rejected (should be accepted with flag)"
                )
