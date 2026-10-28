"""
Reconciliation module: post-load data quality checks and report generation.

Checks:
1. Per-partition: source rows == loaded + rejected (explain any difference).
2. fact_orders primary key uniqueness (no duplicates).
3. No orphan customer_id / product_id in fact_orders (referential integrity).
4. sum(total_amount) in target == sum recomputed from valid source rows.
5. Every rejected record has run_id, source_file, row_number, reason (traceable).
6. Rerun produces identical counts.

Outputs:
- Readable summary printed to stdout.
- output/reconciliation_report.json with full details.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from backfill.db import get_connection

logger = logging.getLogger(__name__)


def run_reconcile(
    db_path: str | Path,
    run_id: str | None = None,
    output_dir: str | Path = "output",
) -> dict:
    """
    Run reconciliation checks against the target database.

    Args:
        db_path: Path to the SQLite warehouse database.
        run_id: Run to reconcile; defaults to the most recent run.
        output_dir: Directory to write the JSON report.

    Returns a dict with all check results.
    """
    conn = get_connection(db_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Identify run to reconcile
    if run_id is None:
        row = conn.execute(
            "SELECT run_id FROM backfill_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            print("No runs found.")
            conn.close()
            return {}
        run_id = row["run_id"]

    logger.info("reconcile: run_id=%s", run_id)

    report: dict = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checks": {},
        "passed": True,
    }
    checks = report["checks"]

    # -----------------------------------------------------------------------
    # Check 1: Per-partition row counts
    # -----------------------------------------------------------------------
    partitions = conn.execute(
        """
        SELECT partition_date, rows_in, rows_out, rows_rejected
        FROM backfill_partitions
        WHERE run_id = ? AND status = 'SUCCESS'
        ORDER BY partition_date
        """,
        (run_id,),
    ).fetchall()

    partition_check: list[dict] = []
    for p in partitions:
        expected = p["rows_in"]
        actual = p["rows_out"] + p["rows_rejected"]
        ok = expected == actual
        partition_check.append(
            {
                "partition_date": p["partition_date"],
                "rows_in": expected,
                "rows_out": p["rows_out"],
                "rows_rejected": p["rows_rejected"],
                "sum_out_rejected": actual,
                "balanced": ok,
                "difference": expected - actual,
            }
        )
        if not ok:
            report["passed"] = False

    checks["partition_row_balance"] = {
        "description": "rows_in == rows_out + rows_rejected per partition",
        "passed": all(p["balanced"] for p in partition_check),
        "details": partition_check,
    }
    if not checks["partition_row_balance"]["passed"]:
        report["passed"] = False

    # -----------------------------------------------------------------------
    # Check 1b: History-only rows breakdown
    # (rows that are valid but not the latest event for their order_id;
    #  they go to order_status_history only, not fact_orders — this is expected)
    # -----------------------------------------------------------------------
    history_only_total = sum(
        (dict(p).get("rows_history_only") or 0) for p in
        conn.execute(
            "SELECT rows_history_only FROM backfill_partitions WHERE run_id = ? AND status = 'SUCCESS'",
            (run_id,),
        ).fetchall()
    )
    fact_orders_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    history_total = conn.execute("SELECT COUNT(*) FROM order_status_history").fetchone()[0]
    checks["history_only_rows"] = {
        "description": (
            "Rows valid but superseded by a newer event for the same order_id. "
            "Written to order_status_history only — NOT counted as rejected. "
            "rows_out = fact_rows + history_only_rows."
        ),
        "history_only_rows_this_run": history_only_total,
        "fact_orders_total": fact_orders_count,
        "order_status_history_total": history_total,
    }

    # -----------------------------------------------------------------------
    # Check 2: fact_orders PK uniqueness
    # -----------------------------------------------------------------------
    dup_row = conn.execute(
        "SELECT COUNT(*) AS total, COUNT(DISTINCT order_id) AS distinct_count FROM fact_orders"
    ).fetchone()
    pk_ok = dup_row["total"] == dup_row["distinct_count"]
    if not pk_ok:
        report["passed"] = False
    checks["fact_orders_pk_unique"] = {
        "description": "fact_orders has no duplicate order_id (PK uniqueness)",
        "passed": pk_ok,
        "total_rows": dup_row["total"],
        "distinct_order_ids": dup_row["distinct_count"],
    }

    # -----------------------------------------------------------------------
    # Check 3: Referential integrity
    # -----------------------------------------------------------------------
    orphan_customers = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM fact_orders f
        WHERE NOT EXISTS (SELECT 1 FROM dim_customers c WHERE c.customer_id = f.customer_id)
        """
    ).fetchone()["cnt"]

    orphan_products = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM fact_orders f
        WHERE NOT EXISTS (SELECT 1 FROM dim_products p WHERE p.product_id = f.product_id)
        """
    ).fetchone()["cnt"]

    ri_ok = orphan_customers == 0 and orphan_products == 0
    if not ri_ok:
        report["passed"] = False
    checks["referential_integrity"] = {
        "description": "No orphan customer_id or product_id in fact_orders",
        "passed": ri_ok,
        "orphan_customers": orphan_customers,
        "orphan_products": orphan_products,
    }

    # -----------------------------------------------------------------------
    # Check 4: total_amount reconciliation (sum in DB vs recomputed)
    # -----------------------------------------------------------------------
    amount_row = conn.execute(
        "SELECT ROUND(SUM(total_amount), 2) AS db_sum, "
        "ROUND(SUM(ROUND(quantity * unit_price, 2)), 2) AS recomputed "
        "FROM fact_orders"
    ).fetchone()
    db_sum = amount_row["db_sum"] or 0.0
    recomputed = amount_row["recomputed"] or 0.0
    amount_ok = abs(db_sum - recomputed) < 0.01  # floating-point tolerance
    if not amount_ok:
        report["passed"] = False
    checks["total_amount_reconciliation"] = {
        "description": "sum(total_amount) in fact_orders matches recomputed quantity*unit_price",
        "passed": amount_ok,
        "db_sum_total_amount": db_sum,
        "recomputed_sum": recomputed,
        "difference": round(db_sum - recomputed, 4),
    }

    # -----------------------------------------------------------------------
    # Check 5: Rejected records traceability
    # -----------------------------------------------------------------------
    rejects = conn.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN run_id IS NULL OR run_id = '' THEN 1 ELSE 0 END)     AS missing_run_id,
               SUM(CASE WHEN source_file IS NULL OR source_file = '' THEN 1 ELSE 0 END) AS missing_source_file,
               SUM(CASE WHEN row_number IS NULL THEN 1 ELSE 0 END)                AS missing_row_number,
               SUM(CASE WHEN reject_reason IS NULL OR reject_reason = '' THEN 1 ELSE 0 END) AS missing_reason
        FROM rejected_records
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    trace_ok = (
        rejects["missing_run_id"] == 0
        and rejects["missing_source_file"] == 0
        and rejects["missing_row_number"] == 0
        and rejects["missing_reason"] == 0
    )
    if not trace_ok:
        report["passed"] = False
    checks["reject_traceability"] = {
        "description": "Every rejected record has run_id, source_file, row_number, reject_reason",
        "passed": trace_ok,
        "total_rejected": rejects["total"],
        "missing_run_id": rejects["missing_run_id"],
        "missing_source_file": rejects["missing_source_file"],
        "missing_row_number": rejects["missing_row_number"],
        "missing_reason": rejects["missing_reason"],
    }

    # -----------------------------------------------------------------------
    # Check 6: Reject reason breakdown
    # -----------------------------------------------------------------------
    reasons = conn.execute(
        """
        SELECT reject_reason, COUNT(*) AS count
        FROM rejected_records
        WHERE run_id = ?
        GROUP BY reject_reason
        ORDER BY count DESC
        """,
        (run_id,),
    ).fetchall()
    checks["reject_reason_breakdown"] = {
        "description": "Reject reason counts for this run",
        "details": [{"reason": r["reject_reason"], "count": r["count"]} for r in reasons],
    }

    # -----------------------------------------------------------------------
    # Print summary
    # -----------------------------------------------------------------------
    _print_summary(report, checks)

    # -----------------------------------------------------------------------
    # Write JSON report
    # -----------------------------------------------------------------------
    report_path = output_path / f"reconciliation_report_{run_id}.json"
    # Also write to the canonical name
    canonical_path = output_path / "reconciliation_report.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    with open(canonical_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    logger.info("reconcile: report written to %s", report_path)
    conn.close()
    return report


def _print_summary(report: dict, checks: dict) -> None:
    """Print a human-readable reconciliation summary."""
    status = "[PASSED]" if report["passed"] else "[FAILED]"
    print(f"\n{'='*60}")
    print(f"  Reconciliation Report -- run_id: {report['run_id']}")
    print(f"  Generated: {report['generated_at']}")
    print(f"  Overall: {status}")
    print(f"{'='*60}\n")

    for check_name, check in checks.items():
        if "passed" in check:
            icon = "OK " if check["passed"] else "FAIL"
            print(f"  [{icon}] {check_name}: {check['description']}")
            if not check["passed"]:
                print(f"      DETAILS: {json.dumps({k:v for k,v in check.items() if k not in ('description','passed')}, indent=6)}")
        else:
            print(f"  [INFO] {check_name}: {check.get('description','')}")
            for item in check.get("details", []):
                print(f"      {item}")

    print()
