"""
Backfill controller: orchestrates partition planning, parallelism, retry, resume, and progress.

Design:
- ThreadPoolExecutor for bounded parallel READ + VALIDATE + TRANSFORM (CPU/IO work).
- Single writer thread (main thread) performs SQLite writes to avoid lock contention.
- Per-partition retry loop (up to max_retries attempts).
- Resume: skip partitions already SUCCESS; retry FAILED and stale RUNNING.
- Dry-run: runs extraction + validation in memory only; makes zero DB writes.
- Failure isolation: one partition failing does not stop others.
- Final status: SUCCESS (all ok) / PARTIAL (some failed) / FAILED (all failed).
- Exit code: 0 = SUCCESS, 1 = PARTIAL or FAILED.

Stale RUNNING:
  Any RUNNING partition row found at startup is from a crashed prior run.
  Since only one process runs against the DB at a time, we reset all RUNNING
  rows to FAILED (error='stale_running') before planning. This is simpler and
  more correct than a time-based threshold.

Progress commits:
  upsert_partition_progress always issues its own conn.commit() so status writes
  are independent of the data transaction in load_partition. A data-transaction
  rollback cannot erase a FAILED status that was committed separately.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backfill import config as cfg
from backfill.db import get_connection, create_schema
from backfill.extract import read_partition, read_customers, read_products, SchemaError
from backfill.load import load_customers, load_products, load_partition
from backfill.transform import transform_rows, resolve_latest_per_order
from backfill.validate import validate_rows

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_run_id() -> str:
    """Generate a timestamp-based run ID: run_YYYYMMDDTHHMMSSZ."""
    now = datetime.now(timezone.utc)
    return f"{cfg.RUN_ID_PREFIX}{now.strftime('%Y%m%dT%H%M%SZ')}"


def now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def date_range(start: str, end: str) -> list[str]:
    """Return a list of date strings (YYYY-MM-DD) from start to end inclusive."""
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    out: list[str] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def orders_file_path(source_dir: str | Path, partition_date: str) -> Path:
    """Return the expected path for an orders partition CSV."""
    return (
        Path(source_dir)
        / cfg.ORDERS_SUBDIR
        / cfg.ORDERS_FILE_PATTERN.format(date=partition_date)
    )


def discover_partitions(source_dir: str | Path) -> list[str]:
    """
    Auto-discover partition dates from the source directory.

    Scans <source_dir>/orders/orders_YYYY-MM-DD.csv and returns sorted dates.
    Used as the default when no --start-date / --end-date / --dates are supplied.
    """
    orders_dir = Path(source_dir) / cfg.ORDERS_SUBDIR
    if not orders_dir.exists():
        return []
    pattern = re.compile(r"orders_(\d{4}-\d{2}-\d{2})\.csv$")
    dates: list[str] = []
    for f in orders_dir.iterdir():
        m = pattern.match(f.name)
        if m:
            dates.append(m.group(1))
    return sorted(dates)


# ---------------------------------------------------------------------------
# Partition planning
# ---------------------------------------------------------------------------

def _reset_stale_running(conn: sqlite3.Connection) -> int:
    """
    Mark all RUNNING partition rows as FAILED with error='stale_running'.

    Called once at the start of every run. Since only one process touches
    the DB at a time, any RUNNING row is from a previously crashed run.

    Returns the number of rows updated.
    """
    cur = conn.execute(
        """
        UPDATE backfill_partitions
        SET status = ?, error = 'stale_running', ended_at = ?
        WHERE status = ?
        """,
        (cfg.STATUS_FAILED, now_iso(), cfg.STATUS_RUNNING),
    )
    conn.commit()
    count = cur.rowcount
    if count:
        logger.warning("reset %d stale RUNNING partition(s) to FAILED", count)
    return count


def plan_partitions(
    partitions: list[str],
    db_path: str | Path,
    force: bool,
    run_id: str,
) -> list[dict]:
    """
    Determine what to do with each requested partition date.

    Returns a list of partition plan dicts:
        {partition_date, action: PROCESS|SKIP, previous_status}

    Rules:
    - force=True  -> always PROCESS
    - Previous SUCCESS -> SKIP (unless force)
    - Previous FAILED / PENDING -> PROCESS
    - No previous record -> PROCESS
    NOTE: RUNNING rows have already been reset to FAILED by _reset_stale_running
    before this function is called, so they will be PROCESS'd here.
    """
    existing: dict[str, dict] = {}
    try:
        conn = get_connection(db_path)
        rows = conn.execute(
            """
            SELECT partition_date, status, started_at
            FROM backfill_partitions
            WHERE partition_date IN ({placeholders})
            ORDER BY started_at DESC
            """.format(placeholders=",".join("?" * len(partitions))),
            partitions,
        ).fetchall()
        conn.close()
        for row in rows:
            pd = row["partition_date"]
            if pd not in existing:  # keep first (latest started_at)
                existing[pd] = dict(row)
    except Exception as exc:
        logger.warning("Could not read partition history: %s", exc)

    plan: list[dict] = []

    for pd in partitions:
        prev = existing.get(pd)
        if prev is None:
            action = "PROCESS"
            prev_status = None
        elif force:
            action = "PROCESS"
            prev_status = prev["status"]
        elif prev["status"] == cfg.STATUS_SUCCESS:
            action = "SKIP"
            prev_status = cfg.STATUS_SUCCESS
        else:
            # FAILED, PENDING, SKIPPED, or any other state -> retry
            action = "PROCESS"
            prev_status = prev["status"]

        plan.append(
            {
                "partition_date": pd,
                "action": action,
                "previous_status": prev_status,
                "run_id": run_id,
            }
        )

    return plan


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def upsert_partition_progress(
    conn,
    run_id: str,
    partition_date: str,
    status: str,
    rows_in: int = 0,
    rows_out: int = 0,
    rows_history_only: int = 0,
    rows_rejected: int = 0,
    late_arriving: int = 0,
    attempts: int = 0,
    error: str | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> None:
    """
    Insert or update a row in backfill_partitions.

    Always issues conn.commit() after the write so the status record is
    durable regardless of what happens to the data transaction in load_partition.
    This means a rollback in load_partition cannot erase a FAILED status.
    """
    conn.execute(
        """
        INSERT INTO backfill_partitions
            (run_id, partition_date, status, rows_in, rows_out, rows_history_only,
             rows_rejected, late_arriving, attempts, error, started_at, ended_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, partition_date) DO UPDATE SET
            status            = excluded.status,
            rows_in           = excluded.rows_in,
            rows_out          = excluded.rows_out,
            rows_history_only = excluded.rows_history_only,
            rows_rejected     = excluded.rows_rejected,
            late_arriving     = excluded.late_arriving,
            attempts          = excluded.attempts,
            error             = excluded.error,
            started_at        = COALESCE(backfill_partitions.started_at, excluded.started_at),
            ended_at          = excluded.ended_at
        """,
        (
            run_id,
            partition_date,
            status,
            rows_in,
            rows_out,
            rows_history_only,
            rows_rejected,
            late_arriving,
            attempts,
            error,
            started_at,
            ended_at,
        ),
    )
    conn.commit()  # always commit immediately — independent of data transaction


def upsert_run(conn, run_id: str, **kwargs) -> None:
    """Update columns in backfill_runs for the given run_id."""
    if not kwargs:
        return
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [run_id]
    conn.execute(f"UPDATE backfill_runs SET {sets} WHERE run_id = ?", vals)
    conn.commit()


# ---------------------------------------------------------------------------
# Process one partition (read + validate + transform) — runs in worker thread
# ---------------------------------------------------------------------------

def _process_partition(
    partition_date: str,
    source_dir: str | Path,
    valid_customers: set[str],
    valid_products: set[str],
    run_id: str,
    inject_failure_date: str | None,
) -> dict:
    """
    Read, validate, and transform one partition.  Called in worker thread.

    Returns a result dict:
        {partition_date, error, rows_in, rows_out, rows_history_only,
         rejected_rows, transformed_rows, fact_rows, late_arriving_count}
    """
    if inject_failure_date and partition_date == inject_failure_date:
        raise RuntimeError(
            f"Injected failure for partition_date={partition_date}"
        )

    file_path = orders_file_path(source_dir, partition_date)
    source_file = file_path.name
    now_ts = now_iso()

    rows, schema_error = read_partition(file_path)

    if schema_error:
        return {
            "partition_date": partition_date,
            "error": schema_error,
            "rows_in": 0,
            "rows_out": 0,
            "rows_history_only": 0,
            "rejected_rows": [],
            "transformed_rows": [],
            "fact_rows": [],
            "late_arriving_count": 0,
        }

    rows_in = len(rows)

    if rows_in == 0:
        return {
            "partition_date": partition_date,
            "error": None,
            "rows_in": 0,
            "rows_out": 0,
            "rows_history_only": 0,
            "rejected_rows": [],
            "transformed_rows": [],
            "fact_rows": [],
            "late_arriving_count": 0,
        }

    valid_rows, rejected_rows, late_arriving_count = validate_rows(
        rows=rows,
        valid_customers=valid_customers,
        valid_products=valid_products,
        source_file=source_file,
        partition_date=partition_date,
        run_id=run_id,
        now_ts=now_ts,
    )

    transformed = transform_rows(valid_rows, partition_date, run_id, source_file, now_ts)
    fact_rows = resolve_latest_per_order(transformed)

    # rows_out = all valid rows (fact + history-only) so the invariant holds:
    # rows_in == rows_out + rows_rejected
    rows_out = len(transformed)
    rows_history_only = len(transformed) - len(fact_rows)

    return {
        "partition_date": partition_date,
        "error": None,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_history_only": rows_history_only,
        "rejected_rows": rejected_rows,
        "transformed_rows": transformed,
        "fact_rows": fact_rows,
        "late_arriving_count": late_arriving_count,
    }


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def run_dry_run(
    partitions: list[str],
    source_dir: str | Path,
    valid_customers: set[str],
    valid_products: set[str],
    rows_per_second: float = cfg.DEFAULT_ROWS_PER_SECOND,
) -> dict:
    """
    Dry-run: read and validate partitions without writing anything to the DB.

    Prints per-partition details (partition, rows, valid, invalid, missing).
    Returns an estimate dict with counts and projected runtime.
    """
    total_partitions = len(partitions)
    missing_files: list[str] = []
    total_rows_in = 0
    total_valid = 0
    total_rejected = 0
    total_late = 0

    print(f"\n{'='*60}")
    print(f"  DRY-RUN ESTIMATE  ({total_partitions} partitions)")
    print(f"{'='*60}")
    print(f"  {'Partition':<14} {'Status':<12} {'Rows':>6} {'Valid':>6} {'Reject':>7} {'Late':>5}")
    print(f"  {'-'*60}")

    for pd in partitions:
        file_path = orders_file_path(source_dir, pd)
        if not file_path.exists():
            missing_files.append(pd)
            print(f"  {pd:<14} {'MISSING':<12} {'':>6} {'':>6} {'':>7} {'':>5}")
            continue

        rows, schema_error = read_partition(file_path)
        if schema_error:
            logger.warning("dry_run: partition=%s schema_error=%s", pd, schema_error)
            print(f"  {pd:<14} {'SCHEMA_ERR':<12} {'':>6} {'':>6} {'':>7} {'':>5}")
            continue

        total_rows_in += len(rows)
        part_valid = 0
        part_rejected = 0
        part_late = 0
        if rows:
            _, rejected, late = validate_rows(
                rows=rows,
                valid_customers=valid_customers,
                valid_products=valid_products,
                source_file=file_path.name,
                partition_date=pd,
                run_id="dry_run",
                now_ts=now_iso(),
            )
            part_rejected = len(rejected)
            part_valid = len(rows) - part_rejected
            part_late = late
            total_rejected += part_rejected
            total_valid += part_valid
            total_late += part_late

        print(
            f"  {pd:<14} {'OK':<12} {len(rows):>6} {part_valid:>6} {part_rejected:>7} {part_late:>5}"
        )

    print(f"  {'-'*60}")
    estimated_seconds = total_rows_in / rows_per_second if rows_per_second > 0 else 0
    print(f"  Total rows       : {total_rows_in:,}")
    print(f"  Estimated valid  : {total_valid:,}")
    print(f"  Estimated rejected: {total_rejected:,}")
    print(f"  Estimated late   : {total_late:,}")
    print(f"  Missing partitions: {len(missing_files)}")
    print(f"  Est. runtime     : {estimated_seconds:.1f}s "
          f"(at {rows_per_second:,.0f} rows/sec)")
    if missing_files:
        print(f"  Missing dates    : {', '.join(missing_files)}")
    print(f"{'='*60}\n")

    return {
        "mode": "dry_run",
        "partitions_requested": total_partitions,
        "partitions_missing": len(missing_files),
        "missing_dates": missing_files,
        "total_rows_in": total_rows_in,
        "estimated_valid": total_valid,
        "estimated_rejected": total_rejected,
        "estimated_late_arriving": total_late,
        "estimated_runtime_seconds": round(estimated_seconds, 2),
        "rows_per_second_assumption": rows_per_second,
    }


# ---------------------------------------------------------------------------
# Main backfill runner
# ---------------------------------------------------------------------------

def run_backfill(
    partitions: list[str],
    source_dir: str | Path,
    db_path: str | Path,
    max_workers: int = cfg.DEFAULT_MAX_WORKERS,
    max_retries: int = cfg.DEFAULT_MAX_RETRIES,
    force: bool = False,
    inject_failure_date: str | None = None,
    run_id: str | None = None,
    params: dict | None = None,
) -> tuple[str, int, str]:
    """
    Run the backfill for the given list of partition dates.

    Returns (final_status, exit_code, run_id) where exit_code is 0 for SUCCESS, 1 otherwise.
    """
    if run_id is None:
        run_id = make_run_id()

    inject_failure_date = inject_failure_date or os.environ.get(cfg.INJECT_FAILURE_ENV_VAR)

    logger.info("run_id=%s starting backfill: partitions=%d", run_id, len(partitions))

    # Ensure output dir and DB schema exist
    conn = get_connection(db_path)
    create_schema(conn)

    # Bug 6 fix: reset any RUNNING rows from a crashed prior run.
    _reset_stale_running(conn)

    # Load reference data
    source_path = Path(source_dir)
    customers = read_customers(source_path / cfg.CUSTOMERS_FILE)
    products = read_products(source_path / cfg.PRODUCTS_FILE)
    load_customers(conn, customers)
    load_products(conn, products)
    valid_customers = {c["customer_id"] for c in customers}
    valid_products = {p["product_id"] for p in products}

    logger.info(
        "run_id=%s loaded refs: customers=%d products=%d",
        run_id,
        len(valid_customers),
        len(valid_products),
    )

    # Plan: skip/process
    plan = plan_partitions(partitions, db_path, force, run_id)
    to_process = [p for p in plan if p["action"] == "PROCESS"]
    to_skip = [p for p in plan if p["action"] == "SKIP"]

    logger.info(
        "run_id=%s plan: process=%d skip=%d",
        run_id,
        len(to_process),
        len(to_skip),
    )

    # Register run in backfill_runs
    started_at = now_iso()
    conn.execute(
        """
        INSERT OR REPLACE INTO backfill_runs
            (run_id, started_at, status, params, partitions_total,
             partitions_success, partitions_failed, rows_in, rows_out,
             rows_rejected, late_arriving)
        VALUES (?, ?, 'RUNNING', ?, ?, 0, 0, 0, 0, 0, 0)
        """,
        (
            run_id,
            started_at,
            json.dumps(params or {}),
            len(partitions),
        ),
    )
    conn.commit()

    # Mark skipped partitions
    for p in to_skip:
        upsert_partition_progress(
            conn,
            run_id=run_id,
            partition_date=p["partition_date"],
            status=cfg.STATUS_SKIPPED,
        )
        logger.info("run_id=%s SKIPPED partition=%s", run_id, p["partition_date"])

    # -----------------------------------------------------------------------
    # Process partitions with bounded parallelism
    # -----------------------------------------------------------------------
    partition_dates_to_process = [p["partition_date"] for p in to_process]
    total = len(partition_dates_to_process)
    done = 0
    success_count = 0
    fail_count = 0
    total_rows_in = 0
    total_rows_out = 0
    total_rows_history_only = 0
    total_rows_rejected = 0
    total_late_arriving = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_date: dict = {}
        for pd in partition_dates_to_process:
            upsert_partition_progress(
                conn,
                run_id=run_id,
                partition_date=pd,
                status=cfg.STATUS_PENDING,
                started_at=now_iso(),
            )
            future = executor.submit(
                _process_partition,
                pd,
                source_dir,
                valid_customers,
                valid_products,
                run_id,
                inject_failure_date,
            )
            future_to_date[future] = pd

        for future in as_completed(future_to_date):
            pd = future_to_date[future]
            attempts = 0
            result = None
            last_error = None
            rows_out = 0
            rows_history_only = 0
            rows_rej = 0
            late = 0

            # Mark as RUNNING (status written independently with its own commit)
            upsert_partition_progress(
                conn,
                run_id=run_id,
                partition_date=pd,
                status=cfg.STATUS_RUNNING,
                started_at=now_iso(),
                attempts=1,
            )

            # Retry loop
            for attempt in range(1, max_retries + 2):  # +2 = initial + retries
                attempts = attempt
                try:
                    if attempt == 1:
                        result = future.result()
                    else:
                        result = _process_partition(
                            pd,
                            source_dir,
                            valid_customers,
                            valid_products,
                            run_id,
                            inject_failure_date,
                        )
                    last_error = result.get("error")
                    if last_error:
                        # Schema error or missing file: not retriable
                        break
                    # Write partition to DB (single writer, main thread)
                    fact_count, hist_only, rej_count = load_partition(
                        conn=conn,
                        partition_date=pd,
                        transformed_rows=result["transformed_rows"],
                        fact_rows=result["fact_rows"],
                        rejected_rows=result["rejected_rows"],
                        run_id=run_id,
                        source_file=orders_file_path(source_dir, pd).name,
                    )
                    rows_out = result["rows_out"]          # all valid (fact + history)
                    rows_history_only = result["rows_history_only"]
                    rows_rej = rej_count
                    late = result["late_arriving_count"]
                    last_error = None
                    break  # success
                except Exception as exc:
                    last_error = str(exc)
                    logger.warning(
                        "run_id=%s partition=%s attempt=%d error=%s",
                        run_id,
                        pd,
                        attempt,
                        last_error,
                    )
                    if attempt >= max_retries + 1:
                        break
                    time.sleep(0.1 * attempt)

            done += 1
            if last_error:
                fail_count += 1
                status = cfg.STATUS_FAILED
                rows_in = result["rows_in"] if result else 0
                rows_out = 0
                rows_history_only = 0
                rows_rej = 0
                late = 0
                logger.error(
                    "[%d/%d] run_id=%s partition=%s FAILED error=%s",
                    done, total, run_id, pd, last_error,
                )
            else:
                success_count += 1
                status = cfg.STATUS_SUCCESS
                rows_in = result["rows_in"]
                logger.info(
                    "[%d/%d] run_id=%s %s SUCCESS rows_in=%d rows_out=%d "
                    "rows_rejected=%d history_only=%d late_arriving=%d",
                    done, total, run_id, pd,
                    rows_in, rows_out, rows_rej, rows_history_only, late,
                )

            total_rows_in += rows_in
            total_rows_out += rows_out
            total_rows_history_only += rows_history_only
            total_rows_rejected += rows_rej
            total_late_arriving += late

            # Progress written independently (its own commit — Bug 5 fix)
            upsert_partition_progress(
                conn,
                run_id=run_id,
                partition_date=pd,
                status=status,
                rows_in=rows_in,
                rows_out=rows_out,
                rows_history_only=rows_history_only,
                rows_rejected=rows_rej,
                late_arriving=late,
                attempts=attempts,
                error=last_error,
                ended_at=now_iso(),
            )

    # -----------------------------------------------------------------------
    # Finalize run
    # -----------------------------------------------------------------------
    if fail_count == 0:
        final_status = "SUCCESS"
    elif success_count == 0 and fail_count > 0:
        final_status = "FAILED"
    else:
        final_status = "PARTIAL"

    ended_at = now_iso()
    upsert_run(
        conn,
        run_id,
        ended_at=ended_at,
        status=final_status,
        partitions_success=success_count,
        partitions_failed=fail_count,
        rows_in=total_rows_in,
        rows_out=total_rows_out,
        rows_rejected=total_rows_rejected,
        late_arriving=total_late_arriving,
    )

    logger.info(
        "run_id=%s DONE status=%s partitions_success=%d partitions_failed=%d "
        "rows_in=%d rows_out=%d rows_rejected=%d history_only=%d late_arriving=%d",
        run_id, final_status, success_count, fail_count,
        total_rows_in, total_rows_out, total_rows_rejected,
        total_rows_history_only, total_late_arriving,
    )

    conn.close()
    exit_code = 0 if final_status == "SUCCESS" else 1
    return final_status, exit_code, run_id


def print_status(db_path: str | Path) -> None:
    """Print a summary progress table for the latest run."""
    conn = get_connection(db_path)
    run = conn.execute(
        "SELECT run_id, started_at, status FROM backfill_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    if not run:
        print("No runs found in the database.")
        conn.close()
        return

    run_id = run["run_id"]
    print(f"\nLatest run: {run_id}  status={run['status']}  started={run['started_at']}\n")
    print(
        f"{'partition_date':<16} {'status':<10} {'rows_in':>8} {'rows_out':>9} "
        f"{'hist_only':>10} {'rejected':>9} {'late':>5} {'tries':>6}  error"
    )
    print("-" * 105)

    partitions = conn.execute(
        "SELECT * FROM backfill_partitions WHERE run_id = ? ORDER BY partition_date",
        (run_id,),
    ).fetchall()
    for p in partitions:
        err = (p["error"] or "")[:35]
        hist = p["rows_history_only"] if "rows_history_only" in p.keys() else 0
        late = p["late_arriving"] if "late_arriving" in p.keys() else 0
        print(
            f"{p['partition_date']:<16} {p['status']:<10} {p['rows_in']:>8} "
            f"{p['rows_out']:>9} {hist:>10} {p['rows_rejected']:>9} {late:>5} "
            f"{p['attempts']:>6}  {err}"
        )

    print()
    conn.close()
