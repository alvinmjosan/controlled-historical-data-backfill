"""
CLI entry point for the backfill pipeline.

Usage:
    python -m backfill [options]

Run `python -m backfill --help` for full usage.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from backfill import config as cfg
from backfill.controller import (
    date_range,
    discover_partitions,
    make_run_id,
    now_iso,
    plan_partitions,
    print_status,
    run_backfill,
    run_dry_run,
    orders_file_path,
)
from backfill.db import create_schema, get_connection
from backfill.extract import read_customers, read_products
from backfill.reconcile import run_reconcile


def _setup_logging(run_id: str, verbose: bool = False) -> None:
    """Configure logging with run_id in every line."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = f"%(asctime)s run_id={run_id} %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stdout)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m backfill",
        description="Controlled historical backfill pipeline for daily e-commerce orders.",
    )

    # Source / target
    parser.add_argument(
        "--source-dir",
        default=str(cfg.DEFAULT_SOURCE_DIR),
        help=f"Directory containing source CSVs (default: {cfg.DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--db-path",
        default=str(cfg.DEFAULT_DB_PATH),
        help=f"Path to SQLite warehouse database (default: {cfg.DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(cfg.DEFAULT_OUTPUT_DIR),
        help=f"Directory for reports and logs (default: {cfg.DEFAULT_OUTPUT_DIR})",
    )

    # Date selection (mutually exclusive)
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--start-date", help="Start date YYYY-MM-DD (use with --end-date)")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (use with --start-date)")
    date_group.add_argument(
        "--dates",
        help="Comma-separated list of dates: 2025-01-01,2025-01-03",
    )

    # Run modes
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and validate files without writing to DB; print estimates.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print progress table for the latest run and exit.",
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Run reconciliation checks and write report.",
    )

    # Parallelism and retry
    parser.add_argument(
        "--max-workers",
        type=int,
        default=cfg.DEFAULT_MAX_WORKERS,
        help=f"Number of parallel worker threads (default: {cfg.DEFAULT_MAX_WORKERS})",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=cfg.DEFAULT_MAX_RETRIES,
        help=f"Max retries per failed partition (default: {cfg.DEFAULT_MAX_RETRIES})",
    )

    # Resume / force
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all partitions including those already marked SUCCESS.",
    )

    # Testing hooks
    parser.add_argument(
        "--inject-failure-date",
        metavar="YYYY-MM-DD",
        help="Inject a simulated failure for this specific partition date (for testing).",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging.")

    return parser


def _resolve_partitions(args: argparse.Namespace) -> list[str]:
    """
    Resolve the list of partition dates from CLI arguments.

    Priority:
    1. --dates (comma-separated explicit list)
    2. --start-date / --end-date range
    3. Auto-discover all orders_YYYY-MM-DD.csv files in <source_dir>/orders/

    Option 3 means you can run `python -m backfill --source-dir data/sample --dry-run`
    without specifying any dates — the pipeline finds them automatically.
    """
    if args.dates:
        return [d.strip() for d in args.dates.split(",") if d.strip()]
    if args.start_date and args.end_date:
        return date_range(args.start_date, args.end_date)
    if args.start_date:
        return [args.start_date]
    # Auto-discover from source directory
    discovered = discover_partitions(args.source_dir)
    if discovered:
        logging.getLogger(__name__).info(
            "No dates specified — auto-discovered %d partition(s) from %s",
            len(discovered),
            args.source_dir,
        )
    return discovered


def _write_run_report(
    run_id: str,
    final_status: str,
    partitions: list[str],
    args: argparse.Namespace,
    output_dir: str | Path,
    db_path: str | Path,
) -> None:
    """Write a JSON run report to the output directory."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    run_row = conn.execute(
        "SELECT * FROM backfill_runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    partition_rows = conn.execute(
        "SELECT partition_date, status, rows_in, rows_out, rows_history_only, "
        "rows_rejected, late_arriving, attempts, error "
        "FROM backfill_partitions WHERE run_id = ? ORDER BY partition_date",
        (run_id,),
    ).fetchall()
    conn.close()

    report = {
        "run_id": run_id,
        "final_status": final_status,
        "partitions_requested": partitions,
        "args": {
            "source_dir": args.source_dir,
            "db_path": args.db_path,
            "max_workers": args.max_workers,
            "max_retries": args.max_retries,
            "force": args.force,
            "dry_run": args.dry_run,
        },
    }
    if run_row:
        report.update(dict(run_row))
    if partition_rows:
        report["partitions"] = [dict(p) for p in partition_rows]

    report_path = Path(output_dir) / f"run_report_{run_id}.json"
    # Also write canonical name so --status can find the latest easily
    canonical_path = Path(output_dir) / "run_report_latest.json"
    for path in (report_path, canonical_path):
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
    logging.getLogger(__name__).info("run_report written to %s", report_path)


def main(argv: list[str] | None = None) -> int:
    """Main CLI entry point. Returns exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    run_id = make_run_id()
    _setup_logging(run_id, verbose=args.verbose)
    log = logging.getLogger(__name__)

    # --status mode
    if args.status:
        print_status(args.db_path)
        return 0

    # --reconcile mode
    if args.reconcile:
        run_reconcile(args.db_path, output_dir=args.output_dir)
        return 0

    # Resolve partitions (auto-discover if no date args given)
    partitions = _resolve_partitions(args)
    if not partitions:
        parser.error(
            "No partitions found. Specify via --start-date/--end-date, --dates, "
            "or ensure <source_dir>/orders/ contains orders_YYYY-MM-DD.csv files. "
            "Use --status or --reconcile for info-only modes."
        )

    log.info(
        "mode=%s partitions=%d source_dir=%s db=%s workers=%d retries=%d",
        "dry_run" if args.dry_run else "backfill",
        len(partitions),
        args.source_dir,
        args.db_path,
        args.max_workers,
        args.max_retries,
    )

    # --dry-run mode
    if args.dry_run:
        source_path = Path(args.source_dir)
        customers_path = source_path / cfg.CUSTOMERS_FILE
        products_path = source_path / cfg.PRODUCTS_FILE

        customers = read_customers(customers_path) if customers_path.exists() else []
        products = read_products(products_path) if products_path.exists() else []
        valid_customers = {c["customer_id"] for c in customers}
        valid_products = {p["product_id"] for p in products}

        estimate = run_dry_run(
            partitions=partitions,
            source_dir=args.source_dir,
            valid_customers=valid_customers,
            valid_products=valid_products,
        )
        print("\n=== DRY-RUN ESTIMATE ===")
        print(json.dumps(estimate, indent=2))
        return 0

    # Full backfill
    params = {
        "partitions": partitions,
        "source_dir": args.source_dir,
        "max_workers": args.max_workers,
        "max_retries": args.max_retries,
        "force": args.force,
        "inject_failure_date": args.inject_failure_date,
    }

    final_status, exit_code, actual_run_id = run_backfill(
        partitions=partitions,
        source_dir=args.source_dir,
        db_path=args.db_path,
        max_workers=args.max_workers,
        max_retries=args.max_retries,
        force=args.force,
        inject_failure_date=args.inject_failure_date,
        run_id=run_id,
        params=params,
    )

    _write_run_report(actual_run_id, final_status, partitions, args, args.output_dir, args.db_path)

    if final_status != "SUCCESS":
        log.warning("Run completed with status=%s", final_status)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
