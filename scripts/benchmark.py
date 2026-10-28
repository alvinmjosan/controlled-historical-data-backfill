"""
Benchmark script for the backfill pipeline.

Generates a large dataset (100,000+ rows) and measures:
- Rows/second for workers=1 vs workers=4
- Before/after for batch INSERT (executemany) vs row-by-row INSERT optimization
- Results saved to docs/performance.md

Usage:
    python scripts/benchmark.py                     # full benchmark (both modes)
    python scripts/benchmark.py --skip-row-by-row   # skip slow row-by-row mode
    python scripts/benchmark.py --clean             # generate clean data (no schema-change/empty
                                                    # partitions) so the run reports SUCCESS

Note on PARTIAL status:
    By default the generator injects a schema-change partition (day index 2) and an
    empty-file partition (last day) to exercise all edge cases. Those produce PARTIAL
    on the benchmark run. Use --clean to generate clean data if you want SUCCESS.
    The PARTIAL is intentional and expected — it demonstrates failure isolation.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_data import generate_dataset
from backfill.controller import run_backfill, make_run_id, now_iso
from backfill.db import get_connection, create_schema
from backfill.extract import read_customers, read_products
from backfill.load import load_customers, load_products, load_partition, load_partition_row_by_row
from backfill.transform import transform_rows, resolve_latest_per_order
from backfill.validate import validate_rows
from backfill.extract import read_partition


LARGE_DATA_DIR = PROJECT_ROOT / "data" / "generated"
LARGE_DATA_DIR_CLEAN = PROJECT_ROOT / "data" / "generated_clean"
BENCHMARK_DAYS = 25
ROWS_PER_DAY = 4000   # 25 * 4000 = 100,000 rows


def _generate_clean_data(output_dir: Path) -> list[str]:
    """
    Generate a clean dataset: all partitions are valid (no schema-change, no empty file).
    Used with --clean so the benchmark reports SUCCESS instead of PARTIAL.
    """
    import csv as _csv
    from scripts.generate_data import (
        CUSTOMERS, PRODUCTS, VALID_STATUSES, ORDERS_COLUMNS,
        _make_event_ts, _make_order_id, generate_normal_row, _write_csv,
    )
    import random
    from datetime import date as _date, timedelta as _td

    rng = random.Random(42)
    orders_dir = output_dir / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "customers.csv", CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(output_dir / "products.csv", PRODUCTS, ["product_id", "product_name", "category"])

    start = _date(2025, 1, 1)
    dates = [(start + _td(days=i)).isoformat() for i in range(BENCHMARK_DAYS)]
    for day_idx, date_str in enumerate(dates):
        file_path = orders_dir / f"orders_{date_str}.csv"
        part_date = _date.fromisoformat(date_str)
        with open(file_path, "w", newline="", encoding="utf-8") as fh:
            writer = _csv.writer(fh)
            writer.writerow(ORDERS_COLUMNS)
            for row_idx in range(ROWS_PER_DAY):
                oid = _make_order_id(day_idx, row_idx)
                row = generate_normal_row(rng, oid, part_date)
                writer.writerow([row.get(col, "") for col in ORDERS_COLUMNS])
    return dates


def generate_large_data(clean: bool = False) -> list[str]:
    """Generate large dataset and return list of partition dates."""
    if clean:
        print(f"Generating CLEAN {BENCHMARK_DAYS} days x {ROWS_PER_DAY} rows/day = "
              f"{BENCHMARK_DAYS * ROWS_PER_DAY:,} rows (no schema-change/empty partitions)...")
        return _generate_clean_data(LARGE_DATA_DIR_CLEAN)

    print(f"Generating {BENCHMARK_DAYS} days x {ROWS_PER_DAY} rows/day = "
          f"{BENCHMARK_DAYS * ROWS_PER_DAY:,} rows (includes schema-change + empty partition)...")
    generate_dataset(
        output_dir=str(LARGE_DATA_DIR),
        days=BENCHMARK_DAYS,
        rows_per_day=ROWS_PER_DAY,
        seed=42,
        start_date="2025-01-01",
    )
    from datetime import date, timedelta
    start = date(2025, 1, 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(BENCHMARK_DAYS)]


def run_pipeline_benchmark(
    partitions: list[str],
    max_workers: int,
    db_suffix: str,
    source_dir: Path | None = None,
) -> dict:
    """Run the full pipeline (batch mode) and measure performance."""
    src = source_dir or LARGE_DATA_DIR
    db_path = PROJECT_ROOT / "output" / f"benchmark_{db_suffix}.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    start_time = time.perf_counter()
    status, exit_code, run_id = run_backfill(
        partitions=partitions,
        source_dir=src,
        db_path=db_path,
        max_workers=max_workers,
        max_retries=0,
    )
    elapsed = time.perf_counter() - start_time

    conn = get_connection(db_path)
    rows_out = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    rows_rejected = conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]
    history_rows = conn.execute("SELECT COUNT(*) FROM order_status_history").fetchone()[0]
    conn.close()

    total_rows = rows_out + rows_rejected
    rows_per_sec = total_rows / elapsed if elapsed > 0 else 0

    return {
        "mode": f"batch (executemany), workers={max_workers}",
        "workers": max_workers,
        "status": status,
        "elapsed_seconds": round(elapsed, 2),
        "total_rows_processed": total_rows,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "history_rows": history_rows,
        "rows_per_second": round(rows_per_sec, 0),
    }


def run_row_by_row_benchmark(partitions: list[str]) -> dict:
    """
    Run the load phase in row-by-row mode (one commit per INSERT).

    This is intentionally slow — it exists to demonstrate why executemany
    in a single transaction is dramatically faster. The read/validate/transform
    phases still run normally; only the load phase uses row-by-row commits.
    """
    db_path = PROJECT_ROOT / "output" / "benchmark_row_by_row.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    conn = get_connection(db_path)
    create_schema(conn)
    # Load reference data
    customers = read_customers(LARGE_DATA_DIR / "customers.csv")
    products = read_products(LARGE_DATA_DIR / "products.csv")
    load_customers(conn, customers)
    load_products(conn, products)
    valid_customers = {c["customer_id"] for c in customers}
    valid_products = {p["product_id"] for p in products}

    run_id = make_run_id()
    total_rows = 0

    print(f"  Row-by-row mode: processing {len(partitions)} partitions "
          f"(this will be slow)...")

    start_time = time.perf_counter()
    for pd in partitions:
        from backfill.controller import orders_file_path
        file_path = orders_file_path(LARGE_DATA_DIR, pd)
        rows, err = read_partition(file_path)
        if err or not rows:
            continue

        now_ts = now_iso()
        valid_rows, rejected_rows, _ = validate_rows(
            rows=rows,
            valid_customers=valid_customers,
            valid_products=valid_products,
            source_file=file_path.name,
            partition_date=pd,
            run_id=run_id,
            now_ts=now_ts,
        )
        transformed = transform_rows(valid_rows, pd, run_id, file_path.name, now_ts)
        fact_rows = resolve_latest_per_order(transformed)

        load_partition_row_by_row(
            conn=conn,
            partition_date=pd,
            transformed_rows=transformed,
            fact_rows=fact_rows,
            rejected_rows=rejected_rows,
            run_id=run_id,
            source_file=file_path.name,
        )
        total_rows += len(rows)

    elapsed = time.perf_counter() - start_time

    rows_out = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    rows_rejected = conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]
    conn.close()

    rows_per_sec = total_rows / elapsed if elapsed > 0 else 0

    return {
        "mode": "row-by-row (individual commits), workers=1",
        "workers": 1,
        "elapsed_seconds": round(elapsed, 2),
        "total_rows_processed": total_rows,
        "rows_out": rows_out,
        "rows_rejected": rows_rejected,
        "rows_per_second": round(rows_per_sec, 0),
    }


def write_performance_doc(
    pipeline_results: list[dict],
    row_by_row_result: dict | None,
    total_rows: int,
) -> None:
    """Write performance results to docs/performance.md."""
    docs_dir = PROJECT_ROOT / "docs"
    docs_dir.mkdir(exist_ok=True)

    lines = [
        "# Performance Benchmark Results",
        "",
        f"Dataset: **{total_rows:,} rows** ({BENCHMARK_DAYS} days x {ROWS_PER_DAY:,} rows/day)",
        "Generated with seed=42 for reproducibility.",
        "",
        "## 1. Throughput: workers=1 vs workers=4 (batch mode)",
        "",
        "| Mode | Workers | Elapsed (s) | Rows/sec | Total rows | Status |",
        "|------|---------|------------|----------|------------|--------|",
    ]
    for r in pipeline_results:
        lines.append(
            f"| batch (executemany) | {r['workers']} | {r['elapsed_seconds']} "
            f"| {r['rows_per_second']:,.0f} | {r['total_rows_processed']:,} | {r['status']} |"
        )

    speedup = None
    if len(pipeline_results) == 2:
        t1 = pipeline_results[0]["elapsed_seconds"]
        t4 = pipeline_results[1]["elapsed_seconds"]
        if t4 > 0:
            speedup = round(t1 / t4, 2)

    if speedup:
        lines.append("")
        lines.append(f"**Speedup (workers=4 vs workers=1): {speedup}x**")

    lines += [
        "",
        "## 2. INSERT Strategy: executemany (batch) vs row-by-row",
        "",
        "| Mode | Elapsed (s) | Rows/sec | Notes |",
        "|------|------------|----------|-------|",
    ]
    if row_by_row_result:
        rbr = row_by_row_result
        batch = pipeline_results[0]  # workers=1 for fair comparison
        speedup_batch = round(rbr["elapsed_seconds"] / batch["elapsed_seconds"], 1) if batch["elapsed_seconds"] > 0 else "N/A"
        lines.append(
            f"| executemany, 1 transaction | {batch['elapsed_seconds']} "
            f"| {batch['rows_per_second']:,.0f} | Single WAL flush per partition |"
        )
        lines.append(
            f"| row-by-row, 1 commit/INSERT | {rbr['elapsed_seconds']} "
            f"| {rbr['rows_per_second']:,.0f} | Per-row WAL flush |"
        )
        lines.append("")
        lines.append(f"**Batch INSERT speedup over row-by-row: {speedup_batch}x**")
    else:
        lines.append("| executemany | See table above | | |")
        lines.append("| row-by-row  | (skipped — use --full-benchmark to measure) | | |")

    lines += [
        "",
        "## 3. Threading Model",
        "",
        "- **ThreadPoolExecutor** parallelizes CSV reading and validation (I/O-bound).",
        "- The Python **GIL** limits CPU-bound parallelism in threads; speedup comes",
        "  primarily from overlapping disk I/O across partitions.",
        "- **SQLite writes are single-threaded** (main thread only) to avoid WAL",
        "  lock contention. The write phase is the throughput bottleneck.",
        "",
        "## 4. Suggested Next Optimizations",
        "",
        "- Use **`multiprocessing.Pool`** for read+validate+transform (bypasses GIL).",
        "- Switch target to **PostgreSQL or Parquet** to remove the single-writer limit.",
        "- Use **`PRAGMA synchronous=NORMAL`** (vs FULL) for ~2x write throughput",
        "  at the cost of slightly reduced durability guarantees.",
    ]

    perf_path = docs_dir / "performance.md"
    perf_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nPerformance report written to: {perf_path}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark the backfill pipeline.")
    parser.add_argument(
        "--skip-row-by-row",
        action="store_true",
        help="Skip the slow row-by-row benchmark.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help=(
            "Generate a clean dataset (no schema-change or empty partition) so the "
            "benchmark run reports SUCCESS. Default dataset includes these edge-case "
            "partitions intentionally, causing PARTIAL status."
        ),
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Backfill Pipeline Benchmark")
    if args.clean:
        print("Mode: CLEAN (no schema-change/empty partitions => STATUS will be SUCCESS)")
    else:
        print("Mode: DEFAULT (includes schema-change + empty partitions => STATUS will be PARTIAL)")
        print("      Use --clean to run without edge-case partitions for a SUCCESS result.")
    print("=" * 60)

    src_dir = LARGE_DATA_DIR_CLEAN if args.clean else None
    partitions = generate_large_data(clean=args.clean)
    total_rows_approx = BENCHMARK_DAYS * ROWS_PER_DAY
    print(f"\nPartitions to process: {len(partitions)}")
    print(f"Approximate total rows: {total_rows_approx:,}")

    pipeline_results = []

    print("\n--- Benchmark: batch mode, workers=1 ---")
    r1 = run_pipeline_benchmark(partitions, max_workers=1, db_suffix="w1", source_dir=src_dir)
    pipeline_results.append(r1)
    print(f"  workers=1: {r1['elapsed_seconds']}s  {r1['rows_per_second']:,.0f} rows/sec  status={r1['status']}")

    print("\n--- Benchmark: batch mode, workers=4 ---")
    r4 = run_pipeline_benchmark(partitions, max_workers=4, db_suffix="w4", source_dir=src_dir)
    pipeline_results.append(r4)
    print(f"  workers=4: {r4['elapsed_seconds']}s  {r4['rows_per_second']:,.0f} rows/sec  status={r4['status']}")

    if r1["elapsed_seconds"] > 0 and r4["elapsed_seconds"] > 0:
        speedup = r1["elapsed_seconds"] / r4["elapsed_seconds"]
        print(f"\nSpeedup with 4 workers: {speedup:.2f}x")

    row_by_row_result = None
    if not args.skip_row_by_row:
        # Limit row-by-row to first 3 partitions to keep it feasible
        sample_partitions = partitions[:3]
        sample_rows = ROWS_PER_DAY * len(sample_partitions)
        print(f"\n--- Benchmark: row-by-row mode, {len(sample_partitions)} partitions "
              f"(~{sample_rows:,} rows) ---")
        rbr = run_row_by_row_benchmark(sample_partitions)
        print(f"  row-by-row: {rbr['elapsed_seconds']}s  {rbr['rows_per_second']:,.0f} rows/sec")

        # Scale row-by-row result to full dataset for comparison table
        scale = BENCHMARK_DAYS / len(sample_partitions)
        rbr_scaled = dict(rbr)
        rbr_scaled["elapsed_seconds"] = round(rbr["elapsed_seconds"] * scale, 2)
        rbr_scaled["total_rows_processed"] = rbr["total_rows_processed"] * int(scale)
        row_by_row_result = rbr  # keep unscaled for display
    else:
        print("\n--- Row-by-row benchmark skipped (--skip-row-by-row) ---")

    write_performance_doc(pipeline_results, row_by_row_result, r1["total_rows_processed"])

    print("\n" + "=" * 60)
    print("Benchmark complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
