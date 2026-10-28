# Performance Benchmark Results

Dataset: **100,000 rows** (25 days × 4,000 rows/day, seed=42).
Generated with seed=42 for reproducibility.

> **Two benchmark runs are tracked here:**
> - **Default run** (`python scripts/benchmark.py`): includes a schema-change partition (day 3)
>   and an empty-file partition (day 25) to exercise all edge cases. Status = PARTIAL (intentional).
> - **Clean run** (`python scripts/benchmark.py --clean`): all 25 partitions are valid.
>   Status = SUCCESS. **Use these numbers as the authoritative throughput figures.**

---

## 1. Throughput: workers=1 vs workers=4 (batch mode, --clean run)

| Mode | Workers | Elapsed (s) | Rows/sec | Total rows | Status |
|------|---------|------------|----------|------------|--------|
| batch (executemany) | 1 | 4.20 | 23,816 | 100,000 | SUCCESS |
| batch (executemany) | 4 | 4.64 | 21,558 | 100,000 | SUCCESS |

**Speedup (workers=4 vs workers=1): 0.91x** — essentially no gain.

**Why minimal speedup from more workers?**
SQLite enforces a single writer at a time. The ThreadPoolExecutor parallelizes CSV
reading and validation (I/O-bound), but the write phase is serialized through the
main thread — so all thread overhead yields no throughput gain. On a machine with
much slower disk I/O or a remote data source, multi-worker parallelism would benefit
the read+validate stages more noticeably.

> **The optimisation that matters is batch inserts, not thread count.**
> See Section 2 below.

---

## 2. INSERT Strategy: executemany (batch) vs row-by-row (measured)

> **Methodology note**: Row-by-row was measured on **3 partitions (~12,000 rows)**
> because it is intentionally slow. The rate (rows/sec) is directly comparable
> to the batch rate measured over 100,000 rows — both are steady-state throughput
> figures for the same hardware. This is not a full 25-partition row-by-row run.

| Mode | Elapsed (s) | Rows/sec | Notes |
|------|------------|----------|-------|
| executemany, 1 transaction | 4.20 | 23,816 | Single WAL flush per partition — 25 partitions |
| row-by-row, 1 commit/INSERT | 10.94 | 732 | Per-row WAL flush — 3 partitions × 4,000 rows |

**Batch INSERT speedup over row-by-row: ~32.5x** (23,816 ÷ 732, measured, not estimated)

At the row-by-row rate, processing all 25 partitions (100,000 rows) would take
approximately **137 seconds** vs **4.2 seconds** with batch inserts.

---

## 3. Default run (with edge-case partitions) — for reference

Running without `--clean` injects a schema-change partition (2025-01-03, missing
`status` column) and an empty-file partition (last day). Both exercises pipeline
failure-isolation. The final status is **PARTIAL** — one partition fails, the rest
succeed.

Previous numbers from the default run:

| Mode | Workers | Elapsed (s) | Rows/sec | Total rows | Status |
|------|---------|------------|----------|------------|--------|
| batch (executemany) | 1 | 2.51 | 36,725 | 92,069 | PARTIAL* |
| batch (executemany) | 4 | 2.48 | 37,095 | 92,069 | PARTIAL* |

*PARTIAL is expected and intentional — one partition (schema-change) fails by design,
demonstrating failure isolation. The remaining partitions all complete successfully.

---

## 4. Threading Model

- **ThreadPoolExecutor** parallelizes CSV reading and validation (I/O-bound).
- The Python **GIL** limits CPU-bound parallelism in threads; speedup comes
  primarily from overlapping disk I/O across partitions.
- **SQLite writes are single-threaded** (main thread only) to avoid WAL
  lock contention. The write phase is the throughput bottleneck.
- **Multiprocessing** (bypassing the GIL) is the recommended next step for
  CPU-bound transform work.

---

## 5. Suggested Next Optimizations

- Use **`multiprocessing.Pool`** for read+validate+transform (bypasses GIL).
- Switch target to **PostgreSQL or Parquet** to remove the single-writer limit.
- Use **`PRAGMA synchronous=NORMAL`** (vs FULL) for ~2x write throughput
  at the cost of slightly reduced durability guarantees.