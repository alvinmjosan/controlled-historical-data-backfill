# Backfill Pipeline — Historical Data Backfill System

> **GitHub**: https://github.com/alvinmjosan/controlled-historical-data-backfill

> **AI Usage**: [Disclosure & decisions](ai_usage/AI_USAGE.md) · [Prompt history](ai_usage/prompt_history.md)

A controlled historical backfill pipeline for daily e-commerce orders. Supports dry-run estimates, bounded parallelism, idempotent writes, progress tracking, and safe restart. Built with Python stdlib + SQLite only.

---


## Architecture & Data Flow

```
Source CSVs (data/sample/orders/orders_YYYY-MM-DD.csv)
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│                  Backfill Controller                     │
│   • Plan partitions (PENDING/SKIP/RETRY/FORCE)           │
│   • ThreadPoolExecutor (bounded parallelism)             │
│   • Retry logic (max_retries per partition)              │
│   • Progress tracking → backfill_partitions              │
└────────────┬────────────────────────────────────────────┘
             │  Per partition (parallel read/validate/transform)
             ▼
┌────────────────────────────────────┐
│  EXTRACT (extract.py)              │
│  • Read CSV rows                   │
│  • Validate header schema          │
│  • Detect missing/extra columns    │
└────────────┬───────────────────────┘
             ▼
┌────────────────────────────────────┐
│  VALIDATE (validate.py)            │
│  • Required fields                 │
│  • Type/range checks               │
│  • Status whitelist                │
│  • Reference key checks            │
│  • Duplicate detection             │
└────────────┬───────────────────────┘
             │
     ┌───────┴────────┐
     ▼                ▼
  Valid rows      Rejected rows
     │                │
     ▼                ▼
┌──────────┐   ┌──────────────────┐
│TRANSFORM │   │ rejected_records  │
│(transform│   │ (quarantine)      │
│ .py)     │   └──────────────────┘
│• total_  │
│  amount  │
│• norm ts │
│• late-   │
│  arriving│
└────┬─────┘
     │   Single writer thread (SQLite lock-safe)
     ▼
┌────────────────────────────────────┐
│  LOAD (load.py) — One Transaction  │
│  • DELETE + INSERT fact_orders     │
│  • INSERT OR IGNORE status_history │
│  • UPSERT dim_customers/products   │
│  • re-insert rejected_records      │
└────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────┐
│  TARGET: SQLite (output/warehouse.db)│
│  • fact_orders                     │
│  • order_status_history            │
│  • dim_customers / dim_products    │
│  • rejected_records                │
│  • backfill_runs                   │
│  • backfill_partitions             │
└────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────┐
│  RECONCILE (reconcile.py)          │
│  • Source = loaded + rejected      │
│  • PK uniqueness                   │
│  • FK integrity                    │
│  • sum(total_amount) check         │
│  → output/reconciliation_report.json│
└────────────────────────────────────┘
```

---

## Quick Setup (Clean Environment)

```bash
# 1. Clone / navigate to project
git clone https://github.com/alvinmjosan/controlled-historical-data-backfill.git
cd controlled-historical-data-backfill

# 2. Create and activate virtual environment
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Verify Python version (3.10+ required)
python --version
```

---

## Commands

### Generate sample data
```bash
# Small sample (committed to data/sample/) — 6 days: 4 normal + 1 schema-change (day 3) + 1 empty file (day 6)
python scripts/generate_data.py --days 6 --rows-per-day 20 --seed 42 --output-dir data/sample

# Large dataset for benchmarking
python scripts/generate_data.py --days 30 --rows-per-day 4000 --seed 42 --output-dir data/generated
```

### Dry-run (no writes, just estimates)
```bash
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06 --dry-run
```

**Sample output:**
```
============================================================
  DRY-RUN ESTIMATE  (6 partitions)
============================================================
  Partition      Status         Rows  Valid  Reject  Late
  ------------------------------------------------------------
  2025-01-01     OK               25     16       9     1
  2025-01-02     OK               25     16       9     1
  2025-01-03     SCHEMA_ERR
  2025-01-04     OK               25     16       9     1
  2025-01-05     OK               25     16       9     1
  2025-01-06     OK                0      0       0     0
  ------------------------------------------------------------
  Total rows       : 100
  Estimated valid  : 64
  Estimated rejected: 36
  Estimated late   : 4
  Missing partitions: 0
  Est. runtime     : 0.0s (at 5,000 rows/sec)
============================================================
```
*(2025-01-03 is the intentional schema-change partition; 2025-01-06 is the intentional empty file)*  
*(The SCHEMA_ERR and empty-file partitions contribute 0 rows to the Total rows count — only the 4 normal days × 25 rows = 100 are counted)*

### Run backfill
```bash
# Full date range (including empty-file day 2025-01-06)
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06

# Specific dates only
python -m backfill --source-dir data/sample --dates 2025-01-01,2025-01-03

# With custom parallelism
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06 --max-workers 2
```

### Resume after failure (default behavior)
```bash
# Re-running automatically skips SUCCESS partitions, retries FAILED ones
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06

# Force reprocess everything including SUCCESS partitions
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06 --force
```

### End-to-end restart demo (inject failure → rerun → status)

**Step 1: Delete the DB, then run with injected failure on 2025-01-04**
```bash
# PowerShell:
Remove-Item output\warehouse.db -ErrorAction SilentlyContinue
# Linux / Mac:
# rm -f output/warehouse.db
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06 --force --inject-failure-date 2025-01-04
```
```
# (log lines abridged — real output includes timestamps and run_id= prefix on every line)
INFO  [1/6] 2025-01-02 SUCCESS rows_in=25 rows_out=16 rows_rejected=9
INFO  [2/6] 2025-01-01 SUCCESS rows_in=25 rows_out=16 rows_rejected=9
INFO  [3/6] 2025-01-05 SUCCESS rows_in=25 rows_out=16 rows_rejected=9
ERROR [4/6] partition=2025-01-03 FAILED error=schema_mismatch: missing columns ['status']
ERROR [5/6] partition=2025-01-04 FAILED error=Injected failure (all 3 attempts: 1 initial try + 2 retries)
INFO  [6/6] 2025-01-06 SUCCESS rows_in=0 rows_out=0 rows_rejected=0
DONE  status=PARTIAL partitions_success=4 partitions_failed=2
```

**Step 2: Rerun (no inject flag) — 2025-01-04 is automatically retried**
```bash
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06
```
```
SKIPPED 2025-01-01 (already SUCCESS)
SKIPPED 2025-01-02 (already SUCCESS)
SKIPPED 2025-01-05 (already SUCCESS)
SKIPPED 2025-01-06 (already SUCCESS)
ERROR [1/2] partition=2025-01-03 FAILED error=schema_mismatch (data is broken — expected)
INFO  [2/2] 2025-01-04 SUCCESS rows_in=25 rows_out=16 rows_rejected=9
DONE  status=PARTIAL partitions_success=1 partitions_failed=1
```

**Step 3: Check final status**
```bash
python -m backfill --status
```
```
# (SKIPPED = no work done this run, data from the earlier run is intact)
Latest run: run_20261029T063711Z  status=PARTIAL  started=2026-10-29T06:37:11.088828+00:00

partition_date   status      rows_in  rows_out  hist_only  rejected  late  tries  error
---------------------------------------------------------------------------------------------------------
2025-01-01       SKIPPED           0         0          0         0     0      0
2025-01-02       SKIPPED           0         0          0         0     0      0
2025-01-03       FAILED            0         0          0         0     0      1  schema_mismatch: missing columns ['status']
2025-01-04       SUCCESS          25        16          2         9     1      1
2025-01-05       SKIPPED           0         0          0         0     0      0
2025-01-06       SKIPPED           0         0          0         0     0      0
```

> **Note on SKIPPED**: `SKIPPED` means the partition was already `SUCCESS` in the database
> and was not reprocessed — data was **not** lost. Use `--force` to
> reprocess all partitions regardless of prior status.

> **Note**: `--inject-failure-date` only injects a failure if the partition has not already been marked
> `SUCCESS` in the DB. If you previously ran the pipeline successfully, use `--force` to reprocess
> all partitions, or delete `output/warehouse.db` to start from a clean state.

### Check progress / status
```bash
python -m backfill --status
```

### Run reconciliation report
```bash
python -m backfill --reconcile
```

### Run tests
```bash
python -m pytest tests/ -v
```

### Run benchmark
```bash
# Default (includes edge-case partitions, status=PARTIAL)
python scripts/benchmark.py

# Clean (no edge cases, status=SUCCESS — use these numbers as authoritative)
python scripts/benchmark.py --clean
```

---

## Retry / Restart / Backfill Behavior

| Scenario | Behavior |
|---|---|
| Re-run same dates (no --force) | Partitions already SUCCESS are **skipped** |
| Re-run with --force | All partitions reprocessed, including SUCCESS |
| Partition FAILED on last run | Automatically **retried** on next run |
| Partition stale RUNNING | Treated as FAILED, retried on next run |
| --max-retries N | Each partition gets 1 initial try + N retries (default N=2, so 3 total attempts) before marking FAILED |
| One partition fails | Others continue; final status = PARTIAL |
| Same input rerun | Produces **identical** row counts (idempotent) |

---

## Naming Conventions

- **Tables & columns**: `snake_case` (e.g., `fact_orders`, `customer_id`)
- **run_id**: `run_YYYYMMDDTHHMMSSZ` (e.g., `run_20250101T120000Z`)
- **reject_id**: deterministic SHA-256 hash of `source_file + row_number`
- **partition_date**: `YYYY-MM-DD` (date from filename)
- **Timestamps**: ISO-8601 UTC (e.g., `2025-01-01T12:00:00+00:00`)
- **Source files**: `orders_YYYY-MM-DD.csv`

---

## Data Quality & Reject Reasons

| Reject Reason | Description |
|---|---|
| `missing_required_field:<name>` | Required column is null/empty |
| `invalid_quantity` | quantity is not a positive integer |
| `invalid_unit_price` | unit_price is not a number >= 0 |
| `invalid_status` | status not in {placed, paid, shipped, delivered, cancelled} |
| `invalid_event_ts` | event_ts not parseable as ISO-8601 |
| `invalid_reference_customer` | customer_id not found in dim_customers |
| `invalid_reference_product` | product_id not found in dim_products |
| `malformed_row` | Wrong column count or unparseable row |
| `duplicate_event` | Exact duplicate (order_id + event_ts + status), second+ occurrence |
| `schema_mismatch` | Whole partition fails: missing expected header columns |
| `source_file_not_found` | CSV file does not exist for requested partition |

---

## Assumptions

1. Source CSVs are UTF-8 encoded.
2. Partition date is derived from the filename, not from event_ts.
3. A "status update" (same order_id, different status or event_ts) is NOT a duplicate — it's a valid history event.
4. fact_orders keeps only the row with the LATEST event_ts per order_id; older events still go to order_status_history.
5. unit_price = 0 is valid (free items / promotions).
6. quantity must be a positive integer (>= 1).
7. dim_customers and dim_products are loaded before order validation to support reference checks.
8. SQLite WAL mode is used for better concurrent read access.
9. The single-writer pattern avoids SQLite lock contention under parallel reads.
10. Timestamps are stored as ISO-8601 strings (SQLite TEXT affinity).

---

## Sample Queries

### Daily Revenue
```sql
SELECT partition_date, ROUND(SUM(total_amount), 2) AS daily_revenue
FROM fact_orders
GROUP BY partition_date
ORDER BY partition_date;
```
**Output:** *(from sample run — see [docs/sample_output/queries.md](docs/sample_output/queries.md) for all queries)*
```
partition_date  daily_revenue
2025-01-01      14342.19
2025-01-02      29515.30
2025-01-04      16862.37
2025-01-05      20060.34
```
*(2025-01-03 absent: intentional schema_mismatch — demonstrates failure isolation)*  
*(2025-01-06 absent: empty file, 0 rows — SUCCESS with no revenue)*

### Top Customers by Spend
```sql
SELECT c.customer_name, c.country, ROUND(SUM(f.total_amount), 2) AS total_spend, COUNT(*) AS order_count
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY f.customer_id
ORDER BY total_spend DESC
LIMIT 5;
```

| customer_name | country | total_spend | order_count |
|---|---|---|---|
| Iris Taylor | JP | 13618.59 | 4 |
| Liam Jackson | MX | 10785.03 | 4 |
| Alice Johnson | US | 10077.74 | 7 |
| Olivia Garcia | AR | 9761.94 | 5 |
| Frank Miller | AU | 8172.89 | 3 |

### Reject Reasons Breakdown
```sql
SELECT reject_reason, COUNT(*) AS count
FROM rejected_records
GROUP BY reject_reason
ORDER BY count DESC;
```

| reject_reason | count |
|---|---|
| malformed_row | 4 |
| missing_required_field:customer_id | 4 |
| invalid_unit_price | 4 |
| invalid_status | 4 |
| invalid_reference_product | 4 |
| invalid_reference_customer | 4 |
| invalid_quantity | 4 |
| invalid_event_ts | 4 |
| duplicate_event | 4 |

9 reject types × 4 partitions = 36 rejected rows.

### Status History of One Order
```sql
SELECT order_id, status, event_ts, source_file
FROM order_status_history
WHERE order_id = 'ORD-000-09001'
ORDER BY event_ts;
```

| order_id | status | event_ts | source_file |
|---|---|---|---|
| ORD-000-09001 | placed | 2025-01-01T08:00:00+00:00 | orders_2025-01-01.csv |
| ORD-000-09001 | paid | 2025-01-01T10:00:00+00:00 | orders_2025-01-01.csv |
| ORD-000-09001 | shipped | 2025-01-01T14:00:00+00:00 | orders_2025-01-01.csv |

---

## Test Results

Full output saved to [docs/test_results.txt](docs/test_results.txt).

```
$ python -m pytest tests/ -v
============================= test session starts =============================
platform win32 -- Python 3.13.3, pytest-9.2.0, pluggy-1.7.0
collected 86 items

tests/test_controller.py::test_happy_path_end_to_end PASSED              [  1%]
tests/test_controller.py::test_empty_file_succeeds PASSED                [  2%]
tests/test_controller.py::test_header_only_file_succeeds PASSED          [  3%]
tests/test_controller.py::test_schema_mismatch_fails_partition PASSED    [  4%]
tests/test_controller.py::test_idempotent_two_runs PASSED                [  5%]
tests/test_controller.py::test_partial_failure_and_restart PASSED        [  6%]
tests/test_controller.py::test_dry_run_writes_nothing PASSED             [  8%]
tests/test_controller.py::test_dry_run_detects_missing_files PASSED      [  9%]
tests/test_controller.py::test_bounded_parallelism_results_identical PASSED [ 10%]
tests/test_controller.py::test_missing_source_file_fails_partition PASSED [ 11%]
tests/test_extract.py::test_read_partition_valid PASSED                  [ 12%]
tests/test_extract.py::test_read_partition_empty_file PASSED             [ 13%]
tests/test_extract.py::test_read_partition_header_only PASSED            [ 15%]
tests/test_extract.py::test_read_partition_missing_file PASSED           [ 16%]
tests/test_extract.py::test_read_partition_schema_mismatch_missing_column PASSED [ 17%]
tests/test_extract.py::test_read_partition_extra_column_ignored PASSED   [ 18%]
tests/test_extract.py::test_short_row_flagged_malformed_and_rejected PASSED [ 19%]
tests/test_extract.py::test_long_row_flagged_malformed_and_rejected PASSED [ 20%]
tests/test_load.py::test_happy_path_load PASSED                          [ 22%]
tests/test_load.py::test_idempotent_load PASSED                          [ 23%]
tests/test_load.py::test_late_arriving_does_not_overwrite_newer_fact PASSED [ 24%]
tests/test_load.py::test_rejected_records_stored PASSED                  [ 25%]
tests/test_load.py::test_status_history_append_only PASSED               [ 26%]
tests/test_load.py::test_empty_batch_load PASSED                         [ 27%]
tests/test_load.py::test_history_only_rows_counted PASSED                [ 29%]
tests/test_load.py::test_reject_records_idempotent PASSED                [ 30%]
tests/test_reconcile.py::test_reconciliation_passes PASSED               [ 31%]
tests/test_reconcile.py::test_reconciliation_row_balance PASSED          [ 32%]
tests/test_reconcile.py::test_reconciliation_report_written PASSED       [ 33%]
tests/test_reconcile.py::test_reconciliation_idempotent PASSED           [ 34%]
tests/test_sample_data.py::test_expected_labels_file_is_valid PASSED     [ 36%]
tests/test_sample_data.py::test_reject_reasons_match_expected_labels PASSED [ 37%]
tests/test_sample_data.py::test_schema_mismatch_partition_fails PASSED   [ 38%]
tests/test_sample_data.py::test_empty_file_partition_succeeds_with_zero_rows PASSED [ 39%]
tests/test_sample_data.py::test_all_reject_reasons_are_labelled PASSED   [ 40%]
tests/test_sample_data.py::test_late_arriving_rows_are_accepted PASSED   [ 41%]
tests/test_transform.py::test_total_amount_computed_correctly PASSED     [ 43%]
tests/test_transform.py::test_total_amount_rounded_to_2dp PASSED         [ 44%]
tests/test_transform.py::test_unit_price_zero_total_amount_zero PASSED   [ 45%]
tests/test_transform.py::test_event_ts_normalized_to_utc PASSED          [ 46%]
tests/test_transform.py::test_status_normalized_to_lowercase PASSED      [ 47%]
tests/test_transform.py::test_metadata_attached PASSED                   [ 48%]
tests/test_transform.py::test_resolve_latest_per_order_keeps_newest PASSED [ 50%]
tests/test_transform.py::test_resolve_latest_per_order_different_orders PASSED [ 51%]
tests/test_validate.py::test_valid_row_passes PASSED                     [ 52%]
tests/test_validate.py::test_multiple_valid_rows PASSED                  [ 53%]
tests/test_validate.py::test_missing_required_field[order_id] PASSED     [ 54%]
tests/test_validate.py::test_missing_required_field[customer_id] PASSED  [ 55%]
tests/test_validate.py::test_missing_required_field[product_id] PASSED   [ 56%]
tests/test_validate.py::test_missing_required_field[quantity] PASSED     [ 58%]
tests/test_validate.py::test_missing_required_field[unit_price] PASSED   [ 59%]
tests/test_validate.py::test_missing_required_field[status] PASSED       [ 60%]
tests/test_validate.py::test_missing_required_field[event_ts] PASSED     [ 61%]
tests/test_validate.py::test_invalid_quantity_zero PASSED                [ 62%]
tests/test_validate.py::test_invalid_quantity_negative PASSED            [ 63%]
tests/test_validate.py::test_invalid_quantity_string PASSED              [ 65%]
tests/test_validate.py::test_valid_quantity_one PASSED                   [ 66%]
tests/test_validate.py::test_invalid_unit_price_negative PASSED          [ 67%]
tests/test_validate.py::test_valid_unit_price_zero PASSED                [ 68%]
tests/test_validate.py::test_invalid_unit_price_string PASSED            [ 69%]
tests/test_validate.py::test_valid_statuses[placed] PASSED               [ 70%]
tests/test_validate.py::test_valid_statuses[paid] PASSED                 [ 72%]
tests/test_validate.py::test_valid_statuses[shipped] PASSED              [ 73%]
tests/test_validate.py::test_valid_statuses[delivered] PASSED            [ 74%]
tests/test_validate.py::test_valid_statuses[cancelled] PASSED            [ 75%]
tests/test_validate.py::test_invalid_status[returned] PASSED             [ 76%]
tests/test_validate.py::test_invalid_status[pending] PASSED              [ 77%]
tests/test_validate.py::test_invalid_status[refunded] PASSED             [ 79%]
tests/test_validate.py::test_invalid_status[RETURNED] PASSED             [ 80%]
tests/test_validate.py::test_cancelled_status_is_valid PASSED            [ 81%]
tests/test_validate.py::test_valid_event_ts_formats[2025-01-01T10:00:00+00:00] PASSED [ 82%]
tests/test_validate.py::test_valid_event_ts_formats[2025-01-01T10:00:00Z] PASSED [ 83%]
tests/test_validate.py::test_valid_event_ts_formats[2025-01-01T10:00:00] PASSED [ 84%]
tests/test_validate.py::test_valid_event_ts_formats[2025-01-01 10:00:00] PASSED [ 86%]
tests/test_validate.py::test_invalid_event_ts[not-a-date] PASSED         [ 87%]
tests/test_validate.py::test_invalid_event_ts[01/01/2025] PASSED         [ 88%]
tests/test_validate.py::test_invalid_event_ts[2025/01/01] PASSED         [ 89%]
tests/test_validate.py::test_invalid_event_ts[2025-13-01T00:00:00] PASSED [ 90%]
tests/test_validate.py::test_invalid_customer_reference PASSED           [ 91%]
tests/test_validate.py::test_invalid_product_reference PASSED            [ 93%]
tests/test_validate.py::test_exact_duplicate_rejected PASSED             [ 94%]
tests/test_validate.py::test_status_update_not_duplicate PASSED          [ 95%]
tests/test_validate.py::test_late_arriving_counted PASSED                [ 96%]
tests/test_validate.py::test_on_time_row_not_counted_as_late PASSED      [ 97%]
tests/test_validate.py::test_malformed_row_too_few_columns PASSED        [ 98%]
tests/test_validate.py::test_reject_id_is_deterministic PASSED           [100%]

============================== 86 passed in 1.30s ==============================
```

8 new tests added to verify the malformed-row fix and the sample dataset end-to-end:

**`test_extract.py` (2 new — exercise the real CSV reader path):**
- `test_short_row_flagged_malformed_and_rejected` — BAD_ROW,missing_cols sets `_malformed=True`, rejected as `malformed_row`
- `test_long_row_flagged_malformed_and_rejected` — 9-field row under 7-col header, None overflow key detected, rejected as `malformed_row`

**`test_sample_data.py` (6 — end-to-end integration against data/sample):**
- `test_expected_labels_file_is_valid` — expected JSON file is well-formed
- `test_reject_reasons_match_expected_labels` — pipeline rejects match labelled reasons
- `test_schema_mismatch_partition_fails` — 2025-01-03 partition fails with correct error
- `test_empty_file_partition_succeeds_with_zero_rows` — 2025-01-06 succeeds with 0 rows
- `test_all_reject_reasons_are_labelled` — no surprise unlabelled rejects
- `test_late_arriving_rows_are_accepted` — late-arriving rows accepted, not rejected

---

## Performance Results

Dataset: **100,000 rows** (25 days × 4,000 rows/day, seed=42). Run: `python scripts/benchmark.py --clean`.

### Clean run (authoritative, status = SUCCESS)

| Mode | Workers | Elapsed (s) | Rows/sec | Total rows | Status |
|------|---------|-------------|----------|------------|--------|
| batch (executemany) | 1 | 4.20 | 23,816 | 100,000 | SUCCESS |
| batch (executemany) | 4 | 4.64 | 21,558 | 100,000 | SUCCESS |

**Workers speedup: 0.91x** — effectively no gain. This is expected: SQLite enforces a single
writer, and Python threads share the GIL. The `ThreadPoolExecutor` overlaps CSV reads but the
write phase is fully serialized. **The optimisation that matters is batch inserts, not thread count.**
Multiprocessing (which bypasses the GIL) is the recommended next step.

### INSERT strategy: batch vs row-by-row

Row-by-row was measured on **3 partitions (~12,000 rows)** because it is intentionally slow.
The rate (rows/sec) is directly comparable — it is a steady-state throughput figure:

| Mode | Rows/sec | Notes |
|------|----------|-------|
| executemany, 1 transaction | 23,816 | 25 partitions × 4,000 rows |
| row-by-row, 1 commit/INSERT | 732 | 3 partitions × 4,000 rows (rate comparable) |

**Batch INSERT speedup: ~30x** (measured on this machine; exact ratio varies with partition size and OS scheduling).

### Default run (with edge-case partitions, status = PARTIAL)

Running without `--clean` injects a schema-change partition (day 3) and empty-file partition
(day 25). Status = PARTIAL intentionally — demonstrates failure isolation.

| Mode | Workers | Rows/sec | Status |
|------|---------|----------|--------|
| batch (executemany) | 1 | 36,725 | PARTIAL* |
| batch (executemany) | 4 | 37,095 | PARTIAL* |

*PARTIAL is expected. One partition (schema-change) fails; all others succeed.

See [docs/performance.md](docs/performance.md) for full analysis and threading model discussion.

> **Note**: Throughput figures vary by run and machine (background I/O, OS scheduling, SQLite page
> cache warm-up). Numbers differ from run to run; use the clean run figures as
> the authoritative throughput benchmark.

---

## Known Limitations

1. **SQLite concurrency**: Single-writer pattern limits write throughput; for high-volume production use, migrate to PostgreSQL or Parquet.
2. **GIL constraint**: ThreadPoolExecutor helps with I/O-bound CSV reading but not CPU-bound processing; multiprocessing would improve CPU-bound transforms.
3. **No schema registry**: Schema validation is hardcoded in config.py; a real system would use a registry (e.g., Confluent Schema Registry, Great Expectations).
4. **No alerting**: Failures are logged and recorded, but no email/Slack notifications.
5. **Memory**: All rows for a partition are loaded into memory; very large single-day files (millions of rows) may require streaming/chunked processing.
6. **SQLite file locking**: Running multiple backfill processes concurrently against the same DB file is not safe; use a coordination layer.
7. **Single process at a time**: The pipeline assumes only one `python -m backfill` process runs against the same `warehouse.db` at a time. Concurrent processes would cause SQLite lock errors. Use a job scheduler (e.g., Airflow) that enforces single-instance execution.

---

## Possible Next Steps

- **Multiprocessing for validate/transform**: the benchmark showed no gain from threads
  (0.91x) because of the GIL and the single SQLite writer, so process-based workers are the natural next step.
- **PostgreSQL or Parquet target**: removes the single-writer limit and the one-process-at-a-time restriction.
- **Streaming reads for large partitions**: each partition is currently loaded fully into memory.
- **Run locking**: a lock file or DB-level lock so two backfills cannot start against the same database.
- **Orchestration and alerting** (Airflow/Prefect, Slack on PARTIAL/FAILED): to schedule backfills and notify the operator.
