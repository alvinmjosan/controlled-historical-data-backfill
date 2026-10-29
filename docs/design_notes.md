# Design Notes & Trade-offs

## Why SQLite?

SQLite was chosen as the target store for this assignment because:

1. **Zero setup**: Runs on any machine with Python, no server required. The assignment
   explicitly restricts us to no Docker or cloud services.
2. **Transactional correctness**: SQLite supports ACID transactions, `ON CONFLICT` clauses,
   and WAL (Write-Ahead Logging), all of which are used by this pipeline.
3. **File portability**: A single `.db` file is easy to inspect, share, and include in a
   clean-environment demo.

**In production**, SQLite has significant limitations (see Known Limitations in README).
A real team would migrate to PostgreSQL for concurrent writes or Parquet/Delta Lake for
analytics at scale.

---

## Why Partition Overwrite + Upsert?

### Partition overwrite for `fact_orders`
The idempotency strategy is **DELETE + INSERT** within a single transaction:

```sql
DELETE FROM fact_orders WHERE partition_date = ?;
-- then re-insert from fresh source data
```

**Why not MERGE/UPSERT only?**
- Idempotent overwrite is simpler to reason about: a re-run always produces the
  same result as the first run.
- It cleanly handles the case where a previously valid row is now invalid in the
  source (the old fact row is removed, the new reject is recorded).
- It avoids accumulation of stale rows if the source data is corrected.

### UPSERT guard for late-arriving data
For orders that span partition dates (late-arriving events), we add a WHERE guard:

```sql
ON CONFLICT(order_id) DO UPDATE SET ...
WHERE excluded.event_ts >= fact_orders.event_ts
```

This ensures that a late-arriving **older** event cannot overwrite a **newer** fact row
from another partition. The older event is still captured in `order_status_history`.

---

## Why a Single Writer Thread?

SQLite does not support concurrent writes from multiple threads without locking.
Even with WAL mode enabled, write transactions are serialized. Using a
`ThreadPoolExecutor` for reads + validation (I/O-bound) and then writing in the
**main thread** (after collecting results from futures) avoids:

- `sqlite3.OperationalError: database is locked` errors
- Race conditions on shared connection state

This "single writer" pattern is safe and correct. The trade-off is that write
throughput is bounded by a single thread.

**Alternative (multiprocessing)**: Each worker could use its own SQLite connection
to an in-memory or per-process DB, with results merged into the main DB at the end.
This bypasses the GIL for CPU-bound work but adds complexity.

---

## Why `order_status_history` is Append-Only

Business requirement: *"Preserve change or event history instead of silently overwriting it."*

Using `INSERT OR IGNORE` with a composite PK `(order_id, event_ts, status)` ensures:
- Every distinct event is recorded exactly once.
- Re-running the same partition never creates duplicate history rows.
- Out-of-order or late-arriving events are captured without disturbing `fact_orders`.

---

## Reject / Quarantine Design

All validation failures go to `rejected_records` (never silently dropped). Key decisions:

1. **Deterministic `reject_id`**: SHA-256 of `source_file:row_number`. Allows idempotent
   re-insertion: if a partition is reloaded, the same bad row always gets the same `reject_id`.
2. **`raw_record` as JSON**: Preserves the original values for debugging, even if they
   would fail schema parsing.
3. **Rules applied in order**: Earlier rules (malformed, required fields) short-circuit later
   ones (reference checks) so a single row gets one clear reject reason, not a cascade.

---

## Parallelism Trade-offs

| Approach | GIL Impact | Write Safety | Complexity |
|----------|------------|--------------|------------|
| Single thread | N/A | Safe | Low |
| ThreadPoolExecutor (current) | CPU-bound work limited by GIL | Single-writer pattern needed | Low |
| ProcessPoolExecutor | No GIL limitation | Each process needs its own DB connection / merge step | Medium |
| asyncio | No GIL but single-threaded | Safe | Medium |

The current ThreadPoolExecutor approach is appropriate for I/O-bound CSV reads (network
or disk). CPU-bound validation gets limited benefit from threads due to the GIL.

---

## Naming Decisions

- **`snake_case`**: Consistent with SQL conventions and Python variable naming.
- **`run_id` as `run_YYYYMMDDTHHMMSSZ`**: Human-readable, sortable lexicographically,
  and avoids UUID opacity. For high-frequency runs (< 1 second apart), a UUID suffix
  could be added.
- **`reject_id` as hash**: Deterministic identity allows `INSERT OR REPLACE` to be truly
  idempotent — re-running the same source always produces the same reject_id.
- **`source_file` in every row**: Maintains data lineage from analytical store back to
  the exact source file for auditability.

---

## Limitations

1. **Memory**: All rows for a partition are loaded into memory at once.
   For very large single-day files (>1M rows), a streaming/chunked reader would
   be needed.
2. **SQLite write serialization**: A single SQLite writer is safe but limits write
   throughput. Postgres with proper connection pooling would remove this bottleneck.
3. **No schema registry**: Column definitions are hardcoded in `config.py`. A production
   system would use a schema registry or validation framework (Great Expectations, Pydantic).
4. **No alerting**: Failures are logged and recorded in the DB, but operators must actively
   query `backfill_runs` or read log files to discover failures.
5. **Simple retry**: Retry uses a linear back-off (0.1 s × attempt number) with no jitter.
   Production would use exponential back-off with jitter.
6. **dim tables not SCD Type 2**: Customer/product changes overwrite the previous record
   (SCD Type 1). Adding SCD Type 2 would require effective date columns and a valid_to
   sentinel.
