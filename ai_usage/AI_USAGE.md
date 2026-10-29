# AI Usage Disclosure

## Tool used

**Antigravity (Google DeepMind)** — an agentic AI coding assistant backed by Claude Sonnet.
All prompts were submitted through the Antigravity chat interface.
The tool cannot export a full prompt/response log, so `prompt_history.md` is a reconstruction
from the conversation transcripts stored locally by the tool.

---

## Where AI accelerated my work

| Area | Role of AI |
|---|---|
| Code scaffolding | Generated initial module stubs (`extract.py`, `validate.py`, `transform.py`, `load.py`, `reconcile.py`, `controller.py`, `__main__.py`) from my specification |
| Test writing | Wrote the majority of unit and integration tests based on my descriptions of the scenarios |
| README drafting | Drafted section text, tables, and code blocks; I reviewed and corrected every section |
| Data generator | Generated `scripts/generate_data.py` from my spec (6-day sample, seed=42, all edge-case rows) |
| Benchmark script | Generated `scripts/benchmark.py` and `docs/performance.md` from my spec |

---

## Where I relied on the AI heavily

- The pipeline modules (`extract.py`, `validate.py`, `transform.py`, `load.py`, `controller.py`) were largely AI-generated within a single long session.
- The test suite (`test_controller.py`, `test_validate.py`, `test_transform.py`, `test_load.py`, `test_reconcile.py`) was written by the AI based on my descriptions of the required test cases.
- The two new `test_extract.py` malformed-row tests were added in a follow-up prompt after I identified the gap.

---

## What I reviewed, changed, and validated

- **Caught the BAD_ROW label problem**: the AI's first fix changed the expected label to match wrong code. I identified that the code and spec disagreed and directed the AI to fix `extract.py` instead (detect `None` values from `csv.DictReader` and mark rows as `_malformed=True`).
- **Ran all tests myself**: `python -m pytest tests/ -v` — 86 passed, confirmed locally.
- **Ran the clean-environment flow**: verified generate → dry-run → backfill → rerun → inject failure → resume → `--status` → `--reconcile` all produce the outputs shown in the README.
- **Checked benchmark claims**: caught the "32.5x" headline overstating results from a small sample; directed the AI to soften it to "~30x".
- **Reviewed all README sections**: caught the stale dry-run date (`2025-01-05` → `2025-01-06`), collapsed test lines, stale run_id in Step 3, "in the same session" wording, and the `--days 5` vs `--days 6` inconsistency.
- **Reviewed the retry semantics**: confirmed `range(1, max_retries + 2)` gives 1 initial try + 2 retries = 3 total attempts.

---

## Decisions I own

| Decision | Rationale |
|---|---|
| **SQLite as the target** | Zero external dependencies; required by the constraint of Python stdlib + SQLite only. WAL mode used for better concurrent reads. |
| **Partition overwrite (DELETE + INSERT) not upsert** | Makes reruns idempotent per partition; cleaner than partial-row merges for date-partitioned batch loads. |
| **Single writer thread** | Avoids SQLite lock contention when multiple threads produce results concurrently. |
| **Retry semantics** | `--max-retries N` means N retries after the initial attempt (N=2 default → 3 total tries). Exponential backoff with jitter between attempts. |
| **Reject-ID as SHA-256 of source_file + row_number** | Deterministic so idempotent reruns don't duplicate rejected records. |
| **`fact_orders` keeps only the latest event per order_id** | Business rule: analytical consumers want current state. Full history goes to `order_status_history`. |
| **`history_only` row category** | Rows that are valid but superseded by a newer event; counted in `rows_out` so partition row balance always holds (rows_in = rows_out + rows_rejected). |

---

## Commit history note

The work was built interactively with an AI agent over a single session and committed in logical groups
(DB schema and load, controller and CLI, reconcile, tests, benchmark, docs). The commit timestamps
reflect that development pattern. Artifact timestamps in the Antigravity tool can be used to verify
the 72-hour window.

---

## What a reviewer can ask me to explain

I can walk through:
- The validation order in `validate.py` (malformed check → required fields → type/range → status whitelist → reference keys → duplicate detection)
- The idempotent load in `load.py` (DELETE by partition_date, then INSERT fact_orders; INSERT OR IGNORE for status_history; UPSERT for dims; deterministic reject_id prevents duplicate rejects)
- The restart logic in `controller.py` (at startup, RUNNING rows are treated as FAILED; SUCCESS rows are skipped unless `--force`; FAILED rows are retried)
- The dry-run path (reads and validates in memory, no DB writes, prints partition-level estimates)
- The reconciliation checks (row balance, PK uniqueness, FK integrity, sum(total_amount) recompute, reject traceability)
