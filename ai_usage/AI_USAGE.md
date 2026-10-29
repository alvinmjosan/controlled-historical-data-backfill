# AI Usage Disclosure

## Tools used

1. **Antigravity (Google DeepMind)**, an agentic coding assistant backed by Claude Sonnet.
   It wrote and edited the code, tests, data, and docs in this repo. It cannot export a prompt log,
   so `prompt_history.md` is a reconstruction from locally stored conversation transcripts.
2. **Claude (claude.ai chat)**, used outside the repo to write the initial agent prompt,
   simplify it, review each iteration of the agent's output against the problem statement,
   and draft follow-up fix instructions. Several of the later prompts in `prompt_history.md`
   are review feedback written by Claude in that chat and pasted by me into Antigravity.

## Where AI accelerated my work

| Area | Role of AI |
|---|---|
| Code scaffolding | Antigravity generated the modules (`extract.py`, `validate.py`, `transform.py`, `load.py`, `reconcile.py`, `controller.py`, `__main__.py`) from the specification in the agent prompt |
| Tests | Antigravity wrote most unit and integration tests from scenario descriptions |
| Data generator and benchmark | Antigravity generated `scripts/generate_data.py`, `scripts/benchmark.py`, and `docs/performance.md` |
| README and docs | Antigravity drafted them; Claude's review flagged inconsistencies, which I relayed back for fixing |
| Review | Claude reviewed each iteration against the problem statement and listed gaps |

## Where I relied on AI heavily

- The pipeline modules and the test suite were largely AI-generated in long agent sessions.
- The two `test_extract.py` malformed-row tests were added after a Claude review pointed out that the fix had no unit test.

## What I reviewed and validated

- **BAD_ROW label problem**: the agent first changed the expected label to match incorrect behavior.
  A Claude review flagged that the labels were following the code. I relayed this, and the agent fixed
  `extract.py` to flag short and long rows as `_malformed`, and restored the `malformed_row` label.
- **Benchmark claims**: a review flagged the "32.5x" headline as overstated, since row-by-row was
  measured on only 3 partitions. I had it softened to "~30x" and the method documented.
- **README consistency**: reviews flagged the stale dry-run date, the `--days 5` vs `--days 6`
  mismatch, the stale run_id, and the placeholder GitHub URL, and I had each corrected.
- **Test and run results**: 
  - "The agent ran these commands and I reviewed the outputs; I also ran a fresh clone and pytest myself."

## Decisions and design

| Decision | Rationale |
|---|---|
| SQLite target, stdlib only | No external services; runs locally from a clean environment |
| Partition overwrite plus guarded upsert | Each partition's fact rows are deleted and reinserted in one transaction, so reruns are idempotent; the fact upsert only applies when the new `event_ts` is >= the existing one, so late data cannot overwrite newer rows |
| Single writer (main thread) | Workers read and validate in parallel; one thread writes to avoid SQLite lock contention |
| Retries | `--max-retries N` means 1 initial try + N retries (default 3 attempts). Backoff between attempts is linear (0.1 s × attempt number), with no jitter. |
| Deterministic `reject_id` | SHA-256 of `source_file + row_number`, so reruns do not duplicate rejects |
| `fact_orders` keeps latest event per order | Current state for consumers; full history in `order_status_history` |
| `history_only` rows | Valid rows superseded by a newer event; counted in `rows_out` so `rows_in = rows_out + rows_rejected` holds |

## Commit history note

The work was built interactively with an AI agent and committed in logical groups
(skeleton, generator and data, pipeline modules, controller and CLI, tests, benchmark, docs).
The commits were made close together, and their timestamps are unmodified.

## What I can explain

- The validation order in `validate.py`
- The idempotent load in `load.py`
- The restart logic in `controller.py`
- The dry-run path
- The reconciliation checks