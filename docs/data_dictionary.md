# Data Dictionary — Backfill Pipeline

## Overview

The target analytical store is a SQLite database (`output/warehouse.db`). All table and column names use `snake_case`. Timestamps are stored as ISO-8601 UTC strings (SQLite TEXT affinity).

---

## Source Schemas

### `customers.csv`
Reference file. Grain: one row per customer.

| Column | Type | Nullable | Unique | Notes |
|--------|------|----------|--------|-------|
| `customer_id` | TEXT | NO | YES | Business key (e.g., `C0001`) |
| `customer_name` | TEXT | NO | NO | Full display name |
| `country` | TEXT | NO | NO | ISO 3166-1 alpha-2 country code |

### `products.csv`
Reference file. Grain: one row per product.

| Column | Type | Nullable | Unique | Notes |
|--------|------|----------|--------|-------|
| `product_id` | TEXT | NO | YES | Business key (e.g., `P0001`) |
| `product_name` | TEXT | NO | NO | Display name |
| `category` | TEXT | NO | NO | Product category |

### `orders/orders_YYYY-MM-DD.csv`
Daily partition files. Grain: one row per order event (status change).
Same `order_id` may appear multiple times (status updates).

| Column | Type | Nullable | Unique | Notes |
|--------|------|----------|--------|-------|
| `order_id` | TEXT | NO | NO | Business key; may repeat across status updates |
| `customer_id` | TEXT | NO | NO | Foreign key → `customers.csv` |
| `product_id` | TEXT | NO | NO | Foreign key → `products.csv` |
| `quantity` | INTEGER | NO | NO | Must be ≥ 1 |
| `unit_price` | REAL | NO | NO | Must be ≥ 0 (0 = free item) |
| `status` | TEXT | NO | NO | One of: `placed`, `paid`, `shipped`, `delivered`, `cancelled` |
| `event_ts` | TEXT | NO | NO | ISO-8601 timestamp of the status event |

---

## Target Tables

### `dim_customers`
Reference dimension. Grain: one row per customer. Upserted from `customers.csv`.

| Column | Type | PK | Nullable | Unique | FK | Notes |
|--------|------|----|----|----|----|-------|
| `customer_id` | TEXT | YES | NO | YES | — | Business key |
| `customer_name` | TEXT | — | NO | NO | — | |
| `country` | TEXT | — | NO | NO | — | |
| `loaded_at` | TEXT | — | NO | NO | — | ISO-8601 UTC, set on upsert |

---

### `dim_products`
Reference dimension. Grain: one row per product. Upserted from `products.csv`.

| Column | Type | PK | Nullable | Unique | FK | Notes |
|--------|------|----|----|----|----|-------|
| `product_id` | TEXT | YES | NO | YES | — | Business key |
| `product_name` | TEXT | — | NO | NO | — | |
| `category` | TEXT | — | NO | NO | — | |
| `loaded_at` | TEXT | — | NO | NO | — | ISO-8601 UTC, set on upsert |

---

### `fact_orders`
Fact table. **Grain: one row per `order_id` (latest event wins).**

For each `order_id`, the row with the highest `event_ts` is kept. Older status
events are preserved in `order_status_history` but do not overwrite this table.

| Column | Type | PK | Nullable | Unique | FK | Notes |
|--------|------|----|----|----|----|-------|
| `order_id` | TEXT | YES | NO | YES | — | Business key from source |
| `customer_id` | TEXT | — | NO | NO | dim_customers | Referential integrity enforced |
| `product_id` | TEXT | — | NO | NO | dim_products | Referential integrity enforced |
| `quantity` | INTEGER | — | NO | NO | — | Validated: ≥ 1 |
| `unit_price` | REAL | — | NO | NO | — | Validated: ≥ 0 |
| `total_amount` | REAL | — | NO | NO | — | **Derived**: `round(quantity × unit_price, 2)` |
| `status` | TEXT | — | NO | NO | — | Normalized to lowercase |
| `event_ts` | TEXT | — | NO | NO | — | ISO-8601 UTC; latest event for this order |
| `partition_date` | TEXT | — | NO | NO | — | Date from filename (YYYY-MM-DD) |
| `run_id` | TEXT | — | NO | NO | — | Which backfill run loaded this row |
| `source_file` | TEXT | — | NO | NO | — | Filename of source CSV |
| `loaded_at` | TEXT | — | NO | NO | — | ISO-8601 UTC; when row was inserted |

---

### `order_status_history`
Append-only history. **Grain: one row per (order_id, event_ts, status) event.**

Never overwrites; INSERT OR IGNORE ensures deduplication. Preserves full
lifecycle of each order including out-of-order or late-arriving events.

| Column | Type | PK | Nullable | Notes |
|--------|------|----|----|-------|
| `order_id` | TEXT | YES (part) | NO | Business key |
| `status` | TEXT | YES (part) | NO | Normalized to lowercase |
| `event_ts` | TEXT | YES (part) | NO | ISO-8601 UTC |
| `run_id` | TEXT | — | NO | Run that inserted this row |
| `source_file` | TEXT | — | NO | Source CSV filename |

**Composite PK**: `(order_id, event_ts, status)` — guarantees each event is stored at most once.

---

### `rejected_records`
Quarantine table. **Grain: one row per rejected source row.**

| Column | Type | PK | Nullable | Notes |
|--------|------|----|----|-------|
| `reject_id` | TEXT | YES | NO | SHA-256 hash of `source_file:row_number` — deterministic |
| `run_id` | TEXT | — | NO | Run that rejected this row |
| `partition_date` | TEXT | — | NO | Partition date (YYYY-MM-DD) |
| `source_file` | TEXT | — | NO | Source CSV filename |
| `row_number` | INTEGER | — | NO | 1-indexed row number in source file (excl. header) |
| `raw_record` | TEXT | — | NO | JSON-serialized original row values |
| `reject_reason` | TEXT | — | NO | See reject reason list in README |
| `rejected_at` | TEXT | — | NO | ISO-8601 UTC timestamp |

---

### `backfill_runs`
Run-level summary. **Grain: one row per run_id.**

| Column | Type | PK | Nullable | Notes |
|--------|------|----|----|-------|
| `run_id` | TEXT | YES | NO | `run_YYYYMMDDTHHMMSSZ` |
| `started_at` | TEXT | — | NO | ISO-8601 UTC |
| `ended_at` | TEXT | — | YES | ISO-8601 UTC; null while running |
| `status` | TEXT | — | NO | `RUNNING` / `SUCCESS` / `PARTIAL` / `FAILED` |
| `params` | TEXT | — | YES | JSON-encoded CLI parameters |
| `partitions_total` | INTEGER | — | NO | Total partitions requested |
| `partitions_success` | INTEGER | — | NO | |
| `partitions_failed` | INTEGER | — | NO | |
| `rows_in` | INTEGER | — | NO | Total source rows read |
| `rows_out` | INTEGER | — | NO | Total rows written to fact_orders |
| `rows_rejected` | INTEGER | — | NO | Total rows quarantined |
| `failure_reason` | TEXT | — | YES | Overall failure message if applicable |

---

### `backfill_partitions`
Per-partition progress tracking. **Grain: one row per (run_id, partition_date).**

| Column | Type | PK | Nullable | Notes |
|--------|------|----|----|-------|
| `run_id` | TEXT | YES (part) | NO | Run ID |
| `partition_date` | TEXT | YES (part) | NO | YYYY-MM-DD |
| `status` | TEXT | — | NO | `PENDING` / `RUNNING` / `SUCCESS` / `FAILED` / `SKIPPED` |
| `rows_in` | INTEGER | — | NO | Source rows read for this partition |
| `rows_out` | INTEGER | — | NO | Rows written to fact_orders |
| `rows_rejected` | INTEGER | — | NO | Rows quarantined |
| `attempts` | INTEGER | — | NO | Number of attempts (including retries) |
| `error` | TEXT | — | YES | Error message if FAILED |
| `started_at` | TEXT | — | YES | ISO-8601 UTC |
| `ended_at` | TEXT | — | YES | ISO-8601 UTC |

---

## ID / Key Conventions

| ID Field | Format | Notes |
|----------|--------|-------|
| `run_id` | `run_YYYYMMDDTHHMMSSZ` | Timestamp-based, unique per run |
| `reject_id` | SHA-256 hex | Deterministic hash of `source_file:row_number` |
| `order_id` | `ORD-NNN-NNNNN` | Stable business key from source |
| `customer_id` | `C####` | 4-digit zero-padded |
| `product_id` | `P####` | 4-digit zero-padded |
| `partition_date` | `YYYY-MM-DD` | ISO-8601 date; derived from filename |
| All timestamps | ISO-8601 UTC | e.g., `2025-01-01T12:00:00+00:00` |
