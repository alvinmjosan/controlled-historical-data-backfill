# Sample Query Results

Results after running the backfill against `data/sample` (2025-01-01 → 2025-01-06).

- `2025-01-03` intentionally contains a schema-change file (missing `status` column) → **FAILED** (demonstrates schema-mismatch handling)
- `2025-01-06` is an empty file (0 bytes) → **SUCCESS** with 0 rows (demonstrates empty-file handling)

Run command:
```bash
python -m backfill --source-dir data/sample --start-date 2025-01-01 --end-date 2025-01-06 --force
```

---

## Daily Revenue

```sql
SELECT partition_date, ROUND(SUM(total_amount), 2) AS daily_revenue
FROM fact_orders
GROUP BY partition_date
ORDER BY partition_date;
```

| partition_date | daily_revenue |
|---|---|
| 2025-01-01 | 14342.19 |
| 2025-01-02 | 29515.30 |
| 2025-01-04 | 16862.37 |
| 2025-01-05 | 20060.34 |

*(2025-01-03 absent: schema_mismatch caused partition to FAIL — intentional test case)*
*(2025-01-06 absent: empty file partition loaded 0 rows — SUCCESS with no revenue)*

---

## Top 5 Customers by Spend

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

---

## Reject Reasons Breakdown

```sql
SELECT reject_reason, COUNT(*) AS count
FROM rejected_records
GROUP BY reject_reason
ORDER BY count DESC;
```

| reject_reason | count |
|---|---|
| missing_required_field:product_id | 4 |
| missing_required_field:customer_id | 4 |
| invalid_unit_price | 4 |
| invalid_status | 4 |
| invalid_reference_product | 4 |
| invalid_reference_customer | 4 |
| invalid_quantity | 4 |
| invalid_event_ts | 4 |
| duplicate_event | 4 |

9 reject types × 4 partitions = 36 total rejected rows (out of 100 source rows across 4 successful partitions).

> **Note**: The `BAD_ROW,missing_cols` row in each partition is rejected as `missing_required_field:product_id`
> because `csv.DictReader` fills all 7 header slots from the 2-column row, leaving `product_id` empty.

---

## Status History for One Order

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

*(ORD-000-09001 appears 3 times with different statuses — valid status history, not duplicates)*

---

## Reconciliation Report Summary

```
Overall: [PASSED]
[OK ] partition_row_balance   — rows_in == rows_out + rows_rejected per partition
[OK ] fact_orders_pk_unique   — no duplicate order_id
[OK ] referential_integrity   — no orphan customer_id or product_id
[OK ] total_amount_reconciliation — sum(total_amount) matches recomputed quantity×unit_price
[OK ] reject_traceability     — every rejected row has run_id, source_file, row_number, reason
```

---

## Run Summary

| run_id | status | partitions_total | partitions_success | partitions_failed | rows_in | rows_out | rows_rejected |
|---|---|---|---|---|---|---|---|
| run_20261028T160456Z | PARTIAL | 6 | 5 | 1 | 100 | 64 | 36 |

PARTIAL because `2025-01-03` is deliberately broken (schema change) to demonstrate failure isolation.
`2025-01-06` succeeds with 0 rows (empty file is a valid empty batch).
