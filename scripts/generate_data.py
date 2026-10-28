"""
Data generator for the backfill pipeline.

Generates reproducible sample and large-scale datasets with:
- Normal rows, boundary values (qty=1, price=0, max values)
- Duplicate events (same order_id + event_ts + status)
- Status updates for the same order_id (valid history events)
- Missing required fields
- Malformed rows (wrong column count)
- Invalid status values
- Invalid reference keys (non-existent customer_id / product_id)
- Late-arriving rows (event_ts date != file's partition date)
- Out-of-order status updates for the same order_id
- One partition with schema change (missing 'status' column)
- One empty file (zero bytes)

Usage:
    python scripts/generate_data.py --days 6 --rows-per-day 20 --seed 42 --output-dir data/sample
    python scripts/generate_data.py --days 30 --rows-per-day 4000 --seed 42 --output-dir data/generated
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Reference data pools
# ---------------------------------------------------------------------------

CUSTOMERS: list[dict] = [
    {"customer_id": f"C{i:04d}", "customer_name": name, "country": country}
    for i, (name, country) in enumerate(
        [
            ("Alice Johnson", "US"),
            ("Bob Smith", "UK"),
            ("Carol White", "DE"),
            ("Dave Brown", "FR"),
            ("Eve Davis", "CA"),
            ("Frank Miller", "AU"),
            ("Grace Wilson", "US"),
            ("Hank Moore", "UK"),
            ("Iris Taylor", "JP"),
            ("Jake Anderson", "IN"),
            ("Karen Thomas", "BR"),
            ("Liam Jackson", "MX"),
            ("Mia Harris", "ZA"),
            ("Noah Martin", "NG"),
            ("Olivia Garcia", "AR"),
        ],
        start=1,
    )
]

PRODUCTS: list[dict] = [
    {"product_id": f"P{i:04d}", "product_name": name, "category": cat}
    for i, (name, cat) in enumerate(
        [
            ("Widget A", "Electronics"),
            ("Widget B", "Electronics"),
            ("Gadget Pro", "Electronics"),
            ("Smart Watch", "Electronics"),
            ("Running Shoes", "Apparel"),
            ("Yoga Mat", "Sports"),
            ("Coffee Mug", "Kitchen"),
            ("Water Bottle", "Kitchen"),
            ("Notebook", "Stationery"),
            ("Pen Set", "Stationery"),
            ("Desk Lamp", "Home"),
            ("Chair Cushion", "Home"),
            ("Backpack", "Accessories"),
            ("Sunglasses", "Accessories"),
            ("Headphones", "Electronics"),
        ],
        start=1,
    )
]

VALID_STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
ORDERS_COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "status", "event_ts"]


def _make_event_ts(dt: date, rng: random.Random, offset_hours: int = 0) -> str:
    """Create an ISO-8601 UTC timestamp on the given date."""
    hour = rng.randint(0, 23) if offset_hours == 0 else offset_hours
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    dt_obj = datetime(dt.year, dt.month, dt.day, hour, minute, second, tzinfo=timezone.utc)
    return dt_obj.isoformat()


def _make_order_id(day_idx: int, row_idx: int) -> str:
    return f"ORD-{day_idx:03d}-{row_idx:05d}"


def generate_normal_row(
    rng: random.Random,
    order_id: str,
    partition_date: date,
    status: str | None = None,
) -> dict:
    """Generate a valid normal order row."""
    customer = rng.choice(CUSTOMERS)
    product = rng.choice(PRODUCTS)
    status = status or rng.choice(VALID_STATUSES)
    qty = rng.randint(1, 20)
    price = round(rng.uniform(0.01, 500.0), 2)

    return {
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "product_id": product["product_id"],
        "quantity": qty,
        "unit_price": price,
        "status": status,
        "event_ts": _make_event_ts(partition_date, rng),
    }


def generate_dataset(
    output_dir: str | Path,
    days: int = 5,
    rows_per_day: int = 20,
    seed: int = 42,
    start_date: str = "2025-01-01",
) -> dict:
    """
    Generate a complete dataset with all required data quality scenarios.

    Returns a dict with paths to generated files and expected labels for bad rows.
    """
    rng = random.Random(seed)
    output = Path(output_dir)
    orders_dir = output / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)

    # Write reference files
    _write_csv(output / "customers.csv", CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(output / "products.csv", PRODUCTS, ["product_id", "product_name", "category"])

    start = date.fromisoformat(start_date)
    all_labels: list[dict] = []   # expected bad-row labels (separate from input)
    generated_files: list[str] = []

    for day_idx in range(days):
        partition_date = start + timedelta(days=day_idx)
        date_str = partition_date.isoformat()
        file_path = orders_dir / f"orders_{date_str}.csv"
        generated_files.append(str(file_path))

        # --- Special cases for specific day indices ---

        # Day 3 (index 2): schema change — missing 'status' column
        if day_idx == 2:
            _write_schema_change_partition(file_path, partition_date, day_idx, rng, rows_per_day)
            all_labels.append({
                "file": file_path.name,
                "case": "schema_mismatch",
                "expected_partition_status": "FAILED",
                "reason": "schema_mismatch: missing columns ['status']",
            })
            continue

        # Last day: empty file (0 bytes)
        if day_idx == days - 1:
            file_path.write_bytes(b"")
            all_labels.append({
                "file": file_path.name,
                "case": "empty_file",
                "expected_partition_status": "SUCCESS",
                "expected_rows_out": 0,
                "reason": "empty file (valid empty batch)",
            })
            continue

        rows: list[dict | list] = []
        row_labels: list[dict] = []
        base_row_count = max(rows_per_day - 10, 5)  # leave room for injected bad rows

        # --- Normal rows ---
        for row_idx in range(base_row_count):
            oid = _make_order_id(day_idx, row_idx)
            rows.append(generate_normal_row(rng, oid, partition_date))

        # --- Boundary values ---
        # qty=1, price=0 (free item, unit_price=0 is valid)
        oid_boundary = _make_order_id(day_idx, 9000)
        rows.append({
            "order_id": oid_boundary,
            "customer_id": CUSTOMERS[0]["customer_id"],
            "product_id": PRODUCTS[0]["product_id"],
            "quantity": 1,
            "unit_price": 0.0,
            "status": "placed",
            "event_ts": _make_event_ts(partition_date, rng),
        })

        # --- Status update (same order_id, different status — NOT a duplicate) ---
        # This order goes through: placed -> paid -> shipped
        oid_history = _make_order_id(day_idx, 9001)
        ts1 = datetime(partition_date.year, partition_date.month, partition_date.day, 8, 0, 0, tzinfo=timezone.utc).isoformat()
        ts2 = datetime(partition_date.year, partition_date.month, partition_date.day, 10, 0, 0, tzinfo=timezone.utc).isoformat()
        ts3 = datetime(partition_date.year, partition_date.month, partition_date.day, 14, 0, 0, tzinfo=timezone.utc).isoformat()
        customer_ref = CUSTOMERS[1]["customer_id"]
        product_ref = PRODUCTS[1]["product_id"]
        base_price = 99.99

        rows.append({"order_id": oid_history, "customer_id": customer_ref, "product_id": product_ref, "quantity": 2, "unit_price": base_price, "status": "placed",   "event_ts": ts1})
        rows.append({"order_id": oid_history, "customer_id": customer_ref, "product_id": product_ref, "quantity": 2, "unit_price": base_price, "status": "paid",     "event_ts": ts2})
        rows.append({"order_id": oid_history, "customer_id": customer_ref, "product_id": product_ref, "quantity": 2, "unit_price": base_price, "status": "shipped",  "event_ts": ts3})

        # --- Exact duplicate (rejected as duplicate_event) ---
        oid_dup = _make_order_id(day_idx, 9002)
        ts_dup = _make_event_ts(partition_date, rng)
        dup_row = {
            "order_id": oid_dup,
            "customer_id": CUSTOMERS[2]["customer_id"],
            "product_id": PRODUCTS[2]["product_id"],
            "quantity": 3,
            "unit_price": 25.00,
            "status": "placed",
            "event_ts": ts_dup,
        }
        rows.append(dup_row)
        rows.append(dict(dup_row))  # exact copy — will be rejected
        row_labels.append({
            "order_id": oid_dup,
            "case": "duplicate_event",
            "duplicate_occurrence": 2,
            "expected_reject_reason": "duplicate_event",
            "note": "second occurrence of identical (order_id, event_ts, status)",
        })

        # --- Missing required field: null customer_id ---
        oid_null = _make_order_id(day_idx, 9003)
        rows.append({
            "order_id": oid_null,
            "customer_id": "",         # missing
            "product_id": PRODUCTS[3]["product_id"],
            "quantity": 1,
            "unit_price": 10.0,
            "status": "placed",
            "event_ts": _make_event_ts(partition_date, rng),
        })
        row_labels.append({
            "order_id": oid_null,
            "case": "missing_required_field",
            "expected_reject_reason": "missing_required_field:customer_id",
        })

        # --- Invalid quantity ---
        oid_qty = _make_order_id(day_idx, 9004)
        rows.append({
            "order_id": oid_qty,
            "customer_id": CUSTOMERS[4]["customer_id"],
            "product_id": PRODUCTS[4]["product_id"],
            "quantity": 0,             # invalid (must be >= 1)
            "unit_price": 5.0,
            "status": "placed",
            "event_ts": _make_event_ts(partition_date, rng),
        })
        row_labels.append({
            "order_id": oid_qty,
            "case": "invalid_quantity",
            "expected_reject_reason": "invalid_quantity",
        })

        # --- Invalid unit_price ---
        oid_price = _make_order_id(day_idx, 9005)
        rows.append({
            "order_id": oid_price,
            "customer_id": CUSTOMERS[5]["customer_id"],
            "product_id": PRODUCTS[5]["product_id"],
            "quantity": 1,
            "unit_price": -1.0,        # invalid (must be >= 0)
            "status": "placed",
            "event_ts": _make_event_ts(partition_date, rng),
        })
        row_labels.append({
            "order_id": oid_price,
            "case": "invalid_unit_price",
            "expected_reject_reason": "invalid_unit_price",
        })

        # --- Invalid status ---
        oid_status = _make_order_id(day_idx, 9006)
        rows.append({
            "order_id": oid_status,
            "customer_id": CUSTOMERS[6]["customer_id"],
            "product_id": PRODUCTS[6]["product_id"],
            "quantity": 1,
            "unit_price": 20.0,
            "status": "returned",      # invalid
            "event_ts": _make_event_ts(partition_date, rng),
        })
        row_labels.append({
            "order_id": oid_status,
            "case": "invalid_status",
            "expected_reject_reason": "invalid_status",
        })

        # --- Invalid event_ts ---
        oid_ts = _make_order_id(day_idx, 9007)
        rows.append({
            "order_id": oid_ts,
            "customer_id": CUSTOMERS[7]["customer_id"],
            "product_id": PRODUCTS[7]["product_id"],
            "quantity": 2,
            "unit_price": 15.0,
            "status": "placed",
            "event_ts": "not-a-timestamp",   # invalid
        })
        row_labels.append({
            "order_id": oid_ts,
            "case": "invalid_event_ts",
            "expected_reject_reason": "invalid_event_ts",
        })

        # --- Invalid reference: non-existent customer_id ---
        oid_cust = _make_order_id(day_idx, 9008)
        rows.append({
            "order_id": oid_cust,
            "customer_id": "C9999",    # does not exist
            "product_id": PRODUCTS[8]["product_id"],
            "quantity": 1,
            "unit_price": 30.0,
            "status": "placed",
            "event_ts": _make_event_ts(partition_date, rng),
        })
        row_labels.append({
            "order_id": oid_cust,
            "case": "invalid_reference_customer",
            "expected_reject_reason": "invalid_reference_customer",
        })

        # --- Invalid reference: non-existent product_id ---
        oid_prod = _make_order_id(day_idx, 9009)
        rows.append({
            "order_id": oid_prod,
            "customer_id": CUSTOMERS[9]["customer_id"],
            "product_id": "P9999",     # does not exist
            "quantity": 1,
            "unit_price": 45.0,
            "status": "placed",
            "event_ts": _make_event_ts(partition_date, rng),
        })
        row_labels.append({
            "order_id": oid_prod,
            "case": "invalid_reference_product",
            "expected_reject_reason": "invalid_reference_product",
        })

        # --- Late-arriving row (event_ts on previous day) ---
        oid_late = _make_order_id(day_idx, 9010)
        prev_day = partition_date - timedelta(days=1)
        rows.append({
            "order_id": oid_late,
            "customer_id": CUSTOMERS[10 % len(CUSTOMERS)]["customer_id"],
            "product_id": PRODUCTS[10 % len(PRODUCTS)]["product_id"],
            "quantity": 2,
            "unit_price": 12.50,
            "status": "placed",
            "event_ts": _make_event_ts(prev_day, rng),   # yesterday — late arriving
        })
        row_labels.append({
            "order_id": oid_late,
            "case": "late_arriving",
            "note": "event_ts is on the previous day; row is accepted but flagged as late_arriving",
        })

        # --- Malformed row (wrong column count) ---
        # Written as a raw list so it produces a bad row in CSV
        rows.append("MALFORMED_ROW_MARKER")
        row_labels.append({
            "case": "malformed_row",
            "expected_reject_reason": "malformed_row",
            "note": "row with only 3 columns instead of 7",
        })

        # Shuffle to simulate realistic ordering
        # Keep malformed marker at the end for simplicity
        normal_rows = [r for r in rows if r != "MALFORMED_ROW_MARKER"]
        rng.shuffle(normal_rows)
        rows_final = normal_rows + ["MALFORMED_ROW_MARKER"]

        _write_orders_partition(file_path, rows_final, day_idx)

        all_labels.extend([
            {**lbl, "partition_date": date_str, "file": f"orders_{date_str}.csv"}
            for lbl in row_labels
        ])

    # Write expected labels (SEPARATE from input data)
    expected_dir = output.parent / "expected" if output.name == "sample" else output / "expected"
    expected_dir.mkdir(parents=True, exist_ok=True)
    labels_path = expected_dir / "expected_sample_results.json"
    with open(labels_path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "description": "Expected outcomes for labelled test cases in the sample dataset. "
                               "Keep this file SEPARATE from input data.",
                "seed": seed,
                "start_date": start_date,
                "days": days,
                "rows_per_day": rows_per_day,
                "labels": all_labels,
            },
            fh,
            indent=2,
        )

    print(f"Generated dataset in: {output}")
    print(f"  Customers: {len(CUSTOMERS)} ({output / 'customers.csv'})")
    print(f"  Products:  {len(PRODUCTS)} ({output / 'products.csv'})")
    print(f"  Order files: {len(generated_files)} ({orders_dir})")
    print(f"  Expected labels: {labels_path}")
    return {"output_dir": str(output), "labels_path": str(labels_path)}


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Write a list of dicts to a CSV file."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_orders_partition(
    path: Path, rows: list, day_idx: int
) -> None:
    """Write an orders partition CSV, handling the malformed row marker."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(ORDERS_COLUMNS)
        for row in rows:
            if row == "MALFORMED_ROW_MARKER":
                # Write a row with only 3 columns to trigger malformed_row
                writer.writerow(["BAD_ROW", "missing_cols"])
            else:
                writer.writerow([row.get(col, "") for col in ORDERS_COLUMNS])


def _write_schema_change_partition(
    path: Path,
    partition_date: date,
    day_idx: int,
    rng: random.Random,
    rows_per_day: int,
) -> None:
    """Write a partition CSV missing the 'status' column (schema change simulation)."""
    bad_columns = [c for c in ORDERS_COLUMNS if c != "status"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(bad_columns)
        for row_idx in range(min(rows_per_day, 5)):
            oid = _make_order_id(day_idx, row_idx)
            customer = rng.choice(CUSTOMERS)
            product = rng.choice(PRODUCTS)
            writer.writerow([
                oid,
                customer["customer_id"],
                product["product_id"],
                rng.randint(1, 10),
                round(rng.uniform(1.0, 100.0), 2),
                _make_event_ts(partition_date, rng),
            ])


def main():
    parser = argparse.ArgumentParser(description="Generate reproducible e-commerce order datasets.")
    parser.add_argument("--days", type=int, default=6, help="Number of date partitions to generate")
    parser.add_argument("--rows-per-day", type=int, default=20, help="Approximate rows per partition")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--output-dir", default="data/sample", help="Output directory")
    parser.add_argument("--start-date", default="2025-01-01", help="Start date YYYY-MM-DD")
    args = parser.parse_args()

    generate_dataset(
        output_dir=args.output_dir,
        days=args.days,
        rows_per_day=args.rows_per_day,
        seed=args.seed,
        start_date=args.start_date,
    )


if __name__ == "__main__":
    main()
