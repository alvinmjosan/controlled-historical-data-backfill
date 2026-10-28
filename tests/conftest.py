"""
Shared pytest fixtures for the backfill test suite.
"""
from __future__ import annotations

import csv
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backfill.db import create_schema, get_connection
from backfill.load import load_customers, load_products

# ---------------------------------------------------------------------------
# Standard reference data used across tests
# ---------------------------------------------------------------------------

SAMPLE_CUSTOMERS = [
    {"customer_id": "C0001", "customer_name": "Alice Johnson", "country": "US"},
    {"customer_id": "C0002", "customer_name": "Bob Smith",     "country": "UK"},
    {"customer_id": "C0003", "customer_name": "Carol White",   "country": "DE"},
]

SAMPLE_PRODUCTS = [
    {"product_id": "P0001", "product_name": "Widget A", "category": "Electronics"},
    {"product_id": "P0002", "product_name": "Widget B", "category": "Electronics"},
    {"product_id": "P0003", "product_name": "Gadget Pro", "category": "Electronics"},
]

VALID_CUSTOMER_IDS = {c["customer_id"] for c in SAMPLE_CUSTOMERS}
VALID_PRODUCT_IDS  = {p["product_id"] for p in SAMPLE_PRODUCTS}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_conn(tmp_path):
    """Return a fresh in-memory-equivalent SQLite connection with full schema."""
    db_path = tmp_path / "test_warehouse.db"
    conn = get_connection(db_path)
    create_schema(conn)
    load_customers(conn, SAMPLE_CUSTOMERS)
    load_products(conn, SAMPLE_PRODUCTS)
    yield conn
    conn.close()


@pytest.fixture
def db_path(tmp_path):
    """Return a fresh DB path (file not yet created)."""
    path = tmp_path / "test_warehouse.db"
    conn = get_connection(path)
    create_schema(conn)
    load_customers(conn, SAMPLE_CUSTOMERS)
    load_products(conn, SAMPLE_PRODUCTS)
    conn.close()
    return path


@pytest.fixture
def source_dir(tmp_path):
    """Return a temporary source directory with customers.csv and products.csv."""
    src = tmp_path / "source"
    src.mkdir()
    orders_dir = src / "orders"
    orders_dir.mkdir()

    _write_csv(src / "customers.csv", SAMPLE_CUSTOMERS, ["customer_id", "customer_name", "country"])
    _write_csv(src / "products.csv", SAMPLE_PRODUCTS, ["product_id", "product_name", "category"])
    return src


@pytest.fixture
def run_id():
    return "run_20250101T120000Z"


@pytest.fixture
def now_ts():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ORDERS_COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "status", "event_ts"]


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_orders_csv(path: Path, rows: list[dict]) -> None:
    """Write an orders CSV with the standard header."""
    _write_csv(path, rows, ORDERS_COLUMNS)


def make_valid_row(
    order_id: str = "ORD-001",
    customer_id: str = "C0001",
    product_id: str = "P0001",
    quantity: int = 2,
    unit_price: float = 10.0,
    status: str = "placed",
    event_ts: str = "2025-01-01T10:00:00+00:00",
) -> dict:
    """Return a dict that passes all validation rules."""
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "status": status,
        "event_ts": event_ts,
    }
