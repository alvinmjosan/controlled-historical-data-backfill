"""
Configuration: paths, defaults, and constants for the backfill pipeline.

All configurable values live here so callers never use hard-coded paths or magic numbers.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root (two levels up from this file: backfill/config.py -> project/)
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Default paths (overridable via CLI)
# ---------------------------------------------------------------------------
DEFAULT_SOURCE_DIR: Path = PROJECT_ROOT / "data" / "source"
DEFAULT_DB_PATH: Path = PROJECT_ROOT / "output" / "warehouse.db"
DEFAULT_OUTPUT_DIR: Path = PROJECT_ROOT / "output"

# Reference data filenames
CUSTOMERS_FILE: str = "customers.csv"
PRODUCTS_FILE: str = "products.csv"

# Orders partition file pattern  (e.g. orders_2025-01-01.csv)
ORDERS_FILE_PATTERN: str = "orders_{date}.csv"
ORDERS_SUBDIR: str = os.path.join("orders")

# ---------------------------------------------------------------------------
# Pipeline defaults
# ---------------------------------------------------------------------------
DEFAULT_MAX_WORKERS: int = 4
DEFAULT_MAX_RETRIES: int = 2
DEFAULT_ROWS_PER_SECOND: float = 5_000.0   # used for dry-run time estimate

# ---------------------------------------------------------------------------
# Schema: expected columns in each source file
# ---------------------------------------------------------------------------
ORDERS_REQUIRED_COLUMNS: list[str] = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "status",
    "event_ts",
]

CUSTOMERS_REQUIRED_COLUMNS: list[str] = ["customer_id", "customer_name", "country"]
PRODUCTS_REQUIRED_COLUMNS: list[str] = ["product_id", "product_name", "category"]

# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------
VALID_STATUSES: frozenset[str] = frozenset(
    {"placed", "paid", "shipped", "delivered", "cancelled"}
)

# ---------------------------------------------------------------------------
# Partition statuses (used in backfill_partitions table)
# ---------------------------------------------------------------------------
STATUS_PENDING: str = "PENDING"
STATUS_RUNNING: str = "RUNNING"
STATUS_SUCCESS: str = "SUCCESS"
STATUS_FAILED: str = "FAILED"
STATUS_SKIPPED: str = "SKIPPED"

# Stale running threshold (legacy fallback for multi-process safety).
# Primary stale detection: at startup, any RUNNING row is reset to FAILED
# (only one process runs against the DB at a time, so RUNNING == crashed).
# This threshold is kept as a belt-and-suspenders guard for edge cases.
STALE_RUNNING_SECONDS: int = 3600

# ---------------------------------------------------------------------------
# Inject failure env var (for testing partial failure / retry)
# ---------------------------------------------------------------------------
INJECT_FAILURE_ENV_VAR: str = "INJECT_FAILURE_DATE"

# ---------------------------------------------------------------------------
# Run ID prefix
# ---------------------------------------------------------------------------
RUN_ID_PREFIX: str = "run_"
