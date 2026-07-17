#!/usr/bin/env python3
"""
infra/postgres/load_berka.py
=============================
Load 5 bảng Berka CSV vào Postgres container qua docker cp + COPY command.

Lý do dùng docker cp thay vì psycopg2 trực tiếp:
  Trên WSL2, psycopg2 connect qua 'localhost' có thể hit Unix socket
  của Postgres native thay vì Docker container — dẫn đến data bị load
  vào sai instance (silent mismatch, không có error message).
  
  docker cp + COPY command chạy hoàn toàn bên trong container,
  bypass hoàn toàn vấn đề socket routing của WSL2.

CÁCH CHẠY:
  python3 infra/postgres/load_berka.py
  hoặc:
  make load-data
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONTAINER = os.environ.get("POSTGRES_CONTAINER", "banking-stream-enrichment-postgres-1")
DB_USER   = os.environ.get("POSTGRES_USER", "airflow")
DB_NAME   = os.environ.get("BERKA_DB", "berka")
RAW_DIR   = Path(os.environ.get("DATA_RAW_DIR", "data/raw"))

# Thứ tự load theo FK dependency: district → account → client → disposition → trans
TABLES = [
    {
        "name": "district",
        "file": "district.csv",
        "columns": None,  # load tất cả columns
        "null": "?",      # Berka dùng "?" cho missing values trong district
    },
    {
        "name": "account",
        "file": "account.csv",
        "columns": None,
        "null": "",
    },
    {
        "name": "client",
        "file": "client.csv",
        "columns": None,
        "null": "",
    },
    {
        "name": "disposition",
        "file": "disp.csv",
        "columns": "disp_id, client_id, account_id, type",
        "null": "",
    },
    {
        "name": "trans",
        "file": "trans.csv",
        "columns": "trans_id, account_id, date, type, operation, amount, balance, k_symbol, bank, account",
        "null": "",
    },
]


def run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Chạy shell command và log output."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        logger.error(f"Command failed: {cmd}")
        logger.error(f"stderr: {result.stderr}")
        sys.exit(1)
    return result


def load_table(table: dict) -> None:
    name    = table["name"]
    file    = RAW_DIR / table["file"]
    columns = table["columns"]
    null    = table["null"]

    if not file.exists():
        logger.error(f"File không tìm thấy: {file}")
        sys.exit(1)

    # 1. Copy CSV vào container
    logger.info(f"  Copying {file.name} → container:/tmp/")
    run(f"docker cp {file} {CONTAINER}:/tmp/{file.name}")

    # 2. Build COPY command
    col_clause = f"({columns})" if columns else ""
    copy_sql = (
        f"COPY {name} {col_clause} "
        f"FROM '/tmp/{file.name}' "
        f"WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"', NULL '{null}');"
    )

    # 3. Chạy COPY bên trong container
    # 3. Chạy COPY bên trong container — dùng list args tránh shell escaping
    logger.info(f"  Loading {name}...")
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB_NAME, "-c", copy_sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"COPY failed: {result.stderr}")
        sys.exit(1)
    logger.info(f"  {result.stdout.strip()}")

    # 4. Verify count
    count_result = run(
        f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -t -c "SELECT COUNT(*) FROM {name};"'
    )
    count = count_result.stdout.strip()
    logger.info(f"  → {name}: {count} rows")


def main() -> None:
    logger.info("=" * 60)
    logger.info("Load Berka data → Postgres (via docker cp + COPY)")
    logger.info(f"  Container: {CONTAINER}")
    logger.info(f"  Database:  {DB_NAME}")
    logger.info(f"  Raw dir:   {RAW_DIR}")
    logger.info("=" * 60)

    # Truncate tất cả bảng theo thứ tự FK ngược (con trước, cha sau)
    logger.info("Truncating existing data...")
    run(
        f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} '
        f'-c "TRUNCATE trans, disposition, client, account, district CASCADE;"'
    )

    # Load từng bảng theo thứ tự FK
    for table in TABLES:
        load_table(table)

    # Grant permissions cho Debezium
    logger.info("Granting permissions to replicator...")
    run(
        f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -c "'
        f"GRANT CREATE ON DATABASE {DB_NAME} TO replicator; "
        f"ALTER TABLE district OWNER TO replicator; "
        f"ALTER TABLE account OWNER TO replicator; "
        f"ALTER TABLE client OWNER TO replicator; "
        f"ALTER TABLE disposition OWNER TO replicator; "
        f"ALTER TABLE trans OWNER TO replicator;"
        f'"'
    )

    logger.info("=" * 60)
    logger.info("DONE — tất cả bảng đã load thành công")
    logger.info("Tiếp theo: make register-connector")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
