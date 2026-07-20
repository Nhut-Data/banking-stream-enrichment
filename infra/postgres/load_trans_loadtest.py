#!/usr/bin/env python3
"""
infra/postgres/load_trans_loadtest.py
=======================================
Load thêm data/output/trans_loadtest.csv (~1,000,000 dòng synthetic, có
~2% trans_id trùng lặp do CORRUPTION_RATE_DUPLICATE) vào bảng `trans`.

Dùng staging table (không constraint) + INSERT ... ON CONFLICT DO NOTHING
để xử lý đúng phần duplicate injection có chủ đích — không phải bug.

CÁCH CHẠY:
  python3 infra/postgres/load_trans_loadtest.py
"""
import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONTAINER = os.environ.get("POSTGRES_CONTAINER", "banking-stream-enrichment-postgres-1")
DB_USER   = os.environ.get("POSTGRES_USER", "airflow")
DB_NAME   = os.environ.get("BERKA_DB", "berka")
OUTPUT_DIR = Path(os.environ.get("DATA_OUTPUT_DIR", "data/output"))
LOADTEST_CSV = OUTPUT_DIR / "trans_loadtest.csv"

COLUMNS = "trans_id, account_id, date, type, operation, amount, k_symbol, balance, bank, account"


def run(cmd, check=True):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        logger.error(f"Command failed: {cmd}")
        logger.error(f"stderr: {result.stderr}")
        sys.exit(1)
    return result


def psql(sql: str):
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB_NAME, "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"psql failed: {result.stderr}")
        sys.exit(1)
    return result


def main():
    if not LOADTEST_CSV.exists():
        logger.error(f"Không tìm thấy {LOADTEST_CSV}. Chạy `make generate` trước.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Load test — append trans_loadtest.csv (qua staging, dedup ON CONFLICT)")
    logger.info(f"  File: {LOADTEST_CSV} ({LOADTEST_CSV.stat().st_size / 1e6:.1f} MB)")
    logger.info("=" * 60)

    before = run(f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -t -c "SELECT COUNT(*) FROM trans;"')
    logger.info(f"  Trước khi load: {before.stdout.strip()} rows")

    logger.info("  Tạo staging table...")
    psql("DROP TABLE IF EXISTS trans_staging;")
    psql("CREATE TABLE trans_staging (LIKE trans INCLUDING DEFAULTS);")
    psql("ALTER TABLE trans_staging DROP CONSTRAINT IF EXISTS trans_staging_pkey;")

    logger.info("  Copying trans_loadtest.csv → container:/tmp/")
    run(f"docker cp {LOADTEST_CSV} {CONTAINER}:/tmp/trans_loadtest.csv")

    copy_sql = (
        f"COPY trans_staging ({COLUMNS}) "
        f"FROM '/tmp/trans_loadtest.csv' "
        f"WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"', NULL '');"
    )
    logger.info("  Loading vào staging...")
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB_NAME, "-c", copy_sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"COPY failed: {result.stderr}")
        sys.exit(1)
    logger.info(f"  {result.stdout.strip()}")

    staging_count = run(f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -t -c "SELECT COUNT(*) FROM trans_staging;"')
    logger.info(f"  Staging: {staging_count.stdout.strip()} rows (bao gồm cả duplicate)")

    logger.info("  Merging staging → trans (dedup ON CONFLICT)...")
    merge_result = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB_NAME, "-c",
         f"INSERT INTO trans ({COLUMNS}) SELECT {COLUMNS} FROM trans_staging "
         f"ON CONFLICT (trans_id) DO NOTHING;"],
        capture_output=True, text=True
    )
    if merge_result.returncode != 0:
        logger.error(f"Merge failed: {merge_result.stderr}")
        sys.exit(1)
    logger.info(f"  {merge_result.stdout.strip()}")

    psql("DROP TABLE trans_staging;")

    after = run(f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -t -c "SELECT COUNT(*) FROM trans;"')
    logger.info(f"  Sau khi load: {after.stdout.strip()} rows")
    logger.info("DONE — load test data đã append thành công (duplicate đã bị loại bỏ có chủ đích)")


if __name__ == "__main__":
    main()
