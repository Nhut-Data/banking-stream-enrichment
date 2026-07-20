#!/usr/bin/env python3
"""
infra/postgres/load_trans_loadtest.py
=======================================
Load thêm data/output/trans_loadtest.csv (~1,000,000 dòng synthetic) vào
bảng `trans` đã có sẵn — dùng để load test throughput CDC + dbt ở quy mô
lớn hơn dataset Berka gốc.

Khác với load_berka.py: KHÔNG TRUNCATE, chỉ APPEND vào bảng trans.
Lưu ý: trans_loadtest.csv có THỨ TỰ CỘT KHÁC trans.csv gốc
(k_symbol và balance đổi chỗ) — đã map đúng theo thứ tự thật của file này.

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

# Thứ tự cột ĐÚNG theo file trans_loadtest.csv (k_symbol trước balance!)
COLUMNS = "trans_id, account_id, date, type, operation, amount, k_symbol, balance, bank, account"


def run(cmd, check=True):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        logger.error(f"Command failed: {cmd}")
        logger.error(f"stderr: {result.stderr}")
        sys.exit(1)
    return result


def main():
    if not LOADTEST_CSV.exists():
        logger.error(f"Không tìm thấy {LOADTEST_CSV}. Chạy `make generate` trước.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Load test — append trans_loadtest.csv vào bảng trans")
    logger.info(f"  File: {LOADTEST_CSV} ({LOADTEST_CSV.stat().st_size / 1e6:.1f} MB)")
    logger.info("=" * 60)

    before = run(f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -t -c "SELECT COUNT(*) FROM trans;"')
    logger.info(f"  Trước khi load: {before.stdout.strip()} rows")

    logger.info("  Copying trans_loadtest.csv → container:/tmp/")
    run(f"docker cp {LOADTEST_CSV} {CONTAINER}:/tmp/trans_loadtest.csv")

    copy_sql = (
        f"COPY trans ({COLUMNS}) "
        f"FROM '/tmp/trans_loadtest.csv' "
        f"WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"', NULL '');"
    )
    logger.info("  Loading (append, không truncate)...")
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "psql", "-U", DB_USER, "-d", DB_NAME, "-c", copy_sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"COPY failed: {result.stderr}")
        sys.exit(1)
    logger.info(f"  {result.stdout.strip()}")

    after = run(f'docker exec {CONTAINER} psql -U {DB_USER} -d {DB_NAME} -t -c "SELECT COUNT(*) FROM trans;"')
    logger.info(f"  Sau khi load: {after.stdout.strip()} rows")
    logger.info("DONE — load test data đã append thành công")


if __name__ == "__main__":
    main()
