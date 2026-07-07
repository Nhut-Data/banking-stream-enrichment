#!/usr/bin/env python3
"""
infra/postgres/load_berka.py
=============================
Load 5 bảng Berka CSV vào database 'berka' trong Postgres.

THỨ TỰ LOAD (FK dependency):
  1. district    — không phụ thuộc ai
  2. account     — FK → district
  3. client      — FK → district
  4. disposition — FK → client, account
  5. trans       — không có FK constraint (intentional — referential violation injection)

CÁCH CHẠY:
  python3 infra/postgres/load_berka.py
  hoặc:
  make load-data

LƯU Ý district.csv:
  File gốc Berka dùng tên cột A1-A16 (không có tên thật).
  Script này map sang tên cột đúng theo data dictionary Berka.
"""

import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "127.0.0.1"),
    "port":     int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname":   os.environ.get("BERKA_DB", "berka"),
    "user":     os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
}

RAW_DIR = Path(os.environ.get("DATA_RAW_DIR", "data/raw"))

# Batch size khi insert — tránh memory issue với bảng lớn (trans ~1M dòng)
BATCH_SIZE = 5000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_csv(filename: str, **kwargs) -> pd.DataFrame:
    path = RAW_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy {path}")
    df = pd.read_csv(path, sep=";", quotechar='"', low_memory=False, **kwargs)
    logger.info(f"  Đọc {filename}: {len(df):,} dòng")
    return df


def insert_batch(cur, table: str, columns: list[str], df: pd.DataFrame) -> int:
    """Insert DataFrame vào table theo batch, trả về số dòng đã insert."""
    # Chuyển NaN → None để psycopg2 insert đúng NULL
    records = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[columns].itertuples(index=False)
    ]

    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        execute_values(
            cur,
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING",
            batch,
        )
        total += len(batch)

    return total


# ---------------------------------------------------------------------------
# Load functions — 1 function cho mỗi bảng
# ---------------------------------------------------------------------------

def load_district(cur) -> None:
    logger.info("Loading district...")

    # district.csv dùng tên cột A1-A16 — map sang tên thật theo Berka data dictionary
    col_map = {
        "A1":  "district_id",
        "A2":  "name",
        "A3":  "region",
        "A4":  "num_inhabitants",
        "A5":  "num_municipalities_lt_499",
        "A6":  "num_municipalities_500_1999",
        "A7":  "num_municipalities_2000_9999",
        "A8":  "num_municipalities_gt_10000",
        "A9":  "num_cities",
        "A10": "ratio_urban",
        "A11": "avg_salary",
        "A12": "unemployment_rate_95",
        "A13": "unemployment_rate_96",
        "A14": "num_entrepreneurs_per_1000",
        "A15": "num_crimes_95",
        "A16": "num_crimes_96",
    }

    df = load_csv(
        "district.csv",
        header=0,
        names=list(col_map.keys()),  # đọc với header A1-A16
    )
    # Header thật đã là A1-A16 → skip row 0 nếu đó là header
    # Kiểm tra: nếu district_id (A1) không phải số thì row 0 là header cũ
    df = df.rename(columns=col_map)

    # Ép kiểu an toàn — một số cột có thể bị đọc là string
    df = df.replace("?", None)
    
    df["district_id"] = pd.to_numeric(df["district_id"], errors="coerce")
    df = df.dropna(subset=["district_id"])
    df["district_id"] = df["district_id"].astype(int)

    cols = list(col_map.values())
    n = insert_batch(cur, "district", cols, df)
    logger.info(f"  district: {n:,} dòng inserted")


def load_account(cur) -> None:
    logger.info("Loading account...")
    df = load_csv("account.csv")
    df.columns = [c.strip('"') for c in df.columns]

    cols = ["account_id", "district_id", "frequency", "date"]
    n = insert_batch(cur, "account", cols, df)
    logger.info(f"  account: {n:,} dòng inserted")


def load_client(cur) -> None:
    logger.info("Loading client...")
    df = load_csv("client.csv")
    df.columns = [c.strip('"') for c in df.columns]

    # birth_number đọc vào là string (vd: "706213") — strip quotes
    df["birth_number"] = df["birth_number"].astype(str).str.strip('"').astype(int)

    cols = ["client_id", "birth_number", "district_id"]
    n = insert_batch(cur, "client", cols, df)
    logger.info(f"  client: {n:,} dòng inserted")


def load_disposition(cur) -> None:
    logger.info("Loading disposition...")

    # File tên disp.csv → bảng tên disposition
    df = load_csv("disp.csv")
    df.columns = [c.strip('"') for c in df.columns]

    # Rename disp_id nếu cần (tên cột trong file là disp_id, bảng cũng là disp_id)
    cols = ["disp_id", "client_id", "account_id", "type"]
    n = insert_batch(cur, "disposition", cols, df)
    logger.info(f"  disposition: {n:,} dòng inserted")


def load_trans(cur, use_synthetic: bool = False) -> None:
    """
    Load trans data vào Postgres.

    Args:
        use_synthetic: True → load trans_dev.csv (synthetic, có lỗi tiêm)
                       False → load trans.csv gốc (Berka thật, sạch)
    """
    if use_synthetic:
        filename = "output/trans_dev.csv"
        logger.info("Loading trans (synthetic dev data với corruption)...")
    else:
        filename = "trans.csv"
        logger.info("Loading trans (Berka gốc)...")

    # Resolve path — synthetic nằm ở data/output/, gốc ở data/raw/
    if use_synthetic:
        path = Path(os.environ.get("DATA_OUTPUT_DIR", "data/output")) / "trans_dev.csv"
        df = pd.read_csv(path, sep=";", low_memory=False)
    else:
        df = load_csv("trans.csv")

    df.columns = [c.strip('"') for c in df.columns]

    # Chỉ insert các cột có trong schema — bỏ balance/bank/account nếu toàn NaN
    cols = ["trans_id", "account_id", "date", "type", "operation",
            "amount", "balance", "k_symbol", "bank", "account"]

    # Đảm bảo tất cả cột tồn tại (synthetic data có thể thiếu một số cột)
    for col in cols:
        if col not in df.columns:
            df[col] = None

    n = insert_batch(cur, "trans", cols, df)
    logger.info(f"  trans: {n:,} dòng inserted")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(load_synthetic_trans: bool = False) -> None:
    """
    Args:
        load_synthetic_trans: Nếu True, load trans_dev.csv thay vì trans.csv gốc.
                              Dùng khi muốn test pipeline với data có lỗi tiêm.
    """
    logger.info("=" * 60)
    logger.info("Load Berka data → Postgres")
    logger.info(f"  DB: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    logger.info(f"  Trans: {'synthetic (dev)' if load_synthetic_trans else 'Berka gốc'}")
    logger.info("=" * 60)

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Thứ tự load theo FK dependency
            load_district(cur)
            load_account(cur)
            load_client(cur)
            load_disposition(cur)
            load_trans(cur, use_synthetic=load_synthetic_trans)

        conn.commit()
        logger.info("=" * 60)
        logger.info("DONE — tất cả bảng đã load thành công")
        logger.info("Debezium sẽ tự động bắt WAL và đẩy sang Kafka topics")
        logger.info("Kiểm tra: http://localhost:8085 (kafka-ui)")
        logger.info("=" * 60)

    except Exception as e:
        conn.rollback()
        logger.error(f"Lỗi khi load data: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load Berka data vào Postgres")
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Load trans_dev.csv (synthetic, có lỗi tiêm) thay vì trans.csv gốc",
    )
    args = parser.parse_args()
    main(load_synthetic_trans=args.synthetic)