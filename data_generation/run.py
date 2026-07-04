"""
data_generation/run.py
=======================
Entrypoint của Bước 0 — chạy toàn bộ pipeline data generation.

CÁCH CHẠY:
  python -m data_generation.run
  hoặc:
  make generate

LUỒNG XỬ LÝ:
  1. Load config từ .env
  2. FIT   — profiler.load_and_fit() học phân phối từ trans.csv gốc
  3. SAMPLE — generator.generate() sinh dev + loadtest
  4. CORRUPT — corruption.inject() tiêm 3 loại lỗi
  5. EXPORT — ghi CSV ra data/output/ + manifest ra docs/
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd

from . import profiler, generator, corruption, manifest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _get_env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def main() -> None:
    # ------------------------------------------------------------------
    # Config — đọc từ .env (đã load bởi docker-compose hoặc python-dotenv)
    # ------------------------------------------------------------------
    random_seed    = int(_get_env("RANDOM_SEED", "42"))
    dev_size       = int(_get_env("DEV_SIZE", "100000"))
    loadtest_size  = int(_get_env("LOADTEST_SIZE", "1000000"))

    rate_dup = float(_get_env("CORRUPTION_RATE_DUPLICATE",   "0.02"))
    rate_ref = float(_get_env("CORRUPTION_RATE_REFERENTIAL", "0.02"))
    rate_mis = float(_get_env("CORRUPTION_RATE_MISSING",     "0.03"))

    # Paths — resolve từ root repo (nơi chạy lệnh make generate)
    repo_root    = Path(_get_env("REPO_ROOT", "."))
    raw_dir      = repo_root / _get_env("DATA_RAW_DIR",    "data/raw")
    output_dir   = repo_root / _get_env("DATA_OUTPUT_DIR", "data/output")
    manifest_path = repo_root / _get_env("CORRUPTION_MANIFEST_PATH", "docs/corruption_manifest.json")

    trans_csv = raw_dir / "trans.csv"
    dev_csv   = output_dir / "trans_dev.csv"
    lt_csv    = output_dir / "trans_loadtest.csv"

    logger.info("=" * 60)
    logger.info("Banking Stream-Table Join — Data Generation")
    logger.info(f"  seed={random_seed}, dev={dev_size:,}, loadtest={loadtest_size:,}")
    logger.info(f"  corruption: dup={rate_dup}, ref={rate_ref}, missing={rate_mis}")
    logger.info("=" * 60)

    if not trans_csv.exists():
        raise FileNotFoundError(
            f"Không tìm thấy {trans_csv}. "
            "Đặt file trans.csv vào data/raw/ trước khi chạy."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Bước A — FIT
    # ------------------------------------------------------------------
    rng = np.random.default_rng(random_seed)
    fitted = profiler.load_and_fit(trans_csv)

    # ------------------------------------------------------------------
    # Bước B — SAMPLE (dev + loadtest, trans_id không trùng nhau)
    # ------------------------------------------------------------------
    dev_start_id = fitted.max_trans_id + 1
    lt_start_id  = dev_start_id + dev_size  # loadtest bắt đầu sau dev

    df_dev = generator.generate(fitted, dev_size, dev_start_id, rng)
    df_lt  = generator.generate(fitted, loadtest_size, lt_start_id, rng)

    # ------------------------------------------------------------------
    # Bước C — CORRUPT (độc lập cho mỗi tập)
    # ------------------------------------------------------------------
    df_dev_c, mf_dev = corruption.inject(
        df_dev, fitted.account_ids, rate_dup, rate_ref, rate_mis, rng
    )
    df_lt_c, mf_lt = corruption.inject(
        df_lt, fitted.account_ids, rate_dup, rate_ref, rate_mis, rng
    )

    # ------------------------------------------------------------------
    # Bước D — EXPORT
    # ------------------------------------------------------------------
    df_dev_c.to_csv(dev_csv, sep=";", index=False)
    logger.info(f"Đã xuất dev  → {dev_csv} ({len(df_dev_c):,} dòng)")

    df_lt_c.to_csv(lt_csv, sep=";", index=False)
    logger.info(f"Đã xuất load → {lt_csv} ({len(df_lt_c):,} dòng)")

    manifest.write(
        mf_dev, mf_lt,
        manifest_path,
        dev_size, loadtest_size,
        random_seed,
    )

    logger.info("=" * 60)
    logger.info("DONE. Kiểm tra kết quả:")
    logger.info(f"  {dev_csv}")
    logger.info(f"  {lt_csv}")
    logger.info(f"  {manifest_path}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()