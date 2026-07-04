"""
data_generation/corruption.py
===============================
Bước C — CORRUPT: tiêm 3 loại lỗi có chủ đích vào synthetic data.

3 LOẠI LỖI (đã chốt trong context handoff, Mục 4):
  1. Duplicate          — mô phỏng Kafka producer retry khi mất kết nối tạm thời
  2. Referential        — account_id không tồn tại trong bảng account
                          (mô phỏng speed/batch layer chạy không đồng bộ)
  3. Missing value      — null ở field không bắt buộc (k_symbol)

CÁC LỖI ĐỘC LẬP VỚI NHAU:
  1 dòng có thể dính nhiều loại lỗi cùng lúc — đúng thực tế,
  các lỗi này không loại trừ lẫn nhau trong production system.

OUTPUT:
  (df_corrupted, manifest_dict) — manifest dùng để đối chiếu với
  dbt test và Speed Layer DLQ sau này (bắt đúng số lỗi đã tiêm không?)
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def inject(
    df: pd.DataFrame,
    real_account_ids: np.ndarray,
    rate_duplicate: float,
    rate_referential: float,
    rate_missing: float,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, dict]:
    """
    Tiêm lỗi vào DataFrame và trả về manifest ghi chi tiết những gì đã tiêm.

    Args:
        df:                 DataFrame synthetic data sạch (output của generator)
        real_account_ids:   Mảng account_id thật từ Berka (để tạo account_id giả chắc chắn không trùng)
        rate_duplicate:     Tỷ lệ dòng bị duplicate (vd: 0.02 = 2%)
        rate_referential:   Tỷ lệ dòng bị referential violation
        rate_missing:       Tỷ lệ dòng bị missing value
        rng:                numpy Generator (reproducible)

    Returns:
        (df_corrupted, manifest) — manifest là dict log chi tiết để export JSON
    """
    df = df.copy()
    n = len(df)
    manifest: dict = {
        "total_rows_before_corruption": n,
        "corruptions": {},
    }

    # ------------------------------------------------------------------
    # Lỗi 1: Duplicate
    # Mô phỏng: Kafka producer retry khi broker timeout tạm thời →
    # cùng 1 message được publish 2 lần → consumer nhận 2 bản y hệt.
    # Cách tiêm: chọn ngẫu nhiên n_dup dòng, append bản sao y hệt.
    # Pipeline phải detect bằng idempotency key (trans_id).
    # dbt test: unique(trans_id) sẽ bắt được lỗi này.
    # ------------------------------------------------------------------
    n_dup = max(1, int(n * rate_duplicate))
    dup_idx = rng.choice(df.index, size=n_dup, replace=False)
    dup_rows = df.loc[dup_idx].copy()

    manifest["corruptions"]["duplicate"] = {
        "rate": rate_duplicate,
        "count": n_dup,
        "trans_ids": dup_rows["trans_id"].tolist(),
        "description": "Kafka producer retry — same trans_id inserted twice",
    }

    df = pd.concat([df, dup_rows], ignore_index=True)
    logger.info(f"[corrupt] Duplicate: +{n_dup:,} dòng → tổng {len(df):,}")

    # ------------------------------------------------------------------
    # Lỗi 2: Referential integrity violation
    # Mô phỏng: transaction đến speed layer trước khi account record
    # được sync về (batch/speed layer không đồng bộ thời gian thực).
    # Cách tiêm: thay account_id thật bằng account_id giả chắc chắn
    # không tồn tại trong bảng account.
    # Sinh account_id giả = max_real + offset lớn để tránh collision.
    # Speed Layer DLQ và dbt test relationships() sẽ bắt được.
    # ------------------------------------------------------------------
    n_ref = max(1, int(n * rate_referential))
    ref_idx = rng.choice(df.index, size=n_ref, replace=False)

    fake_base = int(real_account_ids.max()) + 100_000
    fake_ids = rng.integers(fake_base, fake_base + n_ref * 10, size=n_ref)
    # Đảm bảo fake_ids unique với nhau
    fake_ids = np.unique(fake_ids)[:n_ref]
    if len(fake_ids) < n_ref:
        # Fallback nếu unique() trả về ít hơn mong đợi
        fake_ids = np.arange(fake_base, fake_base + n_ref)

    original_account_ids = df.loc[ref_idx, "account_id"].tolist()
    df.loc[ref_idx, "account_id"] = fake_ids

    manifest["corruptions"]["referential_violation"] = {
        "rate": rate_referential,
        "count": n_ref,
        "trans_ids": df.loc[ref_idx, "trans_id"].tolist(),
        "original_account_ids": original_account_ids,
        "fake_account_ids": fake_ids.tolist(),
        "description": "account_id không tồn tại trong bảng account — speed/batch async",
    }
    logger.info(f"[corrupt] Referential: {n_ref:,} dòng account_id thay bằng id giả")

    # ------------------------------------------------------------------
    # Lỗi 3: Missing value
    # Mô phỏng: field không bắt buộc bị thiếu do upstream system
    # không populate đủ (vd: legacy transaction không có purpose code).
    # Cách tiêm: set k_symbol = NaN trên n_missing dòng ngẫu nhiên.
    # Chọn k_symbol vì: đây là field nullable trong schema, và đã có
    # sẵn NaN tự nhiên trong data gốc — tiêm thêm là hợp lý.
    # Không tiêm null vào: trans_id, account_id, amount — các field
    # bắt buộc, nếu null sẽ phá vỡ tính dùng được của bản ghi.
    # dbt test: not_null(k_symbol) KHÔNG nên dùng vì field này nullable.
    # Kiểm tra qua data quality metric (% null so với baseline).
    # ------------------------------------------------------------------
    n_missing = max(1, int(n * rate_missing))
    missing_idx = rng.choice(df.index, size=n_missing, replace=False)
    original_k_symbols = df.loc[missing_idx, "k_symbol"].tolist()
    df.loc[missing_idx, "k_symbol"] = np.nan

    manifest["corruptions"]["missing_value"] = {
        "rate": rate_missing,
        "count": n_missing,
        "trans_ids": df.loc[missing_idx, "trans_id"].tolist(),
        "field": "k_symbol",
        "original_values_sample": original_k_symbols[:20],  # chỉ log 20 dòng đầu tránh manifest quá lớn
        "description": "k_symbol = NULL — legacy transaction không có purpose code",
    }
    logger.info(f"[corrupt] Missing: {n_missing:,} dòng k_symbol → NaN")

    manifest["total_rows_after_corruption"] = len(df)
    manifest["summary"] = {
        "duplicate_count": n_dup,
        "referential_violation_count": n_ref,
        "missing_value_count": n_missing,
    }

    return df, manifest