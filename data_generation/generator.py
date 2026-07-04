"""
data_generation/generator.py
==============================
Bước B — SAMPLE + ASSEMBLE: sinh giao dịch mới từ tham số đã học.

LUỒNG XỬ LÝ (joint sampling — không sample độc lập từng cột):
  1. Sample operation   → biến chính, quyết định tất cả còn lại
  2. Suy type           → từ TYPE_BY_OPERATION mapping cố định
  3. Sample k_symbol    → có điều kiện theo operation
  4. Sample amount      → log-normal có điều kiện theo operation
  5. Sample account_id  → weighted theo tần suất gốc
  6. Sinh date          → tiếp nối tuyến tính sau date_max của data gốc
  7. Gán trans_id       → tăng dần từ max_trans_id + 1, không trùng

THIẾT KẾ VECTORIZED:
  Group theo operation rồi xử lý từng nhóm một lần — tránh Python loop
  từng dòng (sẽ rất chậm với 100K-1M dòng).
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .profiler import FittedDistributions, MISSING_OPERATION

logger = logging.getLogger(__name__)

# Date ngay sau date_max của data gốc (981231 = 1998-12-31)
# Synthetic data bắt đầu từ 990101 = 1999-01-01
# Lý do giữ nguyên timeline gốc (không dịch sang hiện tại):
#   - account.date (ngày mở TK) vẫn là 1993-1998 trong Postgres
#   - Nếu dịch trans sang 2024+ sẽ tạo nghịch lý: giao dịch xảy ra
#     trước khi tài khoản được mở → vi phạm referential logic
#   - CDC event timestamp (lúc INSERT vào Postgres) vẫn là real-time dù
#     giá trị cột date bên trong là 1999 — Debezium không quan tâm
SYNTHETIC_START = pd.Timestamp("1999-01-01")
# Rải đều trong khoảng 6 năm — khớp với duration của data gốc (1993-1998)
SYNTHETIC_DURATION_DAYS = 365 * 6


def _date_to_berka_int(ts: pd.Timestamp) -> int:
    """Convert Timestamp → YYMMDD integer (Berka format)."""
    return int(ts.strftime("%y%m%d"))


def generate(
    fitted: FittedDistributions,
    n: int,
    start_trans_id: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Sinh n giao dịch mới theo phân phối đã học.

    Args:
        fitted:         FittedDistributions từ profiler.load_and_fit()
        n:              Số dòng cần sinh
        start_trans_id: trans_id đầu tiên — đảm bảo không trùng với data gốc
        rng:            numpy Generator với seed cố định (reproducibility)

    Returns:
        DataFrame với cùng schema như trans.csv gốc
    """
    logger.info(f"Sinh {n:,} giao dịch mới (start_trans_id={start_trans_id:,})...")

    # ------------------------------------------------------------------
    # 1. Sample operation — biến chính
    # ------------------------------------------------------------------
    op_keys = rng.choice(
        fitted.operation_keys,
        size=n,
        p=fitted.operation_probs,
    )

    # ------------------------------------------------------------------
    # 2. Sample account_id — weighted theo tần suất gốc
    # ------------------------------------------------------------------
    account_ids = rng.choice(
        fitted.account_ids,
        size=n,
        p=fitted.account_weights,
    )

    # ------------------------------------------------------------------
    # 3. Sinh date — tiếp nối tuyến tính sau data gốc
    # Rải ngẫu nhiên trong SYNTHETIC_DURATION_DAYS ngày
    # ------------------------------------------------------------------
    day_offsets = rng.integers(0, SYNTHETIC_DURATION_DAYS, size=n)
    timestamps = SYNTHETIC_START + pd.to_timedelta(day_offsets, unit="D")
    dates = np.array([_date_to_berka_int(ts) for ts in timestamps], dtype=int)

    # ------------------------------------------------------------------
    # 4. Sample type, k_symbol, amount — CÓ ĐIỀU KIỆN theo operation
    # Vectorized: group theo operation, xử lý mỗi nhóm 1 lần
    # ------------------------------------------------------------------
    types = np.empty(n, dtype=object)
    k_symbols = np.empty(n, dtype=object)
    amounts = np.empty(n, dtype=float)

    op_series = pd.Series(op_keys)

    for op_key in fitted.operation_keys:
        if op_key == MISSING_OPERATION:
            mask = op_series.isna().values | (op_series == MISSING_OPERATION).values
        else:
            mask = (op_series == op_key).values

        n_group = int(mask.sum())
        if n_group == 0:
            continue

        profile = fitted.operation_profiles[op_key]

        # type: suy ra từ mapping, không sample
        types[mask] = profile.type_

        # k_symbol: sample có điều kiện theo operation
        k_symbols[mask] = rng.choice(
            profile.k_symbol_values,
            size=n_group,
            p=profile.k_symbol_probs,
        )

        # amount: log-normal có điều kiện theo operation
        # Clip vào [amount_min, amount_max] quan sát được trong data thật
        # tránh log-normal sinh outlier phi thực tế ở đuôi dài
        raw_amounts = rng.lognormal(
            mean=profile.log_mean,
            sigma=profile.log_std,
            size=n_group,
        )
        amounts[mask] = np.clip(raw_amounts, profile.amount_min, profile.amount_max)

    # ------------------------------------------------------------------
    # 5. Xử lý operation column — đổi MISSING_OPERATION → NaN
    # để output khớp với format trans.csv gốc
    # ------------------------------------------------------------------
    operations = np.where(op_keys == MISSING_OPERATION, None, op_keys)

    # ------------------------------------------------------------------
    # 6. Assemble DataFrame
    # ------------------------------------------------------------------
    df = pd.DataFrame({
        "trans_id":   np.arange(start_trans_id, start_trans_id + n, dtype=int),
        "account_id": account_ids,
        "date":       dates,
        "type":       types,
        "operation":  operations,
        "amount":     np.round(amounts, 2),
        "k_symbol":   k_symbols,
        # balance: lũy kế theo account — cần xử lý stateful theo từng account,
        # nằm ngoài scope Bước 0. Set NaN, dbt staging sẽ handle sau.
        "balance":    np.nan,
        # bank/account: chỉ có giá trị khi chuyển khoản liên ngân hàng.
        # Không đủ thông tin để sinh realistic — set NaN nhất quán với data gốc.
        "bank":       None,
        "account":    None,
    })

    logger.info(
        f"Sinh xong {len(df):,} dòng | "
        f"trans_id: {df['trans_id'].min():,} → {df['trans_id'].max():,} | "
        f"date: {df['date'].min()} → {df['date'].max()}"
    )
    return df