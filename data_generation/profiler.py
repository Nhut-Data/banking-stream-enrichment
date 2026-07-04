"""
data_generation/profiler.py
============================
Bước A — FIT: học phân phối thống kê từ dữ liệu Berka thật.

OUTPUT: FittedDistributions dataclass — tập hợp tham số phân phối đã học,
        truyền sang generator.py để sample dữ liệu mới.

NGUYÊN TẮC THIẾT KẾ:
  - operation là biến chính (categorical, sample trước)
  - type      suy ra từ operation qua mapping cố định (không sample độc lập)
  - k_symbol  sample CÓ ĐIỀU KIỆN theo operation (conditional distribution)
  - amount    fit log-normal CÓ ĐIỀU KIỆN theo operation (conditional distribution)

  Lý do: type/operation/k_symbol/amount KHÔNG độc lập với nhau trong dữ liệu thật.
  Nếu sample độc lập từng cột sẽ tạo ra kết hợp vô lý
  (vd: type=PRIJEM nhưng operation=VYBER KARTOU).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Sentinel thay cho NaN/None khi dùng làm dict key.
# NaN không thể làm dict key tin cậy vì NaN != NaN trong Python.
# Pandas đôi khi tự convert None -> NaN khi đi qua .unique()/.where(),
# dùng sentinel string loại bỏ hoàn toàn sự mơ hồ này.
MISSING_OPERATION = "__MISSING__"

# Mapping operation -> type: quan hệ 1-1 CỐ ĐỊNH trong dữ liệu thật.
# Rút ra từ pd.crosstab(df['type'], df['operation']) trên trans.csv.
# Không phải giả định — là fact từ dữ liệu gốc.
TYPE_BY_OPERATION: dict[str, str] = {
    "VKLAD":           "PRIJEM",   # gửi tiền mặt → thu vào
    "PREVOD Z UCTU":   "PRIJEM",   # nhận chuyển khoản → thu vào
    "PREVOD NA UCET":  "VYDAJ",    # chuyển khoản đi → chi ra
    "VYBER KARTOU":    "VYDAJ",    # rút tiền qua thẻ → chi ra
    "VYBER":           "VYDAJ",    # rút tiền mặt → chi ra
    MISSING_OPERATION: "VYBER",    # NaN operation = legacy VYBER code
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class OperationProfile:
    """
    Tham số phân phối học được cho từng operation riêng biệt.
    Đây là kết quả của conditional fitting:
      - amount   | operation → log-normal params
      - k_symbol | operation → categorical distribution
    """
    operation: str                        # key (dùng MISSING_OPERATION thay NaN)
    probability: float                    # P(operation) trong toàn dataset
    type_: str                            # suy ra từ TYPE_BY_OPERATION
    log_mean: float                       # mean(log(amount)) cho nhóm này
    log_std: float                        # std(log(amount)) cho nhóm này
    amount_min: float                     # clip lower bound — tránh outlier âm
    amount_max: float                     # clip upper bound — tránh outlier quá lớn
    k_symbol_values: list = field(default_factory=list)   # các giá trị k_symbol có thể xuất hiện
    k_symbol_probs: list = field(default_factory=list)    # xác suất tương ứng


@dataclass
class FittedDistributions:
    """
    Toàn bộ tham số phân phối đã học từ dữ liệu thật.
    Đây là "model" của data generator — truyền sang generator.py để sample.
    """
    operation_profiles: dict[str, OperationProfile]  # operation_key -> profile
    operation_keys: list[str]                         # thứ tự cố định để sample
    operation_probs: list[float]                      # P(operation) tương ứng
    account_ids: np.ndarray                           # 4500 account_id thật từ Berka
    account_weights: np.ndarray                       # trọng số theo tần suất gốc
    max_trans_id: int                                 # để generator tạo trans_id mới không trùng
    date_min: int                                     # YYMMDD — ngày đầu tiên trong data gốc
    date_max: int                                     # YYMMDD — ngày cuối cùng trong data gốc


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def load_and_fit(csv_path: Path) -> FittedDistributions:
    """
    Đọc trans.csv thật và học toàn bộ phân phối cần thiết.

    Args:
        csv_path: Path đến file trans.csv gốc của Berka

    Returns:
        FittedDistributions chứa tất cả tham số đã học
    """
    logger.info(f"Đọc dữ liệu gốc từ {csv_path}")
    df = pd.read_csv(
        csv_path,
        sep=";",
        quotechar='"',
        encoding="utf-8",
        low_memory=False,
    )
    logger.info(f"Đã đọc {len(df):,} dòng, {df.shape[1]} cột")

    # amount = 0 không hợp lệ cho log-normal (log(0) = -inf).
    # Chỉ có 14/1,056,320 dòng — loại khỏi tập fit, không ảnh hưởng phân phối.
    n_before = len(df)
    df_fit = df[df["amount"] > 0].copy()
    n_dropped = n_before - len(df_fit)
    if n_dropped:
        logger.info(f"Loại {n_dropped} dòng amount <= 0 khỏi tập fit")

    # Chuẩn hóa NaN operation → MISSING_OPERATION sentinel
    df_fit["op_key"] = df_fit["operation"].where(
        df_fit["operation"].notna(), MISSING_OPERATION
    )

    # Tính tần suất từng operation để lấy P(operation)
    op_counts = df_fit["op_key"].value_counts()
    total = len(df_fit)

    operation_keys = op_counts.index.tolist()
    operation_probs = (op_counts / total).tolist()

    # Fit từng operation profile
    profiles: dict[str, OperationProfile] = {}
    for op_key in operation_keys:
        group = df_fit[df_fit["op_key"] == op_key]
        log_amounts = np.log(group["amount"].values)

        # k_symbol | operation:
        # Phân biệt NaN (thực sự thiếu) với "" (chuỗi rỗng — category hợp lệ riêng).
        # Cả 2 đều là giá trị hợp lệ, không được gộp làm 1.
        ks_counts = group["k_symbol"].value_counts(dropna=False)
        ks_values = ks_counts.index.tolist()
        ks_probs = (ks_counts / ks_counts.sum()).tolist()

        profiles[op_key] = OperationProfile(
            operation=op_key,
            probability=op_counts[op_key] / total,
            type_=TYPE_BY_OPERATION[op_key],
            log_mean=float(log_amounts.mean()),
            log_std=float(log_amounts.std()),
            amount_min=float(group["amount"].min()),
            amount_max=float(group["amount"].max()),
            k_symbol_values=ks_values,
            k_symbol_probs=ks_probs,
        )
        logger.info(
            f"  op={op_key!r:20s} n={len(group):>8,}  "
            f"type={profiles[op_key].type_:6s}  "
            f"amount=[{profiles[op_key].amount_min:.0f}, {profiles[op_key].amount_max:.0f}]  "
            f"log_mean={profiles[op_key].log_mean:.3f}  log_std={profiles[op_key].log_std:.3f}"
        )

    # account_id weighted theo tần suất giao dịch gốc.
    # Lý do: account active nhiều trong data gốc → tiếp tục active nhiều
    # trong data sinh thêm. Phản ánh đúng thực tế hành vi khách hàng.
    acc_counts = df["account_id"].value_counts()
    account_ids = acc_counts.index.values.astype(int)
    account_weights = (acc_counts / acc_counts.sum()).values

    fitted = FittedDistributions(
        operation_profiles=profiles,
        operation_keys=operation_keys,
        operation_probs=operation_probs,
        account_ids=account_ids,
        account_weights=account_weights,
        max_trans_id=int(df["trans_id"].max()),
        date_min=int(df["date"].min()),
        date_max=int(df["date"].max()),
    )

    logger.info(
        f"Fit hoàn tất: {len(profiles)} operations, "
        f"{len(account_ids):,} accounts, "
        f"max_trans_id={fitted.max_trans_id:,}, "
        f"date range={fitted.date_min}→{fitted.date_max}"
    )
    return fitted