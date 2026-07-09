"""
speed_layer/enrichment.py
==========================
Hàm lookup + join: transaction → enriched transaction.

Input:  raw transaction message từ cdc.public.trans
Output: enriched transaction (transaction + customer profile)
        hoặc None nếu lookup thất bại (→ DLQ)
"""

from __future__ import annotations

import logging
from typing import Optional

from .state_manager import StateManager

logger = logging.getLogger(__name__)


def enrich(
    transaction: dict,
    state: StateManager,
) -> tuple[Optional[dict], Optional[dict]]:
    """
    Stream-Table Join: lookup profile cho transaction.

    Args:
        transaction: raw CDC message từ cdc.public.trans
        state:       StateManager chứa profile tables trong memory

    Returns:
        (enriched_record, None)  nếu lookup thành công
        (None, dlq_record)       nếu lookup thất bại (referential violation)
    """
    account_id = transaction.get("account_id")

    if account_id is None:
        logger.warning(f"Transaction thiếu account_id: {transaction.get('trans_id')}")
        return None, _make_dlq_record(transaction, "missing_account_id")

    profile = state.lookup(account_id)

    if profile is None:
        # Referential integrity violation:
        # account_id không tồn tại trong State Table
        # Có 2 nguyên nhân có thể:
        #   1. Data corruption đã tiêm có chủ đích (xem corruption_manifest.json)
        #   2. Profile chưa kịp load vào State Table (eventual consistency)
        # Cả 2 đều được xử lý giống nhau: đẩy sang DLQ để xử lý sau
        logger.debug(f"Referential violation: account_id={account_id} không tìm thấy trong State Table")
        return None, _make_dlq_record(transaction, "referential_violation")

    enriched = {
        # --- Transaction fields ---
        "trans_id":        transaction.get("trans_id"),
        "account_id":      account_id,
        "date":            transaction.get("date"),
        "type":            transaction.get("type"),
        "operation":       transaction.get("operation"),
        "amount":          transaction.get("amount"),
        "balance":         transaction.get("balance"),
        "k_symbol":        transaction.get("k_symbol"),

        # --- Customer profile (từ State Table) ---
        "client_id":       profile.get("client_id"),
        "birth_number":    profile.get("birth_number"),
        "account_frequency": profile.get("account_frequency"),

        # --- Geographic context ---
        "district_name":   profile.get("district_name"),
        "district_region": profile.get("district_region"),
        "avg_salary":      profile.get("avg_salary"),

        # --- CDC metadata ---
        "__op":            transaction.get("__op"),
        "__lsn":           transaction.get("__lsn"),
        "__source_ts_ms":  transaction.get("__source_ts_ms"),
        "__enriched":      True,
    }

    return enriched, None


def _make_dlq_record(transaction: dict, reason: str) -> dict:
    """Tạo DLQ record với thông tin debug đầy đủ."""
    return {
        "original_transaction": transaction,
        "dlq_reason":           reason,
        "trans_id":             transaction.get("trans_id"),
        "account_id":           transaction.get("account_id"),
        "__source_ts_ms":       transaction.get("__source_ts_ms"),
    }