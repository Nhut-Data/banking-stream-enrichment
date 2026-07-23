"""
Unit tests cho data_generation/generator.py — hàm generate().

Chạy: pytest tests/unit/test_generator.py -v
"""
import numpy as np
import pandas as pd
import pytest

from data_generation.generator import generate, SYNTHETIC_START, SYNTHETIC_DURATION_DAYS
from data_generation.profiler import FittedDistributions, OperationProfile, MISSING_OPERATION


def _make_operation_profile(op_key, type_, log_mean=5.0, log_std=1.0,
                             amount_min=10.0, amount_max=5000.0,
                             k_symbol_values=None, k_symbol_probs=None):
    return OperationProfile(
        operation=op_key,
        probability=0.5,
        type_=type_,
        log_mean=log_mean,
        log_std=log_std,
        amount_min=amount_min,
        amount_max=amount_max,
        k_symbol_values=k_symbol_values or ["SIPO", ""],
        k_symbol_probs=k_symbol_probs or [0.5, 0.5],
    )


@pytest.fixture
def fitted():
    """FittedDistributions giả lập — 2 operation thật + 1 MISSING, đúng cấu trúc profiler.py."""
    profiles = {
        "VKLAD": _make_operation_profile("VKLAD", "PRIJEM", amount_min=100, amount_max=2000),
        "VYBER": _make_operation_profile("VYBER", "VYDAJ", amount_min=50, amount_max=1500),
        MISSING_OPERATION: _make_operation_profile(MISSING_OPERATION, "VYBER", amount_min=20, amount_max=500),
    }
    return FittedDistributions(
        operation_profiles=profiles,
        operation_keys=["VKLAD", "VYBER", MISSING_OPERATION],
        operation_probs=[0.4, 0.4, 0.2],
        account_ids=np.array([100, 200, 300]),
        account_weights=np.array([0.5, 0.3, 0.2]),
        max_trans_id=5000,
        date_min=930101,
        date_max=981231,
    )


@pytest.fixture
def rng():
    return np.random.default_rng(42)


class TestGeneratorSchema:
    def test_output_has_expected_columns(self, fitted, rng):
        df = generate(fitted, n=100, start_trans_id=5001, rng=rng)

        expected_columns = {
            "trans_id", "account_id", "date", "type", "operation",
            "amount", "k_symbol", "balance", "bank", "account",
        }
        assert set(df.columns) == expected_columns

    def test_output_row_count_matches_n(self, fitted, rng):
        df = generate(fitted, n=250, start_trans_id=1, rng=rng)
        assert len(df) == 250

    def test_balance_bank_account_are_null_by_design(self, fitted, rng):
        """balance yêu cầu tính lũy kế stateful — ngoài scope Bước 0, luôn NaN/None."""
        df = generate(fitted, n=100, start_trans_id=1, rng=rng)

        assert df["balance"].isna().all()
        assert df["bank"].isna().all()
        assert df["account"].isna().all()


class TestGeneratorTransId:
    def test_trans_id_sequential_from_start(self, fitted, rng):
        df = generate(fitted, n=100, start_trans_id=5001, rng=rng)

        assert df["trans_id"].min() == 5001
        assert df["trans_id"].max() == 5100

    def test_trans_id_has_no_duplicates(self, fitted, rng):
        df = generate(fitted, n=500, start_trans_id=1, rng=rng)
        assert df["trans_id"].is_unique

    def test_trans_id_never_overlaps_with_source_data(self, fitted, rng):
        """Đảm bảo generator không bao giờ sinh trans_id đụng với data gốc (start > max_trans_id)."""
        start = fitted.max_trans_id + 1
        df = generate(fitted, n=100, start_trans_id=start, rng=rng)

        assert df["trans_id"].min() > fitted.max_trans_id


class TestGeneratorTypeOperationConsistency:
    """
    Đây là test quan trọng nhất của module này: type KHÔNG được sample độc lập,
    mà PHẢI khớp 1-1 với operation qua profile.type_ — nếu code sau này bị sửa
    thành sample type độc lập, test này phải fail ngay.
    """

    def test_type_always_matches_operation_profile(self, fitted, rng):
        df = generate(fitted, n=2000, start_trans_id=1, rng=rng)

        for op_key, profile in fitted.operation_profiles.items():
            if op_key == MISSING_OPERATION:
                mask = df["operation"].isna()
            else:
                mask = df["operation"] == op_key

            if mask.sum() == 0:
                continue

            assert (df.loc[mask, "type"] == profile.type_).all(), (
                f"operation={op_key!r} có type không khớp profile.type_={profile.type_!r}"
            )

    def test_missing_operation_becomes_none_in_output(self, fitted, rng):
        """MISSING_OPERATION sentinel không được rò rỉ ra output — phải convert về None."""
        df = generate(fitted, n=2000, start_trans_id=1, rng=rng)

        assert MISSING_OPERATION not in df["operation"].values
        assert df["operation"].isna().any()  # với operation_probs 0.2 cho MISSING, phải xuất hiện


class TestGeneratorAmountBounds:
    def test_amount_respects_per_operation_clip_bounds(self, fitted, rng):
        df = generate(fitted, n=3000, start_trans_id=1, rng=rng)

        vklad_amounts = df.loc[df["operation"] == "VKLAD", "amount"]
        assert (vklad_amounts >= 100).all()
        assert (vklad_amounts <= 2000).all()

        vyber_amounts = df.loc[df["operation"] == "VYBER", "amount"]
        assert (vyber_amounts >= 50).all()
        assert (vyber_amounts <= 1500).all()

    def test_amount_rounded_to_2_decimals(self, fitted, rng):
        df = generate(fitted, n=100, start_trans_id=1, rng=rng)
        rounded = df["amount"].round(2)
        assert np.allclose(df["amount"].values, rounded.values)


class TestGeneratorDateRange:
    """
    LƯU Ý QUAN TRỌNG: date sinh ra dạng YYMMDD-as-INTEGER, và int() cắt mất
    số 0 đầu với năm 2000-2009 (VD: "000101" -> int 101) — ĐÂY LÀ CÙNG HIỆN TƯỢNG
    đã gây ra bug date-parsing thật ở tầng dbt (xem stg_berka__trans.sql, LPAD fix).
    Không thể so sánh giá trị YYMMDD-int trực tiếp qua ranh giới thế kỷ vì nó
    KHÔNG đơn điệu tăng — phải decode đúng cách (giống hệt logic dbt) trước khi so sánh.
    """

    @staticmethod
    def _decode_berka_date(date_int: int) -> pd.Timestamp:
        """Decode YYMMDD-int (kể cả bị mất số 0 đầu) thành Timestamp thật, dùng đúng
        logic phân biệt thế kỷ như dbt: yy>=90 -> 19xx, else 20xx."""
        padded = str(int(date_int)).zfill(6)
        yy, mm, dd = int(padded[0:2]), int(padded[2:4]), int(padded[4:6])
        year = 1900 + yy if yy >= 90 else 2000 + yy
        return pd.Timestamp(year=year, month=mm, day=dd)

    def test_date_within_synthetic_period(self, fitted, rng):
        df = generate(fitted, n=1000, start_trans_id=1, rng=rng)
        decoded = df["date"].apply(self._decode_berka_date)

        synthetic_end = SYNTHETIC_START + pd.Timedelta(days=SYNTHETIC_DURATION_DAYS)
        assert (decoded >= SYNTHETIC_START).all()
        assert (decoded <= synthetic_end).all()

    def test_date_never_before_source_data_max(self, fitted, rng):
        """Đảm bảo không có nghịch lý: giao dịch synthetic xảy ra trước khi data gốc kết thúc."""
        df = generate(fitted, n=500, start_trans_id=1, rng=rng)
        decoded = df["date"].apply(self._decode_berka_date)
        date_max_decoded = self._decode_berka_date(fitted.date_max)

        assert (decoded > date_max_decoded).all()

    def test_date_stored_as_raw_int_can_lose_leading_zero_documented_behavior(self, fitted, rng):
        """
        Test này KHÔNG assert lỗi — nó DOCUMENT rõ ràng 1 đặc điểm đã biết:
        generator sinh date cho năm 2000-2009 dưới dạng int bị mất số 0 đầu
        (VD: 2000-01-01 -> 101, không phải 000101). Đây là lý do downstream
        (stg_berka__trans.sql) BẮT BUỘC phải LPAD trước khi parse — nếu ai đó
        sau này đổi generator để tự động zero-pad, test này sẽ fail và nhắc
        phải xem lại có còn cần LPAD ở dbt nữa không.
        """
        rng_early_2000s = np.random.default_rng(99)
        df = generate(fitted, n=2000, start_trans_id=1, rng=rng_early_2000s)

        has_short_date = (df["date"].astype(str).str.len() < 6).any()
        assert has_short_date, (
            "Kỳ vọng thấy ít nhất 1 date bị mất số 0 đầu (năm 2000s) trong "
            "2000 dòng sinh ra — nếu assertion này fail, nghĩa là generator "
            "đã được sửa để tự zero-pad, cần rà lại xem LPAD ở dbt còn cần thiết không."
        )


class TestGeneratorReproducibility:
    def test_same_seed_produces_identical_output(self, fitted):
        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)

        df1 = generate(fitted, n=200, start_trans_id=1, rng=rng1)
        df2 = generate(fitted, n=200, start_trans_id=1, rng=rng2)

        pd.testing.assert_frame_equal(df1, df2)

    def test_different_seed_produces_different_output(self, fitted):
        rng1 = np.random.default_rng(1)
        rng2 = np.random.default_rng(2)

        df1 = generate(fitted, n=200, start_trans_id=1, rng=rng1)
        df2 = generate(fitted, n=200, start_trans_id=1, rng=rng2)

        assert not df1["amount"].equals(df2["amount"])
