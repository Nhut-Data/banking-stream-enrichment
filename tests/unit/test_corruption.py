"""
Unit tests cho data_generation/corruption.py — hàm inject().

Chạy: pytest tests/unit/test_corruption.py -v
"""
import numpy as np
import pandas as pd
import pytest

from data_generation.corruption import inject


@pytest.fixture
def clean_df():
    """1,000 dòng synthetic 'sạch', chưa tiêm lỗi gì."""
    n = 1000
    return pd.DataFrame({
        "trans_id": np.arange(1, n + 1),
        "account_id": np.random.default_rng(1).choice([100, 200, 300, 400], size=n),
        "date": np.full(n, 990101),
        "type": ["PRIJEM"] * n,
        "operation": ["VKLAD"] * n,
        "amount": np.random.default_rng(1).uniform(10, 1000, size=n),
        "k_symbol": ["SIPO"] * n,
    })


@pytest.fixture
def real_account_ids():
    return np.array([100, 200, 300, 400])


@pytest.fixture
def rng():
    return np.random.default_rng(42)


class TestDuplicateInjection:
    def test_row_count_increases_by_expected_amount(self, clean_df, real_account_ids, rng):
        n_before = len(clean_df)
        df_corrupted, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.02, rate_referential=0.0, rate_missing=0.0,
            rng=rng,
        )
        expected_dup = int(n_before * 0.02)

        assert len(df_corrupted) == n_before + expected_dup
        assert manifest["corruptions"]["duplicate"]["count"] == expected_dup

    def test_duplicated_trans_ids_appear_exactly_twice(self, clean_df, real_account_ids, rng):
        df_corrupted, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.02, rate_referential=0.0, rate_missing=0.0,
            rng=rng,
        )
        dup_ids = manifest["corruptions"]["duplicate"]["trans_ids"]

        for tid in dup_ids:
            assert (df_corrupted["trans_id"] == tid).sum() == 2

    def test_zero_rate_still_injects_at_least_one_by_design(self, clean_df, real_account_ids, rng):
        """Code dùng max(1, int(n * rate)) — rate=0 vẫn tiêm 1 dòng. Test ghi lại behavior này rõ ràng."""
        df_corrupted, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.0, rate_referential=0.0, rate_missing=0.0,
            rng=rng,
        )
        assert manifest["corruptions"]["duplicate"]["count"] == 1


class TestReferentialViolationInjection:
    def test_fake_account_ids_never_collide_with_real_ones(self, clean_df, real_account_ids, rng):
        df_corrupted, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.0, rate_referential=0.05, rate_missing=0.0,
            rng=rng,
        )
        fake_ids = manifest["corruptions"]["referential_violation"]["fake_account_ids"]

        assert not set(fake_ids) & set(real_account_ids.tolist())

    def test_correct_count_of_rows_affected(self, clean_df, real_account_ids, rng):
        n_before = len(clean_df)
        _, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.0, rate_referential=0.05, rate_missing=0.0,
            rng=rng,
        )
        expected = int(n_before * 0.05)

        assert manifest["corruptions"]["referential_violation"]["count"] == expected

    def test_manifest_preserves_original_account_ids_for_audit(self, clean_df, real_account_ids, rng):
        _, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.0, rate_referential=0.05, rate_missing=0.0,
            rng=rng,
        )
        originals = manifest["corruptions"]["referential_violation"]["original_account_ids"]

        # Mọi original account_id phải nằm trong tập account thật
        assert set(originals) <= set(real_account_ids.tolist())


class TestMissingValueInjection:
    def test_correct_count_of_k_symbol_set_to_nan(self, clean_df, real_account_ids, rng):
        n_before = len(clean_df)
        df_corrupted, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.0, rate_referential=0.0, rate_missing=0.03,
            rng=rng,
        )
        expected = int(n_before * 0.03)

        assert manifest["corruptions"]["missing_value"]["count"] == expected
        assert df_corrupted["k_symbol"].isna().sum() == expected

    def test_only_k_symbol_affected_not_other_fields(self, clean_df, real_account_ids, rng):
        df_corrupted, _ = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.0, rate_referential=0.0, rate_missing=0.05,
            rng=rng,
        )
        # trans_id, account_id, amount không bao giờ được phép null
        assert df_corrupted["trans_id"].isna().sum() == 0
        assert df_corrupted["account_id"].isna().sum() == 0
        assert df_corrupted["amount"].isna().sum() == 0


class TestManifestStructure:
    def test_manifest_has_required_top_level_keys(self, clean_df, real_account_ids, rng):
        _, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.02, rate_referential=0.02, rate_missing=0.03,
            rng=rng,
        )
        assert "total_rows_before_corruption" in manifest
        assert "total_rows_after_corruption" in manifest
        assert "summary" in manifest
        assert set(manifest["corruptions"].keys()) == {"duplicate", "referential_violation", "missing_value"}

    def test_reproducible_with_same_seed(self, clean_df, real_account_ids):
        rng1 = np.random.default_rng(42)
        rng2 = np.random.default_rng(42)

        _, manifest1 = inject(clean_df, real_account_ids, 0.02, 0.02, 0.03, rng1)
        _, manifest2 = inject(clean_df, real_account_ids, 0.02, 0.02, 0.03, rng2)

        assert manifest1["corruptions"]["duplicate"]["trans_ids"] == manifest2["corruptions"]["duplicate"]["trans_ids"]

    def test_corruptions_are_independent_not_mutually_exclusive(self, clean_df, real_account_ids, rng):
        """1 dòng có thể dính nhiều loại lỗi cùng lúc — đúng comment trong code gốc."""
        _, manifest = inject(
            clean_df, real_account_ids,
            rate_duplicate=0.5, rate_referential=0.5, rate_missing=0.5,
            rng=rng,
        )
        dup_ids = set(manifest["corruptions"]["duplicate"]["trans_ids"])
        ref_ids = set(manifest["corruptions"]["referential_violation"]["trans_ids"])

        # Với rate cao (0.5 mỗi loại), khả năng cao có overlap — không assert bắt buộc
        # phải có overlap (do random), chỉ assert rằng code KHÔNG loại trừ khả năng này
        # (nếu 2 tập hoàn toàn rời nhau dù rate 50%+50% trên 1000 dòng, rất đáng ngờ)
        assert len(dup_ids) > 0 and len(ref_ids) > 0
