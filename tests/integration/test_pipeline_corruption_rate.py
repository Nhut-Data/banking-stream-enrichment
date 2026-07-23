"""
Integration test — end-to-end profiler → generator → corruption,
verify tỷ lệ lỗi tiêm ra khớp với config đã set (giống .env thật:
CORRUPTION_RATE_DUPLICATE=0.02, CORRUPTION_RATE_REFERENTIAL=0.02,
CORRUPTION_RATE_MISSING=0.03).

KHÔNG cần Docker/Postgres/Kafka/Berka CSV thật — dùng synthetic "real" data
giả lập để profiler fit, chạy được thuần trong CI.

Chạy: pytest tests/integration/test_pipeline_corruption_rate.py -v
"""
import numpy as np
import pandas as pd
import pytest

from data_generation.profiler import load_and_fit
from data_generation.generator import generate
from data_generation.corruption import inject


@pytest.fixture(scope="module")
def fake_real_trans_csv(tmp_path_factory):
    """
    Giả lập 1 file trans.csv 'thật' đủ lớn (5,000 dòng) để profiler fit ra
    phân phối hợp lý — thay thế cho việc phải download Berka CSV thật trong CI.
    """
    rng = np.random.default_rng(7)
    n = 5000
    operations = rng.choice(
        ["VKLAD", "VYBER", "PREVOD NA UCET", None],
        size=n, p=[0.4, 0.3, 0.2, 0.1],
    )
    type_map = {"VKLAD": "PRIJEM", "VYBER": "VYDAJ", "PREVOD NA UCET": "VYDAJ", None: "VYDAJ"}
    types = [type_map[o] for o in operations]

    df = pd.DataFrame({
        "trans_id": np.arange(1, n + 1),
        "account_id": rng.choice(np.arange(1, 101), size=n),
        "date": rng.integers(930101, 981231, size=n),
        "type": types,
        "operation": operations,
        "amount": rng.lognormal(mean=5, sigma=1, size=n).round(2),
        "k_symbol": rng.choice(["SIPO", "UVER", ""], size=n),
        "balance": np.nan,
        "bank": None,
        "account": None,
    })

    path = tmp_path_factory.mktemp("data") / "trans.csv"
    df.to_csv(path, sep=";", index=False)
    return path


@pytest.fixture(scope="module")
def fitted(fake_real_trans_csv):
    return load_and_fit(fake_real_trans_csv)


class TestEndToEndCorruptionRateAccuracy:
    """
    Config thật trong .env.example: DUPLICATE=0.02, REFERENTIAL=0.02, MISSING=0.03.
    Test này verify với đúng các rate này, trên n đủ lớn (100K), manifest phản ánh
    đúng tỷ lệ config — đây chính là con số dùng để đối chiếu dbt test sau này.
    """

    def test_configured_rates_produce_expected_counts_at_scale(self, fitted):
        rng = np.random.default_rng(42)
        n = 100_000
        df_clean = generate(fitted, n=n, start_trans_id=fitted.max_trans_id + 1, rng=rng)

        df_corrupted, manifest = inject(
            df_clean,
            real_account_ids=fitted.account_ids,
            rate_duplicate=0.02,
            rate_referential=0.02,
            rate_missing=0.03,
            rng=rng,
        )

        assert manifest["corruptions"]["duplicate"]["count"] == int(n * 0.02)
        assert manifest["corruptions"]["referential_violation"]["count"] == int(n * 0.02)
        assert manifest["corruptions"]["missing_value"]["count"] == int(n * 0.03)

    def test_total_row_count_increases_only_from_duplicates(self, fitted):
        """Referential violation và missing value MUTATE dòng có sẵn, không thêm dòng mới — chỉ duplicate mới tăng row count."""
        rng = np.random.default_rng(42)
        n = 10_000
        df_clean = generate(fitted, n=n, start_trans_id=fitted.max_trans_id + 1, rng=rng)

        df_corrupted, manifest = inject(
            df_clean, fitted.account_ids,
            rate_duplicate=0.02, rate_referential=0.02, rate_missing=0.03,
            rng=rng,
        )

        expected_total = n + manifest["corruptions"]["duplicate"]["count"]
        assert len(df_corrupted) == expected_total

    def test_manifest_referential_violation_ids_actually_not_in_real_accounts(self, fitted):
        """Đối chiếu cuối: mọi fake account_id trong manifest phải KHÔNG có trong bảng account thật — nếu không, dbt relationships test sẽ không bắt được lỗi này như kỳ vọng."""
        rng = np.random.default_rng(42)
        n = 5_000
        df_clean = generate(fitted, n=n, start_trans_id=fitted.max_trans_id + 1, rng=rng)

        df_corrupted, manifest = inject(
            df_clean, fitted.account_ids,
            rate_duplicate=0.02, rate_referential=0.02, rate_missing=0.03,
            rng=rng,
        )
        fake_ids = manifest["corruptions"]["referential_violation"]["fake_account_ids"]
        real_ids = set(fitted.account_ids.tolist())

        assert not (set(fake_ids) & real_ids), (
            "Fake account_id trùng với account thật — dbt relationships test "
            "sẽ KHÔNG bắt được referential violation này, làm sai lệch bằng chứng "
            "trong docs/corruption_manifest.json"
        )

    def test_duplicate_trans_ids_result_in_exactly_double_count_in_final_df(self, fitted):
        rng = np.random.default_rng(42)
        n = 5_000
        df_clean = generate(fitted, n=n, start_trans_id=fitted.max_trans_id + 1, rng=rng)

        df_corrupted, manifest = inject(
            df_clean, fitted.account_ids,
            rate_duplicate=0.02, rate_referential=0.0, rate_missing=0.0,
            rng=rng,
        )
        dup_ids = manifest["corruptions"]["duplicate"]["trans_ids"]

        counts = df_corrupted[df_corrupted["trans_id"].isin(dup_ids)]["trans_id"].value_counts()
        assert (counts == 2).all()
