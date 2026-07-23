"""
Unit tests cho speed_layer/enrichment.py — hàm enrich() (pure function, không I/O).

Chạy: pytest tests/unit/test_enrichment.py -v
"""
import pytest

from speed_layer.enrichment import enrich


class FakeStateManager:
    """Test double cho StateManager — chỉ cần implement .lookup()."""

    def __init__(self, profiles: dict):
        self._profiles = profiles

    def lookup(self, account_id):
        return self._profiles.get(account_id)


@pytest.fixture
def sample_transaction():
    return {
        "trans_id": 1001,
        "account_id": 42,
        "date": 930101,
        "type": "PRIJEM",
        "operation": "VKLAD",
        "amount": 700.0,
        "balance": 700.0,
        "k_symbol": "",
        "__op": "c",
        "__lsn": 123456,
        "__source_ts_ms": 1700000000000,
    }


@pytest.fixture
def sample_profile():
    return {
        "client_id": 501,
        "birth_number": 700101,
        "account_frequency": "POPLATEK MESICNE",
        "district_name": "Hl.m. Praha",
        "district_region": "Prague",
        "avg_salary": 12541,
    }


class TestEnrichHappyPath:
    def test_successful_enrichment_returns_merged_record(self, sample_transaction, sample_profile):
        state = FakeStateManager({42: sample_profile})

        enriched, dlq = enrich(sample_transaction, state)

        assert dlq is None
        assert enriched is not None

    def test_enriched_record_contains_all_transaction_fields(self, sample_transaction, sample_profile):
        state = FakeStateManager({42: sample_profile})
        enriched, _ = enrich(sample_transaction, state)

        assert enriched["trans_id"] == 1001
        assert enriched["account_id"] == 42
        assert enriched["amount"] == 700.0
        assert enriched["type"] == "PRIJEM"

    def test_enriched_record_contains_all_profile_fields(self, sample_transaction, sample_profile):
        state = FakeStateManager({42: sample_profile})
        enriched, _ = enrich(sample_transaction, state)

        assert enriched["client_id"] == 501
        assert enriched["district_name"] == "Hl.m. Praha"
        assert enriched["avg_salary"] == 12541

    def test_enriched_record_flagged_correctly(self, sample_transaction, sample_profile):
        state = FakeStateManager({42: sample_profile})
        enriched, _ = enrich(sample_transaction, state)

        assert enriched["__enriched"] is True

    def test_cdc_metadata_preserved(self, sample_transaction, sample_profile):
        state = FakeStateManager({42: sample_profile})
        enriched, _ = enrich(sample_transaction, state)

        assert enriched["__op"] == "c"
        assert enriched["__lsn"] == 123456
        assert enriched["__source_ts_ms"] == 1700000000000


class TestEnrichMissingAccountId:
    def test_none_account_id_routes_to_dlq(self, sample_transaction):
        sample_transaction["account_id"] = None
        state = FakeStateManager({})

        enriched, dlq = enrich(sample_transaction, state)

        assert enriched is None
        assert dlq is not None
        assert dlq["dlq_reason"] == "missing_account_id"

    def test_key_absent_entirely_treated_as_missing(self, sample_transaction):
        del sample_transaction["account_id"]
        state = FakeStateManager({})

        enriched, dlq = enrich(sample_transaction, state)

        assert enriched is None
        assert dlq["dlq_reason"] == "missing_account_id"

    def test_account_id_zero_is_NOT_treated_as_missing(self, sample_transaction, sample_profile):
        """
        Edge case quan trọng: account_id=0 là giá trị falsy trong Python
        nhưng là 1 account_id hợp lệ. Code dùng `is None` (không phải `if not account_id`)
        — test này bảo vệ chống lại regression nếu ai đó sau này đổi thành `if not account_id`.
        """
        sample_transaction["account_id"] = 0
        state = FakeStateManager({0: sample_profile})

        enriched, dlq = enrich(sample_transaction, state)

        assert dlq is None
        assert enriched is not None
        assert enriched["account_id"] == 0


class TestEnrichReferentialViolation:
    def test_account_id_not_found_routes_to_dlq(self, sample_transaction):
        state = FakeStateManager({})  # State Table rỗng — account_id 42 không tồn tại

        enriched, dlq = enrich(sample_transaction, state)

        assert enriched is None
        assert dlq is not None
        assert dlq["dlq_reason"] == "referential_violation"

    def test_dlq_record_preserves_original_transaction(self, sample_transaction):
        state = FakeStateManager({})

        _, dlq = enrich(sample_transaction, state)

        assert dlq["original_transaction"] == sample_transaction
        assert dlq["trans_id"] == 1001
        assert dlq["account_id"] == 42

    def test_dlq_record_preserves_source_timestamp(self, sample_transaction):
        state = FakeStateManager({})

        _, dlq = enrich(sample_transaction, state)

        assert dlq["__source_ts_ms"] == 1700000000000

    def test_lookup_called_with_correct_account_id(self, sample_transaction, sample_profile, mocker):
        state = mocker.Mock()
        state.lookup.return_value = sample_profile

        enrich(sample_transaction, state)

        state.lookup.assert_called_once_with(42)


class TestEnrichReturnTypeContract:
    def test_exactly_one_of_enriched_or_dlq_is_none_happy_path(self, sample_transaction, sample_profile):
        state = FakeStateManager({42: sample_profile})
        enriched, dlq = enrich(sample_transaction, state)

        assert (enriched is None) != (dlq is None)  # XOR — đúng 1 trong 2 phải None

    def test_exactly_one_of_enriched_or_dlq_is_none_failure_path(self, sample_transaction):
        state = FakeStateManager({})
        enriched, dlq = enrich(sample_transaction, state)

        assert (enriched is None) != (dlq is None)
