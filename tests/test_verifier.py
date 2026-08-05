import pytest

from recovery_control_plane.normalization import BronzeOrder, normalize_and_dedupe
from recovery_control_plane.verifier import SettlementRecord, VerificationFailed, verify_candidate

from conftest import BASE_TIME


def valid_candidate(mapping_profile):
    source = BronzeOrder(
        transport_id="tx-v3",
        partner="marketplace_it",
        payload_version="v3",
        order_id="order-200",
        source_sequence=7,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME,
        rescued_data={
            "buyer": {"customerId": "redacted"},
            "grossAmount": {"units": 12, "nanos": 500_000_000, "currencyCode": "EUR"},
        },
    )
    return normalize_and_dedupe([source], mapping_profile)


def test_valid_v3_candidate_passes(mapping_profile) -> None:
    candidates = valid_candidate(mapping_profile)
    key = ("marketplace_it", "order-200", 7)
    metrics = verify_candidate(
        expected_keys={key},
        expected_v3_keys={key},
        candidates=candidates,
        settlements=[
            SettlementRecord(
                partner=key[0],
                order_id=key[1],
                source_sequence=key[2],
                currency="EUR",
                settled_amount_minor=1_250,
            )
        ],
        required_profile_digest=mapping_profile.profile_digest,
        maximum_rows=10,
    )
    assert metrics.unproven_expected_v3_rows == 0
    assert metrics.settlement_difference_minor == 0


@pytest.mark.parametrize(
    ("currency", "amount", "metric"),
    [
        (None, 1_250, "null_settlement_currency_rows"),
        ("EUR", None, "null_settlement_amount_rows"),
    ],
)
def test_null_settlement_evidence_fails_closed(mapping_profile, currency, amount, metric) -> None:
    candidates = valid_candidate(mapping_profile)
    key = ("marketplace_it", "order-200", 7)
    with pytest.raises(VerificationFailed, match=metric):
        verify_candidate(
            expected_keys={key},
            expected_v3_keys={key},
            candidates=candidates,
            settlements=[
                SettlementRecord(
                    partner=key[0],
                    order_id=key[1],
                    source_sequence=key[2],
                    currency=currency,
                    settled_amount_minor=amount,
                )
            ],
            required_profile_digest=mapping_profile.profile_digest,
            maximum_rows=10,
        )

