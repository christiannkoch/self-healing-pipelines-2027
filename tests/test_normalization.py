from datetime import timedelta

import pytest

from recovery_control_plane.normalization import BronzeOrder, normalize_and_dedupe
from recovery_control_plane.verifier import SettlementRecord, VerificationFailed, verify_candidate

from conftest import BASE_TIME


def v2_order(*, transport_id: str = "tx-v2", sequence: int = 1) -> BronzeOrder:
    return BronzeOrder(
        transport_id=transport_id,
        partner="marketplace_it",
        payload_version="v2",
        order_id="order-100",
        source_sequence=sequence,
        event_type="REFUND",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME,
        customer_id="customer-redacted",
        order_total_minor=-850,
        currency="EUR",
    )


def v3_order(*, nanos: int, transport_id: str = "tx-v3", sequence: int = 1) -> BronzeOrder:
    return BronzeOrder(
        transport_id=transport_id,
        partner="marketplace_it",
        payload_version="v3",
        order_id="order-100",
        source_sequence=sequence,
        event_type="REFUND",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME + timedelta(seconds=1),
        rescued_data={
            "buyer": {"customerId": "customer-redacted"},
            "grossAmount": {"units": -8, "nanos": nanos, "currencyCode": "EUR"},
        },
    )


def test_valid_v3_wins_over_overlapping_v2(mapping_profile) -> None:
    winners = normalize_and_dedupe(
        [v2_order(), v3_order(nanos=-500_000_000)],
        mapping_profile,
    )
    assert len(winners) == 1
    assert winners[0].payload_version == "v3"
    assert winners[0].order_total_minor == -850
    assert winners[0].canonical_valid is True
    assert winners[0].applied_profile_digest == mapping_profile.profile_digest


def test_invalid_v3_falls_back_to_v2_but_cannot_prove_v3(mapping_profile) -> None:
    winners = normalize_and_dedupe(
        [v2_order(), v3_order(nanos=500_000_000)],
        mapping_profile,
    )
    assert winners[0].payload_version == "v2"

    key = ("marketplace_it", "order-100", 1)
    settlements = [
        SettlementRecord(
            partner=key[0],
            order_id=key[1],
            source_sequence=key[2],
            currency="EUR",
            settled_amount_minor=-850,
        )
    ]
    with pytest.raises(VerificationFailed, match="unproven_expected_v3_rows"):
        verify_candidate(
            expected_keys={key},
            expected_v3_keys={key},
            candidates=winners,
            settlements=settlements,
            required_profile_digest=mapping_profile.profile_digest,
            maximum_rows=10,
        )


def test_transport_id_breaks_an_exact_v3_ingestion_tie(mapping_profile) -> None:
    lower = v3_order(nanos=-500_000_000, transport_id="delivery-a")
    higher = v3_order(nanos=-600_000_000, transport_id="delivery-z")

    winner = normalize_and_dedupe([higher, lower], mapping_profile)[0]

    assert winner.transport_id == "delivery-z"
    assert winner.order_total_minor == -860
