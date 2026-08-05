from datetime import timedelta

import pytest
from pydantic import ValidationError

from recovery_control_plane.normalization import BronzeOrder, normalize_and_dedupe
from recovery_control_plane.profiles import build_profile
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
            source_rows=[v2_order(), v3_order(nanos=500_000_000)],
            candidates=winners,
            settlements=settlements,
            required_profile_digest=mapping_profile.profile_digest,
            required_currency_code=mapping_profile.currency_code,
            backfill_start=BASE_TIME - timedelta(minutes=1),
            backfill_end=BASE_TIME + timedelta(minutes=1),
            maximum_rows=10,
        )


def test_transport_id_breaks_an_exact_v3_ingestion_tie(mapping_profile) -> None:
    lower = v3_order(nanos=-500_000_000, transport_id="delivery-a")
    higher = v3_order(nanos=-600_000_000, transport_id="delivery-z")

    winner = normalize_and_dedupe([higher, lower], mapping_profile)[0]

    assert winner.transport_id == "delivery-z"
    assert winner.order_total_minor == -860


def test_unknown_version_is_quarantined_and_verification_fails(mapping_profile) -> None:
    unknown = BronzeOrder(
        transport_id="tx-v4",
        partner="marketplace_it",
        payload_version="v4-unknown",
        order_id="order-100",
        source_sequence=2,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME,
        customer_id="customer-redacted",
        order_total_minor=100,
        currency="EUR",
    )
    winners = normalize_and_dedupe(
        [v3_order(nanos=-500_000_000), unknown], mapping_profile
    )
    quarantined = next(row for row in winners if row.payload_version == "v4-unknown")
    assert quarantined.payload_version_authorized is False
    assert quarantined.mapping_valid is False
    assert quarantined.canonical_valid is False

    keys = {
        ("marketplace_it", "order-100", 1),
        ("marketplace_it", "order-100", 2),
    }
    settlements = [
        SettlementRecord(
            partner=row.partner,
            order_id=row.order_id,
            source_sequence=row.source_sequence,
            currency="EUR",
            settled_amount_minor=row.order_total_minor,
        )
        for row in winners
        if row.order_id is not None
        and row.source_sequence is not None
        and row.order_total_minor is not None
    ]
    with pytest.raises(VerificationFailed, match="unauthorized_payload_versions"):
        verify_candidate(
            expected_keys=keys,
            expected_v3_keys={("marketplace_it", "order-100", 1)},
            source_rows=[v3_order(nanos=-500_000_000), unknown],
            candidates=winners,
            settlements=settlements,
            required_profile_digest=mapping_profile.profile_digest,
            required_currency_code=mapping_profile.currency_code,
            backfill_start=BASE_TIME - timedelta(minutes=1),
            backfill_end=BASE_TIME + timedelta(minutes=1),
            required_profile_id=mapping_profile.profile_id,
            maximum_rows=10,
        )


@pytest.mark.parametrize(
    "buyer",
    [
        {"customerId": {"nested": "not-a-scalar"}},
        {"customerId": ""},
        {"customerId": "   "},
    ],
)
def test_malformed_v3_customer_is_quarantined_not_raised(mapping_profile, buyer) -> None:
    malformed = v3_order(nanos=-500_000_000).model_copy(
        update={
            "rescued_data": {
                "buyer": buyer,
                "grossAmount": {
                    "units": -8,
                    "nanos": -500_000_000,
                    "currencyCode": "EUR",
                },
            }
        }
    )

    result = normalize_and_dedupe([malformed], mapping_profile)[0]

    assert result.customer_id is None
    assert result.mapping_valid is False
    assert result.canonical_valid is False


def test_declared_transport_dedupe_key_is_honored(mapping_profile) -> None:
    spec = mapping_profile.model_dump(exclude={"profile_digest"})
    spec["dedupe_key"] = ("transport_id",)
    transport_profile = build_profile(**spec)

    winners = normalize_and_dedupe(
        [
            v2_order(transport_id="delivery-a"),
            v3_order(nanos=-500_000_000, transport_id="delivery-b"),
        ],
        transport_profile,
    )

    assert {row.transport_id for row in winners} == {"delivery-a", "delivery-b"}


def test_conflicting_total_tie_is_stable_and_explicit(mapping_profile) -> None:
    first = v3_order(nanos=-500_000_000, transport_id="same-delivery")
    second = v3_order(nanos=-600_000_000, transport_id="same-delivery")

    forward = normalize_and_dedupe([first, second], mapping_profile)[0]
    reverse = normalize_and_dedupe([second, first], mapping_profile)[0]

    assert forward == reverse
    assert forward.dedupe_ambiguous is True
    assert forward.canonical_valid is False


@pytest.mark.parametrize(
    ("field", "value"),
    [("source_sequence", True), ("source_sequence", "1"), ("order_total_minor", "100")],
)
def test_bronze_accounting_fields_are_strict(field, value) -> None:
    payload = v2_order().model_dump()
    payload[field] = value
    with pytest.raises(ValidationError):
        BronzeOrder.model_validate(payload)


def test_profile_cannot_name_an_unshipped_normalizer(mapping_profile) -> None:
    spec = mapping_profile.model_dump(exclude={"profile_digest"})
    spec["normalizer_key"] = "MODEL_WROTE_THIS_FUNCTION"

    with pytest.raises(ValidationError, match="not allowlisted"):
        build_profile(**spec)
