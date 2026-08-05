from datetime import timedelta

import pytest
from pydantic import ValidationError

from recovery_control_plane.normalization import BronzeOrder, normalize_and_dedupe
from recovery_control_plane.verifier import (
    SettlementRecord,
    VerificationFailed,
    assert_verified,
    calculate_metrics,
    verify_candidate,
)

from conftest import BASE_TIME


def valid_source() -> BronzeOrder:
    return BronzeOrder(
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


def valid_candidate(mapping_profile):
    return normalize_and_dedupe([valid_source()], mapping_profile)


def test_valid_v3_candidate_passes(mapping_profile) -> None:
    candidates = valid_candidate(mapping_profile)
    key = ("marketplace_it", "order-200", 7)
    metrics = verify_candidate(
        expected_keys={key},
        expected_v3_keys={key},
        source_rows=[valid_source()],
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
        required_currency_code=mapping_profile.currency_code,
        backfill_start=BASE_TIME - timedelta(minutes=1),
        backfill_end=BASE_TIME + timedelta(minutes=1),
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
            source_rows=[valid_source()],
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
            required_currency_code=mapping_profile.currency_code,
            backfill_start=BASE_TIME - timedelta(minutes=1),
            backfill_end=BASE_TIME + timedelta(minutes=1),
            maximum_rows=10,
        )


def test_unexpected_settlement_row_fails_closed(mapping_profile) -> None:
    candidates = valid_candidate(mapping_profile)
    key = ("marketplace_it", "order-200", 7)
    unexpected = ("marketplace_it", "order-999", 99)
    with pytest.raises(VerificationFailed, match="settlement_without_candidate"):
        verify_candidate(
            expected_keys={key},
            expected_v3_keys={key},
            source_rows=[valid_source()],
            candidates=candidates,
            settlements=[
                SettlementRecord(
                    partner=key[0],
                    order_id=key[1],
                    source_sequence=key[2],
                    currency="EUR",
                    settled_amount_minor=1_250,
                ),
                SettlementRecord(
                    partner=unexpected[0],
                    order_id=unexpected[1],
                    source_sequence=unexpected[2],
                    currency="EUR",
                    settled_amount_minor=10,
                ),
            ],
            required_profile_digest=mapping_profile.profile_digest,
            required_currency_code=mapping_profile.currency_code,
            backfill_start=BASE_TIME - timedelta(minutes=1),
            backfill_end=BASE_TIME + timedelta(minutes=1),
            required_profile_id=mapping_profile.profile_id,
            maximum_rows=10,
        )


def test_legacy_row_cannot_self_report_profile_provenance(mapping_profile) -> None:
    legacy = BronzeOrder(
        transport_id="tx-v2",
        partner="marketplace_it",
        payload_version="v2",
        order_id="order-legacy",
        source_sequence=1,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME,
        customer_id="redacted",
        order_total_minor=100,
        currency="EUR",
    )
    candidate = normalize_and_dedupe([legacy], mapping_profile)[0].model_copy(
        update={
            "applied_profile_id": mapping_profile.profile_id,
            "applied_profile_digest": mapping_profile.profile_digest,
        }
    )
    key = ("marketplace_it", "order-legacy", 1)
    with pytest.raises(VerificationFailed, match="profile_provenance_mismatches"):
        verify_candidate(
            expected_keys={key},
            expected_v3_keys={key},
            source_rows=[valid_source()],
            candidates=[candidate.model_copy(update={"payload_version": "v3"})],
            settlements=[
                SettlementRecord(
                    partner=key[0],
                    order_id=key[1],
                    source_sequence=key[2],
                    currency="EUR",
                    settled_amount_minor=100,
                )
            ],
            required_profile_digest=mapping_profile.profile_digest,
            required_currency_code=mapping_profile.currency_code,
            backfill_start=BASE_TIME - timedelta(minutes=1),
            backfill_end=BASE_TIME + timedelta(minutes=1),
            required_profile_id="different-approved-profile",
            maximum_rows=10,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("source_sequence", True), ("source_sequence", "7"), ("settled_amount_minor", "1250")],
)
def test_settlement_accounting_fields_are_strict(field, value) -> None:
    payload = {
        "partner": "marketplace_it",
        "order_id": "order-200",
        "source_sequence": 7,
        "currency": "EUR",
        "settled_amount_minor": 1_250,
    }
    payload[field] = value
    with pytest.raises(ValidationError):
        SettlementRecord.model_validate(payload)


def test_expected_key_rejects_boolean_sequence(mapping_profile) -> None:
    with pytest.raises(TypeError, match="strict"):
        verify_candidate(
            expected_keys={("marketplace_it", "order-200", True)},
            expected_v3_keys={("marketplace_it", "order-200", True)},
            source_rows=[valid_source()],
            candidates=valid_candidate(mapping_profile),
            settlements=[],
            required_profile_digest=mapping_profile.profile_digest,
            required_currency_code=mapping_profile.currency_code,
            backfill_start=BASE_TIME - timedelta(minutes=1),
            backfill_end=BASE_TIME + timedelta(minutes=1),
            maximum_rows=10,
        )


SEMANTIC_FAILURE_CASES = (
    ("mapping-false", {"mapping_valid": False}, "mapping_validation_failures"),
    ("mapping-null", {"mapping_valid": None}, "mapping_validation_failures"),
    (
        "authorization-false",
        {"payload_version_authorized": False},
        "payload_authorization_failures",
    ),
    (
        "authorization-null",
        {"payload_version_authorized": None},
        "payload_authorization_failures",
    ),
    ("order-id-null", {"order_id": None}, "missing_order_id_rows"),
    ("order-id-empty", {"order_id": ""}, "missing_order_id_rows"),
    ("order-id-blank", {"order_id": "   "}, "missing_order_id_rows"),
    ("customer-id-null", {"customer_id": None}, "missing_customer_id_rows"),
    ("customer-id-empty", {"customer_id": ""}, "missing_customer_id_rows"),
    ("customer-id-blank", {"customer_id": "   "}, "missing_customer_id_rows"),
    ("event-time-null", {"event_time": None}, "invalid_event_time_rows"),
    (
        "event-time-before-window",
        {"event_time": BASE_TIME - timedelta(minutes=2)},
        "invalid_event_time_rows",
    ),
    (
        "event-time-at-exclusive-end",
        {"event_time": BASE_TIME + timedelta(minutes=1)},
        "invalid_event_time_rows",
    ),
    ("event-type-null", {"event_type": None}, "unsupported_event_type_rows"),
    (
        "event-type-unsupported",
        {"event_type": "CHARGEBACK"},
        "unsupported_event_type_rows",
    ),
    (
        "sale-zero",
        {"event_type": "SALE", "order_total_minor": 0},
        "invalid_sale_amount_rows",
    ),
    (
        "sale-negative",
        {"event_type": "SALE", "order_total_minor": -1},
        "invalid_sale_amount_rows",
    ),
    (
        "refund-zero",
        {"event_type": "REFUND", "order_total_minor": 0},
        "invalid_refund_amount_rows",
    ),
    (
        "refund-positive",
        {"event_type": "REFUND", "order_total_minor": 1},
        "invalid_refund_amount_rows",
    ),
)


@pytest.mark.parametrize(
    ("_case", "updates", "failure_metric"),
    SEMANTIC_FAILURE_CASES,
    ids=[case[0] for case in SEMANTIC_FAILURE_CASES],
)
def test_independent_verifier_recomputes_candidate_semantics(
    mapping_profile,
    _case,
    updates,
    failure_metric,
) -> None:
    """A forged success flag cannot hide invalid canonical field values."""

    candidate = valid_candidate(mapping_profile)[0].model_copy(update=updates)
    key = ("marketplace_it", "order-200", 7)
    metrics = calculate_metrics(
        expected_keys={key},
        expected_v3_keys={key},
        source_rows=[valid_source()],
        candidates=[candidate],
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
        required_currency_code=mapping_profile.currency_code,
        backfill_start=BASE_TIME - timedelta(minutes=1),
        backfill_end=BASE_TIME + timedelta(minutes=1),
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )

    assert getattr(metrics, failure_metric) == 1
    with pytest.raises(VerificationFailed, match=failure_metric):
        assert_verified(
            metrics,
            maximum_rows=10,
            settlement_tolerance_minor=0,
        )


@pytest.mark.parametrize(
    ("updates", "failure_metric"),
    [
        ({"order_id": None}, "source_invalid_business_key_rows"),
        ({"order_id": "   "}, "source_invalid_business_key_rows"),
        ({"source_sequence": None}, "source_invalid_business_key_rows"),
        (
            {"payload_version": "v99-unsupported"},
            "source_unauthorized_payload_versions",
        ),
    ],
    ids=["null-order", "blank-order", "null-sequence", "unauthorized-version"],
)
def test_independent_verifier_counts_malformed_retained_source_rows(
    mapping_profile,
    updates,
    failure_metric,
) -> None:
    key = ("marketplace_it", "order-200", 7)
    malformed_source = valid_source().model_copy(update=updates)
    metrics = calculate_metrics(
        expected_keys={key},
        expected_v3_keys={key},
        source_rows=[valid_source(), malformed_source],
        candidates=valid_candidate(mapping_profile),
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
        required_currency_code=mapping_profile.currency_code,
        backfill_start=BASE_TIME - timedelta(minutes=1),
        backfill_end=BASE_TIME + timedelta(minutes=1),
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )

    assert metrics.source_rows == 2
    assert getattr(metrics, failure_metric) == 1
    with pytest.raises(VerificationFailed, match=failure_metric):
        assert_verified(
            metrics,
            maximum_rows=10,
            settlement_tolerance_minor=0,
        )


@pytest.mark.parametrize(
    ("updates", "failure_metric"),
    [
        (
            {"event_time": BASE_TIME + timedelta(seconds=30)},
            "source_candidate_event_time_mismatches",
        ),
        ({"event_type": "REFUND"}, "source_candidate_event_type_mismatches"),
        (
            {"customer_id": "different-customer"},
            "source_candidate_customer_id_mismatches",
        ),
    ],
    ids=["event-time", "event-type", "customer-id"],
)
def test_independent_verifier_compares_selected_candidate_fields_to_source(
    mapping_profile,
    updates,
    failure_metric,
) -> None:
    key = ("marketplace_it", "order-200", 7)
    candidate = valid_candidate(mapping_profile)[0].model_copy(update=updates)
    metrics = calculate_metrics(
        expected_keys={key},
        expected_v3_keys={key},
        source_rows=[valid_source()],
        candidates=[candidate],
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
        required_currency_code=mapping_profile.currency_code,
        backfill_start=BASE_TIME - timedelta(minutes=1),
        backfill_end=BASE_TIME + timedelta(minutes=1),
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )

    assert getattr(metrics, failure_metric) == 1
    with pytest.raises(VerificationFailed, match=failure_metric):
        assert_verified(
            metrics,
            maximum_rows=10,
            settlement_tolerance_minor=0,
        )


def test_self_consistent_wrong_currency_cannot_impersonate_profile(
    mapping_profile,
) -> None:
    source = valid_source().model_copy(
        update={
            "rescued_data": {
                "buyer": {"customerId": "redacted"},
                "grossAmount": {
                    "units": 12,
                    "nanos": 500_000_000,
                    "currencyCode": "USD",
                },
            }
        }
    )
    candidate = valid_candidate(mapping_profile)[0].model_copy(
        update={"currency": "USD"}
    )
    key = ("marketplace_it", "order-200", 7)
    metrics = calculate_metrics(
        expected_keys={key},
        expected_v3_keys={key},
        source_rows=[source],
        candidates=[candidate],
        settlements=[
            SettlementRecord(
                partner=key[0],
                order_id=key[1],
                source_sequence=key[2],
                currency="USD",
                settled_amount_minor=1_250,
            )
        ],
        required_profile_digest=mapping_profile.profile_digest,
        required_currency_code=mapping_profile.currency_code,
        backfill_start=BASE_TIME - timedelta(minutes=1),
        backfill_end=BASE_TIME + timedelta(minutes=1),
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )

    assert metrics.source_profile_currency_mismatches == 1
    assert metrics.candidate_profile_currency_mismatches == 1
    with pytest.raises(VerificationFailed, match="profile_currency_mismatches"):
        assert_verified(
            metrics,
            maximum_rows=10,
            settlement_tolerance_minor=0,
        )
