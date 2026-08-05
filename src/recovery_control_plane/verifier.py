"""Independent, fail-closed verification for a bounded repair candidate."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Collection
from datetime import datetime, timezone
from typing import Annotated, Iterable, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from .normalization import BronzeOrder, CanonicalOrder


BusinessKey: TypeAlias = tuple[str, str, int]
Count: TypeAlias = Annotated[int, Field(ge=0, strict=True)]


class VerificationFailed(RuntimeError):
    """Raised when candidate evidence is incomplete or violates an invariant."""


class SettlementRecord(BaseModel):
    """Raw independent-ledger evidence retained before aggregation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    partner: str
    order_id: str
    source_sequence: StrictInt
    currency: str | None
    settled_amount_minor: StrictInt | None

    @property
    def business_key(self) -> BusinessKey:
        return (self.partner, self.order_id, self.source_sequence)


class VerificationMetrics(BaseModel):
    """All metrics required for a fail-closed promotion decision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_rows: Count
    candidate_rows: Count
    expected_rows: Count
    expected_v3_rows: Count
    source_invalid_business_key_rows: Count
    source_unauthorized_payload_versions: Count
    source_profile_currency_mismatches: Count
    duplicate_business_events: Count
    mapping_validation_failures: Count
    payload_authorization_failures: Count
    missing_order_id_rows: Count
    missing_customer_id_rows: Count
    invalid_event_time_rows: Count
    unsupported_event_type_rows: Count
    invalid_sale_amount_rows: Count
    invalid_refund_amount_rows: Count
    candidate_profile_currency_mismatches: Count
    source_candidate_event_time_mismatches: Count
    source_candidate_event_type_mismatches: Count
    source_candidate_customer_id_mismatches: Count
    canonical_quality_failures: Count
    missing_amount_rows: Count
    missing_currency_rows: Count
    unauthorized_payload_versions: Count
    profile_digest_mismatches: Count
    profile_provenance_mismatches: Count
    dedupe_ambiguities: Count
    missing_expected_rows: Count
    unexpected_candidate_rows: Count
    unproven_expected_v3_rows: Count
    null_settlement_currency_rows: Count
    null_settlement_amount_rows: Count
    settlement_without_candidate: Count
    candidate_without_settlement: Count
    currency_mismatches: Count
    settlement_difference_minor: Count


ZERO_FIELDS = (
    "source_invalid_business_key_rows",
    "source_unauthorized_payload_versions",
    "source_profile_currency_mismatches",
    "duplicate_business_events",
    "mapping_validation_failures",
    "payload_authorization_failures",
    "missing_order_id_rows",
    "missing_customer_id_rows",
    "invalid_event_time_rows",
    "unsupported_event_type_rows",
    "invalid_sale_amount_rows",
    "invalid_refund_amount_rows",
    "candidate_profile_currency_mismatches",
    "source_candidate_event_time_mismatches",
    "source_candidate_event_type_mismatches",
    "source_candidate_customer_id_mismatches",
    "canonical_quality_failures",
    "missing_amount_rows",
    "missing_currency_rows",
    "unauthorized_payload_versions",
    "profile_digest_mismatches",
    "profile_provenance_mismatches",
    "dedupe_ambiguities",
    "missing_expected_rows",
    "unexpected_candidate_rows",
    "unproven_expected_v3_rows",
    "null_settlement_currency_rows",
    "null_settlement_amount_rows",
    "settlement_without_candidate",
    "candidate_without_settlement",
    "currency_mismatches",
)


def _strict_key(candidate: CanonicalOrder) -> BusinessKey | None:
    if candidate.order_id is None or candidate.source_sequence is None:
        return None
    return (candidate.partner, candidate.order_id, candidate.source_sequence)


def _validate_expected_key(key: object) -> None:
    if (
        type(key) is not tuple
        or len(key) != 3
        or type(key[0]) is not str
        or type(key[1]) is not str
        or type(key[2]) is not int
    ):
        raise TypeError("expected business keys must be strict (str, str, int) tuples")


def _utc_bound(value: datetime, *, name: str) -> datetime:
    """Validate and normalize one independently supplied replay bound."""

    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _missing_text(value: object) -> bool:
    """Treat NULL, non-text, empty, and whitespace-only identifiers as absent."""

    return type(value) is not str or not value.strip()


def calculate_metrics(
    *,
    expected_keys: set[BusinessKey],
    expected_v3_keys: set[BusinessKey],
    source_rows: Iterable[BronzeOrder],
    candidates: Iterable[CanonicalOrder],
    settlements: Iterable[SettlementRecord],
    required_profile_digest: str,
    required_currency_code: str,
    backfill_start: datetime,
    backfill_end: datetime,
    required_profile_version: str = "v3",
    required_profile_id: str | None = None,
    authorized_legacy_versions: Collection[str] = ("v2",),
) -> VerificationMetrics:
    """Calculate complete evidence without converting nulls into success."""

    if not expected_v3_keys.issubset(expected_keys):
        raise ValueError("expected v3 keys must be a subset of the replay scope")
    for key in expected_keys | expected_v3_keys:
        _validate_expected_key(key)
    if type(required_profile_version) is not str or not required_profile_version:
        raise ValueError("required profile version must be non-empty")
    if type(required_profile_digest) is not str or not required_profile_digest:
        raise ValueError("required profile digest must be non-empty")
    if (
        type(required_currency_code) is not str
        or len(required_currency_code) != 3
        or not required_currency_code.isalpha()
        or not required_currency_code.isupper()
    ):
        raise ValueError("required currency code must be three uppercase letters")
    if required_profile_id is not None and (
        type(required_profile_id) is not str or not required_profile_id
    ):
        raise ValueError("required profile ID must be non-empty when supplied")
    if isinstance(authorized_legacy_versions, str):
        raise TypeError("authorized legacy versions must be a collection of strings")
    legacy_versions = frozenset(authorized_legacy_versions)
    if any(type(version) is not str or not version for version in legacy_versions):
        raise TypeError("authorized legacy versions must be non-empty strings")
    if legacy_versions != {"v2"}:
        raise ValueError("the checked-in verifier and SQL support exactly legacy v2")
    if required_profile_version in legacy_versions:
        raise ValueError("required profile version cannot also be a legacy version")
    window_start = _utc_bound(backfill_start, name="backfill_start")
    window_end = _utc_bound(backfill_end, name="backfill_end")
    if window_end <= window_start:
        raise ValueError("backfill_end must be later than backfill_start")

    source_records = list(source_rows)
    candidate_rows = list(candidates)
    settlement_rows = list(settlements)
    candidate_keys = [_strict_key(row) for row in candidate_rows]
    key_counts = Counter(key for key in candidate_keys if key is not None)
    duplicate_count = sum(count - 1 for count in key_counts.values() if count > 1)

    def source_identity(row: BronzeOrder) -> tuple[object, ...]:
        return (
            row.transport_id,
            row.partner,
            row.order_id,
            row.source_sequence,
            row.payload_version,
        )

    def candidate_source_identity(row: CanonicalOrder) -> tuple[object, ...]:
        return (
            row.transport_id,
            row.partner,
            row.order_id,
            row.source_sequence,
            row.payload_version,
        )

    def rescued_string(row: BronzeOrder, parent: str, field: str) -> str | None:
        container = row.rescued_data.get(parent)
        if not isinstance(container, dict):
            return None
        value = container.get(field)
        return value if type(value) is str else None

    def reviewed_source_customer(row: BronzeOrder) -> str | None:
        if row.payload_version == required_profile_version:
            value = rescued_string(row, "buyer", "customerId")
            return value if value is not None and value.strip() else None
        if row.payload_version in legacy_versions:
            return row.customer_id
        return None

    def reviewed_source_currency(row: BronzeOrder) -> str | None:
        if row.payload_version == required_profile_version:
            return rescued_string(row, "grossAmount", "currencyCode")
        if row.payload_version in legacy_versions:
            return row.currency
        return None

    source_by_identity: dict[tuple[object, ...], list[BronzeOrder]] = defaultdict(list)
    for row in source_records:
        source_by_identity[source_identity(row)].append(row)

    def matching_source_rows(row: CanonicalOrder) -> list[BronzeOrder]:
        return source_by_identity.get(candidate_source_identity(row), [])

    # Keep one lookup only for reconciliation; duplicate_count prevents it from
    # authorizing a result when multiple candidates exist for the same key.
    by_key: dict[BusinessKey, CanonicalOrder] = {}
    for row, key in zip(candidate_rows, candidate_keys, strict=True):
        if key is not None:
            by_key.setdefault(key, row)

    actual_keys = set(by_key)
    missing_expected = expected_keys - actual_keys
    unexpected = actual_keys - expected_keys

    unproven_v3 = 0
    for key in expected_v3_keys:
        winner = by_key.get(key)
        if (
            winner is None
            or winner.payload_version != required_profile_version
            or not winner.canonical_valid
            or winner.applied_profile_digest != required_profile_digest
        ):
            unproven_v3 += 1

    null_currency = sum(row.currency is None for row in settlement_rows)
    null_amount = sum(row.settled_amount_minor is None for row in settlement_rows)

    # Aggregate only complete ledger rows. Null counters above make exclusion a
    # failing condition rather than silently treating missing evidence as zero.
    settlement_by_key: dict[BusinessKey, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in settlement_rows:
        if row.currency is not None and row.settled_amount_minor is not None:
            settlement_by_key[row.business_key][row.currency] += row.settled_amount_minor

    settlement_keys = set(settlement_by_key)
    candidate_without_settlement = len(actual_keys - settlement_keys)
    settlement_without_candidate = len(settlement_keys - actual_keys)
    currency_mismatches = 0
    settlement_difference = 0
    for key in actual_keys & settlement_keys:
        candidate = by_key[key]
        ledger_by_currency = settlement_by_key[key]
        if candidate.currency is None or candidate.currency not in ledger_by_currency:
            currency_mismatches += 1
            continue
        currency_mismatches += len(set(ledger_by_currency) - {candidate.currency})
        if candidate.order_total_minor is not None:
            settlement_difference += abs(
                candidate.order_total_minor - ledger_by_currency[candidate.currency]
            )

    def expected_provenance(row: CanonicalOrder) -> bool:
        if row.payload_version == required_profile_version:
            profile_id_matches = (
                row.applied_profile_id == required_profile_id
                if required_profile_id is not None
                else row.applied_profile_id is not None
            )
            return bool(
                row.payload_version_authorized
                and profile_id_matches
                and row.applied_profile_digest == required_profile_digest
            )
        if row.payload_version in legacy_versions:
            return bool(
                row.payload_version_authorized
                and row.applied_profile_id is None
                and row.applied_profile_digest is None
            )
        return bool(
            not row.payload_version_authorized
            and row.applied_profile_id is None
            and row.applied_profile_digest is None
        )

    def invalid_event_time(row: CanonicalOrder) -> bool:
        value = row.event_time
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            return True
        instant = value.astimezone(timezone.utc)
        return instant < window_start or instant >= window_end

    def invalid_sale_amount(row: CanonicalOrder) -> bool:
        value = row.order_total_minor
        return row.event_type == "SALE" and (type(value) is not int or value <= 0)

    def invalid_refund_amount(row: CanonicalOrder) -> bool:
        value = row.order_total_minor
        return row.event_type == "REFUND" and (type(value) is not int or value >= 0)

    return VerificationMetrics(
        source_rows=len(source_records),
        candidate_rows=len(candidate_rows),
        expected_rows=len(expected_keys),
        expected_v3_rows=len(expected_v3_keys),
        source_invalid_business_key_rows=sum(
            _missing_text(row.order_id) or type(row.source_sequence) is not int
            for row in source_records
        ),
        source_unauthorized_payload_versions=sum(
            row.payload_version != required_profile_version
            and row.payload_version not in legacy_versions
            for row in source_records
        ),
        source_profile_currency_mismatches=sum(
            row.payload_version in ({required_profile_version} | legacy_versions)
            and reviewed_source_currency(row) != required_currency_code
            for row in source_records
        ),
        duplicate_business_events=duplicate_count,
        mapping_validation_failures=sum(
            row.mapping_valid is not True for row in candidate_rows
        ),
        payload_authorization_failures=sum(
            row.payload_version_authorized is not True for row in candidate_rows
        ),
        missing_order_id_rows=sum(
            _missing_text(row.order_id) for row in candidate_rows
        ),
        missing_customer_id_rows=sum(
            _missing_text(row.customer_id) for row in candidate_rows
        ),
        invalid_event_time_rows=sum(invalid_event_time(row) for row in candidate_rows),
        unsupported_event_type_rows=sum(
            row.event_type not in {"SALE", "REFUND"} for row in candidate_rows
        ),
        invalid_sale_amount_rows=sum(
            invalid_sale_amount(row) for row in candidate_rows
        ),
        invalid_refund_amount_rows=sum(
            invalid_refund_amount(row) for row in candidate_rows
        ),
        candidate_profile_currency_mismatches=sum(
            row.currency != required_currency_code for row in candidate_rows
        ),
        source_candidate_event_time_mismatches=sum(
            not any(source.event_time == row.event_time for source in matching_source_rows(row))
            for row in candidate_rows
        ),
        source_candidate_event_type_mismatches=sum(
            not any(source.event_type == row.event_type for source in matching_source_rows(row))
            for row in candidate_rows
        ),
        source_candidate_customer_id_mismatches=sum(
            not any(
                reviewed_source_customer(source) == row.customer_id
                for source in matching_source_rows(row)
            )
            for row in candidate_rows
        ),
        canonical_quality_failures=sum(not row.canonical_valid for row in candidate_rows),
        missing_amount_rows=sum(row.order_total_minor is None for row in candidate_rows),
        missing_currency_rows=sum(row.currency is None for row in candidate_rows),
        unauthorized_payload_versions=sum(
            row.payload_version != required_profile_version
            and row.payload_version not in legacy_versions
            for row in candidate_rows
        ),
        profile_digest_mismatches=sum(
            row.payload_version == required_profile_version
            and row.applied_profile_digest != required_profile_digest
            for row in candidate_rows
        ),
        profile_provenance_mismatches=sum(
            not expected_provenance(row) for row in candidate_rows
        ),
        dedupe_ambiguities=sum(row.dedupe_ambiguous for row in candidate_rows),
        missing_expected_rows=len(missing_expected),
        unexpected_candidate_rows=len(unexpected),
        unproven_expected_v3_rows=unproven_v3,
        null_settlement_currency_rows=null_currency,
        null_settlement_amount_rows=null_amount,
        settlement_without_candidate=settlement_without_candidate,
        candidate_without_settlement=candidate_without_settlement,
        currency_mismatches=currency_mismatches,
        settlement_difference_minor=settlement_difference,
    )


def assert_verified(
    metrics: VerificationMetrics,
    *,
    maximum_rows: int,
    settlement_tolerance_minor: int,
) -> None:
    """Fail before promotion unless every required invariant is true."""

    if type(maximum_rows) is not int or maximum_rows < 1:
        raise ValueError("maximum_rows must be positive")
    if type(settlement_tolerance_minor) is not int or settlement_tolerance_minor < 0:
        raise ValueError("settlement tolerance cannot be negative")
    violations: dict[str, int | str] = {}
    if metrics.candidate_rows <= 0:
        violations["candidate_rows"] = "empty"
    if metrics.expected_v3_rows <= 0:
        violations["expected_v3_rows"] = "no v3 key to prove"
    if metrics.candidate_rows > maximum_rows:
        violations["row_budget"] = metrics.candidate_rows
    if metrics.candidate_rows != metrics.expected_rows:
        violations["coverage_count"] = metrics.candidate_rows - metrics.expected_rows
    for field in ZERO_FIELDS:
        value = getattr(metrics, field)
        if value != 0:
            violations[field] = value
    if metrics.settlement_difference_minor > settlement_tolerance_minor:
        violations["settlement_difference_minor"] = metrics.settlement_difference_minor
    if violations:
        raise VerificationFailed(f"candidate verification failed: {violations}")


def verify_candidate(
    *,
    expected_keys: set[BusinessKey],
    expected_v3_keys: set[BusinessKey],
    source_rows: Iterable[BronzeOrder],
    candidates: Iterable[CanonicalOrder],
    settlements: Iterable[SettlementRecord],
    required_profile_digest: str,
    required_currency_code: str,
    backfill_start: datetime,
    backfill_end: datetime,
    maximum_rows: int,
    settlement_tolerance_minor: int = 0,
    required_profile_version: str = "v3",
    required_profile_id: str | None = None,
    authorized_legacy_versions: Collection[str] = ("v2",),
) -> VerificationMetrics:
    """Calculate metrics, assert them, and return the auditable result."""

    metrics = calculate_metrics(
        expected_keys=expected_keys,
        expected_v3_keys=expected_v3_keys,
        source_rows=source_rows,
        candidates=candidates,
        settlements=settlements,
        required_profile_digest=required_profile_digest,
        required_currency_code=required_currency_code,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
        required_profile_version=required_profile_version,
        required_profile_id=required_profile_id,
        authorized_legacy_versions=authorized_legacy_versions,
    )
    assert_verified(
        metrics,
        maximum_rows=maximum_rows,
        settlement_tolerance_minor=settlement_tolerance_minor,
    )
    return metrics
