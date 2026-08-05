"""Independent, fail-closed verification for a bounded repair candidate."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Annotated, Iterable, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

from .normalization import CanonicalOrder


BusinessKey: TypeAlias = tuple[str, str, int]
Count: TypeAlias = Annotated[int, Field(ge=0, strict=True)]


class VerificationFailed(RuntimeError):
    """Raised when candidate evidence is incomplete or violates an invariant."""


class SettlementRecord(BaseModel):
    """Raw independent-ledger evidence retained before aggregation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    partner: str
    order_id: str
    source_sequence: int
    currency: str | None
    settled_amount_minor: int | None

    @property
    def business_key(self) -> BusinessKey:
        return (self.partner, self.order_id, self.source_sequence)


class VerificationMetrics(BaseModel):
    """All metrics required for a fail-closed promotion decision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_rows: Count
    expected_rows: Count
    expected_v3_rows: Count
    duplicate_business_events: Count
    canonical_quality_failures: Count
    missing_amount_rows: Count
    missing_currency_rows: Count
    profile_digest_mismatches: Count
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
    "duplicate_business_events",
    "canonical_quality_failures",
    "missing_amount_rows",
    "missing_currency_rows",
    "profile_digest_mismatches",
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


def calculate_metrics(
    *,
    expected_keys: set[BusinessKey],
    expected_v3_keys: set[BusinessKey],
    candidates: Iterable[CanonicalOrder],
    settlements: Iterable[SettlementRecord],
    required_profile_digest: str,
) -> VerificationMetrics:
    """Calculate complete evidence without converting nulls into success."""

    if not expected_v3_keys.issubset(expected_keys):
        raise ValueError("expected v3 keys must be a subset of the replay scope")

    candidate_rows = list(candidates)
    settlement_rows = list(settlements)
    candidate_keys = [_strict_key(row) for row in candidate_rows]
    key_counts = Counter(key for key in candidate_keys if key is not None)
    duplicate_count = sum(count - 1 for count in key_counts.values() if count > 1)

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
            or winner.payload_version != "v3"
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

    return VerificationMetrics(
        candidate_rows=len(candidate_rows),
        expected_rows=len(expected_keys),
        expected_v3_rows=len(expected_v3_keys),
        duplicate_business_events=duplicate_count,
        canonical_quality_failures=sum(not row.canonical_valid for row in candidate_rows),
        missing_amount_rows=sum(row.order_total_minor is None for row in candidate_rows),
        missing_currency_rows=sum(row.currency is None for row in candidate_rows),
        profile_digest_mismatches=sum(
            row.payload_version == "v3"
            and row.applied_profile_digest != required_profile_digest
            for row in candidate_rows
        ),
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

    if maximum_rows < 1:
        raise ValueError("maximum_rows must be positive")
    if settlement_tolerance_minor < 0:
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
    candidates: Iterable[CanonicalOrder],
    settlements: Iterable[SettlementRecord],
    required_profile_digest: str,
    maximum_rows: int,
    settlement_tolerance_minor: int = 0,
) -> VerificationMetrics:
    """Calculate metrics, assert them, and return the auditable result."""

    metrics = calculate_metrics(
        expected_keys=expected_keys,
        expected_v3_keys=expected_v3_keys,
        candidates=candidates,
        settlements=settlements,
        required_profile_digest=required_profile_digest,
    )
    assert_verified(
        metrics,
        maximum_rows=maximum_rows,
        settlement_tolerance_minor=settlement_tolerance_minor,
    )
    return metrics
