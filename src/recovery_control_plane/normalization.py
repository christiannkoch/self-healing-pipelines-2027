"""Pure-Python normalization and mixed-version deduplication.

This module mirrors the Spark transformation semantics from the article while
remaining small enough to unit-test without Spark or a Databricks workspace.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from enum import Enum
import hashlib
import json
from typing import Any, Callable, Iterable

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from .profiles import (
    MARKETPLACE_V3_NORMALIZER_KEY,
    MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST,
    MARKETPLACE_V3_NORMALIZER_VERSION,
    MappingProfile,
)


LONG_MIN = -(2**63)
LONG_MAX = 2**63 - 1


class NormalizationError(ValueError):
    """Raised when a money representation cannot be converted exactly."""


class EventType(str, Enum):
    SALE = "SALE"
    REFUND = "REFUND"


class BronzeOrder(BaseModel):
    """Minimal source record retained by the evidence-preserving data plane."""

    model_config = ConfigDict(extra="allow", frozen=True)

    transport_id: str
    partner: str
    payload_version: str
    order_id: str | None
    source_sequence: StrictInt | None
    event_type: str | None
    event_time: datetime | None
    ingested_at: datetime
    customer_id: str | None = None
    order_total_minor: StrictInt | None = None
    currency: str | None = None
    rescued_data: dict[str, Any] = Field(default_factory=dict)


class CanonicalOrder(BaseModel):
    """A candidate row plus explicit validity and provenance evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transport_id: str
    partner: str
    order_id: str | None
    source_sequence: StrictInt | None
    payload_version: str
    event_type: str | None
    event_time: datetime | None
    ingested_at: datetime
    customer_id: str | None
    order_total_minor: StrictInt | None
    currency: str | None
    mapping_valid: bool = Field(strict=True)
    canonical_valid: bool = Field(strict=True)
    payload_version_authorized: bool = Field(strict=True)
    applied_profile_id: str | None
    applied_profile_digest: str | None
    dedupe_ambiguous: bool = Field(default=False, strict=True)

    @property
    def business_key(self) -> tuple[str, str | None, int | None]:
        return (self.partner, self.order_id, self.source_sequence)


def units_nanos_to_minor(units: int, nanos: int, currency_exponent: int) -> int:
    """Convert protobuf-style units/nanos to exact integral minor units.

    The function refuses mixed signs, out-of-range nanos, sub-minor-unit values,
    and signed 64-bit overflow. It never silently rounds accounting data.
    """

    # ``bool`` is a subclass of ``int`` in Python. Reject it (and coercible
    # strings/floats) explicitly so malformed wire data cannot become money.
    if type(units) is not int or type(nanos) is not int or type(currency_exponent) is not int:
        raise NormalizationError("money components must be strict integers")
    if not 0 <= currency_exponent <= 9:
        raise NormalizationError("currency exponent must be between 0 and 9")
    if abs(nanos) >= 1_000_000_000:
        raise NormalizationError("nanos must be strictly within one unit")
    if units > 0 and nanos < 0 or units < 0 and nanos > 0:
        raise NormalizationError("units and nanos must have compatible signs")

    minor_per_unit = 10**currency_exponent
    numerator = nanos * minor_per_unit
    if numerator % 1_000_000_000 != 0:
        raise NormalizationError("amount has precision below one minor unit")

    value = units * minor_per_unit + numerator // 1_000_000_000
    if not LONG_MIN <= value <= LONG_MAX:
        raise NormalizationError("minor-unit amount exceeds signed 64-bit range")
    return value


def _v3_fields(
    row: BronzeOrder,
    profile: MappingProfile,
) -> tuple[str | None, int | None, str | None, bool]:
    """Read the reviewed v3 JSON shape without coercing malformed scalars."""

    gross_value = row.rescued_data.get("grossAmount")
    buyer_value = row.rescued_data.get("buyer")
    gross = gross_value if isinstance(gross_value, dict) else {}
    buyer = buyer_value if isinstance(buyer_value, dict) else {}
    raw_customer = buyer.get("customerId")
    raw_currency = gross.get("currencyCode")
    customer = (
        raw_customer
        if type(raw_customer) is str and bool(raw_customer.strip())
        else None
    )
    currency = raw_currency if type(raw_currency) is str else None

    try:
        amount = units_nanos_to_minor(
            gross["units"],
            gross["nanos"],
            profile.currency_exponent,
        )
    except (KeyError, TypeError, ValueError, NormalizationError):
        amount = None

    valid = bool(
        row.partner == profile.partner
        and row.payload_version == profile.payload_version
        and currency == profile.currency_code
        and customer is not None
        and amount is not None
    )
    return customer, amount, currency, valid


Normalizer = Callable[
    [BronzeOrder, MappingProfile],
    tuple[str | None, int | None, str | None, bool],
]
_NORMALIZER_REGISTRY: dict[tuple[str, str, str], Normalizer] = {
    (
        MARKETPLACE_V3_NORMALIZER_KEY,
        MARKETPLACE_V3_NORMALIZER_VERSION,
        MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST,
    ): _v3_fields
}


def _resolve_normalizer(profile: MappingProfile) -> Normalizer:
    identity = (
        profile.normalizer_key,
        profile.normalizer_version,
        profile.normalizer_source_digest,
    )
    try:
        return _NORMALIZER_REGISTRY[identity]
    except KeyError as exc:  # Defensive even if the profile was model-validated.
        raise NormalizationError("mapping profile does not name a shipped normalizer") from exc


def normalize_order(row: BronzeOrder, profile: MappingProfile) -> CanonicalOrder:
    """Normalize one record without mutating source evidence."""

    is_profile_version = (
        row.partner == profile.partner and row.payload_version == profile.payload_version
    )
    is_legacy_version = (
        row.partner == profile.partner
        and row.payload_version in profile.legacy_payload_versions
    )
    if is_profile_version:
        normalizer = _resolve_normalizer(profile)
        customer, amount, currency, mapping_valid = normalizer(row, profile)
        applied_profile_id = profile.profile_id if mapping_valid else None
        applied_profile_digest = profile.profile_digest if mapping_valid else None
    elif is_legacy_version:
        customer = row.customer_id
        amount = row.order_total_minor
        currency = row.currency
        mapping_valid = bool(
            type(customer) is str
            and customer.strip()
            and amount is not None
            and currency == profile.currency_code
        )
        applied_profile_id = None
        applied_profile_digest = None
    else:
        # Unknown versions and out-of-profile partners retain enough content for
        # quarantine diagnostics, but they can never become canonical output.
        customer = row.customer_id
        amount = row.order_total_minor
        currency = row.currency
        mapping_valid = False
        applied_profile_id = None
        applied_profile_digest = None

    payload_version_authorized = is_profile_version or is_legacy_version

    event_shape_valid = bool(
        (row.event_type == EventType.SALE and amount is not None and amount > 0)
        or (row.event_type == EventType.REFUND and amount is not None and amount < 0)
    )
    canonical_valid = bool(
        mapping_valid
        and payload_version_authorized
        and type(row.order_id) is str
        and bool(row.order_id.strip())
        and row.source_sequence is not None
        and row.event_time is not None
        and customer is not None
        and currency is not None
        and event_shape_valid
    )

    return CanonicalOrder(
        transport_id=row.transport_id,
        partner=row.partner,
        order_id=row.order_id,
        source_sequence=row.source_sequence,
        payload_version=row.payload_version,
        event_type=row.event_type,
        event_time=row.event_time,
        ingested_at=row.ingested_at,
        customer_id=customer,
        order_total_minor=amount,
        currency=currency,
        mapping_valid=mapping_valid,
        canonical_valid=canonical_valid,
        payload_version_authorized=payload_version_authorized,
        applied_profile_id=applied_profile_id,
        applied_profile_digest=applied_profile_digest,
    )


def _operational_rank(
    row: CanonicalOrder,
    profile: MappingProfile,
) -> tuple[int, int, datetime, str]:
    """Rank reviewed candidates before the content-level ambiguity check."""

    return (
        int(row.canonical_valid),
        int(row.payload_version == profile.payload_version),
        row.ingested_at,
        row.transport_id,
    )


def _content_digest(row: CanonicalOrder) -> str:
    """Return a stable final ordering key for otherwise tied candidates."""

    payload = row.model_dump(mode="json", exclude={"dedupe_ambiguous"})
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dedupe_value(row: CanonicalOrder, profile: MappingProfile) -> tuple[Any, ...]:
    """Materialize the profile's signed dedupe key from canonical fields."""

    return tuple(getattr(row, field) for field in profile.dedupe_key)


def _pick_winner(
    candidates: list[CanonicalOrder],
    profile: MappingProfile,
) -> CanonicalOrder:
    best_rank = max(_operational_rank(item, profile) for item in candidates)
    finalists = [
        item for item in candidates if _operational_rank(item, profile) == best_rank
    ]
    by_digest = {_content_digest(item): item for item in finalists}
    winner = by_digest[max(by_digest)]
    if len(by_digest) > 1:
        # The operational evidence cannot distinguish conflicting deliveries.
        # Select a stable diagnostic row, mark it invalid, and force independent
        # verification to reject promotion instead of trusting input order.
        return winner.model_copy(
            update={"canonical_valid": False, "dedupe_ambiguous": True}
        )
    return winner


def normalize_and_dedupe(
    rows: Iterable[BronzeOrder],
    profile: MappingProfile,
) -> list[CanonicalOrder]:
    """Choose one deterministic candidate per stable business key."""

    grouped: dict[tuple[Any, ...], list[CanonicalOrder]] = defaultdict(list)
    for row in rows:
        candidate = normalize_order(row, profile)
        grouped[_dedupe_value(candidate, profile)].append(candidate)

    winners = [_pick_winner(candidates, profile) for candidates in grouped.values()]
    return sorted(
        winners,
        key=lambda item: tuple(str(part) for part in _dedupe_value(item, profile)),
    )
