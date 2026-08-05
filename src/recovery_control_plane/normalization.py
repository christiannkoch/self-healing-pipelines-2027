"""Pure-Python normalization and mixed-version deduplication.

This module mirrors the Spark transformation semantics from the article while
remaining small enough to unit-test without Spark or a Databricks workspace.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field

from .profiles import MappingProfile


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
    source_sequence: int | None
    event_type: str | None
    event_time: datetime | None
    ingested_at: datetime
    customer_id: str | None = None
    order_total_minor: int | None = None
    currency: str | None = None
    rescued_data: dict[str, Any] = Field(default_factory=dict)


class CanonicalOrder(BaseModel):
    """A candidate row plus explicit validity and provenance evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transport_id: str
    partner: str
    order_id: str | None
    source_sequence: int | None
    payload_version: str
    event_type: str | None
    event_time: datetime | None
    ingested_at: datetime
    customer_id: str | None
    order_total_minor: int | None
    currency: str | None
    mapping_valid: bool
    canonical_valid: bool
    applied_profile_id: str | None
    applied_profile_digest: str | None

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


def _v3_fields(row: BronzeOrder, profile: MappingProfile) -> tuple[str | None, int | None, str | None, bool]:
    gross_value = row.rescued_data.get("grossAmount")
    buyer_value = row.rescued_data.get("buyer")
    gross = gross_value if isinstance(gross_value, dict) else {}
    buyer = buyer_value if isinstance(buyer_value, dict) else {}
    customer = buyer.get("customerId")
    currency = gross.get("currencyCode")

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
        and customer
        and amount is not None
    )
    return customer, amount, currency, valid


def normalize_order(row: BronzeOrder, profile: MappingProfile) -> CanonicalOrder:
    """Normalize one record without mutating source evidence."""

    is_profile_version = (
        row.partner == profile.partner and row.payload_version == profile.payload_version
    )
    if is_profile_version:
        customer, amount, currency, mapping_valid = _v3_fields(row, profile)
        applied_profile_id = profile.profile_id if mapping_valid else None
        applied_profile_digest = profile.profile_digest if mapping_valid else None
    else:
        customer = row.customer_id
        amount = row.order_total_minor
        currency = row.currency
        mapping_valid = customer is not None and amount is not None and currency is not None
        applied_profile_id = None
        applied_profile_digest = None

    event_shape_valid = bool(
        (row.event_type == EventType.SALE and amount is not None and amount > 0)
        or (row.event_type == EventType.REFUND and amount is not None and amount < 0)
    )
    canonical_valid = bool(
        mapping_valid
        and row.order_id is not None
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
        applied_profile_id=applied_profile_id,
        applied_profile_digest=applied_profile_digest,
    )


def _rank(row: CanonicalOrder, profile: MappingProfile) -> tuple[int, int, datetime, str]:
    """Return a deterministic winner rank; transport ID breaks exact-time ties."""

    return (
        int(row.canonical_valid),
        int(row.payload_version == profile.payload_version),
        row.ingested_at,
        row.transport_id,
    )


def normalize_and_dedupe(
    rows: Iterable[BronzeOrder],
    profile: MappingProfile,
) -> list[CanonicalOrder]:
    """Choose one deterministic candidate per stable business key."""

    grouped: dict[tuple[str, str | None, int | None], list[CanonicalOrder]] = defaultdict(list)
    for row in rows:
        candidate = normalize_order(row, profile)
        grouped[candidate.business_key].append(candidate)

    winners = [max(candidates, key=lambda item: _rank(item, profile)) for candidates in grouped.values()]
    return sorted(winners, key=lambda item: tuple(str(part) for part in item.business_key))
