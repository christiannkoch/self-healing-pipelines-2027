"""Failure Passport assembly behind read-only evidence protocols."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Literal, Mapping, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from .contracts import IncidentEvidence


class EvidenceSnapshot(BaseModel):
    """Already-aggregated facts returned by governed, read-only queries."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_table: str
    source_delta_version: int = Field(ge=0, strict=True)
    source_fingerprint: str
    native_retry_exhausted: bool = Field(strict=True)
    affected_start: datetime
    affected_end: datetime
    source_replayable_from: datetime
    source_replayable_until: datetime
    quarantine_ratio: float = Field(strict=True, ge=0, le=1, allow_inf_nan=False)
    estimated_rows: int = Field(strict=True, ge=0)
    estimated_dbus: float = Field(strict=True, ge=0, allow_inf_nan=False)
    downstream_assets: tuple[str, ...] = Field(default_factory=tuple)
    candidate_profile_ids: tuple[str, ...] = Field(default_factory=tuple)
    evidence_event_ids: tuple[str, ...] = Field(min_length=1)
    redacted_samples: tuple[str, ...] = Field(default_factory=tuple)
    redaction_attested: Literal[True]
    business_checks: Mapping[str, float | int | str | bool] = Field(
        default_factory=dict,
        max_length=20,
    )

    @field_validator("redaction_attested", mode="before")
    @classmethod
    def require_explicit_redaction_attestation(cls, value: object) -> object:
        """Reject integer or string coercion at the repository boundary."""

        if value is not True:
            raise ValueError("redaction_attested must be the literal boolean true")
        return value

    @field_validator("business_checks", mode="after")
    @classmethod
    def freeze_business_checks(
        cls,
        value: Mapping[str, float | int | str | bool],
    ) -> Mapping[str, float | int | str | bool]:
        return MappingProxyType(dict(value))

    @field_serializer("business_checks")
    def serialize_business_checks(
        self,
        value: Mapping[str, float | int | str | bool],
    ) -> dict[str, float | int | str | bool]:
        return dict(value)


class EvidenceRepository(Protocol):
    """Read-only port whose implementation must redact samples before return."""

    def snapshot(self, *, partner: str, payload_version: str) -> EvidenceSnapshot:
        """Return one bounded snapshot without exposing unrestricted source rows."""


class Clock(Protocol):
    def now(self) -> datetime:
        """Return a timezone-aware current instant."""


class UtcClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class StaticEvidenceRepository:
    """Offline repository used by examples and tests."""

    def __init__(self, snapshot: EvidenceSnapshot) -> None:
        self._snapshot = snapshot

    def snapshot(self, *, partner: str, payload_version: str) -> EvidenceSnapshot:
        del partner, payload_version
        return self._snapshot


def _bounded(values: Sequence[str], limit: int) -> tuple[str, ...]:
    """Sort and cap evidence so model input is stable and size-limited."""

    return tuple(sorted(dict.fromkeys(values))[:limit])


def build_failure_passport(
    repository: EvidenceRepository,
    *,
    incident_id: str,
    generation: int,
    pipeline_id: str,
    dataset: str,
    partner: str,
    payload_version: str,
    clock: Clock | None = None,
    ttl: timedelta = timedelta(minutes=30),
) -> IncidentEvidence:
    """Build a bounded passport from deterministic evidence queries."""

    current_clock = clock or UtcClock()
    now = current_clock.now()
    if now.tzinfo is None:
        raise ValueError("the evidence clock must return a timezone-aware instant")

    snapshot = repository.snapshot(partner=partner, payload_version=payload_version)
    return IncidentEvidence(
        incident_id=incident_id,
        generation=generation,
        detected_at=now,
        pipeline_id=pipeline_id,
        dataset=dataset,
        source_table=snapshot.source_table,
        source_delta_version=snapshot.source_delta_version,
        partner=partner,
        payload_version=payload_version,
        source_fingerprint=snapshot.source_fingerprint,
        native_retry_exhausted=snapshot.native_retry_exhausted,
        affected_start=snapshot.affected_start,
        affected_end=snapshot.affected_end,
        evidence_expires_at=now + ttl,
        source_replayable_from=snapshot.source_replayable_from,
        source_replayable_until=snapshot.source_replayable_until,
        quarantine_ratio=snapshot.quarantine_ratio,
        estimated_rows=snapshot.estimated_rows,
        estimated_dbus=snapshot.estimated_dbus,
        downstream_assets=_bounded(snapshot.downstream_assets, 50),
        candidate_profile_ids=_bounded(snapshot.candidate_profile_ids, 20),
        evidence_event_ids=_bounded(snapshot.evidence_event_ids, 50),
        redacted_samples=_bounded(snapshot.redacted_samples, 10),
        redaction_attested=snapshot.redaction_attested,
        business_checks=snapshot.business_checks,
    )
