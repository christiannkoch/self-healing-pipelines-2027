"""Failure Passport assembly behind read-only evidence protocols."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field

from .contracts import IncidentEvidence


class EvidenceSnapshot(BaseModel):
    """Already-aggregated facts returned by governed, read-only queries."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_fingerprint: str
    native_retry_exhausted: bool
    affected_start: datetime
    affected_end: datetime
    source_replayable_from: datetime
    source_replayable_until: datetime
    quarantine_ratio: float = Field(ge=0, le=1)
    estimated_rows: int = Field(ge=0)
    estimated_dbus: float = Field(ge=0)
    downstream_assets: list[str] = Field(default_factory=list)
    candidate_profile_ids: list[str] = Field(default_factory=list)
    evidence_event_ids: list[str] = Field(min_length=1)
    redacted_samples: list[str] = Field(default_factory=list)
    business_checks: dict[str, float | int | str | bool] = Field(default_factory=dict)


class EvidenceRepository(Protocol):
    """Read-only port implemented by SQL, fixtures, or another evidence store."""

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


def _bounded(values: Sequence[str], limit: int) -> list[str]:
    """Sort and cap evidence so model input is stable and size-limited."""

    return sorted(dict.fromkeys(values))[:limit]


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
        business_checks=snapshot.business_checks,
    )

