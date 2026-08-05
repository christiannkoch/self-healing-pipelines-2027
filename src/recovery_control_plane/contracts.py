"""Typed repair IR shared by the reasoning and policy layers."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


ShortText = Annotated[str, Field(min_length=1, max_length=240)]
Identifier = Annotated[str, Field(min_length=1, max_length=200)]


class RepairAction(str, Enum):
    """Closed vocabulary available to the reasoning layer."""

    KEEP_QUARANTINED = "KEEP_QUARANTINED"
    ACTIVATE_MAPPING_PROFILE = "ACTIVATE_MAPPING_PROFILE"
    ESCALATE = "ESCALATE"


class BackfillWindow(BaseModel):
    """Explicit, timezone-aware bound for a historical correction."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    start: datetime
    end: datetime
    maximum_rows: int = Field(gt=0, le=500_000)

    @model_validator(mode="after")
    def validate_interval(self) -> "BackfillWindow":
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("backfill timestamps must include a timezone")
        if self.end <= self.start:
            raise ValueError("backfill end must be after start")
        return self


class IncidentEvidence(BaseModel):
    """A bounded, redacted Failure Passport assembled without a model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    incident_id: Identifier
    generation: int = Field(ge=1)
    detected_at: datetime
    pipeline_id: Identifier
    dataset: Identifier
    partner: Identifier
    payload_version: Identifier
    source_fingerprint: Identifier
    native_retry_exhausted: bool
    affected_start: datetime
    affected_end: datetime
    evidence_expires_at: datetime
    source_replayable_from: datetime
    source_replayable_until: datetime
    quarantine_ratio: float = Field(ge=0, le=1)
    estimated_rows: int = Field(ge=0)
    estimated_dbus: float = Field(ge=0)
    downstream_assets: list[Identifier] = Field(default_factory=list, max_length=50)
    candidate_profile_ids: list[Identifier] = Field(default_factory=list, max_length=20)
    evidence_event_ids: list[Identifier] = Field(min_length=1, max_length=50)
    redacted_samples: list[ShortText] = Field(default_factory=list, max_length=10)
    business_checks: dict[str, float | int | str | bool] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_temporal_bounds(self) -> "IncidentEvidence":
        instants = (
            self.detected_at,
            self.affected_start,
            self.affected_end,
            self.evidence_expires_at,
            self.source_replayable_from,
            self.source_replayable_until,
        )
        if any(value.tzinfo is None for value in instants):
            raise ValueError("all evidence timestamps must include a timezone")
        if self.affected_end <= self.affected_start:
            raise ValueError("affected evidence window is invalid")
        if self.source_replayable_until <= self.source_replayable_from:
            raise ValueError("source replay interval is invalid")
        return self


class RepairIntent(BaseModel):
    """A proposal with no SQL, code, object path, or credential field."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["repair-intent/v1"] = "repair-intent/v1"
    incident_id: Identifier
    action: RepairAction
    target_dataset: Identifier
    source_fingerprint: Identifier
    mapping_profile_id: Identifier | None = None
    backfill_window: BackfillWindow | None = None
    confidence: float = Field(ge=0, le=1)
    evidence_event_ids: list[Identifier] = Field(min_length=1, max_length=30)
    rationale: str = Field(min_length=20, max_length=1_200)
    disconfirming_evidence: list[ShortText] = Field(default_factory=list, max_length=10)


class IntentReview(BaseModel):
    """A challenger result whose ACCEPT state cannot retain blockers."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    verdict: Literal["ACCEPT", "REVISE", "ESCALATE"]
    blocking_findings: list[ShortText] = Field(default_factory=list, max_length=10)
    requested_revision: str | None = Field(default=None, max_length=800)

    @model_validator(mode="after")
    def validate_acceptance(self) -> "IntentReview":
        if self.verdict == "ACCEPT" and self.blocking_findings:
            raise ValueError("an accepted review cannot contain blockers")
        return self

