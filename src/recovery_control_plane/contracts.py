"""Typed repair IR shared by the reasoning and policy layers."""

from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator


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
    maximum_rows: int = Field(strict=True, gt=0, le=500_000)

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
    generation: int = Field(strict=True, ge=1)
    detected_at: datetime
    pipeline_id: Identifier
    dataset: Identifier
    source_table: Identifier
    source_delta_version: int = Field(ge=0, strict=True)
    partner: Identifier
    payload_version: Identifier
    source_fingerprint: Identifier
    native_retry_exhausted: bool = Field(strict=True)
    affected_start: datetime
    affected_end: datetime
    evidence_expires_at: datetime
    source_replayable_from: datetime
    source_replayable_until: datetime
    quarantine_ratio: float = Field(strict=True, ge=0, le=1, allow_inf_nan=False)
    estimated_rows: int = Field(strict=True, ge=0)
    estimated_dbus: float = Field(strict=True, ge=0, allow_inf_nan=False)
    downstream_assets: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=50)
    candidate_profile_ids: tuple[Identifier, ...] = Field(default_factory=tuple, max_length=20)
    evidence_event_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=50)
    redacted_samples: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=10)
    redaction_attested: Literal[True]
    business_checks: Mapping[str, float | int | str | bool] = Field(
        default_factory=dict,
        max_length=20,
    )

    @field_validator("redaction_attested", mode="before")
    @classmethod
    def require_explicit_redaction_attestation(cls, value: object) -> object:
        """Reject integer or string coercion at the privacy trust boundary."""

        if value is not True:
            raise ValueError("redaction_attested must be the literal boolean true")
        return value

    @field_validator("redacted_samples")
    @classmethod
    def reject_obvious_unredacted_samples(
        cls,
        values: tuple[str, ...],
    ) -> tuple[str, ...]:
        obvious_pii = re.compile(
            r"(?:[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})|(?:\b(?:\d[ -]?){13,19}\b)",
            re.IGNORECASE,
        )
        if any(obvious_pii.search(value) for value in values):
            raise ValueError("evidence samples contain obvious unredacted identifiers")
        return values

    @field_validator("business_checks", mode="after")
    @classmethod
    def freeze_business_checks(
        cls,
        value: Mapping[str, float | int | str | bool],
    ) -> Mapping[str, float | int | str | bool]:
        """Prevent callers from mutating evidence after validation."""

        for key, item in value.items():
            if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", key) is None:
                raise ValueError("business-check keys must be bounded identifiers")
            if isinstance(item, str) and len(item) > 120:
                raise ValueError("business-check string values are too long")
            if isinstance(item, (int, float)) and not isinstance(item, bool):
                if not (-1_000_000_000_000_000 <= item <= 1_000_000_000_000_000):
                    raise ValueError("business-check numeric value is out of bounds")
        return MappingProxyType(dict(value))

    @field_serializer("business_checks")
    def serialize_business_checks(
        self,
        value: Mapping[str, float | int | str | bool],
    ) -> dict[str, float | int | str | bool]:
        return dict(value)

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
    confidence: float = Field(strict=True, ge=0, le=1, allow_inf_nan=False)
    evidence_event_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=30)
    rationale: str = Field(min_length=20, max_length=1_200)
    disconfirming_evidence: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=10)


class IntentReview(BaseModel):
    """A challenger result whose ACCEPT state cannot retain blockers."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    verdict: Literal["ACCEPT", "REVISE", "ESCALATE"]
    blocking_findings: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=10)
    requested_revision: str | None = Field(default=None, max_length=800)

    @model_validator(mode="after")
    def validate_acceptance(self) -> "IntentReview":
        if self.verdict == "ACCEPT" and self.blocking_findings:
            raise ValueError("an accepted review cannot contain blockers")
        return self
