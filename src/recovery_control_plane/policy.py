"""Deterministic compiler from probabilistic intent to executable authority."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Mapping

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

from .authority import AuthoritySeal, PlanAuthoritySigner
from .contracts import IncidentEvidence, RepairAction, RepairIntent


class PolicyDenied(RuntimeError):
    """Raised when an intent falls outside the current recovery contract."""


class ApprovedProfile(BaseModel):
    """Authority metadata loaded from a reviewed registry."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str
    profile_digest: str
    pipeline_id: str
    pipeline_deployment: str
    dataset: str
    candidate_dataset: str
    source_table: str
    partner: str = Field(pattern=r"^[a-z0-9_]{1,64}$")
    payload_version: str
    authorized_legacy_versions: tuple[str, ...] = ("v2",)
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")
    settlement_relation: str = Field(min_length=1, max_length=300)
    source_fingerprint: str
    recovery_job_id: int = Field(strict=True, gt=0)
    refresh_selection: tuple[str, ...] = Field(min_length=1)
    target_predicate_dimensions: tuple[str, ...] = ("partner", "event_time")
    maximum_auto_rows: int = Field(strict=True, gt=0)
    settlement_tolerance_minor: int = Field(default=0, strict=True, ge=0)
    approved: bool = Field(strict=True)
    auto_eligible: bool = Field(strict=True)
    semantic_attestation_required: bool = Field(strict=True)

    @model_validator(mode="after")
    def validate_version_allowlist(self) -> "ApprovedProfile":
        if self.candidate_dataset == self.dataset:
            raise ValueError("candidate dataset must be separate from the production dataset")
        if self.authorized_legacy_versions != ("v2",):
            raise ValueError("the shipped recovery profile supports exactly legacy v2")
        if self.payload_version in self.authorized_legacy_versions:
            raise ValueError("profile version cannot also be an authorized legacy version")
        if len(set(self.authorized_legacy_versions)) != len(
            self.authorized_legacy_versions
        ):
            raise ValueError("authorized legacy versions must be unique")
        return self


class RecoveryPolicy(BaseModel):
    """Versioned blast-radius and approval policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: str
    valid_until: datetime
    maximum_auto_quarantine_ratio: float = Field(
        strict=True,
        ge=0,
        le=1,
        allow_inf_nan=False,
    )
    maximum_auto_rows: int = Field(strict=True, gt=0)
    maximum_auto_dbus: float = Field(strict=True, gt=0, allow_inf_nan=False)
    maximum_downstream_assets: int = Field(strict=True, ge=0)
    maximum_replay_duration: timedelta = Field(strict=True, gt=timedelta(0))
    profiles: Mapping[str, ApprovedProfile]

    @field_validator("profiles", mode="after")
    @classmethod
    def validate_and_freeze_registry(
        cls,
        value: Mapping[str, ApprovedProfile],
    ) -> Mapping[str, ApprovedProfile]:
        for registry_key, profile in value.items():
            if registry_key != profile.profile_id:
                raise ValueError(
                    "profile registry key must equal the embedded profile_id"
                )
        return MappingProxyType(dict(value))

    @field_serializer("profiles")
    def serialize_profiles(
        self,
        value: Mapping[str, ApprovedProfile],
    ) -> dict[str, ApprovedProfile]:
        return dict(value)


class CompiledPlan(BaseModel):
    """The only type accepted by a recovery actuator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    action_id: str = Field(pattern=r"^rpr-[0-9a-f]{60}$")
    incident_id: str
    generation: int = Field(strict=True, ge=1)
    job_id: int = Field(strict=True, gt=0)
    pipeline_id: str
    profile_id: str
    profile_digest: str
    pipeline_deployment: str
    target_dataset: str
    candidate_dataset: str
    source_table: str
    source_delta_version: int = Field(ge=0, strict=True)
    partner: str
    payload_version: str
    authorized_legacy_versions: tuple[str, ...]
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")
    settlement_relation: str = Field(min_length=1, max_length=300)
    source_fingerprint: str
    refresh_selection: tuple[str, ...]
    target_predicate_dimensions: tuple[str, ...]
    backfill_start: datetime
    backfill_end: datetime
    maximum_rows: int = Field(strict=True, gt=0)
    estimated_dbus: float = Field(strict=True, ge=0, allow_inf_nan=False)
    policy_version: str
    policy_valid_until: datetime
    evidence_expires_at: datetime
    execution_deadline: datetime
    settlement_tolerance_minor: int = Field(strict=True, ge=0)
    requires_human: bool = Field(strict=True)
    authority_seal: AuthoritySeal

    def authority_payload(self) -> dict[str, object]:
        """Return every authority-bearing field covered by ``action_id``."""

        return {
            "incident_id": self.incident_id,
            "generation": self.generation,
            "policy_version": self.policy_version,
            "policy_valid_until": self.policy_valid_until.astimezone(timezone.utc).isoformat(),
            "evidence_expires_at": self.evidence_expires_at.astimezone(timezone.utc).isoformat(),
            "execution_deadline": self.execution_deadline.astimezone(timezone.utc).isoformat(),
            "profile_id": self.profile_id,
            "profile_digest": self.profile_digest,
            "pipeline_id": self.pipeline_id,
            "pipeline_deployment": self.pipeline_deployment,
            "recovery_job_id": self.job_id,
            "target_dataset": self.target_dataset,
            "candidate_dataset": self.candidate_dataset,
            "source_table": self.source_table,
            "source_delta_version": self.source_delta_version,
            "partner": self.partner,
            "payload_version": self.payload_version,
            "authorized_legacy_versions": self.authorized_legacy_versions,
            "currency_code": self.currency_code,
            "settlement_relation": self.settlement_relation,
            "source_fingerprint": self.source_fingerprint,
            "refresh_selection": self.refresh_selection,
            "predicate_dimensions": self.target_predicate_dimensions,
            "window_start": self.backfill_start.astimezone(timezone.utc).isoformat(),
            "window_end": self.backfill_end.astimezone(timezone.utc).isoformat(),
            "maximum_rows": self.maximum_rows,
            "estimated_dbus": self.estimated_dbus,
            "settlement_tolerance_minor": self.settlement_tolerance_minor,
            "requires_human": self.requires_human,
        }

    @model_validator(mode="after")
    def validate_authority_window(self) -> "CompiledPlan":
        timestamps = (
            self.backfill_start,
            self.backfill_end,
            self.policy_valid_until,
            self.evidence_expires_at,
            self.execution_deadline,
        )
        if any(value.tzinfo is None for value in timestamps):
            raise ValueError("compiled plan timestamps must include a timezone")
        expected_deadline = min(
            self.policy_valid_until.astimezone(timezone.utc),
            self.evidence_expires_at.astimezone(timezone.utc),
        )
        if self.execution_deadline.astimezone(timezone.utc) != expected_deadline:
            raise ValueError("execution deadline must be the earliest authority expiry")
        if self.backfill_end.astimezone(timezone.utc) <= self.backfill_start.astimezone(
            timezone.utc
        ):
            raise ValueError("compiled plan replay window is invalid")
        if self.authorized_legacy_versions != ("v2",):
            raise ValueError("compiled plan contains an unsupported legacy-version set")
        if self.candidate_dataset == self.target_dataset:
            raise ValueError("candidate dataset must be separate from the production dataset")
        if self.action_id != _action_id(self.authority_payload()):
            raise ValueError("compiled plan action ID does not match its authority fields")
        return self


def _action_id(authority: dict[str, object]) -> str:
    encoded = json.dumps(authority, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "rpr-" + hashlib.sha256(encoded).hexdigest()[:60]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise PolicyDenied("policy comparisons require timezone-aware timestamps")
    return value.astimezone(timezone.utc)


def _deny_if(condition: bool, message: str) -> None:
    if condition:
        raise PolicyDenied(message)


def compile_intent(
    evidence: IncidentEvidence,
    intent: RepairIntent,
    policy: RecoveryPolicy,
    *,
    now: datetime,
    authority_signer: PlanAuthoritySigner,
) -> CompiledPlan:
    """Rebind a model proposal to one allowlisted, content-addressed plan."""

    # Frozen Pydantic models do not automatically freeze nested caller-owned
    # collections. Re-validate typed snapshots before making policy decisions so
    # a concurrent caller mutation cannot change compilation mid-flight.
    evidence = IncidentEvidence.model_validate(evidence.model_dump())
    intent = RepairIntent.model_validate(intent.model_dump())
    policy = RecoveryPolicy.model_validate(policy.model_dump())

    current = _utc(now)
    evidence_expires_at = _utc(evidence.evidence_expires_at)
    policy_valid_until = _utc(policy.valid_until)
    _deny_if(intent.action != RepairAction.ACTIVATE_MAPPING_PROFILE, "action is not executable")
    _deny_if(not evidence.native_retry_exhausted, "native recovery still owns the incident")
    _deny_if(evidence_expires_at <= current, "Failure Passport expired")
    _deny_if(policy_valid_until <= current, "recovery policy expired")
    _deny_if(intent.incident_id != evidence.incident_id, "incident identity changed")
    _deny_if(intent.target_dataset != evidence.dataset, "target dataset changed")
    _deny_if(intent.source_fingerprint != evidence.source_fingerprint, "source changed")
    _deny_if(
        not set(intent.evidence_event_ids).issubset(evidence.evidence_event_ids),
        "intent cites evidence outside the passport",
    )
    _deny_if(
        intent.mapping_profile_id not in evidence.candidate_profile_ids,
        "profile was not offered for this incident",
    )

    profile = policy.profiles.get(intent.mapping_profile_id or "")
    _deny_if(profile is None or not profile.approved, "profile is absent or unapproved")
    assert profile is not None  # Narrow the type after the fail-closed check.

    scope_matches = (
        profile.pipeline_id == evidence.pipeline_id
        and profile.dataset == evidence.dataset
        and profile.source_table == evidence.source_table
        and profile.partner == evidence.partner
        and profile.payload_version == evidence.payload_version
        and profile.source_fingerprint == evidence.source_fingerprint
    )
    _deny_if(not scope_matches, "profile is registered for a different scope")
    _deny_if(
        not profile.auto_eligible and not profile.semantic_attestation_required,
        "profile is not eligible for execution",
    )
    _deny_if(
        evidence.quarantine_ratio > policy.maximum_auto_quarantine_ratio,
        "quarantine blast radius exceeds policy",
    )
    _deny_if(evidence.estimated_dbus > policy.maximum_auto_dbus, "cost exceeds policy")
    _deny_if(
        len(evidence.downstream_assets) > policy.maximum_downstream_assets,
        "downstream impact exceeds policy",
    )
    _deny_if(intent.backfill_window is None, "historical scope is missing")
    assert intent.backfill_window is not None

    start = _utc(intent.backfill_window.start)
    end = _utc(intent.backfill_window.end)
    _deny_if(
        end - start > policy.maximum_replay_duration,
        "proposed window exceeds maximum replay duration",
    )
    _deny_if(
        start < _utc(evidence.affected_start) or end > _utc(evidence.affected_end),
        "proposed window exceeds observed affected bounds",
    )
    _deny_if(
        start < _utc(evidence.source_replayable_from)
        or end > _utc(evidence.source_replayable_until),
        "source retention does not cover the replay window",
    )

    maximum_rows = min(
        intent.backfill_window.maximum_rows,
        profile.maximum_auto_rows,
        policy.maximum_auto_rows,
    )
    _deny_if(evidence.estimated_rows > maximum_rows, "estimated rows exceed budget")

    # Registry collections are canonicalized both in the hash and in the
    # returned plan. This keeps equivalent registry ordering from producing
    # observably different execution requests.
    refresh_selection = tuple(sorted(profile.refresh_selection))
    predicate_dimensions = tuple(sorted(profile.target_predicate_dimensions))
    execution_deadline = min(evidence_expires_at, policy_valid_until)
    authority = {
        "incident_id": evidence.incident_id,
        "generation": evidence.generation,
        "policy_version": policy.version,
        "policy_valid_until": policy_valid_until.isoformat(),
        "evidence_expires_at": evidence_expires_at.isoformat(),
        "execution_deadline": execution_deadline.isoformat(),
        "profile_id": profile.profile_id,
        "profile_digest": profile.profile_digest,
        "pipeline_id": profile.pipeline_id,
        "pipeline_deployment": profile.pipeline_deployment,
        "recovery_job_id": profile.recovery_job_id,
        "target_dataset": profile.dataset,
        "candidate_dataset": profile.candidate_dataset,
        "source_table": profile.source_table,
        "source_delta_version": evidence.source_delta_version,
        "partner": profile.partner,
        "payload_version": profile.payload_version,
        "authorized_legacy_versions": tuple(sorted(profile.authorized_legacy_versions)),
        "currency_code": profile.currency_code,
        "settlement_relation": profile.settlement_relation,
        "source_fingerprint": profile.source_fingerprint,
        "refresh_selection": refresh_selection,
        "predicate_dimensions": predicate_dimensions,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "maximum_rows": maximum_rows,
        "estimated_dbus": evidence.estimated_dbus,
        "settlement_tolerance_minor": profile.settlement_tolerance_minor,
        "requires_human": profile.semantic_attestation_required,
    }
    action_id = _action_id(authority)
    authority_seal = authority_signer.sign_plan(
        action_id=action_id,
        authority_payload=authority,
    )

    return CompiledPlan(
        action_id=action_id,
        incident_id=evidence.incident_id,
        generation=evidence.generation,
        job_id=profile.recovery_job_id,
        pipeline_id=profile.pipeline_id,
        profile_id=profile.profile_id,
        profile_digest=profile.profile_digest,
        pipeline_deployment=profile.pipeline_deployment,
        target_dataset=profile.dataset,
        candidate_dataset=profile.candidate_dataset,
        source_table=profile.source_table,
        source_delta_version=evidence.source_delta_version,
        partner=profile.partner,
        payload_version=profile.payload_version,
        authorized_legacy_versions=tuple(sorted(profile.authorized_legacy_versions)),
        currency_code=profile.currency_code,
        settlement_relation=profile.settlement_relation,
        source_fingerprint=profile.source_fingerprint,
        refresh_selection=refresh_selection,
        target_predicate_dimensions=predicate_dimensions,
        backfill_start=start,
        backfill_end=end,
        maximum_rows=maximum_rows,
        estimated_dbus=evidence.estimated_dbus,
        policy_version=policy.version,
        policy_valid_until=policy_valid_until,
        evidence_expires_at=evidence_expires_at,
        execution_deadline=execution_deadline,
        settlement_tolerance_minor=profile.settlement_tolerance_minor,
        requires_human=profile.semantic_attestation_required,
        authority_seal=authority_seal,
    )
