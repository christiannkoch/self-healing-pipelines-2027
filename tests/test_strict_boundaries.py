"""Regression tests for primitive coercion at trusted model boundaries."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from conftest import BASE_TIME
from recovery_control_plane.agent_graph import HumanReviewDecision
from recovery_control_plane.contracts import IncidentEvidence, RepairIntent
from recovery_control_plane.evidence import EvidenceSnapshot
from recovery_control_plane.normalization import CanonicalOrder
from recovery_control_plane.policy import ApprovedProfile, CompiledPlan, RecoveryPolicy, compile_intent
from recovery_control_plane.profiles import MappingProfile, build_profile


def _reject_field(model, field: str, bad_value: object) -> None:
    payload = model.model_dump()
    payload[field] = bad_value
    with pytest.raises(ValidationError):
        type(model).model_validate(payload)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("generation", True),
        ("native_retry_exhausted", "yes"),
        ("native_retry_exhausted", 1),
        ("quarantine_ratio", False),
        ("estimated_rows", False),
        ("estimated_dbus", False),
        ("redaction_attested", 1),
        ("redaction_attested", "true"),
    ],
)
def test_incident_evidence_rejects_coercible_authority_values(
    evidence: IncidentEvidence,
    field: str,
    bad_value: object,
) -> None:
    _reject_field(evidence, field, bad_value)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("recovery_job_id", True),
        ("maximum_auto_rows", True),
        ("settlement_tolerance_minor", True),
        ("approved", "yes"),
        ("auto_eligible", 1),
        ("semantic_attestation_required", 0),
    ],
)
def test_approved_profile_rejects_coercible_authority_values(
    approved_profile: ApprovedProfile,
    field: str,
    bad_value: object,
) -> None:
    _reject_field(approved_profile, field, bad_value)


def test_shipped_profile_rejects_a_legacy_set_the_sql_cannot_verify(
    mapping_profile: MappingProfile,
    approved_profile: ApprovedProfile,
) -> None:
    mapping_payload = mapping_profile.model_dump(exclude={"profile_digest"})
    mapping_payload["legacy_payload_versions"] = ("v1",)
    with pytest.raises(ValidationError, match="exactly legacy payload version v2"):
        build_profile(**mapping_payload)

    approved_payload = approved_profile.model_dump()
    approved_payload["authorized_legacy_versions"] = ("v1",)
    with pytest.raises(ValidationError, match="exactly legacy v2"):
        ApprovedProfile.model_validate(approved_payload)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("maximum_auto_quarantine_ratio", False),
        ("maximum_auto_rows", True),
        ("maximum_auto_dbus", False),
        ("maximum_downstream_assets", False),
        ("maximum_replay_duration", 60),
        ("maximum_replay_duration", "PT1H"),
    ],
)
def test_recovery_policy_rejects_coercible_budget_values(
    policy: RecoveryPolicy,
    field: str,
    bad_value: object,
) -> None:
    _reject_field(policy, field, bad_value)


def test_snapshot_requires_explicit_redaction_and_strict_retry() -> None:
    base = {
        "source_table": "commerce.orders_bronze",
        "source_delta_version": 2718,
        "source_fingerprint": "sha256:source-v3",
        "native_retry_exhausted": True,
        "affected_start": BASE_TIME,
        "affected_end": BASE_TIME + timedelta(minutes=20),
        "source_replayable_from": BASE_TIME - timedelta(days=1),
        "source_replayable_until": BASE_TIME + timedelta(days=1),
        "quarantine_ratio": 0.05,
        "estimated_rows": 2,
        "estimated_dbus": 0.5,
        "evidence_event_ids": ("event-1",),
        "redaction_attested": True,
    }
    EvidenceSnapshot.model_validate(base)
    for field, bad_value in (
        ("native_retry_exhausted", "yes"),
        ("estimated_rows", False),
        ("redaction_attested", 1),
    ):
        candidate = dict(base)
        candidate[field] = bad_value
        with pytest.raises(ValidationError):
            EvidenceSnapshot.model_validate(candidate)


def test_backfill_and_semantic_models_reject_boolean_numbers(
    repair_intent: RepairIntent,
) -> None:
    window = repair_intent.backfill_window
    assert window is not None
    _reject_field(window, "maximum_rows", True)
    _reject_field(repair_intent, "confidence", False)
    with pytest.raises(ValidationError):
        HumanReviewDecision(
            approved=1,
            reviewer="reviewer",
            rationale="semantic review only",
        )


def test_candidate_flags_are_strict_booleans() -> None:
    payload = {
        "transport_id": "delivery-1",
        "partner": "marketplace_it",
        "order_id": "order-1",
        "source_sequence": 1,
        "payload_version": "v3",
        "event_type": "SALE",
        "event_time": BASE_TIME,
        "ingested_at": BASE_TIME,
        "customer_id": "redacted",
        "order_total_minor": 100,
        "currency": "EUR",
        "mapping_valid": True,
        "canonical_valid": True,
        "payload_version_authorized": True,
        "applied_profile_id": "profile-1",
        "applied_profile_digest": "a" * 64,
        "dedupe_ambiguous": False,
    }
    CanonicalOrder.model_validate(payload)
    for field in (
        "mapping_valid",
        "canonical_valid",
        "payload_version_authorized",
        "dedupe_ambiguous",
    ):
        candidate = dict(payload)
        candidate[field] = "yes"
        with pytest.raises(ValidationError):
            CanonicalOrder.model_validate(candidate)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("generation", True),
        ("job_id", True),
        ("maximum_rows", True),
        ("estimated_dbus", False),
        ("settlement_tolerance_minor", True),
        ("requires_human", 1),
    ],
)
def test_compiled_plan_rejects_coercible_authority_values(
    evidence: IncidentEvidence,
    repair_intent: RepairIntent,
    policy: RecoveryPolicy,
    authority,
    field: str,
    bad_value: object,
) -> None:
    plan = compile_intent(
        evidence,
        repair_intent,
        policy,
        now=BASE_TIME + timedelta(minutes=10),
        authority_signer=authority,
    )
    assert isinstance(plan, CompiledPlan)
    _reject_field(plan, field, bad_value)
