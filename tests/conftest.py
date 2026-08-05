from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from recovery_control_plane.authority import HmacRecoveryAuthority
from recovery_control_plane.contracts import BackfillWindow, IncidentEvidence, RepairAction, RepairIntent
from recovery_control_plane.policy import ApprovedProfile, RecoveryPolicy
from recovery_control_plane.profiles import MappingProfile, build_profile
from recovery_control_plane.profiles import MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST


UTC = timezone.utc
BASE_TIME = datetime(2027, 11, 26, 8, 0, tzinfo=UTC)


@pytest.fixture
def authority() -> HmacRecoveryAuthority:
    """Ephemeral test trust root; production key material is never hard-coded."""

    return HmacRecoveryAuthority(
        key_id="test-recovery-key-1",
        issuer="test-policy-service",
        secret=b"test-only-recovery-authority-key-material-0001",
    )


@pytest.fixture
def mapping_profile() -> MappingProfile:
    return build_profile(
        profile_id="marketplace-v3-gross-eur-v2",
        partner="marketplace_it",
        payload_version="v3",
        source_fingerprint="sha256:source-v3",
        normalizer_key="MARKETPLACE_V3_GROSS",
        normalizer_version="1.0.1",
        normalizer_source_digest=MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST,
        currency_code="EUR",
        currency_exponent=2,
        dedupe_key=("partner", "order_id", "source_sequence"),
    )


@pytest.fixture
def evidence(mapping_profile: MappingProfile) -> IncidentEvidence:
    return IncidentEvidence(
        incident_id="inc-black-friday-2027",
        generation=2,
        detected_at=BASE_TIME + timedelta(minutes=7),
        pipeline_id="pipeline-orders",
        dataset="commerce.orders_normalized",
        source_table="commerce.orders_bronze",
        source_delta_version=2718,
        partner="marketplace_it",
        payload_version="v3",
        source_fingerprint=mapping_profile.source_fingerprint,
        native_retry_exhausted=True,
        affected_start=BASE_TIME,
        affected_end=BASE_TIME + timedelta(minutes=20),
        evidence_expires_at=BASE_TIME + timedelta(hours=1),
        source_replayable_from=BASE_TIME - timedelta(days=7),
        source_replayable_until=BASE_TIME + timedelta(days=1),
        quarantine_ratio=0.05,
        estimated_rows=2,
        estimated_dbus=0.5,
        downstream_assets=["commerce.orders_gold"],
        candidate_profile_ids=[mapping_profile.profile_id],
        evidence_event_ids=["event-1", "event-2"],
        redacted_samples=["v3 EUR refund; identifiers removed"],
        redaction_attested=True,
        business_checks={"currency": "EUR"},
    )


@pytest.fixture
def approved_profile(mapping_profile: MappingProfile) -> ApprovedProfile:
    return ApprovedProfile(
        profile_id=mapping_profile.profile_id,
        profile_digest=mapping_profile.profile_digest,
        pipeline_id="pipeline-orders",
        pipeline_deployment="bundle:orders@abc123",
        dataset="commerce.orders_normalized",
        candidate_dataset="commerce.orders_repaired",
        source_table="commerce.orders_bronze",
        partner=mapping_profile.partner,
        payload_version=mapping_profile.payload_version,
        currency_code=mapping_profile.currency_code,
        settlement_relation="finance.settlements",
        source_fingerprint=mapping_profile.source_fingerprint,
        recovery_job_id=42,
        refresh_selection=("orders_repaired",),
        target_predicate_dimensions=("partner", "event_time"),
        maximum_auto_rows=50_000,
        approved=True,
        auto_eligible=False,
        semantic_attestation_required=True,
    )


@pytest.fixture
def policy(approved_profile: ApprovedProfile) -> RecoveryPolicy:
    return RecoveryPolicy(
        version="recovery-policy/7",
        valid_until=BASE_TIME + timedelta(days=1),
        maximum_auto_quarantine_ratio=0.10,
        maximum_auto_rows=100_000,
        maximum_auto_dbus=5,
        maximum_downstream_assets=5,
        maximum_replay_duration=timedelta(hours=2),
        profiles={approved_profile.profile_id: approved_profile},
    )


@pytest.fixture
def repair_intent(evidence: IncidentEvidence, mapping_profile: MappingProfile) -> RepairIntent:
    return RepairIntent(
        incident_id=evidence.incident_id,
        action=RepairAction.ACTIVATE_MAPPING_PROFILE,
        target_dataset=evidence.dataset,
        source_fingerprint=evidence.source_fingerprint,
        mapping_profile_id=mapping_profile.profile_id,
        backfill_window=BackfillWindow(
            start=evidence.affected_start,
            end=evidence.affected_end,
            maximum_rows=10_000,
        ),
        confidence=0.87,
        evidence_event_ids=evidence.evidence_event_ids,
        rationale="The certified v3 profile matches the bounded Italian source fingerprint.",
    )
