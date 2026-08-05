from __future__ import annotations

from datetime import timedelta

import pytest

from recovery_control_plane.authority import HmacRecoveryAuthority
from recovery_control_plane.coordinator import (
    _proof_digest,
    build_manifest_digest,
    build_evidence_manifest,
    build_verification_receipt,
    DryRunPromotionAdapter,
    EvidenceArtifact,
    RecoveryCoordinator,
    VerificationEvidence,
    VerificationReceipt,
)
from recovery_control_plane.incident_store import InMemoryIncidentStore
from recovery_control_plane.normalization import BronzeOrder, normalize_and_dedupe
from recovery_control_plane.policy import compile_intent
from recovery_control_plane.verifier import SettlementRecord, VerificationFailed
from recovery_control_plane.worker import (
    DryRunPipelineAdapter,
    RecoveryWorker,
)

from conftest import BASE_TIME


class FrozenClock:
    def __init__(self, instant) -> None:
        self.instant = instant

    def now(self):
        return self.instant


class SequenceClock:
    def __init__(self, *instants) -> None:
        self.instants = list(instants)

    def now(self):
        if not self.instants:
            raise AssertionError("coordinator clock read more often than expected")
        return self.instants.pop(0)


class RecordingPipeline(DryRunPipelineAdapter):
    def __init__(self, events) -> None:
        super().__init__()
        self.events = events

    def start_bounded_update(self, *, plan, predicate, action_id):
        self.events.append("execute")
        return super().start_bounded_update(
            plan=plan,
            predicate=predicate,
            action_id=action_id,
        )


class RecordingProvider:
    def __init__(self, mapping_profile, events, *, settlement_minor=1_250) -> None:
        self.mapping_profile = mapping_profile
        self.settlement_minor = settlement_minor
        self.events = events
        self.calls = 0

    def load(self, *, plan, update_id):
        assert plan.action_id
        assert update_id
        self.calls += 1
        self.events.append("verify")
        return verification_evidence(
            self.mapping_profile,
            plan,
            update_id,
            settlement_minor=self.settlement_minor,
        )


class FactoryProvider:
    def __init__(self, factory) -> None:
        self.factory = factory

    def load(self, *, plan, update_id):
        return self.factory(plan, update_id)


class RecordingStore(InMemoryIncidentStore):
    def __init__(self, events) -> None:
        super().__init__()
        self.events = events

    def put_stage(self, action_id, stage, payload):
        super().put_stage(action_id, stage, payload)
        if stage == RecoveryCoordinator.VERIFICATION_STAGE:
            self.events.append("persist_receipt")

    def get_stage(self, action_id, stage):
        payload = super().get_stage(action_id, stage)
        if stage == RecoveryCoordinator.VERIFICATION_STAGE:
            self.events.append("reload_receipt")
        return payload


class RecordingPromotion(DryRunPromotionAdapter):
    def __init__(self, events) -> None:
        super().__init__()
        self.events = events
        self.calls = 0

    def promote(self, *, plan, receipt):
        self.calls += 1
        self.events.append("promote")
        return super().promote(plan=plan, receipt=receipt)


class ReceiptRejectingStore(RecordingStore):
    def put_stage(self, action_id, stage, payload):
        if stage == RecoveryCoordinator.VERIFICATION_STAGE:
            raise RuntimeError("durable receipt write failed")
        super().put_stage(action_id, stage, payload)


def verification_evidence(
    mapping_profile,
    plan,
    update_id,
    *,
    settlement_minor=1_250,
    observed_start=None,
    observed_end=None,
    source_delta_version=None,
    candidate_event_time=BASE_TIME,
    candidate_table=None,
):
    source = BronzeOrder(
        transport_id="tx-v3",
        partner="marketplace_it",
        payload_version="v3",
        order_id="order-200",
        source_sequence=7,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME,
        rescued_data={
            "buyer": {"customerId": "redacted"},
            "grossAmount": {
                "units": 12,
                "nanos": 500_000_000,
                "currencyCode": "EUR",
            },
        },
    )
    key = ("marketplace_it", "order-200", 7)
    expected_keys = frozenset({key})
    expected_v3_keys = frozenset({key})
    candidates = tuple(
        candidate.model_copy(update={"event_time": candidate_event_time})
        for candidate in normalize_and_dedupe([source], mapping_profile)
    )
    settlements = (
            SettlementRecord(
                partner=key[0],
                order_id=key[1],
                source_sequence=key[2],
                currency="EUR",
                settled_amount_minor=settlement_minor,
            ),
        )
    candidate_delta_version = 9001
    observed_candidate_table = candidate_table or plan.candidate_dataset
    manifest, manifest_digest = build_evidence_manifest(
        source_table=plan.source_table,
        source_delta_version=(
            plan.source_delta_version
            if source_delta_version is None
            else source_delta_version
        ),
        candidate_table=observed_candidate_table,
        candidate_delta_version=candidate_delta_version,
        update_id=update_id,
        settlement_relation=plan.settlement_relation,
        settlement_snapshot_reference="batch:2027-11-26T08:30:00Z",
        source_rows=(source,),
        candidates=candidates,
        settlements=settlements,
    )
    return VerificationEvidence(
        action_id=plan.action_id,
        update_id=update_id,
        pipeline_id=plan.pipeline_id,
        pipeline_deployment=plan.pipeline_deployment,
        profile_digest=plan.profile_digest,
        source_table=plan.source_table,
        source_delta_version=(
            plan.source_delta_version
            if source_delta_version is None
            else source_delta_version
        ),
        source_fingerprint=plan.source_fingerprint,
        candidate_table=observed_candidate_table,
        candidate_delta_version=candidate_delta_version,
        observed_start=observed_start or plan.backfill_start,
        observed_end=observed_end or plan.backfill_end,
        manifest=manifest,
        manifest_digest=manifest_digest,
        source_rows=(source,),
        expected_keys=expected_keys,
        expected_v3_keys=expected_v3_keys,
        candidates=candidates,
        settlements=settlements,
    )


def build_runtime(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
    *,
    settlement_minor=1_250,
):
    plan = compile_intent(
        evidence,
        repair_intent,
        policy,
        now=BASE_TIME,
        authority_signer=authority,
    )
    events = []
    pipeline = RecordingPipeline(events)
    provider = RecordingProvider(
        mapping_profile,
        events,
        settlement_minor=settlement_minor,
    )
    promotion = RecordingPromotion(events)
    store = RecordingStore(events)
    worker = RecoveryWorker(
        pipeline,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
        plan_authority=authority,
        approval_authority=authority,
    )
    coordinator = RecoveryCoordinator(
        worker=worker,
        evidence_provider=provider,
        receipt_authority_signer=authority,
        receipt_authority_verifier=authority,
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
    )
    approval = authority.issue_approval(
        plan=plan,
        record_id="approval-1",
        approved_by="data-owner",
        approved_at=BASE_TIME,
        expires_at=BASE_TIME + timedelta(minutes=30),
    )
    return plan, events, pipeline, provider, promotion, store, coordinator, approval


def test_happy_path_persists_and_reloads_proof_before_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, events, _, provider, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )

    result = coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert events == ["execute", "verify", "persist_receipt", "reload_receipt", "promote"]
    assert provider.calls == 1
    assert promotion.calls == 1
    assert result.verification.action_id == plan.action_id
    assert result.verification.update_id == result.update_id
    assert result.verification.candidate_table == plan.candidate_dataset
    assert result.verification.candidate_table != plan.target_dataset
    assert result.promotion.verification_digest == result.verification.proof_digest
    assert store.has_stage(plan.action_id, coordinator.PROMOTION_STAGE)


@pytest.mark.parametrize(
    "candidate_table",
    ["commerce.orders_normalized", "commerce.orders_repaired_shadow"],
    ids=["production-target", "different-candidate"],
)
def test_observed_evidence_must_name_the_registry_candidate_relation(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
    candidate_table,
) -> None:
    plan, _, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )

    coordinator.evidence_provider = FactoryProvider(
        lambda observed_plan, update_id: verification_evidence(
            mapping_profile,
            observed_plan,
            update_id,
            candidate_table=candidate_table,
        )
    )

    with pytest.raises(ValueError, match="different candidate table"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert promotion.calls == 0
    assert not store.has_stage(plan.action_id, coordinator.VERIFICATION_STAGE)


@pytest.mark.parametrize(
    "candidate_table",
    ["commerce.orders_normalized", "commerce.orders_repaired_shadow"],
    ids=["production-target", "different-candidate"],
)
def test_even_a_validly_signed_receipt_must_name_the_registry_candidate_relation(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
    candidate_table,
) -> None:
    plan, _, _, _, _, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    result = coordinator.run(plan, store, approval=approval, maximum_polls=1)
    forged_payload = result.verification.proof_payload()
    forged_payload["candidate_table"] = candidate_table
    forged_digest = _proof_digest(**forged_payload)
    forged_seal = authority.sign_verification_receipt(
        proof_payload=forged_payload,
        proof_digest=forged_digest,
    )
    forged_receipt = VerificationReceipt(
        **forged_payload,
        proof_digest=forged_digest,
        authority_seal=forged_seal,
    )

    with pytest.raises(ValueError, match="different candidate table"):
        forged_receipt.validate_for(plan, result.update_id)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("update", "different update"),
        ("snapshot", "different Delta version"),
        ("window", "different replay window"),
        ("candidate", "outside the observed replay window"),
    ],
)
def test_observed_evidence_must_match_exact_update_snapshot_and_window(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
    mutation,
    message,
) -> None:
    plan, _, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )

    def forged(observed_plan, update_id):
        if mutation == "update":
            update_id = "different-completed-update"
        kwargs = {}
        if mutation == "snapshot":
            kwargs["source_delta_version"] = observed_plan.source_delta_version + 1
        if mutation == "window":
            kwargs["observed_start"] = observed_plan.backfill_start - timedelta(days=1)
            kwargs["observed_end"] = observed_plan.backfill_end - timedelta(days=1)
        if mutation == "candidate":
            kwargs["candidate_event_time"] = observed_plan.backfill_end + timedelta(
                minutes=1
            )
        return verification_evidence(
            mapping_profile,
            observed_plan,
            update_id,
            **kwargs,
        )

    coordinator.evidence_provider = FactoryProvider(forged)
    with pytest.raises(ValueError, match=message):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert promotion.calls == 0
    assert not store.has_stage(plan.action_id, coordinator.PROMOTION_STAGE)


def test_manifest_digest_is_recomputed_from_observed_rows(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )

    def forged(observed_plan, update_id):
        valid = verification_evidence(mapping_profile, observed_plan, update_id)
        return valid.model_copy(update={"manifest_digest": "0" * 64})

    coordinator.evidence_provider = FactoryProvider(forged)
    with pytest.raises(ValueError, match="manifest digest"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert promotion.calls == 0


def test_unavailable_verification_path_fails_closed(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )

    def unavailable(plan, update_id):
        del plan, update_id
        raise RuntimeError("independent evidence service unavailable")

    coordinator.evidence_provider = FactoryProvider(unavailable)
    with pytest.raises(RuntimeError, match="evidence service unavailable"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert promotion.calls == 0
    assert not store.has_stage(plan.action_id, coordinator.VERIFICATION_STAGE)


@pytest.mark.parametrize(
    ("candidate_event_time", "message"),
    [
        (None, "candidate event_time cannot be null"),
        (BASE_TIME.replace(tzinfo=None), "candidate event_time must include a timezone"),
    ],
    ids=["null", "timezone-naive"],
)
def test_verification_evidence_envelope_rejects_unusable_candidate_event_time(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
    candidate_event_time,
    message,
) -> None:
    plan, *_ = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )

    with pytest.raises(ValueError, match=message):
        verification_evidence(
            mapping_profile,
            plan,
            "update-null-event-time",
            candidate_event_time=candidate_event_time,
        )


SOURCE_SCOPE_FAILURE_CASES = (
    ("null-order", {"order_id": None}, "missing business key"),
    ("blank-order", {"order_id": "   "}, "missing business key"),
    ("null-sequence", {"source_sequence": None}, "non-strict business key"),
    ("boolean-sequence", {"source_sequence": True}, "non-strict business key"),
    ("string-sequence", {"source_sequence": "7"}, "non-strict business key"),
    ("null-event-time", {"event_time": None}, "event_time cannot be null"),
    (
        "naive-event-time",
        {"event_time": BASE_TIME.replace(tzinfo=None)},
        "event_time must include a timezone",
    ),
    (
        "before-window",
        {"event_time": BASE_TIME - timedelta(minutes=1)},
        "outside the replay window",
    ),
    (
        "at-exclusive-end",
        {"event_time": BASE_TIME + timedelta(minutes=20)},
        "outside the replay window",
    ),
    ("wrong-partner", {"partner": "marketplace_de"}, "different partner"),
    (
        "unauthorized-version",
        {"payload_version": "v99-unsupported"},
        "unauthorized payload version",
    ),
)


@pytest.mark.parametrize(
    ("_case", "updates", "message"),
    SOURCE_SCOPE_FAILURE_CASES,
    ids=[case[0] for case in SOURCE_SCOPE_FAILURE_CASES],
)
def test_verification_evidence_rejects_malformed_retained_source_rows(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
    _case,
    updates,
    message,
) -> None:
    plan, *_ = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    update_id = "update-malformed-source"
    observed = verification_evidence(mapping_profile, plan, update_id)
    malformed = observed.source_rows[0].model_copy(
        update={"transport_id": f"malformed-{_case}", **updates}
    )
    source_rows = (*observed.source_rows, malformed)
    settlement_artifact = next(
        item for item in observed.manifest if item.kind == "SETTLEMENT_QUERY"
    )
    manifest, manifest_digest = build_evidence_manifest(
        source_table=observed.source_table,
        source_delta_version=observed.source_delta_version,
        candidate_table=observed.candidate_table,
        candidate_delta_version=observed.candidate_delta_version,
        update_id=update_id,
        settlement_relation=settlement_artifact.relation,
        settlement_snapshot_reference=settlement_artifact.snapshot_reference,
        source_rows=source_rows,
        candidates=observed.candidates,
        settlements=observed.settlements,
    )
    forged = observed.model_copy(
        update={
            "source_rows": source_rows,
            "manifest": manifest,
            "manifest_digest": manifest_digest,
        }
    )

    with pytest.raises(ValueError, match=message):
        forged.validate_for(plan, update_id)


def test_verification_evidence_rejects_unapproved_settlement_relation(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, *_ = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    update_id = "update-forged-settlement-relation"
    observed = verification_evidence(mapping_profile, plan, update_id)
    manifest = tuple(
        item.model_copy(update={"relation": plan.candidate_dataset})
        if item.kind == "SETTLEMENT_QUERY"
        else item
        for item in observed.manifest
    )
    forged = observed.model_copy(
        update={
            "manifest": manifest,
            "manifest_digest": build_manifest_digest(manifest),
        }
    )

    with pytest.raises(ValueError, match="different settlement relation"):
        forged.validate_for(plan, update_id)


def test_failed_independent_verification_never_creates_receipt_or_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, events, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
        settlement_minor=1_249,
    )

    with pytest.raises(VerificationFailed, match="settlement_difference_minor"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert events == ["execute", "verify"]
    assert promotion.calls == 0
    assert not store.has_stage(plan.action_id, coordinator.VERIFICATION_STAGE)
    assert not store.has_stage(plan.action_id, coordinator.PROMOTION_STAGE)


def test_failed_receipt_persistence_blocks_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, events, pipeline, provider, promotion, _, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    store = ReceiptRejectingStore(events)

    with pytest.raises(RuntimeError, match="receipt write failed"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert events == ["execute", "verify"]
    assert len(pipeline.updates) == 1
    assert provider.calls == 1
    assert promotion.calls == 0
    assert not store.has_stage(plan.action_id, coordinator.PROMOTION_STAGE)


def test_expiry_after_persisted_proof_blocks_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, events, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    coordinator.clock = SequenceClock(
        BASE_TIME + timedelta(minutes=2),
        BASE_TIME + timedelta(minutes=2),
        plan.execution_deadline,
    )

    with pytest.raises(PermissionError, match="authority has expired"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert events == ["execute", "verify", "persist_receipt", "reload_receipt"]
    assert promotion.calls == 0
    assert store.has_stage(plan.action_id, coordinator.VERIFICATION_STAGE)
    assert not store.has_stage(plan.action_id, coordinator.PROMOTION_STAGE)


def test_retry_from_persisted_receipt_skips_execution_and_verification(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, events, pipeline, provider, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    coordinator.clock = SequenceClock(
        BASE_TIME + timedelta(minutes=2),
        BASE_TIME + timedelta(minutes=2),
        plan.execution_deadline,
    )
    with pytest.raises(PermissionError):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    events.clear()
    result = RecoveryCoordinator(
        worker=RecoveryWorker(
            pipeline,
            clock=FrozenClock(BASE_TIME + timedelta(minutes=3)),
            plan_authority=authority,
            approval_authority=authority,
        ),
        evidence_provider=provider,
        receipt_authority_signer=authority,
        receipt_authority_verifier=authority,
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=3)),
    ).run(plan, store, approval=None, maximum_polls=1)

    assert events == ["reload_receipt", "promote"]
    assert provider.calls == 1
    assert len(pipeline.updates) == 1
    assert promotion.calls == 1
    assert result.promotion.promotion_id


def test_tampered_persisted_receipt_cannot_reach_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, provider, promotion, store, coordinator, _ = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    # First obtain a correctly digested receipt without retaining promotion.
    good_store = InMemoryIncidentStore()
    good_promotion = RecordingPromotion([])
    good = RecoveryCoordinator(
        worker=coordinator.worker,
        evidence_provider=provider,
        receipt_authority_signer=authority,
        receipt_authority_verifier=authority,
        promotion_adapter=good_promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
    )
    approval = authority.issue_approval(
        plan=plan,
        record_id="approval-2",
        approved_by="data-owner",
        approved_at=BASE_TIME,
        expires_at=BASE_TIME + timedelta(minutes=30),
    )
    result = good.run(plan, good_store, approval=approval, maximum_polls=1)
    forged_payload = result.verification.proof_payload()
    forged_manifest = list(forged_payload["evidence_manifest"])
    forged_manifest[0] = {**forged_manifest[0], "content_digest": "f" * 64}
    forged_manifest_models = tuple(
        EvidenceArtifact.model_validate(item) for item in forged_manifest
    )
    forged_manifest_digest = build_manifest_digest(forged_manifest_models)
    forged_payload["evidence_manifest"] = forged_manifest
    forged_payload["evidence_manifest_digest"] = forged_manifest_digest
    forged = result.verification.model_dump(mode="json")
    forged["evidence_manifest"] = forged_manifest
    forged["evidence_manifest_digest"] = forged_manifest_digest
    # This recreates the original exploit: the store recomputes the unkeyed
    # content digest so the forged receipt remains internally self-consistent.
    forged["proof_digest"] = _proof_digest(**forged_payload)
    forged_receipt = VerificationReceipt.model_validate(forged)
    store.put_external_id(plan.action_id, RecoveryWorker.STEP, result.update_id)
    assert store.compare_and_set_state(
        plan.action_id,
        RecoveryWorker.STEP,
        None,
        RecoveryWorker.COMPLETED,
    )
    store.put_stage(
        plan.action_id,
        coordinator.VERIFICATION_STAGE,
        forged_receipt.model_dump(mode="json"),
    )

    with pytest.raises(PermissionError, match="receipt authority signature is invalid"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert promotion.calls == 0


def test_receipt_with_tampered_authority_seal_cannot_promote(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, _, promotion, _, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    source_store = InMemoryIncidentStore()
    result = coordinator.run(plan, source_store, approval=approval, maximum_polls=1)
    bad_seal = result.verification.authority_seal.model_copy(
        update={"signature": "0" * 64}
    )
    forged = result.verification.model_copy(update={"authority_seal": bad_seal})
    store = InMemoryIncidentStore()
    store.put_external_id(plan.action_id, RecoveryWorker.STEP, result.update_id)
    assert store.compare_and_set_state(
        plan.action_id,
        RecoveryWorker.STEP,
        None,
        RecoveryWorker.COMPLETED,
    )
    store.put_stage(
        plan.action_id,
        coordinator.VERIFICATION_STAGE,
        forged.model_dump(mode="json"),
    )

    with pytest.raises(PermissionError, match="receipt authority signature is invalid"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert promotion.calls == 1  # Only the trusted source run promoted.


def test_receipt_from_untrusted_issuer_cannot_promote(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, _, _, _, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    source_store = InMemoryIncidentStore()
    result = coordinator.run(plan, source_store, approval=approval, maximum_polls=1)
    rogue = HmacRecoveryAuthority(
        key_id="rogue-receipt-key",
        issuer="rogue-verifier",
        secret=b"rogue-test-receipt-authority-key-material-0001",
    )
    rogue_seal = rogue.sign_verification_receipt(
        proof_payload=result.verification.proof_payload(),
        proof_digest=result.verification.proof_digest,
    )
    forged = result.verification.model_copy(update={"authority_seal": rogue_seal})
    store = InMemoryIncidentStore()
    store.put_external_id(plan.action_id, RecoveryWorker.STEP, result.update_id)
    assert store.compare_and_set_state(
        plan.action_id,
        RecoveryWorker.STEP,
        None,
        RecoveryWorker.COMPLETED,
    )
    store.put_stage(
        plan.action_id,
        coordinator.VERIFICATION_STAGE,
        forged.model_dump(mode="json"),
    )

    with pytest.raises(PermissionError, match="untrusted authority"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)


def test_missing_receipt_authority_fails_before_pipeline_mutation(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, pipeline, provider, promotion, _, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    untrusted_coordinator = RecoveryCoordinator(
        worker=coordinator.worker,
        evidence_provider=provider,
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
    )

    with pytest.raises(PermissionError, match="receipt signer is unavailable"):
        untrusted_coordinator.run(
            plan,
            InMemoryIncidentStore(),
            approval=approval,
            maximum_polls=1,
        )

    assert pipeline.updates == {}


def test_source_manifest_digest_covers_complete_retained_rows(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )

    def altered_source(observed_plan, update_id):
        valid = verification_evidence(mapping_profile, observed_plan, update_id)
        altered = valid.source_rows[0].model_copy(
            update={"rescued_data": {"unexpected": "different retained payload"}}
        )
        return valid.model_copy(update={"source_rows": (altered,)})

    coordinator.evidence_provider = FactoryProvider(altered_source)
    with pytest.raises(ValueError, match="source_snapshot digest"):
        coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert promotion.calls == 0


def test_validly_digested_future_receipt_is_rejected_before_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, _, _, good_store, good_coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    good = good_coordinator.run(
        plan,
        good_store,
        approval=approval,
        maximum_polls=1,
    )
    future_receipt = build_verification_receipt(
        plan=plan,
        evidence=verification_evidence(
            mapping_profile,
            plan,
            good.update_id,
        ),
        verified_at=BASE_TIME + timedelta(minutes=10),
        metrics=good.verification.metrics,
        receipt_authority_signer=authority,
    )
    store = InMemoryIncidentStore()
    store.put_stage(
        plan.action_id,
        RecoveryCoordinator.VERIFICATION_STAGE,
        future_receipt.model_dump(mode="json"),
    )
    promotion = RecordingPromotion([])
    coordinator = RecoveryCoordinator(
        worker=good_coordinator.worker,
        evidence_provider=good_coordinator.evidence_provider,
        receipt_authority_signer=authority,
        receipt_authority_verifier=authority,
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=5)),
    )

    with pytest.raises(ValueError, match="future-dated"):
        coordinator.run(plan, store, approval=None, maximum_polls=1)

    assert promotion.calls == 0


def test_correctly_redigested_failing_metrics_receipt_cannot_promote(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
) -> None:
    plan, _, _, _, _, good_store, good_coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    good = good_coordinator.run(
        plan,
        good_store,
        approval=approval,
        maximum_polls=1,
    )
    failing_metrics = good.verification.metrics.model_copy(
        update={"duplicate_business_events": 1}
    )
    failing_receipt = build_verification_receipt(
        plan=plan,
        evidence=verification_evidence(
            mapping_profile,
            plan,
            good.update_id,
        ),
        verified_at=BASE_TIME + timedelta(minutes=3),
        metrics=failing_metrics,
        receipt_authority_signer=authority,
    )
    store = InMemoryIncidentStore()
    store.put_external_id(plan.action_id, RecoveryWorker.STEP, good.update_id)
    assert store.compare_and_set_state(
        plan.action_id,
        RecoveryWorker.STEP,
        None,
        RecoveryWorker.COMPLETED,
    )
    store.put_stage(
        plan.action_id,
        RecoveryCoordinator.VERIFICATION_STAGE,
        failing_receipt.model_dump(mode="json"),
    )
    promotion = RecordingPromotion([])
    coordinator = RecoveryCoordinator(
        worker=good_coordinator.worker,
        evidence_provider=good_coordinator.evidence_provider,
        receipt_authority_signer=authority,
        receipt_authority_verifier=authority,
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=4)),
    )

    with pytest.raises(VerificationFailed, match="duplicate_business_events"):
        coordinator.run(plan, store, approval=None, maximum_polls=1)

    assert promotion.calls == 0


@pytest.mark.parametrize(
    ("journal_state", "message"),
    [
        (RecoveryWorker.COMPLETED, "lacks its exact update journal"),
        (RecoveryWorker.ACCEPTED, "not journaled as COMPLETED"),
    ],
)
def test_persisted_receipt_requires_exact_completed_update_journal(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
    authority,
    journal_state,
    message,
) -> None:
    plan, _, _, _, _, good_store, good_coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
        authority,
    )
    good = good_coordinator.run(
        plan,
        good_store,
        approval=approval,
        maximum_polls=1,
    )
    store = InMemoryIncidentStore()
    if journal_state == RecoveryWorker.COMPLETED:
        # Completed state without an external update ID is still insufficient.
        assert store.compare_and_set_state(
            plan.action_id,
            RecoveryWorker.STEP,
            None,
            RecoveryWorker.COMPLETED,
        )
    else:
        store.put_external_id(plan.action_id, RecoveryWorker.STEP, good.update_id)
        assert store.compare_and_set_state(
            plan.action_id,
            RecoveryWorker.STEP,
            None,
            journal_state,
        )
    store.put_stage(
        plan.action_id,
        RecoveryCoordinator.VERIFICATION_STAGE,
        good.verification.model_dump(mode="json"),
    )
    promotion = RecordingPromotion([])
    coordinator = RecoveryCoordinator(
        worker=good_coordinator.worker,
        evidence_provider=good_coordinator.evidence_provider,
        receipt_authority_signer=authority,
        receipt_authority_verifier=authority,
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=4)),
    )

    with pytest.raises(ValueError, match=message):
        coordinator.run(plan, store, approval=None, maximum_polls=1)

    assert promotion.calls == 0
