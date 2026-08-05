from __future__ import annotations

from datetime import timedelta

import pytest

from recovery_control_plane.coordinator import (
    build_verification_receipt,
    DryRunPromotionAdapter,
    RecoveryCoordinator,
    VerificationEvidence,
)
from recovery_control_plane.incident_store import InMemoryIncidentStore
from recovery_control_plane.normalization import BronzeOrder, normalize_and_dedupe
from recovery_control_plane.policy import compile_intent
from recovery_control_plane.verifier import SettlementRecord, VerificationFailed
from recovery_control_plane.worker import (
    DryRunPipelineAdapter,
    ExecutionApproval,
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
    def __init__(self, evidence, events) -> None:
        self.evidence = evidence
        self.events = events
        self.calls = 0

    def load(self, *, plan, update_id):
        assert plan.action_id
        assert update_id
        self.calls += 1
        self.events.append("verify")
        return self.evidence


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


def verification_evidence(mapping_profile, *, settlement_minor=1_250):
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
    return VerificationEvidence(
        expected_keys=frozenset({key}),
        expected_v3_keys=frozenset({key}),
        candidates=tuple(normalize_and_dedupe([source], mapping_profile)),
        settlements=(
            SettlementRecord(
                partner=key[0],
                order_id=key[1],
                source_sequence=key[2],
                currency="EUR",
                settled_amount_minor=settlement_minor,
            ),
        ),
    )


def build_runtime(evidence, repair_intent, policy, mapping_profile, *, settlement_minor=1_250):
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    events = []
    pipeline = RecordingPipeline(events)
    provider = RecordingProvider(
        verification_evidence(mapping_profile, settlement_minor=settlement_minor),
        events,
    )
    promotion = RecordingPromotion(events)
    store = RecordingStore(events)
    worker = RecoveryWorker(
        pipeline,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
    )
    coordinator = RecoveryCoordinator(
        worker=worker,
        evidence_provider=provider,
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
    )
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    return plan, events, pipeline, provider, promotion, store, coordinator, approval


def test_happy_path_persists_and_reloads_proof_before_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
) -> None:
    plan, events, _, provider, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
    )

    result = coordinator.run(plan, store, approval=approval, maximum_polls=1)

    assert events == ["execute", "verify", "persist_receipt", "reload_receipt", "promote"]
    assert provider.calls == 1
    assert promotion.calls == 1
    assert result.verification.action_id == plan.action_id
    assert result.verification.update_id == result.update_id
    assert result.promotion.verification_digest == result.verification.proof_digest
    assert store.has_stage(plan.action_id, coordinator.PROMOTION_STAGE)


def test_failed_independent_verification_never_creates_receipt_or_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
) -> None:
    plan, events, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
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
) -> None:
    plan, events, pipeline, provider, promotion, _, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
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
) -> None:
    plan, events, _, _, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
    )
    coordinator.clock = SequenceClock(
        BASE_TIME + timedelta(minutes=2),
        BASE_TIME + timedelta(minutes=2),
        plan.execution_deadline,
    )

    with pytest.raises(PermissionError, match="expired before promotion"):
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
) -> None:
    plan, events, pipeline, provider, promotion, store, coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
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
        ),
        evidence_provider=provider,
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
) -> None:
    plan, _, _, provider, promotion, store, coordinator, _ = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
    )
    # First obtain a correctly digested receipt without retaining promotion.
    good_store = InMemoryIncidentStore()
    good_promotion = RecordingPromotion([])
    good = RecoveryCoordinator(
        worker=coordinator.worker,
        evidence_provider=provider,
        promotion_adapter=good_promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
    )
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    result = good.run(plan, good_store, approval=approval, maximum_polls=1)
    forged = result.verification.model_dump(mode="json")
    forged["verified_at"] = (BASE_TIME + timedelta(minutes=4)).isoformat()
    store.put_stage(plan.action_id, coordinator.VERIFICATION_STAGE, forged)

    with pytest.raises(ValueError, match="digest does not match"):
        coordinator.run(plan, store, approval=None, maximum_polls=1)

    assert promotion.calls == 0


def test_validly_digested_future_receipt_is_rejected_before_promotion(
    evidence,
    repair_intent,
    policy,
    mapping_profile,
) -> None:
    plan, _, _, _, _, good_store, good_coordinator, approval = build_runtime(
        evidence,
        repair_intent,
        policy,
        mapping_profile,
    )
    good = good_coordinator.run(
        plan,
        good_store,
        approval=approval,
        maximum_polls=1,
    )
    future_receipt = build_verification_receipt(
        plan=plan,
        update_id=good.update_id,
        verified_at=BASE_TIME + timedelta(minutes=10),
        metrics=good.verification.metrics,
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
        promotion_adapter=promotion,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=5)),
    )

    with pytest.raises(ValueError, match="future-dated"):
        coordinator.run(plan, store, approval=None, maximum_polls=1)

    assert promotion.calls == 0
