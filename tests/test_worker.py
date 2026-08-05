from datetime import datetime, timedelta, timezone

import pytest

from recovery_control_plane.incident_store import InMemoryIncidentStore
from recovery_control_plane.policy import CompiledPlan, compile_intent
from recovery_control_plane.worker import (
    bounded_predicate,
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
            raise AssertionError("worker clock read more often than expected")
        return self.instants.pop(0)


def test_worker_reuses_one_local_update(evidence, repair_intent, policy) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    plan = CompiledPlan.model_validate_json(plan.model_dump_json())
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    worker = RecoveryWorker(
        adapter,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
    )
    approval = ExecutionApproval(
        action_id=plan.action_id,
        approved_by="data-owner",
        approved_at=BASE_TIME + timedelta(minutes=1),
        expires_at=BASE_TIME + timedelta(minutes=30),
    )

    first = worker.run(plan, store, approval=approval, maximum_polls=1)
    second = worker.run(plan, store, approval=approval, maximum_polls=1)

    assert first == second
    assert len(adapter.updates) == 1
    assert store.get_external_id(plan.action_id, worker.STEP) == first
    assert store.get_state(plan.action_id, worker.STEP) == worker.COMPLETED


def test_human_lane_refuses_execution_without_attestation(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    store = InMemoryIncidentStore()

    with pytest.raises(PermissionError, match="requires human approval"):
        RecoveryWorker(clock=FrozenClock(BASE_TIME)).run(
            plan,
            store,
            maximum_polls=1,
        )

    assert store.snapshot() == {}
    assert store.get_state(plan.action_id, RecoveryWorker.STEP) is None


def test_predicate_preserves_exact_microsecond_bounds(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    start = plan.backfill_start + timedelta(microseconds=999)
    exact = plan.model_copy(update={"backfill_start": start})
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    start_us = (start - epoch) // timedelta(microseconds=1)

    predicate = bounded_predicate(exact)

    assert f"timestamp_micros({start_us})" in predicate
    assert "payload_version" not in predicate


class AcceptedThenLostAdapter(DryRunPipelineAdapter):
    """Simulate an accepted update whose HTTP response never reaches us."""

    def __init__(self) -> None:
        super().__init__()
        self.start_calls = 0

    def start_bounded_update(self, *, plan, predicate, action_id):
        self.start_calls += 1
        super().start_bounded_update(
            plan=plan,
            predicate=predicate,
            action_id=action_id,
        )
        raise TimeoutError("response lost after acceptance")


def test_lost_start_response_reconciles_without_a_second_start(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    adapter = AcceptedThenLostAdapter()
    store = InMemoryIncidentStore()

    update_id = RecoveryWorker(
        adapter,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
    ).run(
        plan,
        store,
        approval=approval,
        maximum_polls=1,
    )

    assert update_id.startswith("dry-")
    assert adapter.start_calls == 1
    assert store.get_state(plan.action_id, RecoveryWorker.STEP) == RecoveryWorker.COMPLETED


class AmbiguousAdapter:
    def __init__(self) -> None:
        self.start_calls = 0

    def start_bounded_update(self, *, plan, predicate, action_id):
        del plan, predicate, action_id
        self.start_calls += 1
        raise TimeoutError("response lost")

    def find_updates(self, *, pipeline_id, action_id):
        del pipeline_id, action_id
        return ["u1", "u2"]

    def get_update_state(self, *, pipeline_id, update_id):
        raise AssertionError(f"must not poll ambiguous update {pipeline_id}/{update_id}")


def test_ambiguous_acceptance_becomes_exclusive_indeterminate_state(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    adapter = AmbiguousAdapter()
    store = InMemoryIncidentStore()
    worker = RecoveryWorker(
        adapter,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
    )

    with pytest.raises(RuntimeError, match="acceptance is indeterminate"):
        worker.run(plan, store, approval=approval, maximum_polls=1)
    with pytest.raises(RuntimeError, match="acceptance is indeterminate"):
        worker.run(plan, store, approval=approval, maximum_polls=1)

    assert adapter.start_calls == 1
    assert store.get_state(plan.action_id, worker.STEP) == worker.INDETERMINATE


def test_expired_persisted_plan_cannot_start_an_update(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    plan = CompiledPlan.model_validate_json(plan.model_dump_json())
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    worker = RecoveryWorker(
        adapter,
        clock=FrozenClock(plan.execution_deadline),
    )

    with pytest.raises(PermissionError, match="authority has expired"):
        worker.run(plan, store, approval=approval, maximum_polls=1)

    assert adapter.updates == {}
    assert store.snapshot() == {}
    assert store.get_state(plan.action_id, worker.STEP) is None


def test_deadline_is_rechecked_after_journaling_before_external_start(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    worker = RecoveryWorker(
        adapter,
        clock=SequenceClock(
            BASE_TIME + timedelta(minutes=1),
            plan.execution_deadline,
        ),
    )

    with pytest.raises(PermissionError, match="authority has expired"):
        worker.run(plan, store, approval=approval, maximum_polls=1)

    assert adapter.updates == {}
    assert store.get_state(plan.action_id, worker.STEP) == worker.EXPIRED
    assert store.has_stage(plan.action_id, f"{worker.STEP}_EXPIRED")


def test_future_dated_approval_is_rejected_before_journaling(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    store = InMemoryIncidentStore()
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME + timedelta(seconds=1),
        BASE_TIME + timedelta(minutes=30),
    )

    with pytest.raises(ValueError, match="cannot be in the future"):
        RecoveryWorker(clock=FrozenClock(BASE_TIME)).run(
            plan,
            store,
            approval=approval,
            maximum_polls=1,
        )

    assert store.snapshot() == {}


def test_expired_persisted_approval_cannot_authorize_a_fresh_start(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    store = InMemoryIncidentStore()
    approval = ExecutionApproval(
        plan.action_id,
        "data-owner",
        BASE_TIME,
        BASE_TIME + timedelta(minutes=2),
    )
    store.put_stage(plan.action_id, "HUMAN_APPROVED", approval.to_payload())
    adapter = DryRunPipelineAdapter()

    with pytest.raises(PermissionError, match="approval has expired"):
        RecoveryWorker(
            adapter,
            clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
        ).run(plan, store, approval=None, maximum_polls=1)

    assert adapter.updates == {}
    assert store.get_state(plan.action_id, RecoveryWorker.STEP) is None


def test_tampered_persisted_plan_fails_authority_seal_before_start(
    evidence,
    repair_intent,
    policy,
) -> None:
    plan = compile_intent(evidence, repair_intent, policy, now=BASE_TIME)
    persisted = plan.model_dump(mode="json")
    extended = (plan.execution_deadline + timedelta(hours=2)).isoformat()
    persisted["policy_valid_until"] = extended
    persisted["evidence_expires_at"] = extended
    persisted["execution_deadline"] = extended
    with pytest.raises(ValueError, match="action ID does not match"):
        CompiledPlan.model_validate(persisted)
    # Construct through model_copy to simulate an unsafe deserializer that did
    # not validate. The worker's mandatory JSON round-trip must still catch it.
    tampered = plan.model_copy(
        update={
            "policy_valid_until": plan.execution_deadline + timedelta(hours=2),
            "evidence_expires_at": plan.execution_deadline + timedelta(hours=2),
            "execution_deadline": plan.execution_deadline + timedelta(hours=2),
        }
    )
    adapter = DryRunPipelineAdapter()

    with pytest.raises(ValueError, match="action ID does not match"):
        RecoveryWorker(
            adapter,
            clock=FrozenClock(BASE_TIME),
        ).run(tampered, InMemoryIncidentStore(), maximum_polls=1)

    assert adapter.updates == {}
