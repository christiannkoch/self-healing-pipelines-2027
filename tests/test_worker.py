from datetime import datetime, timedelta, timezone

import pytest

from recovery_control_plane.authority import HmacRecoveryAuthority
from recovery_control_plane.incident_store import InMemoryIncidentStore
from recovery_control_plane.policy import CompiledPlan, compile_intent
from recovery_control_plane.worker import (
    bounded_predicate,
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
            raise AssertionError("worker clock read more often than expected")
        return self.instants.pop(0)


def compile_plan(authority, evidence, repair_intent, policy):
    return compile_intent(
        evidence,
        repair_intent,
        policy,
        now=BASE_TIME,
        authority_signer=authority,
    )


def approve(authority, plan, approved_at, expires_at, *, record_id="approval-1"):
    return authority.issue_approval(
        plan=plan,
        record_id=record_id,
        approved_by="data-owner",
        approved_at=approved_at,
        expires_at=expires_at,
    )


def secure_worker(authority, adapter=None, **kwargs):
    return RecoveryWorker(
        adapter,
        plan_authority=authority,
        approval_authority=authority,
        **kwargs,
    )


def test_worker_reuses_one_local_update(evidence, repair_intent, policy, authority) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    plan = CompiledPlan.model_validate_json(plan.model_dump_json())
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    worker = secure_worker(
        authority,
        adapter,
        clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
    )
    approval = approve(
        authority,
        plan,
        BASE_TIME + timedelta(minutes=1),
        BASE_TIME + timedelta(minutes=30),
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
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    store = InMemoryIncidentStore()

    with pytest.raises(PermissionError, match="requires human approval"):
        secure_worker(authority, clock=FrozenClock(BASE_TIME)).run(
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
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
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
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    adapter = AcceptedThenLostAdapter()
    store = InMemoryIncidentStore()

    update_id = secure_worker(
        authority,
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
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    adapter = AmbiguousAdapter()
    store = InMemoryIncidentStore()
    worker = secure_worker(
        authority,
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
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    plan = CompiledPlan.model_validate_json(plan.model_dump_json())
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    worker = secure_worker(
        authority,
        adapter,
        clock=FrozenClock(plan.execution_deadline),
    )

    with pytest.raises(PermissionError, match="authority has expired"):
        worker.run(plan, store, approval=approval, maximum_polls=1)

    assert adapter.updates == {}
    assert store.snapshot() == {}
    assert store.get_state(plan.action_id, worker.STEP) is None


def test_worker_fails_closed_without_plan_authority_verifier(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)

    with pytest.raises(PermissionError, match="verifier is unavailable"):
        RecoveryWorker(clock=FrozenClock(BASE_TIME)).run(
            plan,
            InMemoryIncidentStore(),
            maximum_polls=1,
        )


def test_recomputed_action_id_cannot_replace_compiler_signature(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    changed = plan.model_copy(update={"requires_human": False})
    from recovery_control_plane.policy import _action_id

    forged = changed.model_copy(update={"action_id": _action_id(changed.authority_payload())})
    adapter = DryRunPipelineAdapter()

    with pytest.raises(PermissionError, match="signature is invalid"):
        secure_worker(
            authority,
            adapter,
            clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
        ).run(forged, InMemoryIncidentStore(), maximum_polls=1)

    assert adapter.updates == {}


def test_self_asserted_or_modified_approval_is_rejected(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    signed = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    forged = signed.model_copy(update={"approved_by": "attacker-asserted-owner"})
    adapter = DryRunPipelineAdapter()

    with pytest.raises(PermissionError, match="approval signature is invalid"):
        secure_worker(
            authority,
            adapter,
            clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
        ).run(plan, InMemoryIncidentStore(), approval=forged, maximum_polls=1)

    assert adapter.updates == {}


def test_untrusted_approval_issuer_is_rejected(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    untrusted = HmacRecoveryAuthority(
        key_id="untrusted-key",
        issuer="untrusted-issuer",
        secret=b"different-test-only-key-material-000000001",
    )
    approval = untrusted.issue_approval(
        plan=plan,
        record_id="self-issued-approval",
        approved_by="attacker",
        approved_at=BASE_TIME,
        expires_at=BASE_TIME + timedelta(minutes=30),
    )

    with pytest.raises(PermissionError, match="untrusted authority"):
        secure_worker(
            authority,
            clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
        ).run(plan, InMemoryIncidentStore(), approval=approval, maximum_polls=1)


def test_approval_is_rechecked_at_final_pre_call_boundary(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=2),
    )
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    worker = secure_worker(
        authority,
        adapter,
        clock=SequenceClock(
            BASE_TIME + timedelta(minutes=1),
            BASE_TIME + timedelta(minutes=2),
        ),
    )

    with pytest.raises(PermissionError, match="approval has expired"):
        worker.run(plan, store, approval=approval, maximum_polls=1)

    assert adapter.updates == {}
    assert store.get_state(plan.action_id, worker.STEP) == worker.EXPIRED


class NoMatchReconciliationAdapter(DryRunPipelineAdapter):
    def find_updates(self, *, pipeline_id, action_id):
        del pipeline_id, action_id
        return []


def test_zero_match_restart_reconciliation_becomes_indeterminate(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    store = InMemoryIncidentStore()
    assert store.compare_and_set_state(
        plan.action_id,
        RecoveryWorker.STEP,
        None,
        RecoveryWorker.REQUESTING,
    )
    worker = secure_worker(
        authority,
        NoMatchReconciliationAdapter(),
        clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
    )

    with pytest.raises(RuntimeError, match="acceptance is indeterminate"):
        worker.run(plan, store, approval=approval, maximum_polls=1)

    assert store.get_state(plan.action_id, worker.STEP) == worker.INDETERMINATE


def test_live_requesting_lease_blocks_a_competing_reconciler(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    store = InMemoryIncidentStore()
    assert store.compare_and_set_state(
        plan.action_id,
        RecoveryWorker.STEP,
        None,
        RecoveryWorker.REQUESTING,
    )
    assert store.acquire_lease(
        plan.action_id,
        RecoveryWorker.STEP,
        owner_id="first-worker",
        now=BASE_TIME,
        duration=timedelta(minutes=5),
    ) is not None
    adapter = NoMatchReconciliationAdapter()

    with pytest.raises(RuntimeError, match="another live worker"):
        secure_worker(
            authority,
            adapter,
            clock=FrozenClock(BASE_TIME + timedelta(minutes=1)),
        ).run(plan, store, maximum_polls=1)

    assert store.get_state(plan.action_id, RecoveryWorker.STEP) == RecoveryWorker.REQUESTING
    assert adapter.updates == {}


def test_expired_fence_prevents_stale_worker_start_then_allows_safe_reconciliation(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    stale = secure_worker(
        authority,
        adapter,
        operation_lease_duration=timedelta(minutes=1),
        clock=SequenceClock(BASE_TIME, BASE_TIME + timedelta(minutes=1)),
    )

    with pytest.raises(RuntimeError, match="fencing lease expired"):
        stale.run(plan, store, approval=approval, maximum_polls=1)

    assert adapter.updates == {}
    assert store.get_state(plan.action_id, stale.STEP) == stale.REQUESTING

    recovered = secure_worker(
        authority,
        NoMatchReconciliationAdapter(),
        clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
    )
    with pytest.raises(RuntimeError, match="acceptance is indeterminate"):
        recovered.run(plan, store, approval=approval, maximum_polls=1)
    assert store.get_state(plan.action_id, recovered.STEP) == recovered.INDETERMINATE


def test_deadline_is_rechecked_after_journaling_before_external_start(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    adapter = DryRunPipelineAdapter()
    store = InMemoryIncidentStore()
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=30),
    )
    worker = secure_worker(
        authority,
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
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    store = InMemoryIncidentStore()
    approval = approve(
        authority,
        plan,
        BASE_TIME + timedelta(seconds=1),
        BASE_TIME + timedelta(minutes=30),
    )

    with pytest.raises(ValueError, match="cannot be in the future"):
        secure_worker(authority, clock=FrozenClock(BASE_TIME)).run(
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
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
    store = InMemoryIncidentStore()
    approval = approve(
        authority,
        plan,
        BASE_TIME,
        BASE_TIME + timedelta(minutes=2),
    )
    store.put_stage(
        plan.action_id,
        "HUMAN_APPROVED",
        approval.model_dump(mode="json"),
    )
    adapter = DryRunPipelineAdapter()

    with pytest.raises(PermissionError, match="approval has expired"):
        secure_worker(
            authority,
            adapter,
            clock=FrozenClock(BASE_TIME + timedelta(minutes=2)),
        ).run(plan, store, approval=None, maximum_polls=1)

    assert adapter.updates == {}
    assert store.get_state(plan.action_id, RecoveryWorker.STEP) is None


def test_tampered_persisted_plan_fails_authority_seal_before_start(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    plan = compile_plan(authority, evidence, repair_intent, policy)
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
        secure_worker(
            authority,
            adapter,
            clock=FrozenClock(BASE_TIME),
        ).run(tampered, InMemoryIncidentStore(), maximum_polls=1)

    assert adapter.updates == {}
