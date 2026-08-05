"""Idempotent recovery worker over injected pipeline adapters."""

from __future__ import annotations

import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from .authority import (
    ApprovalAuthorityVerifier,
    ExecutionApproval,
    PlanAuthorityVerifier,
)
from .incident_store import IncidentStore, LeaseClaim
from .policy import CompiledPlan


SAFE_PARTNER = re.compile(r"[a-z0-9_]{1,64}")
TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}


class PipelineAdapter(Protocol):
    """Narrow side-effect port implemented by dry-run or cloud adapters."""

    def start_bounded_update(
        self,
        *,
        plan: CompiledPlan,
        predicate: str,
        action_id: str,
    ) -> str: ...

    def find_updates(self, *, pipeline_id: str, action_id: str) -> list[str]: ...

    def get_update_state(self, *, pipeline_id: str, update_id: str) -> str: ...


class Sleeper(Protocol):
    def sleep(self, seconds: float) -> None: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemSleeper:
    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class UtcClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def bounded_predicate(plan: CompiledPlan) -> str:
    """Compile a fixed-shape predicate from registry-bound plan values."""

    if plan.target_predicate_dimensions != ("event_time", "partner"):
        raise ValueError("unexpected predicate dimensions")
    if not SAFE_PARTNER.fullmatch(plan.partner):
        raise ValueError("partner is unsafe for the fixed predicate template")
    start = plan.backfill_start.astimezone(timezone.utc)
    end = plan.backfill_end.astimezone(timezone.utc)
    if end <= start:
        raise ValueError("compiled interval is empty or inverted")
    # Integer calendar arithmetic preserves microseconds. Float timestamps and
    # millisecond truncation could silently widen a policy-approved interval.
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    start_us = (start - epoch) // timedelta(microseconds=1)
    end_us = (end - epoch) // timedelta(microseconds=1)
    return (
        f"partner = '{plan.partner}' "
        f"AND event_time >= timestamp_micros({start_us}) "
        f"AND event_time < timestamp_micros({end_us})"
    )


@dataclass
class DryRunPipelineAdapter:
    """Default adapter: records local calls and reports immediate completion."""

    updates: dict[str, tuple[str, str]]

    def __init__(self) -> None:
        self.updates = {}

    def start_bounded_update(
        self,
        *,
        plan: CompiledPlan,
        predicate: str,
        action_id: str,
    ) -> str:
        del predicate
        update_id = f"dry-{action_id[-12:]}"
        self.updates[update_id] = (plan.pipeline_id, action_id)
        return update_id

    def find_updates(self, *, pipeline_id: str, action_id: str) -> list[str]:
        return [
            update_id
            for update_id, (stored_pipeline, stored_action) in self.updates.items()
            if stored_pipeline == pipeline_id and stored_action == action_id
        ]

    def get_update_state(self, *, pipeline_id: str, update_id: str) -> str:
        stored = self.updates.get(update_id)
        if stored is None or stored[0] != pipeline_id:
            raise LookupError("dry-run update does not exist")
        return "COMPLETED"


class RecoveryWorker:
    """Reconcile before retrying and advance only on a completed update."""

    STEP = "PIPELINE_UPDATE"
    REQUESTING = "REQUESTING"
    ACCEPTED = "ACCEPTED"
    COMPLETED = "COMPLETED"
    INDETERMINATE = "INDETERMINATE"
    EXPIRED = "EXPIRED"

    def __init__(
        self,
        adapter: PipelineAdapter | None = None,
        *,
        sleeper: Sleeper | None = None,
        clock: Clock | None = None,
        poll_interval_seconds: float = 5,
        operation_lease_duration: timedelta = timedelta(minutes=1),
        plan_authority: PlanAuthorityVerifier | None = None,
        approval_authority: ApprovalAuthorityVerifier | None = None,
    ) -> None:
        self.adapter = adapter or DryRunPipelineAdapter()
        self.sleeper = sleeper or SystemSleeper()
        self.clock = clock or UtcClock()
        self.poll_interval_seconds = poll_interval_seconds
        if operation_lease_duration <= timedelta(0):
            raise ValueError("operation lease duration must be positive")
        self.operation_lease_duration = operation_lease_duration
        self.plan_authority = plan_authority
        self.approval_authority = approval_authority

    def _now(self) -> datetime:
        value = self.clock.now()
        if value.tzinfo is None:
            raise ValueError("worker clock must return a timezone-aware instant")
        return value.astimezone(timezone.utc)

    def _assert_live(self, plan: CompiledPlan, *, now: datetime) -> None:
        if now >= plan.execution_deadline.astimezone(timezone.utc):
            raise PermissionError("compiled recovery authority has expired")

    def assert_plan_authorized(self, plan: CompiledPlan) -> None:
        """Authenticate a plan through the injected trust root, or fail closed."""

        if self.plan_authority is None:
            raise PermissionError("compiled plan authority verifier is unavailable")
        self.plan_authority.verify_plan(plan)

    def _record_or_validate_approval(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        approval: ExecutionApproval | None,
        *,
        now: datetime,
        require_current: bool = True,
    ) -> None:
        if not plan.requires_human:
            return
        if self.approval_authority is None:
            raise PermissionError("approval authority verifier is unavailable")
        stage = "HUMAN_APPROVED"
        if approval is None:
            if not store.has_stage(plan.action_id, stage):
                raise PermissionError("this recovery plan requires human approval")
            approval = ExecutionApproval.model_validate(
                store.get_stage(plan.action_id, stage)
            )
        self.approval_authority.verify_approval(
            approval,
            plan,
            now=now,
            require_current=require_current,
        )
        store.put_stage(
            plan.action_id,
            stage,
            approval.model_dump(mode="json"),
        )

    def assert_mutation_authorized(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        approval: ExecutionApproval | None,
        *,
        now: datetime,
        require_current_approval: bool = True,
    ) -> None:
        """Authenticate every authority immediately before an external effect."""

        self.assert_plan_authorized(plan)
        self._assert_live(plan, now=now)
        self._record_or_validate_approval(
            plan,
            store,
            approval,
            now=now,
            require_current=require_current_approval,
        )

    def _acquire_operation_lease(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        *,
        now: datetime,
    ) -> LeaseClaim:
        claim = store.acquire_lease(
            plan.action_id,
            self.STEP,
            owner_id=secrets.token_hex(16),
            now=now,
            duration=self.operation_lease_duration,
        )
        if claim is None:
            raise RuntimeError("recovery action is held by another live worker")
        return claim

    def _reconcile(self, plan: CompiledPlan) -> str:
        matches = self.adapter.find_updates(
            pipeline_id=plan.pipeline_id,
            action_id=plan.action_id,
        )
        if len(matches) != 1:
            raise RuntimeError(f"update reconciliation found {len(matches)} candidates")
        return matches[0]

    def _wait(self, plan: CompiledPlan, update_id: str, maximum_polls: int) -> str:
        for attempt in range(maximum_polls):
            state = self.adapter.get_update_state(
                pipeline_id=plan.pipeline_id,
                update_id=update_id,
            )
            if state in TERMINAL_STATES:
                if state != "COMPLETED":
                    raise RuntimeError(f"pipeline update {update_id} ended in {state}")
                return update_id
            if attempt + 1 < maximum_polls:
                self.sleeper.sleep(self.poll_interval_seconds)
        raise TimeoutError(f"pipeline update {update_id} is still non-terminal")

    def _record_completion(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        update_id: str,
    ) -> str:
        """Commit one terminal success without creating competing states."""

        current = store.get_state(plan.action_id, self.STEP)
        if current != self.COMPLETED:
            if current not in {self.REQUESTING, self.ACCEPTED}:
                raise RuntimeError(f"cannot complete pipeline update from state {current!r}")
            if not store.compare_and_set_state(
                plan.action_id,
                self.STEP,
                current,
                self.COMPLETED,
            ):
                raise RuntimeError("pipeline update state changed while completing")
        store.put_stage(
            plan.action_id,
            f"{self.STEP}_COMPLETED",
            {"update_id": update_id},
        )
        return update_id

    def run(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        *,
        approval: ExecutionApproval | None = None,
        maximum_polls: int = 720,
    ) -> str:
        """Start once, reconcile an ambiguous response, then wait for success."""

        # Pydantic's ``model_copy(update=...)`` can bypass validation. Round-trip
        # through JSON data so every actuator call rechecks the authority seal.
        plan = CompiledPlan.model_validate(plan.model_dump(mode="json"))
        self.assert_plan_authorized(plan)
        if maximum_polls < 1:
            raise ValueError("maximum_polls must be positive")
        # Validate deterministic local inputs before creating a REQUESTING
        # journal entry. Only transport ambiguity should enter reconciliation.
        predicate = bounded_predicate(plan)
        state = store.get_state(plan.action_id, self.STEP)
        if state == self.EXPIRED:
            raise PermissionError("compiled recovery authority has expired")
        if state == self.INDETERMINATE:
            raise RuntimeError("pipeline update acceptance is indeterminate")
        existing = store.get_external_id(plan.action_id, self.STEP)
        if existing:
            if state == self.COMPLETED:
                return existing
            self._record_or_validate_approval(
                plan,
                store,
                approval,
                now=self._now(),
                require_current=False,
            )
            if state in {None, self.REQUESTING}:
                if not store.compare_and_set_state(
                    plan.action_id,
                    self.STEP,
                    state,
                    self.ACCEPTED,
                ):
                    raise RuntimeError("pipeline update state changed during reconciliation")
            completed = self._wait(plan, existing, maximum_polls)
            return self._record_completion(plan, store, completed)

        if state == self.REQUESTING:
            reconcile_now = self._now()
            claim = self._acquire_operation_lease(plan, store, now=reconcile_now)
            try:
                self._record_or_validate_approval(
                    plan,
                    store,
                    approval,
                    now=reconcile_now,
                    require_current=False,
                )
                try:
                    update_id = self._reconcile(plan)
                except Exception as reconcile_error:
                    store.put_stage(
                        plan.action_id,
                        f"{self.STEP}_INDETERMINATE",
                        {"reason": "restart reconciliation did not find exactly one update"},
                    )
                    store.compare_and_set_state(
                        plan.action_id,
                        self.STEP,
                        self.REQUESTING,
                        self.INDETERMINATE,
                    )
                    raise RuntimeError(
                        "pipeline update acceptance is indeterminate"
                    ) from reconcile_error
                store.put_external_id(plan.action_id, self.STEP, update_id)
                if not store.compare_and_set_state(
                    plan.action_id,
                    self.STEP,
                    self.REQUESTING,
                    self.ACCEPTED,
                ):
                    raise RuntimeError("pipeline update state changed during reconciliation")
                completed = self._wait(plan, update_id, maximum_polls)
                return self._record_completion(plan, store, completed)
            finally:
                store.release_lease(plan.action_id, self.STEP, claim)
        if state is not None:
            raise RuntimeError(f"pipeline update has inconsistent state {state!r}")

        initial_now = self._now()
        claim = self._acquire_operation_lease(plan, store, now=initial_now)
        try:
            self._assert_live(plan, now=initial_now)
            self._record_or_validate_approval(
                plan,
                store,
                approval,
                now=initial_now,
            )

            if not store.compare_and_set_state(
                plan.action_id,
                self.STEP,
                None,
                self.REQUESTING,
            ):
                raise RuntimeError("another worker claimed this recovery action")

            store.put_stage(
                plan.action_id,
                f"{self.STEP}_REQUESTING",
                {"recovery_action_id": plan.action_id},
            )
            # Recheck authentication, expiry, and the fencing lease at the last
            # possible safe boundary before calling the external API.
            try:
                final_now = self._now()
                self.assert_mutation_authorized(
                    plan,
                    store,
                    approval,
                    now=final_now,
                    require_current_approval=True,
                )
                store.assert_lease(
                    plan.action_id,
                    self.STEP,
                    claim,
                    now=final_now,
                )
            except PermissionError:
                store.compare_and_set_state(
                    plan.action_id,
                    self.STEP,
                    self.REQUESTING,
                    self.EXPIRED,
                )
                store.put_stage(
                    plan.action_id,
                    f"{self.STEP}_EXPIRED",
                    {
                        "deadline": plan.execution_deadline.isoformat(),
                        "reason": "execution or approval authority expired",
                    },
                )
                raise
            try:
                update_id = self.adapter.start_bounded_update(
                    plan=plan,
                    predicate=predicate,
                    action_id=plan.action_id,
                )
            except Exception:
                try:
                    update_id = self._reconcile(plan)
                except Exception as reconcile_error:
                    store.put_stage(
                        plan.action_id,
                        f"{self.STEP}_INDETERMINATE",
                        {"reason": "start response unavailable; reconciliation not unique"},
                    )
                    store.compare_and_set_state(
                        plan.action_id,
                        self.STEP,
                        self.REQUESTING,
                        self.INDETERMINATE,
                    )
                    raise RuntimeError(
                        "pipeline update acceptance is indeterminate"
                    ) from reconcile_error

            store.put_external_id(plan.action_id, self.STEP, update_id)
            if not store.compare_and_set_state(
                plan.action_id,
                self.STEP,
                self.REQUESTING,
                self.ACCEPTED,
            ):
                raise RuntimeError("pipeline update state changed after acceptance")
            completed = self._wait(plan, update_id, maximum_polls)
            return self._record_completion(plan, store, completed)
        finally:
            store.release_lease(plan.action_id, self.STEP, claim)
