"""Idempotent recovery worker over injected pipeline adapters."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from .incident_store import IncidentStore
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


@dataclass(frozen=True)
class ExecutionApproval:
    """Minimal attestation required by plans outside the auto lane."""

    action_id: str
    approved_by: str
    approved_at: datetime
    expires_at: datetime

    def validate_for(
        self,
        plan: CompiledPlan,
        *,
        now: datetime,
        require_current: bool = True,
    ) -> None:
        if self.action_id != plan.action_id:
            raise ValueError("approval belongs to a different recovery action")
        if not self.approved_by.strip():
            raise ValueError("approval must identify an approver")
        if self.approved_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("approval timestamps must include a timezone")
        current = now.astimezone(timezone.utc)
        approved = self.approved_at.astimezone(timezone.utc)
        expires = self.expires_at.astimezone(timezone.utc)
        if approved > current:
            raise ValueError("approval timestamp cannot be in the future")
        if expires <= approved:
            raise ValueError("approval expiry must be after approval time")
        if expires > plan.execution_deadline.astimezone(timezone.utc):
            raise ValueError("approval cannot outlive compiled execution authority")
        if approved >= plan.execution_deadline.astimezone(timezone.utc):
            raise ValueError("approval was issued after execution authority expired")
        if require_current and current >= expires:
            raise PermissionError("human approval has expired")

    def to_payload(self) -> dict[str, str]:
        return {
            "action_id": self.action_id,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at.astimezone(timezone.utc).isoformat(),
            "expires_at": self.expires_at.astimezone(timezone.utc).isoformat(),
        }

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> "ExecutionApproval":
        required = {"action_id", "approved_by", "approved_at", "expires_at"}
        if set(payload) != required:
            raise ValueError("persisted approval has an unexpected schema")
        try:
            return cls(
                action_id=str(payload["action_id"]),
                approved_by=str(payload["approved_by"]),
                approved_at=datetime.fromisoformat(str(payload["approved_at"])),
                expires_at=datetime.fromisoformat(str(payload["expires_at"])),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("persisted approval is malformed") from exc


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
    ) -> None:
        self.adapter = adapter or DryRunPipelineAdapter()
        self.sleeper = sleeper or SystemSleeper()
        self.clock = clock or UtcClock()
        self.poll_interval_seconds = poll_interval_seconds

    def _now(self) -> datetime:
        value = self.clock.now()
        if value.tzinfo is None:
            raise ValueError("worker clock must return a timezone-aware instant")
        return value.astimezone(timezone.utc)

    def _assert_live(self, plan: CompiledPlan, *, now: datetime) -> None:
        if now >= plan.execution_deadline.astimezone(timezone.utc):
            raise PermissionError("compiled recovery authority has expired")

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
        stage = "HUMAN_APPROVED"
        if approval is None:
            if not store.has_stage(plan.action_id, stage):
                raise PermissionError("this recovery plan requires human approval")
            approval = ExecutionApproval.from_payload(
                store.get_stage(plan.action_id, stage)
            )
        approval.validate_for(plan, now=now, require_current=require_current)
        store.put_stage(
            plan.action_id,
            stage,
            approval.to_payload(),
        )

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
            self._record_or_validate_approval(
                plan,
                store,
                approval,
                now=self._now(),
                require_current=False,
            )
            update_id = self._reconcile(plan)
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
        if state is not None:
            raise RuntimeError(f"pipeline update has inconsistent state {state!r}")

        initial_now = self._now()
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
        # Authority can expire between local validation/journaling and the
        # external call. Check again at the last possible safe boundary.
        try:
            self._assert_live(plan, now=self._now())
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
                {"deadline": plan.execution_deadline.isoformat()},
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
                raise RuntimeError("pipeline update acceptance is indeterminate") from reconcile_error

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
