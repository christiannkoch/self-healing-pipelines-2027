"""Immutable incident-journal ports and an offline in-memory implementation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any, Protocol


class StageConflict(RuntimeError):
    """Raised when one immutable stage key is reused with different evidence."""


@dataclass(frozen=True)
class LeaseClaim:
    """One exclusive operation lease with a monotonically increasing fence."""

    owner_id: str
    fencing_token: int
    expires_at: datetime


class IncidentStore(Protocol):
    def put_stage(self, action_id: str, stage: str, payload: dict[str, Any]) -> None: ...

    def has_stage(self, action_id: str, stage: str) -> bool: ...

    def get_stage(self, action_id: str, stage: str) -> dict[str, Any]: ...

    def put_external_id(self, action_id: str, step: str, external_id: str) -> None: ...

    def get_external_id(self, action_id: str, step: str) -> str | None: ...

    def compare_and_set_state(
        self,
        action_id: str,
        step: str,
        expected: str | None,
        new: str,
    ) -> bool: ...

    def get_state(self, action_id: str, step: str) -> str | None: ...

    def acquire_lease(
        self,
        action_id: str,
        step: str,
        *,
        owner_id: str,
        now: datetime,
        duration: timedelta,
    ) -> LeaseClaim | None: ...

    def assert_lease(
        self,
        action_id: str,
        step: str,
        claim: LeaseClaim,
        *,
        now: datetime,
    ) -> None: ...

    def release_lease(self, action_id: str, step: str, claim: LeaseClaim) -> bool: ...


class InMemoryIncidentStore:
    """Thread-safe local journal with the same immutable-key semantics as Delta."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], str] = {}
        self._states: dict[tuple[str, str], str] = {}
        self._leases: dict[tuple[str, str], LeaseClaim] = {}
        self._fencing_tokens: dict[tuple[str, str], int] = {}
        self._lock = RLock()

    def put_stage(self, action_id: str, stage: str, payload: dict[str, Any]) -> None:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        key = (action_id, stage)
        with self._lock:
            existing = self._rows.get(key)
            if existing is not None and existing != canonical:
                raise StageConflict(f"stage {stage!r} already has different evidence")
            self._rows[key] = canonical

    def has_stage(self, action_id: str, stage: str) -> bool:
        with self._lock:
            return (action_id, stage) in self._rows

    def get_stage(self, action_id: str, stage: str) -> dict[str, Any]:
        with self._lock:
            payload = self._rows.get((action_id, stage))
        if payload is None:
            raise LookupError(f"stage {stage!r} does not exist")
        return json.loads(payload)

    def put_external_id(self, action_id: str, step: str, external_id: str) -> None:
        self.put_stage(action_id, f"EXTERNAL_ID:{step}", {"external_id": external_id})

    def get_external_id(self, action_id: str, step: str) -> str | None:
        stage = f"EXTERNAL_ID:{step}"
        if not self.has_stage(action_id, stage):
            return None
        return str(self.get_stage(action_id, stage)["external_id"])

    def compare_and_set_state(
        self,
        action_id: str,
        step: str,
        expected: str | None,
        new: str,
    ) -> bool:
        """Atomically move one operation between mutually exclusive states."""

        key = (action_id, step)
        with self._lock:
            if self._states.get(key) != expected:
                return False
            self._states[key] = new
            return True

    def get_state(self, action_id: str, step: str) -> str | None:
        with self._lock:
            return self._states.get((action_id, step))

    def acquire_lease(
        self,
        action_id: str,
        step: str,
        *,
        owner_id: str,
        now: datetime,
        duration: timedelta,
    ) -> LeaseClaim | None:
        if now.tzinfo is None or duration <= timedelta(0):
            raise ValueError("lease acquisition requires an aware time and positive duration")
        if not owner_id:
            raise ValueError("lease owner must be non-empty")
        current = now.astimezone(timezone.utc)
        key = (action_id, step)
        with self._lock:
            existing = self._leases.get(key)
            if existing is not None and existing.expires_at > current:
                return None
            token = self._fencing_tokens.get(key, 0) + 1
            claim = LeaseClaim(owner_id, token, current + duration)
            self._fencing_tokens[key] = token
            self._leases[key] = claim
            return claim

    def assert_lease(
        self,
        action_id: str,
        step: str,
        claim: LeaseClaim,
        *,
        now: datetime,
    ) -> None:
        if now.tzinfo is None:
            raise ValueError("lease verification time must include a timezone")
        with self._lock:
            current = self._leases.get((action_id, step))
            if current != claim:
                raise RuntimeError("recovery operation lost its fencing lease")
            if current.expires_at <= now.astimezone(timezone.utc):
                raise RuntimeError("recovery operation fencing lease expired")

    def release_lease(self, action_id: str, step: str, claim: LeaseClaim) -> bool:
        key = (action_id, step)
        with self._lock:
            if self._leases.get(key) != claim:
                return False
            del self._leases[key]
            return True

    def snapshot(self) -> dict[tuple[str, str], dict[str, Any]]:
        """Return a copy suitable for assertions and local demonstrations."""

        with self._lock:
            return {key: json.loads(value) for key, value in self._rows.items()}

    def state_snapshot(self) -> dict[tuple[str, str], str]:
        """Return exclusive operation states for diagnostics and tests."""

        with self._lock:
            return dict(self._states)
