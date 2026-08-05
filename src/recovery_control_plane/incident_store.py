"""Immutable incident-journal ports and an offline in-memory implementation."""

from __future__ import annotations

import json
from threading import RLock
from typing import Any, Protocol


class StageConflict(RuntimeError):
    """Raised when one immutable stage key is reused with different evidence."""


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


class InMemoryIncidentStore:
    """Thread-safe local journal with the same immutable-key semantics as Delta."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], str] = {}
        self._states: dict[tuple[str, str], str] = {}
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

    def snapshot(self) -> dict[tuple[str, str], dict[str, Any]]:
        """Return a copy suitable for assertions and local demonstrations."""

        with self._lock:
            return {key: json.loads(value) for key, value in self._rows.items()}

    def state_snapshot(self) -> dict[tuple[str, str], str]:
        """Return exclusive operation states for diagnostics and tests."""

        with self._lock:
            return dict(self._states)
