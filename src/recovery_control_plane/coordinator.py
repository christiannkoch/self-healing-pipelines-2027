"""Execute, independently verify, and only then promote a repair candidate."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .incident_store import IncidentStore
from .normalization import CanonicalOrder
from .policy import CompiledPlan
from .verifier import (
    BusinessKey,
    SettlementRecord,
    VerificationMetrics,
    verify_candidate,
)
from .worker import Clock, ExecutionApproval, RecoveryWorker, UtcClock


VERIFIER_VERSION = "independent-verifier/1"


@dataclass(frozen=True)
class VerificationEvidence:
    """Evidence returned through a query path independent of the actuator."""

    expected_keys: frozenset[BusinessKey]
    expected_v3_keys: frozenset[BusinessKey]
    candidates: tuple[CanonicalOrder, ...]
    settlements: tuple[SettlementRecord, ...]


class VerificationEvidenceProvider(Protocol):
    """Load post-update candidate and independent business evidence."""

    def load(self, *, plan: CompiledPlan, update_id: str) -> VerificationEvidence: ...


def _proof_payload(
    *,
    action_id: str,
    update_id: str,
    profile_digest: str,
    source_fingerprint: str,
    backfill_start: datetime,
    backfill_end: datetime,
    execution_deadline: datetime,
    verified_at: datetime,
    metrics: VerificationMetrics,
) -> dict[str, object]:
    timestamps = (backfill_start, backfill_end, execution_deadline, verified_at)
    if any(value.tzinfo is None for value in timestamps):
        raise ValueError("verification proof timestamps must include a timezone")
    return {
        "action_id": action_id,
        "update_id": update_id,
        "profile_digest": profile_digest,
        "source_fingerprint": source_fingerprint,
        "backfill_start": backfill_start.astimezone(timezone.utc).isoformat(),
        "backfill_end": backfill_end.astimezone(timezone.utc).isoformat(),
        "execution_deadline": execution_deadline.astimezone(timezone.utc).isoformat(),
        "verified_at": verified_at.astimezone(timezone.utc).isoformat(),
        "verifier_version": VERIFIER_VERSION,
        "metrics": metrics.model_dump(mode="json"),
    }


def _proof_digest(**values: object) -> str:
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class VerificationReceipt(BaseModel):
    """Content-addressed proof bound to one plan and one completed update."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["verification-receipt/v1"] = "verification-receipt/v1"
    action_id: str = Field(pattern=r"^rpr-[0-9a-f]{60}$")
    update_id: str = Field(min_length=1, max_length=200)
    profile_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_fingerprint: str = Field(min_length=1, max_length=200)
    backfill_start: datetime
    backfill_end: datetime
    execution_deadline: datetime
    verified_at: datetime
    verifier_version: Literal["independent-verifier/1"] = VERIFIER_VERSION
    metrics: VerificationMetrics
    proof_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_proof(self) -> "VerificationReceipt":
        if self.verified_at.tzinfo is None:
            raise ValueError("verification receipt timestamp must include a timezone")
        expected = _proof_digest(
            **_proof_payload(
                action_id=self.action_id,
                update_id=self.update_id,
                profile_digest=self.profile_digest,
                source_fingerprint=self.source_fingerprint,
                backfill_start=self.backfill_start,
                backfill_end=self.backfill_end,
                execution_deadline=self.execution_deadline,
                verified_at=self.verified_at,
                metrics=self.metrics,
            )
        )
        if self.proof_digest != expected:
            raise ValueError("verification receipt digest does not match its proof")
        return self

    def validate_for(
        self,
        plan: CompiledPlan,
        update_id: str,
        *,
        now: datetime | None = None,
    ) -> None:
        if self.action_id != plan.action_id:
            raise ValueError("verification receipt belongs to a different action")
        if self.update_id != update_id:
            raise ValueError("verification receipt belongs to a different update")
        if self.profile_digest != plan.profile_digest:
            raise ValueError("verification receipt proves a different profile")
        if self.source_fingerprint != plan.source_fingerprint:
            raise ValueError("verification receipt proves a different source")
        if self.backfill_start.astimezone(timezone.utc) != plan.backfill_start.astimezone(
            timezone.utc
        ) or self.backfill_end.astimezone(timezone.utc) != plan.backfill_end.astimezone(
            timezone.utc
        ):
            raise ValueError("verification receipt proves a different window")
        if self.execution_deadline.astimezone(
            timezone.utc
        ) != plan.execution_deadline.astimezone(timezone.utc):
            raise ValueError("verification receipt has a different authority deadline")
        verified = self.verified_at.astimezone(timezone.utc)
        if verified >= plan.execution_deadline.astimezone(timezone.utc):
            raise ValueError("verification occurred after execution authority expired")
        if now is not None and verified > now.astimezone(timezone.utc):
            raise ValueError("verification receipt is future-dated")


def build_verification_receipt(
    *,
    plan: CompiledPlan,
    update_id: str,
    verified_at: datetime,
    metrics: VerificationMetrics,
) -> VerificationReceipt:
    payload = _proof_payload(
        action_id=plan.action_id,
        update_id=update_id,
        profile_digest=plan.profile_digest,
        source_fingerprint=plan.source_fingerprint,
        backfill_start=plan.backfill_start,
        backfill_end=plan.backfill_end,
        execution_deadline=plan.execution_deadline,
        verified_at=verified_at,
        metrics=metrics,
    )
    return VerificationReceipt(
        **payload,
        proof_digest=_proof_digest(**payload),
    )


class PromotionAdapter(Protocol):
    """Idempotently expose a candidate proven by a verification receipt."""

    def promote(self, *, plan: CompiledPlan, receipt: VerificationReceipt) -> str: ...


class DryRunPromotionAdapter:
    """Offline promotion port that refuses unbound or conflicting receipts."""

    def __init__(self) -> None:
        self.promotions: dict[str, tuple[str, str]] = {}

    def promote(self, *, plan: CompiledPlan, receipt: VerificationReceipt) -> str:
        if not isinstance(receipt, VerificationReceipt):
            raise TypeError("promotion requires a typed verification receipt")
        receipt.validate_for(plan, receipt.update_id)
        promotion_id = "prom-" + hashlib.sha256(
            f"{receipt.action_id}:{receipt.update_id}:{receipt.proof_digest}".encode("utf-8")
        ).hexdigest()[:24]
        current = self.promotions.get(plan.action_id)
        proposed = (receipt.proof_digest, promotion_id)
        if current is not None and current != proposed:
            raise RuntimeError("a different proof already promoted this recovery action")
        self.promotions[plan.action_id] = proposed
        return promotion_id


class PromotionReceipt(BaseModel):
    """Immutable journal record for a completed promotion."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["promotion-receipt/v1"] = "promotion-receipt/v1"
    action_id: str = Field(pattern=r"^rpr-[0-9a-f]{60}$")
    update_id: str = Field(min_length=1, max_length=200)
    verification_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    promotion_id: str = Field(min_length=1, max_length=200)
    promoted_at: datetime

    @model_validator(mode="after")
    def validate_timestamp(self) -> "PromotionReceipt":
        if self.promoted_at.tzinfo is None:
            raise ValueError("promotion receipt timestamp must include a timezone")
        return self


@dataclass(frozen=True)
class RecoveryResult:
    update_id: str
    verification: VerificationReceipt
    promotion: PromotionReceipt


class RecoveryCoordinator:
    """Enforce the only safe order: execute, verify, persist proof, promote."""

    VERIFICATION_STAGE = "VERIFICATION_RECEIPT"
    PROMOTION_STAGE = "PROMOTION_RECEIPT"

    def __init__(
        self,
        *,
        worker: RecoveryWorker,
        evidence_provider: VerificationEvidenceProvider,
        promotion_adapter: PromotionAdapter | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.worker = worker
        self.evidence_provider = evidence_provider
        self.promotion_adapter = promotion_adapter or DryRunPromotionAdapter()
        self.clock = clock or UtcClock()

    def _now(self) -> datetime:
        value = self.clock.now()
        if value.tzinfo is None:
            raise ValueError("coordinator clock must return a timezone-aware instant")
        return value.astimezone(timezone.utc)

    def _assert_live(self, plan: CompiledPlan, *, now: datetime) -> None:
        if now >= plan.execution_deadline.astimezone(timezone.utc):
            raise PermissionError("compiled recovery authority expired before promotion")

    def _load_receipt(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        update_id: str,
        *,
        now: datetime,
    ) -> VerificationReceipt | None:
        if not store.has_stage(plan.action_id, self.VERIFICATION_STAGE):
            return None
        receipt = VerificationReceipt.model_validate(
            store.get_stage(plan.action_id, self.VERIFICATION_STAGE)
        )
        receipt.validate_for(plan, update_id, now=now)
        return receipt

    def _load_promotion(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        receipt: VerificationReceipt,
    ) -> PromotionReceipt | None:
        if not store.has_stage(plan.action_id, self.PROMOTION_STAGE):
            return None
        promotion = PromotionReceipt.model_validate(
            store.get_stage(plan.action_id, self.PROMOTION_STAGE)
        )
        if (
            promotion.action_id != plan.action_id
            or promotion.update_id != receipt.update_id
            or promotion.verification_digest != receipt.proof_digest
        ):
            raise ValueError("persisted promotion is bound to different proof")
        return promotion

    def run(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        *,
        approval: ExecutionApproval | None = None,
        maximum_polls: int = 720,
    ) -> RecoveryResult:
        """Complete one idempotent recovery without a fail-open promotion path."""

        plan = CompiledPlan.model_validate(plan.model_dump(mode="json"))
        receipt: VerificationReceipt | None = None
        if store.has_stage(plan.action_id, self.VERIFICATION_STAGE):
            receipt = VerificationReceipt.model_validate(
                store.get_stage(plan.action_id, self.VERIFICATION_STAGE)
            )
            update_id = receipt.update_id
            stored_update_id = store.get_external_id(plan.action_id, self.worker.STEP)
            if stored_update_id is not None and stored_update_id != update_id:
                raise ValueError("verification receipt conflicts with the incident journal")
            receipt.validate_for(plan, update_id, now=self._now())
        else:
            update_id = self.worker.run(
                plan,
                store,
                approval=approval,
                maximum_polls=maximum_polls,
            )
        if receipt is None:
            evidence = self.evidence_provider.load(plan=plan, update_id=update_id)
            metrics = verify_candidate(
                expected_keys=set(evidence.expected_keys),
                expected_v3_keys=set(evidence.expected_v3_keys),
                candidates=evidence.candidates,
                settlements=evidence.settlements,
                required_profile_digest=plan.profile_digest,
                maximum_rows=plan.maximum_rows,
                settlement_tolerance_minor=plan.settlement_tolerance_minor,
            )
            verified_at = self._now()
            self._assert_live(plan, now=verified_at)
            receipt = build_verification_receipt(
                plan=plan,
                update_id=update_id,
                verified_at=verified_at,
                metrics=metrics,
            )
            store.put_stage(
                plan.action_id,
                self.VERIFICATION_STAGE,
                receipt.model_dump(mode="json"),
            )
            # Promotion consumes the immutable journal representation, never
            # the in-memory object that happened to produce it.
            receipt = self._load_receipt(
                plan,
                store,
                update_id,
                now=self._now(),
            )
            if receipt is None:  # pragma: no cover - defensive store contract check
                raise RuntimeError("verification receipt disappeared after persistence")

        existing_promotion = self._load_promotion(plan, store, receipt)
        if existing_promotion is not None:
            return RecoveryResult(update_id, receipt, existing_promotion)

        # This is a separate side effect from candidate execution, so authority
        # is checked again immediately before the promotion adapter is called.
        self._assert_live(plan, now=self._now())
        promotion_id = self.promotion_adapter.promote(plan=plan, receipt=receipt)
        promotion = PromotionReceipt(
            action_id=plan.action_id,
            update_id=update_id,
            verification_digest=receipt.proof_digest,
            promotion_id=promotion_id,
            promoted_at=self._now(),
        )
        store.put_stage(
            plan.action_id,
            self.PROMOTION_STAGE,
            promotion.model_dump(mode="json"),
        )
        return RecoveryResult(update_id, receipt, promotion)
