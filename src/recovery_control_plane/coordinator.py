"""Execute, independently verify, and only then promote a repair candidate."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .authority import (
    AuthoritySeal,
    VerificationReceiptAuthoritySigner,
    VerificationReceiptAuthorityVerifier,
)
from .incident_store import IncidentStore
from .normalization import BronzeOrder, CanonicalOrder
from .policy import CompiledPlan
from .verifier import (
    BusinessKey,
    SettlementRecord,
    VerificationMetrics,
    assert_verified,
    verify_candidate,
)
from .worker import Clock, ExecutionApproval, RecoveryWorker, UtcClock


VERIFIER_VERSION = "independent-verifier/1"


class EvidenceArtifact(BaseModel):
    """One immutable query result represented in a verification manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_id: str = Field(min_length=1, max_length=200)
    kind: Literal["SOURCE_SNAPSHOT", "CANDIDATE_QUERY", "SETTLEMENT_QUERY"]
    relation: str = Field(min_length=1, max_length=300)
    snapshot_reference: str = Field(min_length=1, max_length=300)
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(ge=0, strict=True)


def build_manifest_digest(manifest: tuple[EvidenceArtifact, ...]) -> str:
    """Content-address one ordered-independent evidence manifest."""

    payload = [
        item.model_dump(mode="json")
        for item in sorted(manifest, key=lambda item: item.artifact_id)
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _content_digest(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_evidence_manifest(
    *,
    source_table: str,
    source_delta_version: int,
    candidate_table: str,
    candidate_delta_version: int,
    update_id: str,
    settlement_relation: str,
    settlement_snapshot_reference: str,
    source_rows: tuple[BronzeOrder, ...],
    candidates: tuple[CanonicalOrder, ...],
    settlements: tuple[SettlementRecord, ...],
) -> tuple[tuple[EvidenceArtifact, ...], str]:
    """Build artifact digests directly from observed query results."""

    source_payload = sorted(
        (row.model_dump(mode="json") for row in source_rows),
        key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
    )
    candidate_payload = sorted(
        (row.model_dump(mode="json") for row in candidates),
        key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
    )
    settlement_payload = sorted(
        (row.model_dump(mode="json") for row in settlements),
        key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
    )
    manifest = (
        EvidenceArtifact(
            artifact_id="source-snapshot",
            kind="SOURCE_SNAPSHOT",
            relation=source_table,
            snapshot_reference=f"delta-version:{source_delta_version}",
            content_digest=_content_digest(source_payload),
            row_count=len(source_rows),
        ),
        EvidenceArtifact(
            artifact_id="candidate-query",
            kind="CANDIDATE_QUERY",
            relation=candidate_table,
            snapshot_reference=(
                f"update:{update_id}:delta-version:{candidate_delta_version}"
            ),
            content_digest=_content_digest(candidate_payload),
            row_count=len(candidates),
        ),
        EvidenceArtifact(
            artifact_id="settlement-query",
            kind="SETTLEMENT_QUERY",
            relation=settlement_relation,
            snapshot_reference=settlement_snapshot_reference,
            content_digest=_content_digest(settlement_payload),
            row_count=len(settlements),
        ),
    )
    return manifest, build_manifest_digest(manifest)


class VerificationEvidence(BaseModel):
    """Observed post-update evidence loaded independently from the actuator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["verification-evidence/v1"] = "verification-evidence/v1"
    action_id: str = Field(pattern=r"^rpr-[0-9a-f]{60}$")
    update_id: str = Field(min_length=1, max_length=200)
    pipeline_id: str = Field(min_length=1, max_length=200)
    pipeline_deployment: str = Field(min_length=1, max_length=300)
    profile_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_table: str = Field(min_length=1, max_length=300)
    source_delta_version: int = Field(ge=0, strict=True)
    source_fingerprint: str = Field(min_length=1, max_length=200)
    candidate_table: str = Field(min_length=1, max_length=300)
    candidate_delta_version: int = Field(ge=0, strict=True)
    observed_start: datetime
    observed_end: datetime
    manifest: tuple[EvidenceArtifact, ...] = Field(min_length=3)
    manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_rows: tuple[BronzeOrder, ...]
    expected_keys: frozenset[BusinessKey]
    expected_v3_keys: frozenset[BusinessKey]
    candidates: tuple[CanonicalOrder, ...]
    settlements: tuple[SettlementRecord, ...]

    @model_validator(mode="after")
    def validate_observed_evidence(self) -> "VerificationEvidence":
        if self.observed_start.tzinfo is None or self.observed_end.tzinfo is None:
            raise ValueError("observed replay timestamps must include a timezone")
        if self.observed_end <= self.observed_start:
            raise ValueError("observed replay window is invalid")
        if any(candidate.event_time is None for candidate in self.candidates):
            raise ValueError("candidate event_time cannot be null")
        if any(
            candidate.event_time is not None
            and (
                candidate.event_time.tzinfo is None
                or candidate.event_time.utcoffset() is None
            )
            for candidate in self.candidates
        ):
            raise ValueError("candidate event_time must include a timezone")
        if len({item.artifact_id for item in self.manifest}) != len(self.manifest):
            raise ValueError("evidence manifest artifact IDs must be unique")
        required_kinds = {"SOURCE_SNAPSHOT", "CANDIDATE_QUERY", "SETTLEMENT_QUERY"}
        kinds = [item.kind for item in self.manifest]
        if len(self.manifest) != 3 or set(kinds) != required_kinds:
            raise ValueError("evidence manifest requires exactly three query artifacts")
        if self.manifest_digest != build_manifest_digest(self.manifest):
            raise ValueError("evidence manifest digest does not match its artifacts")
        source_artifacts = [item for item in self.manifest if item.kind == "SOURCE_SNAPSHOT"]
        candidate_artifacts = [item for item in self.manifest if item.kind == "CANDIDATE_QUERY"]
        settlement_artifacts = [
            item for item in self.manifest if item.kind == "SETTLEMENT_QUERY"
        ]
        if (
            len(source_artifacts) != 1
            or len(candidate_artifacts) != 1
            or len(settlement_artifacts) != 1
        ):
            raise ValueError(
                "evidence manifest requires one source, candidate, and settlement artifact"
            )
        source = source_artifacts[0]
        if (
            source.relation != self.source_table
            or source.snapshot_reference != f"delta-version:{self.source_delta_version}"
        ):
            raise ValueError("source manifest artifact does not match observed snapshot")
        candidate = candidate_artifacts[0]
        expected_candidate_snapshot = (
            f"update:{self.update_id}:delta-version:{self.candidate_delta_version}"
        )
        if (
            candidate.relation != self.candidate_table
            or candidate.snapshot_reference != expected_candidate_snapshot
        ):
            raise ValueError("candidate manifest artifact does not match observed update")
        observed_content = {
            "SOURCE_SNAPSHOT": sorted(
                (row.model_dump(mode="json") for row in self.source_rows),
                key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
            ),
            "CANDIDATE_QUERY": sorted(
                (row.model_dump(mode="json") for row in self.candidates),
                key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
            ),
            "SETTLEMENT_QUERY": sorted(
                (row.model_dump(mode="json") for row in self.settlements),
                key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
            ),
        }
        observed_counts = {
            "SOURCE_SNAPSHOT": len(self.source_rows),
            "CANDIDATE_QUERY": len(self.candidates),
            "SETTLEMENT_QUERY": len(self.settlements),
        }
        for artifact in self.manifest:
            if artifact.row_count != observed_counts[artifact.kind]:
                raise ValueError(
                    f"{artifact.kind.lower()} row count does not match observed rows"
                )
            if artifact.content_digest != _content_digest(observed_content[artifact.kind]):
                raise ValueError(
                    f"{artifact.kind.lower()} digest does not match observed rows"
                )
        return self

    def validate_for(self, plan: CompiledPlan, update_id: str) -> None:
        """Bind observed facts to the exact sealed action and completed update."""

        comparisons = (
            (self.action_id, plan.action_id, "action"),
            (self.update_id, update_id, "update"),
            (self.pipeline_id, plan.pipeline_id, "pipeline"),
            (self.pipeline_deployment, plan.pipeline_deployment, "deployment"),
            (self.profile_digest, plan.profile_digest, "profile"),
            (self.source_table, plan.source_table, "source table"),
            (self.source_delta_version, plan.source_delta_version, "Delta version"),
            (self.source_fingerprint, plan.source_fingerprint, "source fingerprint"),
            (self.candidate_table, plan.candidate_dataset, "candidate table"),
        )
        for observed, expected, label in comparisons:
            if observed != expected:
                raise ValueError(f"verification evidence proves a different {label}")
        settlement_artifact = next(
            item for item in self.manifest if item.kind == "SETTLEMENT_QUERY"
        )
        if settlement_artifact.relation != plan.settlement_relation:
            raise ValueError("verification evidence proves a different settlement relation")
        start = self.observed_start.astimezone(timezone.utc)
        end = self.observed_end.astimezone(timezone.utc)
        if start != plan.backfill_start.astimezone(timezone.utc) or end != plan.backfill_end.astimezone(
            timezone.utc
        ):
            raise ValueError("verification evidence proves a different replay window")
        authorized_versions = {
            plan.payload_version,
            *plan.authorized_legacy_versions,
        }
        derived_expected_keys: set[BusinessKey] = set()
        derived_profile_keys: set[BusinessKey] = set()
        for row in self.source_rows:
            if row.partner != plan.partner:
                raise ValueError("retained source rows include a different partner")
            order_id = row.order_id
            if type(order_id) is not str or not order_id.strip():
                raise ValueError("retained source row has a missing business key")
            source_sequence = row.source_sequence
            if type(source_sequence) is not int:
                raise ValueError("retained source row has a non-strict business key")
            event_time = row.event_time
            if event_time is None:
                raise ValueError("retained source event_time cannot be null")
            if event_time.tzinfo is None or event_time.utcoffset() is None:
                raise ValueError("retained source event_time must include a timezone")
            event_instant = event_time.astimezone(timezone.utc)
            if event_instant < start or event_instant >= end:
                raise ValueError("retained source row falls outside the replay window")
            if row.payload_version not in authorized_versions:
                raise ValueError("retained source row has an unauthorized payload version")
            key = (row.partner, order_id, source_sequence)
            derived_expected_keys.add(key)
            if row.payload_version == plan.payload_version:
                derived_profile_keys.add(key)
        if frozenset(derived_expected_keys) != self.expected_keys:
            raise ValueError("expected keys do not match the retained source rows")
        if frozenset(derived_profile_keys) != self.expected_v3_keys:
            raise ValueError("profile keys do not match the retained source rows")
        for candidate in self.candidates:
            candidate_event_time = candidate.event_time
            if candidate_event_time is None:  # Defensive after envelope validation.
                raise ValueError("candidate event_time cannot be null")
            event_time = candidate_event_time.astimezone(timezone.utc)
            if event_time < start or event_time >= end:
                raise ValueError("candidate row falls outside the observed replay window")


class VerificationEvidenceProvider(Protocol):
    """Load post-update candidate and independent business evidence."""

    def load(self, *, plan: CompiledPlan, update_id: str) -> VerificationEvidence: ...


def _proof_payload(
    *,
    action_id: str,
    update_id: str,
    pipeline_id: str,
    pipeline_deployment: str,
    profile_digest: str,
    source_table: str,
    source_delta_version: int,
    source_fingerprint: str,
    candidate_table: str,
    candidate_delta_version: int,
    observed_start: datetime,
    observed_end: datetime,
    evidence_manifest: tuple[EvidenceArtifact, ...],
    evidence_manifest_digest: str,
    execution_deadline: datetime,
    verified_at: datetime,
    metrics: VerificationMetrics,
) -> dict[str, object]:
    timestamps = (observed_start, observed_end, execution_deadline, verified_at)
    if any(value.tzinfo is None for value in timestamps):
        raise ValueError("verification proof timestamps must include a timezone")
    return {
        "action_id": action_id,
        "update_id": update_id,
        "pipeline_id": pipeline_id,
        "pipeline_deployment": pipeline_deployment,
        "profile_digest": profile_digest,
        "source_table": source_table,
        "source_delta_version": source_delta_version,
        "source_fingerprint": source_fingerprint,
        "candidate_table": candidate_table,
        "candidate_delta_version": candidate_delta_version,
        "observed_start": observed_start.astimezone(timezone.utc).isoformat(),
        "observed_end": observed_end.astimezone(timezone.utc).isoformat(),
        "evidence_manifest": [
            item.model_dump(mode="json") for item in evidence_manifest
        ],
        "evidence_manifest_digest": evidence_manifest_digest,
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
    pipeline_id: str = Field(min_length=1, max_length=200)
    pipeline_deployment: str = Field(min_length=1, max_length=300)
    profile_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_table: str = Field(min_length=1, max_length=300)
    source_delta_version: int = Field(ge=0, strict=True)
    source_fingerprint: str = Field(min_length=1, max_length=200)
    candidate_table: str = Field(min_length=1, max_length=300)
    candidate_delta_version: int = Field(ge=0, strict=True)
    observed_start: datetime
    observed_end: datetime
    evidence_manifest: tuple[EvidenceArtifact, ...] = Field(min_length=3)
    evidence_manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_deadline: datetime
    verified_at: datetime
    verifier_version: Literal["independent-verifier/1"] = VERIFIER_VERSION
    metrics: VerificationMetrics
    proof_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_seal: AuthoritySeal

    def proof_payload(self) -> dict[str, object]:
        """Return every proof field covered by the receipt authority signature."""

        return _proof_payload(
            action_id=self.action_id,
            update_id=self.update_id,
            pipeline_id=self.pipeline_id,
            pipeline_deployment=self.pipeline_deployment,
            profile_digest=self.profile_digest,
            source_table=self.source_table,
            source_delta_version=self.source_delta_version,
            source_fingerprint=self.source_fingerprint,
            candidate_table=self.candidate_table,
            candidate_delta_version=self.candidate_delta_version,
            observed_start=self.observed_start,
            observed_end=self.observed_end,
            evidence_manifest=self.evidence_manifest,
            evidence_manifest_digest=self.evidence_manifest_digest,
            execution_deadline=self.execution_deadline,
            verified_at=self.verified_at,
            metrics=self.metrics,
        )

    @model_validator(mode="after")
    def validate_proof(self) -> "VerificationReceipt":
        if self.verified_at.tzinfo is None:
            raise ValueError("verification receipt timestamp must include a timezone")
        required_kinds = {"SOURCE_SNAPSHOT", "CANDIDATE_QUERY", "SETTLEMENT_QUERY"}
        if (
            len(self.evidence_manifest) != 3
            or {item.kind for item in self.evidence_manifest} != required_kinds
            or len({item.artifact_id for item in self.evidence_manifest}) != 3
        ):
            raise ValueError("verification receipt requires three unique evidence artifacts")
        if build_manifest_digest(self.evidence_manifest) != self.evidence_manifest_digest:
            raise ValueError("verification receipt manifest digest does not match metadata")
        expected = _proof_digest(**self.proof_payload())
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
        if self.pipeline_id != plan.pipeline_id:
            raise ValueError("verification receipt proves a different pipeline")
        if self.pipeline_deployment != plan.pipeline_deployment:
            raise ValueError("verification receipt proves a different deployment")
        if self.profile_digest != plan.profile_digest:
            raise ValueError("verification receipt proves a different profile")
        if self.source_table != plan.source_table:
            raise ValueError("verification receipt proves a different source table")
        if self.source_delta_version != plan.source_delta_version:
            raise ValueError("verification receipt proves a different Delta version")
        if self.source_fingerprint != plan.source_fingerprint:
            raise ValueError("verification receipt proves a different source")
        if self.candidate_table != plan.candidate_dataset:
            raise ValueError("verification receipt proves a different candidate table")
        if self.observed_start.astimezone(timezone.utc) != plan.backfill_start.astimezone(
            timezone.utc
        ) or self.observed_end.astimezone(timezone.utc) != plan.backfill_end.astimezone(
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
        # A valid digest proves immutability, not correctness. Re-run the
        # deterministic promotion invariants whenever a receipt is consumed.
        assert_verified(
            self.metrics,
            maximum_rows=plan.maximum_rows,
            settlement_tolerance_minor=plan.settlement_tolerance_minor,
        )


def build_verification_receipt(
    *,
    plan: CompiledPlan,
    evidence: VerificationEvidence,
    verified_at: datetime,
    metrics: VerificationMetrics,
    receipt_authority_signer: VerificationReceiptAuthoritySigner,
) -> VerificationReceipt:
    evidence.validate_for(plan, evidence.update_id)
    payload = _proof_payload(
        action_id=plan.action_id,
        update_id=evidence.update_id,
        pipeline_id=evidence.pipeline_id,
        pipeline_deployment=evidence.pipeline_deployment,
        profile_digest=evidence.profile_digest,
        source_table=evidence.source_table,
        source_delta_version=evidence.source_delta_version,
        source_fingerprint=evidence.source_fingerprint,
        candidate_table=evidence.candidate_table,
        candidate_delta_version=evidence.candidate_delta_version,
        observed_start=evidence.observed_start,
        observed_end=evidence.observed_end,
        evidence_manifest=evidence.manifest,
        evidence_manifest_digest=evidence.manifest_digest,
        execution_deadline=plan.execution_deadline,
        verified_at=verified_at,
        metrics=metrics,
    )
    proof_digest = _proof_digest(**payload)
    authority_seal = receipt_authority_signer.sign_verification_receipt(
        proof_payload=payload,
        proof_digest=proof_digest,
    )
    return VerificationReceipt(
        **payload,
        proof_digest=proof_digest,
        authority_seal=authority_seal,
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
        receipt_authority_signer: VerificationReceiptAuthoritySigner | None = None,
        receipt_authority_verifier: VerificationReceiptAuthorityVerifier | None = None,
        promotion_adapter: PromotionAdapter | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.worker = worker
        self.evidence_provider = evidence_provider
        self.receipt_authority_signer = receipt_authority_signer
        self.receipt_authority_verifier = receipt_authority_verifier
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

    def _assert_receipt_authority_available(self) -> None:
        if self.receipt_authority_signer is None:
            raise PermissionError("verification receipt signer is unavailable")
        if self.receipt_authority_verifier is None:
            raise PermissionError("verification receipt verifier is unavailable")

    def _authenticate_receipt(self, receipt: VerificationReceipt) -> None:
        if self.receipt_authority_verifier is None:
            raise PermissionError("verification receipt verifier is unavailable")
        self.receipt_authority_verifier.verify_verification_receipt(receipt)

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
        self._authenticate_receipt(receipt)
        receipt.validate_for(plan, update_id, now=now)
        self._assert_completed_update(plan, store, update_id)
        return receipt

    def _assert_completed_update(
        self,
        plan: CompiledPlan,
        store: IncidentStore,
        update_id: str,
    ) -> None:
        stored_update_id = store.get_external_id(plan.action_id, self.worker.STEP)
        if stored_update_id != update_id:
            raise ValueError("verification receipt lacks its exact update journal")
        if store.get_state(plan.action_id, self.worker.STEP) != self.worker.COMPLETED:
            raise ValueError("verification receipt update is not journaled as COMPLETED")

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
        self._assert_receipt_authority_available()
        # A persisted verification receipt must never bypass plan
        # authentication merely because execution completed in an earlier run.
        self.worker.assert_plan_authorized(plan)
        receipt: VerificationReceipt | None = None
        if store.has_stage(plan.action_id, self.VERIFICATION_STAGE):
            persisted = VerificationReceipt.model_validate(
                store.get_stage(plan.action_id, self.VERIFICATION_STAGE)
            )
            update_id = persisted.update_id
            self._authenticate_receipt(persisted)
            persisted.validate_for(plan, update_id, now=self._now())
            self._assert_completed_update(plan, store, update_id)
            receipt = persisted
        else:
            update_id = self.worker.run(
                plan,
                store,
                approval=approval,
                maximum_polls=maximum_polls,
            )
        if receipt is None:
            loaded = self.evidence_provider.load(plan=plan, update_id=update_id)
            evidence = VerificationEvidence.model_validate(
                loaded.model_dump(mode="json")
            )
            evidence.validate_for(plan, update_id)
            metrics = verify_candidate(
                expected_keys=set(evidence.expected_keys),
                expected_v3_keys=set(evidence.expected_v3_keys),
                source_rows=evidence.source_rows,
                candidates=evidence.candidates,
                settlements=evidence.settlements,
                required_profile_digest=plan.profile_digest,
                required_currency_code=plan.currency_code,
                backfill_start=plan.backfill_start,
                backfill_end=plan.backfill_end,
                required_profile_version=plan.payload_version,
                required_profile_id=plan.profile_id,
                authorized_legacy_versions=plan.authorized_legacy_versions,
                maximum_rows=plan.maximum_rows,
                settlement_tolerance_minor=plan.settlement_tolerance_minor,
            )
            verified_at = self._now()
            self._assert_live(plan, now=verified_at)
            receipt = build_verification_receipt(
                plan=plan,
                evidence=evidence,
                verified_at=verified_at,
                metrics=metrics,
                receipt_authority_signer=self.receipt_authority_signer,
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
        promotion_now = self._now()
        self.worker.assert_mutation_authorized(
            plan,
            store,
            approval,
            now=promotion_now,
            require_current_approval=True,
        )
        # Re-authenticate the exact reloaded envelope at the final promotion
        # boundary. An unkeyed content digest alone is not execution authority.
        self._authenticate_receipt(receipt)
        promotion_id = self.promotion_adapter.promote(plan=plan, receipt=receipt)
        promotion = PromotionReceipt(
            action_id=plan.action_id,
            update_id=update_id,
            verification_digest=receipt.proof_digest,
            promotion_id=promotion_id,
            promoted_at=promotion_now,
        )
        store.put_stage(
            plan.action_id,
            self.PROMOTION_STAGE,
            promotion.model_dump(mode="json"),
        )
        return RecoveryResult(update_id, receipt, promotion)
