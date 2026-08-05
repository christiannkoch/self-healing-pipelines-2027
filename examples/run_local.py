"""Run the Black Friday recovery story without a model or cloud account."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from recovery_control_plane.agent_graph import Challenger, Proposer, run_review_loop
from recovery_control_plane.contracts import (
    BackfillWindow,
    IncidentEvidence,
    IntentReview,
    RepairAction,
    RepairIntent,
)
from recovery_control_plane.coordinator import (
    RecoveryCoordinator,
    VerificationEvidence,
)
from recovery_control_plane.evidence import (
    EvidenceSnapshot,
    StaticEvidenceRepository,
    build_failure_passport,
)
from recovery_control_plane.incident_store import InMemoryIncidentStore
from recovery_control_plane.normalization import BronzeOrder, normalize_and_dedupe
from recovery_control_plane.policy import ApprovedProfile, RecoveryPolicy, compile_intent
from recovery_control_plane.profiles import MappingProfile, build_profile
from recovery_control_plane.verifier import SettlementRecord
from recovery_control_plane.worker import ExecutionApproval, RecoveryWorker


UTC = timezone.utc
INCIDENT_TIME = datetime(2027, 11, 26, 8, 7, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[1]


class FixedClock:
    """Keep this example deterministic so its evidence TTL never drifts."""

    def __init__(self, instant: datetime) -> None:
        self.instant = instant

    def now(self) -> datetime:
        return self.instant


class RuleProposer(Proposer):
    """Offline stand-in implementing exactly the same proposer protocol."""

    def __init__(self, profile: MappingProfile) -> None:
        self.profile = profile

    def propose(
        self,
        evidence: IncidentEvidence,
        previous: RepairIntent | None,
        review: IntentReview | None,
    ) -> RepairIntent:
        # A real model receives previous/review on its second bounded attempt.
        # The deterministic fixture is already sufficient, so neither is used.
        del previous, review
        return RepairIntent(
            incident_id=evidence.incident_id,
            action=RepairAction.ACTIVATE_MAPPING_PROFILE,
            target_dataset=evidence.dataset,
            source_fingerprint=evidence.source_fingerprint,
            mapping_profile_id=self.profile.profile_id,
            backfill_window=BackfillWindow(
                start=evidence.affected_start,
                end=evidence.affected_end,
                maximum_rows=10_000,
            ),
            confidence=0.88,
            evidence_event_ids=evidence.evidence_event_ids,
            rationale=(
                "The certified v3 EUR profile matches the observed source fingerprint "
                "and preserves the established business-event dedupe key."
            ),
            disconfirming_evidence=[
                "A v2/v3 overlap means winner selection must be proven after replay."
            ],
        )


class RuleChallenger(Challenger):
    """Offline challenger checks the semantic hazards from the narrative."""

    def challenge(self, evidence: IncidentEvidence, intent: RepairIntent) -> IntentReview:
        blockers: list[str] = []
        if evidence.business_checks.get("expected_currency") != "EUR":
            blockers.append("the expected settlement currency is not proven")
        if not evidence.business_checks.get("settlement_feed_available"):
            blockers.append("no independent settlement feed is available")
        if intent.mapping_profile_id not in evidence.candidate_profile_ids:
            blockers.append("the proposal names a profile outside the passport")
        if blockers:
            return IntentReview(verdict="ESCALATE", blocking_findings=blockers)
        return IntentReview(verdict="ACCEPT")


def load_json(name: str) -> object:
    return json.loads((ROOT / "fixtures" / name).read_text(encoding="utf-8"))


class FixtureVerificationProvider:
    """Load candidate and settlement proof only after the update completes."""

    def __init__(self, profile: MappingProfile) -> None:
        self.profile = profile
        self.candidates = ()

    def load(self, *, plan, update_id) -> VerificationEvidence:
        # ``plan`` and ``update_id`` identify the completed candidate snapshot
        # in production. The offline fixture has one deterministic snapshot.
        if not plan.action_id or not update_id:
            raise ValueError("verification requires a completed recovery update")
        source_rows = tuple(
            BronzeOrder.model_validate(item) for item in load_json("orders_replay.json")
        )
        expected_keys = frozenset(
            (row.partner, row.order_id, row.source_sequence)
            for row in source_rows
            if row.order_id is not None and row.source_sequence is not None
        )
        expected_v3_keys = frozenset(
            (row.partner, row.order_id, row.source_sequence)
            for row in source_rows
            if row.payload_version == "v3"
            and row.order_id is not None
            and row.source_sequence is not None
        )
        self.candidates = tuple(normalize_and_dedupe(source_rows, self.profile))
        settlements = tuple(
            SettlementRecord.model_validate(item) for item in load_json("settlements.json")
        )
        return VerificationEvidence(
            expected_keys=expected_keys,
            expected_v3_keys=expected_v3_keys,
            candidates=self.candidates,
            settlements=settlements,
        )


def main() -> None:
    # The content-addressed registry profile points to reviewed normalization
    # code. The agent can select this ID, but cannot author or alter the code.
    profile = build_profile(
        profile_id="marketplace-v3-gross-eur-v2",
        partner="marketplace_it",
        payload_version="v3",
        source_fingerprint="sha256:marketplace-it-v3-2027-11-26",
        normalizer_key="MARKETPLACE_V3_GROSS",
        normalizer_version="1.0.1",
        normalizer_source_digest="sha256:reviewed-normalizer-2027-11-25",
        currency_code="EUR",
        currency_exponent=2,
        dedupe_key=("partner", "order_id", "source_sequence"),
    )

    # Evidence is assembled by deterministic, read-only code before reasoning.
    snapshot = EvidenceSnapshot.model_validate(load_json("evidence_snapshot.json"))
    passport = build_failure_passport(
        StaticEvidenceRepository(snapshot),
        incident_id="inc-black-friday-2027",
        generation=2,
        pipeline_id="pipeline-orders",
        dataset="commerce.orders_normalized",
        partner="marketplace_it",
        payload_version="v3",
        clock=FixedClock(INCIDENT_TIME),
        ttl=timedelta(minutes=30),
    )

    # Proposer and challenger can be replaced by the LangChain adapters without
    # changing the policy, execution, or verification layers.
    outcome = run_review_loop(
        passport,
        RuleProposer(profile),
        RuleChallenger(),
        maximum_proposals=2,
    )

    approved = ApprovedProfile(
        profile_id=profile.profile_id,
        profile_digest=profile.profile_digest,
        pipeline_id="pipeline-orders",
        pipeline_deployment="bundle:orders@abc123",
        dataset="commerce.orders_normalized",
        partner=profile.partner,
        payload_version=profile.payload_version,
        source_fingerprint=profile.source_fingerprint,
        recovery_job_id=42,
        refresh_selection=("orders_repaired",),
        target_predicate_dimensions=("partner", "event_time"),
        maximum_auto_rows=50_000,
        approved=True,
        auto_eligible=False,
        semantic_attestation_required=True,
    )
    policy = RecoveryPolicy(
        version="recovery-policy/7",
        valid_until=INCIDENT_TIME + timedelta(days=1),
        maximum_auto_quarantine_ratio=0.10,
        maximum_auto_rows=100_000,
        maximum_auto_dbus=5,
        maximum_downstream_assets=5,
        profiles={approved.profile_id: approved},
    )
    plan = compile_intent(passport, outcome.intent, policy, now=INCIDENT_TIME)

    # The fixture deliberately uses the human lane. Approval is bound to the
    # compiled action ID and expires before the plan's own authority deadline.
    approval = ExecutionApproval(
        action_id=plan.action_id,
        approved_by="on-call-data-owner",
        approved_at=INCIDENT_TIME + timedelta(minutes=4),
        expires_at=INCIDENT_TIME + timedelta(minutes=20),
    )
    store = InMemoryIncidentStore()
    verification_provider = FixtureVerificationProvider(profile)
    coordinator = RecoveryCoordinator(
        worker=RecoveryWorker(
            clock=FixedClock(INCIDENT_TIME + timedelta(minutes=5))
        ),
        evidence_provider=verification_provider,
        clock=FixedClock(INCIDENT_TIME + timedelta(minutes=6)),
    )
    result = coordinator.run(
        plan,
        store,
        approval=approval,
        maximum_polls=1,
    )
    metrics = result.verification.metrics

    print(
        json.dumps(
            {
                "incident_id": passport.incident_id,
                "review_verdict": outcome.review.verdict,
                "action_id": plan.action_id,
                "winner_versions": [
                    row.payload_version for row in verification_provider.candidates
                ],
                "winner_amounts_minor": [
                    row.order_total_minor for row in verification_provider.candidates
                ],
                "verified_rows": metrics.candidate_rows,
                "settlement_difference_minor": metrics.settlement_difference_minor,
                "update_id": result.update_id,
                "verification_digest": result.verification.proof_digest,
                "promotion_id": result.promotion.promotion_id,
                "operation_state": store.get_state(plan.action_id, RecoveryWorker.STEP),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
