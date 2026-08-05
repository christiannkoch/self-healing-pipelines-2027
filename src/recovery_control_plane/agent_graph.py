"""Bounded proposer/challenger wiring with an optional LangGraph runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, NotRequired, Protocol, TypedDict

from pydantic import BaseModel, ConfigDict, Field

from .contracts import IncidentEvidence, IntentReview, RepairAction, RepairIntent


class Proposer(Protocol):
    """Tool-less semantic proposer port."""

    def propose(
        self,
        evidence: IncidentEvidence,
        previous: RepairIntent | None,
        review: IntentReview | None,
    ) -> RepairIntent:
        """Return one schema-valid intent and perform no external mutation."""


class Challenger(Protocol):
    """Tool-less adversarial review port."""

    def challenge(self, evidence: IncidentEvidence, intent: RepairIntent) -> IntentReview:
        """Try to disprove the proposed interpretation."""


@dataclass(frozen=True)
class ReviewOutcome:
    intent: RepairIntent
    review: IntentReview
    proposal_count: int


class HumanReviewDecision(BaseModel):
    """A persisted semantic-review decision, never execution authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    approved: bool = Field(strict=True)
    reviewer: str = Field(min_length=1, max_length=120)
    rationale: str = Field(min_length=1, max_length=500)


def escalation_intent(evidence: IncidentEvidence, review: IntentReview) -> RepairIntent:
    """Create a safe terminal result when the bounded loop cannot agree."""

    detail = "; ".join(review.blocking_findings or ["insufficient evidence"])
    return RepairIntent(
        incident_id=evidence.incident_id,
        action=RepairAction.ESCALATE,
        target_dataset=evidence.dataset,
        source_fingerprint=evidence.source_fingerprint,
        confidence=0,
        evidence_event_ids=evidence.evidence_event_ids[:30],
        rationale=("The bounded review did not resolve: " + detail)[:1_200],
    )


def run_review_loop(
    evidence: IncidentEvidence,
    proposer: Proposer,
    challenger: Challenger,
    *,
    maximum_proposals: int = 2,
) -> ReviewOutcome:
    """Run a finite proposer/challenger loop; exhaustion always escalates."""

    if maximum_proposals < 1:
        raise ValueError("maximum_proposals must be positive")

    previous: RepairIntent | None = None
    review: IntentReview | None = None
    for proposal_count in range(1, maximum_proposals + 1):
        candidate = proposer.propose(evidence, previous, review)
        current_review = challenger.challenge(evidence, candidate)
        if current_review.verdict == "ACCEPT":
            return ReviewOutcome(candidate, current_review, proposal_count)
        if current_review.verdict != "REVISE" or proposal_count == maximum_proposals:
            return ReviewOutcome(
                escalation_intent(evidence, current_review),
                current_review,
                proposal_count,
            )
        previous, review = candidate, current_review

    raise AssertionError("the bounded review loop must return from inside the loop")


class GraphState(TypedDict):
    evidence: IncidentEvidence
    candidate: RepairIntent | None
    review: IntentReview | None
    proposal_count: int
    human_review: NotRequired[HumanReviewDecision | None]


def build_langgraph(
    proposer: Proposer,
    challenger: Challenger,
    *,
    maximum_proposals: int = 2,
    checkpointer: Any | None = None,
    require_human_review: bool = False,
) -> Any:
    """Compile the finite review machine with optional durable interruption.

    Importing the package never imports LangGraph. This function is the explicit
    opt-in boundary and raises a useful error if the ``agent`` extra is absent.
    A human interrupt is a semantic review only. It neither creates an
    ``ExecutionApproval`` nor bypasses deterministic policy compilation.
    """

    if maximum_proposals < 1:
        raise ValueError("maximum_proposals must be positive")
    if require_human_review and checkpointer is None:
        raise ValueError("human review requires a persistent LangGraph checkpointer")

    try:
        from langgraph.graph import END, START, StateGraph
        from langgraph.types import interrupt
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError(
            "LangGraph is optional; install self-healing-pipelines-2027[agent]"
        ) from exc

    def propose_node(state: GraphState) -> dict[str, Any]:
        candidate = proposer.propose(state["evidence"], state["candidate"], state["review"])
        return {"candidate": candidate, "proposal_count": state["proposal_count"] + 1}

    def challenge_node(state: GraphState) -> dict[str, Any]:
        candidate = state["candidate"]
        if candidate is None:
            raise RuntimeError("the challenger received no proposal")
        return {"review": challenger.challenge(state["evidence"], candidate)}

    def route(state: GraphState) -> str:
        review = state["review"]
        if review is None:
            return "escalate"
        if review.verdict == "ACCEPT":
            return "human" if require_human_review else "accept"
        if review.verdict == "REVISE" and state["proposal_count"] < maximum_proposals:
            return "revise"
        return "escalate"

    def escalate_node(state: GraphState) -> dict[str, Any]:
        review = state["review"] or IntentReview(
            verdict="ESCALATE",
            blocking_findings=["review state is missing"],
        )
        return {"candidate": escalation_intent(state["evidence"], review)}

    def human_review_node(state: GraphState) -> dict[str, Any]:
        """Pause on serializable state and validate the later resume payload."""

        candidate = state["candidate"]
        review = state["review"]
        if candidate is None or review is None or review.verdict != "ACCEPT":
            raise RuntimeError("human review requires a challenger-accepted candidate")

        # LangGraph re-enters this node from the beginning after resume. Keep all
        # side effects out of the pre-interrupt section.
        resumed = interrupt(
            {
                "kind": "semantic_repair_review",
                "incident_id": state["evidence"].incident_id,
                "candidate": candidate.model_dump(mode="json"),
                "challenger_review": review.model_dump(mode="json"),
                "notice": (
                    "This decision resumes semantic review only; it does not "
                    "authorize a Databricks update."
                ),
            }
        )
        decision = HumanReviewDecision.model_validate(resumed)
        if decision.approved:
            return {"human_review": decision}

        rejection = IntentReview(
            verdict="ESCALATE",
            blocking_findings=(
                (
                    f"Semantic reviewer {decision.reviewer} rejected the proposal: "
                    f"{decision.rationale}"
                )[:240],
            ),
        )
        return {
            "candidate": escalation_intent(state["evidence"], rejection),
            "review": rejection,
            "human_review": decision,
        }

    graph = StateGraph(GraphState)
    graph.add_node("propose", propose_node)
    graph.add_node("challenge", challenge_node)
    graph.add_node("escalate", escalate_node)
    if require_human_review:
        graph.add_node("human_review", human_review_node)
    graph.add_edge(START, "propose")
    graph.add_edge("propose", "challenge")
    route_destinations: dict[str, Any] = {
        "accept": END,
        "revise": "propose",
        "escalate": "escalate",
    }
    if require_human_review:
        route_destinations["human"] = "human_review"
    graph.add_conditional_edges(
        "challenge",
        route,
        route_destinations,
    )
    graph.add_edge("escalate", END)
    if require_human_review:
        graph.add_edge("human_review", END)
    return graph.compile(checkpointer=checkpointer)
