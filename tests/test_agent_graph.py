import pytest

from recovery_control_plane.agent_graph import build_langgraph
from recovery_control_plane.contracts import IntentReview, RepairAction


class FixedProposer:
    def __init__(self, intent) -> None:
        self.intent = intent
        self.calls = 0

    def propose(self, evidence, previous, review):
        del evidence, previous, review
        self.calls += 1
        return self.intent


class AcceptingChallenger:
    def challenge(self, evidence, intent):
        del evidence, intent
        return IntentReview(verdict="ACCEPT")


def _runtime():
    pytest.importorskip("langgraph")
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import Command

    return InMemorySaver, Command


def _initial_state(evidence):
    return {
        "evidence": evidence,
        "candidate": None,
        "review": None,
        "proposal_count": 0,
        "human_review": None,
    }


def test_human_interrupt_persists_and_resumes_on_the_same_thread(
    evidence,
    repair_intent,
) -> None:
    InMemorySaver, Command = _runtime()
    proposer = FixedProposer(repair_intent)
    graph = build_langgraph(
        proposer,
        AcceptingChallenger(),
        checkpointer=InMemorySaver(),
        require_human_review=True,
    )
    config = {"configurable": {"thread_id": "incident-review-1"}}

    paused = graph.invoke(_initial_state(evidence), config=config)

    assert proposer.calls == 1
    assert paused["__interrupt__"][0].value["kind"] == "semantic_repair_review"
    assert "does not authorize" in paused["__interrupt__"][0].value["notice"]

    resumed = graph.invoke(
        Command(
            resume={
                "approved": True,
                "reviewer": "data-owner-ui-label",
                "rationale": "The interpretation matches the reviewed profile.",
            }
        ),
        config=config,
    )

    # Resuming the interrupted node must not call either model again.
    assert proposer.calls == 1
    assert resumed["candidate"] == repair_intent
    assert resumed["human_review"].approved is True


def test_human_rejection_converts_the_candidate_to_escalation(
    evidence,
    repair_intent,
) -> None:
    InMemorySaver, Command = _runtime()
    graph = build_langgraph(
        FixedProposer(repair_intent),
        AcceptingChallenger(),
        checkpointer=InMemorySaver(),
        require_human_review=True,
    )
    config = {"configurable": {"thread_id": "incident-review-rejected"}}
    graph.invoke(_initial_state(evidence), config=config)

    resumed = graph.invoke(
        Command(
            resume={
                "approved": False,
                "reviewer": "data-owner-ui-label",
                "rationale": "Settlement evidence is still incomplete.",
            }
        ),
        config=config,
    )

    assert resumed["candidate"].action is RepairAction.ESCALATE
    assert resumed["review"].verdict == "ESCALATE"
    assert resumed["human_review"].approved is False


def test_human_review_requires_a_checkpointer(repair_intent) -> None:
    with pytest.raises(ValueError, match="requires a persistent"):
        build_langgraph(
            FixedProposer(repair_intent),
            AcceptingChallenger(),
            require_human_review=True,
        )


def test_langgraph_rejects_an_unbounded_zero_proposal_loop(repair_intent) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        build_langgraph(
            FixedProposer(repair_intent),
            AcceptingChallenger(),
            maximum_proposals=0,
        )
