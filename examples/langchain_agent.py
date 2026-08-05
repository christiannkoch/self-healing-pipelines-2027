"""Paid, explicit LangChain review with durable LangGraph human resume."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from recovery_control_plane.agent_graph import build_langgraph
from recovery_control_plane.contracts import IncidentEvidence


ROOT = Path(__file__).resolve().parents[1]


class _ResumeOnlyProposer:
    """Guard against accidentally starting a fresh model run during resume."""

    def propose(self, evidence, previous, review):
        del evidence, previous, review
        raise RuntimeError(
            "No resumable human interrupt exists for this thread; model calls remain disabled"
        )


class _ResumeOnlyChallenger:
    """Guard against accidentally challenging again during resume."""

    def challenge(self, evidence, intent):
        del evidence, intent
        raise RuntimeError(
            "No resumable human interrupt exists for this thread; model calls remain disabled"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Start or resume persisted semantic review. A successful review "
            "still does not authorize a Databricks update."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--start-model-review",
        action="store_true",
        help=(
            "start a proposer/challenger run; an accepted first proposal costs "
            "two provider calls and one revision can raise the total to four"
        ),
    )
    mode.add_argument(
        "--resume-human-review",
        choices=("approve", "reject"),
        help="resume an interrupt without a model call; this is not execution approval",
    )
    parser.add_argument("--thread-id", help="stable ID shared by start and resume")
    parser.add_argument(
        "--checkpoint-db",
        type=Path,
        help="SQLite checkpoint file shared by start and resume",
    )
    parser.add_argument(
        "--reviewer",
        help="self-asserted reviewer label stored in the semantic checkpoint",
    )
    parser.add_argument("--rationale", help="reason for approving or rejecting")
    args = parser.parse_args()
    active = args.start_model_review or args.resume_human_review
    if active and (not args.thread_id or args.checkpoint_db is None):
        parser.error("start and resume require --thread-id and --checkpoint-db")
    if args.resume_human_review and (not args.reviewer or not args.rationale):
        parser.error("resume requires --reviewer and --rationale")
    return args


def _json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "value"):
        return value.value
    return value


def _print_result(result: dict[str, Any]) -> None:
    payload = {
        key: _json_value(result[key])
        for key in ("candidate", "review", "human_review")
        if result.get(key) is not None
    }
    interrupts = [
        {
            "id": getattr(item, "id", None),
            "value": _json_value(getattr(item, "value", item)),
        }
        for item in result.get("__interrupt__", ())
    ]
    if interrupts:
        payload["interrupts"] = interrupts
    payload["authority_notice"] = (
        "Semantic review cannot create ExecutionApproval. Compile policy and use "
        "the authenticated worker approval lane before any external update."
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


def _start_ports() -> tuple[Any, Any]:
    """Construct paid model adapters only for an explicit start command."""

    from langchain_openai import ChatOpenAI

    from recovery_control_plane.adapters.langchain import (
        LangChainChallenger,
        LangChainProposer,
    )

    try:
        proposer_name = os.environ["RECOVERY_PROPOSER_MODEL"]
        challenger_name = os.environ["RECOVERY_CHALLENGER_MODEL"]
    except KeyError as exc:
        raise SystemExit(f"Missing required environment variable: {exc.args[0]}") from exc

    return (
        LangChainProposer(ChatOpenAI(model=proposer_name, temperature=0)),
        LangChainChallenger(ChatOpenAI(model=challenger_name, temperature=0)),
    )


def main() -> None:
    args = parse_args()
    if not args.start_model_review and not args.resume_human_review:
        print(
            "No model call made. Use --start-model-review with explicit models, "
            "or --resume-human-review with an existing checkpoint."
        )
        return

    # SQLite is appropriate for this local example. A service needs a durable,
    # concurrent production saver such as Postgres instead.
    from langgraph.checkpoint.sqlite import SqliteSaver
    from langgraph.types import Command

    assert args.checkpoint_db is not None
    assert args.thread_id is not None
    checkpoint_path = args.checkpoint_db.expanduser().resolve()
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    config = {"configurable": {"thread_id": args.thread_id}}

    with SqliteSaver.from_conn_string(str(checkpoint_path)) as checkpointer:
        if args.start_model_review:
            proposer, challenger = _start_ports()
            graph = build_langgraph(
                proposer,
                challenger,
                maximum_proposals=2,
                checkpointer=checkpointer,
                require_human_review=True,
            )
            evidence = IncidentEvidence.model_validate_json(
                (ROOT / "fixtures" / "failure_passport.json").read_text(
                    encoding="utf-8"
                )
            )
            result = graph.invoke(
                {
                    "evidence": evidence,
                    "candidate": None,
                    "review": None,
                    "proposal_count": 0,
                    "human_review": None,
                },
                config=config,
            )
        else:
            # A valid resume continues inside the interrupted node. These guard
            # ports prevent a wrong thread from silently spending model tokens.
            graph = build_langgraph(
                _ResumeOnlyProposer(),
                _ResumeOnlyChallenger(),
                maximum_proposals=2,
                checkpointer=checkpointer,
                require_human_review=True,
            )
            result = graph.invoke(
                Command(
                    resume={
                        "approved": args.resume_human_review == "approve",
                        "reviewer": args.reviewer,
                        "rationale": args.rationale,
                    }
                ),
                config=config,
            )

    _print_result(result)


if __name__ == "__main__":
    main()
