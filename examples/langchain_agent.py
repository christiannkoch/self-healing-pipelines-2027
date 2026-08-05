"""Explicit, paid opt-in for the tool-less LangChain proposer/challenger."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from recovery_control_plane.agent_graph import build_langgraph
from recovery_control_plane.contracts import IncidentEvidence


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--execute-model",
        action="store_true",
        help="make two external model calls; this can incur provider charges",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.execute_model:
        print(
            "No model call made. Set RECOVERY_PROPOSER_MODEL and "
            "RECOVERY_CHALLENGER_MODEL, then pass --execute-model explicitly."
        )
        return

    # Optional imports stay behind the explicit execution boundary. No model is
    # created when the package or this module is merely imported.
    from langchain_openai import ChatOpenAI

    from recovery_control_plane.adapters.langchain import (
        LangChainChallenger,
        LangChainProposer,
    )

    proposer_name = os.environ["RECOVERY_PROPOSER_MODEL"]
    challenger_name = os.environ["RECOVERY_CHALLENGER_MODEL"]
    evidence = IncidentEvidence.model_validate_json(
        (ROOT / "fixtures" / "failure_passport.json").read_text(encoding="utf-8")
    )

    # Neither model receives tools. Their only output is a validated Pydantic
    # object. The result still needs deterministic policy compilation.
    graph = build_langgraph(
        LangChainProposer(ChatOpenAI(model=proposer_name, temperature=0)),
        LangChainChallenger(ChatOpenAI(model=challenger_name, temperature=0)),
        maximum_proposals=2,
    )
    result = graph.invoke(
        {
            "evidence": evidence,
            "candidate": None,
            "review": None,
            "proposal_count": 0,
        }
    )
    print(
        json.dumps(
            {
                "candidate": result["candidate"].model_dump(mode="json"),
                "review": (
                    result["review"].model_dump(mode="json")
                    if result.get("review")
                    else None
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
