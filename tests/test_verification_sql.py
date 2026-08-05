import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "databricks" / "sql" / "verification.sql").read_text(encoding="utf-8")
ADVERSARIES = json.loads(
    (ROOT / "fixtures" / "sql_null_adversaries.json").read_text(encoding="utf-8")
)


@pytest.mark.parametrize("adversary", ADVERSARIES, ids=lambda item: item["case"])
def test_verification_sql_uses_explicit_null_safe_guards(adversary) -> None:
    """A NULL proof value must increment a failure metric, never disappear."""

    assert adversary["required_expression"] in SQL
    assert adversary["unsafe_expression"] not in SQL


def test_verification_sql_does_not_coalesce_proof_columns_to_success() -> None:
    normalized = " ".join(SQL.lower().split())
    assert "coalesce(canonical_valid, true)" not in normalized
    assert "coalesce(applied_profile_digest, :required_profile_digest)" not in normalized
