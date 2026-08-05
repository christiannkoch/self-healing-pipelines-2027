import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "databricks" / "sql" / "verification.sql").read_text(encoding="utf-8")
PASSPORT_SQL = (ROOT / "databricks" / "sql" / "failure_passport.sql").read_text(
    encoding="utf-8"
)
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


def test_verification_sql_pins_all_evidence_snapshots() -> None:
    assert "VERSION AS OF :source_delta_version" in SQL
    assert "VERSION AS OF :candidate_delta_version" in SQL
    assert "VERSION AS OF :settlement_delta_version" in SQL
    assert "IDENTIFIER(:settlement_relation)" in SQL
    assert "IDENTIFIER(:settlement_table)" not in SQL


def test_verification_sql_counts_source_failures_before_key_derivation() -> None:
    source_scope = SQL.split("source_scope AS (", 1)[1].split("),", 1)[0]
    expected = SQL.split("expected AS (", 1)[1].split("),", 1)[0]
    assert "order_id IS NOT NULL" not in source_scope
    assert "source_sequence IS NOT NULL" not in source_scope
    assert "payload_version = :required_profile_version" not in source_scope
    assert "FROM source_semantics" in expected
    assert "source_invalid_business_key_rows" in SQL
    assert "source_unauthorized_payload_versions" in SQL
    assert "is a verifier-provider trust obligation" in source_scope


def test_verification_sql_does_not_hide_unexpected_settlements() -> None:
    settlement_cte = SQL.split("settlement_raw AS (", 1)[1].split("),", 1)[0]
    assert "JOIN expected" not in settlement_cte
    assert "settlement_keys s LEFT ANTI JOIN candidate_keyed c" in SQL


def test_verification_sql_excludes_incomplete_keys_only_from_key_set_math() -> None:
    keyed_cte = SQL.split("candidate_keyed AS (", 1)[1].split("),", 1)[0]
    candidate_metrics = SQL.split("candidate_metrics AS (", 1)[1].split("),", 1)[0]
    assert "order_id IS NOT NULL AND source_sequence IS NOT NULL" in keyed_cte
    assert "FROM candidate" in candidate_metrics
    assert "FROM candidate_keyed" not in candidate_metrics


def test_verification_sql_scopes_history_without_hiding_expected_key_corruption() -> None:
    candidate_cte = SQL.split("candidate AS (", 1)[1].split("),", 1)[0]
    assert "c.event_time >= :backfill_start" in candidate_cte
    assert "c.event_time < :backfill_end" in candidate_cte
    assert "OR EXISTS" in candidate_cte
    assert "e.order_id = c.order_id" in candidate_cte
    assert "e.source_sequence = c.source_sequence" in candidate_cte
    assert "Delta CDF" in candidate_cte
    assert "event_time IS NULL" in SQL
    assert "event_time < :backfill_start" in SQL
    assert "event_time >= :backfill_end" in SQL


@pytest.mark.parametrize(
    "required_expression",
    [
        "mapping_valid IS NOT TRUE",
        "payload_version_authorized IS NOT TRUE",
        "order_id IS NULL OR TRIM(order_id) = ''",
        "customer_id IS NULL OR TRIM(customer_id) = ''",
        "event_type IS NULL OR event_type NOT IN ('SALE', 'REFUND')",
        "order_total_minor IS NULL OR order_total_minor <= 0",
        "order_total_minor IS NULL OR order_total_minor >= 0",
        "currency IS DISTINCT FROM :required_currency_code",
        "reviewed_currency IS DISTINCT FROM :required_currency_code",
        "s.event_time <=> c.event_time",
        "s.event_type <=> c.event_type",
        "s.reviewed_customer_id <=> c.customer_id",
        "TRY_PARSE_JSON(s._rescued_data)",
        "SCHEMA_OF_VARIANT(VARIANT_GET(",
    ],
)
def test_verification_sql_declares_independent_semantic_guards(
    required_expression,
) -> None:
    assert required_expression in SQL


def test_failure_passport_binds_source_identity_version_and_content() -> None:
    assert "VERSION AS OF :source_delta_version" in PASSPORT_SQL
    assert "CAST(:bronze_table AS STRING)" in PASSPORT_SQL
    assert "row_content_digest" in PASSPORT_SQL
    assert "SORT_ARRAY(COLLECT_LIST(row_content_digest))" in PASSPORT_SQL
    assert "_rescued_data" in PASSPORT_SQL
