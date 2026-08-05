-- Illustrative independent verifier. Run from a principal/query path separate
-- from the repair transformation. A missing/NULL/non-zero invariant blocks.

WITH source_scope AS (
  -- This pinned query defines the bounded replay input. Keep malformed keys and
  -- unauthorized versions visible; only the registry-authorized partner and
  -- half-open incident window establish scope. Completeness of this snapshot
  -- is a verifier-provider trust obligation.
  SELECT *
  FROM IDENTIFIER(:bronze_table) VERSION AS OF :source_delta_version
  WHERE partner = :partner
    AND event_time >= :backfill_start
    AND event_time < :backfill_end
),
source_parsed AS (
  SELECT s.*, TRY_PARSE_JSON(s._rescued_data) AS rescued_variant
  FROM source_scope s
),
source_semantics AS (
  -- Mirror the reviewed normalizer's strict scalar extraction. A coercible
  -- number is not accepted as a customer or currency string.
  SELECT
    s.*,
    CASE
      WHEN payload_version = :required_profile_version
        AND SCHEMA_OF_VARIANT(VARIANT_GET(
          rescued_variant, '$.buyer.customerId', 'variant'
        )) = 'STRING'
        AND LENGTH(TRIM(TRY_VARIANT_GET(
          rescued_variant, '$.buyer.customerId', 'string'
        ))) > 0
      THEN TRY_VARIANT_GET(rescued_variant, '$.buyer.customerId', 'string')
      WHEN payload_version = 'v2' THEN customer_id
      ELSE NULL
    END AS reviewed_customer_id,
    CASE
      WHEN payload_version = :required_profile_version
        AND SCHEMA_OF_VARIANT(VARIANT_GET(
          rescued_variant, '$.grossAmount.currencyCode', 'variant'
        )) = 'STRING'
      THEN TRY_VARIANT_GET(
        rescued_variant, '$.grossAmount.currencyCode', 'string'
      )
      WHEN payload_version = 'v2' THEN currency
      ELSE NULL
    END AS reviewed_currency
  FROM source_parsed s
),
expected AS (
  SELECT DISTINCT partner, order_id, source_sequence
  FROM source_semantics
  WHERE order_id IS NOT NULL
    AND source_sequence IS NOT NULL
),
expected_v3 AS (
  SELECT DISTINCT partner, order_id, source_sequence
  FROM source_semantics
  WHERE payload_version = :required_profile_version
    AND order_id IS NOT NULL
    AND source_sequence IS NOT NULL
),
candidate AS (
  -- Keep the bounded replay slice, plus any row claiming an expected business
  -- key. The latter preserves NULL/out-of-window event_time corruption without
  -- treating unrelated same-partner history as part of this repair. Detecting
  -- arbitrary out-of-window writes with new keys requires update lineage such
  -- as Delta CDF or an immutable action/update marker in the candidate query.
  SELECT c.*
  FROM IDENTIFIER(:candidate_table) VERSION AS OF :candidate_delta_version AS c
  WHERE c.partner = :partner
    AND (
      (c.event_time >= :backfill_start AND c.event_time < :backfill_end)
      OR EXISTS (
        SELECT 1
        FROM expected e
        WHERE e.partner = c.partner
          AND e.order_id = c.order_id
          AND e.source_sequence = c.source_sequence
      )
    )
),
candidate_keyed AS (
  -- Key-set and ledger reconciliation use only complete business keys. Rows
  -- with NULL key members remain in candidate_metrics and fail there; this
  -- mirrors the independent Python verifier's strict-key representation.
  SELECT *
  FROM candidate
  WHERE order_id IS NOT NULL AND source_sequence IS NOT NULL
),
candidate_duplicates AS (
  SELECT partner, order_id, source_sequence, COUNT(*) - 1 AS extra_rows
  FROM candidate_keyed
  GROUP BY partner, order_id, source_sequence
  HAVING COUNT(*) > 1
),
settlement_raw AS (
  -- This table/version is the bounded verifier input manifest. Do not inner
  -- join to expected: an unexpected settlement row is itself failing evidence.
  -- :settlement_relation must equal the relation sealed in the compiled plan.
  SELECT s.*
  FROM IDENTIFIER(:settlement_relation) VERSION AS OF :settlement_delta_version AS s
),
settlement_complete AS (
  SELECT
    partner,
    order_id,
    source_sequence,
    currency,
    SUM(settled_amount_minor) AS settled_amount_minor
  FROM settlement_raw
  WHERE currency IS NOT NULL AND settled_amount_minor IS NOT NULL
  GROUP BY partner, order_id, source_sequence, currency
),
settlement_keys AS (
  SELECT DISTINCT partner, order_id, source_sequence
  FROM settlement_complete
),
settlement_currency_sets AS (
  SELECT
    partner,
    order_id,
    source_sequence,
    COLLECT_SET(currency) AS currencies
  FROM settlement_complete
  GROUP BY partner, order_id, source_sequence
),
currency_mismatch_by_key AS (
  SELECT
    c.partner,
    c.order_id,
    c.source_sequence,
    CASE
      WHEN c.currency IS NULL THEN 1
      WHEN NOT ARRAY_CONTAINS(s.currencies, c.currency) THEN 1
      ELSE SIZE(FILTER(s.currencies, currency -> currency <> c.currency))
    END AS mismatch_count
  FROM candidate_keyed c
  JOIN settlement_currency_sets s USING (partner, order_id, source_sequence)
),
source_metrics AS (
  SELECT
    COUNT(*) AS source_rows,
    COUNT_IF(
    order_id IS NULL OR TRIM(order_id) = '' OR source_sequence IS NULL
    ) AS source_invalid_business_key_rows,
    COUNT_IF(
      payload_version IS DISTINCT FROM :required_profile_version
      AND payload_version IS DISTINCT FROM 'v2'
    ) AS source_unauthorized_payload_versions,
    COUNT_IF(
      (payload_version = :required_profile_version OR payload_version = 'v2')
      AND reviewed_currency IS DISTINCT FROM :required_currency_code
    ) AS source_profile_currency_mismatches
  FROM source_semantics
),
candidate_metrics AS (
  SELECT
    COUNT(*) AS candidate_rows,
    COUNT_IF(mapping_valid IS NOT TRUE) AS mapping_validation_failures,
    COUNT_IF(payload_version_authorized IS NOT TRUE)
      AS payload_authorization_failures,
    COUNT_IF(order_id IS NULL OR TRIM(order_id) = '') AS missing_order_id_rows,
    COUNT_IF(customer_id IS NULL OR TRIM(customer_id) = '')
      AS missing_customer_id_rows,
    COUNT_IF(
      event_time IS NULL
      OR event_time < :backfill_start
      OR event_time >= :backfill_end
    ) AS invalid_event_time_rows,
    COUNT_IF(event_type IS NULL OR event_type NOT IN ('SALE', 'REFUND'))
      AS unsupported_event_type_rows,
    COUNT_IF(
      event_type = 'SALE'
      AND (order_total_minor IS NULL OR order_total_minor <= 0)
    ) AS invalid_sale_amount_rows,
    COUNT_IF(
      event_type = 'REFUND'
      AND (order_total_minor IS NULL OR order_total_minor >= 0)
    ) AS invalid_refund_amount_rows,
    COUNT_IF(currency IS DISTINCT FROM :required_currency_code)
      AS candidate_profile_currency_mismatches,
    -- Missing proof is failure under SQL's three-valued logic.
    COUNT_IF(canonical_valid IS NOT TRUE) AS canonical_quality_failures,
    COUNT_IF(order_total_minor IS NULL) AS missing_amount_rows,
    COUNT_IF(currency IS NULL) AS missing_currency_rows,
    COUNT_IF(
      payload_version IS DISTINCT FROM :required_profile_version
      AND payload_version IS DISTINCT FROM 'v2'
    ) AS unauthorized_payload_versions,
    CASE WHEN :required_profile_digest IS NULL THEN 1 ELSE COUNT_IF(
      payload_version = :required_profile_version
      AND applied_profile_digest IS DISTINCT FROM :required_profile_digest
    ) END AS profile_digest_mismatches,
    CASE
      WHEN :required_profile_digest IS NULL OR :required_profile_id IS NULL THEN 1
      ELSE COUNT_IF(
      CASE
        WHEN payload_version = :required_profile_version THEN
          payload_version_authorized IS NOT TRUE
          OR applied_profile_id IS DISTINCT FROM :required_profile_id
          OR applied_profile_digest IS DISTINCT FROM :required_profile_digest
        WHEN payload_version = 'v2' THEN
          payload_version_authorized IS NOT TRUE
          OR applied_profile_id IS NOT NULL
          OR applied_profile_digest IS NOT NULL
        ELSE
          payload_version_authorized IS TRUE
          OR applied_profile_id IS NOT NULL
          OR applied_profile_digest IS NOT NULL
      END
      )
    END AS profile_provenance_mismatches,
    COUNT_IF(dedupe_ambiguous IS NOT FALSE) AS dedupe_ambiguities
  FROM candidate
),
equivalence_metrics AS (
  SELECT
    (SELECT COUNT(*) FROM candidate c WHERE NOT EXISTS (
      SELECT 1 FROM source_semantics s
      WHERE s.transport_id = c.transport_id
        AND s.partner = c.partner
        AND s.order_id = c.order_id
        AND s.source_sequence = c.source_sequence
        AND s.payload_version = c.payload_version
        AND s.event_time <=> c.event_time
    )) AS source_candidate_event_time_mismatches,
    (SELECT COUNT(*) FROM candidate c WHERE NOT EXISTS (
      SELECT 1 FROM source_semantics s
      WHERE s.transport_id = c.transport_id
        AND s.partner = c.partner
        AND s.order_id = c.order_id
        AND s.source_sequence = c.source_sequence
        AND s.payload_version = c.payload_version
        AND s.event_type <=> c.event_type
    )) AS source_candidate_event_type_mismatches,
    (SELECT COUNT(*) FROM candidate c WHERE NOT EXISTS (
      SELECT 1 FROM source_semantics s
      WHERE s.transport_id = c.transport_id
        AND s.partner = c.partner
        AND s.order_id = c.order_id
        AND s.source_sequence = c.source_sequence
        AND s.payload_version = c.payload_version
        AND s.reviewed_customer_id <=> c.customer_id
    )) AS source_candidate_customer_id_mismatches
),
coverage_metrics AS (
  SELECT
    (SELECT COUNT(*) FROM expected) AS expected_rows,
    (SELECT COUNT(*) FROM expected_v3) AS expected_v3_rows,
    (SELECT COUNT(*) FROM expected e LEFT ANTI JOIN candidate c
      USING (partner, order_id, source_sequence)) AS missing_expected_rows,
    (SELECT COUNT(*) FROM candidate_keyed c LEFT ANTI JOIN expected e
      USING (partner, order_id, source_sequence)) AS unexpected_candidate_rows,
    (SELECT COUNT(*) FROM expected_v3 e
      LEFT JOIN candidate_keyed c USING (partner, order_id, source_sequence)
      WHERE :required_profile_digest IS NULL
        OR c.partner IS NULL
        OR c.payload_version IS DISTINCT FROM :required_profile_version
        OR c.canonical_valid IS NOT TRUE
        OR c.applied_profile_digest IS DISTINCT FROM :required_profile_digest
    ) AS unproven_expected_v3_rows
),
duplicate_metrics AS (
  SELECT COALESCE(SUM(extra_rows), 0) AS duplicate_business_events
  FROM candidate_duplicates
),
settlement_metrics AS (
  SELECT
    COUNT_IF(currency IS NULL) AS null_settlement_currency_rows,
    COUNT_IF(settled_amount_minor IS NULL) AS null_settlement_amount_rows
  FROM settlement_raw
),
reconciliation_metrics AS (
  SELECT
    (SELECT COUNT(*) FROM settlement_keys s LEFT ANTI JOIN candidate_keyed c
      USING (partner, order_id, source_sequence)) AS settlement_without_candidate,
    (SELECT COUNT(*) FROM candidate_keyed c LEFT ANTI JOIN settlement_keys s
      USING (partner, order_id, source_sequence)) AS candidate_without_settlement,
    COALESCE((SELECT SUM(mismatch_count) FROM currency_mismatch_by_key), 0)
      AS currency_mismatches,
    COALESCE((SELECT SUM(ABS(c.order_total_minor - s.settled_amount_minor))
      FROM candidate_keyed c JOIN settlement_complete s
      USING (partner, order_id, source_sequence, currency)), 0)
      AS settlement_difference_minor
)
SELECT
  sm.source_rows,
  cm.candidate_rows,
  cov.expected_rows,
  cov.expected_v3_rows,
  sm.source_invalid_business_key_rows,
  sm.source_unauthorized_payload_versions,
  sm.source_profile_currency_mismatches,
  dup.duplicate_business_events,
  cm.mapping_validation_failures,
  cm.payload_authorization_failures,
  cm.missing_order_id_rows,
  cm.missing_customer_id_rows,
  cm.invalid_event_time_rows,
  cm.unsupported_event_type_rows,
  cm.invalid_sale_amount_rows,
  cm.invalid_refund_amount_rows,
  cm.candidate_profile_currency_mismatches,
  eq.source_candidate_event_time_mismatches,
  eq.source_candidate_event_type_mismatches,
  eq.source_candidate_customer_id_mismatches,
  cm.canonical_quality_failures,
  cm.missing_amount_rows,
  cm.missing_currency_rows,
  cm.unauthorized_payload_versions,
  cm.profile_digest_mismatches,
  cm.profile_provenance_mismatches,
  cm.dedupe_ambiguities,
  cov.missing_expected_rows,
  cov.unexpected_candidate_rows,
  cov.unproven_expected_v3_rows,
  stm.null_settlement_currency_rows,
  stm.null_settlement_amount_rows,
  rec.settlement_without_candidate,
  rec.candidate_without_settlement,
  rec.currency_mismatches,
  rec.settlement_difference_minor
FROM source_metrics sm
CROSS JOIN candidate_metrics cm
CROSS JOIN equivalence_metrics eq
CROSS JOIN coverage_metrics cov
CROSS JOIN duplicate_metrics dup
CROSS JOIN settlement_metrics stm
CROSS JOIN reconciliation_metrics rec;
