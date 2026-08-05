-- Illustrative independent verifier. Run from a principal/query path separate
-- from the repair transformation. A missing/NULL/non-zero invariant blocks.

WITH expected AS (
  SELECT DISTINCT partner, order_id, source_sequence
  FROM IDENTIFIER(:bronze_table)
  WHERE partner = :partner
    AND event_time >= :backfill_start
    AND event_time < :backfill_end
    AND order_id IS NOT NULL
    AND source_sequence IS NOT NULL
),
expected_v3 AS (
  SELECT DISTINCT partner, order_id, source_sequence
  FROM IDENTIFIER(:bronze_table)
  WHERE partner = :partner
    AND event_time >= :backfill_start
    AND event_time < :backfill_end
    AND payload_version = 'v3'
    AND order_id IS NOT NULL
    AND source_sequence IS NOT NULL
),
candidate AS (
  SELECT *
  FROM IDENTIFIER(:candidate_table)
  WHERE partner = :partner
    AND event_time >= :backfill_start
    AND event_time < :backfill_end
),
candidate_duplicates AS (
  SELECT partner, order_id, source_sequence, COUNT(*) - 1 AS extra_rows
  FROM candidate
  GROUP BY partner, order_id, source_sequence
  HAVING COUNT(*) > 1
),
settlement_raw AS (
  SELECT s.*
  FROM IDENTIFIER(:settlement_table) s
  INNER JOIN expected e USING (partner, order_id, source_sequence)
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
)
SELECT
  (SELECT COUNT(*) FROM candidate) AS candidate_rows,
  (SELECT COUNT(*) FROM expected) AS expected_rows,
  (SELECT COUNT(*) FROM expected_v3) AS expected_v3_rows,
  COALESCE((SELECT SUM(extra_rows) FROM candidate_duplicates), 0)
    AS duplicate_business_events,
  -- Spark SQL's three-valued logic makes ``NOT NULL`` evaluate to NULL. Use
  -- explicit null-safe predicates so missing proof is counted as a failure.
  (SELECT COUNT_IF(canonical_valid IS NOT TRUE) FROM candidate)
    AS canonical_quality_failures,
  (SELECT COUNT_IF(order_total_minor IS NULL) FROM candidate)
    AS missing_amount_rows,
  (SELECT COUNT_IF(currency IS NULL) FROM candidate)
    AS missing_currency_rows,
  CASE
    WHEN :required_profile_digest IS NULL THEN 1
    ELSE (SELECT COUNT_IF(
      payload_version = 'v3'
      AND applied_profile_digest IS DISTINCT FROM :required_profile_digest
    ) FROM candidate)
  END AS profile_digest_mismatches,
  (SELECT COUNT(*) FROM expected e LEFT ANTI JOIN candidate c
    USING (partner, order_id, source_sequence)) AS missing_expected_rows,
  (SELECT COUNT(*) FROM candidate c LEFT ANTI JOIN expected e
    USING (partner, order_id, source_sequence)) AS unexpected_candidate_rows,
  (SELECT COUNT(*)
   FROM expected_v3 e
   LEFT JOIN candidate c USING (partner, order_id, source_sequence)
   WHERE :required_profile_digest IS NULL
      OR c.partner IS NULL
      OR c.payload_version IS DISTINCT FROM 'v3'
      OR c.canonical_valid IS NOT TRUE
      OR c.applied_profile_digest IS DISTINCT FROM :required_profile_digest
  ) AS unproven_expected_v3_rows,
  (SELECT COUNT_IF(currency IS NULL) FROM settlement_raw)
    AS null_settlement_currency_rows,
  (SELECT COUNT_IF(settled_amount_minor IS NULL) FROM settlement_raw)
    AS null_settlement_amount_rows,
  (SELECT COUNT(*) FROM settlement_complete s LEFT ANTI JOIN candidate c
    USING (partner, order_id, source_sequence)) AS settlement_without_candidate,
  (SELECT COUNT(*) FROM candidate c LEFT ANTI JOIN settlement_complete s
    USING (partner, order_id, source_sequence)) AS candidate_without_settlement,
  (SELECT COUNT(*)
   FROM candidate c
   JOIN settlement_complete s USING (partner, order_id, source_sequence)
   WHERE c.currency IS DISTINCT FROM s.currency
  ) AS currency_mismatches,
  COALESCE((
    SELECT SUM(ABS(c.order_total_minor - s.settled_amount_minor))
    FROM candidate c
    JOIN settlement_complete s
      USING (partner, order_id, source_sequence, currency)
  ), 0) AS settlement_difference_minor;
