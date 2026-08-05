-- Illustrative read-only evidence query. Bind every marker through the SQL API;
-- never concatenate model text into an identifier, predicate, or time bound.
-- The query returns aggregates and redacted facts, not unrestricted source rows.

WITH scoped AS (
  SELECT partner, payload_version, event_time, order_id, source_sequence
  FROM IDENTIFIER(:bronze_table)
  WHERE partner = :partner
    AND event_time >= :affected_start
    AND event_time < :affected_end
),
quarantined AS (
  SELECT *
  FROM IDENTIFIER(:quarantine_table)
  WHERE partner = :partner
    AND event_time >= :affected_start
    AND event_time < :affected_end
)
SELECT
  :incident_id AS incident_id,
  :generation AS generation,
  :pipeline_id AS pipeline_id,
  :dataset AS dataset,
  :partner AS partner,
  :payload_version AS payload_version,
  MIN(event_time) AS affected_start,
  MAX(event_time) + INTERVAL 1 MICROSECOND AS affected_end,
  COUNT(DISTINCT named_struct(
    'partner', partner,
    'order_id', order_id,
    'source_sequence', source_sequence
  )) AS estimated_business_events,
  COUNT_IF(payload_version = 'v3') AS raw_v3_deliveries,
  (SELECT COUNT(*) FROM quarantined) AS quarantined_deliveries,
  (SELECT COUNT(*) FROM quarantined) / NULLIF(COUNT(*), 0) AS quarantine_ratio,
  SORT_ARRAY(COLLECT_SET(payload_version)) AS observed_payload_versions,
  SHA2(CONCAT_WS('|',
    :partner,
    :payload_version,
    CAST(COUNT(*) AS STRING),
    CAST(MIN(event_time) AS STRING),
    CAST(MAX(event_time) AS STRING)
  ), 256) AS bounded_source_fingerprint
FROM scoped;
