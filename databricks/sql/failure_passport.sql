-- Illustrative read-only evidence query. Bind every marker through the SQL API;
-- never concatenate model text into an identifier, predicate, or time bound.
-- The query returns aggregates and redacted facts, not unrestricted source rows.

WITH scoped AS (
  SELECT
    transport_id,
    partner,
    payload_version,
    order_id,
    source_sequence,
    event_type,
    event_time,
    ingested_at,
    customer_id,
    order_total_minor,
    currency,
    _rescued_data
  FROM IDENTIFIER(:bronze_table) VERSION AS OF :source_delta_version
  WHERE partner = :partner
    AND event_time >= :affected_start
    AND event_time < :affected_end
),
quarantined AS (
  SELECT *
  FROM IDENTIFIER(:quarantine_table) VERSION AS OF :quarantine_delta_version
  WHERE partner = :partner
    AND event_time >= :affected_start
    AND event_time < :affected_end
),
fingerprinted AS (
  SELECT
    *,
    SHA2(TO_JSON(NAMED_STRUCT(
      'transport_id', transport_id,
      'partner', partner,
      'payload_version', payload_version,
      'order_id', order_id,
      'source_sequence', source_sequence,
      'event_type', event_type,
      'event_time', event_time,
      'ingested_at', ingested_at,
      'customer_id', customer_id,
      'order_total_minor', order_total_minor,
      'currency', currency,
      '_rescued_data', _rescued_data
    )), 256) AS row_content_digest
  FROM scoped
)
SELECT
  :incident_id AS incident_id,
  :generation AS generation,
  :pipeline_id AS pipeline_id,
  :dataset AS dataset,
  :partner AS partner,
  :payload_version AS payload_version,
  CAST(:bronze_table AS STRING) AS source_table,
  :source_delta_version AS source_delta_version,
  MIN(event_time) AS affected_start,
  MAX(event_time) + INTERVAL 1 MICROSECOND AS affected_end,
  COUNT(DISTINCT named_struct(
    'partner', partner,
    'order_id', order_id,
    'source_sequence', source_sequence
  )) AS estimated_business_events,
  COUNT_IF(payload_version = :payload_version) AS raw_profile_version_deliveries,
  (SELECT COUNT(*) FROM quarantined) AS quarantined_deliveries,
  (SELECT COUNT(*) FROM quarantined) / NULLIF(COUNT(*), 0) AS quarantine_ratio,
  SORT_ARRAY(COLLECT_SET(payload_version)) AS observed_payload_versions,
  -- The fingerprint binds the table identity, immutable Delta version, exact
  -- scope, and every retained row. COUNT/MIN/MAX alone are not content proof.
  SHA2(CONCAT_WS('|',
    CAST(:bronze_table AS STRING),
    CAST(:source_delta_version AS STRING),
    :partner,
    :payload_version,
    CAST(:affected_start AS STRING),
    CAST(:affected_end AS STRING),
    CAST(COUNT(*) AS STRING),
    CONCAT_WS(',', SORT_ARRAY(COLLECT_LIST(row_content_digest)))
  ), 256) AS bounded_source_fingerprint
FROM fingerprinted;
