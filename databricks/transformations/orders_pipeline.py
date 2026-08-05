"""Spark Declarative Pipelines transformation for bounded recovery.

The bronze contract uses Auto Loader's default ``_rescued_data`` JSON string.
The reviewed v3 normalizer parses it as VARIANT so quoted and fractional money
values remain distinguishable from integers. Unknown payload versions remain
visible as invalid quarantine candidates; they never fall back to v2.

The selected pipeline runtime must provide ``try_parse_json``, ``variant_get``,
``try_variant_get``, and ``schema_of_variant``. Bundle validation should reject
a runtime/channel that does not expose those functions.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Iterable

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# Databricks injects ``spark`` before evaluating pipeline functions. Preserve
# that binding there while keeping this module importable for local unit tests.
if "spark" not in globals():
    spark = None


LONG_MIN = -(2**63)
LONG_MAX = 2**63 - 1
NANOS_PER_UNIT = 1_000_000_000
DECIMAL_38_0 = DecimalType(38, 0)
INTEGER_SPARK_TYPES = ("tinyint", "smallint", "int", "bigint")

PROFILE_ID = "marketplace-v3-gross-eur-v2"
PROFILE_PARTNER = "marketplace_it"
PROFILE_PAYLOAD_VERSION = "v3"
PROFILE_SOURCE_FINGERPRINT = "sha256:source-v3"
LEGACY_PAYLOAD_VERSIONS = ("v2",)
NORMALIZER_KEY = "MARKETPLACE_V3_GROSS"
NORMALIZER_VERSION = "1.0.1"
NORMALIZER_SOURCE_DIGEST = (
    "sha256:deefca6e8c07abf7e1acb6c7a890d54bc0aed343774b2a5c3a4b1ccd6a784ac1"
)
PROFILE_CURRENCY = "EUR"
PROFILE_CURRENCY_EXPONENT = 2
PROFILE_DEDUPE_KEY = ("partner", "order_id", "source_sequence")
SUPPORTED_DEDUPE_FIELDS = frozenset(
    {"partner", "order_id", "source_sequence", "transport_id"}
)


def _is_integral_spark_type(value: Column) -> Column:
    """Return whether a column's declared type is an integral Spark type."""

    return F.lower(F.typeof(value)).isin(*INTEGER_SPARK_TYPES)


def exact_eur_minor(
    units: Column,
    nanos: Column,
    *,
    wire_types_valid: Column | None = None,
) -> Column:
    """Translate strict integer units/nanos without rounding or overflow."""

    types_valid = wire_types_valid
    if types_valid is None:
        types_valid = _is_integral_spark_type(units) & _is_integral_spark_type(nanos)

    # ``try_cast`` prevents malformed values from aborting the whole batch;
    # ``types_valid`` prevents successful casts from authorizing coercion.
    strict_units = units.try_cast(DECIMAL_38_0)
    strict_nanos = nanos.try_cast(DECIMAL_38_0)
    numerator = strict_nanos * F.lit(100)
    exact_minor = strict_units * F.lit(100) + numerator / F.lit(NANOS_PER_UNIT)
    signs_match = ~(
        ((strict_units > 0) & (strict_nanos < 0))
        | ((strict_units < 0) & (strict_nanos > 0))
    )
    valid = (
        types_valid
        & strict_units.isNotNull()
        & strict_nanos.isNotNull()
        & (F.abs(strict_nanos) < NANOS_PER_UNIT)
        & signs_match
        & (F.pmod(numerator, F.lit(NANOS_PER_UNIT)) == 0)
        & exact_minor.between(LONG_MIN, LONG_MAX)
    )
    return F.when(valid, exact_minor.cast("long"))


def _validate_profile_runtime(
    *,
    profile_id: str,
    partner: str,
    payload_version: str,
    source_fingerprint: str,
    legacy_payload_versions: tuple[str, ...],
    normalizer_key: str,
    normalizer_version: str,
    normalizer_source_digest: str,
    currency_code: str,
    currency_exponent: int,
    dedupe_key: tuple[str, ...],
    profile_digest: str,
) -> None:
    """Fail pipeline analysis if configuration is not the shipped profile."""

    supplied_implementation = (
        partner,
        payload_version,
        normalizer_key,
        normalizer_version,
        normalizer_source_digest,
        currency_code,
        currency_exponent,
    )
    expected_implementation = (
        PROFILE_PARTNER,
        PROFILE_PAYLOAD_VERSION,
        NORMALIZER_KEY,
        NORMALIZER_VERSION,
        NORMALIZER_SOURCE_DIGEST,
        PROFILE_CURRENCY,
        PROFILE_CURRENCY_EXPONENT,
    )
    if supplied_implementation != expected_implementation:
        raise ValueError("pipeline configuration does not name the shipped mapping profile")
    if not profile_id:
        raise ValueError("profile ID must be non-empty")
    if legacy_payload_versions != LEGACY_PAYLOAD_VERSIONS:
        raise ValueError("the shipped Spark normalizer supports exactly legacy v2")
    _validate_dedupe_key(dedupe_key)
    if re.fullmatch(r"[0-9a-f]{64}", profile_digest) is None:
        raise ValueError("profile digest must be a lowercase SHA-256 digest")

    # The digest is not decoration. Recompute it from every deployed profile
    # field so a bundle cannot retain an approved digest while changing the
    # source contract, normalizer, currency semantics, or dedupe key.
    profile_spec = {
        "profile_id": profile_id,
        "partner": partner,
        "payload_version": payload_version,
        "source_fingerprint": source_fingerprint,
        "normalizer_key": normalizer_key,
        "normalizer_version": normalizer_version,
        "normalizer_source_digest": normalizer_source_digest,
        "currency_code": currency_code,
        "currency_exponent": currency_exponent,
        "legacy_payload_versions": list(legacy_payload_versions),
        "dedupe_key": list(dedupe_key),
    }
    canonical_profile = json.dumps(
        profile_spec,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    observed_digest = hashlib.sha256(canonical_profile).hexdigest()
    if observed_digest != profile_digest:
        raise ValueError("profile digest does not match the deployed profile content")


def canonical_candidates(
    bronze: DataFrame,
    profile_digest: str,
    *,
    rescued_data_column: str = "_rescued_data",
    profile_id: str = PROFILE_ID,
    partner: str = PROFILE_PARTNER,
    payload_version: str = PROFILE_PAYLOAD_VERSION,
    source_fingerprint: str = PROFILE_SOURCE_FINGERPRINT,
    legacy_payload_versions: tuple[str, ...] = LEGACY_PAYLOAD_VERSIONS,
    normalizer_key: str = NORMALIZER_KEY,
    normalizer_version: str = NORMALIZER_VERSION,
    normalizer_source_digest: str = NORMALIZER_SOURCE_DIGEST,
    currency_code: str = PROFILE_CURRENCY,
    currency_exponent: int = PROFILE_CURRENCY_EXPONENT,
    dedupe_key: tuple[str, ...] = PROFILE_DEDUPE_KEY,
) -> DataFrame:
    """Normalize the exact v2/v3 allowlist and retain provenance columns."""

    _validate_profile_runtime(
        profile_id=profile_id,
        partner=partner,
        payload_version=payload_version,
        source_fingerprint=source_fingerprint,
        legacy_payload_versions=legacy_payload_versions,
        normalizer_key=normalizer_key,
        normalizer_version=normalizer_version,
        normalizer_source_digest=normalizer_source_digest,
        currency_code=currency_code,
        currency_exponent=currency_exponent,
        dedupe_key=dedupe_key,
        profile_digest=profile_digest,
    )

    # Auto Loader's rescued-data column is a JSON string. VARIANT preserves
    # scalar types, allowing quoted/fractional money to fail closed.
    rescued = F.try_parse_json(F.col(rescued_data_column))
    units_variant = F.variant_get(rescued, "$.grossAmount.units", "variant")
    nanos_variant = F.variant_get(rescued, "$.grossAmount.nanos", "variant")
    customer_variant = F.variant_get(rescued, "$.buyer.customerId", "variant")
    currency_variant = F.variant_get(
        rescued, "$.grossAmount.currencyCode", "variant"
    )
    units = F.try_variant_get(rescued, "$.grossAmount.units", "bigint")
    nanos = F.try_variant_get(rescued, "$.grossAmount.nanos", "bigint")
    v3_customer = F.try_variant_get(rescued, "$.buyer.customerId", "string")
    v3_currency = F.try_variant_get(
        rescued, "$.grossAmount.currencyCode", "string"
    )
    money_types_valid = (
        (F.schema_of_variant(units_variant) == "BIGINT")
        & (F.schema_of_variant(nanos_variant) == "BIGINT")
    )
    v3_shape_types_valid = F.coalesce(
        money_types_valid
        & (F.schema_of_variant(customer_variant) == "STRING")
        & (F.schema_of_variant(currency_variant) == "STRING"),
        F.lit(False),
    )

    is_profile_version = (F.col("partner") == partner) & (
        F.col("payload_version") == payload_version
    )
    is_legacy_version = (F.col("partner") == partner) & F.col(
        "payload_version"
    ).isin(*legacy_payload_versions)
    payload_version_authorized = F.coalesce(
        is_profile_version | is_legacy_version,
        F.lit(False),
    )

    v3_amount = exact_eur_minor(units, nanos, wire_types_valid=money_types_valid)
    v3_mapping_valid = F.coalesce(
        is_profile_version
        & v3_shape_types_valid
        & v3_amount.isNotNull()
        & (v3_currency == currency_code)
        & (F.length(F.trim(v3_customer)) > 0),
        F.lit(False),
    )
    legacy_amount_type_valid = _is_integral_spark_type(F.col("order_total_minor"))
    legacy_amount = F.col("order_total_minor").try_cast("long")
    legacy_mapping_valid = F.coalesce(
        is_legacy_version
        & legacy_amount_type_valid
        & legacy_amount.isNotNull()
        & (F.col("currency") == currency_code)
        & (F.length(F.trim(F.col("customer_id"))) > 0),
        F.lit(False),
    )

    amount = (
        F.when(is_profile_version, v3_amount)
        .when(is_legacy_version, legacy_amount)
        .otherwise(F.lit(None).cast("long"))
    )
    currency = (
        F.when(is_profile_version, v3_currency)
        .when(is_legacy_version, F.col("currency"))
        .otherwise(F.lit(None).cast("string"))
    )
    customer = (
        F.when(is_profile_version, v3_customer)
        .when(is_legacy_version, F.col("customer_id"))
        .otherwise(F.lit(None).cast("string"))
    )
    mapping_valid = v3_mapping_valid | legacy_mapping_valid
    source_sequence_type_valid = _is_integral_spark_type(F.col("source_sequence"))
    event_shape_valid = (
        ((F.col("event_type") == "SALE") & (amount > 0))
        | ((F.col("event_type") == "REFUND") & (amount < 0))
    )
    canonical_valid = F.coalesce(
        mapping_valid
        & payload_version_authorized
        & (F.length(F.trim(F.col("order_id"))) > 0)
        & source_sequence_type_valid
        & F.col("source_sequence").isNotNull()
        & F.col("event_time").isNotNull()
        & event_shape_valid,
        F.lit(False),
    )

    return bronze.select(
        "transport_id",
        "partner",
        "order_id",
        F.col("source_sequence").try_cast("long").alias("source_sequence"),
        "payload_version",
        "event_type",
        "event_time",
        "ingested_at",
        customer.alias("customer_id"),
        amount.alias("order_total_minor"),
        currency.alias("currency"),
        mapping_valid.alias("mapping_valid"),
        canonical_valid.alias("canonical_valid"),
        payload_version_authorized.alias("payload_version_authorized"),
        F.when(v3_mapping_valid, F.lit(profile_id)).alias("applied_profile_id"),
        F.when(v3_mapping_valid, F.lit(profile_digest)).alias(
            "applied_profile_digest"
        ),
        F.lit(False).alias("dedupe_ambiguous"),
    )


def _validate_dedupe_key(dedupe_key: Iterable[str]) -> tuple[str, ...]:
    fields = tuple(dedupe_key)
    if not fields or len(set(fields)) != len(fields):
        raise ValueError("dedupe key must contain unique fields")
    if set(fields) - SUPPORTED_DEDUPE_FIELDS:
        raise ValueError("dedupe key contains an unsupported field")
    return fields


def deterministic_winners(
    candidates: DataFrame,
    dedupe_key: tuple[str, ...] = PROFILE_DEDUPE_KEY,
) -> DataFrame:
    """Choose a stable winner and flag irreducible conflicting ties."""

    partition_fields = _validate_dedupe_key(dedupe_key)
    partition = Window.partitionBy(*partition_fields)
    operational_order = partition.orderBy(
        F.col("canonical_valid").desc(),
        (F.col("payload_version") == PROFILE_PAYLOAD_VERSION).cast("int").desc(),
        F.col("ingested_at").desc(),
        F.col("transport_id").desc(),
    )
    content_columns = [
        column for column in candidates.columns if column != "dedupe_ambiguous"
    ]
    ranked = (
        candidates.withColumn(
            "_content_digest",
            F.sha2(F.to_json(F.struct(*content_columns)), 256),
        )
        .withColumn("_operational_rank", F.dense_rank().over(operational_order))
    )
    top_digest = F.when(
        F.col("_operational_rank") == 1, F.col("_content_digest")
    )
    ranked = (
        ranked.withColumn("_top_digest_min", F.min(top_digest).over(partition))
        .withColumn("_top_digest_max", F.max(top_digest).over(partition))
        .withColumn(
            "dedupe_ambiguous",
            F.col("_top_digest_min") != F.col("_top_digest_max"),
        )
    )
    deterministic_order = partition.orderBy(
        F.col("canonical_valid").desc(),
        (F.col("payload_version") == PROFILE_PAYLOAD_VERSION).cast("int").desc(),
        F.col("ingested_at").desc(),
        F.col("transport_id").desc(),
        F.col("_content_digest").desc(),
    )
    return (
        ranked.withColumn("_winner_rank", F.row_number().over(deterministic_order))
        .where(F.col("_winner_rank") == 1)
        .withColumn(
            "canonical_valid",
            F.col("canonical_valid") & ~F.col("dedupe_ambiguous"),
        )
        .drop(
            "_content_digest",
            "_operational_rank",
            "_top_digest_min",
            "_top_digest_max",
            "_winner_rank",
        )
    )


@dp.table(
    name="orders_repaired",
    comment="Bounded mixed-version candidate; promotion is a separate verified step.",
    table_properties={"pipelines.reset.allowed": "false"},
    # A start-update override replaces this predicate. The authenticated
    # recovery gateway must therefore enforce the plan scope independently.
    replace_where="partner = 'marketplace_it' AND event_time >= current_timestamp() - INTERVAL 1 DAY",
)
def orders_repaired() -> DataFrame:
    source_table = spark.conf.get("source.orders_table")
    source_delta_version = int(spark.conf.get("recovery.source_delta_version"))
    rescued_data_column = spark.conf.get(
        "source.rescued_data_column", "_rescued_data"
    )
    profile_digest = spark.conf.get("recovery.profile_digest")
    profile_id = spark.conf.get("recovery.profile_id")
    partner = spark.conf.get("recovery.profile_partner")
    payload_version = spark.conf.get("recovery.profile_payload_version")
    source_fingerprint = spark.conf.get("recovery.source_fingerprint")
    legacy_versions = tuple(
        item.strip()
        for item in spark.conf.get("recovery.legacy_payload_versions").split(",")
        if item.strip()
    )
    normalizer_key = spark.conf.get("recovery.normalizer_key")
    normalizer_version = spark.conf.get("recovery.normalizer_version")
    normalizer_source_digest = spark.conf.get("recovery.normalizer_source_digest")
    currency_code = spark.conf.get("recovery.currency_code")
    currency_exponent = int(spark.conf.get("recovery.currency_exponent"))
    dedupe_key = tuple(
        item.strip()
        for item in spark.conf.get("recovery.dedupe_key").split(",")
        if item.strip()
    )

    # Pin the candidate to the exact Delta snapshot sealed into the action.
    bronze = spark.read.option("versionAsOf", source_delta_version).table(source_table)
    candidates = canonical_candidates(
        bronze,
        profile_digest,
        rescued_data_column=rescued_data_column,
        profile_id=profile_id,
        partner=partner,
        payload_version=payload_version,
        source_fingerprint=source_fingerprint,
        legacy_payload_versions=legacy_versions,
        normalizer_key=normalizer_key,
        normalizer_version=normalizer_version,
        normalizer_source_digest=normalizer_source_digest,
        currency_code=currency_code,
        currency_exponent=currency_exponent,
        dedupe_key=dedupe_key,
    )
    return deterministic_winners(candidates, dedupe_key)
