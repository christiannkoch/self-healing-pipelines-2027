"""Illustrative Spark Declarative Pipelines transformation for the article.

This file is intentionally outside the installable Python package. Import it
only inside a compatible Databricks pipeline runtime after bundle validation.
"""

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


LONG_MIN = -(2**63)
LONG_MAX = 2**63 - 1
NANOS_PER_UNIT = 1_000_000_000
MINOR_PER_EUR = 100
DECIMAL_38_0 = DecimalType(38, 0)


def exact_eur_minor(units: Column, nanos: Column) -> Column:
    """Translate units/nanos without floats, rounding, or silent overflow."""

    strict_units = units.cast(DECIMAL_38_0)
    strict_nanos = nanos.cast(DECIMAL_38_0)
    numerator = strict_nanos * F.lit(MINOR_PER_EUR)
    exact_minor = strict_units * F.lit(MINOR_PER_EUR) + numerator / F.lit(
        NANOS_PER_UNIT
    )
    signs_match = ~(
        ((strict_units > 0) & (strict_nanos < 0))
        | ((strict_units < 0) & (strict_nanos > 0))
    )
    valid = (
        strict_units.isNotNull()
        & strict_nanos.isNotNull()
        & (F.abs(strict_nanos) < NANOS_PER_UNIT)
        & signs_match
        & (F.pmod(numerator, F.lit(NANOS_PER_UNIT)) == 0)
        & exact_minor.between(LONG_MIN, LONG_MAX)
    )
    return F.when(valid, exact_minor.cast("long"))


def canonical_candidates(bronze: DataFrame, profile_digest: str) -> DataFrame:
    """Normalize v2/v3 side by side and retain explicit proof columns."""

    is_v3 = (F.col("partner") == "marketplace_it") & (
        F.col("payload_version") == "v3"
    )
    v3_amount = exact_eur_minor(
        F.col("rescued_data.grossAmount.units"),
        F.col("rescued_data.grossAmount.nanos"),
    )
    v3_currency = F.col("rescued_data.grossAmount.currencyCode")
    v3_customer = F.col("rescued_data.buyer.customerId")
    v3_mapping_valid = (
        is_v3
        & v3_amount.isNotNull()
        & (v3_currency == "EUR")
        & v3_customer.isNotNull()
    )

    amount = F.when(is_v3, v3_amount).otherwise(F.col("order_total_minor"))
    currency = F.when(is_v3, v3_currency).otherwise(F.col("currency"))
    customer = F.when(is_v3, v3_customer).otherwise(F.col("customer_id"))
    legacy_mapping_valid = (
        ~is_v3 & amount.isNotNull() & currency.isNotNull() & customer.isNotNull()
    )
    mapping_valid = v3_mapping_valid | legacy_mapping_valid
    event_shape_valid = (
        ((F.col("event_type") == "SALE") & (amount > 0))
        | ((F.col("event_type") == "REFUND") & (amount < 0))
    )
    canonical_valid = (
        mapping_valid
        & F.col("order_id").isNotNull()
        & F.col("source_sequence").isNotNull()
        & F.col("event_time").isNotNull()
        & event_shape_valid
    )

    return bronze.select(
        "transport_id",
        "partner",
        "order_id",
        "source_sequence",
        "payload_version",
        "event_type",
        "event_time",
        "ingested_at",
        customer.alias("customer_id"),
        amount.alias("order_total_minor"),
        currency.alias("currency"),
        mapping_valid.alias("mapping_valid"),
        canonical_valid.alias("canonical_valid"),
        F.when(v3_mapping_valid, F.lit("marketplace-v3-gross-eur-v2")).alias(
            "applied_profile_id"
        ),
        F.when(v3_mapping_valid, F.lit(profile_digest)).alias(
            "applied_profile_digest"
        ),
    )


def deterministic_winners(candidates: DataFrame) -> DataFrame:
    """Select one business event with a stable final transport tie-breaker."""

    order = Window.partitionBy("partner", "order_id", "source_sequence").orderBy(
        F.col("canonical_valid").desc(),
        (F.col("payload_version") == "v3").cast("int").desc(),
        F.col("ingested_at").desc(),
        F.col("transport_id").desc(),
    )
    return (
        candidates.withColumn("_winner_rank", F.row_number().over(order))
        .where(F.col("_winner_rank") == 1)
        .drop("_winner_rank")
    )


@dp.table(
    name="orders_repaired",
    comment="Bounded mixed-version candidate; promotion is a separate verified step.",
    table_properties={"pipelines.reset.allowed": "false"},
    # The recovery worker supplies a one-update predicate override. This static
    # predicate is intentionally narrow as a second line of defense.
    replace_where="partner = 'marketplace_it' AND event_time >= current_timestamp() - INTERVAL 1 DAY",
)
def orders_repaired() -> DataFrame:
    source_table = spark.conf.get("source.orders_table")
    profile_digest = spark.conf.get("recovery.profile_digest")
    bronze = spark.read.table(source_table)
    return deterministic_winners(canonical_candidates(bronze, profile_digest))
