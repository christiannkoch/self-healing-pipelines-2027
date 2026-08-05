"""Executable Python/Spark golden tests for the production transformation."""

from __future__ import annotations

from datetime import timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest

from recovery_control_plane.normalization import (
    BronzeOrder,
    normalize_and_dedupe,
    normalize_order,
)
from recovery_control_plane.verifier import SettlementRecord, calculate_metrics

from conftest import BASE_TIME


pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def spark():
    worker_python = sys.executable
    previous_spark_home = os.environ.pop("SPARK_HOME", None)
    previous_worker = os.environ.get("PYSPARK_PYTHON")
    previous_driver = os.environ.get("PYSPARK_DRIVER_PYTHON")
    os.environ["PYSPARK_PYTHON"] = worker_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = worker_python
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("recovery-normalization-parity")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .getOrCreate()
        )
    except Exception as exc:
        if previous_spark_home is not None:
            os.environ["SPARK_HOME"] = previous_spark_home
        pytest.skip(f"compatible local Spark runtime unavailable: {exc}")
    yield session
    session.stop()
    if previous_spark_home is not None:
        os.environ["SPARK_HOME"] = previous_spark_home
    if previous_worker is None:
        os.environ.pop("PYSPARK_PYTHON", None)
    else:
        os.environ["PYSPARK_PYTHON"] = previous_worker
    if previous_driver is None:
        os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
    else:
        os.environ["PYSPARK_DRIVER_PYTHON"] = previous_driver


@pytest.fixture(scope="module")
def spark_pipeline_module():
    # Local PySpark exposes the pipelines module but not Databricks' current
    # ``replace_where`` decorator argument. Replace registration only; all
    # transformation expressions execute against a real Spark session.
    from pyspark import pipelines as dp

    original_table = dp.table
    dp.table = lambda **_kwargs: lambda function: function
    try:
        path = ROOT / "databricks" / "transformations" / "orders_pipeline.py"
        spec = importlib.util.spec_from_file_location("orders_pipeline_test", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module
    finally:
        dp.table = original_table


BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("transport_id", T.StringType(), False),
        T.StructField("partner", T.StringType(), False),
        T.StructField("payload_version", T.StringType(), False),
        T.StructField("order_id", T.StringType(), True),
        T.StructField("source_sequence", T.LongType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("event_time", T.TimestampType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("order_total_minor", T.LongType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("_rescued_data", T.StringType(), True),
    ]
)

CANDIDATE_SCHEMA = T.StructType(
    [
        T.StructField("transport_id", T.StringType(), False),
        T.StructField("partner", T.StringType(), False),
        T.StructField("order_id", T.StringType(), True),
        T.StructField("source_sequence", T.LongType(), True),
        T.StructField("payload_version", T.StringType(), False),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("event_time", T.TimestampType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("order_total_minor", T.LongType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("mapping_valid", T.BooleanType(), True),
        T.StructField("canonical_valid", T.BooleanType(), True),
        T.StructField("payload_version_authorized", T.BooleanType(), True),
        T.StructField("applied_profile_id", T.StringType(), True),
        T.StructField("applied_profile_digest", T.StringType(), True),
        T.StructField("dedupe_ambiguous", T.BooleanType(), True),
    ]
)


def spark_row(
    *,
    transport_id: str,
    version: str,
    rescued: object | None = None,
    sequence: int = 1,
    customer: str | None = None,
    amount: int | None = None,
    currency: str | None = None,
):
    return (
        transport_id,
        "marketplace_it",
        version,
        "order-100",
        sequence,
        "SALE",
        BASE_TIME.astimezone(timezone.utc),
        (BASE_TIME + timedelta(seconds=1)).astimezone(timezone.utc),
        customer,
        amount,
        currency,
        json.dumps(rescued) if rescued is not None else None,
    )


def spark_candidate_payload(candidate) -> dict[str, object]:
    """Normalize aware instants to UTC without discarding timezone information."""

    payload = candidate.model_dump()
    for field in ("event_time", "ingested_at"):
        value = payload[field]
        if value is not None:
            payload[field] = value.astimezone(timezone.utc)
    return payload


@pytest.mark.parametrize(
    ("units", "nanos"),
    [("1", "0"), (1.5, 0), (True, 0), (1, 1)],
)
def test_spark_never_rounds_or_coerces_money(
    spark,
    spark_pipeline_module,
    mapping_profile,
    units,
    nanos,
) -> None:
    rescued = {
        "buyer": {"customerId": "redacted"},
        "grossAmount": {
            "units": units,
            "nanos": nanos,
            "currencyCode": "EUR",
        },
    }
    frame = spark.createDataFrame(
        [spark_row(transport_id="delivery", version="v3", rescued=rescued)],
        BRONZE_SCHEMA,
    )

    result = spark_pipeline_module.canonical_candidates(
        frame,
        mapping_profile.profile_digest,
        source_fingerprint=mapping_profile.source_fingerprint,
    ).first()

    assert result.order_total_minor is None
    assert result.mapping_valid is False
    assert result.canonical_valid is False


def test_spark_and_python_share_the_valid_v3_golden_result(
    spark,
    spark_pipeline_module,
    mapping_profile,
) -> None:
    rescued = {
        "buyer": {"customerId": "redacted"},
        "grossAmount": {
            "units": 12,
            "nanos": 500_000_000,
            "currencyCode": "EUR",
        },
    }
    frame = spark.createDataFrame(
        [spark_row(transport_id="delivery", version="v3", rescued=rescued)],
        BRONZE_SCHEMA,
    )
    spark_result = spark_pipeline_module.canonical_candidates(
        frame,
        mapping_profile.profile_digest,
        source_fingerprint=mapping_profile.source_fingerprint,
    ).first()
    python_result = normalize_order(
        BronzeOrder(
            transport_id="delivery",
            partner="marketplace_it",
            payload_version="v3",
            order_id="order-100",
            source_sequence=1,
            event_type="SALE",
            event_time=BASE_TIME,
            ingested_at=BASE_TIME + timedelta(seconds=1),
            rescued_data=rescued,
        ),
        mapping_profile,
    )

    assert spark_result.order_total_minor == python_result.order_total_minor == 1_250
    assert spark_result.customer_id == python_result.customer_id == "redacted"
    assert spark_result.mapping_valid == python_result.mapping_valid is True
    assert spark_result.canonical_valid == python_result.canonical_valid is True


@pytest.mark.parametrize("customer_value", [None, 123])
def test_spark_and_python_keep_valid_money_when_customer_shape_is_invalid(
    spark,
    spark_pipeline_module,
    mapping_profile,
    customer_value,
) -> None:
    buyer = {} if customer_value is None else {"customerId": customer_value}
    rescued = {
        "buyer": buyer,
        "grossAmount": {
            "units": 12,
            "nanos": 500_000_000,
            "currencyCode": "EUR",
        },
    }
    frame = spark.createDataFrame(
        [spark_row(transport_id="delivery", version="v3", rescued=rescued)],
        BRONZE_SCHEMA,
    )
    spark_result = spark_pipeline_module.canonical_candidates(
        frame,
        mapping_profile.profile_digest,
        source_fingerprint=mapping_profile.source_fingerprint,
    ).first()
    python_result = normalize_order(
        BronzeOrder(
            transport_id="delivery",
            partner="marketplace_it",
            payload_version="v3",
            order_id="order-100",
            source_sequence=1,
            event_type="SALE",
            event_time=BASE_TIME,
            ingested_at=BASE_TIME + timedelta(seconds=1),
            rescued_data=rescued,
        ),
        mapping_profile,
    )

    assert spark_result.order_total_minor == python_result.order_total_minor == 1_250
    assert spark_result.mapping_valid == python_result.mapping_valid is False
    assert spark_result.canonical_valid == python_result.canonical_valid is False


@pytest.mark.parametrize(
    "changed_configuration",
    [
        {"source_fingerprint": "sha256:changed-source"},
        {"dedupe_key": ("transport_id",)},
    ],
)
def test_spark_recomputes_digest_from_the_deployed_profile(
    spark,
    spark_pipeline_module,
    mapping_profile,
    changed_configuration,
) -> None:
    frame = spark.createDataFrame(
        [
            spark_row(
                transport_id="delivery",
                version="v2",
                customer="redacted",
                amount=100,
                currency="EUR",
            )
        ],
        BRONZE_SCHEMA,
    )
    configuration = {
        "source_fingerprint": mapping_profile.source_fingerprint,
        **changed_configuration,
    }

    with pytest.raises(ValueError, match="profile digest does not match"):
        spark_pipeline_module.canonical_candidates(
            frame,
            mapping_profile.profile_digest,
            **configuration,
        )


@pytest.mark.parametrize(
    ("version", "customer"),
    [("v4-unknown", "redacted"), ("v2", ""), ("v2", "   ")],
)
def test_spark_unknown_version_and_empty_customer_fail_closed(
    spark,
    spark_pipeline_module,
    mapping_profile,
    version,
    customer,
) -> None:
    frame = spark.createDataFrame(
        [
            spark_row(
                transport_id="delivery",
                version=version,
                customer=customer,
                amount=100,
                currency="EUR",
            )
        ],
        BRONZE_SCHEMA,
    )

    result = spark_pipeline_module.canonical_candidates(
        frame,
        mapping_profile.profile_digest,
        source_fingerprint=mapping_profile.source_fingerprint,
    ).first()

    assert result.mapping_valid is False
    assert result.canonical_valid is False
    assert result.payload_version_authorized is (version == "v2")


def test_spark_conflicting_total_tie_is_deterministic_and_invalid(
    spark,
    spark_pipeline_module,
    mapping_profile,
) -> None:
    def raw(units: int):
        return spark_row(
            transport_id="same-delivery",
            version="v3",
            rescued={
                "buyer": {"customerId": "redacted"},
                "grossAmount": {
                    "units": units,
                    "nanos": 0,
                    "currencyCode": "EUR",
                },
            },
        )

    outcomes = []
    for rows in ([raw(1), raw(2)], [raw(2), raw(1)]):
        frame = spark.createDataFrame(rows, BRONZE_SCHEMA)
        candidates = spark_pipeline_module.canonical_candidates(
            frame,
            mapping_profile.profile_digest,
            source_fingerprint=mapping_profile.source_fingerprint,
        )
        outcomes.append(
            spark_pipeline_module.deterministic_winners(candidates).first()
        )

    assert outcomes[0].order_total_minor == outcomes[1].order_total_minor
    assert outcomes[0].dedupe_ambiguous is True
    assert outcomes[0].canonical_valid is False


@pytest.mark.parametrize(
    "settlement_payloads",
    [
        [{"currency": "EUR", "settled_amount_minor": 1_250}],
        [
            {"currency": "EUR", "settled_amount_minor": 1_250},
            {
                "order_id": "unexpected",
                "source_sequence": 99,
                "currency": "EUR",
                "settled_amount_minor": 10,
            },
            {"currency": "USD", "settled_amount_minor": 1},
            {"currency": None, "settled_amount_minor": 1_250},
        ],
    ],
    ids=["valid", "combined-settlement-adversaries"],
)
def test_checked_in_sql_matches_python_verifier_truth_table(
    spark,
    mapping_profile,
    settlement_payloads,
) -> None:
    source = BronzeOrder(
        transport_id="delivery",
        partner="marketplace_it",
        payload_version="v3",
        order_id="order-100",
        source_sequence=1,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME + timedelta(seconds=1),
        rescued_data={
            "buyer": {"customerId": "redacted"},
            "grossAmount": {
                "units": 12,
                "nanos": 500_000_000,
                "currencyCode": "EUR",
            },
        },
    )
    candidates = normalize_and_dedupe([source], mapping_profile)
    settlements = [
        SettlementRecord(
            partner="marketplace_it",
            order_id=item.get("order_id", "order-100"),
            source_sequence=item.get("source_sequence", 1),
            currency=item["currency"],
            settled_amount_minor=item["settled_amount_minor"],
        )
        for item in settlement_payloads
    ]
    expected_key = ("marketplace_it", "order-100", 1)
    python_metrics = calculate_metrics(
        expected_keys={expected_key},
        expected_v3_keys={expected_key},
        source_rows=[source],
        candidates=candidates,
        settlements=settlements,
        required_profile_digest=mapping_profile.profile_digest,
        required_currency_code=mapping_profile.currency_code,
        backfill_start=BASE_TIME - timedelta(days=1),
        backfill_end=BASE_TIME + timedelta(days=1),
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )

    spark.createDataFrame(
        [
            (
                source.transport_id,
                source.partner,
                source.payload_version,
                source.order_id,
                source.source_sequence,
                source.event_type,
                source.event_time.astimezone(timezone.utc),
                source.customer_id,
                source.currency,
                json.dumps(source.rescued_data),
            )
        ],
        "transport_id string, partner string, payload_version string, "
        "order_id string, source_sequence long, event_type string, "
        "event_time timestamp, customer_id string, currency string, "
        "_rescued_data string",
    ).createOrReplaceTempView("verification_bronze")
    spark.createDataFrame(
        [spark_candidate_payload(candidate) for candidate in candidates]
    ).createOrReplaceTempView("verification_candidate")
    settlement_schema = (
        "partner string, order_id string, source_sequence long, "
        "currency string, settled_amount_minor long"
    )
    spark.createDataFrame(
        [settlement.model_dump() for settlement in settlements],
        settlement_schema,
    ).createOrReplaceTempView("verification_settlement")

    sql = (ROOT / "databricks" / "sql" / "verification.sql").read_text(
        encoding="utf-8"
    )
    replacements = {
        "IDENTIFIER(:bronze_table) VERSION AS OF :source_delta_version": "verification_bronze",
        "IDENTIFIER(:candidate_table) VERSION AS OF :candidate_delta_version": "verification_candidate",
        "IDENTIFIER(:settlement_relation) VERSION AS OF :settlement_delta_version": "verification_settlement",
        ":required_profile_digest": f"'{mapping_profile.profile_digest}'",
        ":required_profile_version": f"'{mapping_profile.payload_version}'",
        ":required_profile_id": f"'{mapping_profile.profile_id}'",
        ":required_currency_code": f"'{mapping_profile.currency_code}'",
        ":partner": "'marketplace_it'",
        # Use a timezone-robust local test interval. Production binds the exact
        # UTC half-open window carried by the evidence envelope.
        ":backfill_start": "TIMESTAMP '2027-11-25 00:00:00'",
        ":backfill_end": "TIMESTAMP '2027-11-27 00:00:00'",
    }
    for marker, value in replacements.items():
        sql = sql.replace(marker, value)

    sql_metrics = spark.sql(sql).first().asDict()

    assert sql_metrics == python_metrics.model_dump()


SQL_SEMANTIC_FAILURE_CASES = (
    ("mapping-false", {"mapping_valid": False}, "mapping_validation_failures"),
    ("mapping-null", {"mapping_valid": None}, "mapping_validation_failures"),
    (
        "authorization-false",
        {"payload_version_authorized": False},
        "payload_authorization_failures",
    ),
    (
        "authorization-null",
        {"payload_version_authorized": None},
        "payload_authorization_failures",
    ),
    ("order-id-null", {"order_id": None}, "missing_order_id_rows"),
    ("order-id-empty", {"order_id": ""}, "missing_order_id_rows"),
    ("order-id-blank", {"order_id": "   "}, "missing_order_id_rows"),
    ("customer-id-null", {"customer_id": None}, "missing_customer_id_rows"),
    ("customer-id-empty", {"customer_id": ""}, "missing_customer_id_rows"),
    ("customer-id-blank", {"customer_id": "   "}, "missing_customer_id_rows"),
    ("event-time-null", {"event_time": None}, "invalid_event_time_rows"),
    (
        "event-time-before-window",
        {"event_time": BASE_TIME - timedelta(minutes=2)},
        "invalid_event_time_rows",
    ),
    (
        "event-time-at-exclusive-end",
        {"event_time": BASE_TIME + timedelta(minutes=1)},
        "invalid_event_time_rows",
    ),
    ("event-type-null", {"event_type": None}, "unsupported_event_type_rows"),
    (
        "event-type-unsupported",
        {"event_type": "CHARGEBACK"},
        "unsupported_event_type_rows",
    ),
    (
        "sale-zero",
        {"event_type": "SALE", "order_total_minor": 0},
        "invalid_sale_amount_rows",
    ),
    (
        "sale-negative",
        {"event_type": "SALE", "order_total_minor": -1},
        "invalid_sale_amount_rows",
    ),
    (
        "refund-zero",
        {"event_type": "REFUND", "order_total_minor": 0},
        "invalid_refund_amount_rows",
    ),
    (
        "refund-positive",
        {"event_type": "REFUND", "order_total_minor": 1},
        "invalid_refund_amount_rows",
    ),
    (
        "candidate-profile-currency",
        {"currency": "USD"},
        "candidate_profile_currency_mismatches",
    ),
    (
        "source-event-time-drift",
        {"event_time": BASE_TIME + timedelta(seconds=30)},
        "source_candidate_event_time_mismatches",
    ),
    (
        "source-event-type-drift",
        {"event_type": "REFUND"},
        "source_candidate_event_type_mismatches",
    ),
    (
        "source-customer-drift",
        {"customer_id": "different-customer"},
        "source_candidate_customer_id_mismatches",
    ),
)


def _execute_checked_in_verification_sql(
    spark,
    mapping_profile,
    *,
    source,
    candidates,
    settlements,
    backfill_start,
    backfill_end,
):
    source_rows = source if isinstance(source, (list, tuple)) else [source]
    spark.createDataFrame(
        [
            (
                row.transport_id,
                row.partner,
                row.payload_version,
                row.order_id,
                row.source_sequence,
                row.event_type,
                (
                    row.event_time.astimezone(timezone.utc)
                    if row.event_time is not None
                    else None
                ),
                row.customer_id,
                row.currency,
                json.dumps(row.rescued_data),
            )
            for row in source_rows
        ],
        "transport_id string, partner string, payload_version string, "
        "order_id string, source_sequence long, event_type string, "
        "event_time timestamp, customer_id string, currency string, "
        "_rescued_data string",
    ).createOrReplaceTempView("semantic_verification_bronze")
    spark.createDataFrame(
        [spark_candidate_payload(candidate) for candidate in candidates],
        CANDIDATE_SCHEMA,
    ).createOrReplaceTempView("semantic_verification_candidate")
    spark.createDataFrame(
        [settlement.model_dump() for settlement in settlements],
        "partner string, order_id string, source_sequence long, "
        "currency string, settled_amount_minor long",
    ).createOrReplaceTempView("semantic_verification_settlement")

    sql = (ROOT / "databricks" / "sql" / "verification.sql").read_text(
        encoding="utf-8"
    )
    replacements = {
        "IDENTIFIER(:bronze_table) VERSION AS OF :source_delta_version": (
            "semantic_verification_bronze"
        ),
        "IDENTIFIER(:candidate_table) VERSION AS OF :candidate_delta_version": (
            "semantic_verification_candidate"
        ),
        "IDENTIFIER(:settlement_relation) VERSION AS OF :settlement_delta_version": (
            "semantic_verification_settlement"
        ),
        ":required_profile_digest": f"'{mapping_profile.profile_digest}'",
        ":required_profile_version": f"'{mapping_profile.payload_version}'",
        ":required_profile_id": f"'{mapping_profile.profile_id}'",
        ":required_currency_code": f"'{mapping_profile.currency_code}'",
        ":partner": "'marketplace_it'",
        ":backfill_start": (
            f"TIMESTAMP '{backfill_start.strftime('%Y-%m-%d %H:%M:%S')}'"
        ),
        ":backfill_end": f"TIMESTAMP '{backfill_end.strftime('%Y-%m-%d %H:%M:%S')}'",
    }
    for marker, value in replacements.items():
        sql = sql.replace(marker, value)
    return spark.sql(sql).first().asDict()


def test_checked_in_sql_matches_python_combined_semantic_failures(
    spark,
    mapping_profile,
) -> None:
    sources = []
    candidates = []
    settlements = []
    for index, (_case, updates, _failure_metric) in enumerate(
        SQL_SEMANTIC_FAILURE_CASES,
        start=1,
    ):
        source = BronzeOrder(
            transport_id=f"semantic-delivery-{index}",
            partner="marketplace_it",
            payload_version="v3",
            order_id=f"order-semantic-{index}",
            source_sequence=index,
            event_type="SALE",
            event_time=BASE_TIME,
            ingested_at=BASE_TIME + timedelta(seconds=1),
            rescued_data={
                "buyer": {"customerId": "redacted"},
                "grossAmount": {
                    "units": 12,
                    "nanos": 500_000_000,
                    "currencyCode": "EUR",
                },
            },
        )
        candidate = normalize_and_dedupe([source], mapping_profile)[0].model_copy(
            update=updates
        )
        sources.append(source)
        candidates.append(candidate)
        settlements.append(
            SettlementRecord(
                partner=source.partner,
                order_id=source.order_id,
                source_sequence=source.source_sequence,
                currency="EUR",
                settled_amount_minor=1_250,
            )
        )

    expected_keys = {
        (source.partner, source.order_id, source.source_sequence)
        for source in sources
    }
    backfill_start = BASE_TIME - timedelta(minutes=1)
    backfill_end = BASE_TIME + timedelta(minutes=1)
    python_metrics = calculate_metrics(
        expected_keys=expected_keys,
        expected_v3_keys=expected_keys,
        source_rows=sources,
        candidates=candidates,
        settlements=settlements,
        required_profile_digest=mapping_profile.profile_digest,
        required_currency_code=mapping_profile.currency_code,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )
    sql_metrics = _execute_checked_in_verification_sql(
        spark,
        mapping_profile,
        source=sources,
        candidates=candidates,
        settlements=settlements,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
    )

    assert sql_metrics == python_metrics.model_dump()
    for failure_metric in {case[2] for case in SQL_SEMANTIC_FAILURE_CASES}:
        assert sql_metrics[failure_metric] > 0


def test_checked_in_sql_ignores_unrelated_same_partner_history(
    spark,
    mapping_profile,
) -> None:
    source = BronzeOrder(
        transport_id="current-delivery",
        partner="marketplace_it",
        payload_version="v3",
        order_id="order-current",
        source_sequence=22,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME + timedelta(seconds=1),
        rescued_data={
            "buyer": {"customerId": "redacted"},
            "grossAmount": {
                "units": 12,
                "nanos": 500_000_000,
                "currencyCode": "EUR",
            },
        },
    )
    current = normalize_and_dedupe([source], mapping_profile)[0]
    history = current.model_copy(
        update={
            "transport_id": "historical-delivery",
            "order_id": "order-historical",
            "source_sequence": 999,
            "event_time": BASE_TIME - timedelta(days=10),
        }
    )
    key = (source.partner, source.order_id, source.source_sequence)
    assert key[1] is not None and key[2] is not None
    settlements = [
        SettlementRecord(
            partner=key[0],
            order_id=key[1],
            source_sequence=key[2],
            currency="EUR",
            settled_amount_minor=1_250,
        )
    ]
    backfill_start = BASE_TIME - timedelta(minutes=1)
    backfill_end = BASE_TIME + timedelta(minutes=1)
    expected_metrics = calculate_metrics(
        expected_keys={key},
        expected_v3_keys={key},
        source_rows=[source],
        candidates=[current],
        settlements=settlements,
        required_profile_digest=mapping_profile.profile_digest,
        required_currency_code=mapping_profile.currency_code,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )
    sql_metrics = _execute_checked_in_verification_sql(
        spark,
        mapping_profile,
        source=source,
        candidates=[current, history],
        settlements=settlements,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
    )

    assert sql_metrics == expected_metrics.model_dump()


def test_checked_in_sql_matches_python_combined_source_scope_failures(
    spark,
    mapping_profile,
) -> None:
    source = BronzeOrder(
        transport_id="source-scope-valid",
        partner="marketplace_it",
        payload_version="v3",
        order_id="order-source-scope",
        source_sequence=23,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME + timedelta(seconds=1),
        rescued_data={
            "buyer": {"customerId": "redacted"},
            "grossAmount": {
                "units": 12,
                "nanos": 500_000_000,
                "currencyCode": "EUR",
            },
        },
    )
    source_failure_updates = (
        {"order_id": None},
        {"order_id": "   "},
        {"source_sequence": None},
        {"payload_version": "v99-unsupported"},
    )
    malformed_sources = [
        source.model_copy(
            update={
                "transport_id": f"source-scope-malformed-{index}",
                **updates,
            }
        )
        for index, updates in enumerate(source_failure_updates, start=1)
    ]
    source_rows = [source, *malformed_sources]
    current = normalize_and_dedupe([source], mapping_profile)[0]
    expected_keys = {
        (row.partner, row.order_id, row.source_sequence)
        for row in source_rows
        if row.order_id is not None and row.source_sequence is not None
    }
    expected_v3_keys = {
        (row.partner, row.order_id, row.source_sequence)
        for row in source_rows
        if row.payload_version == mapping_profile.payload_version
        and row.order_id is not None
        and row.source_sequence is not None
    }
    key = (source.partner, source.order_id, source.source_sequence)
    assert key[1] is not None and key[2] is not None
    settlements = [
        SettlementRecord(
            partner=key[0],
            order_id=key[1],
            source_sequence=key[2],
            currency="EUR",
            settled_amount_minor=1_250,
        )
    ]
    backfill_start = BASE_TIME - timedelta(minutes=1)
    backfill_end = BASE_TIME + timedelta(minutes=1)
    python_metrics = calculate_metrics(
        expected_keys=expected_keys,
        expected_v3_keys=expected_v3_keys,
        source_rows=source_rows,
        candidates=[current],
        settlements=settlements,
        required_profile_digest=mapping_profile.profile_digest,
        required_currency_code=mapping_profile.currency_code,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )
    sql_metrics = _execute_checked_in_verification_sql(
        spark,
        mapping_profile,
        source=source_rows,
        candidates=[current],
        settlements=settlements,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
    )

    assert sql_metrics == python_metrics.model_dump()
    assert sql_metrics["source_invalid_business_key_rows"] == 3
    assert sql_metrics["source_unauthorized_payload_versions"] == 1


def test_checked_in_sql_rejects_self_consistent_wrong_profile_currency(
    spark,
    mapping_profile,
) -> None:
    valid_source = BronzeOrder(
        transport_id="wrong-currency-delivery",
        partner="marketplace_it",
        payload_version="v3",
        order_id="order-wrong-currency",
        source_sequence=24,
        event_type="SALE",
        event_time=BASE_TIME,
        ingested_at=BASE_TIME + timedelta(seconds=1),
        rescued_data={
            "buyer": {"customerId": "redacted"},
            "grossAmount": {
                "units": 12,
                "nanos": 500_000_000,
                "currencyCode": "EUR",
            },
        },
    )
    source = valid_source.model_copy(
        update={
            "rescued_data": {
                "buyer": {"customerId": "redacted"},
                "grossAmount": {
                    "units": 12,
                    "nanos": 500_000_000,
                    "currencyCode": "USD",
                },
            }
        }
    )
    candidate = normalize_and_dedupe([valid_source], mapping_profile)[0].model_copy(
        update={"currency": "USD"}
    )
    key = (source.partner, source.order_id, source.source_sequence)
    assert key[1] is not None and key[2] is not None
    settlements = [
        SettlementRecord(
            partner=key[0],
            order_id=key[1],
            source_sequence=key[2],
            currency="USD",
            settled_amount_minor=1_250,
        )
    ]
    backfill_start = BASE_TIME - timedelta(minutes=1)
    backfill_end = BASE_TIME + timedelta(minutes=1)
    python_metrics = calculate_metrics(
        expected_keys={key},
        expected_v3_keys={key},
        source_rows=[source],
        candidates=[candidate],
        settlements=settlements,
        required_profile_digest=mapping_profile.profile_digest,
        required_currency_code=mapping_profile.currency_code,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
        required_profile_version=mapping_profile.payload_version,
        required_profile_id=mapping_profile.profile_id,
        authorized_legacy_versions=mapping_profile.legacy_payload_versions,
    )
    sql_metrics = _execute_checked_in_verification_sql(
        spark,
        mapping_profile,
        source=source,
        candidates=[candidate],
        settlements=settlements,
        backfill_start=backfill_start,
        backfill_end=backfill_end,
    )

    assert sql_metrics == python_metrics.model_dump()
    assert sql_metrics["source_profile_currency_mismatches"] == 1
    assert sql_metrics["candidate_profile_currency_mismatches"] == 1
