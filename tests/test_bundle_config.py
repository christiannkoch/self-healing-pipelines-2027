import ast
from pathlib import Path

import yaml

from recovery_control_plane.profiles import (
    MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST,
)


ROOT = Path(__file__).resolve().parents[1]


def _spark_configuration_keys(source: str) -> set[str]:
    """Extract literal ``spark.conf.get`` keys from the transformation AST."""

    keys: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        function = node.func
        is_spark_conf_get = (
            isinstance(function, ast.Attribute)
            and function.attr == "get"
            and isinstance(function.value, ast.Attribute)
            and function.value.attr == "conf"
            and isinstance(function.value.value, ast.Name)
            and function.value.value.id == "spark"
        )
        if is_spark_conf_get and isinstance(node.args[0], ast.Constant):
            keys.add(node.args[0].value)
    return keys


def _module_constant(source: str, name: str) -> object:
    """Read one literal module constant without importing managed Spark code."""

    for node in ast.parse(source).body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id == name:
            return ast.literal_eval(node.value)
    raise AssertionError(f"missing module constant: {name}")


def test_bundle_supplies_every_configuration_key_read_by_pipeline() -> None:
    bundle = yaml.safe_load(
        (ROOT / "resources" / "orders.pipeline.yml").read_text(encoding="utf-8")
    )
    configuration = bundle["resources"]["pipelines"]["orders_recovery"][
        "configuration"
    ]
    source = (ROOT / "databricks" / "transformations" / "orders_pipeline.py").read_text(
        encoding="utf-8"
    )

    assert _spark_configuration_keys(source) <= set(configuration)
    assert configuration["recovery.source_delta_version"] == "${var.source_delta_version}"
    assert configuration["recovery.source_fingerprint"] == "${var.source_fingerprint}"
    assert configuration["recovery.normalizer_key"] == "MARKETPLACE_V3_GROSS"
    assert configuration["recovery.normalizer_version"] == "1.0.1"
    assert configuration["recovery.normalizer_source_digest"] == (
        MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST
    )
    assert _module_constant(source, "NORMALIZER_SOURCE_DIGEST") == (
        MARKETPLACE_V3_NORMALIZER_SOURCE_DIGEST
    )


def test_bundle_uses_current_schema_field_and_bundle_deployment_kind() -> None:
    bundle = yaml.safe_load(
        (ROOT / "resources" / "orders.pipeline.yml").read_text(encoding="utf-8")
    )
    pipeline = bundle["resources"]["pipelines"]["orders_recovery"]

    assert "schema" in pipeline
    assert "target" not in pipeline
    assert pipeline["deployment"]["kind"] == "BUNDLE"
    assert pipeline["channel"] == "CURRENT"
