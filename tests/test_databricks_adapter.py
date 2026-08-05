import inspect
from importlib.metadata import PackageNotFoundError, version
from types import SimpleNamespace

import pytest

from recovery_control_plane.adapters.databricks import DatabricksPipelineAdapter
from recovery_control_plane.policy import compile_intent

from conftest import BASE_TIME


PINNED_SDK_VERSION = "0.124.0"


def _current_sdk():
    try:
        installed = version("databricks-sdk")
    except PackageNotFoundError:
        pytest.skip("Databricks optional dependency is not installed")
    if installed != PINNED_SDK_VERSION:
        pytest.skip(
            f"adapter contract is pinned to databricks-sdk {PINNED_SDK_VERSION}; "
            f"found {installed}"
        )
    from databricks.sdk.service import pipelines

    return pipelines


class FakePipelines:
    def __init__(self) -> None:
        self.tokens = []

    def list_updates(self, pipeline_id, *, max_results, page_token):
        assert pipeline_id == "pipeline-orders"
        assert max_results == 100
        self.tokens.append(page_token)
        if page_token is None:
            return SimpleNamespace(
                updates=[
                    SimpleNamespace(
                        update_id="unrelated",
                        pipeline_id="pipeline-orders",
                        parameters={"recovery_action_id": "other"},
                    )
                ],
                next_page_token="page-2",
            )
        return SimpleNamespace(
            updates=[
                SimpleNamespace(
                    update_id="update-on-page-two",
                    pipeline_id="pipeline-orders",
                    parameters={"recovery_action_id": "rpr-target"},
                )
            ],
            next_page_token=None,
        )


def test_reconciliation_searches_every_update_page() -> None:
    pipelines = FakePipelines()
    workspace = SimpleNamespace(pipelines=pipelines)
    adapter = DatabricksPipelineAdapter(workspace)

    matches = adapter.find_updates(
        pipeline_id="pipeline-orders",
        action_id="rpr-target",
    )

    assert matches == ["update-on-page-two"]
    assert pipelines.tokens == [None, "page-2"]


def test_pinned_sdk_exposes_every_typed_pipeline_field_used_by_adapter() -> None:
    pipelines = _current_sdk()
    parameters = inspect.signature(pipelines.PipelinesAPI.start_update).parameters

    assert {"parameters", "refresh_selection", "replace_where_overrides"}.issubset(
        parameters
    )
    update = pipelines.UpdateInfo(
        pipeline_id="pipeline-orders",
        update_id="update-1",
        parameters={"recovery_action_id": "rpr-target"},
    )
    assert update.parameters == {"recovery_action_id": "rpr-target"}


class AttestedPipelines:
    def __init__(self, response, start_response) -> None:
        self.response = response
        self.start_response = start_response
        self.start_calls = []

    def get(self, pipeline_id):
        assert pipeline_id == "pipeline-orders"
        return self.response

    def start_update(self, pipeline_id, **kwargs):
        assert pipeline_id == "pipeline-orders"
        self.start_calls.append(kwargs)
        return self.start_response


def _pipeline_response(
    pipelines,
    plan,
    *,
    digest=None,
    version_id="abc123",
    source_table=None,
    source_delta_version=None,
    source_fingerprint=None,
    channel="CURRENT",
):
    return pipelines.GetPipelineResponse(
        pipeline_id=plan.pipeline_id,
        spec=pipelines.PipelineSpec(
            channel=channel,
            serverless=True,
            configuration={
                "recovery.profile_digest": digest or plan.profile_digest,
                "recovery.source_fingerprint": (
                    source_fingerprint or plan.source_fingerprint
                ),
                "source.orders_table": source_table or plan.source_table,
                "recovery.source_delta_version": (
                    source_delta_version or str(plan.source_delta_version)
                ),
            },
            deployment=pipelines.PipelineDeployment(
                kind=pipelines.DeploymentKind.BUNDLE,
                deployment_id="orders",
                version_id=version_id,
            ),
        ),
    )


def test_start_attests_deployment_and_uses_supported_reconciliation_metadata(
    evidence,
    repair_intent,
    policy,
    authority,
) -> None:
    pipelines = _current_sdk()
    plan = compile_intent(
        evidence,
        repair_intent,
        policy,
        now=BASE_TIME,
        authority_signer=authority,
    )
    api = AttestedPipelines(
        _pipeline_response(pipelines, plan),
        pipelines.StartUpdateResponse(update_id="update-accepted"),
    )
    adapter = DatabricksPipelineAdapter(SimpleNamespace(pipelines=api))

    update_id = adapter.start_bounded_update(
        plan=plan,
        predicate="partner = 'marketplace_it'",
        action_id=plan.action_id,
    )

    assert update_id == "update-accepted"
    assert len(api.start_calls) == 1
    call = api.start_calls[0]
    assert call["cause"] is pipelines.StartUpdateCause.API_CALL
    assert call["parameters"] == {"recovery_action_id": plan.action_id}
    assert call["refresh_selection"] == list(plan.refresh_selection)
    assert call["replace_where_overrides"][0].flow_name == plan.refresh_selection[0]


@pytest.mark.parametrize(
    ("response_change", "message"),
    [
        ({"version_id": "different"}, "deployment snapshot"),
        ({"digest": "sha256:different"}, "profile digest"),
        ({"source_table": "other.bronze"}, "source table"),
        ({"source_delta_version": "999"}, "source Delta version"),
        ({"source_fingerprint": "sha256:other-source"}, "source fingerprint"),
        ({"channel": "PREVIEW"}, "CURRENT channel"),
    ],
)
def test_start_fails_before_mutation_when_pipeline_attestation_differs(
    evidence,
    repair_intent,
    policy,
    response_change,
    message,
    authority,
) -> None:
    pipelines = _current_sdk()
    plan = compile_intent(
        evidence,
        repair_intent,
        policy,
        now=BASE_TIME,
        authority_signer=authority,
    )
    api = AttestedPipelines(
        _pipeline_response(pipelines, plan, **response_change),
        pipelines.StartUpdateResponse(update_id="must-not-start"),
    )
    adapter = DatabricksPipelineAdapter(SimpleNamespace(pipelines=api))

    with pytest.raises(RuntimeError, match=message):
        adapter.start_bounded_update(
            plan=plan,
            predicate="partner = 'marketplace_it'",
            action_id=plan.action_id,
        )

    assert api.start_calls == []
