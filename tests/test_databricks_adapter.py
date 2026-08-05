from types import SimpleNamespace

from recovery_control_plane.adapters.databricks import DatabricksPipelineAdapter


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
                        parameters={"recovery_action_id": "other"},
                    )
                ],
                next_page_token="page-2",
            )
        return SimpleNamespace(
            updates=[
                SimpleNamespace(
                    update_id="update-on-page-two",
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
