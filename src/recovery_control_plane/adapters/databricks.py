"""Optional Databricks SDK adapter for the narrow pipeline worker port."""

from __future__ import annotations

from typing import Any

from ..policy import CompiledPlan


class DatabricksPipelineAdapter:
    """Create a real adapter only in an explicitly configured worker process."""

    def __init__(self, workspace_client: Any | None = None) -> None:
        if workspace_client is None:
            try:
                from databricks.sdk import WorkspaceClient
            except ImportError as exc:  # pragma: no cover - optional dependency
                raise RuntimeError(
                    "Databricks support is optional; install "
                    "self-healing-pipelines-2027[databricks]"
                ) from exc
            workspace_client = WorkspaceClient()
        self._workspace = workspace_client

    def start_bounded_update(
        self,
        *,
        plan: CompiledPlan,
        predicate: str,
        action_id: str,
    ) -> str:
        from databricks.sdk.service import pipelines

        response = self._workspace.pipelines.start_update(
            pipeline_id=plan.pipeline_id,
            cause=pipelines.StartUpdateCause.JOB_TASK,
            refresh_selection=list(plan.refresh_selection),
            parameters={"recovery_action_id": action_id},
            replace_where_overrides=[
                pipelines.ReplaceWhereOverride(
                    flow_name=flow_name,
                    predicate_override=predicate,
                )
                for flow_name in plan.refresh_selection
            ],
        )
        if not response.update_id:
            raise RuntimeError("Databricks returned no pipeline update ID")
        return response.update_id

    def find_updates(self, *, pipeline_id: str, action_id: str) -> list[str]:
        matches: list[str] = []
        page_token: str | None = None
        seen_tokens: set[str] = set()
        for _ in range(100):
            response = self._workspace.pipelines.list_updates(
                pipeline_id,
                max_results=100,
                page_token=page_token,
            )
            matches.extend(
                update.update_id
                for update in (response.updates or [])
                if update.update_id
                and (getattr(update, "parameters", None) or {}).get(
                    "recovery_action_id"
                )
                == action_id
            )
            next_token = getattr(response, "next_page_token", None)
            if not next_token:
                return matches
            if next_token in seen_tokens:
                raise RuntimeError("Databricks update pagination repeated a page token")
            seen_tokens.add(next_token)
            page_token = next_token
        raise RuntimeError("Databricks update reconciliation exceeded 100 pages")

    def get_update_state(self, *, pipeline_id: str, update_id: str) -> str:
        response = self._workspace.pipelines.get_update(pipeline_id, update_id)
        if response.update is None or response.update.state is None:
            raise RuntimeError("Databricks returned no pipeline update state")
        value = response.update.state
        return value.value if hasattr(value, "value") else str(value).split(".")[-1]
