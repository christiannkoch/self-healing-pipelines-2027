"""Optional Databricks SDK adapter for the narrow pipeline worker port.

The adapter deliberately uses the Pipelines API directly. It does not pretend
that a Lakeflow Job exists, and it treats update parameters as reconciliation
metadata rather than as a server-side idempotency guarantee.
"""

from __future__ import annotations

from typing import Any

from ..policy import CompiledPlan


RECOVERY_ACTION_PARAMETER = "recovery_action_id"
PROFILE_DIGEST_CONFIGURATION = "recovery.profile_digest"
SOURCE_FINGERPRINT_CONFIGURATION = "recovery.source_fingerprint"
SOURCE_TABLE_CONFIGURATION = "source.orders_table"
SOURCE_DELTA_VERSION_CONFIGURATION = "recovery.source_delta_version"
REQUIRED_PIPELINE_CHANNEL = "CURRENT"


def _enum_value(value: Any) -> str:
    """Return an SDK enum value without depending on its display format."""

    return str(value.value if hasattr(value, "value") else value)


def _parse_expected_bundle_deployment(value: str) -> tuple[str, str]:
    """Parse the registry form ``bundle:<deployment-id>@<version-id>``."""

    prefix = "bundle:"
    if not value.startswith(prefix):
        raise RuntimeError(
            "compiled plan pipeline_deployment must use "
            "bundle:<deployment-id>@<version-id>"
        )
    deployment_id, separator, version_id = value[len(prefix) :].rpartition("@")
    if (
        not separator
        or not deployment_id
        or not version_id
        or deployment_id != deployment_id.strip()
        or version_id != version_id.strip()
    ):
        raise RuntimeError(
            "compiled plan pipeline_deployment must use "
            "bundle:<deployment-id>@<version-id>"
        )
    return deployment_id, version_id


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

    def _attest_pipeline(self, plan: CompiledPlan) -> None:
        """Fail before mutation unless the deployed pipeline matches the plan.

        The pipeline GET response is the strongest identity available on the
        direct Pipelines API: it names the resource, bundle deployment snapshot,
        and immutable profile digest configured on that snapshot. Databricks
        does not offer an atomic compare-and-start precondition, so production
        deployment controls must also prevent a bundle rollout racing this call.
        """

        expected_deployment_id, expected_version_id = _parse_expected_bundle_deployment(
            plan.pipeline_deployment
        )
        response = self._workspace.pipelines.get(plan.pipeline_id)
        if response.pipeline_id != plan.pipeline_id:
            raise RuntimeError("Databricks pipeline identity does not match compiled plan")

        spec = response.spec
        if spec is None:
            raise RuntimeError("Databricks returned no pipeline specification")
        deployment = spec.deployment
        if deployment is None or _enum_value(deployment.kind) != "BUNDLE":
            raise RuntimeError("Databricks pipeline is not managed by a bundle deployment")
        if (
            deployment.deployment_id != expected_deployment_id
            or deployment.version_id != expected_version_id
        ):
            raise RuntimeError(
                "Databricks bundle deployment snapshot does not match compiled plan"
            )

        configuration = spec.configuration or {}
        if configuration.get(PROFILE_DIGEST_CONFIGURATION) != plan.profile_digest:
            raise RuntimeError(
                "Databricks pipeline profile digest does not match compiled plan"
            )
        if (
            configuration.get(SOURCE_FINGERPRINT_CONFIGURATION)
            != plan.source_fingerprint
        ):
            raise RuntimeError(
                "Databricks pipeline source fingerprint does not match compiled plan"
            )
        if configuration.get(SOURCE_TABLE_CONFIGURATION) != plan.source_table:
            raise RuntimeError(
                "Databricks pipeline source table does not match compiled plan"
            )
        if configuration.get(SOURCE_DELTA_VERSION_CONFIGURATION) != str(
            plan.source_delta_version
        ):
            raise RuntimeError(
                "Databricks pipeline source Delta version does not match compiled plan"
            )

        # REPLACE WHERE is GA and the production pipeline should use the CURRENT
        # channel. Serverless remains required for incremental refresh. Pipeline
        # update parameters are a separate Beta feature controlled by the
        # workspace Previews setting, not by this runtime channel.
        if _enum_value(spec.channel or "").upper() != REQUIRED_PIPELINE_CHANNEL:
            raise RuntimeError("Databricks pipeline is not on the CURRENT channel")
        if spec.serverless is not True:
            raise RuntimeError("Databricks recovery pipeline must use serverless compute")

    def start_bounded_update(
        self,
        *,
        plan: CompiledPlan,
        predicate: str,
        action_id: str,
    ) -> str:
        from databricks.sdk.service import pipelines

        self._attest_pipeline(plan)

        # Pipeline parameters are a Beta feature. Python pipeline source cannot
        # read them; this marker exists only so paginated update history can
        # reconcile a response lost after acceptance. It is not an idempotency
        # token, so the worker's exclusive journal remains authoritative.
        response = self._workspace.pipelines.start_update(
            pipeline_id=plan.pipeline_id,
            cause=pipelines.StartUpdateCause.API_CALL,
            refresh_selection=list(plan.refresh_selection),
            parameters={RECOVERY_ACTION_PARAMETER: action_id},
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
            for update in response.updates or []:
                if update.pipeline_id not in (None, pipeline_id):
                    raise RuntimeError(
                        "Databricks returned an update for a different pipeline"
                    )
                if (
                    update.update_id
                    and (update.parameters or {}).get(RECOVERY_ACTION_PARAMETER)
                    == action_id
                    and update.update_id not in matches
                ):
                    matches.append(update.update_id)
            next_token = response.next_page_token
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
