"""CompassResource — inject Compass into any asset/op.

Usage inside an asset::

    from dagster import asset, MetadataValue, MaterializeResult, EnvVar
    from dagster_compass_kit import CompassResource

    @asset
    def weekly_digest(compass: CompassResource) -> MaterializeResult:
        answer = compass.ask("Summarize this week's pipeline health.")
        return MaterializeResult(
            metadata={"summary": MetadataValue.md(answer.text)},
        )

    defs = Definitions(
        assets=[weekly_digest],
        resources={
            "compass": CompassResource(
                dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
                api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
            ),
        },
    )
"""

from __future__ import annotations

import asyncio

from dagster import ConfigurableResource
from pydantic import Field

from .client import (
    CompassResponse,
    assemble,
    stream_ai_chat,
    stream_ai_summary_for_asset,
)


class CompassResource(ConfigurableResource):
    """Resource for asking Dagster+ Compass questions from within pipelines.

    Authenticates with a Dagster+ API token. Both the token and the
    Dagster+ GraphQL URL can be wired through ``EnvVar`` so secrets don't
    land in your repo.
    """

    dagster_cloud_url: str = Field(
        description=(
            "Full Dagster+ GraphQL URL for the deployment, e.g. "
            "'https://my-org.dagster.cloud/prod/graphql'. The ws:// variant is "
            "derived from this automatically."
        ),
    )
    api_token: str = Field(
        description="Dagster+ API token. Use EnvVar to pull from a secret.",
    )
    timeout_seconds: float = Field(
        default=120.0,
        description="Hard cap on how long a single Compass call may take.",
    )

    def ask(self, prompt: str, chat_id: int = 0) -> CompassResponse:
        """Ask Compass a free-form question, synchronously.

        Blocks until the subscription completes. Returns the assembled
        prose answer plus any tool calls and suggested follow-ups.
        """
        return asyncio.run(self._ask_async(prompt, chat_id))

    async def ask_async(self, prompt: str, chat_id: int = 0) -> CompassResponse:
        """Async variant of :meth:`ask` for use inside an existing event loop."""
        return await self._ask_async(prompt, chat_id)

    async def _ask_async(self, prompt: str, chat_id: int) -> CompassResponse:
        return await assemble(
            stream_ai_chat(
                graphql_http_url=self.dagster_cloud_url,
                api_token=self.api_token,
                payload=prompt,
                chat_id=chat_id,
                timeout_seconds=self.timeout_seconds,
            )
        )

    def summarize_materialization(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        """Ask Compass for the AI summary of a specific asset materialization.

        Mirrors the "✨ Summarize this materialization" card shown in the
        web/mobile UI. Feature-gated server-side on ``ENABLE_AI_SUMMARIES``.
        """
        return asyncio.run(self._summarize_async(run_id, asset_key_path))

    async def summarize_materialization_async(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        return await self._summarize_async(run_id, asset_key_path)

    async def _summarize_async(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        return await assemble(
            stream_ai_summary_for_asset(
                graphql_http_url=self.dagster_cloud_url,
                api_token=self.api_token,
                run_id=run_id,
                asset_key_path=asset_key_path,
                timeout_seconds=self.timeout_seconds,
            )
        )

    def classify_exception(
        self,
        exc: BaseException,
        *,
        job_name: str = "",
        op_name: str = "",
        run_id: str = "",
        attempt: int = 1,
    ):
        """One-shot: classify an exception via Compass and return a parsed verdict.

        Returns a ``dagster_compass_kit.retry.ExceptionAnalysis``. Never
        raises — parse failures are represented as ``category="unknown"``.

        For the decorator-driven variant, see
        :func:`dagster_compass_kit.compass_retry_advisor`.
        """
        # Local import to keep retry.py optional at import-time
        from .retry import _build_prompt, parse_analysis

        prompt = _build_prompt(
            exc,
            job_name=job_name,
            op_name=op_name,
            run_id=run_id,
            attempt=attempt,
        )
        response = self.ask(prompt)
        return parse_analysis(response.text or "")
