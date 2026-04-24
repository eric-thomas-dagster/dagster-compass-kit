"""CompassResource — inject Compass into any asset/op.

A thin Dagster-aware wrapper around :class:`CompassClient`. All the
substantive logic (chat, structured responses, conversations, summaries,
runbooks) lives on the client; the resource adds Dagster config plumbing
so ``EnvVar`` / ``ConfigurableResource`` just work.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, TypeVar

from dagster import ConfigurableResource
from pydantic import BaseModel, Field, PrivateAttr

from .client import CompassResponse
from .compass_client import CompassClient, CompassConversation

T = TypeVar("T", bound=BaseModel)


class CompassNotConfiguredError(RuntimeError):
    """Raised when CompassResource is used without URL / API token set.

    The resource deliberately allows empty defaults so Dagster code
    locations load even when ``DAGSTER_CLOUD_URL`` or
    ``DAGSTER_CLOUD_API_TOKEN`` aren't set yet — this is important for
    local dev, CI, and early demo environments. This error only fires
    at call-time, when something actually tries to reach Compass.

    Kit surfaces (hooks, sensors, asset checks) all catch this and
    degrade to a clean warning + skip rather than exploding the run.
    """


class CompassResource(ConfigurableResource):
    """Dagster-facing resource for Dagster+ Compass.

    All methods mirror :class:`CompassClient` — see that class for the full
    surface. Examples::

        compass.ask("What failed this week?")
        compass.ask_structured("Is this anomalous?", AnomalyVerdict)

        with compass.conversation() as chat:
            chat.ask("Which jobs failed yesterday?")
            chat.ask("Of those, which are ETL jobs?")

        compass.summarize_materialization(run_id="...", asset_key_path=["x"])
        compass.generate_job_runbook(job_name="orders_pipeline")

    **Config-time vs call-time validation.** The URL and token fields
    default to empty strings so the resource config resolves even when
    ``EnvVar('DAGSTER_CLOUD_URL')`` can't find the env var. Missing
    config is only rejected when something actually tries to talk to
    Compass — raising :class:`CompassNotConfiguredError`, which kit
    surfaces catch gracefully. That keeps a demo/dev environment
    loadable without a live tenant.
    """

    dagster_cloud_url: str = Field(
        default="",
        description=(
            "Full Dagster+ GraphQL URL, e.g. "
            "'https://my-org.dagster.cloud/prod/graphql'. Empty means "
            "unconfigured — any attempt to call Compass will raise "
            "CompassNotConfiguredError. Set via EnvVar('DAGSTER_CLOUD_URL')."
        ),
    )
    api_token: str = Field(
        default="",
        description=(
            "Dagster+ API token. Empty means unconfigured. "
            "Set via EnvVar('DAGSTER_CLOUD_API_TOKEN')."
        ),
    )
    timeout_seconds: float = Field(
        default=120.0,
        description="Hard cap per Compass call.",
    )

    _client: CompassClient = PrivateAttr()

    @classmethod
    def from_env(
        cls,
        url_var: str = "DAGSTER_CLOUD_URL",
        token_var: str = "DAGSTER_CLOUD_API_TOKEN",
        **kwargs,
    ) -> "CompassResource":
        """Build a CompassResource from env vars, tolerating missing ones.

        Dagster's built-in ``EnvVar(...)`` raises at config-resolution time
        if the variable is unset, which blows up code-location loading in
        dev/CI environments. This classmethod uses ``os.getenv`` with empty
        string defaults instead — the resource loads, and
        :class:`CompassNotConfiguredError` fires at call time if anything
        actually tries to reach Compass with empty creds.

        Identical runtime behavior on Dagster+ deployments: secrets
        configured on the deployment are injected as real env vars, so
        ``os.getenv`` resolves them just fine.

        Example::

            resources={"compass": CompassResource.from_env()}
        """
        import os

        return cls(
            dagster_cloud_url=os.getenv(url_var, ""),
            api_token=os.getenv(token_var, ""),
            **kwargs,
        )

    def _ensure_configured(self) -> None:
        """Raise CompassNotConfiguredError if URL or token is unset."""
        missing: list[str] = []
        if not self.dagster_cloud_url:
            missing.append("dagster_cloud_url (DAGSTER_CLOUD_URL)")
        if not self.api_token:
            missing.append("api_token (DAGSTER_CLOUD_API_TOKEN)")
        if missing:
            raise CompassNotConfiguredError(
                f"CompassResource is missing: {', '.join(missing)}. "
                "Set the corresponding env vars and restart. Until then, "
                "kit surfaces will skip gracefully."
            )

    def setup_for_execution(self, context) -> None:  # noqa: D401
        """Build the client lazily so env-var substitution has happened first.

        Does *not* raise on missing config — that check is deferred to
        call time so a dev environment without Compass env vars can
        still load and start the code server.
        """
        if self.dagster_cloud_url and self.api_token:
            self._client = CompassClient(
                dagster_cloud_url=self.dagster_cloud_url,
                api_token=self.api_token,
                timeout_seconds=self.timeout_seconds,
            )

    # Build-on-first-use fallback so unit tests without setup_for_execution work
    def _ensure_client(self) -> CompassClient:
        self._ensure_configured()
        if not hasattr(self, "_client") or self._client is None:
            self._client = CompassClient(
                dagster_cloud_url=self.dagster_cloud_url,
                api_token=self.api_token,
                timeout_seconds=self.timeout_seconds,
            )
        return self._client

    # ── Free-form ────────────────────────────────────────────────────────────
    def ask(self, prompt: str, chat_id: int = 0) -> CompassResponse:
        return self._ensure_client().ask(prompt, chat_id=chat_id)

    async def ask_async(self, prompt: str, chat_id: int = 0) -> CompassResponse:
        return await self._ensure_client().ask_async(prompt, chat_id=chat_id)

    # ── Structured ───────────────────────────────────────────────────────────
    def ask_structured(self, prompt: str, schema: type[T], chat_id: int = 0) -> T:
        return self._ensure_client().ask_structured(prompt, schema, chat_id=chat_id)

    async def ask_structured_async(
        self, prompt: str, schema: type[T], chat_id: int = 0
    ) -> T:
        return await self._ensure_client().ask_structured_async(
            prompt, schema, chat_id=chat_id
        )

    # ── Conversations ────────────────────────────────────────────────────────
    @contextmanager
    def conversation(self) -> Iterator[CompassConversation]:
        with self._ensure_client().conversation() as chat:
            yield chat

    # ── Per-materialization summary ──────────────────────────────────────────
    def summarize_materialization(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        return self._ensure_client().summarize_materialization(run_id, asset_key_path)

    async def summarize_materialization_async(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        return await self._ensure_client().summarize_materialization_async(
            run_id, asset_key_path
        )

    # ── Runbooks ─────────────────────────────────────────────────────────────
    def generate_job_runbook(self, job_name: str, prompt: str | None = None) -> str:
        from .runbook import generate_job_runbook

        return generate_job_runbook(self._ensure_client(), job_name=job_name, prompt=prompt)

    def generate_asset_runbook(
        self, asset_key_path: list[str], prompt: str | None = None
    ) -> str:
        from .runbook import generate_asset_runbook

        return generate_asset_runbook(
            self._ensure_client(), asset_key_path=asset_key_path, prompt=prompt
        )

    # ── Exception classification (from retry.py) ─────────────────────────────
    def classify_exception(
        self,
        exc: BaseException,
        *,
        job_name: str = "",
        op_name: str = "",
        run_id: str = "",
        attempt: int = 1,
    ):
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
