"""Op hooks that call Compass automatically.

The canonical use case: when an op fails, have Compass write the post-mortem
and attach it as run metadata. No human typing required, no stale
"TODO: add runbook" — the summary is generated from the failing run's
actual context every time.

Notifications are intentionally **not** this hook's job — Dagster+ already
has first-class Slack/Teams/email alerts for run failures. This hook just
enriches the run with Compass's analysis; the existing alert then links
to the run page where the metadata renders.

Example::

    from dagster import job, op
    from dagster_compass_kit import CompassResource, compass_on_failure

    @op(required_resource_keys={"compass"})
    def orders_etl(context):
        ...

    @job(hooks={compass_on_failure()})
    def orders_pipeline():
        orders_etl()

    defs = Definitions(
        jobs=[orders_pipeline],
        resources={"compass": CompassResource(...)},
    )
"""

from __future__ import annotations

from typing import Callable

from dagster import (
    HookContext,
    MetadataValue,
    failure_hook,
)

from .resource import CompassResource

_DEFAULT_PROMPT = (
    "A Dagster run just failed. Here's the context:\n"
    "- Job: {job_name}\n"
    "- Run ID: {run_id}\n"
    "- Failed op/step: {step_key}\n"
    "\n"
    "Explain the most likely root cause based on recent history for this "
    "job and step, what to check first, and whether similar failures have "
    "happened before. Be concise."
)


def compass_on_failure(
    *,
    prompt: str | None = None,
    resource_key: str = "compass",
    metadata_key: str = "compass_post_mortem",
) -> Callable:
    """Return a Dagster failure hook that asks Compass to summarize the failure.

    Args:
        prompt: Format-string prompt. Supports ``{job_name}``, ``{run_id}``,
            ``{step_key}``. Defaults to a Dagster-aware root-cause question.
        resource_key: Which resource holds the :class:`CompassResource` instance.
        metadata_key: Key to attach the summary under on the run's metadata.

    The hook attaches the summary as run metadata. Dagster+'s existing Slack
    alert on failure links to the run page where the metadata renders — no
    need for this hook to send its own notification.
    """
    fmt = prompt or _DEFAULT_PROMPT

    @failure_hook(required_resource_keys={resource_key})
    def _hook(context: HookContext) -> None:
        compass: CompassResource = getattr(context.resources, resource_key)
        filled = fmt.format(
            job_name=context.job_name,
            run_id=context.run_id,
            step_key=context.op.name,
        )
        try:
            response = compass.ask(filled)
            summary = response.text or "(no summary returned)"
            if response.error:
                summary = f"(compass error: {response.error})\n\n{summary}"
        except Exception as e:  # noqa: BLE001 — we never want the hook to raise
            context.log.warning(f"compass_on_failure: Compass call raised {e!r}")
            return

        context.log.info(
            f"Compass post-mortem ({len(summary)} chars) attached as "
            f"run metadata under '{metadata_key}'."
        )

        # Attach as run tags — survives with the run forever, renders in
        # the Dagster+ UI, and is what existing Slack alerts will link to.
        try:
            from dagster import DagsterInstance  # local import keeps module light

            instance = context.instance if hasattr(context, "instance") else DagsterInstance.get()
            instance.add_run_tags(
                context.run_id, {f"dagster-compass/{metadata_key}": summary[:1024]}
            )
        except Exception as e:  # noqa: BLE001
            context.log.warning(f"compass_on_failure: could not attach metadata: {e!r}")

    return _hook
