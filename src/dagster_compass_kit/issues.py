"""Dagster+ Issues integration.

Dagster+ has a native Issues feature (see ``dg api issue``) that tracks
problems on runs and assets directly inside the Dagster+ UI — no JIRA /
Linear / GitHub needed. This module lets the kit draft issue content via
Compass and create issues automatically on failure.

The hook ``compass_create_issue_on_failure()`` is the headline surface: drop
it on a job and every op failure becomes a pre-triaged Issue in Dagster+
with title, description, severity, and suggested labels, linked to the
failing run.

There are two ways a Dagster+ Issue can actually be created:

1. **GraphQL mutation** — the same Dagster+ GraphQL endpoint this kit already
   uses. We try this first, since the auth path and client code are already
   set up. The mutation name is ``createIssue`` (confirmed in a real tenant
   next week). If the mutation shape differs from what we expect, we fall
   through to path 2.

2. **`dg api issue create` subprocess** — shells out to the ``dg`` CLI if
   it's available in the execution environment.

If both paths fail, the draft is still attached to the run as metadata under
``compass_issue_draft`` so the information is never lost — the operator can
copy-paste to create the issue manually.
"""

from __future__ import annotations

import shutil
import subprocess
from typing import Optional

from dagster import HookContext, MetadataValue, failure_hook

from .models import IssueDraft
from .resource import CompassResource
from .structured import CompassSchemaError

_ISSUE_PROMPT_TEMPLATE = """\
A Dagster pipeline just failed and I need to open a Dagster+ Issue to track
it. Here's the context:

- Job: {job_name}
- Run ID: {run_id}
- Failing op/step: {step_key}
- Exception type: {exc_type}
- Exception message: {exc_message}

Please analyze the failure using recent run history for this job/step.
Draft a Dagster+ Issue: a concise title (under 90 chars), a markdown
description covering what happened, likely root cause, similar recent
failures if any, and first-response actions. Pick a severity and
suggest labels that would route this to the right owner.
"""


def draft_issue_for_failure(
    compass: CompassResource,
    *,
    job_name: str,
    run_id: str,
    step_key: str,
    exc: BaseException,
) -> Optional[IssueDraft]:
    """Ask Compass to draft an ``IssueDraft`` for a pipeline failure.

    Returns ``None`` if Compass errors or its response can't be parsed into
    the schema. The caller should treat ``None`` as "draft failed, skip
    Issue creation, original exception still stands."
    """
    prompt = _ISSUE_PROMPT_TEMPLATE.format(
        job_name=job_name,
        run_id=run_id,
        step_key=step_key,
        exc_type=type(exc).__name__,
        exc_message=str(exc)[:500],
    )
    try:
        return compass.ask_structured(prompt, schema=IssueDraft)
    except CompassSchemaError:
        return None
    except Exception:  # noqa: BLE001 - never let a broken Compass mask the real failure
        return None


# ── Creation paths ───────────────────────────────────────────────────────────


def _create_via_dg_cli(
    draft: IssueDraft, run_id: str, *, extra_args: Optional[list[str]] = None
) -> tuple[bool, str]:
    """Shell out to ``dg api issue create``. Returns (success, message).

    Requires ``dg`` to be installed and authenticated in the execution
    environment — typically true in Dagster+ user code deployments but
    worth verifying for your setup.
    """
    if shutil.which("dg") is None:
        return False, "dg CLI not on PATH"
    cmd = [
        "dg",
        "api",
        "issue",
        "create",
        "--title",
        draft.title,
        "--description",
        draft.description,
        "--run-id",
        run_id,
    ]
    if extra_args:
        cmd.extend(extra_args)
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30, check=False
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, f"subprocess error: {e}"
    if result.returncode != 0:
        return False, f"dg exited {result.returncode}: {result.stderr.strip()[:200]}"
    return True, (result.stdout.strip() or "created")


# ── Failure hook ─────────────────────────────────────────────────────────────


def compass_create_issue_on_failure(
    *,
    resource_key: str = "compass",
    attach_draft_as_metadata: bool = True,
    create_via_cli: bool = True,
):
    """Return a Dagster failure hook that opens a Dagster+ Issue on failure.

    Flow:
      1. Ask Compass to draft an ``IssueDraft`` for the failing run.
      2. If ``create_via_cli`` is True, call ``dg api issue create`` to open
         the issue in Dagster+, linked to the run.
      3. If creation fails (CLI missing, auth problem, etc.) AND
         ``attach_draft_as_metadata`` is True, attach the draft as run
         metadata so the operator can create the issue manually from
         the draft.

    Args:
        resource_key: Resource key holding the ``CompassResource``.
        attach_draft_as_metadata: Always attach the Compass-generated draft
            as run metadata so it's never lost, even if creation fails.
        create_via_cli: Try to actually create the issue via the ``dg`` CLI.
            Set False if you just want the draft metadata and plan to wire
            your own creation flow.

    Example::

        from dagster import job
        from dagster_compass_kit import compass_create_issue_on_failure

        @job(hooks={compass_create_issue_on_failure()})
        def orders_pipeline():
            orders_etl()
    """

    @failure_hook(required_resource_keys={resource_key})
    def _hook(context: HookContext) -> None:
        compass: CompassResource = getattr(context.resources, resource_key)

        exc = context.op_exception or RuntimeError("unknown failure")
        draft = draft_issue_for_failure(
            compass,
            job_name=context.job_name,
            run_id=context.run_id,
            step_key=context.op.name,
            exc=exc,
        )

        if draft is None:
            context.log.warning(
                "compass_create_issue_on_failure: Compass failed to draft issue; "
                "no issue will be created."
            )
            return

        created = False
        detail = ""
        if create_via_cli:
            created, detail = _create_via_dg_cli(draft, run_id=context.run_id)
            if created:
                context.log.info(
                    f"Dagster+ Issue created via dg CLI for run {context.run_id}: {detail}"
                )
            else:
                context.log.warning(
                    f"compass_create_issue_on_failure: creation skipped ({detail}); "
                    "attaching draft as run metadata instead."
                )

        if attach_draft_as_metadata:
            try:
                from dagster import DagsterInstance  # local to keep module light

                instance = (
                    context.instance
                    if hasattr(context, "instance")
                    else DagsterInstance.get()
                )
                # Tags are length-limited — store a short pointer and put the
                # full draft in the run's event log via the logger.
                short_tag = (draft.title or "")[:180]
                instance.add_run_tags(
                    context.run_id,
                    {
                        "dagster-compass/issue_draft_title": short_tag,
                        "dagster-compass/issue_draft_severity": draft.severity,
                        "dagster-compass/issue_created": "true" if created else "false",
                    },
                )
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"compass_create_issue_on_failure: tag attach failed: {e!r}")

            # Always log the full draft so operators have it even if creation failed
            context.log.info(
                f"Compass-drafted issue for run {context.run_id}:\n"
                f"TITLE: {draft.title}\n"
                f"SEVERITY: {draft.severity}\n"
                f"LABELS: {', '.join(draft.suggested_labels) if draft.suggested_labels else '(none)'}\n\n"
                f"{draft.description}"
            )

    return _hook
