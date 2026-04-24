"""Dagster+ Issues integration.

Dagster+ has a native Issues feature (see ``dg api issue``) that tracks
problems on runs and assets directly inside the Dagster+ UI — no JIRA /
Linear / GitHub needed. This module lets the kit draft issue content via
Compass and create issues automatically on failure.

The hook ``compass_create_issue_on_failure()`` is the headline surface: drop
it on a job and every op failure becomes a pre-triaged Issue in Dagster+
with title, description, severity, and suggested labels, linked to the
failing run.

**Creation paths (in order of preference):**

1. **GraphQL mutation** — cleanest path, reuses the Bearer token already
   used for Compass and avoids any CLI dependency. The exact mutation
   name + args for Dagster+ Issues isn't publicly documented, so by
   default this path is **off**. Pass ``create_via_graphql=True`` and
   an ``IssueMutationSpec`` describing your tenant's mutation (name +
   argument layout) to opt in. A helper to introspect the tenant's
   schema for Issue-related mutations lives at
   :func:`introspect_issue_mutations`.

2. **`dg api issue create` subprocess** — works if ``dg`` is installed
   on PATH in the execution environment (often true for local dev and
   some hybrid setups, not guaranteed for serverless user code). Auth
   is pulled from whatever env vars ``dg`` was configured against
   (``DAGSTER_CLOUD_ORGANIZATION`` / ``DAGSTER_CLOUD_DEPLOYMENT`` /
   ``DAGSTER_CLOUD_API_TOKEN``).

3. **Fallback: metadata-only** — if both creation paths fail (no
   mutation spec, no ``dg`` CLI, or network error), the Compass-drafted
   title / description / severity / labels are still attached to the
   run as tags and logged in full, so an operator can create the
   issue manually from the preserved draft.

See :func:`compass_create_issue_on_failure` for the headline hook and
:class:`IssueMutationSpec` for the GraphQL configuration.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from dagster import HookContext, MetadataValue, failure_hook

from .models import IssueDraft
from .resource import CompassResource
from .structured import CompassSchemaError


# ── GraphQL mutation configuration ───────────────────────────────────────────


@dataclass
class IssueMutationSpec:
    """Description of your tenant's ``createIssue`` GraphQL mutation.

    The Dagster+ Issues mutation isn't publicly documented at the GraphQL
    level (it's exposed via the ``dg api issue`` CLI), so the kit can't
    invent a shape that works for every tenant. Once you introspect your
    tenant's schema (see :func:`introspect_issue_mutations`), fill this in
    and pass it to :func:`compass_create_issue_on_failure` via
    ``graphql_spec=…``.

    Example — hypothetical shape, confirm against your tenant::

        IssueMutationSpec(
            mutation_name="createIssue",
            input_wrapper_arg="input",        # or None if args are flat
            argument_builder=lambda draft, run_id: {
                "input": {
                    "title": draft.title,
                    "description": draft.description,
                    "runId": run_id,
                    "severity": draft.severity.upper(),
                    "labels": draft.suggested_labels,
                }
            },
            success_typename="CreateIssueSuccess",
        )
    """

    mutation_name: str
    """The field name on the ``Mutation`` type, e.g. ``createIssue``."""

    argument_builder: Callable[[IssueDraft, str], dict[str, Any]]
    """Callable that maps (draft, run_id) → the variables dict for the mutation."""

    input_wrapper_arg: Optional[str] = None
    """Name of the input wrapper arg (e.g. ``'input'``). ``None`` if the mutation
    takes flat top-level args instead."""

    success_typename: Optional[str] = None
    """If the mutation returns a union, the ``__typename`` indicating success.
    Leave ``None`` to treat any non-error response as success."""

    selection_set: str = "__typename"
    """GraphQL selection set inside the mutation. Default just grabs __typename;
    customize if you want the new issue's id / url in the response."""


def introspect_issue_mutations(compass: CompassResource) -> str:
    """Best-effort introspection — returns a human-readable string describing
    all mutation fields with "issue" in the name.

    Intended to be called once from a notebook or a small script to discover
    the right shape for your tenant, then used to construct an
    :class:`IssueMutationSpec`. See the module docstring for the raw ``curl``
    form if you'd rather skip the kit entirely.
    """
    import asyncio

    from .client import _http_to_ws  # reuse auth + url plumbing
    from .compass_client import CompassClient

    client: CompassClient = compass._ensure_client()  # type: ignore[attr-defined]

    # We can't introspect via the WebSocket surface — schema introspection
    # is over HTTP. Use the same Bearer token.
    import urllib.request

    query = (
        "{ __schema { mutationType { fields { name args { name type { name kind "
        "ofType { name kind } } } } } } }"
    )
    req = urllib.request.Request(
        client.dagster_cloud_url,
        method="POST",
        data=json.dumps({"query": query}).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {client.api_token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
    except Exception as e:  # noqa: BLE001
        return f"(introspection failed: {e})"

    fields = (
        body.get("data", {}).get("__schema", {}).get("mutationType", {}).get("fields", [])
    )
    issue_fields = [f for f in fields if "issue" in f.get("name", "").lower()]
    if not issue_fields:
        return "(no mutation fields with 'issue' in the name were found)"

    lines = []
    for f in issue_fields:
        args = ", ".join(f"{a['name']}: {_format_type(a.get('type', {}))}" for a in f.get("args", []))
        lines.append(f"{f['name']}({args})")
    return "\n".join(lines)


def _format_type(t: dict[str, Any]) -> str:
    name = t.get("name")
    if name:
        return name
    of = t.get("ofType") or {}
    kind = t.get("kind", "")
    inner = _format_type(of) if of else "?"
    return {"NON_NULL": f"{inner}!", "LIST": f"[{inner}]"}.get(kind, inner)

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


# Confirmed mutation shape against a live Dagster+ tenant (April 2026):
#   createIssue(
#     title: String!,
#     description: String!,
#     origin: IssueLinkedObjectInput,   # { runId: "..." } for run-linked
#     chatId: Int                       # optional link to a Compass chat
#   ) → CreateIssueSuccess | UnauthorizedError | PythonError
_CREATE_ISSUE_MUTATION = """\
mutation CreateIssue($title: String!, $description: String!, $origin: IssueLinkedObjectInput, $chatId: Int) {
  createIssue(title: $title, description: $description, origin: $origin, chatId: $chatId) {
    __typename
    ... on CreateIssueSuccess {
      issue {
        id
        publicId
        title
        status
      }
    }
    ... on UnauthorizedError { message }
    ... on PythonError { message }
  }
}
"""


def _draft_to_description(draft: IssueDraft) -> str:
    """Fold severity + labels into the description markdown.

    The createIssue mutation only accepts title + description; severity and
    labels aren't first-class fields. So we append them as a metadata footer
    that renders inline in the issue body.
    """
    footer_parts = []
    if draft.severity:
        footer_parts.append(f"**Severity:** {draft.severity}")
    if draft.suggested_labels:
        footer_parts.append(f"**Labels:** {', '.join(draft.suggested_labels)}")
    footer_parts.append("_Auto-generated by dagster-compass-kit._")
    footer = "\n\n---\n\n" + "  \n".join(footer_parts)
    return draft.description + footer


def _create_via_graphql(
    compass: CompassResource,
    draft: IssueDraft,
    run_id: str,
    *,
    chat_id: Optional[int] = None,
    custom_spec: Optional[IssueMutationSpec] = None,
) -> tuple[bool, str]:
    """POST the createIssue mutation. Returns (success, detail).

    Uses the confirmed Dagster+ mutation shape by default. Pass ``custom_spec``
    only if you need to override for a non-standard tenant (shouldn't be
    needed in practice — this mutation is what the Dagster+ UI itself uses).
    """
    import urllib.request

    client = compass._ensure_client()  # type: ignore[attr-defined]

    if custom_spec is not None:
        variables = custom_spec.argument_builder(draft, run_id)
        mutation = _build_custom_mutation(custom_spec, variables)
    else:
        variables = {
            "title": draft.title,
            "description": _draft_to_description(draft),
            "origin": {"runId": run_id},
            "chatId": chat_id,
        }
        mutation = _CREATE_ISSUE_MUTATION

    try:
        req = urllib.request.Request(
            client.dagster_cloud_url,
            method="POST",
            data=json.dumps({"query": mutation, "variables": variables}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {client.api_token}",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
    except Exception as e:  # noqa: BLE001
        return False, f"GraphQL request failed: {e}"

    if body.get("errors"):
        return False, f"GraphQL errors: {body['errors']}"

    mutation_field = custom_spec.mutation_name if custom_spec else "createIssue"
    result = (body.get("data") or {}).get(mutation_field) or {}
    typename = result.get("__typename", "")
    if typename == "CreateIssueSuccess":
        issue = result.get("issue") or {}
        return True, f"public_id={issue.get('publicId')} id={issue.get('id')}"
    if typename in ("UnauthorizedError", "PythonError"):
        return False, f"{typename}: {result.get('message', '(no message)')}"
    if custom_spec and custom_spec.success_typename and typename != custom_spec.success_typename:
        return False, f"Mutation returned {typename}, expected {custom_spec.success_typename}"
    return False, f"Unexpected response shape: {json.dumps(result)[:200]}"


def _build_custom_mutation(spec: IssueMutationSpec, variables: dict[str, Any]) -> str:
    """Escape hatch for non-default tenants. Rarely needed."""
    if spec.input_wrapper_arg:
        return (
            f"mutation CreateIssue($input: CreateIssueInput!) {{ "
            f"{spec.mutation_name}(input: $input) {{ {spec.selection_set} }} "
            f"}}"
        )
    arg_defs = ", ".join(f"${k}: String!" for k in variables)
    arg_calls = ", ".join(f"{k}: ${k}" for k in variables)
    return (
        f"mutation CreateIssue({arg_defs}) {{ "
        f"{spec.mutation_name}({arg_calls}) {{ {spec.selection_set} }} "
        f"}}"
    )


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
    create_via_graphql: bool = True,
    create_via_cli: bool = False,
    custom_mutation_spec: Optional[IssueMutationSpec] = None,
):
    """Return a Dagster failure hook that opens a Dagster+ Issue on failure.

    Creation path (tried in order):
      1. **GraphQL** (default) — hits the ``createIssue`` mutation on your
         tenant's Dagster+ GraphQL endpoint using the Bearer token already
         configured on the ``CompassResource``. No CLI dependency, no
         separate auth plumbing. Mutation shape is hardcoded from the
         real Dagster+ UI's implementation.
      2. **`dg` CLI** (opt-in) — shells out to ``dg api issue create``.
         Requires ``dg`` on PATH and ``DAGSTER_CLOUD_*`` env vars in the
         op's runtime. Off by default since GraphQL supersedes it; keep
         as an escape hatch if GraphQL ever breaks.
      3. **Metadata-only fallback** — if both above fail, the
         Compass-drafted content is attached to the run as tags and
         logged in full so an operator can create the issue manually.

    Args:
        resource_key: Resource key holding the ``CompassResource``.
        attach_draft_as_metadata: Always attach the Compass draft as run
            metadata so it's never lost, even if creation fails.
        create_via_graphql: Use the built-in GraphQL path (default True).
            Issues are created with title + description + origin.runId;
            Compass-generated severity + labels fold into the description
            body since the mutation doesn't accept them as first-class.
        create_via_cli: Fall back to the ``dg`` CLI subprocess if GraphQL
            fails. Off by default.
        custom_mutation_spec: Escape hatch for non-standard tenants where
            ``createIssue`` has a different shape. Shouldn't be needed in
            practice.

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

        # 1. GraphQL first (default path, confirmed mutation shape)
        if create_via_graphql:
            created, detail = _create_via_graphql(
                compass,
                draft,
                context.run_id,
                custom_spec=custom_mutation_spec,
            )
            if created:
                context.log.info(
                    f"Dagster+ Issue created via GraphQL for run {context.run_id}: {detail}"
                )
            else:
                context.log.warning(
                    f"compass_create_issue_on_failure: GraphQL creation failed ({detail})"
                )

        # 2. dg CLI fallback (opt-in)
        if not created and create_via_cli:
            cli_ok, cli_detail = _create_via_dg_cli(draft, run_id=context.run_id)
            if cli_ok:
                created, detail = True, cli_detail
                context.log.info(
                    f"Dagster+ Issue created via dg CLI for run {context.run_id}: {detail}"
                )
            else:
                context.log.warning(
                    f"compass_create_issue_on_failure: dg CLI creation failed ({cli_detail})"
                )

        if not created:
            context.log.warning(
                "compass_create_issue_on_failure: no creation path succeeded; "
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
