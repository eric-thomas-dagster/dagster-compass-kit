"""Compass-powered runbook generator.

Generates a markdown runbook for a given Dagster job/asset, grounded in
actual recent history for the deployment. Output is a plain markdown
string — persist it wherever you want (a repo file, an asset's
metadata, a Notion page, an issue description).
"""

from __future__ import annotations

from typing import Optional

from .compass_client import CompassClient
from .models import RunbookSections
from .structured import CompassSchemaError

_DEFAULT_PROMPT_JOB = """\
You're writing an on-call runbook for the Dagster job ``{job_name}`` in this
deployment. Use recent run history and failure patterns as context.

Include:
1. **Purpose** — what the job does and why it matters, based on what you see
   in the data.
2. **Dependencies** — upstream data sources and downstream consumers.
3. **Common failure modes** — patterns of recent failures, with root cause
   where known.
4. **First triage steps** — an ordered checklist an on-call engineer should
   run through when this job fails.
5. **Escalation** — when to page a specific team or owner.

Format as markdown with these exact section headings. Keep it under ~500 words.
"""

_DEFAULT_PROMPT_ASSET = """\
You're writing an on-call runbook for the Dagster asset ``{asset_key}`` in
this deployment. Use recent materialization history and freshness patterns
as context.

Include:
1. **Purpose** — what this asset represents and downstream consumers.
2. **Dependencies** — upstream assets and how they feed this one.
3. **Common failure modes** — patterns of recent failed or missing
   materializations.
4. **First triage steps** — what to check first when this asset is stale
   or failing.
5. **Escalation** — when to page a specific team or owner.

Format as markdown with these exact section headings. Keep it under ~500 words.
"""


def generate_job_runbook(
    client: CompassClient,
    *,
    job_name: str,
    prompt: Optional[str] = None,
) -> str:
    """Return a markdown runbook for a Dagster job.

    ``prompt`` overrides the default if you want a different structure.
    Supports ``{job_name}`` substitution.
    """
    tpl = prompt or _DEFAULT_PROMPT_JOB
    response = client.ask(tpl.format(job_name=job_name))
    if response.error:
        return f"*Compass error: {response.error}*\n\n{response.text or ''}"
    return response.text or "*(empty response)*"


def generate_asset_runbook(
    client: CompassClient,
    *,
    asset_key_path: list[str],
    prompt: Optional[str] = None,
) -> str:
    """Return a markdown runbook for a Dagster asset."""
    tpl = prompt or _DEFAULT_PROMPT_ASSET
    filled = tpl.format(asset_key="/".join(asset_key_path))
    response = client.ask(filled)
    if response.error:
        return f"*Compass error: {response.error}*\n\n{response.text or ''}"
    return response.text or "*(empty response)*"


def generate_structured_runbook(
    client: CompassClient,
    *,
    job_name: str,
) -> RunbookSections:
    """Structured (Pydantic) variant. Useful when you want to render a custom template."""
    prompt = _DEFAULT_PROMPT_JOB.format(job_name=job_name) + (
        "\n\nInstead of free-form markdown, return a structured response."
    )
    try:
        return client.ask_structured(prompt, schema=RunbookSections)
    except CompassSchemaError:
        return RunbookSections(
            purpose="(Compass response was not parseable as structured output)",
            dependencies="",
            common_failure_modes=[],
            first_checks=[],
            escalation="",
        )
