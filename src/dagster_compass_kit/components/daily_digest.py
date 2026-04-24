"""``DailyInsightDigest`` — one-YAML-block Compass-powered digest asset.

Example ``component.yaml``::

    type: dagster_compass_kit.DailyInsightDigest
    attributes:
      asset_key: analytics/daily_compass_digest
      cron_schedule: "0 9 * * *"
      prompt: |
        Summarize yesterday's pipeline activity across this deployment.
        Call out the most failed jobs, slowest materializations, and any
        unusual patterns. Format as markdown suitable for a Slack post.
      slack_channel: "#data-standup"   # optional
      resource_key: compass

The component generates: an asset (the digest body materialized as markdown
metadata), a schedule that fires that asset, and — if ``slack_channel`` is
set — a post to Slack on each successful materialization.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable

from ..resource import CompassResource


@dataclass
class DailyInsightDigest(Component, Resolvable):
    """Config for the digest. Fields map 1:1 to ``attributes:`` in YAML."""

    asset_key: str
    """Slash-separated asset key, e.g. 'analytics/daily_compass_digest'."""

    prompt: str
    """Prompt handed to Compass. Markdown formatting is honored downstream."""

    cron_schedule: str = "0 9 * * *"
    """When to materialize the digest. Defaults to 9am daily."""

    slack_channel: Optional[str] = None
    """Optional Slack channel to post the digest into on materialization."""

    resource_key: str = "compass"
    """Resource key that maps to a ``CompassResource`` in your Definitions."""

    group_name: str = "compass"
    """Dagster group for the generated asset."""

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        asset_key = dg.AssetKey(self.asset_key.split("/"))
        prompt = self.prompt
        slack_channel = self.slack_channel
        resource_key = self.resource_key
        group_name = self.group_name

        @dg.asset(
            key=asset_key,
            description="Daily Compass-generated insight digest",
            group_name=group_name,
            required_resource_keys={resource_key},
        )
        def _digest_asset(asset_ctx: dg.AssetExecutionContext) -> dg.MaterializeResult:
            compass: CompassResource = getattr(asset_ctx.resources, resource_key)
            response = compass.ask(prompt)
            body = response.text or "(no digest returned)"
            if response.error:
                body = f"(compass error: {response.error})\n\n{body}"

            if slack_channel:
                try:
                    slack = asset_ctx.resources.slack  # type: ignore[attr-defined]
                    slack.get_client().chat_postMessage(channel=slack_channel, text=body)
                except Exception as e:  # noqa: BLE001
                    asset_ctx.log.warning(f"DailyInsightDigest: Slack post failed: {e!r}")

            return dg.MaterializeResult(
                metadata={
                    "digest": dg.MetadataValue.md(body),
                    "tool_calls": dg.MetadataValue.int(len(response.tool_calls)),
                    "suggested_replies": dg.MetadataValue.json(response.suggested_replies),
                }
            )

        digest_job = dg.define_asset_job(
            name=f"{asset_key.to_python_identifier()}_job",
            selection=[asset_key],
        )

        digest_schedule = dg.ScheduleDefinition(
            name=f"{asset_key.to_python_identifier()}_schedule",
            cron_schedule=self.cron_schedule,
            job=digest_job,
        )

        return dg.Definitions(
            assets=[_digest_asset],
            jobs=[digest_job],
            schedules=[digest_schedule],
        )
