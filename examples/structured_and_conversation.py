"""Structured responses + multi-turn conversations.

Two related features demonstrated together:

1. ``ask_structured`` turns Compass into a function-calling primitive — you
   get back a typed Pydantic object instead of raw text.
2. ``conversation`` preserves ``chat_id`` across turns so follow-ups stay
   coherent with earlier questions.
"""

from dagster import Definitions, EnvVar, MaterializeResult, MetadataValue, asset
from pydantic import BaseModel, Field

from dagster_compass_kit import CompassResource


class PipelineHealth(BaseModel):
    overall: str = Field(description="One word: healthy, degraded, or critical.")
    top_issues: list[str] = Field(default_factory=list, description="Up to 3 most urgent issues.")
    trend: str = Field(description="One short sentence on whether things are improving or degrading.")


@asset
def health_snapshot(compass: CompassResource) -> MaterializeResult:
    # Multi-turn chat: drill down into whatever the overall verdict surfaces
    with compass.conversation() as chat:
        verdict = chat.ask_structured(
            "Assess the overall pipeline health for this Dagster+ deployment right now.",
            PipelineHealth,
        )

        # Follow-up preserves context — no need to re-explain what you're asking about
        deep_dive = None
        if verdict.top_issues:
            deep_dive_response = chat.ask(
                f"For the first issue you listed ('{verdict.top_issues[0]}'), "
                "walk me through root cause and recommended next action."
            )
            deep_dive = deep_dive_response.text

    return MaterializeResult(
        metadata={
            "overall": MetadataValue.text(verdict.overall),
            "top_issues": MetadataValue.json(verdict.top_issues),
            "trend": MetadataValue.text(verdict.trend),
            **({"deep_dive": MetadataValue.md(deep_dive)} if deep_dive else {}),
        }
    )


defs = Definitions(
    assets=[health_snapshot],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
