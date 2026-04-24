"""Predefined Pydantic schemas for common Compass-structured-response patterns.

These exist so users don't have to hand-roll a schema for the 80% case. Bring
your own Pydantic models for anything exotic.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class AnomalyVerdict(BaseModel):
    """Schema for 'is this anomalous' style asset checks."""

    is_anomaly: bool = Field(description="Whether Compass considers the observation anomalous.")
    severity: Literal["none", "low", "medium", "high"] = Field(
        description="'none' when is_anomaly is false; otherwise how urgent it is."
    )
    explanation: str = Field(description="One-sentence human-readable reason.")
    similar_events_recently: int = Field(
        default=0,
        description="Count of similar anomalies observed in recent history.",
        ge=0,
    )


class MonitoringDecision(BaseModel):
    """Schema for Compass-driven sensors.

    Compass decides whether the current state warrants a run. If so, it
    names the asset_key(s) to materialize or the job to launch.
    """

    should_trigger: bool = Field(
        description="Whether a Dagster run should be launched right now."
    )
    reason: str = Field(description="One-sentence justification.")
    severity: Literal["info", "low", "medium", "high", "critical"] = "info"
    asset_key_paths: list[str] = Field(
        default_factory=list,
        description=(
            "Optional: slash-separated asset keys to materialize, e.g. "
            "['analytics/orders', 'CLEANED/users']. Empty list means no "
            "specific assets — run the full job instead."
        ),
    )


class IssueDraft(BaseModel):
    """Schema for a Dagster+ Issue generated from a run failure.

    Matches the UI's "Generate an issue" flow: a title, a detailed
    description, and optional tags the user's on-call process might key on.
    """

    title: str = Field(description="Short headline, under ~90 chars.")
    description: str = Field(
        description=(
            "Markdown body explaining what happened, root cause if discernible, "
            "and first-response actions. Reference the run and the failing step."
        )
    )
    severity: Literal["low", "medium", "high", "critical"] = Field(
        default="medium",
        description="Operator-visible urgency level.",
    )
    suggested_labels: list[str] = Field(
        default_factory=list,
        description=(
            "Optional free-form labels for routing. Common choices: "
            "'transient', 'schema-drift', 'oom', 'dependency-outage', 'needs-triage'."
        ),
    )


class RunbookSections(BaseModel):
    """Optional structured runbook output — prefer free-form markdown in most cases."""

    purpose: str = Field(description="What the job/asset does and why it exists.")
    dependencies: str = Field(description="Upstream/downstream lineage in plain English.")
    common_failure_modes: list[str] = Field(
        default_factory=list,
        description="Known failure patterns drawn from recent run history.",
    )
    first_checks: list[str] = Field(
        default_factory=list,
        description="Ordered triage steps for an on-call engineer.",
    )
    escalation: str = Field(default="", description="Who or what to escalate to if triage fails.")
