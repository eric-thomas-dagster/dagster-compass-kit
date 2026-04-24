"""Predefined Pydantic schemas for common Compass-structured-response patterns.

These exist so users don't have to hand-roll a schema for the 80% case. Bring
your own Pydantic models for anything exotic.
"""

from __future__ import annotations

from typing import Literal, Optional

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


class IssueActionPlan(BaseModel):
    """Compass's decision about what to do with a new pipeline failure.

    Used by ``compass_create_issue_on_failure`` to avoid flooding the
    Dagster+ Issues queue with duplicate or redundant tickets. Compass
    checks recent open issues in the deployment (it has access to them
    through the same operational data surface) and decides:

    - ``create_new``  — genuinely new problem, draft fields populated
    - ``link_to_existing`` — there's already an open issue that covers
      this failure; ``existing_issue_public_id`` points to it so the
      hook can attach a link/tag to the new run
    - ``skip`` — intentionally noisy (e.g. we've already filed 10 of
      these in the last hour and nothing has changed)

    The combined decide-and-draft schema keeps the whole flow to a single
    Compass call — one ~30s round-trip per failure instead of two.
    """

    action: Literal["create_new", "link_to_existing", "skip"] = Field(
        description="What to do with this failure."
    )
    reason: str = Field(description="One sentence justifying the decision.")
    existing_issue_public_id: Optional[str] = Field(
        default=None,
        description=(
            "If action is 'link_to_existing', the publicId of the issue that "
            "already covers this failure. Empty otherwise."
        ),
    )
    # When action == 'create_new', these fields drive the new issue
    title: Optional[str] = Field(
        default=None, description="New-issue title. Only set when action is create_new."
    )
    description: Optional[str] = Field(
        default=None, description="New-issue description (markdown). Only set when action is create_new."
    )
    severity: Literal["low", "medium", "high", "critical"] = Field(default="medium")
    suggested_labels: list[str] = Field(default_factory=list)

    def to_draft(self) -> "IssueDraft":
        """Extract the draft fields as an ``IssueDraft`` for the creation path."""
        return IssueDraft(
            title=self.title or "(untitled)",
            description=self.description or "(no description)",
            severity=self.severity,
            suggested_labels=self.suggested_labels,
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
