"""Smoke tests for the Dagster+ Issues integration.

End-to-end creation is exercised against a live tenant in the
integration suite (TODO). These tests verify the module imports, the
schema is well-formed, and the CLI-wrapper error path is graceful.
"""

from dagster_compass_kit import (
    IssueActionPlan,
    IssueDraft,
    compass_create_issue_on_failure,
    draft_issue_for_failure,
    plan_issue_for_failure,
)
from dagster_compass_kit.issues import _create_via_dg_cli


def test_issue_draft_defaults():
    d = IssueDraft(title="t", description="d")
    assert d.severity == "medium"
    assert d.suggested_labels == []


def test_issue_draft_accepts_suggested_labels():
    d = IssueDraft(title="t", description="d", severity="high", suggested_labels=["oom", "transient"])
    assert d.severity == "high"
    assert "oom" in d.suggested_labels


def test_hook_is_callable_and_returns_hook():
    hook = compass_create_issue_on_failure()
    assert callable(hook)


def test_cli_wrapper_graceful_when_dg_missing(monkeypatch):
    """If ``dg`` isn't on PATH, we don't crash — we return (False, reason)."""
    import shutil

    # Force which() to report not-found
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    draft = IssueDraft(title="t", description="d")
    ok, msg = _create_via_dg_cli(draft, run_id="abc")
    assert ok is False
    assert "dg CLI" in msg


def test_draft_issue_for_failure_is_importable():
    # Proves the surface is callable; actual Compass call is exercised
    # against a live tenant in the integration suite.
    assert callable(draft_issue_for_failure)


def test_plan_issue_for_failure_is_importable():
    assert callable(plan_issue_for_failure)


def test_issue_action_plan_create_new_round_trip():
    plan = IssueActionPlan(
        action="create_new",
        reason="No matching open issue in last 24h.",
        title="Orders ETL: Snowflake auth expired",
        description="The job failed because…",
        severity="high",
        suggested_labels=["snowflake", "auth"],
    )
    draft = plan.to_draft()
    assert isinstance(draft, IssueDraft)
    assert draft.title.startswith("Orders ETL")
    assert draft.severity == "high"
    assert "snowflake" in draft.suggested_labels


def test_issue_action_plan_link_to_existing_omits_draft_fields():
    plan = IssueActionPlan(
        action="link_to_existing",
        reason="Issue-42 already covers this Snowflake outage.",
        existing_issue_public_id="issue-42",
    )
    assert plan.action == "link_to_existing"
    assert plan.existing_issue_public_id == "issue-42"
    assert plan.title is None
    assert plan.description is None
    # Defaulted draft fields shouldn't crash to_draft, but we shouldn't be
    # calling it on link_to_existing in practice; sanity-check the safety net.
    fallback = plan.to_draft()
    assert fallback.title == "(untitled)"


def test_issue_action_plan_skip():
    plan = IssueActionPlan(
        action="skip",
        reason="Already filed 8 issues for this transient Kafka error in last hour.",
    )
    assert plan.action == "skip"
    assert plan.existing_issue_public_id is None


def test_hook_accepts_dedup_kwargs():
    """The new dedup kwargs should be accepted without error."""
    h1 = compass_create_issue_on_failure(dedup=True, dedup_window_hours=12)
    h2 = compass_create_issue_on_failure(dedup=False)
    assert callable(h1)
    assert callable(h2)
