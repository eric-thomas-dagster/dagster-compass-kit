"""Smoke tests for the Dagster+ Issues integration.

End-to-end creation is exercised against a live tenant in the
integration suite (TODO). These tests verify the module imports, the
schema is well-formed, and the CLI-wrapper error path is graceful.
"""

from dagster_compass_kit import IssueDraft, compass_create_issue_on_failure, draft_issue_for_failure
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
