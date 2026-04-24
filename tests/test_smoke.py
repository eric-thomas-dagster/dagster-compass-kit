"""Smoke-level tests that don't hit a real Compass.

Exhaustive behavioral testing requires a live Dagster+ deployment with
Compass enabled — those belong in an integration suite. These tests just
verify the package imports cleanly and the helper types are well-formed.
"""

import pytest

from dagster_compass_kit import (
    CompassNotConfiguredError,
    CompassResource,
    CompassResponse,
    ToolCall,
    compass_on_failure,
)
from dagster_compass_kit.client import _http_to_ws


def test_public_exports_importable():
    # Just proves the package layout is sane
    assert CompassResource is not None
    assert callable(compass_on_failure)
    assert CompassResponse is not None
    assert ToolCall is not None


def test_compass_response_defaults():
    r = CompassResponse()
    assert r.text == ""
    assert r.tool_calls == []
    assert r.suggested_replies == []
    assert r.chat_id is None
    assert r.error is None


def test_http_to_ws_substitution():
    assert _http_to_ws("https://org.dagster.cloud/dep/graphql") == "wss://org.dagster.cloud/dep/graphql"
    assert _http_to_ws("http://localhost:3000/graphql") == "ws://localhost:3000/graphql"


def test_tool_call_shape():
    t = ToolCall(tool_type="TOOL_TYPE_RUN_SQL_QUERY", tool_id="x")
    assert t.input_json == ""
    assert t.error is None


def test_hook_is_callable_and_returns_hook():
    hook = compass_on_failure()
    assert callable(hook)


def test_compass_resource_loads_without_env_vars():
    """Empty defaults let the code location load even if env vars are unset."""
    r = CompassResource()
    assert r.dagster_cloud_url == ""
    assert r.api_token == ""


def test_compass_resource_raises_at_call_time_if_unconfigured():
    """Calling Compass without config raises — but the resource itself loads fine."""
    r = CompassResource()
    with pytest.raises(CompassNotConfiguredError):
        r.ask("hello")


def test_compass_resource_error_names_missing_env_vars():
    """The error message should tell the user exactly what to set."""
    r = CompassResource(dagster_cloud_url="https://x/graphql")  # token still empty
    with pytest.raises(CompassNotConfiguredError, match="DAGSTER_CLOUD_API_TOKEN"):
        r.ask("hello")
