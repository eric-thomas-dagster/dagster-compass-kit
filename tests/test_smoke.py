"""Smoke-level tests that don't hit a real Compass.

Exhaustive behavioral testing requires a live Dagster+ deployment with
Compass enabled — those belong in an integration suite. These tests just
verify the package imports cleanly and the helper types are well-formed.
"""

from dagster_compass_kit import (
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
    hook = compass_on_failure(slack_channel="#x")
    assert callable(hook)
