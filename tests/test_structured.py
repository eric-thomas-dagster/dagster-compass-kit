"""Parser tests for structured Compass responses."""

import pytest
from pydantic import BaseModel

from dagster_compass_kit import (
    AnomalyVerdict,
    CompassSchemaError,
    MonitoringDecision,
    build_structured_prompt,
    extract_json,
    parse_structured,
)


class _Simple(BaseModel):
    a: int
    b: str


def test_extract_plain_json():
    assert extract_json('{"a": 1, "b": "x"}') == {"a": 1, "b": "x"}


def test_extract_markdown_fenced():
    assert extract_json('```json\n{"a": 2, "b": "y"}\n```') == {"a": 2, "b": "y"}


def test_extract_prose_wrapped():
    text = 'Here you go:\n\n{"a": 3, "b": "z"}\n\nHope that helps!'
    assert extract_json(text) == {"a": 3, "b": "z"}


def test_extract_garbage_returns_none():
    assert extract_json("i don't know") is None
    assert extract_json("") is None


def test_parse_structured_validates():
    obj = parse_structured('{"a": 1, "b": "x"}', _Simple)
    assert obj.a == 1 and obj.b == "x"


def test_parse_structured_raises_on_bad_json():
    with pytest.raises(CompassSchemaError) as exc:
        parse_structured("not json", _Simple)
    assert "parseable" in str(exc.value)


def test_parse_structured_raises_on_schema_mismatch():
    with pytest.raises(CompassSchemaError) as exc:
        parse_structured('{"a": "not_an_int", "b": "x"}', _Simple)
    assert "validation" in str(exc.value).lower()


def test_build_structured_prompt_includes_schema():
    prompt = build_structured_prompt("Is this thing anomalous?", AnomalyVerdict)
    assert "Is this thing anomalous?" in prompt
    assert "is_anomaly" in prompt
    assert "severity" in prompt
    # Should discourage markdown fencing
    assert "no markdown fences" in prompt


def test_anomaly_verdict_defaults():
    v = AnomalyVerdict(is_anomaly=False, severity="none", explanation="fine")
    assert v.similar_events_recently == 0


def test_monitoring_decision_defaults():
    d = MonitoringDecision(should_trigger=False, reason="all good")
    assert d.severity == "info"
    assert d.asset_key_paths == []
