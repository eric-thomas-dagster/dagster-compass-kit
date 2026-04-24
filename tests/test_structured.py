"""Parser tests for structured Compass responses.

We parse prose-with-SUMMARY-footer as the primary path (matches Compass's
actual output), and fall back to JSON extraction if Compass happens to emit
that instead. Refusals raise CompassSchemaError.
"""

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
from dagster_compass_kit.structured import extract_signals


class _Simple(BaseModel):
    a: int
    b: str


# ── Prose + SUMMARY footer (primary path) ────────────────────────────────────


def test_monitoring_decision_from_prose_footer():
    sample = """
Lots of analysis about the deployment. I checked the runs table and the
alert history and there's a live failure on enriched_data right now.

verdict: yes, run the remediation job.

SUMMARY:
should_trigger: yes
severity: high
reason: enriched_data failed from FileNotFoundError
asset_key_paths: enriched_data, analytics/orders
"""
    r = parse_structured(sample, MonitoringDecision)
    assert r.should_trigger is True
    assert r.severity == "high"
    assert "FileNotFoundError" in r.reason
    assert r.asset_key_paths == ["enriched_data", "analytics/orders"]


def test_anomaly_verdict_from_prose_footer():
    sample = """
Row count is way off versus baseline.

SUMMARY:
is_anomaly: YES
severity: high
explanation: Row count dropped 40% vs baseline
similar_events_recently: 2
"""
    v = parse_structured(sample, AnomalyVerdict)
    assert v.is_anomaly is True
    assert v.severity == "high"
    assert v.similar_events_recently == 2


def test_signal_parsing_is_case_insensitive():
    sample = "analysis text\n\nSummary:\nSHOULD_TRIGGER: No\nReason: all clear\nSeverity: info"
    r = parse_structured(sample, MonitoringDecision)
    assert r.should_trigger is False
    assert r.severity == "info"


# ── JSON path (fallback / bonus when Compass does emit JSON) ────────────────


def test_parse_from_plain_json():
    raw = '{"should_trigger": false, "severity": "info", "reason": "fine"}'
    r = parse_structured(raw, MonitoringDecision)
    assert r.should_trigger is False


def test_parse_from_markdown_fenced_json():
    raw = '```json\n{"is_anomaly": true, "severity": "medium", "explanation": "ok"}\n```'
    v = parse_structured(raw, AnomalyVerdict)
    assert v.is_anomaly is True


# ── Refusals and garbage ─────────────────────────────────────────────────────


def test_refusal_raises():
    refusal = (
        "I'm a conversational assistant and won't respond as a silent API "
        "endpoint. Let me know if you'd like me to analyze the deployment!"
    )
    with pytest.raises(CompassSchemaError) as exc:
        parse_structured(refusal, MonitoringDecision)
    assert "SUMMARY" in str(exc.value) or "JSON" in str(exc.value)


def test_empty_raises():
    with pytest.raises(CompassSchemaError):
        parse_structured("", MonitoringDecision)


def test_summary_present_but_invalid_values():
    """SUMMARY section present but values don't match schema — pydantic rejects."""
    sample = """
SUMMARY:
is_anomaly: maybe_sometime
severity: extreme
explanation: ok
"""
    # 'maybe_sometime' → not a valid bool after coercion
    # 'extreme' → not a valid enum value for severity
    with pytest.raises(CompassSchemaError):
        parse_structured(sample, AnomalyVerdict)


# ── Prompt builder ───────────────────────────────────────────────────────────


def test_prompt_does_not_trigger_injection_detection():
    """Our prompt must not sound like 'act as a silent API endpoint' to Compass's
    safety guardrails. Compass rejects 'respond ONLY with JSON' / 'no prose'
    patterns.
    """
    prompt = build_structured_prompt("Is it broken?", AnomalyVerdict)
    assert "no prose" not in prompt.lower()
    assert "respond only" not in prompt.lower()
    assert "SUMMARY:" in prompt


def test_prompt_includes_field_hints():
    prompt = build_structured_prompt("Is it broken?", AnomalyVerdict)
    assert "IS_ANOMALY: YES/NO" in prompt
    # Severity enum values should be inlined
    assert "none" in prompt and "medium" in prompt and "high" in prompt


# ── Low-level extractors ─────────────────────────────────────────────────────


def test_extract_signals_prefers_summary_section():
    """If random 'severity:' strings appear in prose but a SUMMARY section exists,
    the SUMMARY values win.
    """
    text = """
Some prose mentioning severity: low earlier in the analysis.

SUMMARY:
is_anomaly: yes
severity: high
explanation: the summary is what counts
"""
    fields = extract_signals(text, AnomalyVerdict)
    assert fields is not None
    assert fields["severity"] == "high"


def test_extract_signals_returns_none_when_nothing_matches():
    assert extract_signals("just some prose with nothing useful", AnomalyVerdict) is None


def test_extract_json_still_works():
    assert extract_json('{"a": 1}') == {"a": 1}
    assert extract_json("garbage") is None
