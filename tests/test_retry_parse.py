"""Parser tests for retry-advisor JSON extraction.

These don't hit a real Compass — they just verify we handle the common
response shapes an LLM produces: plain JSON, markdown-fenced JSON,
prose-wrapped JSON, malformed JSON.
"""

from dagster_compass_kit.retry import (
    ExceptionAnalysis,
    exception_fingerprint,
    parse_analysis,
)


def test_parse_plain_json():
    raw = '{"should_retry": true, "retry_after_seconds": 10, "category": "transient", "confidence": "high", "reason": "network blip", "similar_failures_recently": 4}'
    a = parse_analysis(raw)
    assert a.should_retry is True
    assert a.retry_after_seconds == 10
    assert a.category == "transient"
    assert a.confidence == "high"
    assert a.similar_failures_recently == 4


def test_parse_markdown_fenced():
    raw = '```json\n{"should_retry": false, "retry_after_seconds": 0, "category": "deterministic", "confidence": "high", "reason": "missing key", "similar_failures_recently": 0}\n```'
    a = parse_analysis(raw)
    assert a.should_retry is False
    assert a.category == "deterministic"


def test_parse_prose_wrapped():
    raw = 'Here is my analysis:\n\n{"should_retry": true, "retry_after_seconds": 5, "category": "transient", "confidence": "medium", "reason": "x", "similar_failures_recently": 1}\n\nLet me know if you need more.'
    a = parse_analysis(raw)
    assert a.should_retry is True


def test_parse_garbage_is_unknown():
    a = parse_analysis("i don't know")
    assert a.category == "unknown"
    assert a.should_retry is False


def test_parse_empty_is_unknown():
    a = parse_analysis("")
    assert a.category == "unknown"


def test_analysis_unknown_factory():
    a = ExceptionAnalysis.unknown("test reason")
    assert a.should_retry is False
    assert a.confidence == "low"
    assert a.reason == "test reason"


def test_fingerprint_stable():
    e1 = ValueError("foo")
    e2 = ValueError("foo")
    assert exception_fingerprint(e1) == exception_fingerprint(e2)
    e3 = ValueError("bar")
    assert exception_fingerprint(e1) != exception_fingerprint(e3)


def test_fingerprint_type_matters():
    e1 = ValueError("x")
    e2 = KeyError("x")
    assert exception_fingerprint(e1) != exception_fingerprint(e2)
