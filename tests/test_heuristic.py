"""Tests for the static exception-type heuristic.

The heuristic is the fallback when Compass is unavailable AND the caller
opted into ``fallback_to_heuristic=True``. It has to be conservative —
only classify types whose semantics are unambiguous.
"""

import pytest

from dagster_compass_kit import heuristic_classify


@pytest.mark.parametrize(
    "exc,expected_category,expected_retry",
    [
        (ConnectionError("net blip"), "transient", True),
        (TimeoutError("slow"), "transient", True),
        (OSError("disk"), "transient", True),
        (ConnectionResetError(), "transient", True),
        (KeyError("customer_id"), "deterministic", False),
        (AttributeError("foo"), "deterministic", False),
        (TypeError("nope"), "deterministic", False),
        (ValueError("bad input"), "deterministic", False),
        (NameError("undefined"), "deterministic", False),
        (AssertionError("failed"), "deterministic", False),
        (ImportError("no mod"), "deterministic", False),
        (FileNotFoundError("missing"), "deterministic", False),
        (ZeroDivisionError(), "deterministic", False),
        (IndexError("out of range"), "deterministic", False),
    ],
)
def test_common_exception_types_classify(exc, expected_category, expected_retry):
    analysis = heuristic_classify(exc)
    assert analysis is not None, f"Expected {type(exc).__name__} to be known"
    assert analysis.category == expected_category
    assert analysis.should_retry == expected_retry


def test_permission_error_is_dependency():
    analysis = heuristic_classify(PermissionError("denied"))
    assert analysis is not None
    assert analysis.category == "dependency"


def test_unknown_exception_returns_none():
    class MyWeirdCustomError(Exception):
        pass

    assert heuristic_classify(MyWeirdCustomError("shrug")) is None


def test_heuristic_marks_source_as_heuristic():
    analysis = heuristic_classify(ConnectionError("x"))
    assert analysis is not None
    # The decorator uses raw_response="(heuristic)" as the source marker
    assert analysis.raw_response == "(heuristic)"


def test_heuristic_captures_exception_name_in_reason():
    analysis = heuristic_classify(TimeoutError("x"))
    assert analysis is not None
    assert "TimeoutError" in analysis.reason
