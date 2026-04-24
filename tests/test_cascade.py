"""Smoke tests for the cascade-classification surface.

The deterministic classifier is covered exhaustively here. Real Compass
enrichment is exercised in the integration suite against a live tenant —
these tests verify shape, topology rules, and the graceful-fallback paths.
"""

import pytest
from dagster import (
    AssetCheckEvaluation,
    AssetCheckSeverity,
    AssetKey,
    DagsterInstance,
    Definitions,
    MetadataValue,
    asset,
)
from pydantic import ValidationError

from dagster_compass_kit import (
    CascadeDiagnosis,
    classify_cascade_deterministic,
    classify_cascade_for_run,
    compass_classify_cascade_on_failure,
    enrich_explanation_via_compass,
)
from dagster_compass_kit.cascade import _emit_classification_check_evaluations


# ── Schema smoke ────────────────────────────────────────────────────────────


def test_cascade_diagnosis_defaults():
    d = CascadeDiagnosis(
        root_cause_asset_keys=["analytics/orders"],
        explanation="Snowflake auth expired.",
    )
    assert d.cascade_asset_keys == []
    assert d.root_cause_asset_keys == ["analytics/orders"]


def test_cascade_diagnosis_requires_root_cause_keys():
    with pytest.raises(ValidationError):
        CascadeDiagnosis(explanation="x")


# ── Surface smoke ───────────────────────────────────────────────────────────


def test_exports_are_importable():
    assert callable(classify_cascade_for_run)
    assert callable(classify_cascade_deterministic)
    assert callable(enrich_explanation_via_compass)


def test_sensor_factory_returns_sensor():
    sensor = compass_classify_cascade_on_failure()
    assert hasattr(sensor, "name")
    assert sensor.name == "compass_cascade_classifier"


def test_sensor_factory_accepts_custom_check_name_and_metadata_key():
    sensor = compass_classify_cascade_on_failure(
        name="my_cascade",
        check_name="my_check",
        cascade_metadata_key="suppressed_by",
    )
    assert sensor.name == "my_cascade"


# ── Deterministic classifier — topology rules ───────────────────────────────


def _asset_graph(assets_defs):
    """Helper: build an asset graph from a list of @asset-decorated functions."""
    return Definitions(assets=assets_defs).get_repository_def().asset_graph


def test_deterministic_returns_none_when_nothing_affected():
    @asset
    def a() -> int:
        return 1

    ag = _asset_graph([a])
    planned = {AssetKey(["a"])}
    materialized = {AssetKey(["a"])}
    assert (
        classify_cascade_deterministic(
            planned=planned, materialized=materialized, asset_graph=ag
        )
        is None
    )


def test_deterministic_linear_cascade_single_root():
    """a -> b -> c -> d, a fails. Expect 1 root, 3 cascade."""

    @asset
    def a() -> int:
        return 1

    @asset(deps=[a])
    def b() -> int:
        return 2

    @asset(deps=[b])
    def c() -> int:
        return 3

    @asset(deps=[c])
    def d() -> int:
        return 4

    ag = _asset_graph([a, b, c, d])
    planned = {AssetKey([k]) for k in "abcd"}
    materialized: set[AssetKey] = set()

    diagnosis = classify_cascade_deterministic(
        planned=planned, materialized=materialized, asset_graph=ag
    )
    assert diagnosis is not None
    assert diagnosis.root_cause_asset_keys == ["a"]
    assert sorted(diagnosis.cascade_asset_keys) == ["b", "c", "d"]


def test_deterministic_multiple_independent_roots():
    """a -> c, b -> c; both a and b fail. Expect 2 roots, 1 cascade."""

    @asset
    def a() -> int:
        return 1

    @asset
    def b() -> int:
        return 2

    @asset(deps=[a, b])
    def c() -> int:
        return 3

    ag = _asset_graph([a, b, c])
    planned = {AssetKey(["a"]), AssetKey(["b"]), AssetKey(["c"])}
    materialized: set[AssetKey] = set()

    diagnosis = classify_cascade_deterministic(
        planned=planned, materialized=materialized, asset_graph=ag
    )
    assert diagnosis is not None
    assert sorted(diagnosis.root_cause_asset_keys) == ["a", "b"]
    assert diagnosis.cascade_asset_keys == ["c"]


def test_deterministic_one_of_two_parallel_upstreams_fails():
    """a -> c, b -> c; a fails, b succeeds -> c skipped. Expect 1 root (a), 1 cascade (c)."""

    @asset
    def a() -> int:
        return 1

    @asset
    def b() -> int:
        return 2

    @asset(deps=[a, b])
    def c() -> int:
        return 3

    ag = _asset_graph([a, b, c])
    planned = {AssetKey(["a"]), AssetKey(["b"]), AssetKey(["c"])}
    materialized = {AssetKey(["b"])}

    diagnosis = classify_cascade_deterministic(
        planned=planned, materialized=materialized, asset_graph=ag
    )
    assert diagnosis is not None
    assert diagnosis.root_cause_asset_keys == ["a"]
    assert diagnosis.cascade_asset_keys == ["c"]


def test_deterministic_explanation_mentions_root_when_single():
    @asset
    def orders() -> int:
        return 1

    @asset(deps=[orders])
    def downstream() -> int:
        return 2

    ag = _asset_graph([orders, downstream])
    diagnosis = classify_cascade_deterministic(
        planned={AssetKey(["orders"]), AssetKey(["downstream"])},
        materialized=set(),
        asset_graph=ag,
    )
    assert diagnosis is not None
    assert "orders" in diagnosis.explanation


# ── Observation emission ────────────────────────────────────────────────────


def test_emit_classification_check_evaluations_writes_one_eval_per_asset():
    instance = DagsterInstance.ephemeral()
    diagnosis = CascadeDiagnosis(
        root_cause_asset_keys=["analytics/orders"],
        cascade_asset_keys=["analytics/orders_by_day", "mart/finance_daily"],
        explanation="Snowflake auth expired mid-run.",
    )

    captured: list[AssetCheckEvaluation] = []
    original = instance.report_runless_asset_event

    def _capture(event):
        captured.append(event)
        return original(event)

    instance.report_runless_asset_event = _capture  # type: ignore[method-assign]

    root, cascade = _emit_classification_check_evaluations(
        instance,
        diagnosis,
        run_id="run-abc",
        chat_id=12345,
        check_name="compass_root_cause_detected",
        cascade_metadata_key="compass_cascade_of",
    )
    assert root == 1
    assert cascade == 2
    assert len(captured) == 3

    root_eval = next(
        e for e in captured if e.asset_key == AssetKey(["analytics", "orders"])
    )
    cascade_eval = next(
        e for e in captured if e.asset_key == AssetKey(["analytics", "orders_by_day"])
    )

    # Root cause: failed check. This is the event the AlertPolicy pages on.
    assert root_eval.passed is False
    assert root_eval.severity == AssetCheckSeverity.ERROR
    assert root_eval.check_name == "compass_root_cause_detected"
    assert root_eval.metadata["compass_chat_id"] == MetadataValue.int(12345)

    # Cascade: passed check, WARN severity. Recorded but doesn't page.
    assert cascade_eval.passed is True
    assert cascade_eval.severity == AssetCheckSeverity.WARN
    assert cascade_eval.check_name == "compass_root_cause_detected"
    assert cascade_eval.metadata["compass_cascade_of"] == MetadataValue.text(
        "analytics/orders"
    )


def test_emit_classification_with_no_chat_id_omits_field():
    instance = DagsterInstance.ephemeral()
    diagnosis = CascadeDiagnosis(
        root_cause_asset_keys=["x/y"],
        explanation="…",
    )

    captured: list[AssetCheckEvaluation] = []
    instance.report_runless_asset_event = lambda e: captured.append(e)  # type: ignore[method-assign]

    _emit_classification_check_evaluations(
        instance,
        diagnosis,
        run_id="run-abc",
        chat_id=None,
        check_name="compass_root_cause_detected",
        cascade_metadata_key="compass_cascade_of",
    )
    assert len(captured) == 1
    assert "compass_chat_id" not in captured[0].metadata
