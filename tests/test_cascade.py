"""Smoke tests for the cascade-classification surface.

Real behavior (Compass call + observation emission) is exercised against
a live Dagster+ tenant in the integration suite. These verify the shapes
and that the sensor factory returns something Dagster will accept.
"""

from dagster import AssetKey, AssetObservation, DagsterInstance, MetadataValue

from dagster_compass_kit import (
    CascadeDiagnosis,
    classify_cascade_for_run,
    compass_classify_cascade_on_failure,
)
from dagster_compass_kit.cascade import _emit_classification_observations


def test_cascade_diagnosis_defaults():
    d = CascadeDiagnosis(
        root_cause_asset_keys=["analytics/orders"],
        explanation="Snowflake auth expired.",
    )
    assert d.cascade_asset_keys == []
    assert d.root_cause_asset_keys == ["analytics/orders"]


def test_cascade_diagnosis_requires_root_cause_keys():
    """root_cause_asset_keys has no default — the caller (Compass) must provide it."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        CascadeDiagnosis(explanation="x")


def test_classify_cascade_for_run_is_importable():
    assert callable(classify_cascade_for_run)


def test_sensor_factory_returns_sensor():
    sensor = compass_classify_cascade_on_failure()
    # SensorDefinition / @run_failure_sensor objects have a `name` attribute
    assert hasattr(sensor, "name")
    assert sensor.name == "compass_cascade_classifier"


def test_sensor_factory_accepts_custom_keys_and_name():
    sensor = compass_classify_cascade_on_failure(
        name="my_cascade",
        resource_key="compass_prod",
        root_cause_metadata_key="is_root_cause",
        cascade_metadata_key="suppressed_by",
    )
    assert sensor.name == "my_cascade"


def test_emit_classification_observations_writes_one_obs_per_asset():
    """Happy-path: 1 root + 2 cascade → 3 AssetObservations reported to instance."""
    instance = DagsterInstance.ephemeral()
    diagnosis = CascadeDiagnosis(
        root_cause_asset_keys=["analytics/orders"],
        cascade_asset_keys=["analytics/orders_by_day", "mart/finance_daily"],
        explanation="Snowflake auth expired mid-run.",
    )

    captured: list[AssetObservation] = []
    original = instance.report_runless_asset_event

    def _capture(event):
        captured.append(event)
        return original(event)

    instance.report_runless_asset_event = _capture  # type: ignore[method-assign]

    root, cascade = _emit_classification_observations(
        instance,
        diagnosis,
        run_id="run-abc",
        chat_id=12345,
        root_cause_metadata_key="compass_root_cause",
        cascade_metadata_key="compass_cascade_of",
    )
    assert root == 1
    assert cascade == 2
    assert len(captured) == 3

    # Root-cause observation carries compass_root_cause=True; cascade carries False.
    root_obs = next(o for o in captured if o.asset_key == AssetKey(["analytics", "orders"]))
    cascade_obs = next(
        o for o in captured if o.asset_key == AssetKey(["analytics", "orders_by_day"])
    )
    assert root_obs.metadata["compass_root_cause"] == MetadataValue.bool(True)
    assert cascade_obs.metadata["compass_root_cause"] == MetadataValue.bool(False)
    assert cascade_obs.metadata["compass_cascade_of"] == MetadataValue.text(
        "analytics/orders"
    )
    # Chat id rides along on both
    assert root_obs.metadata["compass_chat_id"] == MetadataValue.int(12345)


def test_emit_classification_with_no_chat_id_omits_field():
    """Chat id is optional — observations should emit cleanly without it."""
    instance = DagsterInstance.ephemeral()
    diagnosis = CascadeDiagnosis(
        root_cause_asset_keys=["x/y"],
        explanation="…",
    )

    captured: list[AssetObservation] = []
    instance.report_runless_asset_event = lambda e: captured.append(e)  # type: ignore[method-assign]

    _emit_classification_observations(
        instance,
        diagnosis,
        run_id="run-abc",
        chat_id=None,
        root_cause_metadata_key="compass_root_cause",
        cascade_metadata_key="compass_cascade_of",
    )
    assert len(captured) == 1
    assert "compass_chat_id" not in captured[0].metadata
