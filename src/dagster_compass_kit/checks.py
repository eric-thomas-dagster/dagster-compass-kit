"""Compass-powered asset checks.

Turn any natural-language "is this healthy?" question into a first-class
Dagster asset check that fails or warns based on Compass's structured verdict.
The check hooks into Dagster's regular asset-check surface — it appears in
the UI, contributes to asset health, triggers alerts via your existing
alerting policies.

Example::

    from dagster import asset
    from dagster_compass_kit import compass_asset_check

    @asset
    def orders_augmented(): ...

    orders_anomaly_check = compass_asset_check(
        asset=orders_augmented,
        prompt=(
            "Based on recent materialization history, is the latest materialization "
            "of the '{asset_key}' asset anomalous in row count or runtime?"
        ),
    )

    defs = Definitions(
        assets=[orders_augmented],
        asset_checks=[orders_anomaly_check],
        resources={"compass": CompassResource(...)},
    )
"""

from __future__ import annotations

from typing import Callable, Optional

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    AssetsDefinition,
    OpExecutionContext,
    multi_asset_check,
)

from .models import AnomalyVerdict
from .resource import CompassResource
from .structured import CompassSchemaError

SeverityMapper = Callable[[AnomalyVerdict], AssetCheckSeverity]


def _default_severity_mapper(v: AnomalyVerdict) -> AssetCheckSeverity:
    if v.severity in ("high",):
        return AssetCheckSeverity.ERROR
    return AssetCheckSeverity.WARN


def compass_asset_check(
    *,
    asset: AssetsDefinition | AssetKey,
    prompt: str,
    name: str = "compass_anomaly_check",
    resource_key: str = "compass",
    severity_mapper: Optional[SeverityMapper] = None,
    blocking: bool = False,
):
    """Build a Dagster asset check driven by Compass.

    Args:
        asset: The asset or asset key this check targets.
        prompt: Prompt template. Supports ``{asset_key}`` (slash-separated)
            and ``{asset_key_path}`` (list segments). Compass's response
            is parsed against :class:`AnomalyVerdict`.
        name: Dagster check name.
        resource_key: Resource key holding the :class:`CompassResource`.
        severity_mapper: Maps a verdict to a Dagster severity. Defaults to
            ``high → ERROR``, otherwise ``WARN``.
        blocking: If true, failing this check blocks downstream materializations.

    Returns:
        A Dagster asset-checks definition you can drop into
        ``Definitions(asset_checks=[...])``.
    """
    target_key = asset.key if isinstance(asset, AssetsDefinition) else asset
    mapper = severity_mapper or _default_severity_mapper

    @multi_asset_check(
        specs=[
            AssetCheckSpec(
                name=name,
                asset=target_key,
                description=f"Compass anomaly check: {prompt[:80]}…",
                blocking=blocking,
            )
        ],
        required_resource_keys={resource_key},
    )
    def _check(context: OpExecutionContext):
        compass: CompassResource = getattr(context.resources, resource_key)
        filled = prompt.format(
            asset_key="/".join(target_key.path),
            asset_key_path=target_key.path,
        )
        try:
            verdict = compass.ask_structured(filled, schema=AnomalyVerdict)
        except CompassSchemaError as e:
            yield AssetCheckResult(
                asset_key=target_key,
                check_name=name,
                passed=True,  # fail open — don't block on a broken Compass
                severity=AssetCheckSeverity.WARN,
                description=f"Compass response unparseable ({e}); check skipped.",
                metadata={"raw_response": e.raw_text[:500]},
            )
            return

        yield AssetCheckResult(
            asset_key=target_key,
            check_name=name,
            passed=not verdict.is_anomaly,
            severity=mapper(verdict),
            description=verdict.explanation,
            metadata={
                "severity": verdict.severity,
                "similar_events_recently": verdict.similar_events_recently,
            },
        )

    return _check
