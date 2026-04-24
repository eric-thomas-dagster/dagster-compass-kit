"""Alert-fatigue mitigation via Compass root-cause classification.

**Problem.** Dagster+'s default asset-failure alerts fire one alert per
affected asset. When an upstream asset fails, every downstream asset
that didn't get to run also raises an alert — a single Snowflake outage
becomes 40 pages at 3 AM. Customers stop reading.

**Fix.** When a run fails, ask Compass to split the affected assets into
two groups:

- ``root_cause_asset_keys`` — the genuine originating failure(s).
- ``cascade_asset_keys`` — downstreams that never ran; noise.

Then emit an ``AssetObservation`` on each affected asset carrying the
classification as event **metadata**. Dagster+ alert policies that
filter observation events on metadata can then fire only on root causes,
collapsing 40 pages into 1.

**Why observations and not run tags.** Run tags aren't a filter surface
for asset-alert policies. Asset event metadata is. Observations are the
native way to attach runtime metadata to an asset without pretending it
materialized.

**What this module does NOT do.** It doesn't send notifications itself.
Dagster+ alert policies keep owning routing — the kit just enriches the
observation stream so those policies can be precise.

Usage::

    from dagster import Definitions
    from dagster_compass_kit import compass_classify_cascade_on_failure

    defs = Definitions(
        sensors=[compass_classify_cascade_on_failure()],
        resources={"compass": CompassResource(...)},
        ...,
    )

Then, in Dagster+, configure an AlertPolicy on asset observation events
and filter the event metadata to ``compass_root_cause == true``.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

from dagster import (
    AssetKey,
    AssetObservation,
    DagsterEventType,
    MetadataValue,
    RunFailureSensorContext,
    SkipReason,
    run_failure_sensor,
)

from .models import CascadeDiagnosis
from .resource import CompassResource
from .structured import CompassSchemaError


_CASCADE_PROMPT_TEMPLATE = """\
A Dagster run just failed. It planned to materialize these assets:

{planned_assets}

Only these assets actually materialized successfully:

{materialized_assets}

So these assets either raised an error or were skipped because an
upstream dependency failed:

{failed_or_skipped_assets}

The step-level errors were:

{step_errors}

Please classify each unsuccessful asset as either ROOT_CAUSE or CASCADE:

- ROOT_CAUSE: the asset whose materialization raised the error, or the
  clear originating cause of the cascade.
- CASCADE: the asset didn't run because an upstream failed — it is
  downstream noise, not an independent cause.

Usually one root cause explains everything else. Occasionally a run has
two or more genuinely independent root causes (e.g. two parallel
branches both failed for different reasons). Err toward fewer
root causes — when in doubt, classify as CASCADE.

Use the asset keys exactly as shown above (slash-separated).
"""


def _extract_asset_events(
    instance: Any, run_id: str
) -> tuple[set[AssetKey], set[AssetKey], list[str]]:
    """Walk the run's event log. Returns (planned, materialized, step_error_lines)."""
    planned: set[AssetKey] = set()
    materialized: set[AssetKey] = set()
    step_errors: list[str] = []

    events = instance.all_logs(
        run_id,
        of_type={
            DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
            DagsterEventType.ASSET_MATERIALIZATION,
            DagsterEventType.STEP_FAILURE,
        },
    )
    for entry in events:
        event = entry.dagster_event
        if event is None:
            continue
        et = event.event_type
        if et == DagsterEventType.ASSET_MATERIALIZATION_PLANNED:
            key = getattr(event.event_specific_data, "asset_key", None)
            if key is not None:
                planned.add(key)
        elif et == DagsterEventType.ASSET_MATERIALIZATION:
            key = event.asset_key
            if key is not None:
                materialized.add(key)
        elif et == DagsterEventType.STEP_FAILURE:
            data = event.step_failure_data
            err = data.error if data else None
            if err is not None:
                cls_name = getattr(err, "cls_name", "(unknown)")
                msg = (getattr(err, "message", "") or "")[:200]
                step_errors.append(f"{event.step_key}: {cls_name}: {msg}")

    return planned, materialized, step_errors


def classify_cascade_for_run(
    compass: CompassResource,
    *,
    run_id: str,
    instance: Any,
) -> tuple[Optional[CascadeDiagnosis], Optional[int]]:
    """Ask Compass to classify a failed run's affected assets as root-or-cascade.

    Returns ``(diagnosis, chat_id)``. Either may be ``None`` on failure —
    same permissive pattern as :func:`draft_issue_for_failure`.
    """
    planned, materialized, step_errors = _extract_asset_events(instance, run_id)
    failed_or_skipped = planned - materialized

    if not failed_or_skipped:
        # Run failed but not via asset materialization — nothing to classify
        return None, None

    def _fmt(keys: Iterable[AssetKey]) -> str:
        return "\n".join(f"- {k.to_user_string()}" for k in sorted(keys, key=lambda x: x.to_user_string())) or "(none)"

    prompt = _CASCADE_PROMPT_TEMPLATE.format(
        planned_assets=_fmt(planned),
        materialized_assets=_fmt(materialized),
        failed_or_skipped_assets=_fmt(failed_or_skipped),
        step_errors="\n".join(step_errors) or "(none captured)",
    )

    try:
        with compass.conversation() as chat:
            try:
                diagnosis = chat.ask_structured(prompt, schema=CascadeDiagnosis)
                return diagnosis, (chat.chat_id or None)
            except CompassSchemaError:
                return None, (chat.chat_id or None)
    except Exception:  # noqa: BLE001 - AI outage must never mask the underlying run failure
        return None, None


def _emit_classification_observations(
    instance: Any,
    diagnosis: CascadeDiagnosis,
    *,
    run_id: str,
    chat_id: Optional[int],
    root_cause_metadata_key: str,
    cascade_metadata_key: str,
) -> tuple[int, int]:
    """Emit one AssetObservation per affected asset carrying the classification.

    Returns (root_count, cascade_count) actually emitted.
    """
    common: dict[str, Any] = {
        "compass_explanation": MetadataValue.text(diagnosis.explanation),
        "compass_cascade_size": MetadataValue.int(len(diagnosis.cascade_asset_keys)),
        "compass_run_id": MetadataValue.text(run_id),
    }
    if chat_id is not None:
        common["compass_chat_id"] = MetadataValue.int(int(chat_id))

    root_count = 0
    for key_str in diagnosis.root_cause_asset_keys:
        metadata = {
            **common,
            root_cause_metadata_key: MetadataValue.bool(True),
        }
        obs = AssetObservation(
            asset_key=AssetKey.from_user_string(key_str),
            description="Compass classified this asset as the root cause of a run failure.",
            metadata=metadata,
        )
        instance.report_runless_asset_event(obs)
        root_count += 1

    cascade_count = 0
    roots_joined = ",".join(diagnosis.root_cause_asset_keys)
    for key_str in diagnosis.cascade_asset_keys:
        metadata = {
            **common,
            root_cause_metadata_key: MetadataValue.bool(False),
            cascade_metadata_key: MetadataValue.text(roots_joined),
        }
        obs = AssetObservation(
            asset_key=AssetKey.from_user_string(key_str),
            description=(
                "Compass classified this asset as downstream of a failed upstream; "
                "it did not run independently."
            ),
            metadata=metadata,
        )
        instance.report_runless_asset_event(obs)
        cascade_count += 1

    return root_count, cascade_count


def compass_classify_cascade_on_failure(
    *,
    name: str = "compass_cascade_classifier",
    resource_key: str = "compass",
    root_cause_metadata_key: str = "compass_root_cause",
    cascade_metadata_key: str = "compass_cascade_of",
    monitored_jobs: Optional[list[Any]] = None,
):
    """Return a ``@run_failure_sensor`` that classifies and tags cascades.

    On every run failure:

    1. Walk the run's event log for planned assets vs materialized assets.
       The delta is the set of affected assets (failed outright or
       skipped due to upstream failure).
    2. Ask Compass to classify each affected asset as root-cause or
       cascade. Single ~30s Compass call per failed run.
    3. Emit an ``AssetObservation`` for each affected asset with
       classification metadata:

       - ``compass_root_cause`` (bool) — True for root causes, False for
         cascades. This is the field Dagster+ alert policies should
         filter on.
       - ``compass_cascade_of`` (text, cascade-side only) — comma-joined
         root cause asset keys for traceability.
       - ``compass_explanation``, ``compass_cascade_size``,
         ``compass_run_id``, ``compass_chat_id`` — for the on-call.

    **Downstream:** configure a Dagster+ AlertPolicy on
    AssetObservation events (not materialization-failure events) and
    filter event metadata ``compass_root_cause == true`` to route only
    root-cause alerts.

    Args:
        name: Sensor name as it appears in Dagster+.
        resource_key: Resource key holding the ``CompassResource``.
        root_cause_metadata_key: Metadata field name the alert policy
            will filter on. Override only if it collides with an
            existing metadata convention in your deployment.
        cascade_metadata_key: Metadata field name carrying the
            root-cause back-pointer on cascade observations.
        monitored_jobs: Restrict to specific jobs. ``None`` means all.
    """

    @run_failure_sensor(
        name=name,
        monitored_jobs=monitored_jobs,
    )
    def _sensor(context: RunFailureSensorContext):
        compass: CompassResource = getattr(context.resources, resource_key)
        run = context.dagster_run
        run_id = run.run_id
        instance = context.instance

        diagnosis, chat_id = classify_cascade_for_run(
            compass, run_id=run_id, instance=instance
        )

        if diagnosis is None:
            context.log.info(
                f"compass_cascade_classifier: no classification for run {run_id} "
                "(no affected assets, or Compass unavailable)"
            )
            return SkipReason("No cascade classification produced.")

        try:
            root_count, cascade_count = _emit_classification_observations(
                instance,
                diagnosis,
                run_id=run_id,
                chat_id=chat_id,
                root_cause_metadata_key=root_cause_metadata_key,
                cascade_metadata_key=cascade_metadata_key,
            )
        except Exception as e:  # noqa: BLE001
            context.log.warning(
                f"compass_cascade_classifier: failed to emit observations: {e!r}"
            )
            return SkipReason(f"Observation emit failed: {e!r}")

        context.log.info(
            f"compass_cascade_classifier: run {run_id} → "
            f"{root_count} root cause(s), {cascade_count} cascade asset(s). "
            f"{diagnosis.explanation}"
        )
        return SkipReason(
            f"Classified {root_count} root cause(s), {cascade_count} cascade."
        )

    return _sensor
