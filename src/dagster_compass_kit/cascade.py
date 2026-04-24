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

from typing import Any, Optional

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
from .resource import CompassNotConfiguredError, CompassResource


_ENRICH_PROMPT_TEMPLATE = """\
A Dagster run just failed. I've already classified the affected assets
deterministically from the asset graph:

Root cause asset(s) (failed on their own, nothing upstream failed):
{root_cause_assets}

Cascade asset(s) (didn't run because an upstream failed):
{cascade_assets}

The step-level errors that fired were:

{step_errors}

Please write ONE SENTENCE summarizing what happened and why, suitable
for a Dagster+ asset observation alert. Mention the root cause asset(s)
by name, what went wrong, and (if relevant) that N downstream assets
were affected. No preamble, no bullet points — just the sentence.
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


def classify_cascade_deterministic(
    *,
    planned: set[AssetKey],
    materialized: set[AssetKey],
    asset_graph: Any,
) -> Optional[CascadeDiagnosis]:
    """Pure-graph classifier — no Compass, 100% accurate.

    An affected asset (planned but not materialized) is classified as:

    - **CASCADE** if at least one of its declared upstream assets is
      also in the affected set. It didn't run because something
      upstream failed.
    - **ROOT CAUSE** otherwise. It failed on its own — all its declared
      upstreams either materialized successfully or weren't part of
      this run.

    This rule is deterministic and exact for the alerting use case,
    because Dagster skips downstream assets when an upstream step
    fails; it does not attempt them. There is no ambiguity.

    Returns ``None`` if no assets were affected (i.e. the run failed
    for some non-asset reason — nothing to classify).

    The ``explanation`` field is a generic one-liner. Pass the result
    through :func:`enrich_explanation_via_compass` for a
    Compass-generated narrative that references the actual failure.
    """
    affected = planned - materialized
    if not affected:
        return None

    roots: list[str] = []
    cascades: list[str] = []
    for key in sorted(affected, key=lambda k: k.to_user_string()):
        try:
            parents: set[AssetKey] = set(asset_graph.get(key).parent_keys)
        except KeyError:
            # Asset not in the graph anymore (removed? external?). Treat as root.
            parents = set()
        if parents & affected:
            cascades.append(key.to_user_string())
        else:
            roots.append(key.to_user_string())

    if len(roots) == 1:
        summary = f"Deterministic classification: {roots[0]} is the root cause"
    else:
        summary = f"Deterministic classification: {len(roots)} root cause(s)"
    if cascades:
        summary += f"; {len(cascades)} downstream asset(s) did not run."
    else:
        summary += "."

    return CascadeDiagnosis(
        root_cause_asset_keys=roots,
        cascade_asset_keys=cascades,
        explanation=summary,
    )


def enrich_explanation_via_compass(
    compass: CompassResource,
    diagnosis: CascadeDiagnosis,
    *,
    step_errors: list[str],
) -> tuple[CascadeDiagnosis, Optional[int]]:
    """Replace the deterministic explanation with a Compass-generated narrative.

    The classification itself (root vs cascade) is not changed —
    deterministic is authoritative. This only rewrites ``explanation``.

    Raises the underlying Compass exception (``CompassNotConfiguredError``
    or otherwise) on any failure. Callers typically catch-and-continue
    to keep the deterministic explanation.
    """
    prompt = _ENRICH_PROMPT_TEMPLATE.format(
        root_cause_assets="\n".join(
            f"- {k}" for k in diagnosis.root_cause_asset_keys
        ) or "(none)",
        cascade_assets="\n".join(
            f"- {k}" for k in diagnosis.cascade_asset_keys
        ) or "(none)",
        step_errors="\n".join(step_errors) or "(none captured)",
    )
    with compass.conversation() as chat:
        response = chat.ask(prompt)
        if response.error or not response.text:
            raise RuntimeError(
                f"Compass returned no explanation: {response.error or 'empty'}"
            )
        enriched_text = response.text.strip()[:500]
        enriched = diagnosis.model_copy(update={"explanation": enriched_text})
        return enriched, (chat.chat_id or None)


def classify_cascade_for_run(
    *,
    run_id: str,
    instance: Any,
    asset_graph: Any,
    compass: Optional[CompassResource] = None,
) -> tuple[Optional[CascadeDiagnosis], Optional[int]]:
    """Classify a failed run's affected assets as root-cause vs cascade.

    Deterministic-first: the classification itself is always done by
    :func:`classify_cascade_deterministic` (no LLM, 100% accurate). If
    a ``compass`` resource is provided *and* is configured, the
    deterministic explanation is replaced with a Compass-generated
    narrative. Any Compass failure is swallowed and the deterministic
    explanation is kept.

    Returns ``(diagnosis, chat_id)``. ``diagnosis`` is ``None`` only
    when no assets were affected (nothing to classify). ``chat_id`` is
    ``None`` unless Compass enrichment succeeded.
    """
    planned, materialized, step_errors = _extract_asset_events(instance, run_id)
    diagnosis = classify_cascade_deterministic(
        planned=planned,
        materialized=materialized,
        asset_graph=asset_graph,
    )
    if diagnosis is None:
        return None, None

    if compass is None:
        return diagnosis, None

    try:
        return enrich_explanation_via_compass(
            compass, diagnosis, step_errors=step_errors
        )
    except CompassNotConfiguredError:
        # Signal distinctly so the sensor can log the not-configured case.
        raise
    except Exception:  # noqa: BLE001 - AI outage must never mask the deterministic answer
        return diagnosis, None


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

    **Resource wiring:** the Compass resource must be registered under
    the key ``compass`` in ``Definitions.resources``. Modern Dagster
    sensors bind resources by parameter name, and ``run_failure_sensor``
    doesn't expose a configurable ``required_resource_keys`` kwarg — so
    the key is fixed. If you need a different key, build your own
    sensor with :func:`classify_cascade_for_run`.

    Args:
        name: Sensor name as it appears in Dagster+.
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
    def _sensor(context: RunFailureSensorContext, compass: CompassResource):
        run = context.dagster_run
        run_id = run.run_id
        instance = context.instance
        asset_graph = context.repository_def.asset_graph if context.repository_def else None

        if asset_graph is None:
            context.log.warning(
                f"compass_cascade_classifier: no asset graph available for "
                f"run {run_id}; cannot classify."
            )
            return SkipReason("Asset graph unavailable.")

        try:
            diagnosis, chat_id = classify_cascade_for_run(
                run_id=run_id,
                instance=instance,
                asset_graph=asset_graph,
                compass=compass,
            )
        except CompassNotConfiguredError:
            # Compass env vars unset — fall back to deterministic-only.
            context.log.info(
                f"compass_cascade_classifier: Compass not configured; "
                f"using deterministic classifier only for run {run_id}."
            )
            diagnosis, chat_id = classify_cascade_for_run(
                run_id=run_id,
                instance=instance,
                asset_graph=asset_graph,
                compass=None,
            )

        if diagnosis is None:
            context.log.info(
                f"compass_cascade_classifier: no affected assets for run "
                f"{run_id}; nothing to classify."
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
