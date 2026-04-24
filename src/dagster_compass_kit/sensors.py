"""Compass-powered sensors.

On every tick, the sensor asks Compass a question, parses the structured
response, and emits ``RunRequest``s when Compass decides a run is warranted.
Turns Compass into an autonomous monitoring agent that watches your
deployment's operational state and triggers pipelines in response.

Latency caveat: Compass calls take 20–40s. Pick a tick interval comfortably
larger than that. The default here is 5 minutes.

Example — "every 5 minutes, check if anything critical is broken; if so,
launch the remediation job"::

    from dagster import job, op
    from dagster_compass_kit import compass_sensor, MonitoringDecision

    @op(required_resource_keys={"compass"})
    def remediate(): ...

    @job
    def remediation_job():
        remediate()

    health_watcher = compass_sensor(
        name="compass_health_watcher",
        job=remediation_job,
        prompt=(
            "Is anything broken in this deployment right now that would benefit "
            "from running the remediation job? Consider failed runs in the last "
            "15 minutes and high-severity asset-check failures."
        ),
        minimum_interval_seconds=300,
    )

    defs = Definitions(
        jobs=[remediation_job],
        sensors=[health_watcher],
        resources={"compass": CompassResource(...)},
    )
"""

from __future__ import annotations

from typing import Callable, Optional

from dagster import (
    DefaultSensorStatus,
    JobDefinition,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from .models import MonitoringDecision
from .resource import CompassResource
from .structured import CompassSchemaError

DecisionFilter = Callable[[MonitoringDecision], bool]


def compass_sensor(
    *,
    name: str,
    job: JobDefinition,
    prompt: str,
    resource_key: str = "compass",
    minimum_interval_seconds: int = 300,
    should_trigger: Optional[DecisionFilter] = None,
    default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
) -> SensorDefinition:
    """Build a sensor that consults Compass on each tick.

    Args:
        name: Sensor name.
        job: Job to launch when Compass says to trigger.
        prompt: Prompt asking Compass the yes/no question. Response is
            parsed against :class:`MonitoringDecision`.
        resource_key: Resource key holding the :class:`CompassResource`.
        minimum_interval_seconds: How often the sensor ticks. Defaults to
            5 minutes — don't go below ~60s given Compass's per-call
            latency.
        should_trigger: Optional secondary filter. If provided, the sensor
            only launches a run when both Compass's ``should_trigger`` AND
            this callable are true — useful for ignoring low-severity
            decisions or requiring minimum confidence.
        default_status: Sensor-default-status; new Dagster convention is
            STOPPED so operators opt in, override if you want it on by
            default.
    """

    @sensor(
        name=name,
        job=job,
        minimum_interval_seconds=minimum_interval_seconds,
        required_resource_keys={resource_key},
        default_status=default_status,
        description=f"Compass decides whether to launch '{job.name}'.",
    )
    def _compass_sensor(context: SensorEvaluationContext):
        compass: CompassResource = getattr(context.resources, resource_key)
        try:
            decision = compass.ask_structured(prompt, schema=MonitoringDecision)
        except CompassSchemaError as e:
            context.log.warning(f"compass_sensor: unparseable response: {e}")
            return SkipReason(f"Compass returned unparseable JSON: {e}")

        context.log.info(
            f"compass_sensor verdict: trigger={decision.should_trigger} "
            f"severity={decision.severity} reason={decision.reason!r}"
        )

        if not decision.should_trigger:
            return SkipReason(f"Compass: {decision.reason}")

        if should_trigger is not None and not should_trigger(decision):
            return SkipReason(f"Filtered out: Compass said trigger but filter rejected ({decision.reason})")

        return RunRequest(
            run_key=None,  # rely on Compass's per-tick judgment; no dedupe key
            tags={
                "compass/triggered": "true",
                "compass/severity": decision.severity,
                "compass/reason": decision.reason[:200],
            },
        )

    return _compass_sensor
