"""Compass-powered sensor: autonomous health watcher.

Every 5 minutes, ask Compass if anything is broken that would benefit from
launching the remediation job. Compass decides based on live Dagster+
operational data. When it says yes, the sensor yields a RunRequest with
tags capturing the severity + reason so humans can audit every AI-triggered
launch.
"""

from dagster import Definitions, EnvVar, job, op

from dagster_compass_kit import CompassResource, MonitoringDecision, compass_sensor


@op
def remediate():
    ...  # your remediation logic


@job
def remediation_job():
    remediate()


health_watcher = compass_sensor(
    name="compass_health_watcher",
    job=remediation_job,
    prompt=(
        "Is anything critically broken in this deployment right now that would "
        "benefit from running the remediation job? Consider failed runs in the "
        "last 15 minutes and asset check failures with severity high or critical."
    ),
    minimum_interval_seconds=300,
    # Only trigger for medium+ severity even if Compass says yes on low
    should_trigger=lambda d: d.severity in ("medium", "high", "critical"),
)


defs = Definitions(
    jobs=[remediation_job],
    sensors=[health_watcher],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
