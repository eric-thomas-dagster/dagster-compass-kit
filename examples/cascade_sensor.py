"""Alert-fatigue mitigation: classify root-cause vs cascade on run failure.

When this sensor fires on a failed run, Compass splits the affected
assets into root causes and downstream cascades. An AssetObservation is
emitted on each affected asset carrying the classification as event
metadata — ``compass_root_cause: true|false``.

Alert-policy side (configure in Dagster+):

  1. Create an AlertPolicy on "Asset observation" events.
  2. Filter the event metadata to ``compass_root_cause == true``.
  3. Route to PagerDuty / Slack as you normally would.

Result: an upstream Snowflake outage that knocks over 40 downstreams
now pages 1 time (the root cause), not 40.

Baseline asset-materialization-failure alerts can stay for belt-and-
suspenders, but the root-cause-filtered observation alert should be
the primary on-call surface.
"""

from dagster import Definitions, EnvVar

from dagster_compass_kit import CompassResource, compass_classify_cascade_on_failure

defs = Definitions(
    sensors=[compass_classify_cascade_on_failure()],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
