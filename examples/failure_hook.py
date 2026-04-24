"""A job whose failures are auto-summarized by Compass.

If the op fails, the hook calls Compass with the failing context, attaches
the summary to the run as metadata (tag). Dagster+'s existing Slack/email
alerts link to the run page where the Compass summary renders — no need
for this hook to send its own notification.
"""

from dagster import Definitions, EnvVar, job, op

from dagster_compass_kit import CompassResource, compass_on_failure


@op
def flaky_op():
    raise RuntimeError("something upstream is unhappy")


@job(hooks={compass_on_failure()})
def flaky_pipeline():
    flaky_op()


defs = Definitions(
    jobs=[flaky_pipeline],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
