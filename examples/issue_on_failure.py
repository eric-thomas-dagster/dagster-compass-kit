"""Auto-open a Dagster+ Issue on every op failure.

Dagster+ has a native Issues feature (see ``dg api issue``). This example
hooks into op failures, asks Compass to draft a title + description +
severity + suggested labels, and opens the issue via the ``dg`` CLI with
a link back to the failing run.

If ``dg`` isn't available or the creation fails, the Compass-generated
draft is still attached to the run as metadata so nothing is lost — the
operator can open it manually from the draft.
"""

from dagster import Definitions, EnvVar, job, op

from dagster_compass_kit import CompassResource, compass_create_issue_on_failure


@op
def risky_op():
    raise RuntimeError("something upstream broke")


@job(hooks={compass_create_issue_on_failure()})
def orders_pipeline():
    risky_op()


defs = Definitions(
    jobs=[orders_pipeline],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
