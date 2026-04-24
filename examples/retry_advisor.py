"""Compass-powered retry advisor: ops that ask Compass whether a failure
is worth retrying, using recent run history + error patterns for this
deployment as context.

Classic use case: external-service flakes (Snowflake network blips,
Airbyte 502s) should retry; data-contract violations (``KeyError``,
``AssertionError``) should NOT. Static retry policies can't tell the
difference. Compass, with access to operational history, often can.

Run::

    export DAGSTER_CLOUD_URL='https://my-org.dagster.cloud/prod/graphql'
    export DAGSTER_CLOUD_API_TOKEN='user:...'
    dagster dev -m examples.retry_advisor
"""

import random

from dagster import Definitions, EnvVar, OpExecutionContext, job, op

from dagster_compass_kit import CompassResource, compass_retry_advisor


@op(required_resource_keys={"compass"})
@compass_retry_advisor(max_attempts=3)
def fetch_orders(context: OpExecutionContext):
    # Simulate either a transient network error or a deterministic bug.
    if random.random() < 0.5:
        raise ConnectionError("snowflake: network is unreachable")
    raise KeyError("customer_id")


@job
def orders_pipeline():
    fetch_orders()


defs = Definitions(
    jobs=[orders_pipeline],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
