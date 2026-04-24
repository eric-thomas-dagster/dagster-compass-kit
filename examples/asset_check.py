"""Compass-powered asset check: is this materialization anomalous?

Compass is asked to compare the latest materialization against recent
history for this deployment and returns a structured verdict. The check
renders in Dagster's UI alongside your other asset checks and will fire
alerts through your existing alerting policies.
"""

from dagster import Definitions, EnvVar, asset

from dagster_compass_kit import CompassResource, compass_asset_check


@asset
def orders_augmented() -> None:
    ...  # your real materialization logic


orders_anomaly_check = compass_asset_check(
    asset=orders_augmented,
    prompt=(
        "Based on recent materialization history for the asset '{asset_key}', "
        "is the latest materialization anomalous — unusual row count, unusually "
        "long runtime, or a different failure pattern than normal?"
    ),
    name="orders_anomaly_check",
)


defs = Definitions(
    assets=[orders_augmented],
    asset_checks=[orders_anomaly_check],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
