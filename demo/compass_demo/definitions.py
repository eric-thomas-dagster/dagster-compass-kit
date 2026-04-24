"""Minimal Dagster+ project that demonstrates dagster-compass-kit.

Four assets in an upstream/downstream graph:

    orders ──┬─> orders_by_day ──┐
             │                    ├─> daily_summary
             └─> orders_by_region ┘

The root asset (``orders``) always raises. This project *is* the
cascade-alert demo — every materialization produces exactly the failure
scenario the kit is designed to handle. The three downstream assets
get skipped, and the cascade sensor classifies them.

Just click Materialize. No run tags, no config, no toggles.
"""

from dagster import Definitions, asset

from dagster_compass_kit import (
    CompassResource,
    compass_classify_cascade_on_failure,
)


DEMO_GROUP = "compass_demo"


@asset(group_name=DEMO_GROUP, compute_kind="python")
def orders() -> int:
    """Simulated source that's unavailable — always raises.

    This is the cascade-demo root. Compass should classify this asset
    as the root cause; the three downstream assets as cascade.
    """
    raise RuntimeError(
        "Simulated Snowflake outage (demo). Downstream assets will be "
        "skipped. Compass should flag this asset as root cause and the "
        "downstreams as cascade."
    )


@asset(deps=[orders], group_name=DEMO_GROUP, compute_kind="python")
def orders_by_day() -> int:
    return 7


@asset(deps=[orders], group_name=DEMO_GROUP, compute_kind="python")
def orders_by_region() -> int:
    return 3


@asset(
    deps=[orders_by_day, orders_by_region],
    group_name=DEMO_GROUP,
    compute_kind="python",
)
def daily_summary() -> dict:
    return {"status": "ok"}


defs = Definitions(
    assets=[orders, orders_by_day, orders_by_region, daily_summary],
    sensors=[compass_classify_cascade_on_failure()],
    resources={
        # from_env() tolerates missing env vars — the code location loads
        # even without DAGSTER_CLOUD_URL / DAGSTER_CLOUD_API_TOKEN set.
        # CompassNotConfiguredError fires at call time if anything tries
        # to reach Compass with empty creds; kit surfaces handle it.
        "compass": CompassResource.from_env(),
    },
)
