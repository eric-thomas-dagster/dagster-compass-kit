# demo — minimal Dagster+ project for dagster-compass-kit

Lives at [`demo/`](./) inside the kit repo. Clone/copy this directory
on its own if you want to deploy it as a standalone code location.

Four assets in an upstream/downstream graph:

```
orders ──┬─> orders_by_day ──┐
         │                   ├─> daily_summary
         └─> orders_by_region ┘
```

The root asset (`orders`) always raises. That's intentional — this
project *is* the cascade-alert demo, so every materialization produces
exactly the failure scenario the kit's sensor is designed to handle.
No run tags, no config flags, no toggles. Click Materialize and the
cascade happens.

**Deployment target:** org `ericthomas-dagster`, deployment `prod`.

## Run locally first

```bash
cd demo
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Optional — CompassResource.from_env() tolerates missing vars.
# Set these only once your tenant has Compass enabled.
export DAGSTER_CLOUD_URL="https://ericthomas-dagster.dagster.cloud/prod/graphql"
export DAGSTER_CLOUD_API_TOKEN="…"   # user token with Editor role

dagster dev
```

Open `http://localhost:3000`, find the four assets in the `compass_demo`
group, and click **Materialize all**. The root asset fails on purpose;
the three downstream assets are skipped; the cascade sensor fires and
emits asset check evaluations.

## Deploy to Dagster+ serverless

Requires `dagster-cloud` CLI, authenticated as an org admin on
`ericthomas-dagster`.

```bash
dagster-cloud serverless deploy \
  --organization ericthomas-dagster \
  --deployment prod \
  --location-name compass_demo
```

That reads `dagster_cloud.yaml`, builds the code location, and
publishes it. After the build completes, the `compass_demo` location
appears in the `prod` deployment with the four assets and the cascade
classifier sensor.

## Set secrets

The `CompassResource` reads `DAGSTER_CLOUD_URL` and
`DAGSTER_CLOUD_API_TOKEN` from env vars. Configure them on the
deployment:

```bash
dagster-cloud deployment env-var set DAGSTER_CLOUD_URL \
  --value "https://ericthomas-dagster.dagster.cloud/prod/graphql" \
  --deployment prod

dagster-cloud deployment env-var set DAGSTER_CLOUD_API_TOKEN \
  --value "$DAGSTER_CLOUD_API_TOKEN" \
  --deployment prod
```

(The token needs permission to open Compass chats — a user or agent
token with Editor role on `prod` is sufficient.)

## Demo scenario

1. In the Dagster+ UI, open the `compass_demo` code location.
2. Start the `compass_cascade_classifier` sensor.
3. Materialize all four assets in the `compass_demo` group.
4. The `orders` step fails; the three downstream assets are skipped.
   (That's the point — the root is a deliberately broken demo source.)
5. Within a minute the cascade sensor fires. Each affected asset gets
   an `AssetCheckEvaluation` named `compass_root_cause_detected`:
   - `orders` → **FAILED**, severity=ERROR.
   - The three downstream assets → **PASSED**, severity=WARN.
   Metadata on each carries the explanation, run id, and cascade size.
6. In Dagster+, configure an AlertPolicy on asset check failures
   scoped to any asset selection you want monitored. Re-run the demo —
   exactly one alert fires, on `orders`, with the explanation in the
   alert payload. The three passing evaluations land silently on the
   downstream assets' check histories.

## Why this is the whole project

Everything else in the kit (auto-issue creation, retry advisor, daily
digests, runbook generator) is additional surface on top of the same
`CompassResource`. This demo keeps to the single most-impactful
behavior — alert-fatigue mitigation — so that the deployment footprint
is "four assets + one sensor + one resource" and the Compass
integration isn't competing with demo complexity for attention.
