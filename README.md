# dagster-compass-kit

A Dagster integration for **Dagster+ Compass** — resource, hooks, checks,
sensors, and components for using Compass inside your pipelines.

> **Dagster+ Compass only.** This kit talks to the Compass embedded in Dagster+
> at `wss://<org>.dagster.cloud/<deployment>/graphql`, authenticated with a
> Dagster+ API token. It does **not** talk to standalone
> [compass.dagster.io](https://compass.dagster.io/). Compass queries live
> Dagster+ operational data (run history, materialization stats, check
> results) — it knows your pipelines, not arbitrary customer datasets.

A personal project by [Eric Thomas](https://github.com/eric-thomas-dagster). Not
an official Dagster Labs product.

## What you get

| Piece | Use it for |
| --- | --- |
| **`CompassResource`** | Ask Compass from any asset/op. `compass.ask(prompt)` returns prose; `compass.ask_structured(prompt, PydanticModel)` returns a typed object. |
| **`compass.conversation()`** | Multi-turn chat — `chat_id` is preserved across `chat.ask(...)` calls so follow-ups stay coherent. |
| **`compass_on_failure()` hook** | On op failure, Compass writes a post-mortem, attaches to run metadata, optionally posts to Slack. |
| **`@compass_retry_advisor()` decorator** | On exception, Compass decides whether to retry (transient) or terminate (deterministic). |
| **`compass_asset_check(...)`** | Natural-language asset checks — "is this materialization anomalous?" becomes a first-class Dagster check. |
| **`compass_sensor(...)`** | Autonomous monitoring — sensor asks Compass each tick whether to launch a job. |
| **`compass.generate_job_runbook(...)`** | Markdown runbook for a job, regenerated from latest run history. Persist as an asset, file, or Notion page. |
| **`DailyInsightDigest` component** | One-YAML-block daily digest — asset + schedule that materialize Compass's answer as markdown. |
| **`CompassClient`** | Dagster-free sync client. Same features, usable in scripts, FastAPI, notebooks, other orchestrators. |

## Why this vs. calling OpenAI / Claude directly?

You *could* wire raw OpenAI or Claude into a Dagster hook yourself. This kit
exists because for Dagster-domain questions specifically, Compass has
structural advantages that aren't worth reimplementing.

**Pick this kit when you want:**

| | dagster-compass-kit | Raw OpenAI / Claude |
| --- | --- | --- |
| Access to live Dagster+ operational data | ✅ Built-in — Compass queries `dagster_plus_runs` tables via tool use. No RAG pipeline to build. | ❌ You extract, embed, store, keep fresh. |
| AI infrastructure | ✅ Nothing to procure — Dagster+ subscription covers it. | ❌ Your API key, your rate limits, your cost budget, your vendor contract. |
| Compliance boundary | ✅ Data never leaves Dagster+. | ❌ Operational data flows to a third-party processor. |
| Tool use (SQL against warehouses, chart rendering) | ✅ Pre-wired. | ❌ You define tools, host execution, manage the tool-call loop. |
| Trust model alignment with the Dagster+ chat widget | ✅ Same AI. Same answers. One audit trail. | ❌ Inconsistent with what the customer sees in the Dagster+ UI. |

**Use raw OpenAI / Claude when you want:**

- **Non-Dagster-domain tasks** — summarizing emails, classifying support
  tickets, generating SQL from business specs. Compass is scoped to Dagster+
  operational data; it can't answer *"what does this JIRA ticket mean?"*
- **A specific model** — you can't pick Compass's underlying model. If your
  use case requires Claude Opus or GPT-4o specifically, use them directly.
- **Low latency** — Compass takes 20–40s per call due to agent tool use.
  Raw LLM calls are 3–5s. For interactive or high-frequency paths, use
  raw LLMs.
- **Custom system prompts at the infra level** — with Compass you're a
  guest in someone else's prompt.
- **OSS Dagster / air-gapped deployments** — this kit is Dagster+ only.

The short version: **pick this kit when the question is about Dagster+
itself; pick raw LLMs when the question is about anything else.**

## Install

```bash
pip install dagster-compass-kit
pip install 'dagster-compass-kit[slack]'       # + Slack posting
pip install 'dagster-compass-kit[components]'  # + dagster-components YAML support
```

## How `ask_structured` actually works

> Compass has safety guardrails that refuse "silent API endpoint" / JSON-only
> prompts — it detects them as prompt-injection attempts. This kit works
> around that by asking Compass to answer naturally AND end with a
> ``SUMMARY:`` section of ``FIELD: VALUE`` lines (the way you'd ask for a
> TL;DR). We parse that footer with regex; JSON anywhere in the response
> is parsed too as a fallback. See
> [ASSUMPTIONS.md §4b](ASSUMPTIONS.md#4b-compass-has-safety-guardrails-that-reject-silent-api-endpoint-phrasing)
> for the full finding.

## Quick start — resource + structured

```python
from dagster import asset, Definitions, EnvVar, MaterializeResult, MetadataValue
from pydantic import BaseModel
from dagster_compass_kit import CompassResource

class PipelineHealth(BaseModel):
    overall: str  # healthy | degraded | critical
    top_issues: list[str]
    trend: str

@asset
def health_snapshot(compass: CompassResource) -> MaterializeResult:
    verdict = compass.ask_structured(
        "Assess the overall pipeline health right now.",
        PipelineHealth,
    )
    return MaterializeResult(metadata={
        "overall": MetadataValue.text(verdict.overall),
        "trend": MetadataValue.text(verdict.trend),
        "top_issues": MetadataValue.json(verdict.top_issues),
    })

defs = Definitions(
    assets=[health_snapshot],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
```

## Quick start — multi-turn conversation

```python
with compass.conversation() as chat:
    verdict = chat.ask_structured("Assess pipeline health.", PipelineHealth)
    if verdict.top_issues:
        # Follow-up with full context from previous turn
        drill = chat.ask(f"For '{verdict.top_issues[0]}', walk me through root cause and fix.")
```

## Quick start — auto post-mortem on failure

```python
from dagster import job, op
from dagster_compass_kit import compass_on_failure

@op
def orders_etl(): ...

@job(hooks={compass_on_failure(slack_channel="#data-incidents")})
def orders_pipeline():
    orders_etl()
```

On op failure, Compass gets the job name, run id, and failing step; writes a
summary grounded in recent history; attaches it as run metadata and posts to
Slack. Silent-fails if Slack is down so a broken Slack never masks a real
pipeline failure.

## Quick start — Compass-decided retry

```python
from dagster import op
from dagster_compass_kit import compass_retry_advisor

@op(required_resource_keys={"compass"})
@compass_retry_advisor(max_attempts=3)
def fetch_orders(context):
    ...  # may raise
```

On exception, Compass classifies as transient / deterministic / data_quality /
dependency / unknown and advises retry vs. terminate. Transient errors raise
`RetryRequested` with Compass's suggested backoff; deterministic errors raise
`Failure` with the reason. Fails closed — broken Compass re-raises original
exception.

## Quick start — asset check

```python
from dagster_compass_kit import compass_asset_check

orders_anomaly_check = compass_asset_check(
    asset=orders_augmented,
    prompt="Is the latest materialization of '{asset_key}' anomalous in row count or runtime?",
)
```

Renders in Dagster's UI alongside your other checks; fires alerts through
existing alerting policies; fails open (warn-only) if Compass's response
isn't parseable.

## Quick start — autonomous sensor

```python
from dagster_compass_kit import compass_sensor

health_watcher = compass_sensor(
    name="compass_health_watcher",
    job=remediation_job,
    prompt="Is anything critical broken that needs remediation?",
    minimum_interval_seconds=300,
    should_trigger=lambda d: d.severity in ("medium", "high", "critical"),
)
```

Sensor ticks on your schedule, asks Compass, launches the job when Compass
says to. Runs are tagged with Compass's severity + reason for audit.

## Quick start — runbook generator

```python
@asset
def orders_runbook(compass: CompassResource) -> MaterializeResult:
    md = compass.generate_job_runbook(job_name="orders_pipeline")
    return MaterializeResult(metadata={"runbook": MetadataValue.md(md)})
```

Schedule daily/weekly and your runbooks stay current as the pipeline evolves.

## Quick start — component

```yaml
# components/daily_digest/component.yaml
type: dagster_compass_kit.DailyInsightDigest
attributes:
  asset_key: analytics/daily_compass_digest
  cron_schedule: "0 9 * * *"
  prompt: Summarize yesterday's pipeline activity as markdown.
  slack_channel: "#data-standup"
  resource_key: compass
```

## What Compass can and can't answer

Compass is an agentic SQL-over-operational-data LLM. Empirically:

- ✅ Runtime trends, retry patterns, slowest/busiest jobs, failure root causes,
  stale assets, asset-check failures, materialization history.
- ❌ Billing / credits / seat counts — those live in the billing system, not
  Compass's queryable surface. Don't ask; Compass will hallucinate or error.
- 🤔 Live in-flight state is slightly lagged — the operational tables aren't
  strictly real-time. For "what's running right now?" use Dagster's regular
  GraphQL.

## Dagster-free usage

Most features work from any Python context via `CompassClient`:

```python
from dagster_compass_kit import CompassClient
from pydantic import BaseModel

client = CompassClient(
    dagster_cloud_url="https://my-org.dagster.cloud/prod/graphql",
    api_token="user:...",
)

class X(BaseModel):
    is_broken: bool
    reason: str

x = client.ask_structured("Is our ETL broken right now?", X)

# Multi-turn
with client.conversation() as chat:
    chat.ask("...")
    chat.ask("...")
```

## Assumptions this kit makes

Compass is a hosted product we don't control. [ASSUMPTIONS.md](ASSUMPTIONS.md)
documents every empirical assumption we're relying on so you can sanity-check
them against your own deployment. Highlights:

- **Data freshness is now live** (was hourly in older builds of Compass).
- **Compass can classify exceptions without historical context** — the
  retry-advisor prompt explicitly tells Compass to fall back on general
  Python/library knowledge if no history exists, so brand-new jobs still work.
- **This kit fails closed** — if Compass errors or times out, the original
  exception is re-raised or the asset check fails open (WARN-only). An AI
  outage must never mask a real pipeline failure.
- **Opt-in static heuristic fallback** — pass `fallback_to_heuristic=True` to
  `@compass_retry_advisor` and we'll use a conservative exception-type table
  (`ConnectionError`/`TimeoutError` → retry; `KeyError`/`TypeError` → don't)
  if Compass isn't reachable. See `heuristic_classify()` in `retry.py`.

## Configuration reference

See module docstrings for full parameter detail. Highlights:

- `CompassResource(dagster_cloud_url=, api_token=, timeout_seconds=120)`
- `compass_on_failure(prompt=, slack_channel=, resource_key=, metadata_key=)`
- `compass_retry_advisor(max_attempts=3, on_parse_failure='raise', wait_cap_seconds=120)`
- `compass_asset_check(asset=, prompt=, severity_mapper=, blocking=False)`
- `compass_sensor(job=, prompt=, minimum_interval_seconds=300, should_trigger=, default_status=STOPPED)`

## How it works under the hood

- Direct WebSocket implementation of the legacy `subscriptions-transport-ws`
  protocol (which Dagster Cloud speaks) — no `gql` dependency.
- Bearer auth sent in both the HTTP upgrade headers and the `connection_init`
  payload for max compatibility.
- Streamed chunks (`DeltaTextBlock`, tool blocks, completion) assembled into
  a single `CompassResponse`. Direct chunk access via `stream_ai_chat(...)`
  for real-time consumers.
- Structured responses use a JSON-schema instruction footer plus a forgiving
  parser that handles ```` ```json ```` fences and prose-wrapped JSON.

## Security

- API tokens are secrets — wire through `EnvVar` or a secrets manager.
- Compass responses are markdown. If you render them outside Dagster's UI,
  allow-list URL schemes (today's responses don't contain links but that could
  change).
- This package never logs the token or frame contents.

## Limitations

- Synchronous `ask()` uses `asyncio.run()` internally; use `ask_async` inside
  an existing event loop.
- Per-call latency is 20–40 seconds. Don't use on fast, high-frequency paths.
- Legacy WebSocket subprotocol is deprecated upstream but is what Dagster
  Cloud currently speaks. If Dagster migrates to `graphql-ws`, this package
  will need a transport update.

## License

Apache 2.0.
