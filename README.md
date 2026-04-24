# dagster-compass-kit

A Dagster integration for **Dagster+ Compass** — resource, hooks, checks,
sensors, and components for using Compass inside your pipelines.

**The core idea:** Compass already answers questions in the web UI and in
Slack. This kit is about **persisting** those answers — turning them into
Dagster assets, run metadata, asset checks, auto-filed Dagster+ Issues,
and scheduled digests. Ephemeral AI output becomes a queryable, lineage-
aware, catalog-backed artifact.

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
| **`compass_on_failure()` hook** | On op failure, Compass writes a post-mortem and attaches it to run metadata. Dagster+'s existing Slack/email alerts link to the run page where it renders — this hook doesn't duplicate notifications. |
| **`compass_create_issue_on_failure()` hook** | On op failure, Compass first checks the open Issues queue and decides: open a new pre-filled issue (title + description + severity + labels), link the run to an existing open issue covering the same failure, or skip filing entirely if the queue is already noisy. Linked to the run + the Compass chat that drafted it. Dedup is on by default — flooding the queue with duplicates is the failure mode this hook is designed to *not* have. |
| **`compass_classify_cascade_on_failure()` sensor** | Alert-fatigue killer. On run failure, splits affected assets into `root_cause` vs `cascade` by walking the asset graph (deterministic, 100% accurate, no Compass call needed for classification). Emits an `AssetCheckEvaluation` named `compass_root_cause_detected` per affected asset: `passed=False` on roots (fires the alert), `passed=True` on cascades (recorded for dashboards, doesn't page). Compass, when configured, enriches the explanation with a one-sentence narrative. Configure one Dagster+ AlertPolicy targeting this check failure and a 40-asset cascade pages once, not 40 times. |
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

Not on PyPI. Install directly from GitHub:

```bash
pip install "dagster-compass-kit @ git+https://github.com/eric-thomas-dagster/dagster-compass-kit.git@main"

# Or pin to a tag:
pip install "dagster-compass-kit @ git+https://github.com/eric-thomas-dagster/dagster-compass-kit.git@v0.4.0"
```

For the optional YAML-components extra, append `[components]` after the package name (before the `@`): e.g. `dagster-compass-kit[components] @ git+https://…`.

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
from dagster import asset, Definitions, MaterializeResult, MetadataValue
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
        # Reads DAGSTER_CLOUD_URL + DAGSTER_CLOUD_API_TOKEN from the environment
        # with graceful fallback — code location loads even if they're unset,
        # and CompassNotConfiguredError fires at call time if anything actually
        # tries to reach Compass. Prefer this over EnvVar(...) in most cases.
        "compass": CompassResource.from_env(),
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

@job(hooks={compass_on_failure()})
def orders_pipeline():
    orders_etl()
```

On op failure, Compass gets the job name, run id, and failing step; writes a
summary grounded in recent history; attaches it as a run tag where Dagster+'s
existing Slack/email alerts link. No parallel notifications — the kit
enriches the run, the existing alert does the broadcasting.

## Quick start — auto-filed Dagster+ Issue (with dedup)

```python
from dagster import job
from dagster_compass_kit import compass_create_issue_on_failure

@job(hooks={compass_create_issue_on_failure()})  # dedup is on by default
def orders_pipeline():
    orders_etl()
```

On op failure Compass first checks the open Issues queue (last 24h by
default), then decides:

- **`create_new`** — genuinely new problem. Compass drafts the title +
  description + severity + labels and `createIssue` files it, linked to
  the run *and* to the Compass chat that drafted it.
- **`link_to_existing`** — there's already an open issue covering this
  failure. The run gets tagged with the existing issue's `publicId` so
  the dashboard correlates without filing a duplicate.
- **`skip`** — already too many of these in the recent window;
  Compass returns the reason and the run is tagged with it. No new
  ticket, no noise.

Tighten the dedup window for noisy jobs:

```python
compass_create_issue_on_failure(dedup_window_hours=2)
```

Or revert to the naive always-create behavior:

```python
compass_create_issue_on_failure(dedup=False)
```

Why dedup is the default: without it, a flaky job that fails 50× in a
morning produces 50 issues — the queue becomes useless within a day.
The Compass call adds ~30s per failure, but it's running in the failure
hook (off the hot path) and the alternative is a queue you stop reading.

## Quick start — kill the cascade alert spam

The default Dagster+ asset-failure alerting behavior is one alert per
affected asset. A Snowflake outage that trips a single upstream asset
and skips 40 downstream ones pages 40 times. The on-call stops reading.

```python
from dagster import Definitions
from dagster_compass_kit import CompassResource, compass_classify_cascade_on_failure

defs = Definitions(
    sensors=[compass_classify_cascade_on_failure()],
    resources={"compass": CompassResource.from_env()},
)
```

On every failed run the sensor:

1. Walks the event log for planned-but-unmaterialized assets.
2. **Classifies them deterministically** from the asset graph: any
   affected asset whose declared upstreams are all healthy is a root
   cause; anything downstream of a failed asset is cascade. 100%
   accurate, no LLM call.
3. If Compass is configured and reachable, asks for a one-sentence
   explanation of what happened and substitutes it into the check
   evaluation's metadata. If Compass is unavailable, a generic
   deterministic explanation is used.
4. Emits an `AssetCheckEvaluation` named `compass_root_cause_detected`
   per affected asset:
   - **Root cause** → `passed=False, severity=ERROR`. This is the
     event your AlertPolicy fires on.
   - **Cascade** → `passed=True, severity=WARN`. Lands on the asset's
     check history for post-mortems and dashboards but doesn't page.

   Metadata on every evaluation: `compass_explanation`,
   `compass_cascade_size`, `compass_run_id`, and `compass_chat_id` (when
   Compass was involved). Cascade-side evaluations also carry
   `compass_cascade_of` pointing back to the root.

Then, in Dagster+:

1. Create an AlertPolicy on **asset check failure** events.
2. Scope the asset selection to "any asset", match on check name
   `compass_root_cause_detected`.
3. Route as normal. Demote or remove the naive all-asset-failure alert.

The 40-asset cascade now pages **once** — on the root cause — with
Compass's one-sentence explanation attached to the check evaluation
metadata (which rides through to the alert payload). The other 39
evaluations land as passing checks (useful for dashboards, lineage
correlation, post-mortems) but don't page.

**Why asset checks, not observations.** Dagster+ AlertPolicies on
asset observations are aggregate-over-window only — no per-event
firing. AlertPolicies on asset check failures *are* per-event and
carry the evaluation's metadata in the alert. One holistic policy
targeting the check-name replaces asset-by-asset or job-by-job alert
plumbing.

Custom check name if `compass_root_cause_detected` collides with
something:

```python
compass_classify_cascade_on_failure(
    check_name="dagster_compass_kit/cascade_root_cause",
    cascade_metadata_key="suppressed_by_upstream",
)
```

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
  resource_key: compass
```

> **Why here and not in Slack?** Dagster+ Compass already has a Slack
> integration that generates daily digests straight into a channel. This
> kit exists for a different reason: **persistence**. The digest lands as
> a Dagster asset in your catalog with a full materialization history —
> queryable downstream, diffable week-over-week, composable with other
> assets (feed it into an exec summary, an incident retro, a quarterly
> review). A Slack message is ephemeral; a catalog asset is an artifact.

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

- `CompassResource(dagster_cloud_url="", api_token="", timeout_seconds=120)` — empty defaults; `CompassResource.from_env()` is the usual path.
- `compass_on_failure(prompt=, resource_key=, metadata_key=)`
- `compass_create_issue_on_failure(resource_key=, dedup=True, dedup_window_hours=24, create_via_graphql=True, create_via_cli=False, ...)`
- `compass_classify_cascade_on_failure(name="compass_cascade_classifier", check_name="compass_root_cause_detected", cascade_metadata_key="compass_cascade_of", monitored_jobs=None)`
- `compass_retry_advisor(max_attempts=3, on_parse_failure='raise', wait_cap_seconds=120, fallback_to_heuristic=False)`
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
- Structured responses ask Compass to answer naturally and end with a
  ``SUMMARY:`` section of ``FIELD: VALUE`` lines — the way you'd ask for a
  TL;DR. We parse that footer with regex; JSON-in-response is parsed too
  as a fallback. This sidesteps Compass's prompt-injection guardrails
  (see [ASSUMPTIONS.md §4b](ASSUMPTIONS.md)).

## Security

- API tokens are secrets — `CompassResource.from_env()` reads them from the
  environment (Dagster+ injects configured deployment secrets as real env vars).
  `EnvVar(...)` works too but fails the whole code location if unset; `from_env`
  tolerates unset and errors only at call time.
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
