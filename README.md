# dagster-compass-kit

Community-supported Dagster integration for **Dagster+ Compass** — a resource,
hooks, and a component for using Compass inside your pipelines.

> Compass is an agentic LLM in Dagster+ that queries your operational data
> (run history, materialization stats, check results) to answer natural-language
> questions about your pipelines. This package exposes that surface
> programmatically so you can call Compass from Dagster code the same way you'd
> call any other resource.

Not affiliated with or officially supported by Dagster Labs.

## What you get

| Piece | Use it for |
| --- | --- |
| `CompassResource` | Ask Compass questions from any asset/op. `compass.ask(prompt)` returns a `CompassResponse` with `.text`, `.tool_calls`, `.suggested_replies`. |
| `compass_on_failure()` hook | Attach to a job/op; when a step fails, Compass writes a post-mortem automatically and stores it as run metadata (optionally posts to Slack). |
| `@compass_retry_advisor()` decorator | On exception, asks Compass whether to retry based on recent run history for this deployment. Raises `RetryRequested` or `Failure` accordingly. |
| `DailyInsightDigest` component | One-YAML-block declarative digest — generates an asset + schedule that materialize Compass's answer as markdown metadata each morning. |

## Install

```bash
pip install dagster-compass-kit
# optional: Slack posting from the failure hook and digest component
pip install 'dagster-compass-kit[slack]'
# optional: dagster-components YAML support
pip install 'dagster-compass-kit[components]'
```

## Quick start — resource

```python
from dagster import asset, Definitions, EnvVar, MaterializeResult, MetadataValue
from dagster_compass_kit import CompassResource

@asset
def weekly_digest(compass: CompassResource) -> MaterializeResult:
    answer = compass.ask("Summarize this week's pipeline health.")
    return MaterializeResult(metadata={"digest": MetadataValue.md(answer.text)})

defs = Definitions(
    assets=[weekly_digest],
    resources={
        "compass": CompassResource(
            dagster_cloud_url=EnvVar("DAGSTER_CLOUD_URL"),   # https://<org>.dagster.cloud/<dep>/graphql
            api_token=EnvVar("DAGSTER_CLOUD_API_TOKEN"),
        ),
    },
)
```

`CompassResource.ask(...)` is blocking-synchronous. Use `.ask_async(...)` from
inside an existing event loop. Multi-turn: keep the returned `chat_id` and
pass it to the next `ask(..., chat_id=...)`.

## Quick start — failure hook

```python
from dagster import job, op
from dagster_compass_kit import compass_on_failure

@op
def orders_etl(): ...

@job(hooks={compass_on_failure(slack_channel="#data-incidents")})
def orders_pipeline():
    orders_etl()
```

When any op fails, the hook calls Compass with the job name, run id, and
failed step name, attaches the summary to the run as metadata, and — if a
Slack resource is wired up — posts to the channel. Customize the prompt via
`compass_on_failure(prompt="...")`.

## Quick start — retry advisor

```python
from dagster import op
from dagster_compass_kit import compass_retry_advisor

@op(required_resource_keys={"compass"})
@compass_retry_advisor(max_attempts=3)
def fetch_orders(context):
    ...  # may raise ConnectionError, KeyError, etc.
```

On exception, the decorator asks Compass to analyze the error in the context
of recent run history for this deployment. If Compass says the error is
transient (network blip, upstream 502, rate limit), the op `RetryRequested`s
with Compass's suggested backoff. If it says deterministic (missing key,
schema violation, auth failure), the op raises `Failure` with the reason and
doesn't retry. Either way, the verdict is attached to the op's metadata for
humans to audit.

Fails closed: if Compass is down, times out, or returns unparseable JSON,
the original exception is re-raised unchanged so you never mask a real
failure behind a broken AI.

One-shot classification is also available on the resource directly:

```python
try:
    do_work()
except Exception as e:
    analysis = compass.classify_exception(e, op_name="fetch_orders")
    if analysis.should_retry:
        ...
```

## Quick start — component

```yaml
# components/daily_digest/component.yaml
type: dagster_compass_kit.DailyInsightDigest
attributes:
  asset_key: analytics/daily_compass_digest
  cron_schedule: "0 9 * * *"
  prompt: |
    Summarize yesterday's pipeline activity. Call out failures, slowdowns,
    anything unusual. Format as markdown.
  slack_channel: "#data-standup"    # optional
  resource_key: compass
```

The component generates the asset, a job that materializes it, and a
schedule that fires the job. Your `Definitions` still needs to expose the
`compass` (and optional `slack`) resource.

## What Compass can and can't answer

Compass is an agentic SQL-over-operational-data LLM. Meaningfully:

- ✅ Runtime trends, retry patterns, slowest/busiest jobs, failure root causes,
  stale assets, materialization history.
- ❌ Billing / credits / seat counts — those live in the billing system, not
  Compass's queryable surface. Don't ask about them; you'll get hallucinated
  or errored answers.
- 🤔 Live in-flight state is lagged — the operational tables aren't real-time.
  For "what's running right now?" use Dagster's regular GraphQL.

## Configuration reference

### `CompassResource`

| Field | Required | Notes |
| --- | --- | --- |
| `dagster_cloud_url` | yes | Full GraphQL URL. ws:// is derived. |
| `api_token` | yes | Dagster+ user or service-account token. |
| `timeout_seconds` | no (default 120s) | Hard cap per call. |

### `compass_on_failure(...)`

| Arg | Default | Notes |
| --- | --- | --- |
| `prompt` | auto Dagster-aware | Supports `{job_name}`, `{run_id}`, `{step_key}`. |
| `slack_channel` | `None` | If set, requires a `SlackResource`. |
| `resource_key` | `"compass"` | Resource key on your Definitions. |
| `metadata_key` | `"compass_post_mortem"` | Key for run-metadata attachment. |

### `compass_retry_advisor(...)`

| Arg | Default | Notes |
| --- | --- | --- |
| `resource_key` | `"compass"` | Resource key holding the `CompassResource`. |
| `max_attempts` | `3` | Hard cap on retries regardless of Compass's answer — prevents a hallucinating LLM from looping forever. |
| `on_parse_failure` | `"raise"` | `raise` / `retry_once` / `fail`. What to do if Compass's response isn't parseable JSON. Default re-raises the original exception. |
| `wait_cap_seconds` | `120` | Ignore wildly large `retry_after_seconds` values. |

Compass is asked to return JSON with the shape:

```json
{
  "should_retry": true,
  "retry_after_seconds": 30,
  "category": "transient" | "deterministic" | "data_quality" | "dependency" | "unknown",
  "confidence": "low" | "medium" | "high",
  "reason": "<short explanation>",
  "similar_failures_recently": 3
}
```

The verdict is attached to op metadata under `compass_retry_verdict` for
audit, and logged via the op's logger.

### `DailyInsightDigest` attributes

| Field | Default | Notes |
| --- | --- | --- |
| `asset_key` | required | Slash-separated key, e.g. `analytics/digest`. |
| `prompt` | required | Asked verbatim; supports markdown response formatting. |
| `cron_schedule` | `"0 9 * * *"` | Standard cron. |
| `slack_channel` | `None` | Optional Slack post on each materialization. |
| `resource_key` | `"compass"` |  |
| `group_name` | `"compass"` |  |

## How it works under the hood

Compass is exposed as a GraphQL subscription at
`wss://<org>.dagster.cloud/<deployment>/graphql` using the legacy
`subscriptions-transport-ws` protocol. This package speaks that protocol
directly (no `gql` dependency), handles Bearer auth via both the HTTP
upgrade headers and the `connection_init` payload, and assembles the
streamed `DeltaTextBlock` / tool-call / completion chunks into a tidy
`CompassResponse`.

Feature gates (`DAGSTER_PLUS_COMPASS_ENABLED` / `ENABLE_AI_SUMMARIES`) are
**not** checked client-side here — if the gate is off, Compass errors and
you'll see that in `CompassResponse.error`. Check `identity.featureGates`
via normal Dagster+ GraphQL if you want to gate pipeline logic on it.

## Security

- The API token is a secret. Always wire it through `EnvVar` or a secrets
  manager; never commit it.
- Compass responses are markdown. If you render them somewhere other than
  Dagster's UI, apply an allow-list for URL schemes (Compass responses don't
  currently contain links, but that could change).
- This package does not log the API token or frame contents.

## Limitations

- Synchronous `ask()` uses `asyncio.run()` internally; don't call it inside
  an already-running event loop (use `ask_async` there).
- The legacy WebSocket subprotocol is deprecated upstream but is what
  Dagster Cloud currently speaks. If Dagster migrates to `graphql-ws`, this
  package will need a transport update.
- One-shot subscriptions — no streaming to the caller. For real-time token
  display, subscribe to the chunk stream directly via
  `stream_ai_chat(...)` exported from `dagster_compass_kit.client`.

## License

Apache 2.0.
