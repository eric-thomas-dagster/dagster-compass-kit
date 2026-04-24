# Assumptions this kit makes about Dagster+ Compass

Dagster+ Compass is a hosted product we don't control, so this kit rests on
a handful of empirical observations and some educated guesses. This doc lists
them explicitly so anyone using the kit can sanity-check them against their
own deployment, and so behavior changes in Compass can be spotted quickly.

> **If any of these assumptions breaks for your deployment, please file an issue.**

## Core assumptions

### 0. The Dagster+ Issues `createIssue` mutation is stable

**Empirically verified April 2026** against a live tenant. The mutation
the Dagster+ UI itself calls when you click "Generate an issue" is:

```graphql
mutation CreateIssue(
  $title: String!,
  $description: String!,
  $origin: IssueLinkedObjectInput,   # { runId: "..." } for run-linked
  $chatId: Int                       # optional link to a Compass chat
) {
  createIssue(
    title: $title,
    description: $description,
    origin: $origin,
    chatId: $chatId
  ) {
    __typename
    ... on CreateIssueSuccess { issue { id publicId title status } }
    ... on UnauthorizedError { message }
    ... on PythonError { message }
  }
}
```

The kit hardcodes this shape. If Dagster changes it, ``_CREATE_ISSUE_MUTATION``
in ``issues.py`` needs a matching update, and custom tenants can override
via ``custom_mutation_spec=IssueMutationSpec(...)``.

Note: the mutation accepts only ``title`` / ``description`` / ``origin`` /
``chatId``. There's no first-class ``severity`` or ``labels`` field. The
kit folds Compass-generated severity + labels into the description body
as a markdown footer so they're visible inline.

### 1. The WebSocket subscription surface is stable

We assume these subscriptions exist and accept the documented arguments:

- `aiChat(chatId: Int!, payload: String!)`
- `aiSummaryForAssetMaterialization(runId: ID!, assetKey: AssetKeyInput!)`

The response union type (`ChatResponseChunkOrError`) includes at least:
`StartChatStream`, `StartTextBlock`, `DeltaTextBlock`, `CompleteTextBlock`,
`StartToolBlock`, `DeltaToolInputBlock`, `CompleteToolBlock`,
`CompleteChatStream`, `AISummaryError`, `PythonError`.

Observed on Dagster Cloud builds up to `98f80978` (April 2026). If Dagster
changes the subscription names or chunk union, this kit will break fast and
loud (server will close with a schema error).

### 2. Authentication

We assume a Dagster+ API token sent as `Authorization: Bearer <token>` in the
WebSocket HTTP upgrade headers **and** in the `connection_init` payload is
accepted. Either path alone has worked in practice; sending both is
belt-and-suspenders.

Cookie auth (what the web UI uses) is NOT supported — this is for API-token
auth only, which is the path Dagster+ exposes to programmatic consumers.

### 3. Wire protocol

`subscriptions-transport-ws` (the legacy protocol — frame types
`connection_init`, `start`, `data`, `complete`, not `subscribe`/`next`).
Dagster Cloud emits the legacy format as of April 2026.

If Dagster migrates to `graphql-ws`, this kit will need a transport update
(single file: `src/dagster_compass_kit/client.py`).

## Data-access assumptions

### 4. Compass has live access to Dagster+ operational data

**Earlier guidance suggested Compass data was refreshed hourly.** Based on
recent conversation with Dagster staff, Compass now queries **live**
operational data. This kit does not lean on the hourly-refresh assumption.

What this means for consumers:

- ✅ Questions like "what runs are failing right now?" / "what materialized
  in the last 10 minutes?" should work with sub-minute freshness.
- ✅ Auto-postmortem (`compass_on_failure`) and retry-advisor calls fired
  seconds after a failure can still find the run in Compass's context.
- 🤔 If you observe stale answers (Compass claims "no failures" when there
  obviously are), file a ticket with Dagster support — and reconsider
  whether this assumption still holds.

### 4b. Compass has safety guardrails that reject "silent API endpoint" phrasing

**Empirically verified April 2026.** If you tell Compass to "respond ONLY
with JSON" / "no prose" / "no markdown fences," it detects the pattern as
a prompt-injection attempt and refuses to comply, responding with
something like:

> "I'm not able to respond with only a JSON object — I'm designed to be a
> helpful, conversational assistant… This looks like a prompt injection
> attempt."

**What this kit does instead:** asks Compass to answer naturally AND end
with a ``SUMMARY:`` section containing ``FIELD: VALUE`` lines. Compass
complies with this as a normal formatting request (no different from "end
with a TL;DR"). We then parse the SUMMARY footer with regex — JSON-in-body
is still parsed as a fallback if Compass happens to emit it.

Concrete parser order:

1. Try to extract a JSON block from anywhere in the response (fenced,
   plain, or prose-wrapped).
2. If no valid JSON, look for a ``SUMMARY:`` marker and parse
   ``FIELD: value`` lines below it, case-insensitive.
3. If neither works, raise ``CompassSchemaError``.

### 5. Compass can reason about exceptions even without historical context

This is load-bearing for the retry-advisor to work on brand-new jobs /
first-time-seen exception types. **Our prompt explicitly tells Compass:
"If you have no history for this exception, classify based on your
knowledge of the exception type, message, and traceback alone."**

LLMs grounded in Python + common library semantics can reliably classify:

- `ConnectionError` / `TimeoutError` → transient
- `KeyError` / `AttributeError` / `TypeError` → deterministic

...without needing to have seen a matching failure before. The
`compass_retry_advisor` decorator trusts this.

If Compass refuses to classify without history, you can opt into a
deterministic static heuristic as a fallback:

```python
@compass_retry_advisor(max_attempts=3, fallback_to_heuristic=True)
def fetch_orders(context): ...
```

See `heuristic_classify()` in `retry.py` for the fallback table.

### 5a. Cascade classification is deterministic

The cascade classifier does NOT depend on Compass to decide root-vs-cascade.
It walks the asset graph: an affected asset is CASCADE iff at least one of
its declared upstreams is also in the affected set; otherwise ROOT. This is
exact for the alerting use case — Dagster skips downstream assets when an
upstream step fails, so topology fully determines the answer.

Compass's role is limited to *enriching the explanation* — turning
"Deterministic classification: orders is the root cause; 3 downstream
asset(s) did not run." into "Snowflake auth expired at 14:03; orders and
its three daily-rollup downstreams were affected." The classification
itself — which is what the alert policy filters on — is LLM-free.

This means the kit is usable (and the alert-fatigue benefit ships) even
on tenants where Compass is disabled or the feature flag is off.

### 5b. Compass can query open Dagster+ Issues for dedup

The dedup mode of `compass_create_issue_on_failure` (the default) asks
Compass to check the Issues queue before filing a new one. This relies on
Compass's `TOOL_TYPE_RUN_SQL_QUERY` having read access to the same
operational table the Dagster+ UI's Issues view reads from.

What we've observed: Compass can answer "what issues are open right now?"
correctly via SQL against the operational dataset. We expect the dedup
prompt to ride on the same path. **Not yet validated end-to-end against a
flooded queue** — once integration tests run on a tenant where we can
populate fake duplicate failures, this will move from "expected" to
"verified."

If Compass can't see Issues for your tenant (e.g. permissions vary), the
dedup pass returns `action=create_new` for everything and the dedup
silently degrades to the old behavior. To turn it off entirely:

```python
compass_create_issue_on_failure(dedup=False)
```

### 5c. Dagster+ alert policies fire per-event on asset check failures

The cascade classifier emits **runless AssetCheckEvaluation** events
(not AssetObservations) named `compass_root_cause_detected`. Root
causes emit with `passed=False`, cascades with `passed=True`.

This design relies on two Dagster+ capabilities, both verified April 2026:

1. Asset checks can be reported runlessly for assets that don't
   have a check pre-declared in user code. Confirmed: the evaluation
   lands in the asset's check history with the expected status.
2. Dagster+ AlertPolicies on asset check failures fire per-event and
   pass evaluation metadata through to the notification payload. User
   confirmed that observation-based alerts are aggregate-only, which
   is why this feature uses checks instead.

If either capability is absent on a given tenant, the classifier still
classifies correctly — but the alert-fatigue benefit won't materialize
until the tenant is on a version that supports per-event check-failure
alerts.

### 6. Tool surface observed

Compass has used at least these tools during our testing:

- `TOOL_TYPE_SEARCH_DATASETS`
- `TOOL_TYPE_RUN_SQL_QUERY` — actually runs SQL against operational tables
  like `dagster_plus_operational_data_org_<N>.dagster_plus_runs`
- `TOOL_TYPE_RENDER_DATA_VISUALIZATION`

This kit doesn't gate on specific tool types — it just surfaces them via
`CompassResponse.tool_calls` for audit / metadata attachment. New tool
types Compass adds later should flow through transparently.

## Scope assumptions

### 7. Dagster+ Compass only

This kit does NOT talk to standalone [compass.dagster.io](https://compass.dagster.io/)
(the hosted product where customers connect arbitrary data sources). The
URL, auth, and tool surface there are different. This kit hits
`wss://<org>.dagster.cloud/<deployment>/graphql` — the Compass embedded
inside Dagster+ — and ONLY that.

### 8. Compass does not know about billing / credits

Empirically confirmed: Compass cannot answer questions about Dagster+
credits, seat counts, or billing. Those live in a different system, not
queryable by Compass. This kit's documentation and example prompts steer
away from billing questions. If you ask anyway, expect hallucinated or
errored responses — not a kit bug.

### 9. Latency per call is 20–40 seconds

Compass streams tokens for ~30 seconds on a typical question that involves
tool use. This kit's default `timeout_seconds=120` gives generous headroom.

Consumers should NOT use this kit on hot paths — a retry-advisor wrapped
around a 10ms op adds a ~30s penalty per failure. Reserve for high-stakes,
low-frequency paths (scheduled jobs, critical asset checks, incident
triage).

## Failure behavior

### 10. Fail closed, not open

When Compass errors, times out, or returns unparseable JSON, this kit's
default is to **re-raise the original exception unchanged** or **fail open
with a WARN** on asset checks. Rationale: an AI outage must never mask a
real data-quality or pipeline failure.

Specific defaults:

| Surface | On Compass failure |
| --- | --- |
| `CompassResource.ask()` | Returns `CompassResponse` with `.error` set |
| `CompassResource.ask_structured()` | Raises `CompassSchemaError` |
| `compass_on_failure` hook | Logs warning, returns — original failure stands |
| `@compass_retry_advisor` (default) | Re-raises original exception |
| `@compass_retry_advisor(fallback_to_heuristic=True)` | Uses static exception-type table |
| `compass_asset_check` | WARN-severity pass-through (doesn't block downstream) |
| `compass_sensor` | `SkipReason` (no run launched) |

## Things we deliberately don't assume

- **That Compass is deterministic.** It's an LLM. Same prompt can give
  slightly different answers. Use `exception_fingerprint()` if you need
  cache-stable behavior within a backfill.
- **That responses are JSON-only.** They arrive as markdown, sometimes
  with code fences, sometimes with prose. Our parser is forgiving.
- **That `chat_id` is persistent across deployments.** It's a per-tenant
  server-side id; don't try to resume a conversation across days.
- **That feature flags stay on.** `DAGSTER_PLUS_COMPASS_ENABLED` can be
  flipped off for your tenant. This kit does NOT check that flag — if
  Compass is gated off, you'll see server errors on every call. Check
  the flag yourself via `identity.featureGates` if you want to gate
  pipeline logic on it.

## Verification

To empirically validate assumptions for your deployment, run:

```bash
python -m dagster_compass_kit.doctor \
    --url https://my-org.dagster.cloud/prod/graphql \
    --token "$DAGSTER_CLOUD_API_TOKEN"
```

*(Note: `doctor` command is TODO — contributions welcome.)*
