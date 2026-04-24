"""Compass-powered exception analysis and retry advisor.

Premise: you can't statically know whether a given exception is worth
retrying. A ``SnowflakeOperationalError`` is usually transient; a
``KeyError: 'customer_id'`` is a deterministic bug that'll fail every
retry. Compass has access to recent run history + error patterns for
your deployment — it can actually judge.

Two public surfaces:

1. ``CompassResource.classify_exception(exc, context)`` — one-shot
   classification that returns a structured ``ExceptionAnalysis``.
2. ``@compass_retry_advisor(...)`` — op decorator. On exception, asks
   Compass, then raises ``RetryRequested`` (if Compass says retry) or
   ``Failure`` (if not). Fails closed — if Compass errors or times out,
   the original exception is re-raised so a down Compass never masks a
   real failure.

Latency + cost caveat: a Compass classification is ~20–40 seconds.
Don't use this for fast-failing ops or high-throughput pipelines. Use
it for the handful of important-enough jobs where a wrong retry
decision actually costs something.
"""

from __future__ import annotations

import functools
import hashlib
import json
import re
import traceback as _traceback
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional

from dagster import Failure, OpExecutionContext, RetryRequested

_CLASSIFY_PROMPT = """\
You're analyzing a Dagster pipeline exception to decide if it's worth retrying.

Context:
- Job: {job_name}
- Op: {op_name}
- Run ID: {run_id}
- Attempt: {attempt}

Exception type: {exc_type}
Exception message: {exc_message}

Traceback (most recent call last, abbreviated):
{traceback_str}

If recent run history for this job/op is available in your operational data,
use it to check whether similar failures have happened before and how they
resolved. BUT if this is a brand-new job, a first-time exception, or you
have no historical context at all, still classify based on your general
knowledge of the exception type, its message, and the traceback — Python
semantics and common library failure modes are enough to make a confident
call for most errors. Don't refuse to classify just because you've never
seen this pattern before.

Rules of thumb for when history is unavailable:
- ConnectionError, TimeoutError, OSError, OperationalError (SQL drivers),
  rate-limit / throttling errors → transient
- KeyError, AttributeError, TypeError, ValueError, NameError,
  AssertionError, ImportError, FileNotFoundError → deterministic
- Schema / data-contract violations → data_quality
- Upstream-service 5xx or unauthorized → dependency

Respond ONLY with a single JSON object, no prose, no markdown fences. Schema:

{{
  "should_retry": true | false,
  "retry_after_seconds": <integer 0..600>,
  "category": "transient" | "deterministic" | "data_quality" | "dependency" | "unknown",
  "confidence": "low" | "medium" | "high",
  "reason": "<one sentence>",
  "similar_failures_recently": <integer — 0 if no history available>
}}
"""


# ── Static heuristic fallback ────────────────────────────────────────────────
# Used when Compass is unavailable / errors / returns unparseable output AND
# the caller opted in with ``fallback_to_heuristic=True``. Deliberately
# conservative — we only classify exceptions whose semantics are very well
# known. Anything else stays "unknown" which forces a re-raise of the
# original exception.

_TRANSIENT_TYPES: set[str] = {
    "ConnectionError",
    "ConnectionResetError",
    "ConnectionAbortedError",
    "ConnectionRefusedError",
    "TimeoutError",
    "ReadTimeout",
    "ConnectTimeout",
    "ConnectionTimeoutError",
    # SQL / warehouse clients
    "OperationalError",
    "InterfaceError",
    "DatabaseError",  # broad; could be deterministic but usually transient in practice
    # HTTP clients — name overlap across libs (requests, httpx, aiohttp)
    "RemoteDisconnected",
    "ProtocolError",
    "ChunkedEncodingError",
    # Rate-limiting flavors
    "ThrottlingException",
    "RateLimitError",
    "TooManyRequests",
    "ServiceUnavailable",
    # Cloud SDK transient flavors
    "DeadlineExceeded",
    "ResourceExhausted",
    "Unavailable",
    "SnowflakeInternalError",
    # OS-level flakes
    "OSError",
    "IOError",
    "BlockingIOError",
}

_DETERMINISTIC_TYPES: set[str] = {
    "KeyError",
    "AttributeError",
    "TypeError",
    "NameError",
    "ValueError",
    "ImportError",
    "ModuleNotFoundError",
    "AssertionError",
    "SyntaxError",
    "IndentationError",
    "FileNotFoundError",
    "IsADirectoryError",
    "NotADirectoryError",
    "NotImplementedError",
    "ZeroDivisionError",
    "OverflowError",
    "RecursionError",
    "StopIteration",
    "StopAsyncIteration",
    "LookupError",
    "IndexError",
    "UnboundLocalError",
}

_DATA_QUALITY_TYPES: set[str] = {
    # Pydantic / dataclass-style validation
    "ValidationError",
    "SchemaError",
    "SchemaValidationError",
    "DataError",
    # Great Expectations / pandera / other data-contract libs
    "ExpectationError",
    "AnomalyDetected",
    # Pandas "bad data" types are typically subclasses of ValueError already
}

_DEPENDENCY_TYPES: set[str] = {
    # Auth / upstream-service failures
    "PermissionError",
    "Forbidden",
    "Unauthorized",
    "HTTPError",  # often 4xx/5xx upstream
    "AuthenticationError",
    "AuthorizationError",
    "ClientError",  # boto3 umbrella — many of these are dependency issues
}


def heuristic_classify(exc: BaseException) -> "ExceptionAnalysis | None":
    """Return a conservative static verdict based on exception type alone.

    Returns ``None`` if the type isn't recognized — caller should treat that
    as "unknown, re-raise." Never consults Compass.
    """
    name = type(exc).__name__
    if name in _TRANSIENT_TYPES:
        return ExceptionAnalysis(
            should_retry=True,
            retry_after_seconds=10,
            category="transient",
            confidence="medium",
            reason=f"{name} is a known-transient exception type (heuristic, no Compass consulted)",
            similar_failures_recently=0,
            raw_response="(heuristic)",
        )
    if name in _DETERMINISTIC_TYPES:
        return ExceptionAnalysis(
            should_retry=False,
            retry_after_seconds=0,
            category="deterministic",
            confidence="high",
            reason=f"{name} is a known-deterministic exception type (heuristic, no Compass consulted)",
            similar_failures_recently=0,
            raw_response="(heuristic)",
        )
    if name in _DATA_QUALITY_TYPES:
        return ExceptionAnalysis(
            should_retry=False,
            retry_after_seconds=0,
            category="data_quality",
            confidence="medium",
            reason=f"{name} is a known-data-quality exception type (heuristic, no Compass consulted)",
            similar_failures_recently=0,
            raw_response="(heuristic)",
        )
    if name in _DEPENDENCY_TYPES:
        return ExceptionAnalysis(
            should_retry=True,
            retry_after_seconds=15,
            category="dependency",
            confidence="low",
            reason=f"{name} is a known-dependency exception type (heuristic, no Compass consulted)",
            similar_failures_recently=0,
            raw_response="(heuristic)",
        )
    return None


@dataclass
class ExceptionAnalysis:
    """Compass's verdict on whether an exception is worth retrying."""

    should_retry: bool
    retry_after_seconds: int
    category: Literal["transient", "deterministic", "data_quality", "dependency", "unknown"]
    confidence: Literal["low", "medium", "high"]
    reason: str
    similar_failures_recently: int
    raw_response: str  # for debugging / metadata attachment

    @classmethod
    def unknown(cls, reason: str, raw: str = "") -> "ExceptionAnalysis":
        return cls(
            should_retry=False,
            retry_after_seconds=0,
            category="unknown",
            confidence="low",
            reason=reason,
            similar_failures_recently=0,
            raw_response=raw,
        )


_JSON_BLOCK_RE = re.compile(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", re.DOTALL)


def _extract_json(text: str) -> Optional[dict[str, Any]]:
    """Pull the first JSON object out of a possibly-markdown-fenced response."""
    if not text:
        return None
    # Strip common ```json ... ``` fences up front
    stripped = re.sub(r"^```(?:json)?\s*", "", text.strip())
    stripped = re.sub(r"\s*```\s*$", "", stripped)
    candidates = [stripped] + _JSON_BLOCK_RE.findall(text)
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            continue
    return None


def _truncate(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    return s[: limit - 1] + "…"


def _build_prompt(
    exc: BaseException,
    *,
    job_name: str,
    op_name: str,
    run_id: str,
    attempt: int,
    traceback_chars: int = 2000,
) -> str:
    tb = "".join(_traceback.format_exception(type(exc), exc, exc.__traceback__))
    return _CLASSIFY_PROMPT.format(
        job_name=job_name,
        op_name=op_name,
        run_id=run_id,
        attempt=attempt,
        exc_type=type(exc).__name__,
        exc_message=_truncate(str(exc), 500),
        traceback_str=_truncate(tb, traceback_chars),
    )


def parse_analysis(raw_text: str) -> ExceptionAnalysis:
    """Parse Compass's markdown-or-JSON response into an ``ExceptionAnalysis``.

    Falls back to ``ExceptionAnalysis.unknown`` if the response doesn't
    contain parseable JSON — never raises.
    """
    parsed = _extract_json(raw_text)
    if not parsed:
        return ExceptionAnalysis.unknown(
            reason="Compass response not parseable as JSON",
            raw=raw_text[:500],
        )
    try:
        return ExceptionAnalysis(
            should_retry=bool(parsed.get("should_retry", False)),
            retry_after_seconds=int(parsed.get("retry_after_seconds", 0) or 0),
            category=parsed.get("category", "unknown") or "unknown",
            confidence=parsed.get("confidence", "low") or "low",
            reason=str(parsed.get("reason", "") or ""),
            similar_failures_recently=int(parsed.get("similar_failures_recently", 0) or 0),
            raw_response=raw_text[:500],
        )
    except (ValueError, TypeError) as e:
        return ExceptionAnalysis.unknown(reason=f"malformed JSON: {e}", raw=raw_text[:500])


def exception_fingerprint(exc: BaseException) -> str:
    """Short, stable hash for an exception — lets callers cache/memoize analyses."""
    key = f"{type(exc).__name__}:{str(exc)[:200]}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def compass_retry_advisor(
    *,
    resource_key: str = "compass",
    max_attempts: int = 3,
    on_parse_failure: Literal["raise", "retry_once", "fail"] = "raise",
    wait_cap_seconds: int = 120,
    fallback_to_heuristic: bool = False,
) -> Callable:
    """Decorator: on exception, ask Compass whether to retry.

    Wraps an ``@op``-decorated function so that on exception, Compass is
    consulted. Compass's verdict decides whether to ``RetryRequested`` or
    ``Failure`` (non-retryable).

    Usage::

        @op(required_resource_keys={"compass"})
        @compass_retry_advisor(max_attempts=3)
        def fetch_orders(context):
            ...  # may raise

    Args:
        resource_key: key under which ``CompassResource`` is registered.
        max_attempts: upper bound on retries regardless of Compass's answer
            — a safety cap so a hallucinating LLM can't loop forever.
        on_parse_failure: what to do if Compass's response can't be parsed
            as valid JSON. ``"raise"`` (default) re-raises the original
            exception so a broken Compass never masks a real failure;
            ``"retry_once"`` forces a single retry; ``"fail"`` marks the
            op terminal.
        wait_cap_seconds: cap for ``retry_after_seconds`` — ignores any
            wildly large value Compass returns.
        fallback_to_heuristic: if True and Compass is unavailable or
            returns unparseable output, fall back to
            :func:`heuristic_classify` — a static exception-type table
            covering well-known transient/deterministic Python errors.
            If the heuristic also doesn't recognize the exception type,
            behavior falls through to ``on_parse_failure``. Defaults to
            False (Compass-only) for predictable behavior; enable when
            you'd rather have *some* decision than none.
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(context: OpExecutionContext, *args, **kwargs):
            try:
                return fn(context, *args, **kwargs)
            except (RetryRequested, Failure):
                # Already-classified — pass straight through
                raise
            except Exception as exc:  # noqa: BLE001 - we explicitly want broad
                attempt = context.retry_number + 1

                def _try_heuristic(reason_prefix: str) -> Optional[ExceptionAnalysis]:
                    if not fallback_to_heuristic:
                        return None
                    h = heuristic_classify(exc)
                    if h is not None:
                        context.log.info(
                            f"compass_retry_advisor: {reason_prefix}; "
                            f"heuristic fallback decides {h.category}/retry={h.should_retry}"
                        )
                    return h

                # 1. Resource missing?
                try:
                    compass = getattr(context.resources, resource_key)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(
                        f"compass_retry_advisor: resource '{resource_key}' missing ({e!r})"
                    )
                    h = _try_heuristic("resource unavailable")
                    if h is not None:
                        return _apply_decision(context, exc, h, attempt, max_attempts, wait_cap_seconds)
                    raise exc

                context.log.info(
                    f"compass_retry_advisor: classifying {type(exc).__name__} via Compass "
                    f"(attempt {attempt}/{max_attempts}, fingerprint={exception_fingerprint(exc)})"
                )

                # 2. Compass call itself fails (timeout, WS error)
                try:
                    prompt = _build_prompt(
                        exc,
                        job_name=context.job_name,
                        op_name=context.op.name,
                        run_id=context.run_id,
                        attempt=attempt,
                    )
                    response = compass.ask(prompt)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"compass_retry_advisor: Compass call failed ({e!r})")
                    h = _try_heuristic("Compass call failed")
                    if h is not None:
                        return _apply_decision(context, exc, h, attempt, max_attempts, wait_cap_seconds)
                    raise exc

                analysis = parse_analysis(response.text or "")
                parse_failed = analysis.category == "unknown" and not analysis.raw_response

                # 3. Parse failure — consult heuristic first, then fall through to on_parse_failure
                if parse_failed:
                    h = _try_heuristic("Compass response unparseable")
                    if h is not None:
                        return _apply_decision(context, exc, h, attempt, max_attempts, wait_cap_seconds)
                    if on_parse_failure == "raise":
                        raise exc
                    if on_parse_failure == "fail":
                        raise Failure(
                            description=f"Unclassifiable: {type(exc).__name__}: {exc}"
                        ) from exc
                    # retry_once
                    if attempt < max_attempts:
                        raise RetryRequested(max_retries=1, seconds_to_wait=5) from exc
                    raise Failure(
                        description=f"Exhausted retries for unclassifiable {type(exc).__name__}"
                    ) from exc

                # 4. Compass returned a valid verdict — apply it
                return _apply_decision(context, exc, analysis, attempt, max_attempts, wait_cap_seconds)

        return wrapper

    return decorator


def _apply_decision(
    context: OpExecutionContext,
    exc: BaseException,
    analysis: ExceptionAnalysis,
    attempt: int,
    max_attempts: int,
    wait_cap_seconds: int,
):
    """Take Compass's (or the heuristic's) verdict and either retry, fail, or raise.

    Always attaches the verdict as op metadata under ``compass_retry_verdict``
    before raising so humans can audit every AI decision after the fact.
    """
    context.log.info(
        f"Retry verdict: retry={analysis.should_retry} category={analysis.category} "
        f"confidence={analysis.confidence} reason={analysis.reason!r}"
    )
    context.add_output_metadata(  # type: ignore[attr-defined]
        {
            "compass_retry_verdict": {
                "source": (
                    "heuristic" if analysis.raw_response == "(heuristic)" else "compass"
                ),
                "should_retry": analysis.should_retry,
                "category": analysis.category,
                "confidence": analysis.confidence,
                "reason": analysis.reason,
                "similar_failures_recently": analysis.similar_failures_recently,
            },
        },
    )

    if not analysis.should_retry:
        raise Failure(
            description=f"[{analysis.category}] {analysis.reason}",
            metadata={
                "original_exception": f"{type(exc).__name__}: {exc}",
                "verdict_reason": analysis.reason,
                "confidence": analysis.confidence,
            },
        ) from exc

    if attempt >= max_attempts:
        raise Failure(
            description=(
                f"Retry cap ({max_attempts}) reached; classifier still said retry. "
                f"Last reason: {analysis.reason}"
            ),
        ) from exc

    wait = max(0, min(wait_cap_seconds, analysis.retry_after_seconds))
    raise RetryRequested(max_retries=max_attempts - attempt, seconds_to_wait=wait) from exc
