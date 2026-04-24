"""Structured (Pydantic-validated) Compass responses.

**Design note — why we don't ask for JSON-only output.**

Compass has safety guardrails that reject prompts demanding the model "act
as an API endpoint" or respond "only in JSON / with no prose." It detects
those patterns as prompt-injection attempts and returns a refusal rather
than the requested JSON. (Verified empirically on Dagster Cloud build
``98f80978``, April 2026.)

So instead of forcing JSON-only output, we ask Compass to answer naturally
and end with a conventional summary section — the same way you'd ask any
assistant to "end with a TL;DR." We then parse that footer with
``SIGNAL: VALUE``-style regexes. This:

1. Doesn't trigger guardrails because the request is conversational.
2. Preserves the full prose analysis for humans (attached as metadata).
3. Gives us a reliable, simple parse surface.

As a bonus, if Compass happens to emit a JSON block anywhere in the
response, we'll still parse that successfully too — the JSON extractor
runs first and the signal-phrase parser is the fallback.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)

_JSON_BLOCK_RE = re.compile(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", re.DOTALL)


class CompassSchemaError(ValueError):
    """Raised when Compass's response can't be validated against the schema.

    ``raw_text`` preserves the original response so callers can log or fall
    back without a second round-trip.
    """

    def __init__(self, message: str, raw_text: str = "") -> None:
        super().__init__(message)
        self.raw_text = raw_text


# ── Signal-phrase prompt builder ─────────────────────────────────────────────


def _signal_template_for_schema(schema: type[BaseModel]) -> str:
    """Build a per-field signal-phrase template from a Pydantic schema.

    Example output for AnomalyVerdict::

        IS_ANOMALY: YES/NO
        SEVERITY: none/low/medium/high
        EXPLANATION: <one sentence>
        SIMILAR_EVENTS_RECENTLY: <integer>
    """
    schema_json = schema.model_json_schema()
    props = schema_json.get("properties", {})
    lines = []
    for field_name, spec in props.items():
        spec = _resolve_ref(spec, schema_json)
        hint = _hint_for_spec(spec)
        lines.append(f"  {field_name.upper()}: {hint}")
    return "\n".join(lines)


def _resolve_ref(spec: dict[str, Any], root: dict[str, Any]) -> dict[str, Any]:
    """Inline a $ref so we can see enum values when they live in $defs."""
    ref = spec.get("$ref")
    if not ref or not ref.startswith("#/"):
        return spec
    parts = ref.lstrip("#/").split("/")
    cursor = root
    for p in parts:
        cursor = cursor.get(p, {})
    return cursor


def _hint_for_spec(spec: dict[str, Any]) -> str:
    if "enum" in spec:
        return "/".join(str(v) for v in spec["enum"])
    t = spec.get("type")
    if t == "boolean":
        return "YES/NO"
    if t == "integer":
        return "<integer>"
    if t == "number":
        return "<number>"
    if t == "array":
        return "<comma-separated list or 'none'>"
    return "<short text>"


def build_structured_prompt(prompt: str, schema: type[BaseModel]) -> str:
    """Augment a prompt with a SUMMARY-section request.

    Compass answers naturally; we ask for a short structured footer at the
    end using ``FIELD: VALUE`` lines. This dodges the injection-detection
    pattern that single-shot JSON-only prompts trigger.
    """
    template = _signal_template_for_schema(schema)
    return (
        f"{prompt}\n\n"
        "Please explain your analysis naturally. Then, so I can record "
        "your answer programmatically, end your response with a section "
        'labeled exactly "SUMMARY:" on its own line, followed by these '
        "fields on separate lines (case-insensitive, one per line):\n\n"
        f"SUMMARY:\n{template}"
    )


# ── Extractors ───────────────────────────────────────────────────────────────


def extract_json(text: str) -> Optional[dict[str, Any]]:
    """Forgiving JSON extractor — handles plain, fenced, and prose-wrapped."""
    if not text:
        return None
    stripped = re.sub(r"^```(?:json)?\s*", "", text.strip())
    stripped = re.sub(r"\s*```\s*$", "", stripped)
    candidates = [stripped] + _JSON_BLOCK_RE.findall(text)
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except (json.JSONDecodeError, ValueError):
            continue
    return None


_TRUE_WORDS = {"yes", "true", "y", "1"}
_FALSE_WORDS = {"no", "false", "n", "0"}


def _coerce_value(raw: str, spec: dict[str, Any], root: dict[str, Any]) -> Any:
    """Convert a raw signal-phrase value into the Python type the schema wants."""
    spec = _resolve_ref(spec, root)
    value = raw.strip().rstrip(".,;:")
    low = value.lower()
    t = spec.get("type")

    if "enum" in spec:
        # Match case-insensitively, then return the canonical form from the enum
        enum_vals = spec["enum"]
        for v in enum_vals:
            if str(v).lower() == low:
                return v
        # Substring fallback — Compass might wrap the enum in prose
        for v in enum_vals:
            if str(v).lower() in low:
                return v
        return value  # Pydantic will reject; keep raw for the error message

    if t == "boolean":
        if low in _TRUE_WORDS:
            return True
        if low in _FALSE_WORDS:
            return False
        # Substring fallback for "yes, …" or "no, …"
        first = low.split(None, 1)[0] if low else ""
        if first in _TRUE_WORDS:
            return True
        if first in _FALSE_WORDS:
            return False
        return value

    if t == "integer":
        m = re.search(r"-?\d+", value)
        return int(m.group(0)) if m else 0

    if t == "number":
        m = re.search(r"-?\d+(?:\.\d+)?", value)
        return float(m.group(0)) if m else 0.0

    if t == "array":
        if low in ("none", "n/a", "", "[]"):
            return []
        # Split on commas or semicolons; strip quotes/brackets
        items = re.split(r"[;,]", value.strip("[] "))
        return [i.strip().strip('"').strip("'") for i in items if i.strip()]

    return value  # string passthrough


def extract_signals(text: str, schema: type[T]) -> Optional[dict[str, Any]]:
    """Parse the SUMMARY section of a prose response into a field dict.

    Scans the whole text for ``FIELD: value`` lines matching the schema's
    field names (case-insensitive). Returns ``None`` if no fields matched —
    that's a signal the whole extraction failed, so caller can try a
    different strategy.
    """
    if not text:
        return None
    schema_json = schema.model_json_schema()
    props = schema_json.get("properties", {})

    # Prefer lines below a "SUMMARY:" marker if one exists; otherwise whole text
    sm = re.search(r"SUMMARY\s*:?\s*$", text, re.IGNORECASE | re.MULTILINE)
    search_scope = text[sm.end():] if sm else text

    fields: dict[str, Any] = {}
    for field_name, spec in props.items():
        pattern = rf"(?im)^\s*{re.escape(field_name)}\s*:\s*(.+?)\s*$"
        match = re.search(pattern, search_scope)
        if not match:
            continue
        fields[field_name] = _coerce_value(match.group(1), spec, schema_json)

    return fields if fields else None


def parse_structured(raw_text: str, schema: type[T]) -> T:
    """Validate a Compass response into a Pydantic model.

    Strategy:
      1. Try JSON extraction (works if Compass emits a JSON block).
      2. Fall back to SUMMARY/signal-phrase extraction from the prose.

    Raises ``CompassSchemaError`` if neither path yields a valid object.
    """
    # Path 1: Compass emitted actual JSON somewhere
    data = extract_json(raw_text)
    if data is not None:
        try:
            return schema.model_validate(data)
        except ValidationError:
            pass  # fall through to signal-phrase path

    # Path 2: parse the prose SUMMARY section
    fields = extract_signals(raw_text, schema)
    if fields:
        try:
            return schema.model_validate(fields)
        except ValidationError as e:
            raise CompassSchemaError(
                f"SUMMARY section parsed but failed schema validation: {e}",
                raw_text=raw_text[:1000],
            ) from e

    raise CompassSchemaError(
        "Could not find a JSON block or a SUMMARY section in Compass's response",
        raw_text=raw_text[:1000],
    )
