"""Structured (Pydantic-validated) Compass responses.

Turns Compass into a function-calling primitive: "here's a question and a
Pydantic schema, give me a strongly-typed answer." Used directly, and as
the foundation for compass_asset_check / compass_sensor / retry advisor.
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

    ``raw_text`` on the exception preserves the original response so callers
    can log / debug / fall back without making another round-trip.
    """

    def __init__(self, message: str, raw_text: str = "") -> None:
        super().__init__(message)
        self.raw_text = raw_text


def build_structured_prompt(prompt: str, schema: type[BaseModel]) -> str:
    """Augment a user prompt with a JSON-schema instruction footer.

    Compass tends to wrap JSON in ```json fences or prose — we handle both
    on the parse side, but the clearer the prompt, the fewer retries we need.
    """
    schema_json = schema.model_json_schema()
    schema_str = json.dumps(schema_json, indent=2)
    return (
        f"{prompt}\n\n"
        "Respond ONLY with a single JSON object matching this schema — no "
        "prose, no markdown fences, no explanation before or after:\n\n"
        f"{schema_str}"
    )


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


def parse_structured(raw_text: str, schema: type[T]) -> T:
    """Parse + validate Compass's response against a Pydantic schema.

    Raises ``CompassSchemaError`` with ``raw_text`` preserved on any failure.
    """
    data = extract_json(raw_text)
    if data is None:
        raise CompassSchemaError(
            "Compass response did not contain parseable JSON",
            raw_text=raw_text[:1000],
        )
    try:
        return schema.model_validate(data)
    except ValidationError as e:
        raise CompassSchemaError(
            f"Compass response failed schema validation: {e}",
            raw_text=raw_text[:1000],
        ) from e
