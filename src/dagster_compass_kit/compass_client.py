"""Dagster-free, sync Compass client. Usable in scripts, FastAPI, notebooks.

This is still Dagster+-Compass-specific — it hits the WebSocket subscription
at your ``<org>.dagster.cloud`` tenant. It does NOT talk to standalone
https://compass.dagster.io/. The name ``CompassClient`` reflects the code
shape (a client vs. a Dagster Resource), not product scope.

Typical use::

    from dagster_compass_kit import CompassClient

    client = CompassClient(
        dagster_cloud_url="https://my-org.dagster.cloud/prod/graphql",
        api_token="user:...",
    )

    # One-shot
    response = client.ask("What's broken right now?")

    # Structured
    from pydantic import BaseModel

    class Verdict(BaseModel):
        is_anomaly: bool
        explanation: str

    v = client.ask_structured("Is this materialization anomalous?", Verdict)

    # Multi-turn
    with client.conversation() as chat:
        chat.ask("Which jobs failed yesterday?")
        chat.ask("Of those, which are in the ETL pipeline?")
        chat.ask("Show me the slowest ones.")
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import Iterator, TypeVar

from pydantic import BaseModel

from .client import (
    CompassResponse,
    assemble,
    stream_ai_chat,
    stream_ai_summary_for_asset,
)
from .structured import build_structured_prompt, parse_structured

T = TypeVar("T", bound=BaseModel)


class CompassClient:
    """Synchronous Dagster-free client for Dagster+ Compass."""

    def __init__(
        self,
        *,
        dagster_cloud_url: str,
        api_token: str,
        timeout_seconds: float = 120.0,
    ) -> None:
        self.dagster_cloud_url = dagster_cloud_url
        self.api_token = api_token
        self.timeout_seconds = timeout_seconds

    # ── Core: free-form chat ────────────────────────────────────────────────
    def ask(self, prompt: str, *, chat_id: int = 0) -> CompassResponse:
        """Blocking free-form question. Returns the assembled CompassResponse."""
        return asyncio.run(self._ask_async(prompt, chat_id))

    async def ask_async(self, prompt: str, *, chat_id: int = 0) -> CompassResponse:
        """Async variant for use inside an existing event loop."""
        return await self._ask_async(prompt, chat_id)

    async def _ask_async(self, prompt: str, chat_id: int) -> CompassResponse:
        return await assemble(
            stream_ai_chat(
                graphql_http_url=self.dagster_cloud_url,
                api_token=self.api_token,
                payload=prompt,
                chat_id=chat_id,
                timeout_seconds=self.timeout_seconds,
            )
        )

    # ── Structured: Pydantic schema-validated responses ──────────────────────
    def ask_structured(
        self,
        prompt: str,
        schema: type[T],
        *,
        chat_id: int = 0,
    ) -> T:
        """Ask Compass for a response that matches a Pydantic model.

        Raises ``dagster_compass_kit.structured.CompassSchemaError`` if the
        answer can't be validated — inspect ``.raw_text`` on the exception
        to see what Compass actually returned.
        """
        augmented = build_structured_prompt(prompt, schema)
        response = self.ask(augmented, chat_id=chat_id)
        return parse_structured(response.text or "", schema)

    async def ask_structured_async(
        self,
        prompt: str,
        schema: type[T],
        *,
        chat_id: int = 0,
    ) -> T:
        augmented = build_structured_prompt(prompt, schema)
        response = await self.ask_async(augmented, chat_id=chat_id)
        return parse_structured(response.text or "", schema)

    # ── Per-materialization summary ──────────────────────────────────────────
    def summarize_materialization(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        return asyncio.run(self._summarize_async(run_id, asset_key_path))

    async def summarize_materialization_async(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        return await self._summarize_async(run_id, asset_key_path)

    async def _summarize_async(
        self, run_id: str, asset_key_path: list[str]
    ) -> CompassResponse:
        return await assemble(
            stream_ai_summary_for_asset(
                graphql_http_url=self.dagster_cloud_url,
                api_token=self.api_token,
                run_id=run_id,
                asset_key_path=asset_key_path,
                timeout_seconds=self.timeout_seconds,
            )
        )

    # ── Multi-turn conversations ─────────────────────────────────────────────
    @contextmanager
    def conversation(self) -> Iterator["CompassConversation"]:
        """Yield a ``CompassConversation`` that preserves chat_id across turns."""
        yield CompassConversation(self)


class CompassConversation:
    """Stateful wrapper that threads the server-returned ``chat_id`` across turns.

    Compass assigns a ``chat_id`` on the first message and expects it back on
    every follow-up so it can recall context. This class just tracks that id
    for you. Turns within one conversation stay coherent; a new conversation
    starts fresh.
    """

    def __init__(self, client: CompassClient) -> None:
        self._client = client
        self._chat_id: int = 0
        self._turns: list[tuple[str, CompassResponse]] = []

    @property
    def chat_id(self) -> int:
        return self._chat_id

    @property
    def turns(self) -> list[tuple[str, CompassResponse]]:
        """Snapshot of (prompt, response) tuples exchanged so far."""
        return list(self._turns)

    def ask(self, prompt: str) -> CompassResponse:
        response = self._client.ask(prompt, chat_id=self._chat_id)
        self._track(prompt, response)
        return response

    def ask_structured(self, prompt: str, schema: type[T]) -> T:
        augmented = build_structured_prompt(prompt, schema)
        response = self._client.ask(augmented, chat_id=self._chat_id)
        self._track(prompt, response)
        return parse_structured(response.text or "", schema)

    async def ask_async(self, prompt: str) -> CompassResponse:
        response = await self._client.ask_async(prompt, chat_id=self._chat_id)
        self._track(prompt, response)
        return response

    def _track(self, prompt: str, response: CompassResponse) -> None:
        if response.chat_id is not None:
            self._chat_id = response.chat_id
        self._turns.append((prompt, response))
