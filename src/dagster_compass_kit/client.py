"""Minimal async client for the Compass GraphQL WebSocket subscription.

Dagster Cloud speaks the legacy ``subscriptions-transport-ws`` protocol:
``connection_init`` / ``connection_ack`` / ``start`` / ``data`` / ``complete``.
We don't pull in the ``gql`` library to avoid the graphql-ws vs
subscriptions-transport-ws version mismatch rabbit hole — the protocol is
simple enough to speak directly.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import websockets

_AI_CHAT_QUERY = """
subscription AIChat($chatId: Int!, $payload: String!) {
  aiChat(chatId: $chatId, payload: $payload) {
    __typename
    ... on StartChatStream { chatId }
    ... on StartTextBlock { placeholder }
    ... on DeltaTextBlock { textFragment }
    ... on CompleteTextBlock { placeholder }
    ... on StartToolBlock { toolType toolId }
    ... on DeltaToolInputBlock { jsonFragment }
    ... on CompleteToolBlock { toolError { message } }
    ... on CompleteChatStream { suggestedReplies }
    ... on AISummaryError { message }
    ... on PythonError { message }
  }
}
""".strip()

_AI_SUMMARY_ASSET_QUERY = """
subscription AISummaryForAssetMaterialization($runId: ID!, $assetKey: AssetKeyInput!) {
  aiSummaryForAssetMaterialization(runId: $runId, assetKey: $assetKey) {
    __typename
    ... on StartChatStream { chatId }
    ... on StartTextBlock { placeholder }
    ... on DeltaTextBlock { textFragment }
    ... on CompleteTextBlock { placeholder }
    ... on StartToolBlock { toolType toolId }
    ... on DeltaToolInputBlock { jsonFragment }
    ... on CompleteToolBlock { toolError { message } }
    ... on CompleteChatStream { suggestedReplies }
    ... on AISummaryError { message }
    ... on PythonError { message }
  }
}
""".strip()


@dataclass
class ToolCall:
    tool_type: str
    tool_id: str
    input_json: str = ""
    error: str | None = None


@dataclass
class CompassResponse:
    """Assembled result of one Compass subscription.

    ``text`` is the concatenation of every ``DeltaTextBlock.textFragment``
    (i.e. the prose answer). ``tool_calls`` are what the agent invoked
    en route. ``suggested_replies`` mirrors what the UI shows as follow-up
    chips. ``chat_id`` is the server-returned conversation id (use for
    multi-turn). ``error`` is set if the server streamed an error chunk.
    """

    text: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    suggested_replies: list[str] = field(default_factory=list)
    chat_id: int | None = None
    error: str | None = None


def _http_to_ws(url: str) -> str:
    return re.sub(r"^http", "ws", url, count=1)


async def stream_ai_chat(
    graphql_http_url: str,
    api_token: str,
    payload: str,
    chat_id: int = 0,
    timeout_seconds: float = 120.0,
) -> AsyncIterator[dict[str, Any]]:
    """Yield each decoded chunk from an ``AIChat`` subscription.

    ``graphql_http_url`` is the customer's Dagster+ GraphQL URL, e.g.
    ``https://my-org.dagster.cloud/my-deployment/graphql``. The scheme is
    rewritten to ``wss://`` automatically.
    """
    async for chunk in _stream(
        graphql_http_url=graphql_http_url,
        api_token=api_token,
        query=_AI_CHAT_QUERY,
        variables={"chatId": chat_id, "payload": payload},
        response_field="aiChat",
        timeout_seconds=timeout_seconds,
    ):
        yield chunk


async def stream_ai_summary_for_asset(
    graphql_http_url: str,
    api_token: str,
    run_id: str,
    asset_key_path: list[str],
    timeout_seconds: float = 120.0,
) -> AsyncIterator[dict[str, Any]]:
    """Yield each decoded chunk from ``aiSummaryForAssetMaterialization``."""
    async for chunk in _stream(
        graphql_http_url=graphql_http_url,
        api_token=api_token,
        query=_AI_SUMMARY_ASSET_QUERY,
        variables={"runId": run_id, "assetKey": {"path": asset_key_path}},
        response_field="aiSummaryForAssetMaterialization",
        timeout_seconds=timeout_seconds,
    ):
        yield chunk


async def assemble(stream: AsyncIterator[dict[str, Any]]) -> CompassResponse:
    """Drain a chunk stream into a single ``CompassResponse``."""
    result = CompassResponse()
    current_tool: ToolCall | None = None
    async for chunk in stream:
        tn = chunk.get("__typename")
        if tn == "StartChatStream":
            result.chat_id = chunk.get("chatId")
        elif tn == "DeltaTextBlock":
            result.text += chunk.get("textFragment", "")
        elif tn == "StartToolBlock":
            current_tool = ToolCall(
                tool_type=chunk.get("toolType", ""),
                tool_id=chunk.get("toolId", ""),
            )
            result.tool_calls.append(current_tool)
        elif tn == "DeltaToolInputBlock" and current_tool is not None:
            current_tool.input_json += chunk.get("jsonFragment", "")
        elif tn == "CompleteToolBlock" and current_tool is not None:
            err = (chunk.get("toolError") or {}).get("message")
            if err:
                current_tool.error = err
            current_tool = None
        elif tn == "CompleteChatStream":
            result.suggested_replies = chunk.get("suggestedReplies") or []
        elif tn in ("AISummaryError", "PythonError"):
            result.error = chunk.get("message") or tn
    return result


async def _stream(
    graphql_http_url: str,
    api_token: str,
    query: str,
    variables: dict[str, Any],
    response_field: str,
    timeout_seconds: float,
) -> AsyncIterator[dict[str, Any]]:
    ws_url = _http_to_ws(graphql_http_url)
    auth_header = f"Bearer {api_token}"

    # Dagster Cloud accepts auth via upgrade headers *and* via connection_init
    # payload. We send both for max compatibility across server versions.
    additional_headers = [("Authorization", auth_header)]
    connection_init_payload = {
        "Authorization": auth_header,
        "authorization": auth_header,
        "headers": {"Authorization": auth_header},
    }

    async with websockets.connect(
        ws_url,
        subprotocols=["graphql-ws"],  # legacy subprotocol name, not the newer graphql-transport-ws
        additional_headers=additional_headers,
        open_timeout=15,
        ping_interval=20,
    ) as ws:
        await ws.send(json.dumps({"type": "connection_init", "payload": connection_init_payload}))
        # Wait for connection_ack (and ignore ka frames that some servers emit)
        while True:
            ack_raw = await asyncio.wait_for(ws.recv(), timeout=15)
            ack = json.loads(ack_raw)
            if ack.get("type") == "connection_ack":
                break
            if ack.get("type") == "connection_error":
                raise RuntimeError(f"Compass connection_error: {ack.get('payload')}")

        sub_id = "1"
        await ws.send(
            json.dumps(
                {
                    "id": sub_id,
                    "type": "start",
                    "payload": {"query": query, "variables": variables},
                }
            )
        )

        deadline = asyncio.get_event_loop().time() + timeout_seconds
        try:
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    yield {
                        "__typename": "AISummaryError",
                        "message": f"Compass stream timed out after {timeout_seconds}s",
                    }
                    return
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    yield {
                        "__typename": "AISummaryError",
                        "message": f"Compass stream timed out after {timeout_seconds}s",
                    }
                    return
                msg = json.loads(raw)
                mtype = msg.get("type")
                if mtype == "data":
                    data = (msg.get("payload") or {}).get("data") or {}
                    chunk = data.get(response_field)
                    if chunk:
                        yield chunk
                elif mtype in ("complete", "error"):
                    return
                # ignore 'ka' keepalives and anything else
        finally:
            with contextlib.suppress(Exception):
                await ws.send(json.dumps({"id": sub_id, "type": "stop"}))
