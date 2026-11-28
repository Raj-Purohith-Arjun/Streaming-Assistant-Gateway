from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from examples.resumable_client import ResumableStreamClient, StreamEvent


class _ScriptedSseStream(httpx.AsyncByteStream):
  """Async byte stream yielding chunks and optionally raising mid-stream."""

  def __init__(self, chunks: list[bytes], fail_with: Exception | None = None) -> None:
    self._chunks = chunks
    self._fail_with = fail_with

  async def __aiter__(self) -> AsyncIterator[bytes]:
    for chunk in self._chunks:
      yield chunk
    if self._fail_with is not None:
      raise self._fail_with


def _sse_block(event_id: str, event_type: str, payload: str) -> bytes:
  return f"id: {event_id}\nevent: {event_type}\ndata: {payload}\n\n".encode()


async def test_client_streams_all_tokens_without_disconnect() -> None:
  async def handler(request: httpx.Request) -> httpx.Response:
    assert request.method == "POST"
    assert request.url.path == "/v1/stream"
    chunks = [
      b": heartbeat\n\n",
      _sse_block("v1:0:1-0", "token", '{"index": 0, "text": "Hello"}'),
      _sse_block("v1:1:2-0", "token", '{"index": 1, "text": " world"}'),
      _sse_block("v1:1:2-0", "done", '{"status": "COMPLETED", "last_index": 1}'),
    ]
    return httpx.Response(
      200,
      headers={"x-generation-id": "gen-1", "content-type": "text/event-stream"},
      stream=_ScriptedSseStream(chunks),
    )

  async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
    client = ResumableStreamClient("http://gateway.test", client=http_client, backoff_seconds=0.0)
    events: list[StreamEvent] = [
      ev async for ev in client.stream(session_id="s1", user_id="u1", prompt="hi")
    ]

  tokens = [ev.data["text"] for ev in events if ev.event_type == "token"]
  assert "".join(tokens) == "Hello world"
  assert events[-1].event_type == "done"
  assert events[-1].data["status"] == "COMPLETED"
  assert client.last_generation_id == "gen-1"
  assert client.last_event_id == "v1:1:2-0"


async def test_client_resumes_with_last_event_id_on_midstream_drop() -> None:
  seen_requests: list[tuple[str, str, str | None]] = []

  async def handler(request: httpx.Request) -> httpx.Response:
    seen_requests.append((request.method, request.url.path, request.headers.get("last-event-id")))
    if request.method == "POST":
      chunks = [
        _sse_block("v1:0:1-0", "token", '{"index": 0, "text": "a"}'),
        _sse_block("v1:1:2-0", "token", '{"index": 1, "text": "b"}'),
      ]
      return httpx.Response(
        200,
        headers={"x-generation-id": "gen-42"},
        stream=_ScriptedSseStream(chunks, fail_with=httpx.ReadError("drop")),
      )

    assert request.url.path == "/v1/generations/gen-42/stream"
    assert request.headers.get("last-event-id") == "v1:1:2-0"
    chunks = [
      _sse_block("v1:2:3-0", "token", '{"index": 2, "text": "c"}'),
      _sse_block("v1:2:3-0", "done", '{"status": "COMPLETED", "last_index": 2}'),
    ]
    return httpx.Response(200, stream=_ScriptedSseStream(chunks))

  async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
    client = ResumableStreamClient("http://gateway.test", client=http_client, backoff_seconds=0.0)
    events = [ev async for ev in client.stream(session_id="s1", user_id="u1", prompt="abc")]

  assert seen_requests == [
    ("POST", "/v1/stream", None),
    ("GET", "/v1/generations/gen-42/stream", "v1:1:2-0"),
  ]
  tokens = [ev.data["text"] for ev in events if ev.event_type == "token"]
  assert tokens == ["a", "b", "c"]
  assert events[-1].event_type == "done"


async def test_client_does_not_retry_after_terminal_error_event() -> None:
  calls = 0

  async def handler(_request: httpx.Request) -> httpx.Response:
    nonlocal calls
    calls += 1
    chunks = [_sse_block("v1:0:0-0", "error", '{"message": "upstream failed"}')]
    return httpx.Response(200, headers={"x-generation-id": "gen-err"}, stream=_ScriptedSseStream(chunks))

  async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
    client = ResumableStreamClient("http://gateway.test", client=http_client, backoff_seconds=0.0)
    events = [ev async for ev in client.stream(session_id="s1", user_id="u1", prompt="err")]

  assert calls == 1
  assert len(events) == 1
  assert events[0].event_type == "error"


async def test_client_raises_after_exhausting_retries() -> None:
  calls = 0

  async def handler(_request: httpx.Request) -> httpx.Response:
    nonlocal calls
    calls += 1
    raise httpx.ConnectError("unreachable")

  async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
    client = ResumableStreamClient(
      "http://gateway.test",
      client=http_client,
      max_retries=2,
      backoff_seconds=0.0,
    )
    with pytest.raises(httpx.ConnectError):
      _ = [ev async for ev in client.stream(session_id="s1", user_id="u1", prompt="down")]

  assert calls == 3


async def test_client_explicit_stop() -> None:
  async def handler(request: httpx.Request) -> httpx.Response:
    assert request.method == "POST"
    assert request.url.path == "/v1/generations/gen-9/stop"
    return httpx.Response(200, json={"generation_id": "gen-9", "status": "STOPPED"})

  async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
    client = ResumableStreamClient("http://gateway.test", client=http_client)
    body: dict[str, Any] = await client.stop("gen-9")

  assert body == {"generation_id": "gen-9", "status": "STOPPED"}
