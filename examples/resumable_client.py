"""Lightweight Python client with automatic ``Last-Event-ID`` resume for Streaming Assistant Gateway.

Run against a local gateway (`docker compose up -d`):
    python -m examples.resumable_client
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True, slots=True)
class StreamEvent:
  """Single parsed SSE frame from the gateway."""

  event_id: str | None
  event_type: str
  data: dict[str, Any]


def _parse_sse_block(block: str) -> StreamEvent | None:
  """Parse one SSE block; return ``None`` for empty blocks or heartbeat comments."""
  if not block:
    return None
  lines = block.splitlines()
  if all(not line or line.startswith(":") for line in lines):
    return None

  event_id: str | None = None
  event_type = "message"
  data_lines: list[str] = []
  for line in lines:
    if line.startswith(":"):
      continue
    if line.startswith("id: "):
      event_id = line[4:]
    elif line.startswith("event: "):
      event_type = line[7:]
    elif line.startswith("data: "):
      data_lines.append(line[6:])

  raw_data = "\n".join(data_lines) if data_lines else "{}"
  data = json.loads(raw_data)
  return StreamEvent(event_id=event_id, event_type=event_type, data=data)


class ResumableStreamClient:
  """Async SSE client that resumes automatically at ``Last-Event-ID`` on disconnect."""

  def __init__(
    self,
    base_url: str,
    *,
    client: httpx.AsyncClient | None = None,
    max_retries: int = 5,
    backoff_seconds: float = 0.25,
  ) -> None:
    if max_retries < 0:
      raise ValueError(f"max_retries must be non-negative, got {max_retries}")
    if backoff_seconds < 0:
      raise ValueError(f"backoff_seconds must be non-negative, got {backoff_seconds}")
    self._base_url = base_url.rstrip("/")
    self._external_client = client
    self._max_retries = max_retries
    self._backoff_seconds = backoff_seconds
    self.last_generation_id: str | None = None
    self.last_event_id: str | None = None

  @asynccontextmanager
  async def _http_client(self) -> AsyncIterator[httpx.AsyncClient]:
    if self._external_client is not None:
      yield self._external_client
      return
    async with httpx.AsyncClient(timeout=None) as owned:
      yield owned

  async def stream(
    self,
    *,
    session_id: str,
    user_id: str,
    prompt: str,
  ) -> AsyncIterator[StreamEvent]:
    """Stream events for a prompt, resuming via ``Last-Event-ID`` across dropped connections."""
    self.last_generation_id = None
    self.last_event_id = None
    payload = {"session_id": session_id, "user_id": user_id, "prompt": prompt}

    async with self._http_client() as http:
      for attempt in range(self._max_retries + 1):
        try:
          async for event in self._read_once(http, payload):
            yield event
            if event.event_type in ("done", "error"):
              return
          if attempt >= self._max_retries:
            raise httpx.RemoteProtocolError("Stream ended before terminal event")
        except httpx.HTTPError:
          if attempt >= self._max_retries:
            raise

        if self._backoff_seconds > 0:
          await asyncio.sleep(self._backoff_seconds * (2**attempt))

  async def _read_once(
    self,
    http: httpx.AsyncClient,
    payload: dict[str, str],
  ) -> AsyncIterator[StreamEvent]:
    if self.last_generation_id is None:
      method = "POST"
      url = f"{self._base_url}/v1/stream"
      headers: dict[str, str] = {"Accept": "text/event-stream"}
      json_body: dict[str, str] | None = payload
    else:
      method = "GET"
      url = f"{self._base_url}/v1/generations/{self.last_generation_id}/stream"
      headers = {"Accept": "text/event-stream"}
      if self.last_event_id is not None:
        headers["Last-Event-ID"] = self.last_event_id
      json_body = None

    async with http.stream(method, url, json=json_body, headers=headers) as resp:
      resp.raise_for_status()
      gen_id = resp.headers.get("x-generation-id")
      if gen_id:
        self.last_generation_id = gen_id

      buf = ""
      async for chunk in resp.aiter_text():
        buf += chunk.replace("\r\n", "\n")
        while "\n\n" in buf:
          block, buf = buf.split("\n\n", 1)
          event = _parse_sse_block(block)
          if event is None:
            continue
          if event.event_id is not None:
            self.last_event_id = event.event_id
          yield event

  async def stop(self, generation_id: str) -> dict[str, Any]:
    """Explicitly stop an active generation immediately."""
    async with self._http_client() as http:
      resp = await http.post(f"{self._base_url}/v1/generations/{generation_id}/stop")
      resp.raise_for_status()
      result: dict[str, Any] = resp.json()
      return result


async def _demo_main() -> None:
  client = ResumableStreamClient("http://localhost:8000")
  async for event in client.stream(
    session_id="sdk-demo",
    user_id="demo-user",
    prompt="Explain resumable SSE streams",
  ):
    if event.event_type == "token":
      print(event.data.get("text", ""), end="", flush=True)
    elif event.event_type in ("done", "error"):
      print(f"\n[{event.event_type}: {event.data}]")


if __name__ == "__main__":
  asyncio.run(_demo_main())
