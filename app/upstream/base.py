from collections.abc import AsyncIterator
from typing import Any, Protocol, runtime_checkable

import httpx


@runtime_checkable
class UpstreamAdapter(Protocol):
  """Abstract adapter protocol for streaming tokens from an LLM upstream."""

  def stream(self, prompt: str, *, model: str | None = None) -> AsyncIterator[str]:
    """Yield text token deltas for ``prompt``."""
    ...


async def stream_sse_post(
  *,
  url: str,
  headers: dict[str, str],
  payload: dict[str, Any],
  params: dict[str, str] | None = None,
  timeout_seconds: float = 60.0,
) -> AsyncIterator[str]:
  """POST JSON to an SSE endpoint and yield stripped ``data:`` payloads."""
  async with (
    httpx.AsyncClient(timeout=timeout_seconds) as client,
    client.stream(
      "POST",
      url,
      headers=headers,
      json=payload,
      params=params,
    ) as response,
  ):
    response.raise_for_status()
    async for line in response.aiter_lines():
      if not line.startswith("data:"):
        continue
      yield line[5:].strip()
