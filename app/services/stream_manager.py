from collections.abc import AsyncIterator

from app.services.request_coalescer import RequestCoalescer
from app.services.upstream_client import UpstreamClient


class StreamManager:
  def __init__(self, upstream_client: UpstreamClient, coalescer: RequestCoalescer) -> None:
    self._upstream_client = upstream_client
    self._coalescer = coalescer

  async def stream(self, session_id: str, prompt: str) -> AsyncIterator[str]:
    self._coalescer.begin(session_id)
    try:
      async for chunk in self._upstream_client.stream_query(session_id=session_id, prompt=prompt):
        yield chunk
    finally:
      self._coalescer.end(session_id)
