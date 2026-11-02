from collections.abc import AsyncIterator


class UpstreamClient:
  """Placeholder upstream streaming client."""

  async def stream_query(self, session_id: str, prompt: str) -> AsyncIterator[str]:
    for token in ["Processing", " your", " request", "..."]:
      yield f"{session_id}:{prompt}:{token}"
