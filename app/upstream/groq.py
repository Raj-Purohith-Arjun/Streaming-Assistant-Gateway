import json
from collections.abc import AsyncGenerator

from app.upstream.base import stream_sse_post


class GroqAdapter:
  """Streaming adapter for Groq OpenAI-compatible chat completions SSE API."""

  def __init__(
    self,
    api_key: str,
    model: str = "llama-3.3-70b-versatile",
    base_url: str = "https://api.groq.com/openai/v1",
  ) -> None:
    if not api_key:
      raise ValueError("api_key must be non-empty")
    self._api_key = api_key
    self._model = model
    self._base_url = base_url.rstrip("/")

  async def stream(self, prompt: str, *, model: str | None = None) -> AsyncGenerator[str, None]:
    """Stream text deltas from Groq chat completions."""
    target_model = model or self._model
    url = f"{self._base_url}/chat/completions"
    payload = {
      "model": target_model,
      "messages": [{"role": "user", "content": prompt}],
      "stream": True,
    }

    async for raw_data in stream_sse_post(
      url=url,
      headers={"authorization": f"Bearer {self._api_key}"},
      payload=payload,
    ):
      if not raw_data:
        continue
      if raw_data == "[DONE]":
        break
      chunk = json.loads(raw_data)
      for choice in chunk.get("choices", []):
        content = choice.get("delta", {}).get("content")
        if isinstance(content, str) and content:
          yield content
