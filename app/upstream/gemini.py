import json
from collections.abc import AsyncGenerator

from app.upstream.base import stream_sse_post


class GeminiAdapter:
  """Streaming adapter for Google Gemini ``streamGenerateContent`` SSE API."""

  def __init__(
    self,
    api_key: str,
    model: str = "gemini-2.0-flash",
    base_url: str = "https://generativelanguage.googleapis.com",
  ) -> None:
    if not api_key:
      raise ValueError("api_key must be non-empty")
    self._api_key = api_key
    self._model = model
    self._base_url = base_url.rstrip("/")

  async def stream(self, prompt: str, *, model: str | None = None) -> AsyncGenerator[str, None]:
    """Stream text deltas from Gemini ``streamGenerateContent``."""
    target_model = model or self._model
    url = f"{self._base_url}/v1beta/models/{target_model}:streamGenerateContent"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    async for raw_data in stream_sse_post(
      url=url,
      headers={"x-goog-api-key": self._api_key},
      payload=payload,
      params={"alt": "sse"},
    ):
      if not raw_data or raw_data == "[DONE]":
        continue
      chunk = json.loads(raw_data)
      for candidate in chunk.get("candidates", []):
        parts = candidate.get("content", {}).get("parts", [])
        for part in parts:
          text = part.get("text")
          if isinstance(text, str) and text:
            yield text
