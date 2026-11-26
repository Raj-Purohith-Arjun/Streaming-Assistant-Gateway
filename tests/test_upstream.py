import contextlib
import json

import httpx
import pytest
import respx

from app.upstream import GeminiAdapter, GroqAdapter, SyntheticAdapter, UpstreamAdapter


async def test_synthetic_adapter_streams_tokens_in_order() -> None:
  """SyntheticAdapter satisfies UpstreamAdapter and yields tokens in order."""
  adapter = SyntheticAdapter(tokens=["a", "b", "c"], tokens_per_second=1000)
  assert isinstance(adapter, UpstreamAdapter)

  received = [tok async for tok in adapter.stream("hello")]
  assert received == ["a", "b", "c"]


async def test_synthetic_adapter_fails_at_configured_index() -> None:
  """SyntheticAdapter raises after yielding fail_after_tokens tokens."""
  adapter = SyntheticAdapter(
    tokens=["a", "b", "c", "d"],
    tokens_per_second=1000,
    fail_after_tokens=2,
  )
  received: list[str] = []

  with pytest.raises(RuntimeError):
    async for tok in adapter.stream("hello"):
      received.append(tok)

  assert received == ["a", "b"]


async def test_synthetic_adapter_closes_cleanly_on_abort() -> None:
  """Breaking early and calling aclose() exits cleanly without hanging."""
  adapter = SyntheticAdapter(tokens=["a", "b", "c", "d"], tokens_per_second=10)
  stream = adapter.stream("hello")

  received: list[str] = []
  with contextlib.suppress(StopAsyncIteration):
    received.append(await anext(stream))
  await stream.aclose()

  assert received == ["a"]


@respx.mock
async def test_gemini_adapter_parses_stream_chunks() -> None:
  """GeminiAdapter parses Gemini SSE streamGenerateContent chunks into text deltas."""
  sse_body = "".join(
    [
      f"data: {json.dumps({'candidates': [{'content': {'parts': [{'text': 'Hel'}]}}]})}\n\n",
      f"data: {json.dumps({'candidates': [{'content': {'parts': [{'text': 'lo!'}]}}]})}\n\n",
    ]
  )
  respx.post(url__regex=r"https://generativelanguage\.googleapis\.com/.+:streamGenerateContent").mock(
    return_value=httpx.Response(
      200,
      content=sse_body.encode("utf-8"),
      headers={"content-type": "text/event-stream"},
    )
  )

  adapter = GeminiAdapter(api_key="test-key", model="gemini-2.0-flash")
  assert isinstance(adapter, UpstreamAdapter)
  received = [tok async for tok in adapter.stream("hi")]
  assert received == ["Hel", "lo!"]


@respx.mock
async def test_groq_adapter_parses_stream_chunks() -> None:
  """GroqAdapter parses OpenAI-compatible SSE chat completion chunks into text deltas."""
  sse_body = "".join(
    [
      f"data: {json.dumps({'choices': [{'delta': {'role': 'assistant'}}]})}\n\n",
      f"data: {json.dumps({'choices': [{'delta': {'content': 'foo'}}]})}\n\n",
      f"data: {json.dumps({'choices': [{'delta': {'content': 'bar'}}]})}\n\n",
      "data: [DONE]\n\n",
    ]
  )
  respx.post("https://api.groq.com/openai/v1/chat/completions").mock(
    return_value=httpx.Response(
      200,
      content=sse_body.encode("utf-8"),
      headers={"content-type": "text/event-stream"},
    )
  )

  adapter = GroqAdapter(api_key="test-key", model="llama-3.3-70b-versatile")
  assert isinstance(adapter, UpstreamAdapter)
  received = [tok async for tok in adapter.stream("hi")]
  assert received == ["foo", "bar"]
