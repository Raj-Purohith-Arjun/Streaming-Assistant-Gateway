import asyncio
import json
from collections.abc import AsyncIterator

import httpx

from app.cancel import DeferredCanceller
from app.gateway import GatewayService, create_app
from app.generation import GenerationRegistry
from app.producer import ProducerConfig
from app.sse import SseEncoder
from app.store.protocol import TerminalStatus
from app.store.subscribers import InMemorySubscriberRegistry
from app.upstream import SyntheticAdapter
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore


def _parse_sse_block(block: str) -> tuple[str | None, str, dict[str, object]]:
  event_id: str | None = None
  event_type = ""
  data: dict[str, object] = {}
  for line in block.splitlines():
    if line.startswith("id: "):
      event_id = line[4:]
    elif line.startswith("event: "):
      event_type = line[7:]
    elif line.startswith("data: "):
      data = json.loads(line[6:])
  return event_id, event_type, data


def _build_test_service(
  *,
  tokens: list[str],
  tokens_per_second: float = 500.0,
  drain_timeout_seconds: float = 0.2,
  upstream_override: object | None = None,
) -> tuple[GatewayService, InMemoryGenerationStore]:
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=drain_timeout_seconds,
  )
  upstream = upstream_override or SyntheticAdapter(
    tokens=tokens,
    tokens_per_second=tokens_per_second,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=lease_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=upstream,  # type: ignore[arg-type]
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.02),
  )
  return service, gen_store


async def test_gateway_stream_and_resume_last_event_id() -> None:
  """I1, I2, I5: Disconnect mid-stream and resume at Last-Event-ID with zero gaps/duplicates."""
  service, _ = _build_test_service(tokens=["a", "b", "c", "d"], tokens_per_second=200.0)
  app = create_app(service=service)
  payload = {"session_id": "s1", "user_id": "u1", "prompt": "hi"}

  first_two: list[tuple[str | None, str, dict[str, object]]] = []
  gen_id: str | None = None

  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=app),
    base_url="http://test",
  ) as client:
    async with client.stream("POST", "/v1/stream", json=payload) as resp:
      assert resp.status_code == 200
      gen_id = resp.headers.get("x-generation-id")
      assert gen_id is not None
      buf = ""
      async for chunk in resp.aiter_text():
        buf += chunk
        while "\n\n" in buf:
          block, buf = buf.split("\n\n", 1)
          if block.startswith(":"):
            continue
          parsed = _parse_sse_block(block)
          first_two.append(parsed)
          if len(first_two) == 2:
            break
        if len(first_two) == 2:
          break

    assert [ev[2]["text"] for ev in first_two] == ["a", "b"]
    resume_id = first_two[-1][0]
    assert resume_id is not None

    resumed: list[tuple[str | None, str, dict[str, object]]] = []
    async with client.stream(
      "GET",
      f"/v1/generations/{gen_id}/stream",
      headers={"Last-Event-ID": resume_id},
    ) as resp:
      assert resp.status_code == 200
      buf = ""
      async for chunk in resp.aiter_text():
        buf += chunk
        while "\n\n" in buf:
          block, buf = buf.split("\n\n", 1)
          if block.startswith(":"):
            continue
          resumed.append(_parse_sse_block(block))

  token_events = [ev for ev in resumed if ev[1] == "token"]
  done_events = [ev for ev in resumed if ev[1] == "done"]
  assert [ev[2]["text"] for ev in token_events] == ["c", "d"]
  assert len(done_events) == 1
  reassembled = "".join(str(ev[2]["text"]) for ev in first_two + token_events)
  assert reassembled == "abcd"


async def test_gateway_coalesces_duplicate_requests() -> None:
  """I9: Duplicate POST /v1/stream requests attach to one generation and call upstream once."""
  upstream_calls = 0

  class _CountingAdapter:
    async def stream(self, prompt: str, *, model: str | None = None) -> AsyncIterator[str]:
      nonlocal upstream_calls
      _ = (prompt, model)
      upstream_calls += 1
      for tok in ["x", "y"]:
        await asyncio.sleep(0.02)
        yield tok

  service, _ = _build_test_service(tokens=[], upstream_override=_CountingAdapter())
  app = create_app(service=service)
  payload = {"session_id": "s-dup", "user_id": "u-dup", "prompt": "same"}

  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=app),
    base_url="http://test",
  ) as client:
    r1, r2 = await asyncio.gather(
      client.post("/v1/stream", json=payload),
      client.post("/v1/stream", json=payload),
    )

  assert r1.status_code == 200
  assert r2.status_code == 200
  assert r1.headers.get("x-generation-id") == r2.headers.get("x-generation-id")
  assert upstream_calls == 1


async def test_gateway_explicit_stop_skips_draining() -> None:
  """Explicit POST /v1/generations/{gen_id}/stop marks STOPPED immediately."""
  started = asyncio.Event()

  class _SlowStopAdapter:
    async def stream(self, prompt: str, *, model: str | None = None) -> AsyncIterator[str]:
      _ = (prompt, model)
      yield "t0"
      started.set()
      await asyncio.sleep(5.0)
      yield "t1"

  service, gen_store = _build_test_service(
    tokens=[],
    drain_timeout_seconds=10.0,
    upstream_override=_SlowStopAdapter(),
  )
  app = create_app(service=service)
  gen_id, _ = await service.start_or_attach("req-stop", "slow")
  await started.wait()

  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=app),
    base_url="http://test",
  ) as client:
    stop_resp = await asyncio.wait_for(
      client.post(f"/v1/generations/{gen_id}/stop"),
      timeout=0.5,
    )
    assert stop_resp.status_code == 200
    assert stop_resp.json()["status"] == "STOPPED"

  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.STOPPED


async def test_gateway_disconnect_revives_on_reconnect() -> None:
  """I7: Reconnecting during DRAINING cancels drain timer and completes the stream."""
  service, gen_store = _build_test_service(
    tokens=["p1", "p2", "p3"],
    tokens_per_second=25.0,
    drain_timeout_seconds=0.15,
  )
  app = create_app(service=service)
  payload = {"session_id": "s-revive", "user_id": "u-revive", "prompt": "revive"}

  first_events: list[tuple[str | None, str, dict[str, object]]] = []
  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=app),
    base_url="http://test",
  ) as client:
    async with client.stream("POST", "/v1/stream", json=payload) as resp:
      gen_id = resp.headers["x-generation-id"]
      buf = ""
      async for chunk in resp.aiter_text():
        buf += chunk
        if "\n\n" in buf:
          block, buf = buf.split("\n\n", 1)
          if not block.startswith(":"):
            first_events.append(_parse_sse_block(block))
            break

    # Briefly disconnected -> DRAINING, then reconnect before drain_timeout_seconds=0.15s.
    await asyncio.sleep(0.03)
    last_id = first_events[-1][0]
    assert last_id is not None

    resumed_tokens: list[str] = []
    saw_done = False
    async with client.stream(
      "GET",
      f"/v1/generations/{gen_id}/stream",
      headers={"Last-Event-ID": last_id},
    ) as resp:
      assert resp.status_code == 200
      buf = ""
      async for chunk in resp.aiter_text():
        buf += chunk
        while "\n\n" in buf:
          block, buf = buf.split("\n\n", 1)
          if block.startswith(":"):
            continue
          _, ev_type, data = _parse_sse_block(block)
          if ev_type == "token":
            resumed_tokens.append(str(data["text"]))
          elif ev_type == "done":
            saw_done = True

  assert saw_done is True
  assert str(first_events[0][2]["text"]) + "".join(resumed_tokens) == "p1p2p3"
  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.COMPLETED
