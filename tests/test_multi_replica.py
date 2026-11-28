import asyncio
import contextlib
import json
import socket
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.cancel import DeferredCanceller
from app.core.config import get_settings
from app.gateway import GatewayService, create_app
from app.generation import RedisGenerationRegistry
from app.producer import ProducerConfig
from app.sse import SseEncoder
from app.store.redis_store import RedisGenerationStore
from app.store.subscribers import RedisSubscriberRegistry
from app.upstream import SyntheticAdapter, UpstreamAdapter


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
  redis_url = get_settings().redis_url
  client = Redis.from_url(redis_url, socket_connect_timeout=0.5, decode_responses=True)
  try:
    await client.ping()
  except (RedisError, TimeoutError, OSError):
    await client.aclose()
    pytest.skip(f"Redis not reachable at {redis_url}")

  try:
    yield client
  finally:
    await client.aclose()


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


def _build_replica(
  redis: Redis,
  *,
  upstream: UpstreamAdapter,
  drain_timeout_seconds: float = 0.25,
) -> tuple[GatewayService, DeferredCanceller]:
  gen_store = RedisGenerationStore(redis, ttl_seconds=120)
  subscribers = RedisSubscriberRegistry(redis, ttl_seconds=5.0)
  registry = RedisGenerationRegistry(redis, ttl_seconds=120)
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=drain_timeout_seconds,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=gen_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=upstream,
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.02),
  )
  return service, canceller


@contextlib.asynccontextmanager
async def _uvicorn_server(app: FastAPI) -> AsyncIterator[str]:
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.bind(("127.0.0.1", 0))
  sock.listen(128)
  port = sock.getsockname()[1]
  config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
  server = uvicorn.Server(config)
  task = asyncio.create_task(server.serve(sockets=[sock]))
  try:
    while not server.started:
      await asyncio.sleep(0.01)
    yield f"http://127.0.0.1:{port}"
  finally:
    server.force_exit = True
    server.should_exit = True
    with contextlib.suppress(TimeoutError):
      await asyncio.wait_for(task, timeout=2.0)
    sock.close()


async def test_cross_replica_disconnect_and_resume_last_event_id(redis_client: Redis) -> None:
  """I1, I2, I5, I7: Disconnect on replica A and resume via Last-Event-ID on replica B."""
  tokens = ["t0", "t1", "t2", "t3", "t4", "t5"]
  service_a, cancel_a = _build_replica(
    redis_client,
    upstream=SyntheticAdapter(tokens=tokens, tokens_per_second=10.0),
    drain_timeout_seconds=0.25,
  )
  service_b, cancel_b = _build_replica(
    redis_client,
    upstream=SyntheticAdapter(tokens=tokens, tokens_per_second=10.0),
    drain_timeout_seconds=0.25,
  )
  app_a = create_app(service=service_a)
  app_b = create_app(service=service_b)
  payload = {"session_id": f"s-{uuid.uuid4()}", "user_id": "u1", "prompt": "resume"}

  first_two: list[tuple[str | None, str, dict[str, object]]] = []
  resumed: list[tuple[str | None, str, dict[str, object]]] = []
  gen_id: str | None = None

  try:
    async with (
      _uvicorn_server(app_a) as url_a,
      _uvicorn_server(app_b) as url_b,
      httpx.AsyncClient(base_url=url_a) as client_a,
      httpx.AsyncClient(base_url=url_b) as client_b,
    ):
      async with client_a.stream("POST", "/v1/stream", json=payload) as resp:
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
            if parsed[1] == "token":
              first_two.append(parsed)
              if len(first_two) == 2:
                break
          if len(first_two) == 2:
            break

      assert [ev[2]["text"] for ev in first_two] == ["t0", "t1"]
      resume_id = first_two[-1][0]
      assert resume_id is not None

      async with client_b.stream(
        "GET",
        f"/v1/generations/{gen_id}/stream",
        headers={"Last-Event-ID": resume_id},
      ) as resp_b:
        assert resp_b.status_code == 200
        buf = ""
        async for chunk in resp_b.aiter_text():
          buf += chunk
          while "\n\n" in buf:
            block, buf = buf.split("\n\n", 1)
            if block.startswith(":"):
              continue
            resumed.append(_parse_sse_block(block))
  finally:
    await cancel_a.aclose()
    await cancel_b.aclose()

  resumed_tokens = [str(ev[2]["text"]) for ev in resumed if ev[1] == "token"]
  done_events = [ev for ev in resumed if ev[1] == "done"]
  assert resumed_tokens == ["t2", "t3", "t4", "t5"]
  assert [str(ev[2]["text"]) for ev in first_two] + resumed_tokens == tokens
  assert len(done_events) == 1
  assert done_events[0][2]["status"] == "COMPLETED"


async def test_cross_replica_coalescing_single_upstream_call(redis_client: Redis) -> None:
  """I3, I9: Concurrent identical requests across replicas trigger one upstream call."""
  tokens = ["c0", "c1", "c2", "c3"]

  class _CountingAdapter:
    def __init__(self) -> None:
      self.calls = 0
      self._inner = SyntheticAdapter(tokens=tokens, tokens_per_second=40.0)

    async def stream(self, prompt: str, *, model: str | None = None) -> AsyncIterator[str]:
      self.calls += 1
      async for tok in self._inner.stream(prompt, model=model):
        yield tok

  counting = _CountingAdapter()
  service_a, cancel_a = _build_replica(redis_client, upstream=counting)
  service_b, cancel_b = _build_replica(redis_client, upstream=counting)
  app_a = create_app(service=service_a)
  app_b = create_app(service=service_b)
  payload = {"session_id": f"coal-{uuid.uuid4()}", "user_id": "u1", "prompt": "same"}

  async def _consume(client: httpx.AsyncClient) -> tuple[str | None, list[str]]:
    seen: list[str] = []
    gid: str | None = None
    async with client.stream("POST", "/v1/stream", json=payload) as resp:
      assert resp.status_code == 200
      gid = resp.headers.get("x-generation-id")
      buf = ""
      async for chunk in resp.aiter_text():
        buf += chunk
        while "\n\n" in buf:
          block, buf = buf.split("\n\n", 1)
          if block.startswith(":"):
            continue
          _, ev_type, data = _parse_sse_block(block)
          if ev_type == "token":
            seen.append(str(data["text"]))
    return gid, seen

  try:
    async with (
      _uvicorn_server(app_a) as url_a,
      _uvicorn_server(app_b) as url_b,
      httpx.AsyncClient(base_url=url_a) as client_a,
      httpx.AsyncClient(base_url=url_b) as client_b,
    ):
      (gid_a, toks_a), (gid_b, toks_b) = await asyncio.gather(
        _consume(client_a),
        _consume(client_b),
      )
  finally:
    await cancel_a.aclose()
    await cancel_b.aclose()

  assert gid_a is not None
  assert gid_a == gid_b
  assert toks_a == tokens
  assert toks_b == tokens
  assert counting.calls == 1


async def test_cross_replica_explicit_stop(redis_client: Redis) -> None:
  """I4: Explicit stop on replica B terminates replica A's stream without COMPLETED."""
  tokens = [f"s{i}" for i in range(20)]
  service_a, cancel_a = _build_replica(
    redis_client,
    upstream=SyntheticAdapter(tokens=tokens, tokens_per_second=10.0),
  )
  service_b, cancel_b = _build_replica(
    redis_client,
    upstream=SyntheticAdapter(tokens=tokens, tokens_per_second=10.0),
  )
  app_a = create_app(service=service_a)
  app_b = create_app(service=service_b)
  payload = {"session_id": f"stop-{uuid.uuid4()}", "user_id": "u1", "prompt": "halt"}

  events: list[tuple[str | None, str, dict[str, object]]] = []

  try:
    async with (
      _uvicorn_server(app_a) as url_a,
      _uvicorn_server(app_b) as url_b,
      httpx.AsyncClient(base_url=url_a) as client_a,
      httpx.AsyncClient(base_url=url_b) as client_b,
    ):
      stopped_sent = False
      async with client_a.stream("POST", "/v1/stream", json=payload) as resp:
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
            events.append(parsed)
            if not stopped_sent and parsed[1] == "token":
              stop_resp = await client_b.post(f"/v1/generations/{gen_id}/stop")
              assert stop_resp.status_code == 200
              stop_body = stop_resp.json()
              assert stop_body["gen_id"] == gen_id
              assert stop_body["status"] == "STOPPED"
              stopped_sent = True
  finally:
    await cancel_a.aclose()
    await cancel_b.aclose()

  done_events = [ev for ev in events if ev[1] == "done"]
  token_events = [ev for ev in events if ev[1] == "token"]
  assert len(done_events) == 1
  assert done_events[0][2]["status"] == "STOPPED"
  assert len(token_events) < len(tokens)
