import asyncio
import json
import math
import random
import uuid
from collections.abc import AsyncIterator

import pytest

from app.cancel import DeferredCanceller
from app.gateway import GatewayService
from app.generation import GenerationRegistry, GenerationStatus
from app.producer import Producer, ProducerConfig
from app.sse import SseEncoder
from app.store.lease import StaleLeaseError
from app.store.protocol import TerminalStatus
from app.store.subscribers import InMemorySubscriberRegistry
from app.upstream import SyntheticAdapter
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore


def _parse_sse_frame(frame: str) -> tuple[str | None, str, dict[str, object]]:
  event_id: str | None = None
  event_type = ""
  data: dict[str, object] = {}
  for line in frame.splitlines():
    if line.startswith("id: "):
      event_id = line[4:]
    elif line.startswith("event: "):
      event_type = line[7:]
    elif line.startswith("data: "):
      data = json.loads(line[6:])
  return event_id, event_type, data


async def test_chaos_subscriber_churn_and_reconnect_reassembly() -> None:
  """I1, I2, I5, I7: Churning subscribers + Last-Event-ID reconnects reassemble byte-for-byte."""
  tokens = [f"w{i}-" for i in range(40)]
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.15,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=lease_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=SyntheticAdapter(tokens=tokens, tokens_per_second=120.0),
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.02),
  )

  gen_id, created = await service.start_or_attach(f"churn-{uuid.uuid4()}", "churn")
  assert created is True

  rng = random.Random(42)
  received_a: list[tuple[int, str]] = []
  last_id_a: str | None = None

  async def churn_client_b() -> list[int]:
    indices_b: list[int] = []
    async for frame in service.stream_sse(gen_id):
      if frame.startswith(":"):
        continue
      _, ev_type, data = _parse_sse_frame(frame)
      if ev_type == "token":
        indices_b.append(int(str(data["index"])))
        if len(indices_b) >= 12:
          break
    return indices_b

  b_task = asyncio.create_task(churn_client_b())

  # Client A disconnects randomly every 3-7 tokens and reconnects within < T.
  finished = False
  while not finished:
    cut_after = rng.randint(3, 7)
    leg_count = 0
    async for frame in service.stream_sse(gen_id, last_event_id=last_id_a):
      if frame.startswith(":"):
        continue
      ev_id, ev_type, data = _parse_sse_frame(frame)
      if ev_type == "token":
        received_a.append((int(str(data["index"])), str(data["text"])))
        last_id_a = ev_id
        leg_count += 1
        if leg_count >= cut_after:
          break
      elif ev_type == "done":
        finished = True
        break
    if not finished:
      await asyncio.sleep(0.01)

  indices_b = await b_task
  assert indices_b == list(range(len(indices_b)))

  indices_a = [idx for idx, _ in received_a]
  assert indices_a == list(range(len(tokens)))
  assert "".join(text for _, text in received_a) == "".join(tokens)

  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.COMPLETED


async def test_chaos_gc_pause_lease_takeover() -> None:
  """I3: Lease expiry on GC pause lets Replica B take over and fences out Replica A."""
  clock = 0.0
  lease_store = InMemoryLeaseStore(clock=lambda: clock)
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  gen_id = f"gc-{uuid.uuid4()}"

  lease_a = await lease_store.acquire_lease(gen_id, "replica-a", ttl_ms=200)
  assert lease_a is not None and lease_a.fence == 1
  for i in range(3):
    await gen_store.append_token(gen_id, f"t{i}", fence=lease_a.fence)

  # Replica A freezes in GC past 200ms TTL; Replica B takes over with fence=2.
  clock += 0.35
  lease_b = await lease_store.acquire_lease(gen_id, "replica-b", ttl_ms=200)
  assert lease_b is not None and lease_b.fence == 2
  await gen_store.append_token(gen_id, "t3", fence=lease_b.fence)

  # Replica A wakes up and attempts fenced writes -> rejected with StaleLeaseError.
  with pytest.raises(StaleLeaseError):
    await gen_store.append_token(gen_id, "stale", fence=lease_a.fence)
  with pytest.raises(StaleLeaseError):
    await gen_store.mark_terminal(gen_id, TerminalStatus.FAILED, fence=lease_a.fence)

  await gen_store.mark_terminal(gen_id, TerminalStatus.COMPLETED, fence=lease_b.fence)
  events = [e async for e in gen_store.read_from(gen_id)]
  assert [e.data for e in events] == ["t0", "t1", "t2", "t3"]


async def test_chaos_post_abandonment_token_bound() -> None:
  """I8: Post-abandonment tokens stay bounded by (T + heartbeat_interval) * token_rate."""
  token_rate = 100.0
  drain_t = 0.05
  heartbeat = 0.02

  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=drain_t,
  )

  gen_id, _ = await registry.get_or_create(f"abandon-{uuid.uuid4()}")
  await registry.transition(gen_id, GenerationStatus.RUNNING)

  adapter = SyntheticAdapter(
    tokens=[f"t{i}" for i in range(200)],
    tokens_per_second=token_rate,
  )
  producer = Producer(
    gen_store=gen_store,
    lease_store=lease_store,
    owner_id="rep-1",
    config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=heartbeat),
  )

  await canceller.on_subscribe(gen_id, "sub-1")
  producer_task = asyncio.create_task(producer.run(gen_id, adapter.stream("abandon")))

  # Let a few tokens land, then abandon completely.
  await asyncio.sleep(0.04)
  count_at_abandon = len([e async for e in gen_store.read_from(gen_id)])
  await canceller.on_unsubscribe(gen_id, "sub-1")

  await asyncio.wait_for(producer_task, timeout=0.5)

  total_tokens = len([e async for e in gen_store.read_from(gen_id)])
  post_abandon = total_tokens - count_at_abandon
  max_allowed = math.ceil((drain_t + heartbeat) * token_rate) + 1

  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.EXPIRED
  assert post_abandon <= max_allowed


async def test_chaos_concurrent_duplicate_burst() -> None:
  """I9: 25 concurrent duplicate requests attach to one generation with 1 upstream call."""
  upstream_calls = 0

  class _CountingAdapter:
    async def stream(self, prompt: str, *, model: str | None = None) -> AsyncIterator[str]:
      nonlocal upstream_calls
      _ = (prompt, model)
      upstream_calls += 1
      await asyncio.sleep(0.03)
      yield "ok"

  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.2,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=lease_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=_CountingAdapter(),
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.02),
  )

  results = await asyncio.gather(
    *(service.start_or_attach("burst-key", "hello") for _ in range(25))
  )

  gen_ids = {gid for gid, _ in results}
  assert len(gen_ids) == 1
  assert sum(1 for _, created in results if created) == 1
  assert sum(1 for _, created in results if not created) == 24
  await asyncio.sleep(0.06)
  assert upstream_calls == 1
