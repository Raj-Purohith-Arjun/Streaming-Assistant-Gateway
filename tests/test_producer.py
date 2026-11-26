import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest

from app.producer import Producer, ProducerConfig
from app.store.lease import Lease, StaleLeaseError
from app.store.protocol import TerminalStatus
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore


class _SpyLeaseStore(InMemoryLeaseStore):
  """LeaseStore spy recording renew and release calls."""

  def __init__(self) -> None:
    super().__init__()
    self.renew_calls = 0
    self.release_calls = 0
    self.fail_next_renew = False

  async def renew_lease(
    self,
    gen_id: str,
    owner_id: str,
    fence: int,
    *,
    ttl_ms: int,
  ) -> Lease:
    self.renew_calls += 1
    if self.fail_next_renew:
      raise StaleLeaseError("lease lost during renewal")
    return await super().renew_lease(gen_id, owner_id, fence, ttl_ms=ttl_ms)

  async def release_lease(self, gen_id: str, owner_id: str, fence: int) -> bool:
    self.release_calls += 1
    return await super().release_lease(gen_id, owner_id, fence)


async def test_producer_streams_and_marks_completed() -> None:
  """Producer acquires lease, streams tokens with fence, renews via heartbeat, marks COMPLETED."""
  lease_store = _SpyLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  gen_id = f"gen-{uuid.uuid4()}"
  config = ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.015)
  producer = Producer(gen_store=gen_store, lease_store=lease_store, owner_id="rep-1", config=config)

  async def upstream() -> AsyncIterator[str]:
    yield "hel"
    await asyncio.sleep(0.05)
    yield "lo"

  ran = await producer.run(gen_id, upstream())
  assert ran is True

  events = [e async for e in gen_store.read_from(gen_id)]
  assert [e.data for e in events] == ["hel", "lo"]
  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.COMPLETED
  assert lease_store.renew_calls >= 1
  assert lease_store.release_calls == 1
  assert await lease_store.current_fence(gen_id) is None


async def test_producer_aborts_on_lost_lease() -> None:
  """When renew_lease or append_token raises StaleLeaseError, abort upstream without terminal."""
  lease_store = _SpyLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  gen_id = f"gen-{uuid.uuid4()}"
  config = ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.01)
  producer = Producer(gen_store=gen_store, lease_store=lease_store, owner_id="rep-1", config=config)

  consumed: list[str] = []

  async def slow_upstream() -> AsyncIterator[str]:
    consumed.append("tok-0")
    yield "tok-0"
    lease_store.fail_next_renew = True
    await asyncio.sleep(0.1)
    consumed.append("tok-1")
    yield "tok-1"

  with pytest.raises(StaleLeaseError):
    await producer.run(gen_id, slow_upstream())

  assert consumed == ["tok-0"]
  assert await gen_store.read_terminal(gen_id) is None


async def test_producer_marks_failed_on_upstream_error() -> None:
  """When upstream raises, producer marks FAILED with fence and releases the lease."""
  lease_store = _SpyLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  gen_id = f"gen-{uuid.uuid4()}"
  config = ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.05)
  producer = Producer(gen_store=gen_store, lease_store=lease_store, owner_id="rep-1", config=config)

  async def failing_upstream() -> AsyncIterator[str]:
    yield "partial"
    raise RuntimeError("upstream boom")

  with pytest.raises(RuntimeError, match="upstream boom"):
    await producer.run(gen_id, failing_upstream())

  events = [e async for e in gen_store.read_from(gen_id)]
  assert [e.data for e in events] == ["partial"]
  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.FAILED
  assert lease_store.release_calls == 1
  assert await lease_store.current_fence(gen_id) is None


async def test_producer_skips_when_lease_held() -> None:
  """I3: If acquire_lease returns None, producer does not consume upstream."""
  lease_store = _SpyLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  gen_id = f"gen-{uuid.uuid4()}"
  await lease_store.acquire_lease(gen_id, "other-replica", ttl_ms=5000)

  upstream_started = False

  async def upstream() -> AsyncIterator[str]:
    nonlocal upstream_started
    upstream_started = True
    yield "never-read"

  producer = Producer(gen_store=gen_store, lease_store=lease_store, owner_id="rep-2")
  ran = await producer.run(gen_id, upstream())

  assert ran is False
  assert upstream_started is False
  assert [e async for e in gen_store.read_from(gen_id)] == []
