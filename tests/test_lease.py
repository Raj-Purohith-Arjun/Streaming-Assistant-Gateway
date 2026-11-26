import asyncio
import uuid

import pytest

from app.store.lease import Lease, StaleLeaseError
from app.store.protocol import TerminalStatus
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore


async def test_single_producer() -> None:
  """I3: At most one upstream producer acquires the active lease per gen_id."""
  lease_store = InMemoryLeaseStore()
  gen_id = f"gen-{uuid.uuid4()}"
  in_flight = 0
  max_in_flight = 0

  async def try_produce(owner_id: str) -> Lease | None:
    nonlocal in_flight, max_in_flight
    lease = await lease_store.acquire_lease(gen_id, owner_id, ttl_ms=5000)
    if lease is not None:
      in_flight += 1
      max_in_flight = max(max_in_flight, in_flight)
      await asyncio.sleep(0.02)
      in_flight -= 1
    return lease

  leases = await asyncio.gather(*(try_produce(f"replica-{i}") for i in range(6)))
  acquired = [lease for lease in leases if lease is not None]

  assert len(acquired) == 1
  assert max_in_flight == 1
  assert acquired[0].fence == 1


async def test_stale_fence_rejected() -> None:
  """GC-pause safety: stale fence from an expired lease cannot write or renew."""
  clock = 0.0

  def now() -> float:
    return clock

  lease_store = InMemoryLeaseStore(clock=now)
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  gen_id = f"gen-{uuid.uuid4()}"

  # Replica A acquires lease with fence=1 and writes token 0.
  lease_a = await lease_store.acquire_lease(gen_id, "replica-a", ttl_ms=1000)
  assert lease_a is not None
  assert lease_a.fence == 1
  await gen_store.append_token(gen_id, "tok-0", fence=lease_a.fence)

  # Replica A pauses past the 1000ms lease TTL; Replica B takes over with fence=2.
  clock += 1.5
  lease_b = await lease_store.acquire_lease(gen_id, "replica-b", ttl_ms=1000)
  assert lease_b is not None
  assert lease_b.fence == 2

  # Replica A wakes up and tries to renew or write with stale fence=1 -> all rejected.
  with pytest.raises(StaleLeaseError):
    await lease_store.renew_lease(gen_id, "replica-a", lease_a.fence, ttl_ms=1000)
  with pytest.raises(StaleLeaseError):
    await gen_store.append_token(gen_id, "stale-tok", fence=lease_a.fence)
  with pytest.raises(StaleLeaseError):
    await gen_store.mark_terminal(gen_id, TerminalStatus.FAILED, fence=lease_a.fence)

  # Replica B can still write and close with fence=2.
  await gen_store.append_token(gen_id, "tok-1", fence=lease_b.fence)
  await gen_store.mark_terminal(gen_id, TerminalStatus.COMPLETED, fence=lease_b.fence)

  events = [e async for e in gen_store.read_from(gen_id)]
  assert [e.data for e in events] == ["tok-0", "tok-1"]


async def test_lease_renewal_and_takeover() -> None:
  """Verify renewal preserves fence token and explicit release allows takeover."""
  clock = 0.0
  lease_store = InMemoryLeaseStore(clock=lambda: clock)
  gen_id = f"gen-{uuid.uuid4()}"

  lease_a = await lease_store.acquire_lease(gen_id, "replica-a", ttl_ms=1000)
  assert lease_a is not None
  assert lease_a.fence == 1

  # Renew at t=0.6s extends TTL without changing fence.
  clock += 0.6
  renewed = await lease_store.renew_lease(gen_id, "replica-a", lease_a.fence, ttl_ms=1000)
  assert renewed.fence == 1

  # At t=1.2s (0.6s after renewal), lease is still held by replica-a.
  clock += 0.6
  assert await lease_store.acquire_lease(gen_id, "replica-b", ttl_ms=1000) is None

  # Explicit release lets replica-b acquire with fence=2.
  assert await lease_store.release_lease(gen_id, "replica-a", lease_a.fence) is True
  lease_b = await lease_store.acquire_lease(gen_id, "replica-b", ttl_ms=1000)
  assert lease_b is not None
  assert lease_b.fence == 2


async def test_expired_lease_rejected_before_new_acquire() -> None:
  """Writing with fence=1 after lease expiry (even with no successor) raises StaleLeaseError."""
  clock = 0.0
  lease_store = InMemoryLeaseStore(clock=lambda: clock)
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  gen_id = f"gen-{uuid.uuid4()}"

  lease = await lease_store.acquire_lease(gen_id, "replica-a", ttl_ms=1000)
  assert lease is not None
  assert lease.fence == 1

  # Advance clock past TTL without any other replica acquiring the lease.
  clock += 1.5
  with pytest.raises(StaleLeaseError):
    await gen_store.append_token(gen_id, "after-expiry", fence=lease.fence)
  with pytest.raises(StaleLeaseError):
    await gen_store.mark_terminal(gen_id, TerminalStatus.COMPLETED, fence=lease.fence)
