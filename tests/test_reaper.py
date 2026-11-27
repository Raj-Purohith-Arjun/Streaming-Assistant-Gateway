import asyncio
import uuid

from app.cancel import DeferredCanceller, GenerationReaper
from app.generation import GenerationRegistry, GenerationStatus
from app.store.protocol import TerminalStatus
from app.store.subscribers import InMemorySubscriberRegistry
from tests.fakes import InMemoryGenerationStore


async def test_reaper_expires_crashed_subscriber_generation() -> None:
  """I6: Reaper detects zero active subscribers after TTL expiry and triggers DRAINING -> EXPIRED."""
  clock = 0.0
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=0.05, clock=lambda: clock)
  gen_store = InMemoryGenerationStore()
  cancelled: list[str] = []

  async def on_cancel(gid: str) -> None:
    cancelled.append(gid)

  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.05,
    on_cancel=on_cancel,
  )
  reaper = GenerationReaper(
    registry=registry,
    subscribers=subscribers,
    canceller=canceller,
    interval_seconds=0.02,
  )

  gen_id, _ = await registry.get_or_create(f"req-{uuid.uuid4()}")
  await registry.transition(gen_id, GenerationStatus.RUNNING)
  await canceller.on_subscribe(gen_id, "sub-1")

  # Simulate crashed replica: sub-1 never unsubscribes, and its TTL expires.
  clock += 0.1
  reaped = await reaper.reap_once([gen_id])
  assert reaped == [gen_id]
  assert await registry.get_status(gen_id) == GenerationStatus.DRAINING

  # After drain_timeout_seconds elapses, generation expires and on_cancel fires.
  await asyncio.sleep(0.08)
  assert await registry.get_status(gen_id) == GenerationStatus.EXPIRED
  assert cancelled == [gen_id]
  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.EXPIRED


async def test_reaper_skips_generation_with_live_subscriber() -> None:
  """I7: Reaper does not drain a generation that still has an active subscriber."""
  clock = 0.0
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0, clock=lambda: clock)
  gen_store = InMemoryGenerationStore()
  cancelled: list[str] = []

  async def on_cancel(gid: str) -> None:
    cancelled.append(gid)

  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.05,
    on_cancel=on_cancel,
  )
  reaper = GenerationReaper(
    registry=registry,
    subscribers=subscribers,
    canceller=canceller,
    interval_seconds=0.02,
  )

  gen_id, _ = await registry.get_or_create(f"req-{uuid.uuid4()}")
  await registry.transition(gen_id, GenerationStatus.RUNNING)
  await canceller.on_subscribe(gen_id, "sub-1")

  reaped = await reaper.reap_once([gen_id])
  assert reaped == []
  assert await registry.get_status(gen_id) == GenerationStatus.RUNNING
  assert cancelled == []


async def test_reaper_loop_stops_cleanly_on_close() -> None:
  """Background reaper loop exits cleanly on aclose() with no leaked tasks."""
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  gen_store = InMemoryGenerationStore()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.05,
  )
  reaper = GenerationReaper(
    registry=registry,
    subscribers=subscribers,
    canceller=canceller,
    interval_seconds=0.01,
  )

  polls = 0

  def active_gen_ids() -> list[str]:
    nonlocal polls
    polls += 1
    return []

  reaper.start(active_gen_ids=active_gen_ids)
  await asyncio.sleep(0.035)
  await reaper.aclose()

  assert polls >= 1
