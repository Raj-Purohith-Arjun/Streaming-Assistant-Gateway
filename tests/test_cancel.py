import asyncio
import uuid
from collections.abc import AsyncIterator

from app.cancel import DeferredCanceller
from app.generation import GenerationRegistry, GenerationStatus
from app.producer import Producer, ProducerConfig
from app.store.protocol import TerminalStatus
from app.store.subscribers import InMemorySubscriberRegistry
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore


async def _setup_running_generation(
  registry: GenerationRegistry,
) -> str:
  gen_id, _ = await registry.get_or_create(f"req-{uuid.uuid4()}")
  await registry.transition(gen_id, GenerationStatus.RUNNING)
  return gen_id


async def test_drain_timer_fires() -> None:
  """I6: Zero subscribers for T seconds transitions DRAINING -> EXPIRED and cancels."""
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
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
  gen_id = await _setup_running_generation(registry)

  await canceller.on_subscribe(gen_id, "sub-1")
  await canceller.on_unsubscribe(gen_id, "sub-1")

  assert await registry.get_status(gen_id) == GenerationStatus.DRAINING
  assert cancelled == []

  await asyncio.sleep(0.09)

  assert await registry.get_status(gen_id) == GenerationStatus.EXPIRED
  assert cancelled == [gen_id]
  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.EXPIRED


async def test_no_premature_cancel() -> None:
  """I7: A generation with >= 1 live subscriber (or re-attach < T) is never cancelled."""
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  gen_store = InMemoryGenerationStore()
  cancelled: list[str] = []

  async def on_cancel(gid: str) -> None:
    cancelled.append(gid)

  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.08,
    on_cancel=on_cancel,
  )
  gen_id = await _setup_running_generation(registry)

  await canceller.on_subscribe(gen_id, "sub-1")
  await canceller.on_subscribe(gen_id, "sub-2")
  await canceller.on_unsubscribe(gen_id, "sub-1")
  assert await registry.get_status(gen_id) == GenerationStatus.RUNNING

  await canceller.on_subscribe(gen_id, "sub-3")
  await canceller.on_unsubscribe(gen_id, "sub-2")
  assert await registry.get_status(gen_id) == GenerationStatus.RUNNING

  # Briefly drop to 0, then re-attach before T=0.08s elapses.
  await canceller.on_unsubscribe(gen_id, "sub-3")
  await asyncio.sleep(0.02)
  await canceller.on_subscribe(gen_id, "sub-4")

  await asyncio.sleep(0.1)
  assert await registry.get_status(gen_id) == GenerationStatus.RUNNING
  assert cancelled == []
  assert await gen_store.read_terminal(gen_id) is None


async def test_drain_revival_cancels_timer() -> None:
  """A new subscriber during DRAINING cancels the drain timer and returns to RUNNING."""
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  gen_store = InMemoryGenerationStore()
  cancelled: list[str] = []

  async def on_cancel(gid: str) -> None:
    cancelled.append(gid)

  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.06,
    on_cancel=on_cancel,
  )
  gen_id = await _setup_running_generation(registry)

  await canceller.on_subscribe(gen_id, "sub-1")
  await canceller.on_unsubscribe(gen_id, "sub-1")
  assert await registry.get_status(gen_id) == GenerationStatus.DRAINING

  await asyncio.sleep(0.02)
  await canceller.on_subscribe(gen_id, "sub-2")
  assert await registry.get_status(gen_id) == GenerationStatus.RUNNING

  await asyncio.sleep(0.08)
  assert await registry.get_status(gen_id) == GenerationStatus.RUNNING
  assert cancelled == []
  assert await gen_store.read_terminal(gen_id) is None


async def test_explicit_stop_skips_draining() -> None:
  """Explicit stop cancels immediately and marks STOPPED without waiting T."""
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  gen_store = InMemoryGenerationStore()
  cancelled: list[str] = []

  async def on_cancel(gid: str) -> None:
    cancelled.append(gid)

  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=10.0,
    on_cancel=on_cancel,
  )
  gen_id = await _setup_running_generation(registry)
  await canceller.on_subscribe(gen_id, "sub-1")

  await asyncio.wait_for(canceller.stop(gen_id), timeout=0.5)

  assert await registry.get_status(gen_id) == GenerationStatus.STOPPED
  assert cancelled == [gen_id]
  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.STOPPED


async def test_producer_stops_within_window_on_cancel() -> None:
  """I6/I8: Producer aborts within T + heartbeat_interval once generation becomes terminal."""
  registry = GenerationRegistry()
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.05,
  )
  gen_id = await _setup_running_generation(registry)

  upstream_closed = False
  first_token_sent = asyncio.Event()

  async def slow_upstream() -> AsyncIterator[str]:
    nonlocal upstream_closed
    try:
      yield "tok-0"
      first_token_sent.set()
      await asyncio.sleep(2.0)
      yield "tok-1"
    finally:
      upstream_closed = True

  config = ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.02)
  producer = Producer(gen_store=gen_store, lease_store=lease_store, owner_id="rep-1", config=config)

  await canceller.on_subscribe(gen_id, "sub-1")
  producer_task = asyncio.create_task(producer.run(gen_id, slow_upstream()))

  await first_token_sent.wait()
  await canceller.on_unsubscribe(gen_id, "sub-1")

  # Producer must exit within < 0.2s (well below the 2.0s upstream sleep).
  await asyncio.wait_for(producer_task, timeout=0.2)

  assert upstream_closed is True
  events = [e async for e in gen_store.read_from(gen_id)]
  assert [e.data for e in events] == ["tok-0"]
  terminal = await gen_store.read_terminal(gen_id)
  assert terminal is not None
  assert terminal.status == TerminalStatus.EXPIRED
