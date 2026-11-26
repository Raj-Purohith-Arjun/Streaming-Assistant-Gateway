import asyncio
import contextlib
from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import structlog

from app.cancel import DeferredCanceller
from app.generation import GenerationRegistryProtocol, GenerationStatus, InvalidTransitionError
from app.producer import Producer, ProducerConfig
from app.sse import SseEncoder
from app.store.lease import LeaseStore, StaleLeaseError
from app.store.protocol import (
  GenerationStore,
  TerminalStatus,
  format_event_id,
  parse_event_id,
)
from app.store.subscribers import SubscriberRegistry
from app.upstream import UpstreamAdapter

logger = structlog.get_logger(__name__)


class GatewayService:
  """Coordinates generation lifecycle, coalescing, SSE fan-out/resume, and cancellation."""

  def __init__(
    self,
    gen_store: GenerationStore,
    lease_store: LeaseStore,
    subscribers: SubscriberRegistry,
    registry: GenerationRegistryProtocol,
    canceller: DeferredCanceller,
    upstream: UpstreamAdapter,
    sse_encoder: SseEncoder,
    producer_config: ProducerConfig | None = None,
    owner_id: str | None = None,
  ) -> None:
    self._gen_store = gen_store
    self._lease_store = lease_store
    self._subscribers = subscribers
    self._registry = registry
    self._canceller = canceller
    self._upstream = upstream
    self._sse_encoder = sse_encoder
    self._producer_config = producer_config
    self._owner_id = owner_id or f"gateway-{uuid4()}"
    self._producer_tasks: dict[str, asyncio.Task[None]] = {}

  async def _run_producer(self, gen_id: str, prompt: str) -> None:
    producer = Producer(
      gen_store=self._gen_store,
      lease_store=self._lease_store,
      owner_id=self._owner_id,
      config=self._producer_config,
    )
    try:
      await producer.run(gen_id, self._upstream.stream(prompt))
    except (StaleLeaseError, httpx.HTTPError, RuntimeError, OSError, ValueError):
      logger.exception("producer_run_failed", gen_id=gen_id)
    finally:
      with contextlib.suppress(KeyError, InvalidTransitionError):
        status = await self._registry.get_status(gen_id)
        if status in {GenerationStatus.RUNNING, GenerationStatus.DRAINING}:
          terminal = await self._gen_store.read_terminal(gen_id)
          if terminal is not None:
            target = (
              GenerationStatus.FAILED
              if terminal.status == TerminalStatus.FAILED
              else GenerationStatus.COMPLETED
            )
            await self._registry.transition(gen_id, target)

  async def start_or_attach(self, request_key: str, prompt: str) -> tuple[str, bool]:
    """Attach to a live generation for ``request_key`` or start a new producer (I9)."""
    gen_id, created = await self._registry.get_or_create(request_key)
    if created:
      await self._registry.transition(gen_id, GenerationStatus.RUNNING)
      task = asyncio.create_task(self._run_producer(gen_id, prompt))
      self._producer_tasks[gen_id] = task

      def _cleanup(_task: asyncio.Task[None]) -> None:
        self._producer_tasks.pop(gen_id, None)

      task.add_done_callback(_cleanup)
    return gen_id, created

  async def validate_stream_request(self, gen_id: str, last_event_id: str | None) -> None:
    """Validate that ``gen_id`` exists and ``last_event_id`` is a valid, non-ahead offset."""
    await self._registry.get_status(gen_id)
    if last_event_id is not None:
      parse_event_id(last_event_id)
      async for _ in self._gen_store.read_from(gen_id, offset=last_event_id, block_ms=None):
        break

  async def stream_sse(
    self,
    gen_id: str,
    last_event_id: str | None = None,
  ) -> AsyncIterator[str]:
    """Subscribe to ``gen_id`` and yield encoded SSE frames from ``last_event_id``."""
    sub_id = str(uuid4())
    await self._canceller.on_subscribe(gen_id, sub_id)

    async def _event_frames() -> AsyncIterator[str]:
      cursor = last_event_id
      last_seen_id = last_event_id
      while True:
        async for event in self._gen_store.read_from(gen_id, offset=cursor, block_ms=250):
          cursor = event.event_id
          last_seen_id = event.event_id
          yield self._sse_encoder.encode_token(event)

        terminal = await self._gen_store.read_terminal(gen_id)
        if terminal is not None:
          terminal_event_id = last_seen_id or format_event_id(
            terminal.last_index if terminal.last_index is not None else 0,
            "0-0",
          )
          if terminal.status == TerminalStatus.FAILED:
            yield self._sse_encoder.encode_error(
              terminal_event_id,
              message=terminal.reason or "Generation failed",
            )
          else:
            yield self._sse_encoder.encode_done(
              terminal_event_id,
              status=terminal.status,
              last_index=terminal.last_index,
            )
          break

        await self._subscribers.heartbeat(gen_id, sub_id)

    try:
      async for frame in self._sse_encoder.wrap_with_heartbeats(_event_frames()):
        yield frame
    finally:
      await self._canceller.on_unsubscribe(gen_id, sub_id)

  async def stop(self, gen_id: str) -> None:
    """Immediately stop ``gen_id``, skipping the drain timeout."""
    await self._registry.get_status(gen_id)
    await self._canceller.stop(gen_id)
