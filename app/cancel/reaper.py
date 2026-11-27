import asyncio
import contextlib
import inspect
from collections.abc import Awaitable, Callable, Iterable

import structlog

from app.cancel.canceller import DeferredCanceller
from app.generation import GenerationRegistryProtocol, GenerationStatus
from app.store.subscribers import SubscriberRegistry

logger = structlog.get_logger(__name__)


class GenerationReaper:
  """Background reaper that drains generations whose subscribers expired (crashed replicas)."""

  def __init__(
    self,
    registry: GenerationRegistryProtocol,
    subscribers: SubscriberRegistry,
    canceller: DeferredCanceller,
    interval_seconds: float = 5.0,
  ) -> None:
    if interval_seconds <= 0:
      raise ValueError(f"interval_seconds must be positive, got {interval_seconds}")
    self._registry = registry
    self._subscribers = subscribers
    self._canceller = canceller
    self._interval_seconds = interval_seconds
    self._task: asyncio.Task[None] | None = None

  async def reap_once(self, gen_ids: Iterable[str]) -> list[str]:
    """Scan ``gen_ids`` and arm drain timers for RUNNING generations with 0 active subscribers."""
    reaped: list[str] = []
    for gen_id in gen_ids:
      try:
        status = await self._registry.get_status(gen_id)
      except KeyError:
        continue
      if status != GenerationStatus.RUNNING:
        continue
      if await self._subscribers.active_count(gen_id) == 0 and (
        await self._canceller.schedule_drain_if_empty(gen_id)
      ):
        reaped.append(gen_id)
    return reaped

  def start(
    self,
    active_gen_ids: Callable[[], Iterable[str] | Awaitable[Iterable[str]]],
  ) -> None:
    """Start the periodic background reaper loop (idempotent if already running)."""
    if self._task is not None and not self._task.done():
      return

    async def _loop() -> None:
      while True:
        await asyncio.sleep(self._interval_seconds)
        try:
          ids_or_awaitable = active_gen_ids()
          if inspect.isawaitable(ids_or_awaitable):
            gen_ids = await ids_or_awaitable
          else:
            gen_ids = ids_or_awaitable
          await self.reap_once(gen_ids)
        except asyncio.CancelledError:
          raise
        except (OSError, RuntimeError, ValueError):
          logger.exception("reaper_sweep_failed")

    self._task = asyncio.create_task(_loop())

  async def aclose(self) -> None:
    """Stop and await the background reaper loop cleanly."""
    if self._task is not None and not self._task.done():
      self._task.cancel()
      with contextlib.suppress(asyncio.CancelledError):
        await self._task
    self._task = None
