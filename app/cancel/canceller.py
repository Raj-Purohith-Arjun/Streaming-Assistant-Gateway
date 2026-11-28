import asyncio
import contextlib
from collections.abc import Awaitable, Callable

from app.generation import (
  TERMINAL_STATUSES,
  GenerationRegistryProtocol,
  GenerationStatus,
  InvalidTransitionError,
)
from app.store.protocol import GenerationClosedError, GenerationStore, TerminalStatus
from app.store.subscribers import SubscriberRegistry


class DeferredCanceller:
  """Manages deferred drain-timer cancellation (I6, I7, I8) and immediate explicit stop."""

  def __init__(
    self,
    registry: GenerationRegistryProtocol,
    subscribers: SubscriberRegistry,
    gen_store: GenerationStore,
    drain_timeout_seconds: float,
    on_cancel: Callable[[str], Awaitable[None]] | None = None,
  ) -> None:
    if drain_timeout_seconds <= 0:
      raise ValueError(f"drain_timeout_seconds must be positive, got {drain_timeout_seconds}")
    self._registry = registry
    self._subscribers = subscribers
    self._gen_store = gen_store
    self._drain_timeout_seconds = drain_timeout_seconds
    self._on_cancel = on_cancel
    self._lock = asyncio.Lock()
    self._drain_tasks: dict[str, asyncio.Task[None]] = {}

  async def _cancel_drain_timer(self, gen_id: str) -> None:
    task = self._drain_tasks.pop(gen_id, None)
    if task is not None and not task.done() and task is not asyncio.current_task():
      task.cancel()
      with contextlib.suppress(asyncio.CancelledError):
        await task

  async def _drain_after_timeout(self, gen_id: str) -> None:
    await asyncio.sleep(self._drain_timeout_seconds)

    should_cancel = False
    async with self._lock:
      self._drain_tasks.pop(gen_id, None)
      # Re-verify zero subscribers and DRAINING state after sleep to prevent races (I7).
      if (
        await self._subscribers.active_count(gen_id) == 0
        and await self._registry.get_status(gen_id) == GenerationStatus.DRAINING
      ):
        with contextlib.suppress(InvalidTransitionError):
          await self._registry.transition(gen_id, GenerationStatus.EXPIRED)
          should_cancel = True

    if not should_cancel:
      return

    with contextlib.suppress(GenerationClosedError):
      await self._gen_store.mark_terminal(gen_id, TerminalStatus.EXPIRED)
    if self._on_cancel is not None:
      await self._on_cancel(gen_id)

  async def schedule_drain_if_empty(self, gen_id: str) -> bool:
    """Transition RUNNING -> DRAINING and arm the drain timer if active_count is 0."""
    async with self._lock:
      if (
        await self._subscribers.active_count(gen_id) == 0
        and await self._registry.get_status(gen_id) == GenerationStatus.RUNNING
      ):
        try:
          await self._registry.transition(gen_id, GenerationStatus.DRAINING)
        except InvalidTransitionError:
          return False
        await self._cancel_drain_timer(gen_id)
        self._drain_tasks[gen_id] = asyncio.create_task(self._drain_after_timeout(gen_id))
        return True
    return False

  async def on_subscribe(self, gen_id: str, sub_id: str) -> int:
    """Register ``sub_id``; cancel any active drain timer and revive DRAINING -> RUNNING."""
    count = await self._subscribers.register(gen_id, sub_id)
    async with self._lock:
      await self._cancel_drain_timer(gen_id)
      if await self._registry.get_status(gen_id) == GenerationStatus.DRAINING:
        with contextlib.suppress(InvalidTransitionError):
          await self._registry.transition(gen_id, GenerationStatus.RUNNING)
    return count

  async def on_unsubscribe(self, gen_id: str, sub_id: str) -> int:
    """Unregister ``sub_id``; enter DRAINING and arm the drain timer if count hits 0."""
    count = await self._subscribers.unregister(gen_id, sub_id)
    if count == 0:
      await self.schedule_drain_if_empty(gen_id)
    return count

  async def stop(self, gen_id: str) -> None:
    """Explicit stop: cancel immediately and mark STOPPED without waiting for drain timeout."""
    async with self._lock:
      await self._cancel_drain_timer(gen_id)
      status = await self._registry.get_status(gen_id)
      if status not in TERMINAL_STATUSES:
        with contextlib.suppress(InvalidTransitionError):
          await self._registry.transition(gen_id, GenerationStatus.STOPPED)

    with contextlib.suppress(GenerationClosedError):
      await self._gen_store.mark_terminal(gen_id, TerminalStatus.STOPPED)
    if self._on_cancel is not None:
      await self._on_cancel(gen_id)

  async def aclose(self) -> None:
    """Cancel all pending drain timer tasks cleanly on shutdown."""
    async with self._lock:
      gen_ids = list(self._drain_tasks.keys())
      for gen_id in gen_ids:
        await self._cancel_drain_timer(gen_id)
