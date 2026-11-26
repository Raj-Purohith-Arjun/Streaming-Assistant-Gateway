import asyncio
import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass

from app.store.lease import LeaseStore, StaleLeaseError
from app.store.protocol import GenerationClosedError, GenerationStore, TerminalStatus


@dataclass(frozen=True, slots=True)
class ProducerConfig:
  """Configuration for leased upstream production."""

  lease_ttl_ms: int = 5000
  heartbeat_interval_seconds: float = 1.5

  def __post_init__(self) -> None:
    if self.lease_ttl_ms <= 0:
      raise ValueError(f"lease_ttl_ms must be positive, got {self.lease_ttl_ms}")
    if self.heartbeat_interval_seconds <= 0:
      raise ValueError(
        f"heartbeat_interval_seconds must be positive, got {self.heartbeat_interval_seconds}"
      )


async def _close_upstream(upstream: AsyncIterator[str]) -> None:
  aclose = getattr(upstream, "aclose", None)
  if callable(aclose):
    await aclose()


async def _cancel_task(task: asyncio.Task[None] | None) -> None:
  if task is not None and not task.done():
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
      await task


class Producer:
  """Single-producer runner with background lease heartbeat and fenced writes (I3, I4)."""

  def __init__(
    self,
    gen_store: GenerationStore,
    lease_store: LeaseStore,
    owner_id: str,
    config: ProducerConfig | None = None,
  ) -> None:
    self._gen_store = gen_store
    self._lease_store = lease_store
    self._owner_id = owner_id
    self._config = config or ProducerConfig()

  async def run(self, gen_id: str, upstream: AsyncIterator[str]) -> bool:
    """Acquire lease, stream ``upstream`` into ``gen_store``, and mark terminal on finish."""
    lease = await self._lease_store.acquire_lease(
      gen_id,
      self._owner_id,
      ttl_ms=self._config.lease_ttl_ms,
    )
    if lease is None:
      # Another replica holds the active lease (I3); never touch the upstream iterator.
      await _close_upstream(upstream)
      return False

    async def _heartbeat_loop() -> None:
      while True:
        await asyncio.sleep(self._config.heartbeat_interval_seconds)
        await self._lease_store.renew_lease(
          gen_id,
          self._owner_id,
          lease.fence,
          ttl_ms=self._config.lease_ttl_ms,
        )

    async def _pump_tokens() -> None:
      async for token in upstream:
        await self._gen_store.append_token(gen_id, token, fence=lease.fence)

    heartbeat_task: asyncio.Task[None] | None = None
    pump_task: asyncio.Task[None] | None = None

    try:
      heartbeat_task = asyncio.create_task(_heartbeat_loop())
      pump_task = asyncio.create_task(_pump_tokens())

      done, _ = await asyncio.wait(
        {heartbeat_task, pump_task},
        return_when=asyncio.FIRST_COMPLETED,
      )

      if heartbeat_task in done:
        # Heartbeat only exits on error (e.g. StaleLeaseError); abort pump without mark_terminal.
        await _cancel_task(pump_task)
        exc = heartbeat_task.exception()
        if exc is not None:
          raise exc
        return False

      await _cancel_task(heartbeat_task)
      pump_exc = pump_task.exception()
      if pump_exc is not None:
        if isinstance(pump_exc, StaleLeaseError):
          # Lost fencing mid-write: do not attempt mark_terminal.
          raise pump_exc
        with contextlib.suppress(StaleLeaseError, GenerationClosedError):
          await self._gen_store.mark_terminal(
            gen_id,
            TerminalStatus.FAILED,
            reason=str(pump_exc),
            fence=lease.fence,
          )
        raise pump_exc

      await self._gen_store.mark_terminal(
        gen_id,
        TerminalStatus.COMPLETED,
        fence=lease.fence,
      )
      return True
    except asyncio.CancelledError:
      await _cancel_task(heartbeat_task)
      await _cancel_task(pump_task)
      raise
    finally:
      await _cancel_task(heartbeat_task)
      await _cancel_task(pump_task)
      await _close_upstream(upstream)
      with contextlib.suppress(StaleLeaseError):
        await self._lease_store.release_lease(gen_id, self._owner_id, lease.fence)
