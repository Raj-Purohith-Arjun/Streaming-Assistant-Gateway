import asyncio
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass

from app.store.lease import Lease, StaleLeaseError
from app.store.protocol import (
  GenerationClosedError,
  OffsetAheadError,
  TerminalState,
  TerminalStatus,
  TokenEvent,
  format_event_id,
  parse_event_id,
)


@dataclass(slots=True)
class _LeaseRecord:
  owner_id: str
  fence: int
  expires_at: float


class InMemoryLeaseStore:
  """In-memory ``LeaseStore`` for hermetic single-producer and fencing tests."""

  def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
    self._clock = clock
    self._leases: dict[str, _LeaseRecord] = {}
    self._fence_counters: dict[str, int] = {}

  def _active_record(self, gen_id: str) -> _LeaseRecord | None:
    record = self._leases.get(gen_id)
    if record is None:
      return None
    # Treat expired leases as absent so a successor replica can take over immediately.
    if self._clock() >= record.expires_at:
      del self._leases[gen_id]
      return None
    return record

  def _require_active(self, gen_id: str, owner_id: str, fence: int) -> _LeaseRecord:
    record = self._active_record(gen_id)
    if record is None or record.owner_id != owner_id or record.fence != fence:
      raise StaleLeaseError(
        f"Lease for {gen_id!r} (owner={owner_id!r}, fence={fence}) is expired or superseded"
      )
    return record

  async def acquire_lease(
    self,
    gen_id: str,
    owner_id: str,
    *,
    ttl_ms: int,
  ) -> Lease | None:
    if ttl_ms <= 0:
      raise ValueError(f"ttl_ms must be positive, got {ttl_ms}")
    if self._active_record(gen_id) is not None:
      return None

    # Monotonic per-gen_id fence ensures a GC-paused holder cannot masquerade as current.
    fence = self._fence_counters.get(gen_id, 0) + 1
    self._fence_counters[gen_id] = fence
    self._leases[gen_id] = _LeaseRecord(
      owner_id=owner_id,
      fence=fence,
      expires_at=self._clock() + (ttl_ms / 1000.0),
    )
    return Lease(gen_id=gen_id, owner_id=owner_id, fence=fence)

  async def renew_lease(
    self,
    gen_id: str,
    owner_id: str,
    fence: int,
    *,
    ttl_ms: int,
  ) -> Lease:
    if ttl_ms <= 0:
      raise ValueError(f"ttl_ms must be positive, got {ttl_ms}")
    record = self._require_active(gen_id, owner_id, fence)
    record.expires_at = self._clock() + (ttl_ms / 1000.0)
    return Lease(gen_id=gen_id, owner_id=owner_id, fence=fence)

  async def release_lease(self, gen_id: str, owner_id: str, fence: int) -> bool:
    self._require_active(gen_id, owner_id, fence)
    del self._leases[gen_id]
    return True

  async def current_fence(self, gen_id: str) -> int | None:
    record = self._active_record(gen_id)
    return record.fence if record is not None else None


class InMemoryGenerationStore:
  """In-memory ``GenerationStore`` implementation for hermetic invariant tests."""

  def __init__(self, *, lease_store: InMemoryLeaseStore | None = None) -> None:
    self._streams: dict[str, list[TokenEvent]] = {}
    self._terminals: dict[str, TerminalState] = {}
    self._notify_events: dict[str, asyncio.Event] = {}
    self._lease_store = lease_store

  def _notify(self, gen_id: str) -> None:
    event = self._notify_events.get(gen_id)
    if event is not None:
      event.set()
      self._notify_events[gen_id] = asyncio.Event()

  async def _check_fence(self, gen_id: str, fence: int | None) -> None:
    if fence is None or self._lease_store is None:
      return
    active_fence = await self._lease_store.current_fence(gen_id)
    if active_fence != fence:
      raise StaleLeaseError(f"Stale fence {fence} for {gen_id!r} (active={active_fence})")

  async def append_token(
    self,
    gen_id: str,
    token: str,
    *,
    fence: int | None = None,
  ) -> TokenEvent:
    await self._check_fence(gen_id, fence)
    if gen_id in self._terminals:
      raise GenerationClosedError(f"Generation {gen_id!r} is already terminal")

    stream = self._streams.setdefault(gen_id, [])
    index = len(stream)
    entry_id = f"0-{index + 1}"
    event = TokenEvent(
      index=index,
      entry_id=entry_id,
      event_id=format_event_id(index, entry_id),
      data=token,
    )
    stream.append(event)
    self._notify(gen_id)
    return event

  async def read_from(
    self,
    gen_id: str,
    offset: str | None = None,
    *,
    block_ms: int | None = None,
  ) -> AsyncIterator[TokenEvent]:
    stream = self._streams.get(gen_id, [])

    if offset is None:
      cursor = 0
    else:
      parsed_index, _ = parse_event_id(offset)
      if parsed_index >= len(stream):
        raise OffsetAheadError(
          f"Offset index {parsed_index} is ahead of log length {len(stream)} for {gen_id!r}"
        )
      cursor = parsed_index + 1

    while True:
      stream = self._streams.get(gen_id, [])
      while cursor < len(stream):
        yield stream[cursor]
        cursor += 1

      if gen_id in self._terminals or block_ms is None:
        break

      waiter = self._notify_events.setdefault(gen_id, asyncio.Event())
      try:
        await asyncio.wait_for(waiter.wait(), timeout=block_ms / 1000.0)
      except TimeoutError:
        break

  async def mark_terminal(
    self,
    gen_id: str,
    status: TerminalStatus,
    reason: str | None = None,
    *,
    fence: int | None = None,
  ) -> TerminalState:
    await self._check_fence(gen_id, fence)
    if gen_id in self._terminals:
      raise GenerationClosedError(f"Generation {gen_id!r} is already terminal")

    stream = self._streams.get(gen_id, [])
    last_index = len(stream) - 1 if stream else None
    state = TerminalState(status=status, reason=reason, last_index=last_index)
    self._terminals[gen_id] = state
    self._notify(gen_id)
    return state

  async def read_terminal(self, gen_id: str) -> TerminalState | None:
    return self._terminals.get(gen_id)
