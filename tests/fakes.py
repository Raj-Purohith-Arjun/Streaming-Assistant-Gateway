import asyncio
from collections.abc import AsyncIterator

from app.store.protocol import (
  GenerationClosedError,
  OffsetAheadError,
  TerminalState,
  TerminalStatus,
  TokenEvent,
  format_event_id,
  parse_event_id,
)


class InMemoryGenerationStore:
  """In-memory ``GenerationStore`` implementation for hermetic invariant tests."""

  def __init__(self) -> None:
    self._streams: dict[str, list[TokenEvent]] = {}
    self._terminals: dict[str, TerminalState] = {}
    self._notify_events: dict[str, asyncio.Event] = {}

  def _notify(self, gen_id: str) -> None:
    event = self._notify_events.get(gen_id)
    if event is not None:
      event.set()
      self._notify_events[gen_id] = asyncio.Event()

  async def append_token(self, gen_id: str, token: str) -> TokenEvent:
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
  ) -> TerminalState:
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
