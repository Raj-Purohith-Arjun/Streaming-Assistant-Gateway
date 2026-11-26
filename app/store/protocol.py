from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

EVENT_ID_VERSION = "v1"


class TerminalStatus(StrEnum):
  """Terminal states for a generation lifecycle."""

  COMPLETED = "COMPLETED"
  STOPPED = "STOPPED"
  EXPIRED = "EXPIRED"
  FAILED = "FAILED"


class GenerationClosedError(Exception):
  """Raised when attempting to append to or close an already-terminal generation."""


class InvalidOffsetError(ValueError):
  """Raised when an SSE Last-Event-ID offset is malformed."""


class OffsetAheadError(InvalidOffsetError):
  """Raised when a client resumes from an index ahead of what the log holds."""


@dataclass(frozen=True, slots=True)
class TokenEvent:
  """A single token entry stored in and read from a generation log."""

  index: int
  entry_id: str
  event_id: str
  data: str


@dataclass(frozen=True, slots=True)
class TerminalState:
  """Terminal marker recorded when a generation ends."""

  status: TerminalStatus
  reason: str | None = None
  last_index: int | None = None


def format_event_id(index: int, entry_id: str) -> str:
  """Format an opaque SSE event id as ``v1:<index>:<redis-entry-id>``.

  Clients must treat this string as opaque and never parse it.
  """
  if index < 0:
    raise ValueError(f"Token index must be non-negative, got {index}")
  if not entry_id:
    raise ValueError("entry_id must be non-empty")
  return f"{EVENT_ID_VERSION}:{index}:{entry_id}"


def parse_event_id(event_id: str) -> tuple[int, str]:
  """Split an opaque SSE event id into ``(index, entry_id)``.

  Raises ``InvalidOffsetError`` if the format does not match ``v1:<index>:<entry_id>``.
  """
  parts = event_id.split(":", 2)
  if len(parts) != 3 or parts[0] != EVENT_ID_VERSION or not parts[2]:
    raise InvalidOffsetError(f"Malformed event id: {event_id!r}")
  try:
    index = int(parts[1])
  except ValueError as exc:
    raise InvalidOffsetError(f"Invalid index in event id: {event_id!r}") from exc
  if index < 0:
    raise InvalidOffsetError(f"Negative index in event id: {event_id!r}")
  return index, parts[2]


@runtime_checkable
class GenerationStore(Protocol):
  """Abstract storage protocol for the append-only generation token log."""

  async def append_token(
    self,
    gen_id: str,
    token: str,
    *,
    fence: int | None = None,
  ) -> TokenEvent:
    """Append a token to ``gen_id`` and return the stored ``TokenEvent``.

    Raises ``GenerationClosedError`` if the generation is already terminal (I4).
    """
    ...

  def read_from(
    self,
    gen_id: str,
    offset: str | None = None,
    *,
    block_ms: int | None = None,
  ) -> AsyncIterator[TokenEvent]:
    """Yield ``TokenEvent`` items for ``gen_id`` strictly after ``offset``.

    When ``offset`` is ``None``, yields from the beginning of the log.
    Raises ``InvalidOffsetError`` for malformed offsets and ``OffsetAheadError``
    when the offset index exceeds the latest index in the log.
    """
    ...

  async def mark_terminal(
    self,
    gen_id: str,
    status: TerminalStatus,
    reason: str | None = None,
    *,
    fence: int | None = None,
  ) -> TerminalState:
    """Record a terminal state for ``gen_id``.

    Raises ``GenerationClosedError`` if ``gen_id`` is already terminal (I4).
    """
    ...

  async def read_terminal(self, gen_id: str) -> TerminalState | None:
    """Return the terminal state for ``gen_id``, or ``None`` if not yet terminal."""
    ...
