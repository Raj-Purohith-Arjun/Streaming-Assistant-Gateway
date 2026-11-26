import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator, AsyncIterator
from enum import StrEnum
from typing import Any

from app.store.protocol import TerminalStatus, TokenEvent, parse_event_id

DEFAULT_HEARTBEAT_INTERVAL_SECONDS: float = 15.0
HEARTBEAT_COMMENT: str = ": heartbeat\n\n"


class SseEventType(StrEnum):
  """Typed SSE event names emitted by the gateway."""

  TOKEN = "token"
  DONE = "done"
  ERROR = "error"
  DISCONTINUITY = "discontinuity"


class SseEncoder:
  """Encodes typed generation events and heartbeats into SSE wire format."""

  def __init__(self, heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS) -> None:
    if heartbeat_interval_seconds <= 0:
      raise ValueError(
        f"heartbeat_interval_seconds must be positive, got {heartbeat_interval_seconds}"
      )
    self._heartbeat_interval_seconds = heartbeat_interval_seconds

  @property
  def heartbeat_interval_seconds(self) -> float:
    """Return the configured heartbeat interval in seconds."""
    return self._heartbeat_interval_seconds

  def encode_event(
    self,
    event_type: SseEventType | str,
    event_id: str,
    data: dict[str, Any],
  ) -> str:
    """Encode a typed event with an opaque ``v1:<index>:<entry-id>`` id and JSON payload."""
    try:
      typed_event = SseEventType(event_type)
    except ValueError as exc:
      raise ValueError(f"Unsupported SSE event type: {event_type!r}") from exc

    # Validate the opaque event id format from app.store.protocol before emitting.
    parse_event_id(event_id)

    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"id: {event_id}\nevent: {typed_event.value}\ndata: {payload}\n\n"

  def encode_token(self, token_event: TokenEvent) -> str:
    """Encode a ``TokenEvent`` into a ``token`` SSE frame."""
    return self.encode_event(
      SseEventType.TOKEN,
      token_event.event_id,
      {"index": token_event.index, "text": token_event.data},
    )

  def encode_done(
    self,
    event_id: str,
    status: TerminalStatus | str = TerminalStatus.COMPLETED,
    last_index: int | None = None,
  ) -> str:
    """Encode a terminal ``done`` SSE frame."""
    data: dict[str, Any] = {"status": str(status)}
    if last_index is not None:
      data["last_index"] = last_index
    return self.encode_event(SseEventType.DONE, event_id, data)

  def encode_error(
    self,
    event_id: str,
    message: str,
    code: str | None = None,
  ) -> str:
    """Encode an ``error`` SSE frame."""
    data: dict[str, Any] = {"message": message}
    if code is not None:
      data["code"] = code
    return self.encode_event(SseEventType.ERROR, event_id, data)

  def encode_discontinuity(
    self,
    event_id: str,
    reason: str,
    last_index: int | None = None,
  ) -> str:
    """Encode a ``discontinuity`` SSE frame."""
    data: dict[str, Any] = {"reason": reason}
    if last_index is not None:
      data["last_index"] = last_index
    return self.encode_event(SseEventType.DISCONTINUITY, event_id, data)

  def encode_heartbeat(self) -> str:
    """Return an SSE heartbeat comment line (no ``id:`` line)."""
    return HEARTBEAT_COMMENT

  async def heartbeat_generator(self) -> AsyncGenerator[str, None]:
    """Yield SSE heartbeat comments at the configured interval."""
    while True:
      await asyncio.sleep(self._heartbeat_interval_seconds)
      yield HEARTBEAT_COMMENT

  async def wrap_with_heartbeats(self, events: AsyncIterator[str]) -> AsyncGenerator[str, None]:
    """Yield events from ``events``, emitting heartbeat comments while idle."""

    async def _next_event() -> str:
      return await events.__anext__()

    pending_next: asyncio.Task[str] | None = None
    try:
      while True:
        if pending_next is None:
          pending_next = asyncio.create_task(_next_event())

        done, _ = await asyncio.wait(
          {pending_next},
          timeout=self._heartbeat_interval_seconds,
        )
        if pending_next in done:
          task = pending_next
          pending_next = None
          try:
            item = task.result()
          except StopAsyncIteration:
            break
          yield item
        else:
          yield HEARTBEAT_COMMENT
    finally:
      if pending_next is not None and not pending_next.done():
        pending_next.cancel()
        with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
          await pending_next
