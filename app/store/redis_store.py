import json
from collections.abc import AsyncIterator
from typing import Any

from redis.asyncio import Redis

from app.store.protocol import (
  GenerationClosedError,
  OffsetAheadError,
  TerminalState,
  TerminalStatus,
  TokenEvent,
  format_event_id,
  parse_event_id,
)

DEFAULT_TTL_SECONDS = 3600

# Atomically checks terminal state, assigns monotonic index via XLEN, appends token, and sets TTL.
_APPEND_TOKEN_LUA = """
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {0, 'CLOSED'}
end
local index = redis.call('XLEN', KEYS[1])
local entry_id = redis.call(
  'XADD', KEYS[1], '*',
  'kind', 'token',
  'index', tostring(index),
  'data', ARGV[1]
)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return {1, tostring(index), entry_id}
"""

# Atomically records terminal state with TTL and appends a terminal sentinel to wake XREAD readers.
_MARK_TERMINAL_LUA = """
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {0, 'CLOSED'}
end
local len = redis.call('XLEN', KEYS[1])
local last_index = cjson.null
if len > 0 then
  last_index = len - 1
end
local reason = ARGV[2]
if ARGV[3] == '0' then
  reason = cjson.null
end
local payload = cjson.encode({status = ARGV[1], reason = reason, last_index = last_index})
redis.call('SET', KEYS[2], payload, 'EX', tonumber(ARGV[4]))
redis.call('XADD', KEYS[1], '*', 'kind', 'terminal', 'status', ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return {1, payload}
"""


def _decode(value: str | bytes) -> str:
  if isinstance(value, bytes):
    return value.decode("utf-8")
  return value


def _decode_fields(fields: dict[Any, Any]) -> dict[str, str]:
  return {_decode(k): _decode(v) for k, v in fields.items()}


class RedisGenerationStore:
  """Redis Streams-backed ``GenerationStore`` with atomic I4 enforcement and explicit TTLs."""

  def __init__(self, redis: Redis, *, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
    if ttl_seconds <= 0:
      raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
    self._redis = redis
    self._ttl_seconds = ttl_seconds
    self._append_script = self._redis.register_script(_APPEND_TOKEN_LUA)
    self._mark_terminal_script = self._redis.register_script(_MARK_TERMINAL_LUA)

  @staticmethod
  def stream_key(gen_id: str) -> str:
    """Return the Redis Stream key for ``gen_id``."""
    return f"gen:{gen_id}:stream"

  @staticmethod
  def terminal_key(gen_id: str) -> str:
    """Return the Redis terminal marker key for ``gen_id``."""
    return f"gen:{gen_id}:terminal"

  async def append_token(self, gen_id: str, token: str) -> TokenEvent:
    """Append a token to ``gen_id`` atomically, raising ``GenerationClosedError`` if terminal."""
    result = await self._append_script(
      keys=[self.stream_key(gen_id), self.terminal_key(gen_id)],
      args=[token, self._ttl_seconds],
    )
    ok = int(result[0])
    if ok == 0:
      raise GenerationClosedError(f"Generation {gen_id!r} is already terminal")

    index = int(_decode(result[1]))
    entry_id = _decode(result[2])
    return TokenEvent(
      index=index,
      entry_id=entry_id,
      event_id=format_event_id(index, entry_id),
      data=token,
    )

  async def _latest_token_index(self, stream_key: str) -> int:
    """Return the latest token index in ``stream_key``, or ``-1`` if no tokens exist."""
    entries = await self._redis.xrevrange(stream_key, max="+", min="-", count=2)
    if not entries:
      return -1
    for _, raw_fields in entries:
      if not raw_fields:
        continue
      fields = _decode_fields(raw_fields)
      if fields.get("kind") == "token":
        return int(fields["index"])
    return -1

  @staticmethod
  def _extract_xread_entries(response: Any) -> list[tuple[Any, Any]]:
    if not response:
      return []
    if isinstance(response, dict):
      first_stream_entries: list[Any] = next(iter(response.values()), [])
      if first_stream_entries and isinstance(first_stream_entries[0], list):
        return list(first_stream_entries[0])
      return list(first_stream_entries)
    return list(response[0][1])

  @staticmethod
  def _parse_stream_entries(
    raw_entries: list[tuple[Any, Any]],
  ) -> tuple[list[TokenEvent], str | None, bool]:
    events: list[TokenEvent] = []
    last_cursor: str | None = None
    saw_terminal = False
    for raw_entry_id, raw_fields in raw_entries:
      if raw_entry_id is None or not raw_fields:
        continue
      entry_id = _decode(raw_entry_id)
      last_cursor = entry_id
      fields = _decode_fields(raw_fields)
      if fields.get("kind") == "terminal":
        saw_terminal = True
        break
      index = int(fields["index"])
      events.append(
        TokenEvent(
          index=index,
          entry_id=entry_id,
          event_id=format_event_id(index, entry_id),
          data=fields["data"],
        )
      )
    return events, last_cursor, saw_terminal

  async def read_from(
    self,
    gen_id: str,
    offset: str | None = None,
    *,
    block_ms: int | None = None,
  ) -> AsyncIterator[TokenEvent]:
    """Yield ``TokenEvent`` items for ``gen_id`` strictly after ``offset``."""
    stream_key = self.stream_key(gen_id)

    if offset is None:
      cursor = "0-0"
    else:
      parsed_index, entry_id = parse_event_id(offset)
      latest_index = await self._latest_token_index(stream_key)
      if parsed_index > latest_index:
        raise OffsetAheadError(
          f"Offset index {parsed_index} is ahead of latest index {latest_index} for {gen_id!r}"
        )
      cursor = entry_id

    while True:
      response = await self._redis.xread(streams={stream_key: cursor}, block=None)
      raw_entries = self._extract_xread_entries(response)
      if raw_entries:
        events, next_cursor, saw_terminal = self._parse_stream_entries(raw_entries)
        if next_cursor is not None:
          cursor = next_cursor
        for event in events:
          yield event
        if saw_terminal:
          break
        continue

      if block_ms is None or await self.read_terminal(gen_id) is not None:
        break

      blocked = await self._redis.xread(streams={stream_key: cursor}, block=block_ms)
      blocked_entries = self._extract_xread_entries(blocked)
      if not blocked_entries:
        break

      events, next_cursor, saw_terminal = self._parse_stream_entries(blocked_entries)
      if next_cursor is not None:
        cursor = next_cursor
      for event in events:
        yield event
      if saw_terminal:
        break

  async def mark_terminal(
    self,
    gen_id: str,
    status: TerminalStatus,
    reason: str | None = None,
  ) -> TerminalState:
    """Atomically mark ``gen_id`` as terminal and wake any blocked ``XREAD`` readers."""
    has_reason = "1" if reason is not None else "0"
    result = await self._mark_terminal_script(
      keys=[self.stream_key(gen_id), self.terminal_key(gen_id)],
      args=[status.value, reason or "", has_reason, self._ttl_seconds],
    )
    ok = int(result[0])
    if ok == 0:
      raise GenerationClosedError(f"Generation {gen_id!r} is already terminal")

    data = json.loads(_decode(result[1]))
    return TerminalState(
      status=TerminalStatus(data["status"]),
      reason=data["reason"],
      last_index=data["last_index"],
    )

  async def read_terminal(self, gen_id: str) -> TerminalState | None:
    """Return the terminal state for ``gen_id``, or ``None`` if not yet terminal."""
    raw = await self._redis.get(self.terminal_key(gen_id))
    if raw is None:
      return None
    data = json.loads(_decode(raw))
    return TerminalState(
      status=TerminalStatus(data["status"]),
      reason=data["reason"],
      last_index=data["last_index"],
    )
