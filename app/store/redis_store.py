import json
from collections.abc import AsyncIterator

from redis.asyncio import Redis

from app.store._codec import (
  decode_fields,
  decode_redis_str,
  extract_xread_entries,
  parse_stream_entries,
)
from app.store._lua import (
  ACQUIRE_LEASE_LUA,
  APPEND_TOKEN_LUA,
  MARK_TERMINAL_LUA,
  RELEASE_LEASE_LUA,
  RENEW_LEASE_LUA,
)
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

DEFAULT_TTL_SECONDS = 3600


class RedisGenerationStore:
  """Redis Streams-backed ``GenerationStore`` and ``LeaseStore`` with atomic Lua enforcement."""

  def __init__(self, redis: Redis, *, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
    if ttl_seconds <= 0:
      raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
    self._redis = redis
    self._ttl_seconds = ttl_seconds
    self._append_script = self._redis.register_script(APPEND_TOKEN_LUA)
    self._mark_terminal_script = self._redis.register_script(MARK_TERMINAL_LUA)
    self._acquire_lease_script = self._redis.register_script(ACQUIRE_LEASE_LUA)
    self._renew_lease_script = self._redis.register_script(RENEW_LEASE_LUA)
    self._release_lease_script = self._redis.register_script(RELEASE_LEASE_LUA)

  @staticmethod
  def stream_key(gen_id: str) -> str:
    """Return the Redis Stream key for ``gen_id``."""
    return f"gen:{gen_id}:stream"

  @staticmethod
  def terminal_key(gen_id: str) -> str:
    """Return the Redis terminal marker key for ``gen_id``."""
    return f"gen:{gen_id}:terminal"

  @staticmethod
  def lease_key(gen_id: str) -> str:
    """Return the Redis active-lease key for ``gen_id``."""
    return f"gen:{gen_id}:lease"

  @staticmethod
  def fence_key(gen_id: str) -> str:
    """Return the Redis monotonic fencing-token key for ``gen_id``."""
    return f"gen:{gen_id}:fence"

  async def append_token(
    self,
    gen_id: str,
    token: str,
    *,
    fence: int | None = None,
  ) -> TokenEvent:
    """Append a token to ``gen_id`` atomically, enforcing I4 and optional fencing."""
    fence_arg = str(fence) if fence is not None else ""
    result = await self._append_script(
      keys=[
        self.stream_key(gen_id),
        self.terminal_key(gen_id),
        self.fence_key(gen_id),
        self.lease_key(gen_id),
      ],
      args=[token, self._ttl_seconds, fence_arg],
    )
    ok = int(result[0])
    if ok == 0:
      err_code = decode_redis_str(result[1])
      if err_code == "STALE_FENCE":
        raise StaleLeaseError(f"Stale fence {fence} for {gen_id!r}")
      raise GenerationClosedError(f"Generation {gen_id!r} is already terminal")

    index = int(decode_redis_str(result[1]))
    entry_id = decode_redis_str(result[2])
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
      fields = decode_fields(raw_fields)
      if fields.get("kind") == "token":
        return int(fields["index"])
    return -1

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
      raw_entries = extract_xread_entries(response)
      if raw_entries:
        events, next_cursor, saw_terminal = parse_stream_entries(raw_entries)
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
      blocked_entries = extract_xread_entries(blocked)
      if not blocked_entries:
        break

      events, next_cursor, saw_terminal = parse_stream_entries(blocked_entries)
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
    *,
    fence: int | None = None,
  ) -> TerminalState:
    """Atomically mark ``gen_id`` as terminal and wake any blocked ``XREAD`` readers."""
    has_reason = "1" if reason is not None else "0"
    fence_arg = str(fence) if fence is not None else ""
    result = await self._mark_terminal_script(
      keys=[
        self.stream_key(gen_id),
        self.terminal_key(gen_id),
        self.fence_key(gen_id),
        self.lease_key(gen_id),
      ],
      args=[status.value, reason or "", has_reason, self._ttl_seconds, fence_arg],
    )
    ok = int(result[0])
    if ok == 0:
      err_code = decode_redis_str(result[1])
      if err_code == "STALE_FENCE":
        raise StaleLeaseError(f"Stale fence {fence} for {gen_id!r}")
      raise GenerationClosedError(f"Generation {gen_id!r} is already terminal")

    data = json.loads(decode_redis_str(result[1]))
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
    data = json.loads(decode_redis_str(raw))
    return TerminalState(
      status=TerminalStatus(data["status"]),
      reason=data["reason"],
      last_index=data["last_index"],
    )

  async def acquire_lease(
    self,
    gen_id: str,
    owner_id: str,
    *,
    ttl_ms: int,
  ) -> Lease | None:
    """Atomically acquire the producer lease for ``gen_id`` with explicit TTLs."""
    if ttl_ms <= 0:
      raise ValueError(f"ttl_ms must be positive, got {ttl_ms}")
    result = await self._acquire_lease_script(
      keys=[self.lease_key(gen_id), self.fence_key(gen_id)],
      args=[owner_id, ttl_ms, self._ttl_seconds],
    )
    if int(result[0]) == 0:
      return None
    return Lease(gen_id=gen_id, owner_id=owner_id, fence=int(decode_redis_str(result[1])))

  async def renew_lease(
    self,
    gen_id: str,
    owner_id: str,
    fence: int,
    *,
    ttl_ms: int,
  ) -> Lease:
    """Extend lease TTL atomically; raise ``StaleLeaseError`` if expired or superseded."""
    if ttl_ms <= 0:
      raise ValueError(f"ttl_ms must be positive, got {ttl_ms}")
    ok = await self._renew_lease_script(
      keys=[self.lease_key(gen_id), self.fence_key(gen_id)],
      args=[f"{owner_id}:{fence}", ttl_ms, self._ttl_seconds],
    )
    if int(ok) == 0:
      raise StaleLeaseError(f"Lease for {gen_id!r} is expired or superseded")
    return Lease(gen_id=gen_id, owner_id=owner_id, fence=fence)

  async def release_lease(self, gen_id: str, owner_id: str, fence: int) -> bool:
    """Release lease atomically; raise ``StaleLeaseError`` if expired or superseded."""
    ok = await self._release_lease_script(
      keys=[self.lease_key(gen_id)],
      args=[f"{owner_id}:{fence}"],
    )
    if int(ok) == 0:
      raise StaleLeaseError(f"Lease for {gen_id!r} is expired or superseded")
    return True

  async def current_fence(self, gen_id: str) -> int | None:
    """Return the active fencing token for ``gen_id``, or ``None`` if no lease is active."""
    raw = await self._redis.get(self.lease_key(gen_id))
    if raw is None:
      return None
    _, _, fence_str = decode_redis_str(raw).rpartition(":")
    return int(fence_str)
