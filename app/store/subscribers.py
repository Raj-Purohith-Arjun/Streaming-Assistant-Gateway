import math
import time
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from redis.asyncio import Redis

# Lua scripts use Redis server TIME so expiry checks do not depend on client clock skew.
_REGISTER_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return redis.call('ZCARD', KEYS[1])
"""

_UNREGISTER_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return redis.call('ZCARD', KEYS[1])
"""

_HEARTBEAT_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
"""

_ACTIVE_COUNT_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000.0)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
return redis.call('ZCARD', KEYS[1])
"""


@runtime_checkable
class SubscriberRegistry(Protocol):
  """Tracks active subscribers per generation with TTL-based crash pruning (I6, I7)."""

  async def register(self, gen_id: str, sub_id: str) -> int:
    """Register ``sub_id`` on ``gen_id`` and return the updated active count."""
    ...

  async def unregister(self, gen_id: str, sub_id: str) -> int:
    """Remove ``sub_id`` from ``gen_id`` and return the updated active count."""
    ...

  async def heartbeat(self, gen_id: str, sub_id: str) -> bool:
    """Extend ``sub_id`` TTL; return ``False`` if absent or already expired."""
    ...

  async def active_count(self, gen_id: str) -> int:
    """Prune expired subscribers and return the active subscriber count for ``gen_id``."""
    ...


class InMemorySubscriberRegistry:
  """In-memory ``SubscriberRegistry`` for hermetic cancellation and churn tests."""

  def __init__(
    self,
    ttl_seconds: float = 30.0,
    clock: Callable[[], float] | None = None,
  ) -> None:
    if ttl_seconds <= 0:
      raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
    self._ttl_seconds = ttl_seconds
    self._clock = clock or time.monotonic
    self._subscribers: dict[str, dict[str, float]] = {}

  def _prune(self, gen_id: str) -> dict[str, float]:
    subs = self._subscribers.get(gen_id)
    if subs is None:
      return {}
    now = self._clock()
    expired = [sid for sid, expires_at in subs.items() if expires_at <= now]
    for sid in expired:
      del subs[sid]
    return subs

  async def register(self, gen_id: str, sub_id: str) -> int:
    subs = self._prune(gen_id)
    if gen_id not in self._subscribers:
      self._subscribers[gen_id] = subs
    subs[sub_id] = self._clock() + self._ttl_seconds
    return len(subs)

  async def unregister(self, gen_id: str, sub_id: str) -> int:
    subs = self._prune(gen_id)
    subs.pop(sub_id, None)
    return len(subs)

  async def heartbeat(self, gen_id: str, sub_id: str) -> bool:
    subs = self._prune(gen_id)
    if sub_id not in subs:
      return False
    subs[sub_id] = self._clock() + self._ttl_seconds
    return True

  async def active_count(self, gen_id: str) -> int:
    return len(self._prune(gen_id))


class RedisSubscriberRegistry:
  """Redis ZSET-backed ``SubscriberRegistry`` with atomic TTL pruning in Lua."""

  def __init__(self, redis: Redis, ttl_seconds: float = 30.0) -> None:
    if ttl_seconds <= 0:
      raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
    self._redis = redis
    self._ttl_seconds = ttl_seconds
    self._key_ttl_seconds = max(1, math.ceil(ttl_seconds) + 60)
    self._register_script = self._redis.register_script(_REGISTER_LUA)
    self._unregister_script = self._redis.register_script(_UNREGISTER_LUA)
    self._heartbeat_script = self._redis.register_script(_HEARTBEAT_LUA)
    self._active_count_script = self._redis.register_script(_ACTIVE_COUNT_LUA)

  @staticmethod
  def subscribers_key(gen_id: str) -> str:
    """Return the Redis ZSET key for active subscribers of ``gen_id``."""
    return f"gen:{gen_id}:subscribers"

  async def register(self, gen_id: str, sub_id: str) -> int:
    count = await self._register_script(
      keys=[self.subscribers_key(gen_id)],
      args=[sub_id, self._ttl_seconds, self._key_ttl_seconds],
    )
    return int(count)

  async def unregister(self, gen_id: str, sub_id: str) -> int:
    count = await self._unregister_script(
      keys=[self.subscribers_key(gen_id)],
      args=[sub_id, self._key_ttl_seconds],
    )
    return int(count)

  async def heartbeat(self, gen_id: str, sub_id: str) -> bool:
    ok = await self._heartbeat_script(
      keys=[self.subscribers_key(gen_id)],
      args=[sub_id, self._ttl_seconds, self._key_ttl_seconds],
    )
    return int(ok) == 1

  async def active_count(self, gen_id: str) -> int:
    count = await self._active_count_script(
      keys=[self.subscribers_key(gen_id)],
      args=[],
    )
    return int(count)
