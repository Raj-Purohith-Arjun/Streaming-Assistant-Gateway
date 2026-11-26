from uuid import uuid4

from redis.asyncio import Redis

from app.generation.state import GenerationStatus, InvalidTransitionError

DEFAULT_REGISTRY_TTL_SECONDS = 3600

# Atomically coalesce onto a live (non-terminal) generation or create a new PENDING one with TTL.
_GET_OR_CREATE_LUA = """
local existing = redis.call('GET', KEYS[1])
if existing then
  local status_key = 'gen:' .. existing .. ':meta'
  local st = redis.call('GET', status_key)
  if st and st ~= 'COMPLETED' and st ~= 'STOPPED' and st ~= 'EXPIRED' and st ~= 'FAILED' then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    redis.call('EXPIRE', status_key, tonumber(ARGV[2]))
    return {existing, '0'}
  end
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
redis.call('SET', KEYS[2], ARGV[3], 'EX', tonumber(ARGV[2]))
return {ARGV[1], '1'}
"""

# Atomically validate state machine transition and refresh TTL.
_TRANSITION_LUA = """
local current = redis.call('GET', KEYS[1])
if not current then
  return {0, 'NOT_FOUND'}
end
local target = ARGV[1]
local valid = false
if current == 'PENDING' then
  valid = (target == 'RUNNING')
elseif current == 'RUNNING' then
  valid = (target == 'DRAINING' or target == 'COMPLETED' or target == 'STOPPED' or target == 'FAILED')
elseif current == 'DRAINING' then
  valid = (
    target == 'RUNNING' or target == 'COMPLETED' or target == 'STOPPED'
    or target == 'EXPIRED' or target == 'FAILED'
  )
end
if not valid then
  return {0, 'INVALID', current}
end
redis.call('SET', KEYS[1], target, 'EX', tonumber(ARGV[2]))
return {1, target}
"""


def _decode(value: str | bytes) -> str:
  if isinstance(value, bytes):
    return value.decode("utf-8")
  return value


class RedisGenerationRegistry:
  """Redis-backed generation registry for multi-replica coalescing (I9) and lifecycle state."""

  def __init__(self, redis: Redis, *, ttl_seconds: int = DEFAULT_REGISTRY_TTL_SECONDS) -> None:
    if ttl_seconds <= 0:
      raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
    self._redis = redis
    self._ttl_seconds = ttl_seconds
    self._get_or_create_script = self._redis.register_script(_GET_OR_CREATE_LUA)
    self._transition_script = self._redis.register_script(_TRANSITION_LUA)

  @staticmethod
  def request_key(key: str) -> str:
    """Return the Redis coalescing key for ``key``."""
    return f"gen:coalesce:{key}"

  @staticmethod
  def status_key(gen_id: str) -> str:
    """Return the Redis lifecycle metadata key for ``gen_id``."""
    return f"gen:{gen_id}:meta"

  async def get_or_create(self, request_key: str) -> tuple[str, bool]:
    """Return ``(gen_id, created)`` atomically across replicas, coalescing live generations."""
    new_gen_id = f"gen-{uuid4()}"
    result = await self._get_or_create_script(
      keys=[self.request_key(request_key), self.status_key(new_gen_id)],
      args=[new_gen_id, self._ttl_seconds, GenerationStatus.PENDING.value],
    )
    gen_id = _decode(result[0])
    created = _decode(result[1]) == "1"
    return gen_id, created

  async def transition(self, gen_id: str, target: GenerationStatus) -> GenerationStatus:
    """Atomically transition ``gen_id`` to ``target``; raise on invalid/unknown state."""
    result = await self._transition_script(
      keys=[self.status_key(gen_id)],
      args=[target.value, self._ttl_seconds],
    )
    if int(result[0]) == 0:
      err_kind = _decode(result[1])
      if err_kind == "NOT_FOUND":
        raise KeyError(f"Unknown generation: {gen_id!r}")
      current = _decode(result[2])
      raise InvalidTransitionError(f"Cannot transition from {current} to {target.value}")
    return target

  async def get_status(self, gen_id: str) -> GenerationStatus:
    """Return the current ``GenerationStatus`` for ``gen_id``, or raise ``KeyError``."""
    raw = await self._redis.get(self.status_key(gen_id))
    if raw is None:
      raise KeyError(f"Unknown generation: {gen_id!r}")
    return GenerationStatus(_decode(raw))
