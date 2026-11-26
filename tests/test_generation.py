import asyncio
import uuid

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings
from app.generation import (
  TERMINAL_STATUSES,
  GenerationRegistry,
  GenerationStatus,
  InvalidTransitionError,
  RedisGenerationRegistry,
  validate_transition,
)


async def _connect_redis_or_skip() -> Redis:
  redis_url = get_settings().redis_url
  client = Redis.from_url(redis_url, socket_connect_timeout=0.5, decode_responses=True)
  try:
    await client.ping()
  except (RedisError, TimeoutError, OSError):
    await client.aclose()
    pytest.skip(f"Redis not reachable at {redis_url}")
  return client


def test_state_transitions() -> None:
  """Verify allowed lifecycle transitions and rejection of invalid/post-terminal jumps."""
  assert validate_transition(GenerationStatus.PENDING, GenerationStatus.RUNNING) == (
    GenerationStatus.RUNNING
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.DRAINING) == (
    GenerationStatus.DRAINING
  )
  # The core feature: a new subscriber during DRAINING returns to RUNNING.
  assert validate_transition(GenerationStatus.DRAINING, GenerationStatus.RUNNING) == (
    GenerationStatus.RUNNING
  )
  assert validate_transition(GenerationStatus.DRAINING, GenerationStatus.EXPIRED) == (
    GenerationStatus.EXPIRED
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.COMPLETED) == (
    GenerationStatus.COMPLETED
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.STOPPED) == (
    GenerationStatus.STOPPED
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.FAILED) == (
    GenerationStatus.FAILED
  )

  # Invalid jumps must be rejected.
  with pytest.raises(InvalidTransitionError):
    validate_transition(GenerationStatus.PENDING, GenerationStatus.DRAINING)
  with pytest.raises(InvalidTransitionError):
    validate_transition(GenerationStatus.PENDING, GenerationStatus.COMPLETED)

  # Terminal states are final: zero outgoing transitions (I4).
  for terminal in TERMINAL_STATUSES:
    for target in GenerationStatus:
      with pytest.raises(InvalidTransitionError):
        validate_transition(terminal, target)


@pytest.mark.parametrize("backend", ["memory", "redis"])
async def test_coalesce_attaches(backend: str) -> None:
  """I9: Duplicate requests for a live gen_id attach and cause zero extra upstream calls."""
  redis_client: Redis | None = None
  registry: GenerationRegistry | RedisGenerationRegistry
  if backend == "memory":
    registry = GenerationRegistry()
  else:
    redis_client = await _connect_redis_or_skip()
    registry = RedisGenerationRegistry(redis_client)

  try:
    request_key = f"req-{uuid.uuid4()}"
    upstream_calls = 0

    async def handle_request() -> str:
      nonlocal upstream_calls
      gen_id, created = await registry.get_or_create(request_key)
      if created:
        upstream_calls += 1
        await registry.transition(gen_id, GenerationStatus.RUNNING)
      return gen_id

    results = await asyncio.gather(*(handle_request() for _ in range(10)))

    assert len(set(results)) == 1
    assert upstream_calls == 1
    assert await registry.get_status(results[0]) == GenerationStatus.RUNNING
  finally:
    if redis_client is not None:
      await redis_client.aclose()


async def test_redis_registry_multi_replica_coalesce_and_transitions() -> None:
  """Two RedisGenerationRegistry replicas share coalescing, transitions, and TTLs."""
  redis_a = await _connect_redis_or_skip()
  redis_b = Redis.from_url(get_settings().redis_url, decode_responses=True)
  try:
    replica_a = RedisGenerationRegistry(redis_a, ttl_seconds=120)
    replica_b = RedisGenerationRegistry(redis_b, ttl_seconds=120)
    request_key = f"req-{uuid.uuid4()}"

    gen_id, created_a = await replica_a.get_or_create(request_key)
    assert created_a is True
    assert await replica_a.get_status(gen_id) == GenerationStatus.PENDING
    await replica_a.transition(gen_id, GenerationStatus.RUNNING)

    # Replica B sees the live generation and coalesces onto it (I9).
    gen_id_b, created_b = await replica_b.get_or_create(request_key)
    assert created_b is False
    assert gen_id_b == gen_id
    assert await replica_b.get_status(gen_id) == GenerationStatus.RUNNING

    # Cross-replica lifecycle: RUNNING -> DRAINING (B) -> RUNNING (A) -> COMPLETED (B).
    await replica_b.transition(gen_id, GenerationStatus.DRAINING)
    assert await replica_a.get_status(gen_id) == GenerationStatus.DRAINING
    await replica_a.transition(gen_id, GenerationStatus.RUNNING)
    assert await replica_b.get_status(gen_id) == GenerationStatus.RUNNING
    await replica_b.transition(gen_id, GenerationStatus.COMPLETED)
    assert await replica_a.get_status(gen_id) == GenerationStatus.COMPLETED

    # Invalid / post-terminal transitions must raise InvalidTransitionError.
    with pytest.raises(InvalidTransitionError):
      await replica_a.transition(gen_id, GenerationStatus.RUNNING)
    with pytest.raises(InvalidTransitionError):
      await replica_b.transition(gen_id, GenerationStatus.STOPPED)

    # Every Redis key created by the registry must have an explicit TTL > 0.
    req_ttl = await redis_a.ttl(replica_a.request_key(request_key))
    status_ttl = await redis_a.ttl(replica_a.status_key(gen_id))
    assert 0 < req_ttl <= 120
    assert 0 < status_ttl <= 120
  finally:
    await redis_a.aclose()
    await redis_b.aclose()
