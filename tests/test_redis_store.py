import asyncio
import uuid
from collections.abc import AsyncIterator
from unittest.mock import MagicMock

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import get_settings
from app.store.protocol import (
  GenerationClosedError,
  GenerationStore,
  OffsetAheadError,
  TerminalStatus,
  TokenEvent,
  format_event_id,
)
from app.store.redis_store import RedisGenerationStore


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
  redis_url = get_settings().redis_url
  client = Redis.from_url(redis_url, socket_connect_timeout=0.5, decode_responses=True)
  try:
    await client.ping()
  except (RedisError, TimeoutError, OSError):
    await client.aclose()
    pytest.skip(f"Redis not reachable at {redis_url}")

  try:
    yield client
  finally:
    await client.aclose()


def test_redis_store_satisfies_protocol_and_validates_ttl() -> None:
  mock_redis = MagicMock(spec=Redis)
  store = RedisGenerationStore(mock_redis, ttl_seconds=60)
  assert isinstance(store, GenerationStore)
  assert store.stream_key("abc") == "gen:abc:stream"
  assert store.terminal_key("abc") == "gen:abc:terminal"

  with pytest.raises(ValueError, match="positive"):
    RedisGenerationStore(mock_redis, ttl_seconds=0)


async def test_redis_keys_have_explicit_ttl(redis_client: Redis) -> None:
  store = RedisGenerationStore(redis_client, ttl_seconds=120)
  gen_id = f"gen-{uuid.uuid4()}"

  await store.append_token(gen_id, "tok0")
  stream_ttl = await redis_client.ttl(store.stream_key(gen_id))
  assert 0 < stream_ttl <= 120

  await store.mark_terminal(gen_id, TerminalStatus.COMPLETED, reason="done")
  stream_ttl_after = await redis_client.ttl(store.stream_key(gen_id))
  terminal_ttl = await redis_client.ttl(store.terminal_key(gen_id))
  assert 0 < stream_ttl_after <= 120
  assert 0 < terminal_ttl <= 120


async def test_redis_atomic_post_terminal_rejection(redis_client: Redis) -> None:
  store = RedisGenerationStore(redis_client, ttl_seconds=120)
  gen_id = f"gen-{uuid.uuid4()}"

  await store.append_token(gen_id, "a")
  terminal = await store.mark_terminal(gen_id, TerminalStatus.STOPPED, reason="user-stop")
  assert terminal.status == TerminalStatus.STOPPED
  assert terminal.last_index == 0

  with pytest.raises(GenerationClosedError):
    await store.append_token(gen_id, "b")

  with pytest.raises(GenerationClosedError):
    await store.mark_terminal(gen_id, TerminalStatus.COMPLETED)

  events = [e async for e in store.read_from(gen_id)]
  assert [e.data for e in events] == ["a"]


async def test_redis_offset_ahead_raises(redis_client: Redis) -> None:
  store = RedisGenerationStore(redis_client, ttl_seconds=120)
  gen_id = f"gen-{uuid.uuid4()}"

  with pytest.raises(OffsetAheadError):
    async for _ in store.read_from(gen_id, offset=format_event_id(0, "0-1")):
      pass

  await store.append_token(gen_id, "only-one")
  with pytest.raises(OffsetAheadError):
    async for _ in store.read_from(gen_id, offset=format_event_id(3, "0-4")):
      pass


async def test_redis_blocking_xread_receives_tokens_and_wakes_on_terminal(
  redis_client: Redis,
) -> None:
  store = RedisGenerationStore(redis_client, ttl_seconds=120)
  gen_id = f"gen-{uuid.uuid4()}"
  received: list[TokenEvent] = []

  async def reader() -> None:
    async for event in store.read_from(gen_id, block_ms=2000):
      received.append(event)

  reader_task = asyncio.create_task(reader())
  await asyncio.sleep(0.05)
  await store.append_token(gen_id, "hello ")
  await store.append_token(gen_id, "world")
  await store.mark_terminal(gen_id, TerminalStatus.COMPLETED)

  await asyncio.wait_for(reader_task, timeout=2.0)
  assert [e.data for e in received] == ["hello ", "world"]
  assert [e.index for e in received] == [0, 1]
