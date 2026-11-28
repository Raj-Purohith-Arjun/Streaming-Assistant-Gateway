import asyncio
import itertools
import uuid
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
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
  parse_event_id,
)
from app.store.redis_store import RedisGenerationStore
from tests.fakes import InMemoryGenerationStore

T = TypeVar("T")
StoreFactory = Callable[[], GenerationStore]


def _run_async(coro: Coroutine[Any, Any, T]) -> T:
  """Run an async coroutine inside a fresh event loop for sync Hypothesis tests."""
  return asyncio.run(coro)


async def _can_connect_redis(redis_url: str) -> bool:
  client = Redis.from_url(redis_url, socket_connect_timeout=0.5, decode_responses=True)
  try:
    return bool(await client.ping())
  except (RedisError, TimeoutError, OSError):
    return False
  finally:
    await client.aclose()


@pytest.fixture(scope="module", params=["memory", "redis"])
def store_factory(request: pytest.FixtureRequest) -> StoreFactory:
  """Return a factory creating a fresh ``GenerationStore`` per test example.

  Parameterized by backend name so the same invariant suite runs unchanged
  against ``InMemoryGenerationStore`` and ``RedisGenerationStore``.
  """
  if request.param == "memory":
    return InMemoryGenerationStore
  if request.param == "redis":
    redis_url = get_settings().redis_url
    if not _run_async(_can_connect_redis(redis_url)):
      pytest.skip(f"Redis not reachable at {redis_url}")
    return lambda: RedisGenerationStore(Redis.from_url(redis_url, decode_responses=True))
  raise ValueError(f"Unknown store backend: {request.param}")


async def _populate_generation(
  store: GenerationStore,
  gen_id: str,
  tokens: list[str],
  status: TerminalStatus = TerminalStatus.COMPLETED,
) -> list[TokenEvent]:
  events: list[TokenEvent] = []
  for token in tokens:
    events.append(await store.append_token(gen_id, token))
  await store.mark_terminal(gen_id, status)
  return events


async def _read_with_reconnects(
  store: GenerationStore,
  gen_id: str,
  disconnect_after_counts: list[int],
) -> list[TokenEvent]:
  """Simulate a client reading ``gen_id`` across multiple disconnect/reconnect cycles."""
  received: list[TokenEvent] = []
  last_event_id: str | None = None

  for limit in disconnect_after_counts:
    if limit == 0:
      continue
    count_this_leg = 0
    async for event in store.read_from(gen_id, offset=last_event_id):
      received.append(event)
      last_event_id = event.event_id
      count_this_leg += 1
      if count_this_leg >= limit:
        break

  # Final leg reads everything remaining until the stream ends.
  async for event in store.read_from(gen_id, offset=last_event_id):
    received.append(event)
    last_event_id = event.event_id

  return received


# ---------------------------------------------------------------------------
# I1: Resume without gaps
# ---------------------------------------------------------------------------


@given(
  tokens=st.lists(st.text(min_size=1, max_size=24), min_size=1, max_size=40),
  disconnect_counts=st.lists(st.integers(min_value=0, max_value=10), max_size=8),
  resume_seed=st.integers(min_value=0, max_value=100),
)
@settings(max_examples=50, deadline=None)
def test_resume_no_gaps(
  store_factory: StoreFactory,
  tokens: list[str],
  disconnect_counts: list[int],
  resume_seed: int,
) -> None:
  """I1: Resuming at offset k delivers every token with index > k, in order."""

  async def _execute() -> None:
    store = store_factory()
    gen_id = f"gen-{uuid.uuid4()}"
    written = await _populate_generation(store, gen_id, tokens)

    # Direct resume from an arbitrary offset k in [0, len(tokens) - 1].
    k = resume_seed % len(written)
    resumed: list[TokenEvent] = []
    async for event in store.read_from(gen_id, offset=written[k].event_id):
      resumed.append(event)

    assert [e.index for e in resumed] == list(range(k + 1, len(written)))

    # Multi-reconnect schedule from the beginning also has zero gaps.
    received = await _read_with_reconnects(store, gen_id, disconnect_counts)
    assert [e.index for e in received] == list(range(len(written)))

  _run_async(_execute())


# ---------------------------------------------------------------------------
# I2: No duplicates
# ---------------------------------------------------------------------------


@given(
  tokens=st.lists(st.text(min_size=1, max_size=24), min_size=1, max_size=40),
  disconnect_counts=st.lists(st.integers(min_value=0, max_value=10), max_size=8),
)
@settings(max_examples=50, deadline=None)
def test_resume_no_duplicates(
  store_factory: StoreFactory,
  tokens: list[str],
  disconnect_counts: list[int],
) -> None:
  """I2: No token index is delivered twice to the same subscriber."""

  async def _execute() -> None:
    store = store_factory()
    gen_id = f"gen-{uuid.uuid4()}"
    await _populate_generation(store, gen_id, tokens)

    received = await _read_with_reconnects(store, gen_id, disconnect_counts)
    indices = [e.index for e in received]

    assert indices == sorted(set(indices))
    for prev, curr in itertools.pairwise(indices):
      assert curr > prev

  _run_async(_execute())


# ---------------------------------------------------------------------------
# I4: Terminal event is final
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", list(TerminalStatus))
async def test_terminal_is_final(
  store_factory: StoreFactory,
  status: TerminalStatus,
) -> None:
  """I4: After a terminal event, no further tokens are appended to the log."""
  store = store_factory()
  gen_id = f"gen-{uuid.uuid4()}"

  await store.append_token(gen_id, "hello")
  await store.append_token(gen_id, " world")
  terminal = await store.mark_terminal(gen_id, status, reason="test-stop")

  assert terminal.status == status
  assert await store.read_terminal(gen_id) == terminal

  with pytest.raises(GenerationClosedError):
    await store.append_token(gen_id, "extra")

  with pytest.raises(GenerationClosedError):
    await store.mark_terminal(gen_id, TerminalStatus.COMPLETED)

  events = [e async for e in store.read_from(gen_id)]
  assert [e.data for e in events] == ["hello", " world"]
  assert await store.read_terminal(gen_id) == terminal


# ---------------------------------------------------------------------------
# I5: Reassembled stream equals reference byte-for-byte
# ---------------------------------------------------------------------------


@given(
  tokens=st.lists(st.text(min_size=0, max_size=32), min_size=0, max_size=50),
  disconnect_counts=st.lists(st.integers(min_value=0, max_value=12), max_size=10),
)
@settings(max_examples=50, deadline=None)
def test_reassembly_equals_reference(
  store_factory: StoreFactory,
  tokens: list[str],
  disconnect_counts: list[int],
) -> None:
  """I5: Concatenating all tokens across N reconnects equals the reference byte-for-byte."""

  async def _execute() -> None:
    store = store_factory()
    gen_id = f"gen-{uuid.uuid4()}"
    await _populate_generation(store, gen_id, tokens)

    received = await _read_with_reconnects(store, gen_id, disconnect_counts)
    reassembled = "".join(e.data for e in received)
    reference = "".join(tokens)

    assert reassembled == reference
    assert reassembled.encode("utf-8") == reference.encode("utf-8")

  _run_async(_execute())


# ---------------------------------------------------------------------------
# Opaque event id format & offset-ahead detection
# ---------------------------------------------------------------------------


async def test_event_id_format_and_offset_ahead(store_factory: StoreFactory) -> None:
  """Verify ``v1:<index>:<entry-id>`` format and OffsetAheadError on stale/future ids."""
  store = store_factory()
  gen_id = f"gen-{uuid.uuid4()}"

  ev0 = await store.append_token(gen_id, "tok0")
  parsed_idx, parsed_entry = parse_event_id(ev0.event_id)
  assert ev0.event_id == f"v1:0:{ev0.entry_id}"
  assert parsed_idx == 0
  assert parsed_entry == ev0.entry_id

  ahead_id = format_event_id(5, "0-6")
  with pytest.raises(OffsetAheadError):
    async for _ in store.read_from(gen_id, offset=ahead_id):
      pass
