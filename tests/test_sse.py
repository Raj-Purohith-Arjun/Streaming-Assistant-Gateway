import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from app.sse import (
  DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
  HEARTBEAT_COMMENT,
  SseEncoder,
  SseEventType,
)
from app.store.protocol import InvalidOffsetError, TerminalStatus, TokenEvent, format_event_id


def _parse_sse_frame(frame: str) -> tuple[dict[str, str], dict[str, object]]:
  """Parse a single SSE frame into header fields and decoded JSON data."""
  assert frame.endswith("\n\n"), f"SSE frame must end with blank line: {frame!r}"
  lines = frame[:-2].split("\n")
  fields: dict[str, str] = {}
  for line in lines:
    key, _, value = line.partition(": ")
    fields[key] = value
  payload = json.loads(fields["data"])
  return fields, payload


def test_default_heartbeat_interval_is_15_seconds() -> None:
  assert DEFAULT_HEARTBEAT_INTERVAL_SECONDS == 15.0
  encoder = SseEncoder()
  assert encoder.heartbeat_interval_seconds == 15.0


def test_heartbeat_interval_must_be_positive() -> None:
  with pytest.raises(ValueError, match="positive"):
    SseEncoder(heartbeat_interval_seconds=0)
  with pytest.raises(ValueError, match="positive"):
    SseEncoder(heartbeat_interval_seconds=-5.0)


def test_encode_token_event() -> None:
  encoder = SseEncoder()
  event_id = format_event_id(0, "1700000000000-0")
  token_event = TokenEvent(index=0, entry_id="1700000000000-0", event_id=event_id, data="hello")

  frame = encoder.encode_token(token_event)
  fields, payload = _parse_sse_frame(frame)

  assert fields["id"] == "v1:0:1700000000000-0"
  assert fields["event"] == SseEventType.TOKEN
  assert payload == {"index": 0, "text": "hello"}


def test_encode_done_event() -> None:
  encoder = SseEncoder()
  event_id = format_event_id(4, "1700000000004-0")

  frame = encoder.encode_done(event_id, status=TerminalStatus.COMPLETED, last_index=4)
  fields, payload = _parse_sse_frame(frame)

  assert fields["id"] == "v1:4:1700000000004-0"
  assert fields["event"] == SseEventType.DONE
  assert payload == {"status": "COMPLETED", "last_index": 4}


def test_encode_error_event() -> None:
  encoder = SseEncoder()
  event_id = format_event_id(2, "1700000000002-0")

  frame = encoder.encode_error(event_id, message="upstream timeout", code="UPSTREAM_TIMEOUT")
  fields, payload = _parse_sse_frame(frame)

  assert fields["id"] == "v1:2:1700000000002-0"
  assert fields["event"] == SseEventType.ERROR
  assert payload == {"message": "upstream timeout", "code": "UPSTREAM_TIMEOUT"}


def test_encode_discontinuity_event() -> None:
  encoder = SseEncoder()
  event_id = format_event_id(10, "1700000000010-0")

  frame = encoder.encode_discontinuity(
    event_id,
    reason="producer_failover",
    last_index=9,
  )
  fields, payload = _parse_sse_frame(frame)

  assert fields["id"] == "v1:10:1700000000010-0"
  assert fields["event"] == SseEventType.DISCONTINUITY
  assert payload == {"reason": "producer_failover", "last_index": 9}


def test_encode_event_rejects_malformed_event_id() -> None:
  encoder = SseEncoder()
  with pytest.raises(InvalidOffsetError):
    encoder.encode_event(SseEventType.TOKEN, "invalid-id", {"index": 0, "text": "x"})
  with pytest.raises(InvalidOffsetError):
    encoder.encode_event(SseEventType.TOKEN, "", {"index": 0, "text": "x"})


def test_encode_event_rejects_unknown_event_type() -> None:
  encoder = SseEncoder()
  event_id = format_event_id(0, "1700000000000-0")
  with pytest.raises(ValueError, match="Unsupported SSE event type"):
    encoder.encode_event("custom_unknown", event_id, {"ok": True})


def test_encode_heartbeat_has_no_id_and_matches_format() -> None:
  encoder = SseEncoder()
  hb = encoder.encode_heartbeat()
  assert hb == HEARTBEAT_COMMENT
  assert hb == ": heartbeat\n\n"
  assert "id:" not in hb


async def test_heartbeat_generator_yields_comments_at_configured_interval() -> None:
  encoder = SseEncoder(heartbeat_interval_seconds=0.01)
  gen = encoder.heartbeat_generator()
  try:
    first = await anext(gen)
    second = await anext(gen)
    assert first == ": heartbeat\n\n"
    assert second == ": heartbeat\n\n"
  finally:
    await gen.aclose()


async def test_wrap_with_heartbeats_emits_heartbeat_during_idle_and_preserves_events() -> None:
  encoder = SseEncoder(heartbeat_interval_seconds=0.02)
  ev1 = encoder.encode_token(
    TokenEvent(
      index=0,
      entry_id="1700000000000-0",
      event_id=format_event_id(0, "1700000000000-0"),
      data="a",
    )
  )
  ev2 = encoder.encode_token(
    TokenEvent(
      index=1,
      entry_id="1700000000001-0",
      event_id=format_event_id(1, "1700000000001-0"),
      data="b",
    )
  )

  async def source_events() -> AsyncIterator[str]:
    yield ev1
    await asyncio.sleep(0.05)
    yield ev2

  collected: list[str] = []
  async for frame in encoder.wrap_with_heartbeats(source_events()):
    collected.append(frame)

  assert collected[0] == ev1
  assert ": heartbeat\n\n" in collected[1:-1]
  assert collected[-1] == ev2
