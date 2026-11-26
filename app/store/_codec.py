from typing import Any

from app.store.protocol import TokenEvent, format_event_id


def decode_redis_str(value: str | bytes) -> str:
  """Decode a Redis string or bytes value as UTF-8."""
  if isinstance(value, bytes):
    return value.decode("utf-8")
  return value


def decode_fields(fields: dict[Any, Any]) -> dict[str, str]:
  """Decode a Redis hash/stream entry mapping to ``dict[str, str]``."""
  return {decode_redis_str(k): decode_redis_str(v) for k, v in fields.items()}


def extract_xread_entries(response: Any) -> list[tuple[Any, Any]]:
  """Normalize RESP2 and RESP3 ``XREAD`` responses into ``[(entry_id, fields), ...]``."""
  if not response:
    return []
  if isinstance(response, dict):
    first_stream_entries: list[Any] = next(iter(response.values()), [])
    if first_stream_entries and isinstance(first_stream_entries[0], list):
      return list(first_stream_entries[0])
    return list(first_stream_entries)
  return list(response[0][1])


def parse_stream_entries(
  raw_entries: list[tuple[Any, Any]],
) -> tuple[list[TokenEvent], str | None, bool]:
  """Parse Redis stream entries into ``TokenEvent`` items, last cursor, and terminal flag."""
  events: list[TokenEvent] = []
  last_cursor: str | None = None
  saw_terminal = False
  for raw_entry_id, raw_fields in raw_entries:
    if raw_entry_id is None or not raw_fields:
      continue
    entry_id = decode_redis_str(raw_entry_id)
    last_cursor = entry_id
    fields = decode_fields(raw_fields)
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
