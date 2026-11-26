# ADR-001: Append-Only Token Log and Opaque SSE Event IDs

## Status
Accepted

## Context
A streaming LLM gateway must allow any replica to serve a reconnecting client at `Last-Event-ID`
without gaps (**I1**), duplicates (**I2**), or byte-level corruption across reconnects (**I5**),
and must freeze the stream after any terminal event (**I4**). Keeping stream buffers in process
memory binds clients to a single replica and loses state on restart.

## Decision
We store every generation's output in a Redis Stream (`gen:{gen_id}:stream`) paired with a
terminal marker (`gen:{gen_id}:terminal`), and emit opaque SSE `id:` lines formatted as:

```text
v1:<index>:<redis-entry-id>
```

- `index` is a 0-based monotonic token sequence number used to detect `OffsetAheadError` if a
  client resumes with an index beyond the log's latest token.
- `<redis-entry-id>` is the Redis Stream entry ID passed directly to `XREAD`, which strictly yields
  entries with ID `> cursor`.
- Both `append_token` and `mark_terminal` execute as atomic Redis Lua scripts:
  - `APPEND_TOKEN_LUA` checks `EXISTS gen:{gen_id}:terminal`, derives `index = XLEN`, calls
    `XADD` + `EXPIRE` in one round trip, and raises `GenerationClosedError` if terminal.
  - `MARK_TERMINAL_LUA` checks `EXISTS gen:{gen_id}:terminal`, `SET`s the JSON `TerminalState`
    with `EX`, appends a `kind="terminal"` sentinel entry to `gen:{gen_id}:stream` (waking any
    blocked `XREAD` readers immediately), and refreshes `EXPIRE`.

## Consequences
- **No crash window:** `XADD`/`SET` and `EXPIRE` happen atomically inside Lua, so no key is ever
  created without a TTL.
- **Process-stateless fan-out:** Any replica can serve any subscriber from `0-0` or `Last-Event-ID`.
- **Cost:** One Lua evaluation per token and one extra sentinel entry per completed generation.
