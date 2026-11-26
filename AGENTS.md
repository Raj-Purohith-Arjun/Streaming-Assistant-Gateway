# AGENTS.md — Streaming Assistant Gateway

Read this fully before any task. If a request conflicts with this file, stop and say so.

## What this project is

A **stateful streaming data plane for LLM traffic**. It owns the lifetime of a generation,
not just the routing of a request.

Core inversion: **the connection does not own the stream — an append-only log does.**
Tokens are written once to a Redis Stream; SSE connections are readers at an offset.
Any replica can therefore serve any resume, and the process holds no stream state.

Four features must hold *simultaneously*:
1. **Resumability** — reconnect at `Last-Event-ID`, continue exactly.
2. **Fan-out** — many subscribers on one generation.
3. **Coalescing** — duplicate/retry requests attach to a live generation, never start a second.
4. **Correct cancellation** — deferred on a subscriber refcount, immediate on explicit stop.

(1)–(3) are broken by naive cancel-on-disconnect. Resolving that is the entire point of the project.

## Invariants — never violate, never weaken

Full statements in `docs/invariants.md`. Summary:

- **I1/I2/I5** No gaps, no duplicates; a reassembled multi-reconnect stream equals the full generation byte-for-byte.
- **I3** At most one upstream call in flight per generation, ever.
- **I4** Nothing is appended after a terminal event.
- **I6** Zero subscribers for `T` seconds ⇒ cancelled.
- **I7** A generation with a live subscriber is never cancelled except by explicit stop.
- **I8** Post-abandonment tokens ≤ `T × token_rate`.
- **I9** A duplicate request causes zero additional upstream calls.

If a change could affect any invariant, say which one and point at the test that covers it.

## Architecture map

```
app/core/          config, logging, redis pool
app/sse/           SSE framing: event ids, JSON encoding, heartbeats
app/generation/    lifecycle, state machine, generation keys
app/store/         Redis Streams: XADD, XREAD, lease, subscriber registry
app/producer/      lease election, heartbeat, token pump, cancel polling
app/cancel/        deferred canceller, reaper, stop handling
app/upstream/      adapter interface + synthetic, gemini, groq
app/gateway/       FastAPI routes, pure-ASGI middleware
app/grpc_server/   optional second front-end over the same core
bench/             load + chaos harness, scenario definitions
tests/             unit + property-based invariant tests
docs/              invariants, design, ADRs, benchmarks
```

State machine: `PENDING → RUNNING → DRAINING → {COMPLETED | STOPPED | EXPIRED | FAILED}`.
A new subscriber during `DRAINING` returns the generation to `RUNNING` and cancels the timer.

## Do NOT build (refuse these, even if asked casually mid-task)

- Multi-provider routing, fallback, load balancing
- Cost tracking, budgets, virtual keys
- Semantic or prompt caching
- Any frontend beyond one static demo page
- Kubernetes manifests or Helm charts
- Auth beyond a static API key header
- Multi-region, Redis sharding, global replication
- MCP gateway, guardrails, prompt management

If one of these looks necessary to finish a task, stop and explain why instead of building it.

## Conventions

- Python 3.11+, `async` throughout. No blocking I/O in request paths.
- **Never use `BaseHTTPMiddleware`** — it interferes with streaming responses and breaks
  `ContextVar` propagation. Pure ASGI middleware only.
- SSE payloads are JSON-encoded. Every event carries an `id:`. Heartbeat comment every 15s.
- No bare `except`. Narrow exception types. Cancellation is `asyncio.CancelledError` and is re-raised.
- Type hints on every public function; `mypy` must pass.
- `ruff` and `black`, line length 100.
- No file over 300 lines. Split instead.
- Config via `pydantic-settings` only — no `os.environ` reads scattered in modules.
- Every Redis key has an explicit TTL. No unbounded structures anywhere.

## Workflow rules

1. **Test first, always.** A task is "make `tests/test_x.py::test_y` pass", never "implement X".
   If no test exists for the behaviour, write the test, show it failing, then implement.
2. **Never write an invariant test and its implementation in the same turn.** Tests shaped to
   the implementation prove nothing. If asked to do both, do the test, stop, and ask to continue.
3. **No new dependencies** without asking first.
4. **Small diffs.** One concern per change. If a task needs more than ~200 changed lines, propose
   a split before starting.
5. After any change touching `app/producer/`, `app/cancel/` or `app/store/`, run the full
   invariant test suite, not just the relevant file.
6. Never commit generated protobuf files, `.pyc`, or coverage output.

## Parallel agent boundaries

Agents do not share directories. There is no merge resolution — overlapping writes are lost work.

| Lane | Owns | Must not touch |
| --- | --- | --- |
| core | `app/sse/`, `app/generation/`, `app/store/`, `app/producer/`, `app/cancel/` | `bench/`, `docs/` |
| upstream | `app/upstream/` | `app/producer/`, `app/store/` |
| bench | `bench/`, `tests/chaos/` | all of `app/` |
| docs | `docs/`, `README.md` | all of `app/` |

Declare your lane at the start of a task. If a task needs a file outside your lane, stop and say so.

## Model routing

- **High reasoning** — only for design decisions, the producer lease/takeover path, and bugs that
  survived one fix attempt.
- **Medium** — default for implementation.
- **Low / Flash** — formatting, docstrings, test scaffolding, config, dashboard JSON, docs prose.

Reasoning level is the first dial to turn down. Do not use High for mechanical work.

## Hand-review contract

The lease, takeover and cancellation code will be explained line by line to a human reviewer.
Write it to be read: name things for what they do, comment why rather than what, and prefer an
obvious implementation over a clever one. If a construct needs a paragraph to justify, it is wrong.
