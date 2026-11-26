# Streaming Assistant Gateway

A **stateful streaming data plane for LLM traffic**. The gateway owns the lifetime of each generation in an append-only Redis Stream rather than tying stream state to an individual HTTP connection, allowing any replica to serve resumable SSE streams.

## Core Guarantees

1. **Resumability** — Reconnect with `Last-Event-ID` and continue without gaps or duplicates (**I1, I2, I5**).
2. **Fan-out** — Multiple concurrent SSE subscribers read from the same generation log.
3. **Coalescing** — Duplicate/retry requests attach to a live generation without starting a second upstream call (**I9**).
4. **Deferred & Explicit Cancellation** — Streams enter `DRAINING` for `T` seconds when subscribers drop to zero (reviving to `RUNNING` if a subscriber reconnects), and stop immediately on explicit stop (**I6, I7, I8**).

Lifecycle state machine: `PENDING → RUNNING → DRAINING → {COMPLETED | STOPPED | EXPIRED | FAILED}`.

## Architecture

```text
app/
  core/          pydantic-settings config, structlog logging, Redis pool
  store/         Redis Streams log (XADD/XREAD Lua), lease fencing, ZSET subscribers
  sse/           typed SSE framing (token/done/error/discontinuity), 15s heartbeats
  generation/    lifecycle state machine + multi-replica Redis coalescing registry
  producer/      single-producer lease election, background renewal, cancel polling
  cancel/        deferred drain-timer canceller + immediate explicit stop
  upstream/      UpstreamAdapter protocol + Synthetic, Gemini, and Groq adapters
  gateway/       FastAPI routes (/v1/stream, /v1/generations/{id}/stream, /stop)
  middleware/    pure-ASGI correlation ID, rate-limit, and error-handling middleware
bench/           T-sweep benchmark harness (python -m bench.sweep)
tests/           unit, property-based invariant (Hypothesis), Redis, and chaos tests
docs/            invariants, ADRs, and benchmark sweep results
```

## Documentation

- [Invariants](docs/invariants.md) — formal guarantees (**I1–I9**) and state machine specification.
- [Architecture Decision Records](docs/adr/) — token log & SSE IDs ([ADR-001](docs/adr/001-token-log-and-sse-ids.md)), lease fencing ([ADR-002](docs/adr/002-lease-fencing-and-producer.md)), and ZSET subscribers & deferred cancel ([ADR-003](docs/adr/003-subscriber-zset-and-deferred-cancel.md)).
- [Benchmarks](docs/benchmarks/README.md) — `T`-sweep trade-off measurements and rationale.

## Configuration (`app/core/config.py`)

Configured via `pydantic-settings` (`Settings`):

- `REDIS_URL` (default `redis://redis:6379/0`)
- `DRAIN_TIMEOUT_SECONDS` (default `5.0`)
- `LEASE_TTL_MS` (default `5000`)
- `SUBSCRIBER_TTL_SECONDS` (default `30.0`)
- `GENERATION_TTL_SECONDS` (default `3600`)
- `RATE_LIMIT_REQUESTS_PER_MINUTE` (default `120`), `RATE_LIMIT_BURST` (default `30`)

## Running Tests & Benchmarks

1. Start Redis locally:

   ```bash
   docker compose up -d redis
   ```

2. Run the full test suite (unit, Hypothesis invariants, chaos, and live-Redis tests):

   ```bash
   REDIS_URL=redis://localhost:6379/0 pytest tests/ -v
   ```

3. Run the `T` parameter sweep:

   ```bash
   python -m bench.sweep
   ```
