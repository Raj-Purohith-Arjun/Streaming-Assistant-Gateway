# Streaming Assistant Gateway

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/Raj-Purohith-Arjun/Streaming-Assistant-Gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/Raj-Purohith-Arjun/Streaming-Assistant-Gateway/actions/workflows/ci.yml)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type Checked: Mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](https://mypy-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A **stateful streaming data plane for LLM traffic** that decouples upstream generation lifetime from any single client HTTP connection using an append-only Redis Stream and cursor-based SSE offsets (`Last-Event-ID`).

---

## The Problem

A user on a phone submits a prompt, reads 400 tokens, and switches apps or hits a tunnel drop. Because traditional LLM gateways tie the upstream model call directly to the client's HTTP socket, that transient disconnect immediately aborts the upstream stream — the answer is lost, the tokens already generated are wasted, and the client has to start over from zero.

Mid-stream disconnect rates on mobile traffic routinely run a few percent, and every one triggers client retries. With naive cancel-on-disconnect, those retries cannot attach to the in-flight generation (**coalescing**), cannot pick up from the last received token (**resumability**), and cannot share a live stream across multiple tabs (**fan-out**) — so every transient drop turns into a duplicate upstream LLM call.

---

## Quickstart in 60 Seconds

```bash
git clone https://github.com/Raj-Purohith-Arjun/Streaming-Assistant-Gateway.git
cd Streaming-Assistant-Gateway
cp .env.example .env
pip install -e ".[dev]"
docker compose up -d redis
REDIS_URL="redis://localhost:6379/0" make verify   # runs make proto -> ruff -> mypy -> pytest
docker compose up -d --build
```

Open **`http://localhost:8000/demo`** to test **Start / Coalesce**, **Simulate Disconnect** (5-second `DRAINING` window), **Resume (`Last-Event-ID`)**, and **Explicit Stop**. Without Redis running, `make verify` passes 65 tests and skips the 15 live-Redis tests.

---

## The Core Inversion

> **The connection does not own the stream — an append-only log does.**

Tokens are written once to a Redis Stream by a single lease-fenced producer; SSE connections are stateless readers at an offset. Any gateway replica can serve any resume, and a **deferred cancellation grace window (T)** keeps an abandoned stream alive long enough for transient retries to reattach while bounding token waste.

---

## What This Is Not

Routing across providers is a solved commodity, and this gateway deliberately does not compete there. It does one job: owning the lifetime of a streaming generation.

| Capability | This gateway | LiteLLM / Bifrost / Agent Router | Vercel `resumable-stream` |
| --- | :---: | :---: | :---: |
| Multi-provider routing, budgets, virtual keys | No, by design | Yes | No |
| Owns the stream's lifetime | **Yes** | No | Partially |
| Resume across replicas and devices | **Yes** | No | Single app, same deployment |
| Language-neutral, fronts many apps | **Yes** | Yes | No (TypeScript library) |
| Added overhead | milliseconds | microseconds | n/a |

---

## Formal Invariants

Every guarantee is enforced by unit, property-based, and chaos test suites ([docs/invariants.md](docs/invariants.md)):

| Invariant | Guarantee | Description |
| :---: | :--- | :--- |
| **I1 / I2** | **No Gaps & No Duplicates** | Resuming from `Last-Event-ID` delivers strictly subsequent tokens in order. |
| **I3** | **Single Producer** | At most one upstream model call in flight per generation across all replicas. |
| **I4** | **Terminal Finality** | No tokens are appended after a terminal event (`COMPLETED`, `STOPPED`, `FAILED`, `EXPIRED`). |
| **I5** | **Exact Reassembly** | Output reassembled across N disconnects equals the full generation byte-for-byte. |
| **I6 / I7** | **Deferred Grace vs Stop** | Stream stays alive in `DRAINING` for T seconds when clients drop; cancels immediately only on `/stop`. |
| **I8** | **Wasted Token Bound** | Post-abandonment tokens never exceed `(T + heartbeat_interval) * token_rate`. |
| **I9** | **Live Coalescing** | Duplicate or retry requests attach to the existing stream with zero extra upstream calls. |

---

## State Machine & Architecture

```text
               ┌──────────────── new subscriber ──────────────┐
               │                                              │
               ▼                                              │
PENDING ──► RUNNING ──► (subscribers == 0) ──► DRAINING ──┬──┴──► EXPIRED   (T elapsed)
                                                          ├──► COMPLETED (upstream finished)
                                                          ├──► STOPPED   (explicit /stop)
                                                          └──► FAILED    (upstream error)
```

- **`app/core/`**: `pydantic-settings` config, Redis connection pool, structured logging.
- **`app/sse/`**: Typed SSE framing (`token`, `done`, `error`, `discontinuity`) with heartbeats.
- **`app/generation/`**: State machine transitions and atomic coalescing via Redis Lua.
- **`app/store/`**: Append-only Redis Stream (`XADD`/`XREAD`), fenced leases, ZSET subscriber registry.
- **`app/producer/`**: Fenced lease election, token pump, cancel polling.
- **`app/cancel/`**: Deferred drain canceller, reaper worker, explicit stop coordinator.
- **`app/upstream/`**: Adapter protocol (`synthetic`, `gemini`, `groq`).
- **`app/gateway/` & `app/grpc_server/`**: Pure-ASGI FastAPI routes (no `BaseHTTPMiddleware`) and gRPC servicer.

---

## Benchmarks

### 1. Deferred-Cancellation (`T`) Parameter Sweep

Reproduce with `python -m bench.sweep` (`bench/out/sweep_results.json`). Runs against in-memory stores by design to isolate `DeferredCanceller` policy (`n = 30` runs per point; ×15 time compression: 600 tokens @ 300 tok/s = 2.0s simulated / 30s real-equiv; uniform client backoff 0.25–3.0s real-equiv; T = 0 routes through `DeferredCanceller`):

| T (sim) | T (real-equiv) | `wasted_token_ratio` (p10–p90) | `reconnect_success` (p10–p90) | `retry_attach_rate` (p10–p90) | `coalesce_ctrl` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| `0.000` | `0.0s` | `0.0017` (`0.0017–0.0017`) | `0.0000` (`0.0000–0.0000`) | `0.0000` (`0.0000–0.0000`) | `1.0000` |
| `0.033` | `0.5s` | `0.0067` (`0.0067–0.0067`) | `0.0333` (`0.0000–0.0000`) | `0.0333` (`0.0000–0.0000`) | `1.0000` |
| `0.067` | `1.0s` | `0.0083` (`0.0083–0.0083`) | `0.2667` (`0.0000–1.0000`) | `0.2667` (`0.0000–1.0000`) | `1.0000` |
| `0.133` | `2.0s` | `0.0167` (`0.0167–0.0167`) | `0.5667` (`0.0000–1.0000`) | `0.5667` (`0.0000–1.0000`) | `1.0000` |
| `0.333` | `5.0s` | `0.0424` (`0.0417–0.0433`) | `1.0000` (`1.0000–1.0000`) | `1.0000` (`1.0000–1.0000`) | `1.0000` |
| `0.667` | `10.0s` | `0.0750` (`0.0750–0.0750`) | `1.0000` (`1.0000–1.0000`) | `1.0000` (`1.0000–1.0000`) | `1.0000` |
| `1.333` | `20.0s` | `0.1517` (`0.1517–0.1517`) | `1.0000` (`1.0000–1.0000`) | `1.0000` (`1.0000–1.0000`) | `1.0000` |

### 2. Real-Redis Latency Overhead

Reproduce with `REDIS_URL=redis://localhost:6379/0 python -m bench.latency` (`bench/out/latency_results.json`, 50 tokens/stream against live Redis vs unthrottled in-process adapter):

| Concurrency | Direct TTFT (p50 / p99) | Gateway TTFT (p50 / p99) | Added / Token (p50 / p99) |
| ---: | ---: | ---: | ---: |
| `1` | `0.009 / 0.011 ms` | `2.649 / 3.837 ms` | `0.499 / 1.042 ms` |
| `10` | `0.024 / 0.030 ms` | `59.478 / 73.235 ms` | `2.398 / 6.045 ms` |
| `50` | `0.102 / 0.117 ms` | `410.971 / 697.873 ms` | `11.485 / 25.871 ms` |

*A stateless router adds microseconds and forgets your stream; this adds 0.50 ms per token at concurrency 1 (2.40 ms at 10, 11.49 ms at 50) and can resume it.* See [docs/benchmarks/README.md](docs/benchmarks/README.md).

---

## API Reference & Configuration

- `POST /v1/stream` — Start or coalesce onto a generation (`{"session_id", "user_id", "prompt"}`); returns `text/event-stream` and `X-Generation-ID`.
- `GET /v1/generations/{generation_id}/stream` — Resume SSE from `Last-Event-ID`.
- `POST /v1/generations/{generation_id}/stop` — Immediately transition to `STOPPED`.
- `GET /healthz` — Health check (`{"status": "ok"}`).

| Variable | Default | Description |
| :--- | :--- | :--- |
| `UPSTREAM_PROVIDER` | `synthetic` | `synthetic`, `gemini`, or `groq`. |
| `UPSTREAM_API_KEY` | `""` | Provider API key (local `.env` only). |
| `UPSTREAM_MODEL` | `gemini-2.5-flash` | Model passed to upstream adapter. |
| `SYNTHETIC_TOKENS_PER_SECOND` | `4.0` | Token rate in synthetic mode. |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL. |
| `DRAIN_TIMEOUT_SECONDS` | `5.0` | Grace window T before abandoning detached streams. |
| `LEASE_TTL_MS` | `5000` | Producer lease expiration in milliseconds. |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | `120` | Client rate limit budget. |

---

## Known Limitations

- **Producer death cannot resume the upstream LLM call:** Replicas share the Redis Stream and lease state, so SSE readers survive gateway restarts up to the last flushed token, but if the active producer process itself dies mid-stream, the upstream HTTP stream cannot be migrated; the generation transitions to `FAILED` / `EXPIRED` rather than silently re-prompting (which would risk non-deterministic duplicate output).
- **Added Redis latency vs stateless routers:** Every token incurs a Redis `XADD` Lua round-trip and `XREAD` wake-up (`0.50 ms` p50 at concurrency 1, `11.49 ms` p50 at concurrency 50).
- **Single-machine benchmark scope:** Both `bench/sweep.py` (in-memory stores) and `bench/latency.py` (local Docker Redis) were measured on a single host.

---

## Prior Art & Architecture Decision Records

Built on ideas from [Vercel `resumable-stream`](https://github.com/vercel/resumable-stream), Upstash Redis Streams resumable AI streaming patterns, and deferred-cancellation grace windows for transient mobile disconnects.

- [ADR-001: Token Log & Cursor-Based SSE IDs](docs/adr/001-token-log-and-sse-ids.md)
- [ADR-002: Fenced Leases & Single-Producer Election](docs/adr/002-lease-fencing-and-producer.md)
- [ADR-003: Subscriber ZSET & Deferred Cancellation](docs/adr/003-subscriber-zset-and-deferred-cancel.md)
- [ADR-004: Choosing the Default Drain Timeout (`DRAIN_TIMEOUT_SECONDS`)](docs/adr/004-choosing-drain-timeout.md)

---

## Development & Test Suite

```bash
docker compose up -d redis
REDIS_URL="redis://localhost:6379/0" make verify   # make proto -> ruff -> mypy -> pytest (80 tests)
```

MIT License. See [LICENSE](LICENSE).
