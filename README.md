# Streaming Assistant Gateway

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Tests Passing](https://img.shields.io/badge/tests-80%20passed-success.svg)](tests/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type Checked: Mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](https://mypy-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A **stateful streaming data plane for LLM traffic**. The gateway decouples the lifetime of an LLM generation from any single HTTP connection by writing tokens to an append-only Redis Stream. SSE connections act as cursor-based readers at an offset (`Last-Event-ID`), allowing any replica to serve seamless reconnects without storing local stream state.

---

## The Core Inversion

> **The connection does not own the stream — an append-only log does.**

Traditional LLM gateways tie upstream generation lifetime to the client's HTTP connection. When a client disconnects (tab switch, network glitch, mobile lock), naive gateways immediately cancel the upstream generation. This breaks:
1. **Resumability** — Reconnecting clients cannot pick up where they left off.
2. **Fan-Out** — Multiple UI subscribers cannot tap into a single live generation.
3. **Request Coalescing** — Retries and identical concurrent requests spawn duplicate, expensive upstream LLM calls.

Streaming Assistant Gateway resolves all three simultaneously by introducing a **deferred cancellation grace window ($T$)**, single-producer lease election, and cursor-based SSE offsets.

---

## Formal Invariants

Every guarantee is enforced by property-based and chaos test suites:

| Invariant | Guarantee | Description |
| :---: | :--- | :--- |
| **I1 / I2** | **No Gaps & No Duplicates** | Resuming from `Last-Event-ID` delivers strictly subsequent tokens in order. |
| **I3** | **Single Producer** | At most one upstream model call in flight per generation across all replicas. |
| **I4** | **Terminal Finality** | No tokens are appended after a terminal event (`COMPLETED`, `STOPPED`, `FAILED`, `EXPIRED`). |
| **I5** | **Exact Reassembly** | Output reassembled across $N$ disconnects equals the full generation byte-for-byte. |
| **I6 / I7** | **Deferred Grace vs Stop** | Stream stays alive in `DRAINING` for $T$ seconds when clients drop; cancels immediately only on explicit `/stop`. |
| **I8** | **Wasted Token Bound** | Post-abandonment tokens never exceed $T \times \text{token\_rate}$. |
| **I9** | **Live Coalescing** | Duplicate or retry requests attach to the existing stream with zero extra upstream calls. |

*Detailed formal definitions: [docs/invariants.md](docs/invariants.md).*

---

## State Machine

```text
               ┌──────────────── new subscriber ──────────────┐
               │                                              │
               ▼                                              │
PENDING ──► RUNNING ──► (subscribers == 0) ──► DRAINING ──┬──┴──► EXPIRED   (T elapsed)
                                                          ├──► COMPLETED (upstream finished)
                                                          ├──► STOPPED   (explicit /stop)
                                                          └──► FAILED    (upstream error)
```

- When the last subscriber disconnects, generation moves to `DRAINING` for $T$ seconds (default: 5.0s).
- If a client reconnects during `DRAINING`, the timer is cancelled and status reverts to `RUNNING`.
- An explicit `/stop` skips `DRAINING` and halts immediately.

---

## Architecture

```text
Client A (Web) ──┐        ┌─────────────────────────┐
                 ├─► SSE ─┤  Gateway (FastAPI)      │
Client B (Mobile)┘        │  Pure-ASGI Middleware   │
                          └───────────┬─────────────┘
                                      │  Lease & Streams
                                      ▼
Client C (gRPC) ──► gRPC ─► [ Redis Stream Log ] ◄── Single Producer ◄── Upstream LLM
                            - XADD / XREAD Lua         - Fenced Leases    (Gemini, Groq,
                            - ZSET Subscribers         - Heartbeat Pump    Synthetic)
```

- **`app/core/`**: Pydantic Settings, Redis connection pool, structured logging.
- **`app/sse/`**: Typed SSE event framing (`token`, `done`, `error`, `discontinuity`) with heartbeats.
- **`app/generation/`**: State machine transitions and distributed atomic coalescing via Redis Lua.
- **`app/store/`**: Append-only Redis Stream operations, fenced leases, and active subscriber tracking.
- **`app/producer/`**: Fenced lease election, token pump, and cancel polling.
- **`app/cancel/`**: Deferred drain canceller, reaper background worker, and explicit stop coordinator.
- **`app/upstream/`**: Upstream adapter protocol with implementations for Google Gemini, Groq, and Synthetic.
- **`app/gateway/`**: Pure-ASGI middleware (zero `BaseHTTPMiddleware`) and FastAPI endpoints.
- **`app/grpc_server/`**: Streaming gRPC servicer stubs layered directly over the shared core service.

---

## Quickstart in 60 Seconds

### 1. Clone & Configure
```bash
git clone https://github.com/Raj-Purohith-Arjun/Streaming-Assistant-Gateway.git
cd Streaming-Assistant-Gateway
cp .env.example .env
```

### 2. Start with Docker Compose
```bash
docker compose up -d --build
```
This starts:
- **Gateway**: HTTP on port `8000`, gRPC on port `50051`.
- **Redis**: Port `6379` with automatic health checks.

### 3. Open Interactive Demo
Navigate to **`http://localhost:8000/demo`** in your browser.

- **Start / Coalesce**: Initiates generation or attaches to a live stream.
- **Simulate Disconnect**: Aborts client connection, triggering the 5-second `DRAINING` window.
- **Resume (Last-Event-ID)**: Reconnects seamlessly without missing or duplicated tokens.
- **Explicit Stop**: Immediately terminates stream to terminal `STOPPED` state.

---

## API Reference

### HTTP / SSE Endpoints

#### 1. Start or Attach to Generation
```http
POST /v1/stream
Content-Type: application/json

{
  "session_id": "session-123",
  "user_id": "user-456",
  "prompt": "Explain stateful streaming"
}
```
**Response**: `text/event-stream` with header `X-Generation-ID: gen-<uuid>`.

#### 2. Resume Stream by Offset
```http
GET /v1/generations/{generation_id}/stream
Last-Event-ID: v1:12:1795815734238-0
```
**Response**: Resumes stream starting strictly after token index 12.

#### 3. Explicit Stop
```http
POST /v1/generations/{generation_id}/stop
```
**Response**: `{"generation_id": "...", "status": "STOPPED"}`.

#### 4. Health Check
```http
GET /healthz
```
**Response**: `{"status": "ok"}`.

---

## Configuration (`.env`)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `UPSTREAM_PROVIDER` | `synthetic` | Upstream provider: `synthetic`, `gemini`, or `groq`. |
| `UPSTREAM_API_KEY` | `""` | API key for Gemini or Groq (kept in local `.env`, never committed). |
| `UPSTREAM_MODEL` | `gemini-2.5-flash` | Model identifier passed to upstream adapter. |
| `SYNTHETIC_TOKENS_PER_SECOND` | `4.0` | Token emission rate for synthetic mode. |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL. |
| `DRAIN_TIMEOUT_SECONDS` | `5.0` | Grace window $T$ before abandoning a detached generation. |
| `LEASE_TTL_MS` | `5000` | Producer lease expiration in milliseconds. |
| `RATE_LIMIT_REQUESTS_PER_MINUTE`| `120` | Client rate limit budget. |

---

## Empirical Benchmark: The $T$ Parameter Sweep

Run the trade-off sweep:
```bash
python -m bench.sweep
```

| $T$ (seconds) | Wasted Token Ratio | Reconnect Success Rate | Coalesce Hit Rate |
| :---: | :---: | :---: | :---: |
| `0.00` | 0.0% | **0.0%** (Immediate abort) | 100% |
| `0.05` | 7.5% | 0.0% | 100% |
| `0.10` | 16.2% | 25.0% | 100% |
| `0.25` | 40.6% | 87.5% | 100% |
| **`0.50`** | **76.2%** | **100.0%** | **100%** |
| **`1.00`** | **90.0%** | **100.0%** | **100%** |

*Key finding: Setting $T \ge 0.5\text{s}$ gives 100% reconnect resilience while strictly bounding wasted token spend.*

---

## Development & Test Suite

Verify all 80 tests (unit, Hypothesis property tests, chaos simulations, Redis integration):

```bash
# 1. Start local Redis
docker compose up -d redis

# 2. Run full suite
REDIS_URL="redis://localhost:6379/0" pytest tests/ -v

# 3. Lint and type-check
ruff check app/ tests/ bench/
mypy app/ tests/ bench/
```

---

## License

MIT License. See [LICENSE](LICENSE) for details.
