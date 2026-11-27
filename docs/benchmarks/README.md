# Deferred-Cancellation (`T`) Parameter Sweep

Measured with `python -m bench.sweep` (`bench/out/sweep_results.json`) over a seeded workload (`seed=2026`, `n=30` runs per point).

## Methodology

- **×15 time compression:** Simulated runs use `600` tokens at `300.0 tok/s` (`2.0 s` simulated), equivalent to a production-shaped `600`-token answer at `20.0 tok/s` (`30.0 s` real-equiv).
- **Client backoff distribution:** Uniform `[0.017s, 0.200s]` simulated (`[0.25s, 3.0s]` real-equiv), `HEARTBEAT_SECONDS = 0.02`.
- **In-memory stores by design:** The sweep runs against `InMemoryGenerationStore`, `InMemoryLeaseStore`, and `InMemorySubscriberRegistry` to isolate the deferred-cancellation policy (`DeferredCanceller`) from Redis I/O variance.
- **T = 0 routing:** Every row, including `T = 0.000`, routes through `DeferredCanceller` (zero grace window) rather than explicit `service.stop()`.
- **Probes:**
  - `wasted_token_ratio`: fraction of total tokens generated after a permanent subscriber abandon.
  - `reconnect_success`: fraction of temporary disconnects where a `Last-Event-ID` resume completes the full stream byte-for-byte (**I1/I5**).
  - `retry_attach_rate`: fraction of client retries after disconnect + backoff that attach (`created == False`) to the live generation (**I9**) instead of spawning a second upstream call (**I3**).
  - `coalesce_ctrl`: concurrent duplicate `start_or_attach` control probe (expected `1.0000` at all `T`).

## Observed Results (`n = 30` per point)

| `T` (sim) | `T` (real-equiv) | `wasted_token_ratio` (p10–p90) | `reconnect_success` (p10–p90) | `retry_attach_rate` (p10–p90) | `coalesce_ctrl` |
| --------: | ---------------: | -----------------------------: | ----------------------------: | ----------------------------: | --------------: |
| `0.000` | `0.0s` | `0.0017` (`0.0017–0.0017`) | `0.0000` (`0.0000–0.0000`) | `0.0000` (`0.0000–0.0000`) | `1.0000` |
| `0.033` | `0.5s` | `0.0067` (`0.0067–0.0067`) | `0.0333` (`0.0000–0.0000`) | `0.0333` (`0.0000–0.0000`) | `1.0000` |
| `0.067` | `1.0s` | `0.0083` (`0.0083–0.0083`) | `0.2667` (`0.0000–1.0000`) | `0.2667` (`0.0000–1.0000`) | `1.0000` |
| `0.133` | `2.0s` | `0.0167` (`0.0167–0.0167`) | `0.5667` (`0.0000–1.0000`) | `0.5667` (`0.0000–1.0000`) | `1.0000` |
| `0.333` | `5.0s` | `0.0424` (`0.0417–0.0433`) | `1.0000` (`1.0000–1.0000`) | `1.0000` (`1.0000–1.0000`) | `1.0000` |
| `0.667` | `10.0s` | `0.0750` (`0.0750–0.0750`) | `1.0000` (`1.0000–1.0000`) | `1.0000` (`1.0000–1.0000`) | `1.0000` |
| `1.333` | `20.0s` | `0.1517` (`0.1517–0.1517`) | `1.0000` (`1.0000–1.0000`) | `1.0000` (`1.0000–1.0000`) | `1.0000` |

To reproduce:

```bash
python -m bench.sweep
```

## Shipped Default & Rationale

- **Shipped default in `app/core/config.py`:** `DRAIN_TIMEOUT_SECONDS = 5.0` (configurable via `pydantic-settings`), chosen directly from the measured knee at `T = 5.0s` real-equiv (`0.333s` simulated). See [ADR-004](../adr/004-choosing-drain-timeout.md).
- **Why `T = 0` fails:** At `T = 0.000`, `reconnect_success` and `retry_attach_rate` both collapse to `0.0000` — cancelling immediately on disconnect destroys the live stream before a client retry can attach, breaking **I1**, **I5**, and **I9**.
- **Why `T = 5.0s` is the knee:** `T = 2.0s` real-equiv recovers only `56.67%` of reconnects and retries (`0.5667`), whereas `T = 5.0s` reaches `1.0000` on both metrics while keeping mean wasted tokens on an abandoned `30s` generation to `4.24%` (`0.0424`, **I8**).
