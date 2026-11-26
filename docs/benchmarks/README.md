# Deferred-Cancellation (`T`) Parameter Sweep

Measured with `python -m bench.sweep` (`bench/out/sweep_results.json`) over a seeded workload
(`seed=2026`, `tokens_per_second=200.0`, `TOTAL_TOKENS=40`, `HEARTBEAT_SECONDS=0.02`,
temporary-disconnect backoffs in `[0.03s, 0.35s]`, plus permanent-abandon and duplicate-coalesce probes).

## Observed Results

| `T` (s) | `wasted_token_ratio` | `reconnect_success` | `coalesce_hit` |
| ------: | -------------------: | ------------------: | -------------: |
| `0.00`  | `0.0000`             | `0.0000`            | `1.0000`       |
| `0.05`  | `0.0813`             | `0.0000`            | `1.0000`       |
| `0.10`  | `0.1688`             | `0.2500`            | `1.0000`       |
| `0.25`  | `0.4062`             | `0.8750`            | `1.0000`       |
| `0.50`  | `0.8000`             | `1.0000`            | `1.0000`       |
| `1.00`  | `0.9000`             | `1.0000`            | `1.0000`       |

## Shipped Default & Rationale

- **Shipped default in `app/core/config.py`:** `DRAIN_TIMEOUT_SECONDS = 5.0` (configurable via `pydantic-settings`).
- **Sweep knee (`[0.03s, 0.35s]` simulated backoff):** `T = 0.25s` recovers `87.5%` of temporary disconnects at `0.4062` wasted token ratio, while `T = 0.50s` reaches `100%` recovery at `0.8000` wasted token ratio.
- **Why `T = 0` is wrong:** At `T = 0.00`, `reconnect_success` drops to `0.0000` — cancelling immediately on disconnect destroys the live stream before a client can reattach via `Last-Event-ID`, breaking **I1**, **I5**, and **I9** on transient tab switches or mobile network drops.
- **Why `T` is bounded:** Setting `T` to exceed typical client reconnect backoff (e.g., `5.0s` in production for real-world network drops) preserves resumability (**I1/I2/I5**) and live-subscriber safety (**I7**) while bounding post-abandonment token waste to `(T + heartbeat_interval) × token_rate` (**I8**).
