# ADR-004: Choosing the Default Drain Timeout (`DRAIN_TIMEOUT_SECONDS`)

## Status
Accepted

## Context
`DeferredCanceller` (ADR-003) keeps an abandoned generation alive in `DRAINING` for `T` seconds (`DRAIN_TIMEOUT_SECONDS` in `app/core/config.py`) before transitioning to `EXPIRED`. Choosing `T` too small causes transient client disconnects and retries to miss the live stream (**I1**, **I5**, **I9**); choosing `T` too large increases post-abandonment token waste (**I8**). We need a default grounded in the measured parameter sweep (`docs/benchmarks/README.md`, `bench/out/sweep_results.json`) rather than intuition.

## Decision
Set the default `DRAIN_TIMEOUT_SECONDS = 5.0` based directly on the measured knee at `T = 5.0s` real-equiv (`0.333s` simulated under ×15 compression, `n = 30` runs per point):

- **Measured knee (`T = 5.0s` real-equiv):** Both `retry_attach_rate` and `reconnect_success` reach `1.0000` (`p10–p90: 1.0000–1.0000`), whereas stepping down to `T = 2.0s` (`0.133s` simulated) drops both to `0.5667` (`p10–p90: 0.0000–1.0000`).
- **Accepted wasted-token cost:** At `T = 5.0s`, `wasted_token_ratio` is `0.0424` mean (`p10–p90: 0.0417–0.0433`) on a `30s` (`600`-token @ `20 tok/s`) generation — i.e., `4.24%` accepted waste on permanent abandons, versus `7.50%` at `T = 10.0s` and `15.17%` at `T = 20.0s`.
- **Client backoff assumption:** Assumes transient client reconnect/retry backoffs are bounded by uniform `0.25–3.0s` real-equiv (`0.017–0.200s` simulated), leaving a `2.0s` headroom above the `3.0s` upper bound for transport and heartbeat jitter.

## Consequences
- **Resumability and coalescing preserved:** Transient drops within the `0.25–3.0s` backoff window achieve `100%` `Last-Event-ID` resume and retry attachment without spawning duplicate upstream calls (**I3**, **I9**).
- **Bounded waste:** Post-abandonment upstream generation is capped at `4.24%` mean on `30s` generations (**I8**). Deployments with longer client retry backoffs can override `DRAIN_TIMEOUT_SECONDS` via environment configuration (`pydantic-settings`).
