# ADR-002: Single-Producer Lease Fencing and GC-Pause Safety

## Status
Accepted

## Context
To guarantee at most one upstream LLM call per generation (**I3**) and prevent a stalled replica
from corrupting the token log after a long GC pause or network partition, a simple `SET NX PX`
lock is insufficient: if Replica A pauses past the lease TTL, Replica B acquires the lease, and
Replica A wakes up and writes a token, a naive lock check cannot stop Replica A's in-flight write.

## Decision
We pair an expiring active-lease key (`gen:{gen_id}:lease`, `PX` TTL) with a monotonic fencing
counter (`gen:{gen_id}:fence`, `INCR` + `EXPIRE`):

1. `ACQUIRE_LEASE_LUA` succeeds only if `gen:{gen_id}:lease` does not exist, increments
   `gen:{gen_id}:fence`, stores `owner_id:fence` with `PX` TTL, and returns `Lease(fence)`.
2. `APPEND_TOKEN_LUA` and `MARK_TERMINAL_LUA` both accept the producer's `fence` and atomically
   verify **both** `EXISTS gen:{gen_id}:lease == 1` **and** `GET gen:{gen_id}:fence == fence`
   before mutating the stream or terminal key. Any mismatch or expired lease raises
   `StaleLeaseError`.
3. `Producer` runs a background heartbeat task renewing the lease via `RENEW_LEASE_LUA` and
   polling `read_terminal(gen_id)`. On `StaleLeaseError` or external terminal state, `Producer`
   aborts the upstream iterator immediately without calling `mark_terminal`.

## Known Limitation: Producer Death
If the producing replica dies mid-generation, a successor replica can acquire the lease, but no
LLM provider supports resuming sampling state from token `k`. The generation moves to `FAILED`
with its partial log intact rather than silently re-prompting (which would violate **I5**).

## Consequences
- **Safety:** Eliminates split-brain writes from GC-paused or expired producers.
- **Cost:** One background renewal + terminal check per `heartbeat_interval_seconds` (1.5s default),
  bounding post-cancellation waste to `T + heartbeat_interval` (**I6**, **I8**).
