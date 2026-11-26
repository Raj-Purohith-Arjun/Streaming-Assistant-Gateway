# ADR-003: Expiry-Scored Subscriber ZSET and Deferred Cancellation

## Status
Accepted

## Context
Naive cancel-on-disconnect breaks resumability (**I1/I2/I5**) and request coalescing (**I9**)
whenever a mobile client switches tabs or hits a transient drop. Conversely, never cancelling
abandoned streams wastes upstream tokens. Furthermore, an `INCR`/`DECR` subscriber counter drifts
permanently upward if a gateway replica crashes before running `on_unsubscribe`.

## Decision
1. **Expiry-scored ZSET (`gen:{gen_id}:subscribers`):**
   - `RedisSubscriberRegistry` stores each `sub_id` in a Redis `ZSET` scored by
     `redis_TIME + ttl_seconds`.
   - Every `register`, `unregister`, `heartbeat`, and `active_count` call runs a Lua script that
     first prunes expired members (`ZREMRANGEBYSCORE ... -inf now`) and refreshes `EXPIRE` on
     writes. Crashed subscribers age out automatically without leaking refcounts.
2. **Deferred cancellation window (`T > 0`):**
   - When `active_count` drops to `0` while `RUNNING`, `DeferredCanceller` transitions the
     generation to `DRAINING` and arms a timer for `T` seconds (`drain_timeout_seconds`).
   - If a subscriber reconnects before `T` elapses, `on_subscribe` cancels the drain timer and
     transitions `DRAINING -> RUNNING` (**I7**).
   - If `T` expires with `active_count == 0` still holding, it transitions `DRAINING -> EXPIRED`
     and marks `TerminalStatus.EXPIRED` (**I6**, **I8**).
   - Explicit `stop(gen_id)` bypasses `DRAINING` and marks `STOPPED` immediately.

## Consequences
- **Self-healing refcounts:** Replica crashes cannot permanently pin a generation open.
- **Cost:** Active subscribers send periodic heartbeats; abandoned streams run at most
  `T + heartbeat_interval` seconds (`T × token_rate` wasted tokens, **I8**).
