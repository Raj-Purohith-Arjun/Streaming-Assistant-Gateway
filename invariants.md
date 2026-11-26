# Invariants

These are the guarantees the gateway makes. Every one is testable, and every one has a test.
If a change cannot preserve an invariant, the invariant is weakened *deliberately and in writing*
— never silently.

Notation: a **generation** is one upstream model call, identified by `gen_id`. A **subscriber** is
one SSE connection reading that generation. `T` is the deferred-cancellation window in seconds.

## Delivery

| # | Invariant | Test |
| --- | --- | --- |
| **I1** | A subscriber resuming at offset `k` receives every token with index > `k`, in order, until a terminal event. | `test_resume_no_gaps` — property test over random disconnect points |
| **I2** | No token index is delivered twice to the same subscriber. | `test_resume_no_duplicates` — assert strictly increasing event ids |
| **I5** | Concatenating everything a client received across N reconnects equals the full generation byte-for-byte. | `test_reassembly_equals_reference` — the headline correctness test |

I5 subsumes I1 and I2 but is kept separate because it is the one a reader should run first.

## Execution

| # | Invariant | Test |
| --- | --- | --- |
| **I3** | At most one upstream call is in flight per `gen_id` at any instant. | `test_single_producer` — counter on the fake provider, asserted never > 1 |
| **I4** | After a terminal event, no further tokens are appended to the log. | `test_terminal_is_final` — stream length frozen post-terminal |
| **I9** | A duplicate request for a live `gen_id` causes zero additional upstream calls. | `test_coalesce_attaches` — provider call counter stays at 1 |

## Cancellation

| # | Invariant | Test |
| --- | --- | --- |
| **I6** | A generation with zero subscribers is cancelled within `T + heartbeat_interval`. | `test_drain_timer_fires` |
| **I7** | A generation with ≥1 live subscriber is never cancelled, except by explicit stop. | `test_no_premature_cancel` — under subscriber churn |
| **I8** | Tokens generated after the last subscriber left never exceed `T × token_rate`. | Measured as `wasted_token_ratio` in the benchmark |

I7 is the one that breaks first under refactoring. It is also the whole reason the project exists.

## State machine

```
PENDING ──► RUNNING ──► DRAINING ──┬──► EXPIRED     (drain timer fired)
               ▲           │       ├──► COMPLETED   (upstream finished)
               └───────────┘       ├──► STOPPED     (explicit user stop)
            new subscriber         └──► FAILED      (upstream or producer error)
```

- `DRAINING` means the subscriber count reached zero and the cancel timer is running.
- A new subscriber arriving during `DRAINING` cancels the timer and returns to `RUNNING`.
  **That single transition is the feature.**
- All terminal states are final: I4 applies from the moment one is reached.
- An explicit stop skips `DRAINING` entirely. Intent is known, so nothing is guessed.

## Why `T` is not zero

Cancelling the moment a client disconnects is the obvious design and it is wrong, because a
disconnect is indistinguishable from a tab switch, a backgrounded phone, or a flaky connection.
Cancel on it and:

- the client that reconnects 2 seconds later has nothing to reattach to (breaks I1, I2, I5), and
- a retry has no live execution to coalesce onto (breaks I9).

`T` must exceed the client's reconnect backoff. The gateway's job is to make `T` a parameter with
a measured cost on both sides, not a hardcoded guess.

## Known limitation: producer death

If the producing process dies mid-generation, the tokens already in the log survive and remain
readable, but **the generation cannot be resumed upstream.** No provider offers "continue from
token 412 with identical sampling state." A successor replica can acquire the lease; it cannot
continue the same generation.

Current behaviour: the generation moves to `FAILED` with its partial output intact, and the client
decides whether to restart.

Documented alternative, not implemented: start a *new* generation seeded with the partial text as a
prefix, emitting an explicit `discontinuity` event so the client knows the join is not seamless.

Silently re-prompting and presenting the result as the same stream would violate I5 and is not an
option under any circumstances.
