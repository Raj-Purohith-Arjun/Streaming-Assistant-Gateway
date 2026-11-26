import asyncio
import contextlib
import json
import random
import uuid
from collections.abc import AsyncIterator
from dataclasses import asdict, dataclass
from pathlib import Path

from app.cancel import DeferredCanceller
from app.gateway import GatewayService
from app.generation import GenerationRegistry
from app.producer import ProducerConfig
from app.sse import SseEncoder
from app.store.subscribers import InMemorySubscriberRegistry
from app.upstream import SyntheticAdapter
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore

T_VALUES: tuple[float, ...] = (0.0, 0.05, 0.1, 0.25, 0.5, 1.0)
TOKEN_RATE = 200.0
HEARTBEAT_SECONDS = 0.02
TOTAL_TOKENS = 40
TEMP_DISCONNECT_RUNS = 8
ABANDON_RUNS = 4
COALESCE_DUPLICATES = 5


@dataclass(frozen=True, slots=True)
class SweepPoint:
  """Measured metrics for a single drain-timeout value ``T``."""

  T: float
  wasted_token_ratio: float
  reconnect_success_rate: float
  coalesce_hit_rate: float


def _parse_sse_frame(frame: str) -> tuple[str | None, str, dict[str, object]]:
  event_id: str | None = None
  event_type = ""
  data: dict[str, object] = {}
  for line in frame.splitlines():
    if line.startswith("id: "):
      event_id = line[4:]
    elif line.startswith("event: "):
      event_type = line[7:]
    elif line.startswith("data: "):
      data = json.loads(line[6:])
  return event_id, event_type, data


def _build_service(drain_t: float, tokens: list[str]) -> tuple[GatewayService, InMemoryGenerationStore]:
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  effective_t = drain_t if drain_t > 0 else 1e-6
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=effective_t,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=lease_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=SyntheticAdapter(tokens=tokens, tokens_per_second=TOKEN_RATE),
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=HEARTBEAT_SECONDS),
  )
  return service, gen_store


async def _run_temporary_disconnect(drain_t: float, backoff: float, tokens: list[str]) -> bool:
  service, _ = _build_service(drain_t, tokens)
  gen_id, _ = await service.start_or_attach(f"tmp-{uuid.uuid4()}", "bench")

  received: list[str] = []
  last_id: str | None = None
  async for frame in service.stream_sse(gen_id):
    if frame.startswith(":"):
      continue
    ev_id, ev_type, data = _parse_sse_frame(frame)
    if ev_type == "token":
      received.append(str(data["text"]))
      last_id = ev_id
      if len(received) >= 4:
        break

  if drain_t == 0.0:
    # Naive cancel-on-disconnect: cancel immediately when subscriber leaves.
    await service.stop(gen_id)

  await asyncio.sleep(backoff)

  saw_done = False
  async for frame in service.stream_sse(gen_id, last_event_id=last_id):
    if frame.startswith(":"):
      continue
    _, ev_type, data = _parse_sse_frame(frame)
    if ev_type == "token":
      received.append(str(data["text"]))
    elif ev_type == "done":
      saw_done = True

  return saw_done and "".join(received) == "".join(tokens)


async def _run_permanent_abandon(drain_t: float, tokens: list[str]) -> tuple[int, int]:
  service, gen_store = _build_service(drain_t, tokens)
  gen_id, _ = await service.start_or_attach(f"abn-{uuid.uuid4()}", "bench")

  received = 0
  async for frame in service.stream_sse(gen_id):
    if frame.startswith(":"):
      continue
    _, ev_type, _ = _parse_sse_frame(frame)
    if ev_type == "token":
      received += 1
      if received >= 4:
        break

  if drain_t == 0.0:
    await service.stop(gen_id)
  else:
    await asyncio.sleep(drain_t + HEARTBEAT_SECONDS + 0.03)

  total_generated = len([e async for e in gen_store.read_from(gen_id)])
  wasted = max(0, total_generated - received)
  return wasted, len(tokens)


async def _run_coalesce_probe(drain_t: float, tokens: list[str]) -> tuple[int, int]:
  upstream_calls = 0

  class _CountingAdapter:
    async def stream(self, prompt: str, *, model: str | None = None) -> AsyncIterator[str]:
      nonlocal upstream_calls
      _ = (prompt, model)
      upstream_calls += 1
      for tok in tokens[:5]:
        await asyncio.sleep(1.0 / TOKEN_RATE)
        yield tok

  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=drain_t if drain_t > 0 else 1e-6,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=lease_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=_CountingAdapter(),
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=HEARTBEAT_SECONDS),
  )

  req_key = f"coal-{uuid.uuid4()}"
  results = await asyncio.gather(
    *(service.start_or_attach(req_key, "bench") for _ in range(COALESCE_DUPLICATES))
  )
  coalesced_hits = sum(1 for _, created in results if not created)
  with contextlib.suppress(Exception):
    await asyncio.sleep(0.05)
  return coalesced_hits, COALESCE_DUPLICATES - 1


async def run_sweep(seed: int = 2026) -> list[SweepPoint]:
  """Run the parameter sweep across ``T_VALUES`` and return ``SweepPoint`` results."""
  rng = random.Random(seed)
  tokens = [f"t{i}-" for i in range(TOTAL_TOKENS)]
  backoffs = [round(rng.uniform(0.03, 0.35), 3) for _ in range(TEMP_DISCONNECT_RUNS)]

  points: list[SweepPoint] = []
  for drain_t in T_VALUES:
    reconnect_ok = 0
    for backoff in backoffs:
      if await _run_temporary_disconnect(drain_t, backoff, tokens):
        reconnect_ok += 1

    total_wasted = 0
    total_possible = 0
    for _ in range(ABANDON_RUNS):
      wasted, possible = await _run_permanent_abandon(drain_t, tokens)
      total_wasted += wasted
      total_possible += possible

    hits, expected_hits = await _run_coalesce_probe(drain_t, tokens)

    points.append(
      SweepPoint(
        T=drain_t,
        wasted_token_ratio=round(total_wasted / total_possible, 4) if total_possible else 0.0,
        reconnect_success_rate=round(reconnect_ok / TEMP_DISCONNECT_RUNS, 4),
        coalesce_hit_rate=round(hits / expected_hits, 4) if expected_hits else 1.0,
      )
    )
  return points


def main() -> None:
  """Execute the sweep, write ``bench/out/sweep_results.json``, and print a summary table."""
  points = asyncio.run(run_sweep())
  out_path = Path("bench/out/sweep_results.json")
  out_path.parent.mkdir(parents=True, exist_ok=True)
  out_path.write_text(json.dumps([asdict(p) for p in points], indent=2) + "\n", encoding="utf-8")

  print(f"{'T (s)':>8} | {'wasted_token_ratio':>18} | {'reconnect_success':>17} | {'coalesce_hit':>12}")
  print("-" * 64)
  for p in points:
    print(
      f"{p.T:>8.2f} | {p.wasted_token_ratio:>18.4f} | "
      f"{p.reconnect_success_rate:>17.4f} | {p.coalesce_hit_rate:>12.4f}"
    )


if __name__ == "__main__":
  main()
