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

COMPRESSION_FACTOR = 15
T_VALUES: tuple[float, ...] = (0.0, 0.033, 0.067, 0.133, 0.333, 0.667, 1.333)
REAL_EQUIV_T: dict[float, float] = {
  0.0: 0.0,
  0.033: 0.5,
  0.067: 1.0,
  0.133: 2.0,
  0.333: 5.0,
  0.667: 10.0,
  1.333: 20.0,
}
TOKEN_RATE = 300.0
HEARTBEAT_SECONDS = 0.02
TOTAL_TOKENS = 600
RUNS_PER_POINT = 30
RETRY_READ_TOKENS = 4
BACKOFF_MIN_SECONDS = 0.017
BACKOFF_MAX_SECONDS = 0.2
COALESCE_DUPLICATES = 5


@dataclass(frozen=True, slots=True)
class MetricSummary:
  """Summary statistics (n, mean, p10, p90) for a single metric at one ``T`` point."""

  n: int
  mean: float
  p10: float
  p90: float


@dataclass(frozen=True, slots=True)
class SweepPoint:
  """Measured metrics for a single drain-timeout value ``T``."""

  T: float
  T_real_equiv: float
  wasted_token_ratio: MetricSummary
  reconnect_success_rate: MetricSummary
  retry_attach_rate: MetricSummary
  concurrent_coalesce_rate: MetricSummary


def _percentile(sorted_vals: list[float], pct: float) -> float:
  if not sorted_vals:
    return 0.0
  if len(sorted_vals) == 1:
    return sorted_vals[0]
  idx = (len(sorted_vals) - 1) * (pct / 100.0)
  lo = int(idx)
  hi = min(lo + 1, len(sorted_vals) - 1)
  frac = idx - lo
  return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def _summarize(values: list[float]) -> MetricSummary:
  if not values:
    return MetricSummary(n=0, mean=0.0, p10=0.0, p90=0.0)
  sorted_vals = sorted(values)
  mean = sum(sorted_vals) / len(sorted_vals)
  return MetricSummary(
    n=len(sorted_vals),
    mean=round(mean, 4),
    p10=round(_percentile(sorted_vals, 10.0), 4),
    p90=round(_percentile(sorted_vals, 90.0), 4),
  )


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


def _build_service(
  drain_t: float,
  tokens: list[str],
) -> tuple[GatewayService, InMemoryGenerationStore, DeferredCanceller]:
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
  return service, gen_store, canceller


async def _close_stream(stream: AsyncIterator[str]) -> None:
  aclose = getattr(stream, "aclose", None)
  if aclose is not None:
    await aclose()


async def _run_temporary_disconnect(drain_t: float, backoff: float, tokens: list[str]) -> float:
  service, _, canceller = _build_service(drain_t, tokens)
  gen_id, _ = await service.start_or_attach(f"tmp-{uuid.uuid4()}", "bench")

  received: list[str] = []
  last_id: str | None = None
  stream = service.stream_sse(gen_id)
  try:
    async for frame in stream:
      if frame.startswith(":"):
        continue
      ev_id, ev_type, data = _parse_sse_frame(frame)
      if ev_type == "token":
        received.append(str(data["text"]))
        last_id = ev_id
        if len(received) >= RETRY_READ_TOKENS:
          break
  finally:
    await _close_stream(stream)

  await asyncio.sleep(backoff)

  saw_done = False
  resume_stream = service.stream_sse(gen_id, last_event_id=last_id)
  try:
    async for frame in resume_stream:
      if frame.startswith(":"):
        continue
      _, ev_type, data = _parse_sse_frame(frame)
      if ev_type == "token":
        received.append(str(data["text"]))
      elif ev_type == "done":
        saw_done = True
  finally:
    await _close_stream(resume_stream)

  await canceller.aclose()
  return 1.0 if (saw_done and "".join(received) == "".join(tokens)) else 0.0


async def _run_retry_attach(drain_t: float, backoff: float, tokens: list[str]) -> float:
  service, _, canceller = _build_service(drain_t, tokens)
  req_key = f"retry-{uuid.uuid4()}"
  gen_id, _ = await service.start_or_attach(req_key, "bench")

  received = 0
  stream = service.stream_sse(gen_id)
  try:
    async for frame in stream:
      if frame.startswith(":"):
        continue
      _, ev_type, _ = _parse_sse_frame(frame)
      if ev_type == "token":
        received += 1
        if received >= RETRY_READ_TOKENS:
          break
  finally:
    await _close_stream(stream)

  await asyncio.sleep(backoff)

  _, created = await service.start_or_attach(req_key, "bench")
  await canceller.aclose()
  with contextlib.suppress(Exception):
    await service.stop(gen_id)
  return 1.0 if not created else 0.0


async def _run_permanent_abandon(drain_t: float, tokens: list[str]) -> float:
  service, gen_store, canceller = _build_service(drain_t, tokens)
  gen_id, _ = await service.start_or_attach(f"abn-{uuid.uuid4()}", "bench")

  received = 0
  stream = service.stream_sse(gen_id)
  try:
    async for frame in stream:
      if frame.startswith(":"):
        continue
      _, ev_type, _ = _parse_sse_frame(frame)
      if ev_type == "token":
        received += 1
        if received >= RETRY_READ_TOKENS:
          break
  finally:
    await _close_stream(stream)

  await asyncio.sleep(drain_t + HEARTBEAT_SECONDS + 0.03)

  total_generated = len([e async for e in gen_store.read_from(gen_id)])
  await canceller.aclose()
  with contextlib.suppress(Exception):
    await service.stop(gen_id)
  wasted = max(0, total_generated - received)
  return wasted / len(tokens)


async def _run_concurrent_coalesce(drain_t: float, tokens: list[str]) -> float:
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
  await canceller.aclose()
  expected_hits = COALESCE_DUPLICATES - 1
  return coalesced_hits / expected_hits if expected_hits else 1.0


async def run_sweep(seed: int = 2026) -> list[SweepPoint]:
  """Run the parameter sweep across ``T_VALUES`` and return ``SweepPoint`` results."""
  rng = random.Random(seed)
  tokens = [f"t{i}-" for i in range(TOTAL_TOKENS)]
  backoffs = [
    round(rng.uniform(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS), 4)
    for _ in range(RUNS_PER_POINT)
  ]

  points: list[SweepPoint] = []
  for drain_t in T_VALUES:
    reconnect_samples = list(
      await asyncio.gather(*(_run_temporary_disconnect(drain_t, b, tokens) for b in backoffs))
    )
    retry_samples = [await _run_retry_attach(drain_t, b, tokens) for b in backoffs]
    wasted_samples = list(
      await asyncio.gather(*(_run_permanent_abandon(drain_t, tokens) for _ in range(RUNS_PER_POINT)))
    )
    coalesce_samples = list(
      await asyncio.gather(
        *(_run_concurrent_coalesce(drain_t, tokens) for _ in range(RUNS_PER_POINT))
      )
    )

    points.append(
      SweepPoint(
        T=drain_t,
        T_real_equiv=REAL_EQUIV_T[drain_t],
        wasted_token_ratio=_summarize(wasted_samples),
        reconnect_success_rate=_summarize(reconnect_samples),
        retry_attach_rate=_summarize(retry_samples),
        concurrent_coalesce_rate=_summarize(coalesce_samples),
      )
    )
  return points


def main() -> None:
  """Execute the sweep, write ``bench/out/sweep_results.json``, and print a summary table."""
  points = asyncio.run(run_sweep())
  out_path = Path("bench/out/sweep_results.json")
  out_path.parent.mkdir(parents=True, exist_ok=True)
  payload = {
    "compression_factor": COMPRESSION_FACTOR,
    "total_tokens": TOTAL_TOKENS,
    "token_rate_sim": TOKEN_RATE,
    "runs_per_point": RUNS_PER_POINT,
    "backoff_sim_range": [BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS],
    "points": [asdict(p) for p in points],
  }
  out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

  header = (
    f"{'T (sim)':>8} | {'T (real)':>8} | {'wasted (p10-p90)':>23} | "
    f"{'reconnect (p10-p90)':>23} | {'retry_attach (p10-p90)':>23} | "
    f"{'coalesce_ctrl':>13}"
  )
  print(header)
  print("-" * len(header))
  for p in points:
    w = p.wasted_token_ratio
    r = p.reconnect_success_rate
    a = p.retry_attach_rate
    c = p.concurrent_coalesce_rate
    print(
      f"{p.T:>8.3f} | {p.T_real_equiv:>7.1f}s | "
      f"{w.mean:>6.4f} ({w.p10:.2f}-{w.p90:.2f}) | "
      f"{r.mean:>6.4f} ({r.p10:.2f}-{r.p90:.2f}) | "
      f"{a.mean:>6.4f} ({a.p10:.2f}-{a.p90:.2f}) | "
      f"{c.mean:>13.4f}"
    )


if __name__ == "__main__":
  main()
