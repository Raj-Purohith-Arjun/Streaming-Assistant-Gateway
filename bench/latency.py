import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import asdict, dataclass
from pathlib import Path

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.cancel import DeferredCanceller
from app.core.config import get_settings
from app.gateway import GatewayService
from app.generation import RedisGenerationRegistry
from app.producer import ProducerConfig
from app.sse import SseEncoder
from app.store.redis_store import RedisGenerationStore
from app.store.subscribers import RedisSubscriberRegistry

CONCURRENCY_LEVELS: tuple[int, ...] = (1, 10, 50)
TOKENS_PER_STREAM = 50
MIN_TTFT_SAMPLES = 10


class _UnthrottledAdapter:
  """Minimal in-process adapter yielding tokens with ``asyncio.sleep(0)`` (no timer sleep)."""

  def __init__(self, tokens: list[str]) -> None:
    self._tokens = tokens

  async def stream(self, prompt: str, *, model: str | None = None) -> AsyncIterator[str]:
    _ = (prompt, model)
    for token in self._tokens:
      await asyncio.sleep(0)
      yield token


@dataclass(frozen=True, slots=True)
class LatencyStats:
  """Percentile summary (in milliseconds) for a latency sample series."""

  n: int
  p50_ms: float
  p99_ms: float


@dataclass(frozen=True, slots=True)
class ConcurrencyResult:
  """Direct vs gateway latency results at a single concurrency level."""

  concurrency: int
  rounds: int
  direct_ttft: LatencyStats
  gateway_ttft: LatencyStats
  direct_inter_token: LatencyStats
  gateway_inter_token: LatencyStats
  added_per_token_p50_ms: float
  added_per_token_p99_ms: float


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


def _summarize(values_ms: list[float]) -> LatencyStats:
  if not values_ms:
    return LatencyStats(n=0, p50_ms=0.0, p99_ms=0.0)
  sorted_vals = sorted(values_ms)
  return LatencyStats(
    n=len(sorted_vals),
    p50_ms=round(_percentile(sorted_vals, 50.0), 3),
    p99_ms=round(_percentile(sorted_vals, 99.0), 3),
  )


def _resolve_redis_url() -> str:
  settings = get_settings()
  if settings.redis_url == "redis://redis:6379/0":
    return "redis://localhost:6379/0"
  return settings.redis_url


async def _connect_redis(redis_url: str) -> Redis:
  client: Redis = Redis.from_url(redis_url, max_connections=200, decode_responses=True)
  try:
    await client.ping()
  except (RedisError, OSError) as exc:
    await client.aclose()
    raise SystemExit(f"Redis unreachable at {redis_url}: {exc}") from exc
  return client


async def _measure_direct_stream(
  adapter: _UnthrottledAdapter,
) -> tuple[float, list[float]]:
  t0 = time.perf_counter()
  prev: float | None = None
  ttft_ms = 0.0
  inter_ms: list[float] = []

  async for _ in adapter.stream("bench"):
    now = time.perf_counter()
    if prev is None:
      ttft_ms = (now - t0) * 1000.0
    else:
      inter_ms.append((now - prev) * 1000.0)
    prev = now

  return ttft_ms, inter_ms


def _is_token_or_done(frame: str) -> str:
  for line in frame.splitlines():
    if line.startswith("event: "):
      return line[7:]
  return ""


async def _close_stream(stream: AsyncIterator[str]) -> None:
  aclose = getattr(stream, "aclose", None)
  if aclose is not None:
    await aclose()


async def _measure_gateway_stream(
  service: GatewayService,
) -> tuple[float, list[float]]:
  t0 = time.perf_counter()
  gen_id, _ = await service.start_or_attach(f"lat-{uuid.uuid4()}", "bench")
  prev: float | None = None
  ttft_ms = 0.0
  inter_ms: list[float] = []

  stream = service.stream_sse(gen_id)
  try:
    async for frame in stream:
      if frame.startswith(":"):
        continue
      ev_type = _is_token_or_done(frame)
      if ev_type == "token":
        now = time.perf_counter()
        if prev is None:
          ttft_ms = (now - t0) * 1000.0
        else:
          inter_ms.append((now - prev) * 1000.0)
        prev = now
      elif ev_type == "done":
        break
  finally:
    await _close_stream(stream)

  return ttft_ms, inter_ms


async def _run_concurrency_level(
  redis: Redis,
  concurrency: int,
  tokens: list[str],
) -> ConcurrencyResult:
  rounds = max(1, (MIN_TTFT_SAMPLES + concurrency - 1) // concurrency)
  adapter = _UnthrottledAdapter(tokens=tokens)

  gen_store = RedisGenerationStore(redis, ttl_seconds=300)
  subscribers = RedisSubscriberRegistry(redis, ttl_seconds=30.0)
  registry = RedisGenerationRegistry(redis, ttl_seconds=300)
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=5.0,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=gen_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=adapter,
    sse_encoder=SseEncoder(heartbeat_interval_seconds=15.0),
    producer_config=ProducerConfig(lease_ttl_ms=5000, heartbeat_interval_seconds=1.0),
  )

  direct_ttfts: list[float] = []
  direct_inters: list[float] = []
  gw_ttfts: list[float] = []
  gw_inters: list[float] = []

  try:
    for _ in range(rounds):
      direct_batch = await asyncio.gather(
        *(_measure_direct_stream(adapter) for _ in range(concurrency))
      )
      for ttft, inter in direct_batch:
        direct_ttfts.append(ttft)
        direct_inters.extend(inter)

      gw_batch = await asyncio.gather(
        *(_measure_gateway_stream(service) for _ in range(concurrency))
      )
      for ttft, inter in gw_batch:
        gw_ttfts.append(ttft)
        gw_inters.extend(inter)
  finally:
    await canceller.aclose()

  d_ttft = _summarize(direct_ttfts)
  g_ttft = _summarize(gw_ttfts)
  d_inter = _summarize(direct_inters)
  g_inter = _summarize(gw_inters)

  return ConcurrencyResult(
    concurrency=concurrency,
    rounds=rounds,
    direct_ttft=d_ttft,
    gateway_ttft=g_ttft,
    direct_inter_token=d_inter,
    gateway_inter_token=g_inter,
    added_per_token_p50_ms=round(g_inter.p50_ms - d_inter.p50_ms, 3),
    added_per_token_p99_ms=round(g_inter.p99_ms - d_inter.p99_ms, 3),
  )


async def run_latency_benchmark() -> tuple[str, list[ConcurrencyResult]]:
  """Run the Redis-backed latency benchmark across ``CONCURRENCY_LEVELS``."""
  redis_url = _resolve_redis_url()
  redis = await _connect_redis(redis_url)
  tokens = [f"t{i}-" for i in range(TOKENS_PER_STREAM)]

  try:
    # Warm up Lua scripts and connection pool before timed measurements.
    await _run_concurrency_level(redis, 1, tokens[:5])
    results = [
      await _run_concurrency_level(redis, level, tokens) for level in CONCURRENCY_LEVELS
    ]
  finally:
    await redis.aclose()

  return redis_url, results


def main() -> None:
  """Execute the latency benchmark, write ``bench/out/latency_results.json``, and print table."""
  redis_url, results = asyncio.run(run_latency_benchmark())
  out_path = Path("bench/out/latency_results.json")
  out_path.parent.mkdir(parents=True, exist_ok=True)
  payload = {
    "redis_url": redis_url,
    "tokens_per_stream": TOKENS_PER_STREAM,
    "concurrency_levels": list(CONCURRENCY_LEVELS),
    "results": [asdict(r) for r in results],
  }
  out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

  header = (
    f"{'conc':>4} | {'direct TTFT (p50/p99)':>21} | {'gw TTFT (p50/p99)':>19} | "
    f"{'direct inter (p50/p99)':>22} | {'gw inter (p50/p99)':>20} | "
    f"{'added/tok (p50/p99)':>19}"
  )
  print(header)
  print("-" * len(header))
  for r in results:
    print(
      f"{r.concurrency:>4d} | "
      f"{r.direct_ttft.p50_ms:>8.3f}/{r.direct_ttft.p99_ms:<8.3f} ms | "
      f"{r.gateway_ttft.p50_ms:>7.3f}/{r.gateway_ttft.p99_ms:<7.3f} ms | "
      f"{r.direct_inter_token.p50_ms:>8.3f}/{r.direct_inter_token.p99_ms:<9.3f} ms | "
      f"{r.gateway_inter_token.p50_ms:>7.3f}/{r.gateway_inter_token.p99_ms:<8.3f} ms | "
      f"{r.added_per_token_p50_ms:>7.3f}/{r.added_per_token_p99_ms:<7.3f} ms"
    )


if __name__ == "__main__":
  main()
