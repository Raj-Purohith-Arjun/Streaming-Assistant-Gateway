import asyncio

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Sum
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from app.cancel import DeferredCanceller
from app.core.telemetry import GatewayTelemetry
from app.gateway import GatewayService
from app.generation import GenerationRegistry, GenerationStatus
from app.producer import ProducerConfig
from app.sse import SseEncoder
from app.store.subscribers import InMemorySubscriberRegistry
from app.upstream import SyntheticAdapter
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore


def _build_telemetry() -> tuple[GatewayTelemetry, InMemoryMetricReader, InMemorySpanExporter]:
  reader = InMemoryMetricReader()
  meter_provider = MeterProvider(metric_readers=[reader])
  exporter = InMemorySpanExporter()
  tracer_provider = TracerProvider()
  tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
  telemetry = GatewayTelemetry(
    meter_provider=meter_provider,
    tracer_provider=tracer_provider,
  )
  return telemetry, reader, exporter


def _get_counter_value(reader: InMemoryMetricReader, metric_name: str) -> int:
  data = reader.get_metrics_data()
  if data is None:
    return 0
  total = 0
  for resource_metrics in data.resource_metrics:
    for scope_metrics in resource_metrics.scope_metrics:
      for metric in scope_metrics.metrics:
        if metric.name == metric_name and isinstance(metric.data, Sum):
          for point in metric.data.data_points:
            total += int(point.value)
  return total


async def test_telemetry_records_coalesce_hit_counter_and_span() -> None:
  telemetry, reader, exporter = _build_telemetry()
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.5,
    telemetry=telemetry,
  )
  service = GatewayService(
    gen_store=gen_store,
    lease_store=lease_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=SyntheticAdapter(tokens=["a", "b"], tokens_per_second=100.0),
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.02),
    telemetry=telemetry,
  )

  try:
    gen1, created1 = await service.start_or_attach("req-1", "hello")
    gen2, created2 = await service.start_or_attach("req-1", "hello")
    assert created1 is True
    assert created2 is False
    assert gen1 == gen2

    assert _get_counter_value(reader, "gateway.requests") == 2
    assert _get_counter_value(reader, "gateway.coalesce_hits") == 1

    spans = [s for s in exporter.get_finished_spans() if s.name == "gateway.start_or_attach"]
    assert len(spans) == 2
    assert spans[0].attributes is not None
    assert spans[1].attributes is not None
    assert spans[0].attributes.get("gen_id") == gen1
    assert spans[0].attributes.get("coalesced") is False
    assert spans[1].attributes.get("gen_id") == gen1
    assert spans[1].attributes.get("coalesced") is True
  finally:
    await canceller.aclose()


async def test_telemetry_records_drain_revival_and_expiration() -> None:
  telemetry, reader, _ = _build_telemetry()
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.05,
    telemetry=telemetry,
  )

  try:
    gen_revive, _ = await registry.get_or_create("revive")
    await registry.transition(gen_revive, GenerationStatus.RUNNING)
    await canceller.on_subscribe(gen_revive, "sub-1")
    await canceller.on_unsubscribe(gen_revive, "sub-1")
    await canceller.on_subscribe(gen_revive, "sub-2")
    assert _get_counter_value(reader, "gateway.drain_revivals") == 1

    gen_expire, _ = await registry.get_or_create("expire")
    await registry.transition(gen_expire, GenerationStatus.RUNNING)
    await canceller.on_subscribe(gen_expire, "sub-1")
    await canceller.on_unsubscribe(gen_expire, "sub-1")
    await asyncio.sleep(0.1)
    assert _get_counter_value(reader, "gateway.drain_expirations") == 1
  finally:
    await canceller.aclose()


async def test_telemetry_records_explicit_stop() -> None:
  telemetry, reader, _ = _build_telemetry()
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=0.5,
    telemetry=telemetry,
  )

  try:
    gen_stop, _ = await registry.get_or_create("stop")
    await registry.transition(gen_stop, GenerationStatus.RUNNING)
    await canceller.stop(gen_stop)
    assert _get_counter_value(reader, "gateway.explicit_stops") == 1
  finally:
    await canceller.aclose()
