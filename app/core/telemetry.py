from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import metrics, trace
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import Span, TracerProvider


class GatewayTelemetry:
  """OpenTelemetry counters and spans for gateway request coalescing and cancellation."""

  def __init__(
    self,
    meter_provider: MeterProvider | None = None,
    tracer_provider: TracerProvider | None = None,
  ) -> None:
    meter = (
      meter_provider.get_meter("streaming-assistant-gateway")
      if meter_provider is not None
      else metrics.get_meter("streaming-assistant-gateway")
    )
    tracer = (
      tracer_provider.get_tracer("streaming-assistant-gateway")
      if tracer_provider is not None
      else trace.get_tracer("streaming-assistant-gateway")
    )
    self._tracer = tracer
    self._requests = meter.create_counter(
      "gateway.requests",
      description="Total gateway start_or_attach requests",
    )
    self._coalesce_hits = meter.create_counter(
      "gateway.coalesce_hits",
      description="Requests coalesced onto an existing live generation",
    )
    self._drain_revivals = meter.create_counter(
      "gateway.drain_revivals",
      description="Generations revived from DRAINING back to RUNNING",
    )
    self._drain_expirations = meter.create_counter(
      "gateway.drain_expirations",
      description="Generations expired after drain timeout",
    )
    self._explicit_stops = meter.create_counter(
      "gateway.explicit_stops",
      description="Explicit stop calls",
    )

  @contextmanager
  def start_or_attach_span(self) -> Iterator[Span]:
    """Start a ``gateway.start_or_attach`` trace span."""
    with self._tracer.start_as_current_span("gateway.start_or_attach") as span:
      yield span

  def record_request(self, *, coalesced: bool) -> None:
    """Increment request counter and coalesce hit counter when ``coalesced`` is True."""
    self._requests.add(1)
    if coalesced:
      self._coalesce_hits.add(1)

  def record_drain_revival(self) -> None:
    """Increment the drain revival counter."""
    self._drain_revivals.add(1)

  def record_drain_expiration(self) -> None:
    """Increment the drain expiration counter."""
    self._drain_expirations.add(1)

  def record_explicit_stop(self) -> None:
    """Increment the explicit stop counter."""
    self._explicit_stops.add(1)
