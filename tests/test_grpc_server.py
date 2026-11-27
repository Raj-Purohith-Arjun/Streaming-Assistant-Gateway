from typing import Any
from unittest.mock import MagicMock

import pytest

from app.cancel import DeferredCanceller
from app.gateway.service import GatewayService
from app.generation import GenerationRegistry
from app.grpc_server.server import StreamingAssistantServicer
from app.producer import ProducerConfig
from app.sse import SseEncoder
from app.store.subscribers import InMemorySubscriberRegistry
from app.upstream import SyntheticAdapter
from tests.fakes import InMemoryGenerationStore, InMemoryLeaseStore

try:
  from app.grpc_server.generated import assistant_pb2

  HAS_GRPC = True
except ImportError:
  HAS_GRPC = False
  assistant_pb2 = None  # type: ignore[assignment]

pytestmark = pytest.mark.skipif(not HAS_GRPC, reason="gRPC stubs not generated")


def _build_test_service(
  *,
  tokens: list[str],
  tokens_per_second: float = 500.0,
  drain_timeout_seconds: float = 0.2,
) -> GatewayService:
  lease_store = InMemoryLeaseStore()
  gen_store = InMemoryGenerationStore(lease_store=lease_store)
  subscribers = InMemorySubscriberRegistry(ttl_seconds=5.0)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=gen_store,
    drain_timeout_seconds=drain_timeout_seconds,
  )
  upstream = SyntheticAdapter(
    tokens=tokens,
    tokens_per_second=tokens_per_second,
  )
  return GatewayService(
    gen_store=gen_store,
    lease_store=lease_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=upstream,
    sse_encoder=SseEncoder(heartbeat_interval_seconds=1.0),
    producer_config=ProducerConfig(lease_ttl_ms=500, heartbeat_interval_seconds=0.02),
  )


async def test_grpc_stream_returns_all_tokens_then_done() -> None:
  service = _build_test_service(tokens=["x", "y", "z"], tokens_per_second=500.0)
  servicer = StreamingAssistantServicer(service=service)  # type: ignore[call-arg]

  context = MagicMock()
  context.cancelled.return_value = False

  assert assistant_pb2 is not None
  request = assistant_pb2.QueryRequest(  # type: ignore[attr-defined]
    session_id="s1",
    user_id="u1",
    prompt="hi",
    cancellation_token="req-1",
  )
  responses: list[Any] = []
  async for resp in servicer.StreamQuery(request, context):
    responses.append(resp)

  non_done_chunks = [r.chunk for r in responses if not r.done]
  assert non_done_chunks == ["x", "y", "z"]
  assert responses[-1].done is True
  assert responses[-1].request_id == "req-1"


async def test_grpc_cancel_requested_returns_immediately() -> None:
  service = _build_test_service(tokens=["x", "y", "z"], tokens_per_second=500.0)
  servicer = StreamingAssistantServicer(service=service)  # type: ignore[call-arg]

  context = MagicMock()
  context.cancelled.return_value = False

  assert assistant_pb2 is not None
  request = assistant_pb2.QueryRequest(  # type: ignore[attr-defined]
    session_id="s1",
    user_id="u1",
    prompt="hi",
    cancellation_token="req-1",
    cancel_requested=True,
  )
  responses: list[Any] = []
  async for resp in servicer.StreamQuery(request, context):
    responses.append(resp)

  assert responses == []


async def test_grpc_context_cancelled_stops_early() -> None:
  service = _build_test_service(tokens=["a", "b", "c", "d"], tokens_per_second=50.0)
  servicer = StreamingAssistantServicer(service=service)  # type: ignore[call-arg]

  context = MagicMock()
  context.cancelled.side_effect = [False, True, True, True]

  assert assistant_pb2 is not None
  request = assistant_pb2.QueryRequest(  # type: ignore[attr-defined]
    session_id="s1",
    user_id="u1",
    prompt="hi",
    cancellation_token="req-1",
  )
  responses: list[Any] = []
  async for resp in servicer.StreamQuery(request, context):
    responses.append(resp)

  token_chunks = [r.chunk for r in responses if not r.done]
  assert len(token_chunks) < 4

