import hashlib

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.cancel import DeferredCanceller
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.redis import get_redis_client
from app.gateway.service import GatewayService
from app.generation import GenerationRegistry
from app.middleware.error_handling import ErrorHandlingMiddleware
from app.middleware.rate_limit import RateLimitMiddleware
from app.middleware.tracing import CorrelationIdMiddleware
from app.sse import SseEncoder
from app.store.protocol import InvalidOffsetError, OffsetAheadError
from app.store.redis_store import RedisGenerationStore
from app.store.subscribers import RedisSubscriberRegistry
from app.upstream import SyntheticAdapter


class StreamRequest(BaseModel):
  """Request body for initiating or coalescing onto a streaming generation."""

  session_id: str = Field(min_length=1)
  user_id: str = Field(min_length=1)
  prompt: str = Field(min_length=1)
  metadata: dict[str, str] = Field(default_factory=dict)
  cancellation_token: str | None = None


def _build_default_service() -> GatewayService:
  redis = get_redis_client()
  redis_store = RedisGenerationStore(redis)
  subscribers = RedisSubscriberRegistry(redis)
  registry = GenerationRegistry()
  canceller = DeferredCanceller(
    registry=registry,
    subscribers=subscribers,
    gen_store=redis_store,
    drain_timeout_seconds=5.0,
  )
  upstream = SyntheticAdapter(
    tokens=["Hello", " from", " Streaming", " Assistant", " Gateway!"],
    tokens_per_second=25.0,
  )
  return GatewayService(
    gen_store=redis_store,
    lease_store=redis_store,
    subscribers=subscribers,
    registry=registry,
    canceller=canceller,
    upstream=upstream,
    sse_encoder=SseEncoder(),
  )


def _derive_request_key(request: StreamRequest) -> str:
  raw = f"{request.session_id}:{request.user_id}:{request.prompt}".encode()
  return hashlib.sha256(raw).hexdigest()


def create_app(service: GatewayService | None = None) -> FastAPI:
  """Create and configure the FastAPI gateway application."""
  settings = get_settings()
  configure_logging()
  gateway_service = service or _build_default_service()

  gateway_app = FastAPI(title="Streaming Assistant Gateway", version="0.1.0")
  gateway_app.add_middleware(ErrorHandlingMiddleware)
  gateway_app.add_middleware(CorrelationIdMiddleware)
  gateway_app.add_middleware(
    RateLimitMiddleware,
    requests_per_minute=settings.rate_limit_requests_per_minute,
    burst=settings.rate_limit_burst,
  )

  @gateway_app.get("/healthz")
  async def healthz() -> dict[str, str]:
    return {"status": "ok"}

  @gateway_app.post("/v1/stream")
  async def stream_query(request: StreamRequest) -> StreamingResponse:
    request_key = _derive_request_key(request)
    gen_id, _ = await gateway_service.start_or_attach(request_key, request.prompt)
    return StreamingResponse(
      gateway_service.stream_sse(gen_id),
      media_type="text/event-stream",
      headers={"X-Generation-Id": gen_id},
    )

  @gateway_app.get("/v1/generations/{gen_id}/stream")
  async def resume_stream(
    gen_id: str,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
  ) -> StreamingResponse:
    try:
      await gateway_service.validate_stream_request(gen_id, last_event_id)
    except KeyError as exc:
      raise HTTPException(status_code=404, detail=f"Generation {gen_id!r} not found") from exc
    except (InvalidOffsetError, OffsetAheadError) as exc:
      raise HTTPException(status_code=400, detail=str(exc)) from exc

    return StreamingResponse(
      gateway_service.stream_sse(gen_id, last_event_id=last_event_id),
      media_type="text/event-stream",
      headers={"X-Generation-Id": gen_id},
    )

  @gateway_app.post("/v1/generations/{gen_id}/stop")
  async def stop_generation(gen_id: str) -> dict[str, str]:
    try:
      await gateway_service.stop(gen_id)
    except KeyError as exc:
      raise HTTPException(status_code=404, detail=f"Generation {gen_id!r} not found") from exc
    return {"gen_id": gen_id, "status": "STOPPED"}

  return gateway_app


app = create_app()
