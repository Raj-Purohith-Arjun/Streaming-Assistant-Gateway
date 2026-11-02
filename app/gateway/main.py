from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.middleware.error_handling import ErrorHandlingMiddleware
from app.middleware.rate_limit import RateLimitMiddleware
from app.middleware.tracing import CorrelationIdMiddleware
from app.services.request_coalescer import RequestCoalescer
from app.services.stream_manager import StreamManager
from app.services.upstream_client import UpstreamClient


class StreamRequest(BaseModel):
  session_id: str = Field(min_length=1)
  user_id: str = Field(min_length=1)
  prompt: str = Field(min_length=1)
  metadata: dict[str, str] = Field(default_factory=dict)
  cancellation_token: str | None = None


settings = get_settings()
configure_logging()

app = FastAPI(title="Streaming Assistant Gateway", version="0.1.0")
app.add_middleware(ErrorHandlingMiddleware)
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(
  RateLimitMiddleware,
  requests_per_minute=settings.rate_limit_requests_per_minute,
  burst=settings.rate_limit_burst,
)

_stream_manager = StreamManager(upstream_client=UpstreamClient(), coalescer=RequestCoalescer())


@app.get("/healthz")
async def healthz() -> dict[str, str]:
  return {"status": "ok"}


@app.post("/v1/stream")
async def stream_query(request: StreamRequest) -> StreamingResponse:
  async def event_stream() -> AsyncIterator[str]:
    async for chunk in _stream_manager.stream(session_id=request.session_id, prompt=request.prompt):
      yield f"data: {chunk}\n\n"
    yield "event: done\ndata: complete\n\n"

  return StreamingResponse(event_stream(), media_type="text/event-stream")
