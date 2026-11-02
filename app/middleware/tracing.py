from uuid import uuid4

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.logging import set_correlation_id


class CorrelationIdMiddleware(BaseHTTPMiddleware):
  async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
    correlation_id = request.headers.get("x-correlation-id", str(uuid4()))
    set_correlation_id(correlation_id)
    response = await call_next(request)
    response.headers["x-correlation-id"] = correlation_id
    return response
