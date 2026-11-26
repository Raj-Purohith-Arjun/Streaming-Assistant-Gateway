from uuid import uuid4

import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.logging import set_correlation_id


class CorrelationIdMiddleware:
  """Pure ASGI middleware for extracting or generating and propagating correlation IDs."""

  def __init__(self, app: ASGIApp) -> None:
    self.app = app

  async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] != "http":
      await self.app(scope, receive, send)
      return

    correlation_id: str | None = None
    for name, value in scope.get("headers", []):
      if name.lower() == b"x-correlation-id":
        correlation_id = value.decode("latin1")
        break

    if not correlation_id:
      correlation_id = str(uuid4())

    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
    set_correlation_id(correlation_id)

    async def send_wrapper(message: Message) -> None:
      if message["type"] == "http.response.start":
        headers = [h for h in message.get("headers", []) if h[0].lower() != b"x-correlation-id"]
        headers.append((b"x-correlation-id", correlation_id.encode("latin1")))
        message = dict(message)
        message["headers"] = headers
      await send(message)

    await self.app(scope, receive, send_wrapper)
