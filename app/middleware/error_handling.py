import asyncio

import structlog
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = structlog.get_logger(__name__)


class ErrorHandlingMiddleware:
  """Pure ASGI middleware for capturing unhandled exceptions and returning 500 JSON responses."""

  def __init__(self, app: ASGIApp) -> None:
    self.app = app

  async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] != "http":
      await self.app(scope, receive, send)
      return

    response_started = False

    async def send_wrapper(message: Message) -> None:
      nonlocal response_started
      if message["type"] == "http.response.start":
        response_started = True
      await send(message)

    try:
      await self.app(scope, receive, send_wrapper)
    except asyncio.CancelledError:
      raise
    except Exception:
      logger.exception(
        "unhandled_exception",
        path=scope.get("path", ""),
        method=scope.get("method", ""),
      )
      if not response_started:
        response = JSONResponse(status_code=500, content={"detail": "Internal server error"})
        await response(scope, receive, send)
      else:
        raise
