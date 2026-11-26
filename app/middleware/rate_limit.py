import time
from collections import defaultdict, deque

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class RateLimitMiddleware:
  """In-process sliding window rate limiting pure ASGI middleware."""

  def __init__(self, app: ASGIApp, requests_per_minute: int, burst: int) -> None:
    self.app = app
    self._requests_per_minute = requests_per_minute
    self._burst = burst
    self._request_windows: dict[str, deque[float]] = defaultdict(deque)

  async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] != "http":
      await self.app(scope, receive, send)
      return

    client = scope.get("client")
    client_key = client[0] if client else "unknown"
    now = time.time()
    window = self._request_windows[client_key]

    while window and now - window[0] > 60:
      window.popleft()

    if len(window) >= self._requests_per_minute + self._burst:
      response = JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})
      await response(scope, receive, send)
      return

    window.append(now)
    await self.app(scope, receive, send)
