import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware


class RateLimitMiddleware(BaseHTTPMiddleware):
  def __init__(self, app, requests_per_minute: int, burst: int) -> None:  # type: ignore[no-untyped-def]
    super().__init__(app)
    self._requests_per_minute = requests_per_minute
    self._burst = burst
    self._request_windows: dict[str, deque[float]] = defaultdict(deque)

  async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
    client_key = request.client.host if request.client else "unknown"
    now = time.time()
    window = self._request_windows[client_key]

    while window and now - window[0] > 60:
      window.popleft()

    if len(window) >= self._requests_per_minute + self._burst:
      raise HTTPException(status_code=429, detail="Rate limit exceeded")

    window.append(now)
    return await call_next(request)
