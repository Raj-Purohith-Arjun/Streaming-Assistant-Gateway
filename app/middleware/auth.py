import hmac

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

_PUBLIC_PATHS = frozenset({"/healthz", "/demo"})


def _extract_api_key(headers: list[tuple[bytes, bytes]]) -> str | None:
  for raw_name, raw_value in headers:
    name = raw_name.decode("latin-1").lower()
    if name == "x-api-key":
      return raw_value.decode("latin-1").strip()
    if name == "authorization":
      value = raw_value.decode("latin-1").strip()
      if value.lower().startswith("bearer "):
        return value[7:].strip()
  return None


class ApiKeyAuthMiddleware:
  """Pure-ASGI static API key authentication middleware."""

  def __init__(self, app: ASGIApp, *, api_key: str = "") -> None:
    self.app = app
    self._api_key = api_key

  async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] != "http" or not self._api_key:
      await self.app(scope, receive, send)
      return

    if scope.get("path", "") in _PUBLIC_PATHS:
      await self.app(scope, receive, send)
      return

    candidate = _extract_api_key(scope.get("headers", []))
    if candidate is None or not hmac.compare_digest(candidate, self._api_key):
      response = JSONResponse(status_code=401, content={"error": "unauthorized"})
      await response(scope, receive, send)
      return

    await self.app(scope, receive, send)
