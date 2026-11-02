import structlog
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger(__name__)


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
  async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
    try:
      return await call_next(request)
    except Exception:  # pragma: no cover
      logger.exception("unhandled_exception", path=request.url.path, method=request.method)
      return JSONResponse(status_code=500, content={"detail": "Internal server error"})
