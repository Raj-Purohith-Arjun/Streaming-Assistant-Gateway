import structlog
from fastapi import FastAPI
from starlette.testclient import TestClient

from app.middleware.error_handling import ErrorHandlingMiddleware
from app.middleware.rate_limit import RateLimitMiddleware
from app.middleware.tracing import CorrelationIdMiddleware


def test_correlation_id_propagates_to_route_handler() -> None:
  test_app = FastAPI()
  test_app.add_middleware(CorrelationIdMiddleware)

  captured_id: str | None = None

  @test_app.get("/test-correlation")
  async def sample_route() -> dict[str, str | None]:
    nonlocal captured_id
    ctx = structlog.contextvars.get_contextvars()
    captured_id = ctx.get("correlation_id")
    return {"correlation_id": captured_id}

  client = TestClient(test_app)
  custom_id = "test-corr-id-12345"
  response = client.get("/test-correlation", headers={"x-correlation-id": custom_id})

  assert response.status_code == 200
  assert response.headers.get("x-correlation-id") == custom_id
  assert captured_id == custom_id
  assert response.json() == {"correlation_id": custom_id}


def test_correlation_id_generated_when_missing_and_propagates() -> None:
  test_app = FastAPI()
  test_app.add_middleware(CorrelationIdMiddleware)

  captured_id: str | None = None

  @test_app.get("/test-correlation-gen")
  async def sample_route() -> dict[str, str | None]:
    nonlocal captured_id
    ctx = structlog.contextvars.get_contextvars()
    captured_id = ctx.get("correlation_id")
    return {"correlation_id": captured_id}

  client = TestClient(test_app)
  response = client.get("/test-correlation-gen")

  assert response.status_code == 200
  corr_header = response.headers.get("x-correlation-id")
  assert corr_header is not None
  assert len(corr_header) > 0
  assert captured_id == corr_header
  assert response.json() == {"correlation_id": corr_header}


def test_error_handling_middleware_catches_unhandled_exception() -> None:
  test_app = FastAPI()
  test_app.add_middleware(ErrorHandlingMiddleware)

  @test_app.get("/boom")
  async def boom_route() -> dict[str, str]:
    raise RuntimeError("Something exploded")

  client = TestClient(test_app, raise_server_exceptions=False)
  response = client.get("/boom")

  assert response.status_code == 500
  assert response.json() == {"detail": "Internal server error"}


def test_rate_limit_middleware_blocks_when_limit_exceeded() -> None:
  test_app = FastAPI()
  test_app.add_middleware(RateLimitMiddleware, requests_per_minute=2, burst=1)

  @test_app.get("/limited")
  async def limited_route() -> dict[str, str]:
    return {"status": "ok"}

  client = TestClient(test_app)
  # Limit = 2 + 1 = 3 requests allowed
  for _ in range(3):
    res = client.get("/limited")
    assert res.status_code == 200

  # 4th request must be rate limited
  res = client.get("/limited")
  assert res.status_code == 429
  assert res.json() == {"detail": "Rate limit exceeded"}
