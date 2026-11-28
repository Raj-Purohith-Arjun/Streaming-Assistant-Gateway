import httpx
import pytest
from fastapi import FastAPI

from app.core.config import Settings
from app.middleware.auth import ApiKeyAuthMiddleware


def _build_protected_app(api_key: str) -> FastAPI:
  test_app = FastAPI()
  test_app.add_middleware(ApiKeyAuthMiddleware, api_key=api_key)

  @test_app.get("/healthz")
  async def healthz() -> dict[str, str]:
    return {"status": "ok"}

  @test_app.get("/demo")
  async def demo() -> dict[str, str]:
    return {"page": "demo"}

  @test_app.get("/protected")
  async def protected() -> dict[str, str]:
    return {"access": "granted"}

  @test_app.post("/v1/stream")
  async def stream() -> dict[str, str]:
    return {"stream": "ok"}

  return test_app


async def test_auth_disabled_when_api_key_empty() -> None:
  test_app = _build_protected_app(api_key="")
  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=test_app),
    base_url="http://test",
  ) as client:
    resp = await client.get("/protected")
    assert resp.status_code == 200
    assert resp.json() == {"access": "granted"}


async def test_auth_rejects_missing_or_wrong_key() -> None:
  test_app = _build_protected_app(api_key="secret-123")
  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=test_app),
    base_url="http://test",
  ) as client:
    missing = await client.get("/protected")
    assert missing.status_code == 401
    assert isinstance(missing.json(), dict)

    wrong_x = await client.get("/protected", headers={"X-API-Key": "wrong"})
    assert wrong_x.status_code == 401
    assert isinstance(wrong_x.json(), dict)

    wrong_bearer = await client.get(
      "/protected",
      headers={"Authorization": "Bearer wrong"},
    )
    assert wrong_bearer.status_code == 401
    assert isinstance(wrong_bearer.json(), dict)


async def test_auth_accepts_x_api_key_and_bearer_header() -> None:
  test_app = _build_protected_app(api_key="secret-123")
  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=test_app),
    base_url="http://test",
  ) as client:
    resp_x = await client.get("/protected", headers={"X-API-Key": "secret-123"})
    assert resp_x.status_code == 200
    assert resp_x.json() == {"access": "granted"}

    resp_bearer = await client.get(
      "/protected",
      headers={"Authorization": "Bearer secret-123"},
    )
    assert resp_bearer.status_code == 200
    assert resp_bearer.json() == {"access": "granted"}


async def test_auth_allows_healthz_and_demo_without_key() -> None:
  test_app = _build_protected_app(api_key="secret-123")
  async with httpx.AsyncClient(
    transport=httpx.ASGITransport(app=test_app),
    base_url="http://test",
  ) as client:
    health_resp = await client.get("/healthz")
    assert health_resp.status_code == 200

    demo_resp = await client.get("/demo")
    assert demo_resp.status_code == 200


def test_settings_gateway_api_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.setenv("GATEWAY_API_KEY", "k-env")
  assert Settings().gateway_api_key == "k-env"
