from app.core.config import Settings


def test_settings_load_from_environment(monkeypatch):
  monkeypatch.setenv("REDIS_URL", "redis://localhost:6380/1")
  monkeypatch.setenv("GRPC_UPSTREAM_TARGET", "localhost:6000")
  monkeypatch.setenv("RATE_LIMIT_REQUESTS_PER_MINUTE", "50")

  settings = Settings()

  assert settings.redis_url == "redis://localhost:6380/1"
  assert settings.grpc_upstream_target == "localhost:6000"
  assert settings.rate_limit_requests_per_minute == 50
