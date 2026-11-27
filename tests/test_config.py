import pytest

from app.core.config import Settings
from app.gateway.main import build_upstream_adapter
from app.upstream import GeminiAdapter, GroqAdapter, SyntheticAdapter


def test_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.setenv("REDIS_URL", "redis://localhost:6380/1")
  monkeypatch.setenv("GRPC_UPSTREAM_TARGET", "localhost:6000")
  monkeypatch.setenv("RATE_LIMIT_REQUESTS_PER_MINUTE", "50")

  settings = Settings()

  assert settings.redis_url == "redis://localhost:6380/1"
  assert settings.grpc_upstream_target == "localhost:6000"
  assert settings.rate_limit_requests_per_minute == 50


def test_upstream_adapter_selection_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
  """Single configured provider selection (no multi-provider routing or fallback)."""
  default_adapter = build_upstream_adapter(Settings())
  assert isinstance(default_adapter, SyntheticAdapter)

  monkeypatch.setenv("UPSTREAM_PROVIDER", "gemini")
  monkeypatch.setenv("UPSTREAM_API_KEY", "gem-key")
  monkeypatch.setenv("UPSTREAM_MODEL", "gemini-2.0-flash")
  monkeypatch.setenv("SYNTHETIC_TOKENS_PER_SECOND", "42.0")
  gemini_settings = Settings()
  assert gemini_settings.upstream_provider == "gemini"
  assert gemini_settings.upstream_api_key == "gem-key"
  assert gemini_settings.upstream_model == "gemini-2.0-flash"
  assert gemini_settings.synthetic_tokens_per_second == 42.0
  assert isinstance(build_upstream_adapter(gemini_settings), GeminiAdapter)

  monkeypatch.setenv("UPSTREAM_PROVIDER", "groq")
  monkeypatch.setenv("UPSTREAM_API_KEY", "groq-key")
  assert isinstance(build_upstream_adapter(Settings()), GroqAdapter)

  monkeypatch.setenv("UPSTREAM_PROVIDER", "gemini")
  monkeypatch.delenv("UPSTREAM_API_KEY", raising=False)
  with pytest.raises(ValueError, match="UPSTREAM_API_KEY"):
    build_upstream_adapter(Settings())

  monkeypatch.setenv("UPSTREAM_PROVIDER", "unknown")
  with pytest.raises(ValueError, match="Unsupported"):
    build_upstream_adapter(Settings())
