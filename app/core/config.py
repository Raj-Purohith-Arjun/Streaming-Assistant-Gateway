import sys
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
  model_config = SettingsConfigDict(
    env_file=None if "pytest" in sys.modules else ".env",
    env_file_encoding="utf-8",
    extra="ignore",
  )

  app_name: str = "streaming-assistant-gateway"
  environment: str = "development"

  server_host: str = Field(default="0.0.0.0", alias="SERVER_HOST")
  server_port: int = Field(default=8000, alias="SERVER_PORT")

  grpc_bind_host: str = Field(default="0.0.0.0", alias="GRPC_BIND_HOST")
  grpc_bind_port: int = Field(default=50051, alias="GRPC_BIND_PORT")
  grpc_upstream_target: str = Field(default="mock-upstream:50052", alias="GRPC_UPSTREAM_TARGET")

  redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
  redis_max_connections: int = Field(default=200, alias="REDIS_MAX_CONNECTIONS")

  rate_limit_requests_per_minute: int = Field(
    default=120,
    alias="RATE_LIMIT_REQUESTS_PER_MINUTE",
  )
  rate_limit_burst: int = Field(default=30, alias="RATE_LIMIT_BURST")

  generation_ttl_seconds: int = Field(default=3600, alias="GENERATION_TTL_SECONDS")
  drain_timeout_seconds: float = Field(default=5.0, alias="DRAIN_TIMEOUT_SECONDS")
  lease_ttl_ms: int = Field(default=5000, alias="LEASE_TTL_MS")
  subscriber_ttl_seconds: float = Field(default=30.0, alias="SUBSCRIBER_TTL_SECONDS")

  upstream_provider: str = Field(default="synthetic", alias="UPSTREAM_PROVIDER")
  upstream_api_key: str = Field(default="", alias="UPSTREAM_API_KEY")
  upstream_model: str = Field(default="", alias="UPSTREAM_MODEL")
  synthetic_tokens_per_second: float = Field(default=25.0, alias="SYNTHETIC_TOKENS_PER_SECOND")
  gateway_api_key: str = Field(default="", alias="GATEWAY_API_KEY")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
  return Settings()
