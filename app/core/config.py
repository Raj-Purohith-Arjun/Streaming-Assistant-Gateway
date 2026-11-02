from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
  model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
  return Settings()
