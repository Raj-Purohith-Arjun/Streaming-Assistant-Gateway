from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool

from app.core.config import get_settings

_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
  global _pool
  if _pool is None:
    settings = get_settings()
    _pool = ConnectionPool.from_url(
      settings.redis_url,
      max_connections=settings.redis_max_connections,
      decode_responses=True,
    )
  return _pool


def get_redis_client() -> Redis:
  return Redis(connection_pool=get_pool())
