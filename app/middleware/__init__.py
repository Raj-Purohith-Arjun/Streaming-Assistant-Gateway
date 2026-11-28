"""Gateway middleware components."""

from app.middleware.auth import ApiKeyAuthMiddleware

__all__ = ["ApiKeyAuthMiddleware"]
