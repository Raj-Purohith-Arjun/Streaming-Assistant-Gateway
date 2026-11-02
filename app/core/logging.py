import logging
from contextvars import ContextVar
from typing import Any

import structlog

correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="")


def set_correlation_id(correlation_id: str) -> None:
  correlation_id_ctx.set(correlation_id)


def _inject_correlation_id(_: logging.Logger, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
  event_dict["correlation_id"] = correlation_id_ctx.get()
  return event_dict


def configure_logging(log_level: str = "INFO") -> None:
  timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

  structlog.configure(
    processors=[
      structlog.contextvars.merge_contextvars,
      _inject_correlation_id,
      structlog.stdlib.add_log_level,
      timestamper,
      structlog.processors.EventRenamer("message"),
      structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
  )

  logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO), format="%(message)s")
