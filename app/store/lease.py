from dataclasses import dataclass
from typing import Protocol, runtime_checkable


class StaleLeaseError(Exception):
  """Raised when a producer operates with an expired or superseded fencing token."""


@dataclass(frozen=True, slots=True)
class Lease:
  """Active producer lease with a monotonic fencing token."""

  gen_id: str
  owner_id: str
  fence: int


@runtime_checkable
class LeaseStore(Protocol):
  """Abstract protocol for single-producer lease election and fencing (I3)."""

  async def acquire_lease(
    self,
    gen_id: str,
    owner_id: str,
    *,
    ttl_ms: int,
  ) -> Lease | None:
    """Acquire the lease for ``gen_id``, or return ``None`` if held by another replica."""
    ...

  async def renew_lease(
    self,
    gen_id: str,
    owner_id: str,
    fence: int,
    *,
    ttl_ms: int,
  ) -> Lease:
    """Extend the lease TTL; raise ``StaleLeaseError`` if expired or superseded."""
    ...

  async def release_lease(self, gen_id: str, owner_id: str, fence: int) -> bool:
    """Release the lease if still held by ``(owner_id, fence)``; else raise ``StaleLeaseError``."""
    ...

  async def current_fence(self, gen_id: str) -> int | None:
    """Return the active fencing token for ``gen_id``, or ``None`` if no lease is active."""
    ...
