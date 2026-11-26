import asyncio
from dataclasses import dataclass
from uuid import uuid4

from app.generation.state import GenerationLifecycle, GenerationStatus


@dataclass(frozen=True, slots=True)
class CoalesceResult:
  """Result of attaching to an existing generation or starting a new one."""

  gen_id: str
  coalesced: bool


class GenerationRegistry:
  """In-memory registry enforcing request coalescing (I9) and lifecycle transitions."""

  def __init__(self) -> None:
    self._lock = asyncio.Lock()
    self._by_request_key: dict[str, str] = {}
    self._lifecycles: dict[str, GenerationLifecycle] = {}

  async def get_or_create(self, request_key: str) -> tuple[str, bool]:
    """Return ``(gen_id, created)`` for ``request_key``, coalescing live requests (I9)."""
    async with self._lock:
      existing_gen_id = self._by_request_key.get(request_key)
      if existing_gen_id is not None:
        lifecycle = self._lifecycles.get(existing_gen_id)
        if lifecycle is not None and not lifecycle.is_terminal:
          return existing_gen_id, False

      gen_id = f"gen-{uuid4()}"
      self._by_request_key[request_key] = gen_id
      self._lifecycles[gen_id] = GenerationLifecycle()
      return gen_id, True

  async def transition(self, gen_id: str, target: GenerationStatus) -> GenerationStatus:
    """Transition ``gen_id`` to ``target`` using the lifecycle state machine."""
    async with self._lock:
      lifecycle = self._lifecycles.get(gen_id)
      if lifecycle is None:
        raise KeyError(f"Unknown generation: {gen_id!r}")
      return lifecycle.transition(target)

  async def get_status(self, gen_id: str) -> GenerationStatus:
    """Return the current ``GenerationStatus`` for ``gen_id``."""
    async with self._lock:
      lifecycle = self._lifecycles.get(gen_id)
      if lifecycle is None:
        raise KeyError(f"Unknown generation: {gen_id!r}")
      return lifecycle.state


GenerationCoalescer = GenerationRegistry
