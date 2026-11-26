import asyncio
import uuid

import pytest

from app.generation import (
  TERMINAL_STATUSES,
  GenerationRegistry,
  GenerationStatus,
  InvalidTransitionError,
  validate_transition,
)


def test_state_transitions() -> None:
  """Verify allowed lifecycle transitions and rejection of invalid/post-terminal jumps."""
  assert validate_transition(GenerationStatus.PENDING, GenerationStatus.RUNNING) == (
    GenerationStatus.RUNNING
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.DRAINING) == (
    GenerationStatus.DRAINING
  )
  # The core feature: a new subscriber during DRAINING returns to RUNNING.
  assert validate_transition(GenerationStatus.DRAINING, GenerationStatus.RUNNING) == (
    GenerationStatus.RUNNING
  )
  assert validate_transition(GenerationStatus.DRAINING, GenerationStatus.EXPIRED) == (
    GenerationStatus.EXPIRED
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.COMPLETED) == (
    GenerationStatus.COMPLETED
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.STOPPED) == (
    GenerationStatus.STOPPED
  )
  assert validate_transition(GenerationStatus.RUNNING, GenerationStatus.FAILED) == (
    GenerationStatus.FAILED
  )

  # Invalid jumps must be rejected.
  with pytest.raises(InvalidTransitionError):
    validate_transition(GenerationStatus.PENDING, GenerationStatus.DRAINING)
  with pytest.raises(InvalidTransitionError):
    validate_transition(GenerationStatus.PENDING, GenerationStatus.COMPLETED)

  # Terminal states are final: zero outgoing transitions (I4).
  for terminal in TERMINAL_STATUSES:
    for target in GenerationStatus:
      with pytest.raises(InvalidTransitionError):
        validate_transition(terminal, target)


async def test_coalesce_attaches() -> None:
  """I9: Duplicate requests for a live gen_id attach and cause zero extra upstream calls."""
  registry = GenerationRegistry()
  request_key = f"req-{uuid.uuid4()}"
  upstream_calls = 0

  async def handle_request() -> str:
    nonlocal upstream_calls
    gen_id, created = await registry.get_or_create(request_key)
    if created:
      upstream_calls += 1
      await registry.transition(gen_id, GenerationStatus.RUNNING)
    return gen_id

  results = await asyncio.gather(*(handle_request() for _ in range(10)))

  assert len(set(results)) == 1
  assert upstream_calls == 1
  assert await registry.get_status(results[0]) == GenerationStatus.RUNNING
