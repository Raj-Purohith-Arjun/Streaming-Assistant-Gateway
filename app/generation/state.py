from enum import StrEnum


class GenerationStatus(StrEnum):
  """Lifecycle states for a single generation."""

  PENDING = "PENDING"
  RUNNING = "RUNNING"
  DRAINING = "DRAINING"
  COMPLETED = "COMPLETED"
  STOPPED = "STOPPED"
  EXPIRED = "EXPIRED"
  FAILED = "FAILED"


GenerationState = GenerationStatus

TERMINAL_STATUSES: frozenset[GenerationStatus] = frozenset(
  {
    GenerationStatus.COMPLETED,
    GenerationStatus.STOPPED,
    GenerationStatus.EXPIRED,
    GenerationStatus.FAILED,
  }
)

TERMINAL_STATES = TERMINAL_STATUSES


class InvalidTransitionError(Exception):
  """Raised when an invalid or post-terminal lifecycle transition is attempted."""


_ALLOWED_TRANSITIONS: dict[GenerationStatus, frozenset[GenerationStatus]] = {
  GenerationStatus.PENDING: frozenset({GenerationStatus.RUNNING}),
  GenerationStatus.RUNNING: frozenset(
    {
      GenerationStatus.DRAINING,
      GenerationStatus.COMPLETED,
      GenerationStatus.STOPPED,
      GenerationStatus.FAILED,
    }
  ),
  GenerationStatus.DRAINING: frozenset(
    {
      GenerationStatus.RUNNING,
      GenerationStatus.COMPLETED,
      GenerationStatus.STOPPED,
      GenerationStatus.EXPIRED,
      GenerationStatus.FAILED,
    }
  ),
  GenerationStatus.COMPLETED: frozenset(),
  GenerationStatus.STOPPED: frozenset(),
  GenerationStatus.EXPIRED: frozenset(),
  GenerationStatus.FAILED: frozenset(),
}


def validate_transition(current: GenerationStatus, target: GenerationStatus) -> GenerationStatus:
  """Validate a state transition and return ``target``, or raise ``InvalidTransitionError``."""
  if target not in _ALLOWED_TRANSITIONS[current]:
    raise InvalidTransitionError(f"Cannot transition from {current.value} to {target.value}")
  return target


class GenerationLifecycle:
  """Mutable state machine for a single generation."""

  def __init__(self, initial_state: GenerationStatus = GenerationStatus.PENDING) -> None:
    self._state = initial_state

  @property
  def state(self) -> GenerationStatus:
    """Return the current generation status."""
    return self._state

  @property
  def is_terminal(self) -> bool:
    """Return ``True`` when the generation has reached a terminal status."""
    return self._state in TERMINAL_STATUSES

  def transition(self, target: GenerationStatus) -> GenerationStatus:
    """Transition to ``target`` if allowed, otherwise raise ``InvalidTransitionError``."""
    self._state = validate_transition(self._state, target)
    return self._state
