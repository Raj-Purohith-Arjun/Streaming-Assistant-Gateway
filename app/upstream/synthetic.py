import asyncio
from collections.abc import AsyncGenerator, Sequence


class SyntheticAdapter:
  """Deterministic in-process adapter for testing and benchmarking."""

  def __init__(
    self,
    tokens: Sequence[str],
    tokens_per_second: float = 50.0,
    fail_after_tokens: int | None = None,
  ) -> None:
    if tokens_per_second <= 0:
      raise ValueError(f"tokens_per_second must be positive, got {tokens_per_second}")
    if fail_after_tokens is not None and fail_after_tokens < 0:
      raise ValueError(f"fail_after_tokens must be non-negative, got {fail_after_tokens}")
    self._tokens = list(tokens)
    self._delay_seconds = 1.0 / tokens_per_second
    self._fail_after_tokens = fail_after_tokens

  async def stream(self, prompt: str, *, model: str | None = None) -> AsyncGenerator[str, None]:
    """Yield configured tokens paced at ``tokens_per_second``."""
    _ = (prompt, model)
    for idx, token in enumerate(self._tokens):
      if self._fail_after_tokens is not None and idx >= self._fail_after_tokens:
        raise RuntimeError(f"Synthetic failure after {self._fail_after_tokens} tokens")
      await asyncio.sleep(self._delay_seconds)
      yield token
