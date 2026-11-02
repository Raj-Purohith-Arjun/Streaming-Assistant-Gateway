from collections import defaultdict


class RequestCoalescer:
  """Tracks in-flight requests by session ID for simple de-duplication."""

  def __init__(self) -> None:
    self._active_requests: dict[str, int] = defaultdict(int)

  def begin(self, session_id: str) -> None:
    self._active_requests[session_id] += 1

  def end(self, session_id: str) -> None:
    if session_id not in self._active_requests:
      return
    self._active_requests[session_id] -= 1
    if self._active_requests[session_id] <= 0:
      self._active_requests.pop(session_id, None)

  def is_active(self, session_id: str) -> bool:
    return session_id in self._active_requests
