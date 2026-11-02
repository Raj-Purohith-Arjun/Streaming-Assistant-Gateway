from app.core.logging import _inject_correlation_id, set_correlation_id


def test_logging_injects_correlation_id():
  set_correlation_id("corr-123")
  event_dict = _inject_correlation_id(None, "info", {"event": "hello"})
  assert event_dict["correlation_id"] == "corr-123"
