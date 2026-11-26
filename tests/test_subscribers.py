import uuid

from app.store.subscribers import InMemorySubscriberRegistry, SubscriberRegistry


async def test_register_unregister_and_ttl_expiry() -> None:
  """Register/unregister updates count; expired subscribers are pruned automatically."""
  clock = 0.0
  registry = InMemorySubscriberRegistry(ttl_seconds=10.0, clock=lambda: clock)
  assert isinstance(registry, SubscriberRegistry)
  gen_id = f"gen-{uuid.uuid4()}"

  assert await registry.active_count(gen_id) == 0
  assert await registry.register(gen_id, "sub-1") == 1
  assert await registry.register(gen_id, "sub-2") == 2
  assert await registry.active_count(gen_id) == 2

  assert await registry.unregister(gen_id, "sub-1") == 1
  assert await registry.active_count(gen_id) == 1

  # Simulate replica crash: sub-2 never calls unregister, and its TTL expires.
  clock += 11.0
  assert await registry.active_count(gen_id) == 0


async def test_subscriber_heartbeat_keeps_alive() -> None:
  """Periodic heartbeat extends subscriber TTL so active_count stays >= 1."""
  clock = 0.0
  registry = InMemorySubscriberRegistry(ttl_seconds=10.0, clock=lambda: clock)
  gen_id = f"gen-{uuid.uuid4()}"

  await registry.register(gen_id, "sub-1")

  clock += 6.0
  assert await registry.heartbeat(gen_id, "sub-1") is True

  # At t=12.0 (past original t=10.0, before renewed t=16.0), sub-1 is still alive.
  clock += 6.0
  assert await registry.active_count(gen_id) == 1

  # Once past t=16.0 without another heartbeat, sub-1 expires.
  clock += 5.0
  assert await registry.active_count(gen_id) == 0
  assert await registry.heartbeat(gen_id, "sub-1") is False
