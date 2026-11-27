import hashlib
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

import grpc  # type: ignore[import-untyped]

from app.core.config import get_settings
from app.gateway.main import _build_default_service
from app.gateway.service import GatewayService

settings = get_settings()

if TYPE_CHECKING:
  from app.grpc_server.generated import assistant_pb2, assistant_pb2_grpc
else:
  try:
    from app.grpc_server.generated import assistant_pb2, assistant_pb2_grpc
  except ImportError as exc:  # pragma: no cover
    raise RuntimeError("Generated gRPC stubs are missing. Run `make proto`.") from exc


class StreamingAssistantServicer(assistant_pb2_grpc.StreamingAssistantServicer):  # type: ignore[name-defined]
  def __init__(self, service: GatewayService | None = None) -> None:
    self._service = service or _build_default_service()

  async def StreamQuery(
    self,
    request: Any,
    context: grpc.aio.ServicerContext,
  ) -> AsyncIterator[Any]:
    if request.cancel_requested:
      return

    raw = f"{request.session_id}:{request.user_id}:{request.prompt}".encode()
    request_key = hashlib.sha256(raw).hexdigest()
    gen_id, _ = await self._service.start_or_attach(request_key, request.prompt)

    async for frame in self._service.stream_sse(gen_id):
      if frame.startswith(":"):
        continue

      event_type = ""
      data: dict[str, Any] = {}
      for line in frame.splitlines():
        if line.startswith(":"):
          continue
        if line.startswith("event: "):
          event_type = line[7:].strip()
        elif line.startswith("data: "):
          try:
            data = json.loads(line[6:].strip())
          except json.JSONDecodeError:
            data = {}

      if event_type == "token":
        yield assistant_pb2.QueryResponse(  # type: ignore[attr-defined]
          session_id=request.session_id,
          chunk=str(data.get("text", "")),
          done=False,
          request_id=request.cancellation_token,
        )
        if context.cancelled():
          break
      elif event_type in ("done", "error"):
        yield assistant_pb2.QueryResponse(  # type: ignore[attr-defined]
          session_id=request.session_id,
          done=True,
          request_id=request.cancellation_token,
        )
        return


async def serve() -> None:
  server = grpc.aio.server()
  assistant_pb2_grpc.add_StreamingAssistantServicer_to_server(
    StreamingAssistantServicer(service=_build_default_service()), server
  )
  server.add_insecure_port(f"{settings.grpc_bind_host}:{settings.grpc_bind_port}")
  await server.start()
  await server.wait_for_termination()
