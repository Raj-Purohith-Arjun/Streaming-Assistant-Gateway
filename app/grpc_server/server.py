from collections.abc import AsyncIterator

import grpc

from app.core.config import get_settings
from app.services.request_coalescer import RequestCoalescer
from app.services.stream_manager import StreamManager
from app.services.upstream_client import UpstreamClient

settings = get_settings()

try:
  from app.grpc_server.generated import assistant_pb2, assistant_pb2_grpc
except ImportError as exc:  # pragma: no cover
  raise RuntimeError("Generated gRPC stubs are missing. Run `make proto`.") from exc


class StreamingAssistantServicer(assistant_pb2_grpc.StreamingAssistantServicer):
  def __init__(self) -> None:
    self._manager = StreamManager(UpstreamClient(), RequestCoalescer())

  async def StreamQuery(
    self,
    request: assistant_pb2.QueryRequest,
    context: grpc.aio.ServicerContext,
  ) -> AsyncIterator[assistant_pb2.QueryResponse]:
    if request.cancel_requested:
      return

    async for chunk in self._manager.stream(session_id=request.session_id, prompt=request.prompt):
      if context.cancelled():
        break
      yield assistant_pb2.QueryResponse(
        session_id=request.session_id,
        chunk=chunk,
        done=False,
        request_id=request.cancellation_token,
      )

    yield assistant_pb2.QueryResponse(
      session_id=request.session_id,
      done=True,
      request_id=request.cancellation_token,
    )


async def serve() -> None:
  server = grpc.aio.server()
  assistant_pb2_grpc.add_StreamingAssistantServicer_to_server(StreamingAssistantServicer(), server)
  server.add_insecure_port(f"{settings.grpc_bind_host}:{settings.grpc_bind_port}")
  await server.start()
  await server.wait_for_termination()
