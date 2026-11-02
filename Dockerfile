FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
RUN pip install --upgrade pip && pip install --prefix=/install .

COPY protos ./protos
COPY app ./app

RUN python -m grpc_tools.protoc \
    -I./protos \
    --python_out=./app/grpc_server/generated \
    --grpc_python_out=./app/grpc_server/generated \
    ./protos/assistant.proto \
    && sed -i 's/^import assistant_pb2 as assistant__pb2/from . import assistant_pb2 as assistant__pb2/' ./app/grpc_server/generated/assistant_pb2_grpc.py

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system app && adduser --system --ingroup app app
WORKDIR /app

COPY --from=builder /install /usr/local
COPY --from=builder /app/app ./app
COPY --from=builder /app/protos ./protos
COPY Makefile pyproject.toml README.md ./

USER app
EXPOSE 8000 50051

CMD ["uvicorn", "app.gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
