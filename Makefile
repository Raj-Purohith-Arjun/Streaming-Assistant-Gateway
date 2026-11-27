PYTHON ?= python

.PHONY: proto dev test verify docker-build

proto:
	$(PYTHON) -m grpc_tools.protoc \
		-I./protos \
		--python_out=./app/grpc_server/generated \
		--grpc_python_out=./app/grpc_server/generated \
		./protos/assistant.proto
	sed -i 's/^import assistant_pb2 as assistant__pb2/from . import assistant_pb2 as assistant__pb2/' ./app/grpc_server/generated/assistant_pb2_grpc.py

dev:
	uvicorn app.gateway.main:app --host 0.0.0.0 --port 8000 --reload

test:
	$(PYTHON) -m pytest -q

verify: proto
	$(PYTHON) -m ruff check app/ tests/ bench/
	$(PYTHON) -m mypy app/cancel app/core app/gateway app/generation app/middleware app/producer app/sse app/store app/upstream app/grpc_server/server.py tests/ bench/
	$(PYTHON) -m pytest tests/ -v

docker-build:
	docker build -t streaming-assistant-gateway:latest .
