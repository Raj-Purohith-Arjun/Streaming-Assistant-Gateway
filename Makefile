PYTHON ?= python

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
	pytest -q

docker-build:
	docker build -t streaming-assistant-gateway:latest .
