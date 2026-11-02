# Streaming Assistant Gateway

High-performance streaming gateway that exposes a FastAPI interface and bridges to upstream gRPC assistant providers.

## Features

- FastAPI gateway with server-sent event streaming endpoint
- Async gRPC service contract and server scaffold
- Redis connection pooling primitives
- Structured JSON logging with correlation IDs
- Middleware for rate limiting, tracing, and error handling
- Dockerized local development + Kubernetes deployment manifests

## Repository Layout

```text
protos/
app/
  core/
  grpc_server/
  gateway/
  services/
  middleware/
tests/
deploy/
```

## Quickstart

1. Install dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

2. Compile protobuf stubs:

   ```bash
   make proto
   ```

3. Run locally:

   ```bash
   make dev
   ```

4. Run tests:

   ```bash
   make test
   ```

## Environment Variables

- `REDIS_URL`
- `GRPC_UPSTREAM_TARGET`
- `SERVER_HOST`, `SERVER_PORT`
- `GRPC_BIND_HOST`, `GRPC_BIND_PORT`
- `RATE_LIMIT_REQUESTS_PER_MINUTE`, `RATE_LIMIT_BURST`

## Docker

```bash
docker compose up --build
```

## Kubernetes

Apply manifests under `deploy/`:

```bash
kubectl apply -f deploy/
```
