# AWS Quota Simulator - Agent Instructions

## Project Overview

**AWS Quota Simulator** is a proxy service that sits between applications using AWS SDK and local AWS emulators (Floci/LocalStack) to simulate real AWS quota behavior.

## Problem Statement

Floci and LocalStack emulate AWS services but don't enforce quota/throughput limits. This tool adds that layer to simulate real-world AWS throttling behavior.

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌──────────┐
│ AWS SDK App │ ──► │ Quota Simulator │ ──► │  Floci   │
│             │     │   (Proxy :4567) │     │ (:4566)  │
└─────────────┘     └─────────────────┘     └──────────┘
```

- Applications point to `http://localhost:4567` (Quota Simulator)
- Simulator proxies to `http://localhost:4566` (Floci)
- Before forwarding, Simulator detects service and operation via `X-Amz-Target` header
- If quota exceeded, returns AWS-compatible error without forwarding to Floci

## Supported Services

1. **SQS** - FIFO queue TPS limits (300 TPS send/receive/delete, 3000 TPS with batching)
2. **DynamoDB** - Throughput limits (provisioned and on-demand modes)

## Detection Logic

- **Service detection**: Via Host header (`localhost:4567` → SQS) or query parameters
- **Operation detection**: Via `X-Amz-Target` HTTP header (e.g., `SQS.SendMessage`, `DynamoDB.PutItem`)
- **Resource detection**: Via `QueueUrl` (SQS) or `TableName` (DynamoDB) in request body

## Configuration

- `configs/quotas.yaml` - Default quota values
- Environment variables override YAML: `QUOTA_{SERVICE}_{LIMIT}` (e.g., `QUOTA_SQS_FIFO_TPS`)

## AWS Error Format

When quota exceeded, return AWS-compatible errors:

| Service | HTTP | Error Code | Exception |
|---------|------|------------|-----------|
| SQS | 400 | RequestThrottled | RequestThrottled |
| DynamoDB | 400 | ProvisionedThroughputExceededException | ProvisionedThroughputExceededException |

Response format (AWS wire format):
```json
HTTP/1.1 400 Bad Request
Content-Type: application/x-amz-json-1.0

{"__type":"com.amazonaws.dynamodb.v#ProvisionedThroughputExceededException","message":"Rate exceeded","retryable":true}
```

## Project Structure

```
aws-quota-simulator/
├── cmd/server/main.go              # Entry point
├── internal/
│   ├── proxy/handler.go            # HTTP reverse proxy with quota checks
│   │                                # Detects service via Host header + X-Amz-Target
│   ├── quota/
│   │   ├── manager.go              # Central quota controller + TokenBucket
│   │   └── services/
│   │       ├── sqs.go              # SQS quota implementation + DetectOperationFromAction
│   │       └── dynamodb.go         # DynamoDB quota implementation
│   ├── config/config.go            # Config loading (YAML + env vars)
│   └── errors/aws_errors.go        # AWS-formatted error responses
├── configs/
│   └── quotas.yaml                 # Default quotas
├── docker-compose.yml
├── Dockerfile
├── run.sh                          # Build and up script
├── go.mod
└── README.md
```

## Implementation Guidelines

1. **Extensibility**: Use interface `QuotaService` with `DetectOperationFromAction()` and `CheckLimit()` methods
2. **Rate Limiting**: Token bucket algorithm per resource (queue/table)
3. **HTTP Proxy**: Go `httputil.ReverseProxy` with error handler and custom transport
4. **Operation Detection**: Via `X-Amz-Target` header (AWS SDK v2 standard)
5. **Testing**: Unit tests for all quota logic, rate limiting, error generation

## Run Commands

```bash
# Development
./run.sh

# Or manually:
docker compose down && docker compose build && docker compose up -d

# Development locally
go run cmd/server/main.go

# Tests
go test ./...
go test -v ./internal/quota/services/...

# Build
go build -o quota-simulator ./cmd/server/main.go
```

## Docker

```bash
docker compose up -d
```

## Key Design Decisions

1. **Go**: Chosen for performance and compatibility with Floci (also Java/Quarkus)
2. **Token Bucket**: For rate limiting implementation
3. **YAML + Env Vars**: Static config with runtime overrides
4. **AWS-compatible errors**: SDK should handle these the same as real AWS
5. **X-Amz-Target header**: Standard way AWS SDK v2 specifies operations