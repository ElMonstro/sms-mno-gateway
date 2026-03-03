# emalify-sms-mno-gateway

A unified SMS gateway service that routes messages to Mobile Network Operators (MNOs) in Kenya and internationally. Built with Go using hexagonal architecture for maintainability and testability.

## Overview

This service consolidates two legacy services (`ApiGateway` and `sendtomnohandler`) into a single, well-architected gateway that:

- Consumes SMS messages from configurable RabbitMQ queues
- Routes messages to the appropriate MNO based on network and message type
- Handles retries, circuit breaking, and rate limiting
- Publishes results to downstream queues for persistence

```mermaid
graph LR
    subgraph Input
        Q1[TITANIC-KE_SMS_QUEUE]
        Q2[CONSUME_TO_MNO]
        Q3[SMS_MNO_GATEWAY_QUEUE]
    end

    subgraph Gateway
        GW[emalify-sms-mno-gateway]
    end

    subgraph MNOs
        SAF[Safaricom]
        AIR[Airtel]
        TEL[Telkom]
        EQU[Equitel]
        CM[CM International]
    end

    subgraph Output
        SAVE[SAVE_TO_DB]
        RETRY[SMS_RETRY_QUEUE]
        DLQ[SMS_DEAD_LETTER_QUEUE]
    end

    Q1 --> GW
    Q2 --> GW
    Q3 --> GW
    GW --> SAF
    GW --> AIR
    GW --> TEL
    GW --> EQU
    GW --> CM
    GW --> SAVE
    GW --> RETRY
    GW --> DLQ
```

## Features

- **Multi-MNO Support**: Safaricom (SDP + SMPP), Airtel, Telkom, Equitel, CM International
- **Transactional Routing**: Automatic SMPP routing for Safaricom transactional messages
- **Priority Routing**: Credit-Based Weighted Round Robin with transactional fast-path (EM-155)
- **Resilience**: Per-MNO circuit breakers, rate limiting, retry logic
- **Observability**: Prometheus metrics, structured logging, health endpoints
- **Reliability**: Manual queue acknowledgment ensures no message loss

## Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- Redis
- RabbitMQ

### Running Locally

```bash
# Start dependencies
docker-compose up -d redis rabbitmq

# Set environment variables (see .env.example)
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
export REDIS_HOST=localhost

# Run the service
go run cmd/gateway/main.go
```

### Using Docker

```bash
# Build and run everything
docker-compose up -d
```

### Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Key environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `INPUT_QUEUES` | `TITANIC-KE_SMS_QUEUE,CONSUME_TO_MNO` | Comma-separated list of queues to consume |
| `REDIS_HOST` | `localhost` | Redis host |
| `WORKER_COUNT` | `10` | Concurrent workers |
| `LOG_LEVEL` | `info` | Log verbosity |
| `PRIORITY_ROUTING_ENABLED` | `false` | Enable Credit-Based WRR priority routing |

See [docs/usage.md](docs/usage.md#configuration-reference) for complete configuration reference.

## Architecture

The service follows hexagonal (ports and adapters) architecture:

```mermaid
graph TB
    subgraph "Driving Adapters"
        HTTP[HTTP API]
        CONSUMER[RabbitMQ Consumer]
    end

    subgraph "Application Core"
        PORTS[Port Interfaces]
        DOMAIN[Domain Models]
        SERVICES[Services]
    end

    subgraph "Driven Adapters"
        MNO[MNO Adapters]
        RABBIT[RabbitMQ Publisher]
        REDIS[Redis Cache]
        METRICS[Prometheus]
    end

    HTTP --> PORTS
    CONSUMER --> PORTS
    PORTS --> DOMAIN
    PORTS --> SERVICES
    SERVICES --> MNO
    SERVICES --> RABBIT
    SERVICES --> REDIS
    SERVICES --> METRICS
```

### Message Flow

```mermaid
sequenceDiagram
    participant Q as RabbitMQ
    participant P as Processor
    participant R as Router
    participant M as MNO
    participant O as Output Queues

    Q->>P: Consume batch
    loop Each message
        P->>P: Validate & normalize MSISDN
        P->>R: Get appropriate sender
        R->>M: Send to MNO
        M-->>P: Result (success/retry/fail)
    end
    P->>O: Publish results
    P->>Q: Acknowledge batch
```

### MNO Routing

| Network | Condition | Route |
|---------|-----------|-------|
| SAFARICOM | `packageId != TRANSACTIONAL` | Safaricom SDP (REST API) |
| SAFARICOM | `packageId == TRANSACTIONAL` | Safaricom SMPP (Kannel) |
| AIRTEL | Any | Airtel SMPP |
| TELKOM | Any | Telkom SMPP |
| EQUITEL | Any | Equitel SMPP |
| CM/INTNL | Any | CM International SMPP |

For detailed architecture documentation, see [docs/architecture.md](docs/architecture.md).

## Development

### Project Structure

```
├── cmd/gateway/          # Application entry point
├── internal/
│   ├── api/              # HTTP handlers
│   ├── bootstrap/        # Dependency injection
│   ├── common/           # Shared utilities
│   │   ├── circuitbreaker/
│   │   ├── httpclient/
│   │   ├── logger/
│   │   ├── metrics/
│   │   └── ratelimit/
│   ├── config/           # Configuration
│   └── sms/
│       ├── domain/       # Domain models
│       ├── ports/        # Interface definitions
│       ├── service/      # Business logic
│       ├── adapters/     # External integrations
│       │   ├── mno/      # MNO senders
│       │   ├── rabbitmq/ # Queue adapters
│       │   └── redis/    # Cache adapter
│       └── mocks/        # Test mocks
├── docs/                 # Documentation
└── tests/                # Test files
```

### Building

```bash
# Build binary
go build -o bin/gateway cmd/gateway/main.go

# Build with version info
go build -ldflags "-X main.version=1.0.0" -o bin/gateway cmd/gateway/main.go
```

### Testing

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run with coverage
go test -cover ./...

# Run specific package tests
go test -v ./internal/sms/domain/...
go test -v ./internal/sms/service/...
go test -v ./internal/sms/adapters/mno/...
```

**Test Coverage:**
- Domain layer: 50+ tests (message, network, errors, results)
- Service layer: 14 tests (processor, router, result handler)
- MNO adapters: 30 tests (HTTP mocking, token caching, routing)
- Queue adapters: 9 tests (message parsing, result routing)
- Redis adapter: 4 tests (interface compliance, configuration)

### Code Quality

```bash
# Format code
go fmt ./...

# Lint
golangci-lint run

# Vet
go vet ./...
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check with component status |
| `/ready` | GET | Readiness check for Kubernetes |
| `/metrics` | GET | Prometheus metrics |

## Monitoring

### Prometheus Metrics

Key metrics exposed:

- `sms_messages_processed_total{network, status}` - Message counts
- `sms_send_latency_seconds{network}` - Send latency histogram
- `sms_circuit_breaker_state{network}` - Circuit breaker status
- `sms_retries_total{network}` - Retry counts
- `sms_dead_letters_total{network}` - Dead letter counts
- `sms_priority_messages_routed_total{type, queue}` - Priority routing counts
- `sms_priority_scheduler_weight{queue}` - Current queue weights

### Health Checks

```bash
# Health check
curl http://localhost:8080/health

# Readiness check
curl http://localhost:8080/ready
```

## Deployment

### Docker

```bash
# Build image
docker build -t emalify-sms-mno-gateway .

# Run container
docker run -d \
  -e RABBITMQ_URL=amqp://user:pass@rabbitmq:5672/ \
  -e REDIS_HOST=redis \
  -p 8080:8080 \
  emalify-sms-mno-gateway
```

### Kubernetes

The service exposes `/health` and `/ready` endpoints for liveness and readiness probes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Graceful Shutdown

The service handles SIGINT/SIGTERM signals gracefully:

1. Stops accepting new messages
2. Waits for in-flight processing (30s timeout)
3. Publishes pending results
4. Acknowledges processed deliveries
5. Closes connections cleanly

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | Detailed architecture with 14 Mermaid diagrams |
| [Usage](docs/usage.md) | Complete usage guide, queue configs, MNO details |

## Issues Addressed

This service addresses several issues from the legacy systems:

| Issue | Description | Solution |
|-------|-------------|----------|
| EM-143 | Messages lost due to auto-ack | Manual acknowledgment after processing |
| EM-139 | HTTP response body leaks | Proper `defer resp.Body.Close()` |
| EM-141 | Connection exhaustion | Shared HTTP client with pooling |
| EM-145 | Silent Redis failures | Proper error propagation |
| EM-147 | No observability | Prometheus metrics |
| EM-148 | Cascading MNO failures | Per-MNO circuit breakers |
| EM-149 | Permanent failures retried | Proper DLQ routing |
| EM-155 | No message prioritization | Credit-Based WRR scheduler with transactional fast-path |

## License

Proprietary - Emalify

## Contributing

1. Create a feature branch
2. Make changes with tests
3. Run `go test ./...` and `go vet ./...`
4. Submit a pull request
