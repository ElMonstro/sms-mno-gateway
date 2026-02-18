# Architecture Documentation

## Overview

`emalify-sms-mno-gateway` is a unified SMS gateway service that routes messages to various Mobile Network Operators (MNOs) in Kenya and internationally. It replaces two legacy services (`ApiGateway` and `sendtomnohandler`) with a single, well-architected service following hexagonal (ports and adapters) architecture.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Upstream Services"
        RQH[requesttoqueuehandler<br/>HTTP API :9090]
        BQH[batchingquehandler]
        OTHER[Other Systems]
    end

    subgraph "Message Queues"
        TQ[TITANIC-KE_SMS_QUEUE]
        CMQ[CONSUME_TO_MNO]
    end

    subgraph "emalify-sms-mno-gateway"
        CONSUMER[Queue Consumer]
        PROCESSOR[Message Processor]
        ROUTER[Message Router]

        subgraph "MNO Adapters"
            SDP[Safaricom SDP]
            SMPP_SAF[Safaricom SMPP]
            SMPP_AIR[Airtel SMPP]
            SMPP_TEL[Telkom SMPP]
            SMPP_EQU[Equitel SMPP]
            SMPP_CM[CM International SMPP]
        end

        RESULT[Result Handler]
        PUBLISHER[Queue Publisher]
    end

    subgraph "Output Queues"
        SAVE[SAVE_TO_DB]
        RETRY[SMS_RETRY_QUEUE]
        DLQ[SMS_DEAD_LETTER_QUEUE]
    end

    subgraph "Infrastructure"
        REDIS[(Redis<br/>Token Cache)]
        RABBITMQ[(RabbitMQ)]
    end

    subgraph "External MNOs"
        SAF_API[Safaricom API]
        KANNEL[Kannel SMPP Gateway]
    end

    RQH --> TQ
    BQH --> CMQ
    OTHER --> TQ
    OTHER --> CMQ

    TQ --> CONSUMER
    CMQ --> CONSUMER

    CONSUMER --> PROCESSOR
    PROCESSOR --> ROUTER

    ROUTER --> SDP
    ROUTER --> SMPP_SAF
    ROUTER --> SMPP_AIR
    ROUTER --> SMPP_TEL
    ROUTER --> SMPP_EQU
    ROUTER --> SMPP_CM

    SDP --> REDIS
    SDP --> SAF_API
    SMPP_SAF --> KANNEL
    SMPP_AIR --> KANNEL
    SMPP_TEL --> KANNEL
    SMPP_EQU --> KANNEL
    SMPP_CM --> KANNEL

    SDP --> RESULT
    SMPP_SAF --> RESULT
    SMPP_AIR --> RESULT
    SMPP_TEL --> RESULT
    SMPP_EQU --> RESULT
    SMPP_CM --> RESULT

    RESULT --> PUBLISHER
    PUBLISHER --> SAVE
    PUBLISHER --> RETRY
    PUBLISHER --> DLQ

    CONSUMER -.-> RABBITMQ
    PUBLISHER -.-> RABBITMQ
```

## Hexagonal Architecture

The service follows hexagonal (ports and adapters) architecture to maintain clean separation between business logic and external dependencies.

```mermaid
graph TB
    subgraph "Driving Adapters (Primary)"
        HTTP[HTTP API<br/>/health, /metrics]
        CONSUMER[RabbitMQ Consumer]
    end

    subgraph "Application Core"
        subgraph "Ports (Interfaces)"
            QC[QueueConsumer]
            QP[QueuePublisher]
            MNO[MNOSender]
            TC[TokenCache]
            MET[Metrics]
        end

        subgraph "Domain"
            MSG[Message]
            RES[SendResult]
            NET[Network]
        end

        subgraph "Services"
            PROC[Processor]
            ROUTE[Router]
            RH[ResultHandler]
        end
    end

    subgraph "Driven Adapters (Secondary)"
        RMQ_CONS[RabbitMQ Consumer Adapter]
        RMQ_PUB[RabbitMQ Publisher Adapter]
        REDIS_TC[Redis Token Cache]
        MNO_SDP[Safaricom SDP Adapter]
        MNO_SMPP[SMPP Adapters]
        PROM[Prometheus Metrics]
    end

    HTTP --> PROC
    CONSUMER --> QC

    QC --> RMQ_CONS
    QP --> RMQ_PUB
    MNO --> MNO_SDP
    MNO --> MNO_SMPP
    TC --> REDIS_TC
    MET --> PROM

    PROC --> ROUTE
    ROUTE --> MNO
    PROC --> RH
    RH --> QP
```

## Directory Structure

```
emalify-sms-mno-gateway/
├── cmd/gateway/
│   └── main.go                 # Application entry point
├── internal/
│   ├── api/
│   │   ├── server.go           # HTTP server setup
│   │   └── handlers/           # HTTP handlers
│   │       ├── health.go       # Health check endpoints
│   │       └── metrics.go      # Prometheus metrics endpoint
│   ├── bootstrap/
│   │   ├── app.go              # Dependency injection & wiring
│   │   └── shutdown.go         # Graceful shutdown handling
│   ├── common/
│   │   ├── circuitbreaker/     # Circuit breaker implementation
│   │   ├── httpclient/         # Pooled HTTP client
│   │   ├── logger/             # Structured logging
│   │   ├── metrics/            # Prometheus metrics
│   │   └── ratelimit/          # Per-network rate limiter
│   ├── config/
│   │   └── config.go           # Configuration loading
│   └── sms/
│       ├── domain/             # Domain entities
│       │   ├── message.go      # Message entity
│       │   ├── result.go       # SendResult & BatchResult
│       │   ├── network.go      # Network enum
│       │   └── errors.go       # Domain errors
│       ├── ports/              # Port interfaces
│       │   ├── mno_sender.go   # MNO sender interface
│       │   ├── queue_*.go      # Queue interfaces
│       │   ├── token_cache.go  # Token cache interface
│       │   └── metrics.go      # Metrics interface
│       ├── service/            # Application services
│       │   ├── processor.go    # Message processor
│       │   ├── router.go       # Message router
│       │   └── result_handler.go
│       ├── adapters/           # Adapter implementations
│       │   ├── mno/            # MNO adapters
│       │   ├── rabbitmq/       # RabbitMQ adapters
│       │   └── redis/          # Redis adapters
│       └── mocks/              # Test mocks
├── docs/                       # Documentation
└── tests/                      # Test files
```

## Message Processing Flow

```mermaid
sequenceDiagram
    participant Q as RabbitMQ Queue
    participant C as Consumer
    participant P as Processor
    participant R as Router
    participant RL as Rate Limiter
    participant CB as Circuit Breaker
    participant M as MNO Sender
    participant RH as Result Handler
    participant PUB as Publisher

    Q->>C: Deliver batch [msg1, msg2, ...]
    Note over C: auto-ack = FALSE (EM-143)

    C->>P: ProcessDelivery(delivery)

    loop For each message (via worker pool)
        P->>P: Normalize MSISDN (0xxx → 254xxx)
        P->>P: Validate message

        alt Validation failed
            P->>RH: Permanent failure
        else Validation passed
            P->>RL: Wait for rate limit token

            alt Rate limited
                P->>RH: Retryable failure
            else Allowed
                P->>R: GetSender(msg)
                R->>R: Check transactional routing
                R-->>P: Return appropriate sender

                P->>CB: Check circuit breaker

                alt Circuit open
                    P->>RH: Retryable failure
                else Circuit closed/half-open
                    P->>M: Send(ctx, msg)
                    M-->>P: SendResult
                    P->>RH: Handle result
                end
            end
        end
    end

    RH->>PUB: PublishBatchResults
    PUB->>Q: Publish to SAVE_TO_DB
    PUB->>Q: Publish to RETRY_QUEUE
    PUB->>Q: Publish to DLQ

    P->>C: Processing complete
    C->>Q: Acknowledge delivery
```

## Transactional Routing Flow

Safaricom messages can be either promotional (via SDP API) or transactional (via SMPP). The `packageId` field determines the routing:

```mermaid
flowchart TD
    START[Incoming Message] --> CHECK_NET{Network?}

    CHECK_NET -->|SAFARICOM| CHECK_TX{packageId == TRANSACTIONAL?}
    CHECK_NET -->|AIRTEL| AIRTEL[Airtel SMPP]
    CHECK_NET -->|TELKOM| TELKOM[Telkom SMPP]
    CHECK_NET -->|EQUITEL| EQUITEL[Equitel SMPP]
    CHECK_NET -->|CM/INTNL| CM[CM International SMPP]
    CHECK_NET -->|Unknown| ERROR[Error: Unknown Network]

    CHECK_TX -->|Yes| SAF_SMPP[Safaricom SMPP<br/>via Kannel]
    CHECK_TX -->|No| SAF_SDP[Safaricom SDP<br/>via REST API]

    SAF_SDP --> RESULT[Send Result]
    SAF_SMPP --> RESULT
    AIRTEL --> RESULT
    TELKOM --> RESULT
    EQUITEL --> RESULT
    CM --> RESULT
```

## Result Routing Flow

```mermaid
flowchart TD
    RESULT[Send Result] --> CHECK{Result Type?}

    CHECK -->|SUCCESS| SUCCESS[Status: SENT]
    CHECK -->|RETRYABLE| RETRY_CHECK{retryCount >= maxRetries?}
    CHECK -->|PERMANENT| PERM[Status: FAILED]

    SUCCESS --> SAVE_DB[SAVE_TO_DB Queue]

    RETRY_CHECK -->|No| RETRY[SMS_RETRY_QUEUE]
    RETRY_CHECK -->|Yes| DLQ[SMS_DEAD_LETTER_QUEUE]

    PERM --> DLQ

    SAVE_DB --> DOWNSTREAM[Downstream Processors]
    RETRY --> GATEWAY[Back to Gateway]
    DLQ --> INVESTIGATION[Manual Investigation]
```

## Circuit Breaker State Machine

Each MNO has its own circuit breaker to prevent cascading failures:

```mermaid
stateDiagram-v2
    [*] --> Closed

    Closed --> Open: Consecutive failures >= threshold
    Closed --> Closed: Success / Failure < threshold

    Open --> HalfOpen: Timeout elapsed
    Open --> Open: Requests rejected

    HalfOpen --> Closed: Success
    HalfOpen --> Open: Failure
```

**Configuration:**
- Consecutive failures threshold: 5
- Open state timeout: 30 seconds
- Half-open max requests: 3

## Rate Limiting

Per-network rate limiting protects MNO APIs from being overwhelmed:

```mermaid
graph LR
    subgraph "Rate Limiters"
        SAF[Safaricom<br/>200 req/s]
        AIR[Airtel<br/>50 req/s]
        TEL[Telkom<br/>100 req/s]
        EQU[Equitel<br/>20 req/s]
        CM[CM<br/>20 req/s]
    end

    MSG[Message] --> ROUTER[Router]
    ROUTER --> SAF
    ROUTER --> AIR
    ROUTER --> TEL
    ROUTER --> EQU
    ROUTER --> CM
```

## Token Caching (Safaricom SDP)

The SDP API requires OAuth-style authentication. Tokens are cached in Redis:

```mermaid
sequenceDiagram
    participant S as SDP Sender
    participant C as Redis Cache
    participant A as Safaricom Auth API

    S->>C: Get cached token

    alt Token exists and valid
        C-->>S: Return token
    else Token missing or expired
        S->>A: Request new token
        A-->>S: Token + expires_in
        S->>C: Cache token (TTL: 25 min)
    end

    S->>S: Use token for API call

    Note over S: On 401 response:
    S->>C: Delete invalid token
    S->>S: Mark as retryable
```

## Worker Pool Architecture

```mermaid
graph TB
    subgraph "Message Batch"
        M1[msg1]
        M2[msg2]
        M3[msg3]
        M4[msg4]
        M5[msg5]
    end

    subgraph "Worker Pool (configurable, default: 10)"
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker 3]
        WN[Worker N]
    end

    subgraph "Results"
        R1[result1]
        R2[result2]
        R3[result3]
        R4[result4]
        R5[result5]
    end

    M1 --> W1
    M2 --> W2
    M3 --> W3
    M4 --> W1
    M5 --> W2

    W1 --> R1
    W2 --> R2
    W3 --> R3
    W1 --> R4
    W2 --> R5
```

## Graceful Shutdown

```mermaid
sequenceDiagram
    participant OS as Operating System
    participant APP as Application
    participant CONS as Consumers
    participant PROC as Processors
    participant CONN as Connections

    OS->>APP: SIGINT/SIGTERM

    APP->>CONS: Stop accepting new messages

    Note over PROC: Wait for in-flight<br/>messages (30s timeout)

    PROC->>PROC: Complete processing
    PROC->>PROC: Publish all results
    PROC->>PROC: Acknowledge deliveries

    APP->>CONN: Close RabbitMQ
    APP->>CONN: Close Redis
    APP->>CONN: Close HTTP server

    APP->>OS: Exit 0
```

## Component Interactions

### Bootstrap Sequence

```mermaid
sequenceDiagram
    participant Main
    participant Config
    participant Redis
    participant RabbitMQ
    participant HTTP
    participant MNO
    participant Processor

    Main->>Config: Load configuration
    Main->>Redis: Connect (token cache)
    Main->>RabbitMQ: Connect
    Main->>HTTP: Initialize client (pooled)
    Main->>MNO: Initialize factory
    Main->>Processor: Initialize with dependencies
    Main->>RabbitMQ: Start consumers
    Main->>Main: Start HTTP server
    Main->>Main: Wait for shutdown signal
```

## Error Classification

| Error Type | HTTP Status | Result Type | Queue Destination |
|------------|-------------|-------------|-------------------|
| Network timeout | - | Retryable | RETRY_QUEUE |
| Connection refused | - | Retryable | RETRY_QUEUE |
| Rate limited (429) | 429 | Retryable | RETRY_QUEUE |
| Server error (5xx) | 5xx | Retryable | RETRY_QUEUE |
| Circuit breaker open | - | Retryable | RETRY_QUEUE |
| Bad request (400) | 400 | Permanent | DLQ |
| Unauthorized (401) | 401 | Retryable | RETRY_QUEUE |
| Not found (404) | 404 | Permanent | DLQ |
| Invalid MSISDN | - | Permanent | DLQ |
| Unknown network | - | Permanent | DLQ |
| Max retries exceeded | - | Permanent | DLQ |

## Metrics Exposed

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `sms_messages_processed_total` | Counter | network, status | Total messages processed |
| `sms_send_latency_seconds` | Histogram | network | Send latency distribution |
| `sms_queue_depth` | Gauge | queue | Current queue depth |
| `sms_circuit_breaker_state` | Gauge | network | Circuit breaker state (0=closed, 1=open) |
| `sms_circuit_breaker_trips_total` | Counter | network | Circuit breaker trip count |
| `sms_retries_total` | Counter | network | Total retry count |
| `sms_dead_letters_total` | Counter | network | Total dead letter count |
| `sms_rate_limit_hits_total` | Counter | network | Rate limit hit count |

## Issues Addressed

| Issue ID | Description | Solution |
|----------|-------------|----------|
| EM-143 | Auto-ack losing messages | Manual acknowledgment after processing |
| EM-139 | Response body leak | `defer resp.Body.Close()` in all HTTP calls |
| EM-141 | Connection exhaustion | Shared HTTP client with connection pooling |
| EM-145 | Swallowed Redis errors | Proper error propagation in TokenCache |
| EM-147 | No observability | Prometheus metrics implementation |
| EM-148 | Cascading failures | Per-MNO circuit breakers |
| EM-149 | Permanent failures retried | Proper DLQ routing for permanent failures |
