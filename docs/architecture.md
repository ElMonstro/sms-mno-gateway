# Architecture Documentation

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Complete Message Flow](#complete-message-flow)
4. [Priority Routing System](#priority-routing-system)
5. [Component Details](#component-details)
6. [Error Handling & Edge Cases](#error-handling--edge-cases)
7. [Resilience Patterns](#resilience-patterns)
8. [Data Flow & State](#data-flow--state)
9. [Configuration Reference](#configuration-reference)
10. [Metrics & Observability](#metrics--observability)

---

## Overview

`emalify-sms-mno-gateway` is a unified SMS gateway service that routes messages to various Mobile Network Operators (MNOs) in Kenya and internationally. It replaces two legacy services (`ApiGateway` and `sendtomnohandler`) with a single, well-architected service following hexagonal (ports and adapters) architecture.

### Key Features

- **Multi-MNO Support**: Safaricom (SDP + SMPP), Airtel, Telkom, Equitel, CM International
- **Priority Routing**: Weighted Fair Queuing for promotional, fast-path for transactional
- **Resilience**: Circuit breakers, rate limiting, retry logic
- **Observability**: Prometheus metrics, structured logging
- **Hot-Reload**: Queue weights configurable at runtime via Redis

### Issues Addressed

| Issue | Problem | Solution |
|-------|---------|----------|
| EM-143 | Auto-ack losing messages | Manual acknowledgment after processing |
| EM-139 | Response body leak | `defer resp.Body.Close()` in all HTTP calls |
| EM-141 | Connection exhaustion | Shared HTTP client with connection pooling |
| EM-145 | Swallowed Redis errors | Proper error propagation |
| EM-147 | No observability | Prometheus metrics |
| EM-148 | Cascading failures | Per-MNO circuit breakers |
| EM-149 | Permanent failures retried | Proper DLQ routing |
| EM-155 | No message prioritization | WFQ scheduler with transactional fast-path |

---

## System Architecture

### High-Level System Diagram

```mermaid
graph TB
    subgraph "Upstream Services"
        RQH[requesttoqueuehandler<br/>HTTP API :9090]
        BQH[batchingquehandler<br/>Blue-Green Router]
        API[emalify-api-v2<br/>PHP API]
    end

    subgraph "RabbitMQ Input Queues"
        TQ[TITANIC-KE_SMS_QUEUE<br/>Direct traffic]
        CMQ[CONSUME_TO_MNO<br/>Batched Gold partners]
        GQ[SMS_MNO_GATEWAY_QUEUE<br/>Green lane testing]
    end

    subgraph "emalify-sms-mno-gateway"
        subgraph "Consumers Layer"
            C1[Consumer 1]
            C2[Consumer 2]
            C3[Consumer N]
        end

        subgraph "Routing Layer"
            MR[Message Router<br/>Separates TX/Promo]
        end

        subgraph "Processing Layer"
            TH[Transactional Handler<br/>Fast-Path Workers]
            PS[Priority Scheduler<br/>WFQ Algorithm]
            PROC[Processor<br/>Worker Pool]
        end

        subgraph "MNO Adapters"
            SDP[Safaricom SDP<br/>REST API]
            SMPP_SAF[Safaricom SMPP<br/>Kannel]
            SMPP_AIR[Airtel SMPP]
            SMPP_TEL[Telkom SMPP]
            SMPP_EQU[Equitel SMPP]
            SMPP_CM[CM International]
        end

        subgraph "Result Handling"
            RH[Result Handler]
            PUB[Publisher]
        end
    end

    subgraph "RabbitMQ Output Queues"
        SAVE[SAVE_TO_DB<br/>Success]
        RETRY[SMS_RETRY_QUEUE<br/>Retryable]
        DLQ[SMS_DEAD_LETTER_QUEUE<br/>Permanent failures]
    end

    subgraph "External Infrastructure"
        REDIS[(Redis<br/>Token Cache +<br/>Priority Weights)]
        SAF_API[Safaricom API<br/>dsvc2.safaricom.com]
        KANNEL[Kannel SMPP<br/>Gateway]
    end

    RQH --> TQ
    BQH -->|Blue| CMQ
    BQH -->|Green| GQ
    API --> TQ

    TQ --> C1
    CMQ --> C2
    GQ --> C3

    C1 --> MR
    C2 --> MR
    C3 --> MR

    MR -->|packageId=TRANSACTIONAL| TH
    MR -->|Other| PS

    TH --> PROC
    PS --> PROC

    PROC --> SDP
    PROC --> SMPP_SAF
    PROC --> SMPP_AIR
    PROC --> SMPP_TEL
    PROC --> SMPP_EQU
    PROC --> SMPP_CM

    SDP --> REDIS
    SDP --> SAF_API
    SMPP_SAF --> KANNEL
    SMPP_AIR --> KANNEL
    SMPP_TEL --> KANNEL
    SMPP_EQU --> KANNEL
    SMPP_CM --> KANNEL

    SDP --> RH
    SMPP_SAF --> RH
    SMPP_AIR --> RH
    SMPP_TEL --> RH
    SMPP_EQU --> RH
    SMPP_CM --> RH

    RH --> PUB
    PUB --> SAVE
    PUB --> RETRY
    PUB --> DLQ

    PS -.->|Hot-reload weights| REDIS
```

### Hexagonal Architecture

```mermaid
graph TB
    subgraph "Driving Adapters (Primary)"
        HTTP[HTTP Server<br/>/health, /metrics]
        CONSUMER[RabbitMQ Consumers<br/>One per queue]
    end

    subgraph "Application Core"
        subgraph "Ports (Interfaces)"
            QC[QueueConsumer]
            QP[QueuePublisher]
            MNO[MNOSender]
            TC[TokenCache]
            PS_PORT[PriorityStore]
            MET[Metrics]
        end

        subgraph "Domain"
            MSG[Message]
            RES[SendResult]
            BR[BatchResult]
            NET[Network]
        end

        subgraph "Services"
            MSG_ROUTER[MessageRouter]
            TX_HANDLER[TransactionalHandler]
            PRI_SCHED[PriorityScheduler]
            PROCESSOR[Processor]
            ROUTER[Router]
            RESULT_H[ResultHandler]
        end
    end

    subgraph "Driven Adapters (Secondary)"
        RMQ_CONS[RabbitMQ Consumer]
        RMQ_PUB[RabbitMQ Publisher]
        REDIS_TC[Redis TokenCache]
        REDIS_PS[Redis PriorityStore]
        MNO_SDP[Safaricom SDP]
        MNO_SMPP[SMPP Senders]
        PROM[Prometheus]
    end

    HTTP --> MSG_ROUTER
    CONSUMER --> QC

    QC --> RMQ_CONS
    QP --> RMQ_PUB
    MNO --> MNO_SDP
    MNO --> MNO_SMPP
    TC --> REDIS_TC
    PS_PORT --> REDIS_PS
    MET --> PROM

    MSG_ROUTER --> TX_HANDLER
    MSG_ROUTER --> PRI_SCHED
    TX_HANDLER --> PROCESSOR
    PRI_SCHED --> PROCESSOR
    PROCESSOR --> ROUTER
    ROUTER --> MNO
    PROCESSOR --> RESULT_H
    RESULT_H --> QP
```

---

## Complete Message Flow

### Master Flow Diagram

This diagram shows the complete journey of a message through the system, including all decision points, error handling, and edge cases.

```mermaid
flowchart TD
    START([Message arrives<br/>from RabbitMQ]) --> PARSE{Parse JSON<br/>batch}

    PARSE -->|Parse error| NACK_PARSE[Nack & Requeue]
    PARSE -->|Success| EMPTY_CHECK{Batch<br/>empty?}

    EMPTY_CHECK -->|Yes| ACK_EMPTY[Ack empty delivery]
    EMPTY_CHECK -->|No| PRIORITY_CHECK{Priority<br/>enabled?<br/>PRIORITY_ENABLED}

    PRIORITY_CHECK -->|No - Legacy mode| DIRECT_PROC[Process via<br/>Processor directly<br/>No TX/Promo separation]
    PRIORITY_CHECK -->|Yes| MSG_ROUTER[Message Router]

    MSG_ROUTER --> CLASSIFY{Classify by<br/>packageId}

    CLASSIFY -->|packageId = TRANSACTIONAL| TX_MSGS[Transactional<br/>messages]
    CLASSIFY -->|Other packageId| PROMO_MSGS[Promotional<br/>messages]

    TX_MSGS --> TX_CHECK{Has TX<br/>Handler?}
    TX_CHECK -->|Yes - Normal path| TX_BATCH[TransactionalHandler<br/>.ProcessBatch]
    TX_CHECK -.->|No - Fallback| TX_FALLBACK[FALLBACK: Process via<br/>Processor directly]

    PROMO_MSGS --> SCHED_CHECK{Has<br/>Scheduler?}
    SCHED_CHECK -->|Yes - Normal path| WFQ[PriorityScheduler<br/>.ProcessMessages]
    SCHED_CHECK -.->|No - Fallback| PROMO_FALLBACK[FALLBACK: Process via<br/>Processor directly<br/>No WFQ applied]

    TX_BATCH --> TX_WORKERS[Dedicated TX Workers<br/>Concurrent Processing]
    TX_FALLBACK -.-> PROC_POOL
    WFQ --> WFQ_WAIT[Wait for WFQ slot<br/>based on queue weight]
    WFQ_WAIT --> PROC_POOL
    PROMO_FALLBACK -.-> PROC_POOL

    subgraph "Message Processing"
        TX_WORKERS --> NORMALIZE[Normalize MSISDN<br/>0xxx → 254xxx]
        PROC_POOL[Processor<br/>Worker Pool] --> NORMALIZE

        NORMALIZE --> VALIDATE{Validate<br/>message}

        VALIDATE -->|Missing correlator| PERM_FAIL_VAL[Permanent Failure]
        VALIDATE -->|Missing content| PERM_FAIL_VAL
        VALIDATE -->|Missing MSISDN| PERM_FAIL_VAL
        VALIDATE -->|Missing sender| PERM_FAIL_VAL
        VALIDATE -->|Unknown network| PERM_FAIL_VAL
        VALIDATE -->|Valid| RATE_LIMIT[Rate Limiter<br/>Wait for token]

        RATE_LIMIT -->|Context cancelled| RETRY_RL[Retryable Failure]
        RATE_LIMIT -->|Timeout| RETRY_RL
        RATE_LIMIT -->|Allowed| GET_SENDER[Get MNO Sender]

        GET_SENDER --> ROUTE_DECISION{Network +<br/>packageId?}

        ROUTE_DECISION -->|SAFARICOM + TRANSACTIONAL| SAF_SMPP[Safaricom SMPP]
        ROUTE_DECISION -->|SAFARICOM + other| SAF_SDP[Safaricom SDP]
        ROUTE_DECISION -->|AIRTEL| AIRTEL[Airtel SMPP]
        ROUTE_DECISION -->|TELKOM| TELKOM[Telkom SMPP]
        ROUTE_DECISION -->|EQUITEL| EQUITEL[Equitel SMPP]
        ROUTE_DECISION -->|CM or INTNL| CM[CM SMPP]
        ROUTE_DECISION -->|Unknown| PERM_FAIL_NET[Permanent Failure]

        SAF_SDP --> CB_CHECK{Circuit<br/>Breaker?}
        SAF_SMPP --> CB_CHECK
        AIRTEL --> CB_CHECK
        TELKOM --> CB_CHECK
        EQUITEL --> CB_CHECK
        CM --> CB_CHECK

        CB_CHECK -->|Open| RETRY_CB[Retryable Failure]
        CB_CHECK -->|Closed/Half-open| SEND[Send to MNO]

        SEND --> SEND_RESULT{Send<br/>Result?}

        SEND_RESULT -->|Success 2xx| SUCCESS[Success]
        SEND_RESULT -->|Timeout| RETRY_TO[Retryable Failure]
        SEND_RESULT -->|Connection refused| RETRY_CONN[Retryable Failure]
        SEND_RESULT -->|429 Rate limited| RETRY_429[Retryable Failure]
        SEND_RESULT -->|401 Unauthorized| RETRY_401[Retryable Failure<br/>+ Clear token cache]
        SEND_RESULT -->|5xx Server error| RETRY_5XX[Retryable Failure]
        SEND_RESULT -->|400 Bad request| PERM_400[Permanent Failure]
        SEND_RESULT -->|404 Not found| PERM_404[Permanent Failure]
    end

    SUCCESS --> COLLECT_RESULTS
    PERM_FAIL_VAL --> COLLECT_RESULTS
    PERM_FAIL_NET --> COLLECT_RESULTS
    PERM_400 --> COLLECT_RESULTS
    PERM_404 --> COLLECT_RESULTS
    RETRY_RL --> COLLECT_RESULTS
    RETRY_CB --> COLLECT_RESULTS
    RETRY_TO --> COLLECT_RESULTS
    RETRY_CONN --> COLLECT_RESULTS
    RETRY_429 --> COLLECT_RESULTS
    RETRY_401 --> COLLECT_RESULTS
    RETRY_5XX --> COLLECT_RESULTS

    COLLECT_RESULTS[Collect BatchResult] --> RESULT_HANDLER[Result Handler<br/>Route to queues]

    RESULT_HANDLER --> ROUTE_RESULTS{Result<br/>Type?}

    ROUTE_RESULTS -->|Success| PUB_SAVE[Publish to<br/>SAVE_TO_DB]
    ROUTE_RESULTS -->|Retryable + retries < 10| PUB_RETRY[Publish to<br/>SMS_RETRY_QUEUE]
    ROUTE_RESULTS -->|Retryable + retries >= 10| PUB_DLQ[Publish to<br/>SMS_DEAD_LETTER_QUEUE]
    ROUTE_RESULTS -->|Permanent| PUB_DLQ

    PUB_SAVE --> PUB_CHECK{Publish<br/>success?}
    PUB_RETRY --> PUB_CHECK
    PUB_DLQ --> PUB_CHECK

    PUB_CHECK -->|Yes| ACK_DELIVERY[Ack Delivery]
    PUB_CHECK -->|No| NACK_DELIVERY[Nack & Requeue]

    ACK_DELIVERY --> END_SUCCESS([Message Processed])
    NACK_DELIVERY --> END_RETRY([Will be redelivered])
    ACK_EMPTY --> END_EMPTY([Empty batch handled])
    NACK_PARSE --> END_PARSE([Parse error handled])

    DIRECT_PROC --> NORMALIZE
```

**Diagram Legend:**
- **Solid lines (→)**: Normal/expected flow
- **Dotted lines (-.->)**: Fallback paths (error recovery, should not occur in normal operation)

**When do fallbacks occur?**

| Fallback | Condition | Why it might happen |
|----------|-----------|---------------------|
| TX Handler fallback | `transactionalHandler == nil` | Handler failed to initialize, or config error |
| Scheduler fallback | `scheduler == nil` | Scheduler failed to initialize, or config error |

When `PRIORITY_ENABLED=true`, both handler and scheduler are created during bootstrap. Fallbacks are defensive code for edge cases, not expected paths.

### Sequence Diagram: Standard Processing

```mermaid
sequenceDiagram
    autonumber
    participant Q as RabbitMQ
    participant C as Consumer
    participant MR as MessageRouter
    participant PS as PriorityScheduler
    participant P as Processor
    participant RL as RateLimiter
    participant R as Router
    participant CB as CircuitBreaker
    participant M as MNO Sender
    participant RH as ResultHandler
    participant PUB as Publisher

    Q->>C: Deliver batch [msg1, msg2, ...]
    Note over C: auto-ack = FALSE

    C->>MR: RouteDelivery(ctx, delivery, queueName)

    MR->>MR: Separate TX vs Promotional

    alt Has promotional messages
        MR->>PS: ProcessMessages(ctx, messages, queueName)
        PS->>PS: waitForSlot() - WFQ delay
        PS->>P: ProcessMessages(ctx, messages)
    else Only transactional
        MR->>P: ProcessMessages(ctx, messages)
    end

    loop For each message (worker pool)
        P->>P: Normalize MSISDN
        P->>P: Validate message

        alt Invalid
            P-->>RH: Permanent failure
        else Valid
            P->>RL: Wait(ctx, network)

            alt Rate limited timeout
                P-->>RH: Retryable failure
            else Allowed
                P->>R: GetSender(msg)
                R-->>P: MNO Sender

                P->>CB: Check state

                alt Circuit open
                    P-->>RH: Retryable failure
                else Circuit closed
                    P->>M: Send(ctx, msg)
                    M-->>P: SendResult

                    alt Success
                        P-->>RH: Success
                    else Retryable error
                        P-->>RH: Retryable failure
                    else Permanent error
                        P-->>RH: Permanent failure
                    end
                end
            end
        end
    end

    RH->>PUB: HandleBatchResults(ctx, batchResult)

    par Publish to appropriate queues
        PUB->>Q: Success → SAVE_TO_DB
        PUB->>Q: Retryable → RETRY_QUEUE
        PUB->>Q: Permanent → DLQ
    end

    PUB-->>RH: Publish complete
    RH-->>MR: Processing complete

    MR->>Q: Ack delivery

    Note over Q,MR: Message fully processed
```

---

## Priority Routing System

### Priority Routing Architecture

This diagram shows the **normal flow** when `PRIORITY_ENABLED=true` and all components initialized successfully.

```mermaid
flowchart TB
    subgraph "Input"
        DELIVERY[Incoming Delivery<br/>from RabbitMQ]
    end

    subgraph "Message Router"
        PARSE[Parse Messages]
        CLASSIFY{Classify by<br/>packageId}

        PARSE --> CLASSIFY
    end

    subgraph "Transactional Path - Fast, No WFQ"
        TX_HANDLER[TransactionalHandler]
        TX_BATCH[ProcessBatch<br/>Synchronous]
        TX_POOL[Dedicated Workers<br/>Default: 5]

        TX_HANDLER --> TX_BATCH
        TX_BATCH --> TX_POOL
    end

    subgraph "Promotional Path - WFQ Scheduled"
        SCHEDULER[PriorityScheduler]
        WFQ_CALC[Calculate Wait Time<br/>waitTime = baseWait / weight]
        STARVE_CHECK{Starvation<br/>Check}
        WAIT[Wait for Slot]
        PROCESS[ProcessMessages]

        SCHEDULER --> WFQ_CALC
        WFQ_CALC --> STARVE_CHECK
        STARVE_CHECK -->|Queue starving<br/>Skip wait| PROCESS
        STARVE_CHECK -->|OK| WAIT
        WAIT --> PROCESS
    end

    subgraph "Processor"
        WORKER_POOL[Worker Pool<br/>Default: 10 workers]
        MNO_SEND[Send to MNO]

        WORKER_POOL --> MNO_SEND
    end

    subgraph "Result Handling"
        RESULT[Collect Results]
        ACK[Ack Delivery<br/>After ALL messages processed]
    end

    DELIVERY --> PARSE
    CLASSIFY -->|packageId = TRANSACTIONAL| TX_HANDLER
    CLASSIFY -->|Other| SCHEDULER

    TX_POOL --> WORKER_POOL
    PROCESS --> WORKER_POOL

    MNO_SEND --> RESULT
    RESULT --> ACK
```

**Key Points:**
- Transactional messages **bypass WFQ entirely** - processed immediately
- Promotional messages **wait based on queue weight** before processing
- Delivery is **only acked after ALL messages** (both TX and promo) are processed
- Both paths ultimately use the same Processor worker pool for MNO sending

### WFQ Weight Calculation

```mermaid
flowchart LR
    subgraph "Queue Weights (Redis)"
        W1[GOLD_PARTNERS: 10]
        W2[BETIKA_GOLD: 10]
        W3[TITANIC-KE: 1]
        W4[CONSUME_TO_MNO: 1]
    end

    subgraph "Wait Time Calculation"
        FORMULA["waitTime = baseWait / weight<br/>baseWait = 10ms"]

        CALC1["Weight 10 → 1ms wait"]
        CALC2["Weight 5 → 2ms wait"]
        CALC3["Weight 1 → 10ms wait"]
    end

    subgraph "Processing Share"
        SHARE1["Weight 10 → ~10x throughput"]
        SHARE2["Weight 1 → baseline throughput"]
    end

    W1 --> FORMULA
    W2 --> FORMULA
    W3 --> FORMULA
    W4 --> FORMULA

    FORMULA --> CALC1
    FORMULA --> CALC2
    FORMULA --> CALC3

    CALC1 --> SHARE1
    CALC3 --> SHARE2
```

### Starvation Prevention

```mermaid
flowchart TD
    START[Message from<br/>low-priority queue] --> CHECK_TIME{Time since<br/>last processed?}

    CHECK_TIME -->|< maxStarvationTime| NORMAL_WAIT[Normal WFQ Wait<br/>baseWait / weight]
    CHECK_TIME -->|>= maxStarvationTime| SKIP_WAIT[Skip Wait<br/>Process Immediately]

    NORMAL_WAIT --> PROCESS[Process Message]
    SKIP_WAIT --> LOG[Log starvation<br/>trigger]
    LOG --> METRIC[Increment<br/>starvation_triggers metric]
    METRIC --> PROCESS

    PROCESS --> UPDATE[Update lastProcessed<br/>timestamp]

    NOTE1[/"maxStarvationTime = 1s / starvationRatio<br/>Default ratio 0.1 → 10 second max wait"/]
```

### Hot-Reload Weight Updates

```mermaid
sequenceDiagram
    participant ADMIN as Admin/Script
    participant REDIS as Redis
    participant WATCHER as Weight Watcher<br/>(goroutine)
    participant SCHED as PriorityScheduler

    Note over WATCHER: Started with PriorityScheduler.Start()

    WATCHER->>REDIS: SUBSCRIBE sms:priority:weights:notifications
    REDIS-->>WATCHER: Subscribed

    ADMIN->>REDIS: HSET sms:priority:weights GOLD_QUEUE 20
    REDIS-->>ADMIN: OK

    ADMIN->>REDIS: PUBLISH sms:priority:weights:notifications "updated"

    REDIS-->>WATCHER: Message: "updated"

    WATCHER->>REDIS: HGETALL sms:priority:weights
    REDIS-->>WATCHER: {GOLD_QUEUE: 20, ...}

    WATCHER->>SCHED: updateWeights(weights)
    SCHED->>SCHED: Update internal weights map
    SCHED->>SCHED: Update queue states
    SCHED->>SCHED: Emit metrics

    Note over SCHED: New weights active immediately<br/>No restart required
```

---

## Component Details

### Directory Structure

```
emalify-sms-mno-gateway/
├── cmd/gateway/
│   └── main.go                     # Entry point
├── internal/
│   ├── api/
│   │   ├── server.go               # HTTP server
│   │   └── handlers/               # /health, /metrics
│   ├── bootstrap/
│   │   └── app.go                  # DI & wiring
│   ├── common/
│   │   ├── circuitbreaker/         # Per-MNO circuit breakers
│   │   ├── httpclient/             # Pooled HTTP client
│   │   ├── logger/                 # Structured logging
│   │   ├── metrics/                # Prometheus metrics
│   │   └── ratelimit/              # Per-network rate limiter
│   ├── config/
│   │   └── config.go               # Env var loading
│   └── sms/
│       ├── domain/                 # Entities & errors
│       │   ├── message.go          # Message entity
│       │   ├── result.go           # SendResult, BatchResult
│       │   ├── network.go          # Network enum
│       │   └── errors.go           # Domain errors
│       ├── ports/                  # Interfaces
│       │   ├── mno_sender.go       # MNOSender interface
│       │   ├── queue_consumer.go   # Delivery, QueueConsumer
│       │   ├── queue_publisher.go  # QueuePublisher
│       │   ├── token_cache.go      # TokenCache
│       │   ├── priority_store.go   # PriorityStore
│       │   └── metrics.go          # Metrics interface
│       ├── service/                # Business logic
│       │   ├── message_router.go   # TX/Promo separation
│       │   ├── transactional_handler.go  # Fast-path
│       │   ├── priority_scheduler.go     # WFQ
│       │   ├── processor.go        # Worker pool
│       │   ├── router.go           # MNO selection
│       │   └── result_handler.go   # Queue routing
│       ├── adapters/               # Implementations
│       │   ├── mno/                # MNO senders
│       │   │   ├── factory.go      # Sender factory
│       │   │   ├── safaricom_sdp.go
│       │   │   ├── safaricom_smpp.go
│       │   │   ├── airtel.go
│       │   │   ├── telkom.go
│       │   │   ├── equitel.go
│       │   │   └── cm.go
│       │   ├── rabbitmq/           # Queue adapters
│       │   │   ├── connection.go
│       │   │   ├── consumer.go
│       │   │   └── publisher.go
│       │   └── redis/              # Cache adapters
│       │       ├── token_cache.go
│       │       └── priority_store.go
│       └── mocks/                  # Test mocks
└── docs/                           # Documentation
```

### Component Responsibilities

```mermaid
graph TB
    subgraph "Bootstrap Layer"
        APP[App<br/>Dependency Injection]
    end

    subgraph "Consumer Layer"
        CONSUMER[RabbitMQ Consumer<br/>- Connect to queue<br/>- Parse JSON batches<br/>- Deliver to router]
    end

    subgraph "Routing Layer"
        MSG_ROUTER[MessageRouter<br/>- Classify TX vs Promo<br/>- Route to appropriate handler<br/>- Manage delivery ack]

        TX_HANDLER[TransactionalHandler<br/>- Dedicated worker pool<br/>- Immediate processing<br/>- Bypass scheduler]

        PRI_SCHED[PriorityScheduler<br/>- WFQ algorithm<br/>- Weight-based delays<br/>- Starvation prevention<br/>- Hot-reload support]
    end

    subgraph "Processing Layer"
        PROCESSOR[Processor<br/>- Worker pool<br/>- Message validation<br/>- MSISDN normalization<br/>- Rate limiting<br/>- Send to MNO]

        ROUTER[Router<br/>- Network detection<br/>- TX routing for Safaricom<br/>- Sender selection]
    end

    subgraph "MNO Layer"
        FACTORY[MNO Factory<br/>- Create senders<br/>- Cache instances]

        SDP[Safaricom SDP<br/>- OAuth token mgmt<br/>- REST API calls]

        SMPP[SMPP Senders<br/>- HTTP to Kannel<br/>- URL building]
    end

    subgraph "Result Layer"
        RESULT_H[ResultHandler<br/>- Route by result type<br/>- Retry count check<br/>- DLQ routing]

        PUBLISHER[Publisher<br/>- Publish to RabbitMQ<br/>- Queue selection]
    end

    subgraph "Infrastructure"
        REDIS_TC[TokenCache<br/>- SDP token storage<br/>- TTL management]

        REDIS_PS[PriorityStore<br/>- Weight storage<br/>- Pub/Sub notifications<br/>- Local caching]

        METRICS[Metrics<br/>- Prometheus counters<br/>- Histograms<br/>- Gauges]
    end

    APP --> CONSUMER
    APP --> MSG_ROUTER
    APP --> TX_HANDLER
    APP --> PRI_SCHED
    APP --> PROCESSOR

    CONSUMER --> MSG_ROUTER
    MSG_ROUTER --> TX_HANDLER
    MSG_ROUTER --> PRI_SCHED
    TX_HANDLER --> PROCESSOR
    PRI_SCHED --> PROCESSOR
    PROCESSOR --> ROUTER
    ROUTER --> FACTORY
    FACTORY --> SDP
    FACTORY --> SMPP
    PROCESSOR --> RESULT_H
    RESULT_H --> PUBLISHER

    SDP --> REDIS_TC
    PRI_SCHED --> REDIS_PS
    PROCESSOR --> METRICS
```

---

## Error Handling & Edge Cases

### Error Classification

```mermaid
flowchart TD
    subgraph "Validation Errors → Permanent"
        V1[Missing correlator]
        V2[Missing content]
        V3[Missing MSISDN]
        V4[Missing sender]
        V5[Unknown network]
    end

    subgraph "MNO Errors → Permanent"
        M1[HTTP 400 Bad Request]
        M2[HTTP 404 Not Found]
        M3[Invalid MSISDN format]
    end

    subgraph "Transient Errors → Retryable"
        T1[HTTP 5xx Server Error]
        T2[HTTP 429 Rate Limited]
        T3[HTTP 401 Unauthorized]
        T4[Connection Timeout]
        T5[Connection Refused]
        T6[Circuit Breaker Open]
        T7[Rate Limiter Timeout]
    end

    subgraph "Destinations"
        DLQ[Dead Letter Queue]
        RETRY[Retry Queue]
    end

    V1 --> DLQ
    V2 --> DLQ
    V3 --> DLQ
    V4 --> DLQ
    V5 --> DLQ
    M1 --> DLQ
    M2 --> DLQ
    M3 --> DLQ

    T1 --> RETRY
    T2 --> RETRY
    T3 --> RETRY
    T4 --> RETRY
    T5 --> RETRY
    T6 --> RETRY
    T7 --> RETRY
```

### Retry Flow with Max Retries

```mermaid
flowchart TD
    RETRYABLE[Retryable Error] --> CHECK{retryCount >= 10?}

    CHECK -->|No| INCREMENT[Increment retryCount]
    INCREMENT --> RETRY_Q[Publish to RETRY_QUEUE]
    RETRY_Q --> REPROCESS[Message reprocessed<br/>by gateway]
    REPROCESS --> RESULT{New Result?}
    RESULT -->|Success| SAVE_DB[SAVE_TO_DB]
    RESULT -->|Retryable| RETRYABLE
    RESULT -->|Permanent| DLQ[Dead Letter Queue]

    CHECK -->|Yes| EXHAUST[Max Retries Exhausted]
    EXHAUST --> DLQ
```

### Edge Cases Handled

| Edge Case | Handling |
|-----------|----------|
| Empty delivery | Ack immediately, log warning |
| Parse error | Nack with requeue |
| Mixed TX + Promo batch | Process both paths, ack after all complete |
| Handler not configured | Fall back to Processor directly |
| Scheduler not configured | Fall back to Processor directly |
| Redis connection lost | Use cached weights, log error |
| RabbitMQ connection lost | Reconnect with backoff |
| Context cancelled | Return early, nack if needed |
| Publish failure | Nack delivery for requeue |

---

## Resilience Patterns

### Circuit Breaker State Machine

```mermaid
stateDiagram-v2
    [*] --> Closed: Initial State

    Closed --> Closed: Success
    Closed --> Closed: Failure (count < threshold)
    Closed --> Open: Consecutive failures >= 5

    Open --> Open: Requests rejected
    Open --> HalfOpen: After 30 seconds

    HalfOpen --> Closed: Success (up to 3 requests)
    HalfOpen --> Open: Any failure

    note right of Open: All requests fail fast<br/>with ErrCircuitOpen
    note right of HalfOpen: Allow limited requests<br/>to test recovery
```

### Rate Limiting

```mermaid
flowchart LR
    subgraph "Per-Network Rate Limiters"
        SAF[Safaricom<br/>200 req/s]
        AIR[Airtel<br/>50 req/s]
        TEL[Telkom<br/>100 req/s]
        EQU[Equitel<br/>20 req/s]
        CM[CM<br/>20 req/s]
    end

    MSG[Message] --> ROUTER{Network?}

    ROUTER -->|SAFARICOM| SAF
    ROUTER -->|AIRTEL| AIR
    ROUTER -->|TELKOM| TEL
    ROUTER -->|EQUITEL| EQU
    ROUTER -->|CM| CM

    SAF --> WAIT1[Wait for token]
    AIR --> WAIT2[Wait for token]
    TEL --> WAIT3[Wait for token]
    EQU --> WAIT4[Wait for token]
    CM --> WAIT5[Wait for token]

    WAIT1 --> SEND[Send to MNO]
    WAIT2 --> SEND
    WAIT3 --> SEND
    WAIT4 --> SEND
    WAIT5 --> SEND
```

### Token Caching (Safaricom SDP)

```mermaid
sequenceDiagram
    participant S as SDP Sender
    participant C as Redis Cache
    participant A as Safaricom Auth API

    S->>C: Get cached token (SDP_TOKEN_KEY)

    alt Token exists and valid
        C-->>S: Return token
        S->>S: Use token for API call
    else Token missing or expired
        S->>A: POST /auth/login
        A-->>S: {token, expires_in}
        S->>C: SET token (TTL: 25 min)
        S->>S: Use token for API call
    end

    alt API returns 401 Unauthorized
        S->>C: DELETE token
        S-->>S: Mark as retryable
        Note over S: Will retry with fresh token
    end
```

---

## Data Flow & State

### Message State Transitions

```mermaid
stateDiagram-v2
    [*] --> PENDING: Created

    PENDING --> PROCESSING: Picked up by gateway

    PROCESSING --> SENT: MNO accepted
    PROCESSING --> FAILED: Permanent error
    PROCESSING --> RETRYING: Retryable error

    RETRYING --> PROCESSING: Retry attempt
    RETRYING --> FAILED: Max retries exceeded

    SENT --> [*]: To SAVE_TO_DB
    FAILED --> [*]: To DLQ

    note right of SENT: status = "SENT"
    note right of FAILED: status = "FAILED TO SEND"
    note right of RETRYING: status = "RETRYING"
```

### Queue Data Flow

```mermaid
flowchart LR
    subgraph "Input"
        IN1[TITANIC-KE_SMS_QUEUE]
        IN2[CONSUME_TO_MNO]
        IN3[SMS_MNO_GATEWAY_QUEUE]
    end

    subgraph "Processing"
        GW[emalify-sms-mno-gateway]
    end

    subgraph "Output"
        OUT1[SAVE_TO_DB]
        OUT2[SMS_RETRY_QUEUE]
        OUT3[SMS_DEAD_LETTER_QUEUE]
    end

    subgraph "Downstream"
        DB[Database Service]
        RETRY_GW[Gateway<br/>retry processing]
        MANUAL[Manual Investigation]
    end

    IN1 --> GW
    IN2 --> GW
    IN3 --> GW

    GW -->|Success| OUT1
    GW -->|Retryable| OUT2
    GW -->|Permanent| OUT3

    OUT1 --> DB
    OUT2 --> RETRY_GW
    RETRY_GW --> GW
    OUT3 --> MANUAL
```

---

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| **Application** | | |
| `APP_ENV` | development | Environment mode |
| `LOG_LEVEL` | info | Log level (debug/info/warn/error) |
| `WORKER_COUNT` | 10 | Processor worker pool size |
| `HTTP_PORT` | 8080 | HTTP server port |
| **Redis** | | |
| `REDIS_HOST` | localhost | Redis host |
| `REDIS_PORT` | 6379 | Redis port |
| `REDIS_PASSWORD` | (empty) | Redis password |
| `REDIS_DB` | 0 | Redis database |
| **RabbitMQ** | | |
| `RABBITMQ_URL` | amqp://guest:guest@localhost:5672/ | Connection URL |
| `RABBITMQ_PREFETCH` | 10 | Prefetch count |
| **Queues** | | |
| `INPUT_QUEUES` | TITANIC-KE_SMS_QUEUE,CONSUME_TO_MNO | Comma-separated input queues |
| `SAVE_TO_DB_QUEUE` | SAVE_TO_DB | Success queue |
| `SMS_RETRY_QUEUE` | SMS_RETRY_QUEUE | Retry queue |
| `SMS_DEAD_LETTER_QUEUE` | SMS_DEAD_LETTER_QUEUE | DLQ |
| **Rate Limits** | | |
| `RATE_LIMIT_SAFARICOM` | 200 | Safaricom req/s |
| `RATE_LIMIT_AIRTEL` | 50 | Airtel req/s |
| `RATE_LIMIT_TELKOM` | 100 | Telkom req/s |
| `RATE_LIMIT_EQUITEL` | 20 | Equitel req/s |
| `RATE_LIMIT_CM` | 20 | CM req/s |
| **Priority Routing** | | |
| `PRIORITY_ENABLED` | false | Enable priority routing |
| `PRIORITY_REDIS_WEIGHTS_KEY` | sms:priority:weights | Redis key for weights |
| `PRIORITY_DEFAULT_WEIGHT` | 1 | Default queue weight |
| `PRIORITY_STARVATION_RATIO` | 0.1 | Starvation prevention ratio |
| `PRIORITY_TRANSACTIONAL_WORKERS` | 5 | TX handler workers |
| `PRIORITY_DEFAULT_QUEUE_WEIGHTS` | (see below) | Initial weights |

### Default Queue Weights

```env
PRIORITY_DEFAULT_QUEUE_WEIGHTS=GOLD_PARTNERS_QUEUE:10,BETIKA_GOLD:10,PEPETA_GOLD:10,TITANIC-KE_SMS_QUEUE:1,CONSUME_TO_MNO:1,SMS_MNO_GATEWAY_QUEUE:1
```

---

## Metrics & Observability

### Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| **Message Processing** | | | |
| `sms_messages_processed_total` | Counter | network, status | Messages processed |
| `sms_send_latency_seconds` | Histogram | network | Send latency |
| **Queue Metrics** | | | |
| `sms_queue_depth` | Gauge | queue | Queue depth |
| `sms_queue_published_total` | Counter | queue | Messages published |
| `sms_queue_consumed_total` | Counter | queue | Messages consumed |
| **Resilience** | | | |
| `sms_circuit_breaker_state` | Gauge | network | CB state (0=closed, 1=open) |
| `sms_circuit_breaker_trips_total` | Counter | network | CB trips |
| `sms_retries_total` | Counter | network | Retries |
| `sms_dead_letters_total` | Counter | network | Dead letters |
| `sms_rate_limit_hits_total` | Counter | network | Rate limit hits |
| **Priority Routing** | | | |
| `priority_messages_routed_total` | Counter | type, queue | Messages routed |
| `priority_transactional_processed_total` | Counter | status | TX processed |
| `priority_transactional_queue_depth` | Gauge | - | TX queue depth |
| `priority_scheduler_processed_total` | Counter | queue | Scheduler processed |
| `priority_scheduler_weight` | Gauge | queue | Current weights |
| `priority_starvation_triggers_total` | Counter | queue | Starvation triggers |

### Health Endpoints

```
GET /health     → Component health status
GET /ready      → Consumer readiness
GET /metrics    → Prometheus metrics
```

---

## Bootstrap Sequence

```mermaid
sequenceDiagram
    participant Main
    participant Config
    participant Redis
    participant RabbitMQ
    participant HTTP
    participant Priority
    participant Consumers

    Main->>Config: Load configuration
    Main->>Redis: Connect (token cache + priority store)
    Main->>RabbitMQ: Connect
    Main->>HTTP: Initialize pooled client
    Main->>Main: Initialize MNO Factory
    Main->>Main: Initialize Processor

    alt PRIORITY_ENABLED=true
        Main->>Priority: Initialize PriorityStore
        Main->>Priority: Initialize TransactionalHandler
        Main->>Priority: Initialize PriorityScheduler
        Main->>Priority: Initialize MessageRouter
        Priority->>Priority: Start TX handler workers
        Priority->>Priority: Start weight watcher
    end

    Main->>Consumers: Create consumers for INPUT_QUEUES
    Main->>Consumers: Start consuming
    Main->>Main: Start HTTP server
    Main->>Main: Wait for shutdown signal
```

### Graceful Shutdown

```mermaid
sequenceDiagram
    participant OS
    participant App
    participant HTTP
    participant Priority
    participant Consumers
    participant RabbitMQ
    participant Redis

    OS->>App: SIGINT/SIGTERM

    App->>HTTP: Shutdown (drain connections)

    alt PRIORITY_ENABLED
        App->>Priority: Stop TransactionalHandler
        Note over Priority: Wait for workers to finish
        App->>Priority: Stop PriorityScheduler
        Note over Priority: Cancel weight watcher
    end

    App->>Consumers: Stop all consumers
    Note over Consumers: Complete in-flight messages
    Note over Consumers: Ack pending deliveries

    App->>RabbitMQ: Close connection
    App->>Redis: Close connection

    App->>OS: Exit 0
```

---

*Last updated: 2026-03-03*
*Covers: EM-143, EM-139, EM-141, EM-145, EM-147, EM-148, EM-149, EM-155*
