# Usage Documentation

## Overview

This document describes how the `emalify-sms-mno-gateway` service operates, including queue configurations, MNO routing scenarios, and message handling.

## Service Purpose

The SMS MNO Gateway is responsible for:

1. **Consuming messages** from upstream queues
2. **Routing messages** to the appropriate Mobile Network Operator (MNO)
3. **Handling responses** and routing results to downstream queues
4. **Providing resilience** via circuit breakers, rate limiting, and retry logic

## Queue Configuration

### Input Queues

The service can consume from multiple input queues, configured via a comma-separated list in `INPUT_QUEUES`:

```mermaid
graph LR
    subgraph "Input Sources"
        A[requesttoqueuehandler<br/>HTTP API]
        B[batchingquehandler<br/>Batch processor]
        C[emalify-api-v2<br/>PHP API]
    end

    subgraph "Input Queues"
        TQ[TITANIC-KE_SMS_QUEUE<br/>Direct traffic]
        CMQ[CONSUME_TO_MNO<br/>Batched Gold partner traffic]
        GQ[SMS_MNO_GATEWAY_QUEUE<br/>Blue-green test traffic]
    end

    A --> TQ
    B --> CMQ
    B -->|Green lane| GQ
    C --> TQ
```

**Configuration:**

```env
# Comma-separated list of queues to consume from
INPUT_QUEUES=TITANIC-KE_SMS_QUEUE,CONSUME_TO_MNO,SMS_MNO_GATEWAY_QUEUE
```

| Queue | Purpose | Source |
|-------|---------|--------|
| `TITANIC-KE_SMS_QUEUE` | Direct/standard traffic | requesttoqueuehandler, emalify-api-v2 |
| `CONSUME_TO_MNO` | Batched Gold partner traffic | batchingquehandler (Blue lane) |
| `SMS_MNO_GATEWAY_QUEUE` | Blue-green deployment testing | batchingquehandler (Green lane) |

The service creates a separate consumer for each queue, all feeding into the same processing pipeline.

### Output Queues

Results are published to three output queues based on the outcome:

```mermaid
graph LR
    subgraph "Processing Result"
        SUCCESS[Success]
        RETRY[Retryable Failure]
        PERM[Permanent Failure]
    end

    subgraph "Output Queues"
        SAVE[SAVE_TO_DB]
        RQ[SMS_RETRY_QUEUE]
        DLQ[SMS_DEAD_LETTER_QUEUE]
    end

    SUCCESS --> SAVE
    RETRY --> RQ
    PERM --> DLQ
```

| Queue | Environment Variable | Default Value | Purpose |
|-------|---------------------|---------------|---------|
| Save to DB | `SAVE_TO_DB_QUEUE` | `SAVE_TO_DB` | Successfully sent messages |
| Retry Queue | `SMS_RETRY_QUEUE` | `SMS_RETRY_QUEUE` | Messages to retry |
| Dead Letter | `SMS_DEAD_LETTER_QUEUE` | `SMS_DEAD_LETTER_QUEUE` | Permanently failed messages |

## Message Format

### Input Message Schema

Messages arrive as JSON arrays:

```json
[
  {
    "corelator": "unique-message-id-12345",
    "message": "Your OTP is 123456",
    "msisdn": "254722123456",
    "network": "SAFARICOM",
    "sender": "MyApp",
    "packageId": "0",
    "status": "",
    "createdAt": "2024-01-15T10:30:00Z"
  }
]
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `corelator` | string | Yes | Unique message identifier (note: intentional typo in legacy schema) |
| `message` | string | Yes | SMS content |
| `msisdn` | string | Yes | Recipient phone number |
| `network` | string | Yes | Target network (SAFARICOM, AIRTEL, TELKOM, EQUITEL, CM, INTNL) |
| `sender` | string | Yes | Sender ID displayed to recipient |
| `packageId` | string | No | Package identifier; "TRANSACTIONAL" triggers SMPP routing |
| `status` | string | No | Current status (updated during processing) |
| `createdAt` | string | No | Message creation timestamp |

### Output Message Schema

Processed messages include additional fields:

```json
{
  "corelator": "unique-message-id-12345",
  "message": "Your OTP is 123456",
  "msisdn": "254722123456",
  "network": "SAFARICOM",
  "sender": "MyApp",
  "packageId": "TRANSACTIONAL",
  "status": "SENT",
  "createdAt": "2024-01-15T10:30:00Z",
  "retryCount": 0,
  "lastError": "",
  "processedAt": "2024-01-15T10:30:05Z"
}
```

| Additional Field | Type | Description |
|-----------------|------|-------------|
| `retryCount` | int | Number of retry attempts |
| `lastError` | string | Last error message (if failed) |
| `processedAt` | time | When the message was processed |

## MNO Configuration

### Supported Networks

```mermaid
graph TB
    subgraph "Kenyan Networks"
        SAF[SAFARICOM<br/>Largest carrier]
        AIR[AIRTEL<br/>Second largest]
        TEL[TELKOM<br/>Third carrier]
        EQU[EQUITEL<br/>Equity Bank mobile]
    end

    subgraph "International"
        CM[CM / INTNL<br/>International routes]
    end
```

### Network Prefixes (Kenya)

| Network | Prefixes | Example |
|---------|----------|---------|
| Safaricom | 070x, 071x, 072x, 074x, 075x, 076x, 079x | 254722123456 |
| Airtel | 073x, 078x, 010x | 254733123456 |
| Telkom | 077x | 254770123456 |
| Equitel | 076x | 254763123456 |

### MSISDN Normalization

The service automatically normalizes phone numbers to international format:

```
Input: 0722123456  → Output: 254722123456
Input: 254722123456 → Output: 254722123456
Input: +254722123456 → Output: 254722123456
```

## MNO Routing Scenarios

### Scenario 1: Safaricom Non-Transactional (Promotional)

```mermaid
sequenceDiagram
    participant Q as Queue
    participant GW as Gateway
    participant SDP as Safaricom SDP API
    participant Redis as Redis Cache

    Q->>GW: Message (network=SAFARICOM, packageId=0)
    GW->>Redis: Get cached token
    Redis-->>GW: Token (or miss)

    alt Token cache miss
        GW->>SDP: POST /auth/login
        SDP-->>GW: Bearer token
        GW->>Redis: Cache token (TTL: 25min)
    end

    GW->>SDP: POST /bulksms (Bearer token)
    SDP-->>GW: Success response
    GW->>Q: Publish to SAVE_TO_DB
```

**Configuration:**
```env
SDP_AUTH_URL=https://dsvc2.safaricom.com:9480/api/auth/login
SDP_SEND_URL=https://dsvc2.safaricom.com:9480/api/public/CMS/bulksms
SDP_USERNAME=roamtechapi
SDP_PASSWORD=<secret>
SDP_DLR_URL=https://smsdlr.emalify.com/save
SDP_TOKEN_KEY=SDP_TOKEN_KEY
SDP_TOKEN_TTL=25m
```

### Scenario 2: Safaricom Transactional (OTP/Alerts)

```mermaid
sequenceDiagram
    participant Q as Queue
    participant GW as Gateway
    participant KANNEL as Kannel SMPP

    Q->>GW: Message (network=SAFARICOM, packageId=TRANSACTIONAL)
    Note over GW: Detects transactional flag
    GW->>KANNEL: GET /sendsms?smsc=SAFARICOM&...
    KANNEL-->>GW: 0: Accepted for delivery
    GW->>Q: Publish to SAVE_TO_DB
```

**Configuration:**
```env
SAFARICOM_SMPP_URL=http://10.0.0.87:80/cgi-bin/sendsms
SAFARICOM_SMPP_SMSC=SAFARICOM
SAFARICOM_SMPP_USERNAME=safcomtx
SAFARICOM_SMPP_PASSWORD=<secret>
SAFARICOM_SMPP_DLR_URL=http://10.0.0.100:8088/save
```

### Scenario 3: Airtel

```mermaid
sequenceDiagram
    participant Q as Queue
    participant GW as Gateway
    participant KANNEL as Kannel SMPP

    Q->>GW: Message (network=AIRTEL)
    GW->>KANNEL: GET /sendsms?smsc=AIRTEL&...
    KANNEL-->>GW: 0: Accepted for delivery
    GW->>Q: Publish to SAVE_TO_DB
```

**Configuration:**
```env
AIRTEL_SMPP_URL=http://10.0.0.88:14013/cgi-bin/sendsms
AIRTEL_SMPP_SMSC=AIRTEL
AIRTEL_SMPP_USERNAME=airtel
AIRTEL_SMPP_PASSWORD=<secret>
AIRTEL_SMPP_DLR_URL=http://10.0.0.100:8088/save
```

### Scenario 4: Telkom

```mermaid
sequenceDiagram
    participant Q as Queue
    participant GW as Gateway
    participant KANNEL as Kannel SMPP

    Q->>GW: Message (network=TELKOM)
    GW->>KANNEL: GET /sendsms?smsc=TELKOM&...
    KANNEL-->>GW: 0: Accepted for delivery
    GW->>Q: Publish to SAVE_TO_DB
```

**Configuration:**
```env
TELKOM_SMPP_URL=http://34.77.25.98:14013/cgi-bin/sendsms
TELKOM_SMPP_SMSC=TELKOM
TELKOM_SMPP_USERNAME=eadctrx
TELKOM_SMPP_PASSWORD=<secret>
TELKOM_SMPP_DLR_URL=http://197.248.69.107:48088/save
```

### Scenario 5: Equitel

```mermaid
sequenceDiagram
    participant Q as Queue
    participant GW as Gateway
    participant KANNEL as Kannel SMPP

    Q->>GW: Message (network=EQUITEL)
    GW->>KANNEL: GET /sendsms?smsc=EQUITEL&...
    KANNEL-->>GW: 0: Accepted for delivery
    GW->>Q: Publish to SAVE_TO_DB
```

**Configuration:**
```env
EQUITEL_SMPP_URL=http://10.0.0.87:80/cgi-bin/sendsms
EQUITEL_SMPP_SMSC=EQUITEL
EQUITEL_SMPP_USERNAME=equitel
EQUITEL_SMPP_PASSWORD=<secret>
EQUITEL_SMPP_DLR_URL=http://10.0.0.100:8088/save
```

### Scenario 6: CM / International

```mermaid
sequenceDiagram
    participant Q as Queue
    participant GW as Gateway
    participant KANNEL as Kannel SMPP

    Q->>GW: Message (network=CM or INTNL)
    GW->>KANNEL: GET /sendsms?smsc=CM&...
    KANNEL-->>GW: 0: Accepted for delivery
    GW->>Q: Publish to SAVE_TO_DB
```

**Configuration:**
```env
CM_SMPP_URL=http://34.77.25.98:14013/cgi-bin/sendsms
CM_SMPP_SMSC=CM
CM_SMPP_USERNAME=cm_user
CM_SMPP_PASSWORD=<secret>
CM_SMPP_DLR_URL=http://10.0.0.100:8088/save
```

## MNO Routing Summary

| Network | packageId | Route | Protocol | Gateway |
|---------|-----------|-------|----------|---------|
| SAFARICOM | (any except TRANSACTIONAL) | Safaricom SDP | HTTPS REST | dsvc2.safaricom.com |
| SAFARICOM | TRANSACTIONAL | Safaricom SMPP | HTTP/Kannel | 10.0.0.87:80 |
| AIRTEL | (any) | Airtel SMPP | HTTP/Kannel | 10.0.0.88:14013 |
| TELKOM | (any) | Telkom SMPP | HTTP/Kannel | 34.77.25.98:14013 |
| EQUITEL | (any) | Equitel SMPP | HTTP/Kannel | 10.0.0.87:80 |
| CM / INTNL | (any) | CM International | HTTP/Kannel | 34.77.25.98:14013 |

## Rate Limiting

Each network has configured rate limits to prevent overwhelming MNO APIs:

| Network | Rate Limit | Environment Variable |
|---------|------------|---------------------|
| Safaricom | 200 req/s | `RATE_LIMIT_SAFARICOM` |
| Airtel | 50 req/s | `RATE_LIMIT_AIRTEL` |
| Telkom | 100 req/s | `RATE_LIMIT_TELKOM` |
| Equitel | 20 req/s | `RATE_LIMIT_EQUITEL` |
| CM | 20 req/s | `RATE_LIMIT_CM` |

When rate limited, messages are marked as retryable and sent to the retry queue.

## Priority Routing (EM-155)

When enabled, the priority routing system separates transactional and promotional messages for optimal delivery using **Credit-Based Weighted Round Robin (WRR)**.

### Overview

```mermaid
flowchart LR
    subgraph "Input"
        MSG[Messages]
    end

    subgraph "Message Router"
        CHECK{packageId?}
    end

    subgraph "Processing Paths"
        TX[Transactional Handler<br/>Fast-Path]
        WRR[Priority Scheduler<br/>Credit-Based WRR]
    end

    MSG --> CHECK
    CHECK -->|TRANSACTIONAL| TX
    CHECK -->|Other| WRR

    TX --> MNO[MNO Senders]
    WRR --> MNO
```

### Enabling Priority Routing

Set `PRIORITY_ENABLED=true` in your `.env` file:

```env
PRIORITY_ENABLED=true
PRIORITY_REDIS_WEIGHTS_KEY=sms:priority:weights
PRIORITY_DEFAULT_WEIGHT=1
PRIORITY_TRANSACTIONAL_WORKERS=5

# Credit-Based WRR Configuration
PRIORITY_CREDIT_MULTIPLIER=10       # Credits per weight unit (capacity = weight × multiplier)
PRIORITY_REFILL_PERIOD_MS=100       # Credit refill interval in milliseconds
PRIORITY_MAX_STARVATION_AGE_SEC=10  # Max seconds before starvation prevention
```

### Configuring Queue Weights

Higher weight queues receive proportionally more credits and processing capacity:

```env
# Format: queue1:weight1,queue2:weight2,...
PRIORITY_DEFAULT_QUEUE_WEIGHTS=TITANIC-KE_SMS_QUEUE:10,CONSUME_TO_MNO:5,SMS_MNO_GATEWAY_QUEUE:1
```

| Queue | Weight | Credit Capacity | Refill Rate | Approx Share |
|-------|--------|-----------------|-------------|--------------|
| TITANIC-KE_SMS_QUEUE | 10 | 100 credits | +10/tick | ~62.5% |
| CONSUME_TO_MNO | 5 | 50 credits | +5/tick | ~31.25% |
| SMS_MNO_GATEWAY_QUEUE | 1 | 10 credits | +1/tick | ~6.25% |

**How Credit-Based WRR Works:**

1. Each queue has a credit channel with capacity = `weight × creditMultiplier`
2. Processing a batch requires acquiring one credit from the queue's channel
3. A background goroutine refills credits every `refillPeriod` at rate = `weight` credits/tick
4. **Zero latency when idle**: Credits accumulate, allowing instant burst processing
5. **Fair under contention**: Higher weight = more credits = more throughput

### Hot-Reload Weights at Runtime

Queue weights can be updated without restarting the service:

```bash
# Update a single queue weight
redis-cli HSET sms:priority:weights TITANIC-KE_SMS_QUEUE 20

# Update multiple weights
redis-cli HMSET sms:priority:weights \
    TITANIC-KE_SMS_QUEUE 20 \
    CONSUME_TO_MNO 10 \
    SMS_MNO_GATEWAY_QUEUE 2

# Notify the service of changes
redis-cli PUBLISH sms:priority:weights:notifications weights_updated

# View current weights
redis-cli HGETALL sms:priority:weights
```

### Transactional Fast-Path

Messages with `packageId="TRANSACTIONAL"` bypass the WRR scheduler entirely:

- **Dedicated worker pool**: Configurable via `PRIORITY_TRANSACTIONAL_WORKERS` (default: 5)
- **No credit requirements**: Processed immediately by available workers
- **Separate from promotional**: Won't be delayed by promotional traffic
- **Synchronous batch processing**: `ProcessBatch()` completes before delivery ack

### Starvation Prevention

Low-priority queues are guaranteed minimum processing even under heavy contention:

```env
# Maximum time (seconds) a queue can go without processing before bypass
PRIORITY_MAX_STARVATION_AGE_SEC=10
```

If a queue hasn't processed any messages for longer than `maxStarvationAge`:
- The credit check is bypassed
- Processing proceeds immediately
- A `starvation_triggers` metric is incremented
- The `lastProcessed` timestamp is updated

This ensures that even weight=1 queues don't starve indefinitely when competing with weight=10 queues.

### Priority Metrics

Monitor priority routing via Prometheus:

| Metric | Description |
|--------|-------------|
| `emalify_sms_priority_messages_routed_total{type,queue}` | Messages routed by type |
| `emalify_sms_priority_transactional_processed_total{status}` | Transactional processing |
| `emalify_sms_priority_transactional_queue_depth` | Transactional queue depth |
| `emalify_sms_priority_scheduler_processed_total{queue}` | Scheduler processing per queue |
| `emalify_sms_priority_scheduler_weight{queue}` | Current queue weights |
| `emalify_sms_priority_starvation_triggers_total{queue}` | Starvation prevention triggers |

### Priority Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PRIORITY_ENABLED` | false | Enable priority routing with Credit-Based WRR |
| `PRIORITY_REDIS_WEIGHTS_KEY` | sms:priority:weights | Redis hash key for queue weights |
| `PRIORITY_DEFAULT_QUEUE_WEIGHTS` | (see below) | Initial queue weights (format: `QUEUE1:10,QUEUE2:5`) |
| `PRIORITY_DEFAULT_WEIGHT` | 1 | Weight for unconfigured queues |
| `PRIORITY_TRANSACTIONAL_WORKERS` | 5 | Dedicated workers for transactional fast-path |
| `PRIORITY_CREDIT_MULTIPLIER` | 10 | Credits per weight unit (capacity = weight × multiplier) |
| `PRIORITY_REFILL_PERIOD_MS` | 100 | Credit refill interval in milliseconds |
| `PRIORITY_MAX_STARVATION_AGE_SEC` | 10 | Max seconds without processing before starvation bypass |

**Configuration Details:**

| Parameter | Formula/Behavior | Example |
|-----------|------------------|---------|
| Credit Capacity | `weight × creditMultiplier` | weight=10, multiplier=10 → 100 credits |
| Refill Rate | `weight` credits per `refillPeriod` | weight=10, period=100ms → 100 credits/sec |
| Throughput Ratio | Approximately proportional to weight | weight=10 vs weight=1 → ~10× throughput |
| Starvation Threshold | Process immediately if idle > `maxStarvationAge` | After 10s idle → bypass credits |

## Retry Logic

### Retry Flow

```mermaid
flowchart TD
    SEND[Send Message] --> RESULT{Result?}

    RESULT -->|Success| SAVE[SAVE_TO_DB]
    RESULT -->|Permanent Error| DLQ[Dead Letter Queue]
    RESULT -->|Retryable Error| CHECK{retryCount < 10?}

    CHECK -->|Yes| RETRY[Retry Queue]
    CHECK -->|No| DLQ

    RETRY --> SEND
```

### Retryable Conditions

| Condition | Description |
|-----------|-------------|
| HTTP 5xx | Server-side errors |
| HTTP 429 | Rate limit exceeded |
| HTTP 401 | Token expired (SDP) |
| Timeout | Connection or read timeout |
| Connection refused | Target server unavailable |
| Circuit breaker open | MNO temporarily unavailable |

### Permanent Failure Conditions

| Condition | Description |
|-----------|-------------|
| HTTP 400 | Bad request (invalid data) |
| HTTP 404 | Resource not found |
| Invalid MSISDN | Phone number validation failed |
| Unknown network | Network not recognized |
| Max retries exceeded | 10 retries exhausted |
| Missing required fields | Validation errors |

## Delivery Report (DLR) Handling

The gateway does **not** receive DLRs. DLR callbacks are handled by separate external services. The gateway only configures the DLR callback URL when sending:

| MNO | DLR Callback URL | Handler |
|-----|------------------|---------|
| Safaricom SDP | https://smsdlr.emalify.com/save | External service |
| Safaricom SMPP | http://10.0.0.100:8088/save | External service |
| Airtel | http://10.0.0.100:8088/save | External service |
| Telkom | http://197.248.69.107:48088/save | External service |
| Equitel | http://10.0.0.100:8088/save | External service |
| CM | http://10.0.0.100:8088/save | External service |

## API Endpoints

### Health Check

```
GET /health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "components": {
    "rabbitmq": "connected",
    "redis": "connected"
  }
}
```

### Readiness Check

```
GET /ready
```

Response:
```json
{
  "ready": true,
  "consumers": {
    "TITANIC-KE_SMS_QUEUE": "running",
    "CONSUME_TO_MNO": "running"
  }
}
```

### Metrics

```
GET /metrics
```

Returns Prometheus-formatted metrics.

## Configuration Reference

### Application Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_ENV` | development | Environment (development/production) |
| `LOG_LEVEL` | info | Log level (debug/info/warn/error) |
| `WORKER_COUNT` | 10 | Number of worker goroutines |
| `HTTP_PORT` | 8080 | HTTP server port |

### Redis Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | localhost | Redis host |
| `REDIS_PORT` | 6379 | Redis port |
| `REDIS_PASSWORD` | (empty) | Redis password |
| `REDIS_DB` | 0 | Redis database number |

### RabbitMQ Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `RABBITMQ_URL` | amqp://guest:guest@localhost:5672/ | RabbitMQ connection URL |
| `RABBITMQ_PREFETCH` | 10 | Prefetch count per consumer |

## Operational Procedures

### Starting the Service

```bash
# Using Docker
docker-compose up -d

# Using binary
./emalify-sms-mno-gateway
```

### Graceful Shutdown

Send SIGINT or SIGTERM:
```bash
kill -SIGTERM <pid>
```

The service will:
1. Stop accepting new messages
2. Wait for in-flight messages to complete (30s timeout)
3. Publish all pending results
4. Acknowledge all processed deliveries
5. Close connections cleanly

### Monitoring

1. **Prometheus metrics**: Scrape `/metrics` endpoint
2. **Health checks**: Poll `/health` for component status
3. **Log analysis**: JSON-structured logs with correlator IDs

### Troubleshooting

| Symptom | Possible Cause | Solution |
|---------|---------------|----------|
| Messages not processing | Consumer not started | Check RabbitMQ connection |
| High retry rate | MNO issues | Check circuit breaker state |
| Token errors (SDP) | Redis connectivity | Check Redis connection |
| Slow processing | Rate limiting | Adjust rate limits |
| Messages in DLQ | Permanent failures | Review error messages |

## Migration from Legacy Services

This service replaces:

| Legacy Service | Features Migrated |
|----------------|-------------------|
| ApiGateway | Safaricom SDP, Safaricom SMPP, Airtel, Telkom, Equitel, CM, transactional routing |
| sendtomnohandler | Rate limiting, retry logic, result publishing |

### Key Differences

1. **Unified codebase**: Single service instead of two
2. **Manual acknowledgment**: Messages only acked after processing (EM-143 fix)
3. **Proper error handling**: Redis errors propagated (EM-145 fix)
4. **Connection pooling**: Shared HTTP client (EM-141 fix)
5. **Circuit breakers**: Per-MNO failure isolation (EM-148 fix)
6. **DLQ routing**: Permanent failures go to DLQ (EM-149 fix)
7. **Metrics**: Prometheus observability (EM-147 fix)
8. **Priority routing**: Credit-Based WRR scheduler with transactional fast-path (EM-155)
