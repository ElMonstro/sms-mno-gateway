# Retry Queue Separation — Design Spec
**Date:** 2026-05-13
**Branch:** jeremiahdindi/em-1055-stabilize-core-messaging-submission-and-retry-delays
**Status:** Approved

---

## Problem Statement

The SMS MNO Gateway processes all messages — fresh and retry — through a single shared pool. This causes three compounding problems under high traffic:

1. **No throughput isolation** — retry messages compete directly with main queue messages for workers, prefetch, and rate limit tokens. A large retry backlog (e.g. 160k messages) starves fresh message delivery.
2. **No delay on retry** — failed messages re-enter the retry queue immediately and hammer the MNO at full rate during degradation, causing retry amplification that grows queue depth faster than it drains.
3. **No type-aware prioritisation** — transactional retries (OTP, banking) sit behind promotional retries with no mechanism to process them faster, violating their latency SLA.

---

## Goals

- Separate retry processing from main queue processing at every layer: workers, prefetch, and rate limit budget.
- Add configurable fixed delay before retry attempts — shorter for transactional, longer for promotional.
- Split transactional and promotional retries into independent consumer pools and queues.
- Bound maximum message lifetime under sustained MNO failure to prevent infinite retry amplification.
- Preserve backward compatibility by draining the existing `SMS_RETRY_QUEUE` without hard cutover.

---

## MNO Rate Limits (Reference)

| Network | Total TPS |
|---------|-----------|
| Safaricom SDP | 1000 |
| Safaricom SMPP | 200 |
| Airtel | 25 |
| Equitel | 50 |
| Telkom | 50 |

---

## Section 1: Queue Topology

Four new queues. Delay queues use RabbitMQ native TTL + dead-letter-exchange — no plugin required. When a message TTL expires the broker routes it automatically to the retry queue via DLX.

```
Failure
  IsTransactional? → SMS_TRANSACTIONAL_DELAY_QUEUE ──TTL──▶ SMS_TRANSACTIONAL_RETRY_QUEUE
  IsPromotional?   → SMS_PROMOTIONAL_DELAY_QUEUE   ──TTL──▶ SMS_PROMOTIONAL_RETRY_QUEUE
```

### Queue Declarations

| Queue | TTL | Dead-letter target |
|-------|-----|--------------------|
| `SMS_TRANSACTIONAL_DELAY_QUEUE` | `RETRY_TRANSACTIONAL_DELAY_MS` | `SMS_TRANSACTIONAL_RETRY_QUEUE` |
| `SMS_PROMOTIONAL_DELAY_QUEUE` | `RETRY_PROMOTIONAL_DELAY_MS` | `SMS_PROMOTIONAL_RETRY_QUEUE` |
| `SMS_TRANSACTIONAL_RETRY_QUEUE` | — | — |
| `SMS_PROMOTIONAL_RETRY_QUEUE` | — | — |

Declared with `amqp.Table` args at `Publisher` init. Idempotent — safe on every restart.

### Migration

`SMS_RETRY_QUEUE` stays in `INPUT_QUEUES` until depth reaches zero. All new failures route to the split delay queues immediately after deploy. Once drained, remove from `INPUT_QUEUES`, restart, then delete from RabbitMQ management. No dual-write required.

---

## Section 2: Rate Limiter Split

The current `Limiter` holds one `rate.Limiter` per network. Extended to hold two buckets per network: `main` (existing queues) and `retry` (new retry queues).

### Limiter Structure

```go
type Limiter struct {
    main  map[domain.Network]*rate.Limiter
    retry map[domain.Network]*rate.Limiter
    mu    sync.RWMutex
}
```

### Call Paths

```go
limiter.Wait(ctx, network)        // main queues — unchanged
limiter.WaitRetry(ctx, network)   // retry queues — new
```

Main queue processors call `Wait()`. Retry processors call `WaitRetry()`. Each path is independent.

### Burst Factor

Retry limiters initialised with a configurable burst multiplier:

```go
rate.NewLimiter(rate.Limit(retryRPS), retryRPS*burstFactor)
```

When main queues are idle, retry can absorb up to `burstFactor ×` its reserved rate. Defaults to `1` (strict). Set to `2` for faster backlog drain during low main-queue load.

### Recommended Starting Allocation

| Network | Total | Main | Retry | Retry burst cap |
|---------|-------|------|-------|-----------------|
| Safaricom SDP | 1000 | 800 | 200 | 400 |
| Safaricom SMPP | 200 | 160 | 40 | 80 |
| Airtel | 25 | 20 | 5 | 10 |
| Equitel | 50 | 40 | 10 | 20 |
| Telkom | 50 | 40 | 10 | 20 |

All values hot-adjustable via env restart.

---

## Section 3: Worker Pools

Two dedicated retry `Processor` instances wired at bootstrap alongside existing main queue processors.

### Consumer Architecture

```
Main consumers (existing)
  TITANIC-KE_SMS_QUEUE  ──▶  Processor (WORKER_COUNT, PREFETCH, main limiter)
  CONSUME_TO_MNO        ──▶  Processor (WORKER_COUNT, PREFETCH, main limiter)
  SMS_MNO_GATEWAY_QUEUE ──▶  Processor (WORKER_COUNT, PREFETCH, main limiter)

Retry consumers (new)
  SMS_TRANSACTIONAL_RETRY_QUEUE ──▶  RetryProcessor (RETRY_TRANSACTIONAL_WORKER_COUNT,
                                                       RETRY_TRANSACTIONAL_PREFETCH,
                                                       retry limiter)
  SMS_PROMOTIONAL_RETRY_QUEUE   ──▶  RetryProcessor (RETRY_PROMOTIONAL_WORKER_COUNT,
                                                       RETRY_PROMOTIONAL_PREFETCH,
                                                       retry limiter)
```

### Single-Message Delivery Fix

Retry messages arrive one-per-AMQP-delivery. `processBatch()` currently spawns `workerCount` goroutines unconditionally — 99 idle goroutines created and destroyed per message at `WORKER_COUNT=100`.

Fix: cap workers at actual message count:

```go
workers := min(p.workerCount, len(messages))
for i := 0; i < workers; i++ {
    wg.Add(1)
    go p.worker(ctx, i, msgChan, resultChan, &wg)
}
```

Applied to `Processor.processBatch()`. No impact on main queues (large batches).

### Sizing

Workers set above rate limit ceiling so the limiter is always the binding constraint.

| Pool | Workers | Prefetch | Rate limit ceiling |
|------|---------|----------|--------------------|
| Transactional retry | 50 | 100 | 40 TPS (SMPP) |
| Promotional retry | 200 | 400 | 200 TPS (SDP) |

### Performance Concern: Error Rate Amplification

Under sustained MNO failure with 160k promotional messages and 30s delay:

```
160,000 / 30s = ~5,300 re-entries/second
200 workers at degraded success rate ≪ 5,300/s inflow
→ queue depth grows under sustained degradation
```

`MaxRetries` bounds this: promotional at 30s × 10 = 5 min max lifetime. Transactional at 5s × 5 = 25s max lifetime. After limit, message routes to DLQ — stops amplification.

---

## Section 4: Publisher Routing

### Delay Queue Declaration

```go
conn.DeclareQueueWithArgs(ctx, cfg.TransactionalDelayQueue, amqp.Table{
    "x-message-ttl":             int32(cfg.RetryTransactionalDelayMs),
    "x-dead-letter-exchange":    "",
    "x-dead-letter-routing-key": cfg.TransactionalRetryQueue,
})
conn.DeclareQueueWithArgs(ctx, cfg.PromotionalDelayQueue, amqp.Table{
    "x-message-ttl":             int32(cfg.RetryPromotionalDelayMs),
    "x-dead-letter-exchange":    "",
    "x-dead-letter-routing-key": cfg.PromotionalRetryQueue,
})
```

### `PublishResult()` Routing

```
ResultRetryable + IsTransactional()  → SMS_TRANSACTIONAL_DELAY_QUEUE
ResultRetryable + !IsTransactional() → SMS_PROMOTIONAL_DELAY_QUEUE
ResultSuccess                        → SAVE_TO_DB_QUEUE       (unchanged)
ResultPermanent                      → SMS_DEAD_LETTER_QUEUE  (unchanged)
                                       + SAVE_TO_DB_QUEUE     (unchanged)
```

### Batch Publish for Retryables

`PublishBatchResults()` currently calls individual `Publish()` per failed message. Under high error rates this saturates the channel pool.

Fix: group retryables by type, single `PublishBatch()` per type per batch:

```go
if len(transactionalRetries) > 0 {
    p.PublishBatch(ctx, p.queues.TransactionalDelayQueue, transactionalRetries)
}
if len(promotionalRetries) > 0 {
    p.PublishBatch(ctx, p.queues.PromotionalDelayQueue, promotionalRetries)
}
```

Reduces publish calls from N to 2. Also fixes the single-message delivery problem — retry AMQP messages now carry multiple domain messages, giving `processBatch()` real batches.

Same batching applied to DLQ publishes to prevent channel exhaustion during mass DLQ events.

---

## Section 5: Config and MaxRetries

### New Environment Variables

```bash
# Queue names
SMS_TRANSACTIONAL_RETRY_QUEUE=SMS_TRANSACTIONAL_RETRY_QUEUE
SMS_PROMOTIONAL_RETRY_QUEUE=SMS_PROMOTIONAL_RETRY_QUEUE
SMS_TRANSACTIONAL_DELAY_QUEUE=SMS_TRANSACTIONAL_DELAY_QUEUE
SMS_PROMOTIONAL_DELAY_QUEUE=SMS_PROMOTIONAL_DELAY_QUEUE

# Fixed retry delays (milliseconds)
RETRY_TRANSACTIONAL_DELAY_MS=5000     # 5s — short, preserves OTP urgency
RETRY_PROMOTIONAL_DELAY_MS=30000      # 30s — prevents MNO hammering

# Worker pools
RETRY_TRANSACTIONAL_WORKER_COUNT=50
RETRY_TRANSACTIONAL_PREFETCH=100
RETRY_PROMOTIONAL_WORKER_COUNT=200
RETRY_PROMOTIONAL_PREFETCH=400

# Rate limit budgets (TPS per network)
RATE_LIMIT_RETRY_SAFARICOM_SDP=200
RATE_LIMIT_RETRY_SAFARICOM_SMPP=40
RATE_LIMIT_RETRY_AIRTEL=5
RATE_LIMIT_RETRY_EQUITEL=10
RATE_LIMIT_RETRY_TELKOM=10
RATE_LIMIT_RETRY_BURST_FACTOR=2

# Retry lifecycle
RETRY_MAX_RETRIES_TRANSACTIONAL=5
RETRY_MAX_RETRIES_PROMOTIONAL=10
```

### MaxRetries Split

`ResultHandler.maxRetries` (hardcoded at 10) becomes two configurable fields:

```go
type ResultHandlerConfig struct {
    MaxRetriesTransactional int
    MaxRetriesPromotional   int
}
```

`HandleBatchResults()` selects the limit based on `result.Message.IsTransactional()`.

### Maximum Message Lifetime

| Type | Delay | Max retries | Max lifetime before DLQ |
|------|-------|-------------|--------------------------|
| Transactional | 5s | 5 | ~25 seconds |
| Promotional | 30s | 10 | ~5 minutes |

### Why Transactional MaxRetries = 5

An OTP at retry 5 with 5s delay has been waiting 25+ seconds — likely expired. Further retries waste MNO capacity and delay fresh messages. Routing to DLQ faster keeps transactional throughput high.

---

## Files to Modify

| File | Change |
|------|--------|
| `internal/config/config.go` | Add all new env vars to Config struct |
| `internal/common/ratelimit/limiter.go` | Add `retry` bucket map and `WaitRetry()` method |
| `internal/sms/adapters/rabbitmq/publisher.go` | Add `DeclareQueueWithArgs()`, update routing, batch retryables |
| `internal/sms/service/result_handler.go` | Split MaxRetries by type, route to delay queues |
| `internal/sms/service/processor.go` | Cap `processBatch()` workers at `min(workerCount, len(messages))` |
| `internal/bootstrap/app.go` | Wire two retry consumers, two retry processors, retry rate limiter |

---

## Non-Goals

- Per-queue granularity within main queues — out of scope, addressable separately if needed.
- Exponential backoff — fixed delay is sufficient given the MaxRetries lifetime bound.
- Separate retry microservice — disproportionate complexity for a wiring and config problem.
