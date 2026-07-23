package service

import (
	"context"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// AfricasTalkingProcessor processes deliveries from AFRICAS_TALKING_SMS_QUEUE.
// Unlike Processor, it does not route results through Publisher.PublishResult —
// it calls the AfricasTalking API directly, then reports the outcome to the PHP
// API over HTTP, and acks the delivery only once that report call succeeds.
// Successful and permanently-failed results are also published to SAVE_TO_DB,
// mirroring how Processor/Publisher.PublishResult handles SDP/SMPP; retryable
// results are not, since this processor has no delay/retry-queue path of its own.
// The queue is documented to carry exactly one message per delivery.
//
// If the report call or the SAVE_TO_DB publish fails after a message has already
// been sent via AfricasTalking, handleDownstreamFailure applies a bounded,
// backed-off retry (republishing a fresh copy with an incremented retry count)
// rather than requeueing instantly and indefinitely — which would otherwise
// re-send the same message via the real AfricasTalking API in a tight loop for as
// long as the downstream step keeps failing.
type AfricasTalkingProcessor struct {
	sender          ports.MNOSender
	reporter        ports.ResultReporter
	rateLimiter     *ratelimit.Limiter
	metrics         ports.Metrics
	publisher       ports.QueuePublisher
	saveToDBQueue   string
	queueName       string
	deadLetterQueue string

	// maxReportRetries/reportRetryBaseDelay/reportRetryMaxDelay bound the retry
	// loop after a message has already been sent via AfricasTalking but a
	// downstream step (the PHP callback report, or the SAVE_TO_DB publish)
	// failed. Without a cap and backoff here, a persistently failing downstream
	// step requeues — and thus re-sends via the real AfricasTalking API —
	// indefinitely, with no delay between attempts.
	maxReportRetries     int
	reportRetryBaseDelay time.Duration
	reportRetryMaxDelay  time.Duration

	log logger.Logger
}

// AfricasTalkingProcessorConfig holds configuration for the processor.
type AfricasTalkingProcessorConfig struct {
	Sender          ports.MNOSender
	Reporter        ports.ResultReporter
	RateLimiter     *ratelimit.Limiter
	Metrics         ports.Metrics
	Publisher       ports.QueuePublisher
	SaveToDBQueue   string
	QueueName       string
	DeadLetterQueue string

	MaxReportRetries     int
	ReportRetryBaseDelay time.Duration
	ReportRetryMaxDelay  time.Duration

	Logger logger.Logger
}

// NewAfricasTalkingProcessor creates a new AfricasTalking processor.
func NewAfricasTalkingProcessor(cfg *AfricasTalkingProcessorConfig) *AfricasTalkingProcessor {
	maxRetries := cfg.MaxReportRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}
	baseDelay := cfg.ReportRetryBaseDelay
	if baseDelay <= 0 {
		baseDelay = 2 * time.Second
	}
	maxDelay := cfg.ReportRetryMaxDelay
	if maxDelay <= 0 {
		maxDelay = 60 * time.Second
	}

	return &AfricasTalkingProcessor{
		sender:               cfg.Sender,
		reporter:             cfg.Reporter,
		rateLimiter:          cfg.RateLimiter,
		metrics:              cfg.Metrics,
		publisher:            cfg.Publisher,
		saveToDBQueue:        cfg.SaveToDBQueue,
		queueName:            cfg.QueueName,
		deadLetterQueue:      cfg.DeadLetterQueue,
		maxReportRetries:     maxRetries,
		reportRetryBaseDelay: baseDelay,
		reportRetryMaxDelay:  maxDelay,
		log:                  cfg.Logger,
	}
}

// messageOutcome describes how a single message's processing should affect the
// delivery-level ack/nack decision.
type messageOutcome int

const (
	outcomeAck messageOutcome = iota
	outcomeNackRequeue
	outcomeNackDrop
)

// ProcessDelivery processes a delivery from AFRICAS_TALKING_SMS_QUEUE.
// Matches the same signature shape as Processor.ProcessDelivery so it plugs into
// the same consumer-loop pattern used elsewhere in bootstrap/app.go.
func (p *AfricasTalkingProcessor) ProcessDelivery(ctx context.Context, delivery ports.Delivery) error {
	messages := delivery.Messages()
	if len(messages) == 0 {
		p.log.Warn("Received empty AfricasTalking delivery")
		return delivery.Ack()
	}

	requeue := false
	drop := false

	for _, msg := range messages {
		switch p.processMessage(ctx, msg) {
		case outcomeNackRequeue:
			requeue = true
		case outcomeNackDrop:
			drop = true
		}
	}

	// A report-call failure takes priority: requeueing re-attempts the send for
	// every message in this delivery (acceptable — the spec documents this queue
	// as single-message-per-delivery, so this only affects the message that failed).
	if requeue {
		return delivery.Nack(true)
	}
	if drop {
		return delivery.Nack(false)
	}
	return delivery.Ack()
}

// processMessage sends msg via AfricasTalking and reports the result to the PHP API.
func (p *AfricasTalkingProcessor) processMessage(ctx context.Context, msg *domain.Message) messageOutcome {
	msg = msg.Clone()

	if err := msg.Validate(); err != nil {
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"error":      err.Error(),
		}).Warn("AfricasTalking message validation failed")
		return outcomeNackDrop
	}

	// outboxId is what ties the report call back to a PHP outbox row — without a
	// valid one, the report call can never succeed, so there's no point sending.
	if msg.OutboxID <= 0 {
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"outbox_id":  msg.OutboxID,
		}).Error("AfricasTalking message missing valid outboxId, dropping")
		return outcomeNackDrop
	}

	if p.rateLimiter != nil {
		if err := p.rateLimiter.Wait(ctx, domain.NetworkINTNL); err != nil {
			p.log.WithFields(map[string]interface{}{
				"correlator": msg.Correlator,
				"error":      err.Error(),
			}).Warn("AfricasTalking rate limit wait failed")
			if p.metrics != nil {
				p.metrics.IncRateLimitHits(domain.NetworkINTNL)
			}
			return outcomeNackRequeue
		}
	}

	result := p.sender.Send(ctx, msg)

	if err := p.reporter.Report(ctx, result); err != nil {
		return p.handleDownstreamFailure(ctx, msg, "report_failed", err)
	}

	// Save successful and permanently-failed results to SAVE_TO_DB, matching
	// Publisher.PublishResult's behavior for SDP/SMPP. Retryable results aren't
	// saved here — this processor has no delay/retry-queue path of its own, so a
	// retryable send is already terminal by the time it reaches this point.
	if p.publisher != nil && (result.IsSuccess() || result.IsPermanent()) {
		if err := p.publisher.Publish(ctx, p.saveToDBQueue, result.Message); err != nil {
			return p.handleDownstreamFailure(ctx, msg, "save_to_db_failed", err)
		}
	}

	return outcomeAck
}

// handleDownstreamFailure applies a bounded, backed-off retry after a message has
// already been sent via AfricasTalking but a downstream step (the PHP callback
// report, or the SAVE_TO_DB publish) failed. Retry count travels in the message
// body itself — durable across redeliveries — since a plain Nack(requeue=true)
// redelivers the original unmodified AMQP body, not any in-memory mutation.
// Once the cap is exceeded, the message is dead-lettered and dropped rather than
// retried forever.
func (p *AfricasTalkingProcessor) handleDownstreamFailure(ctx context.Context, msg *domain.Message, reason string, downstreamErr error) messageOutcome {
	msg.IncrementRetryCount()

	logFields := map[string]interface{}{
		"correlator":  msg.Correlator,
		"outbox_id":   msg.OutboxID,
		"retry_count": msg.RetryCount,
		"reason":      reason,
		"error":       downstreamErr.Error(),
	}

	if msg.RetryCount > p.maxReportRetries {
		p.log.WithFields(logFields).Error("AfricasTalking downstream retries exhausted, dropping message")
		if p.publisher != nil && p.deadLetterQueue != "" {
			if err := p.publisher.Publish(ctx, p.deadLetterQueue, msg); err != nil {
				p.log.WithFields(logFields).WithError(err).Error("Failed to publish exhausted AfricasTalking message to DLQ")
			}
		}
		return outcomeNackDrop
	}

	backoff := p.retryBackoff(msg.RetryCount)
	p.log.WithFields(logFields).WithField("backoff_ms", backoff.Milliseconds()).
		Warn("AfricasTalking downstream step failed, backing off before retry")

	select {
	case <-time.After(backoff):
	case <-ctx.Done():
		// Shutting down mid-backoff — let the broker requeue the original delivery
		// as-is rather than block shutdown further.
		return outcomeNackRequeue
	}

	if p.publisher == nil || p.queueName == "" {
		return outcomeNackRequeue
	}
	if err := p.publisher.Publish(ctx, p.queueName, msg); err != nil {
		p.log.WithFields(logFields).WithError(err).Error("Failed to republish AfricasTalking message for retry, requeueing as-is")
		return outcomeNackRequeue
	}
	// A fresh copy carrying the incremented retry count is now durably enqueued —
	// the original delivery just needs to be acked, not requeued.
	return outcomeAck
}

// retryBackoff computes exponential backoff for the given retry attempt
// (base * 2^(attempt-1)), capped at reportRetryMaxDelay.
func (p *AfricasTalkingProcessor) retryBackoff(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	backoff := p.reportRetryBaseDelay * time.Duration(uint(1)<<uint(retryCount-1))
	if p.reportRetryMaxDelay > 0 && backoff > p.reportRetryMaxDelay {
		backoff = p.reportRetryMaxDelay
	}
	return backoff
}
