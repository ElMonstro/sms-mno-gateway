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
// Once sender.Send has produced a result, it is never called again for that
// message. If the report call or the SAVE_TO_DB publish fails afterward,
// retryInProcess retries only that failed step, in-process with exponential
// backoff, up to maxReportRetries attempts. Retrying via the queue instead
// (Nack + redelivery) would restart processMessage from the top and re-invoke
// Send — genuinely re-sending the SMS via the real AfricasTalking API for every
// downstream retry, which is what caused a production incident: a message whose
// send succeeded but whose report kept failing was re-sent dozens of times.
type AfricasTalkingProcessor struct {
	sender          ports.MNOSender
	reporter        ports.ResultReporter
	rateLimiter     *ratelimit.Limiter
	metrics         ports.Metrics
	publisher       ports.QueuePublisher
	saveToDBQueue   string
	deadLetterQueue string

	// maxReportRetries/reportRetryBaseDelay/reportRetryMaxDelay bound the
	// in-process retry of a downstream step (the PHP callback report, or the
	// SAVE_TO_DB publish) after a message has already been sent via
	// AfricasTalking. Without a cap and backoff here, a persistently failing
	// downstream step would retry indefinitely with no delay between attempts.
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

	// Carry the AT-assigned message ID through so SAVE_TO_DB / insert-sent-sms has
	// it available before the report call reaches gateway-dlr-handler — its DLR
	// callback looks up the outbox row by this ID, so publishing it first avoids
	// a race where that lookup runs before the ID has ever been saved anywhere.
	msg.ExternalMessageID = result.ExternalMessageID

	// From this point on, Send is never called again for this message — only the
	// downstream steps below may be retried, and only in-process.

	// Save successful and permanently-failed results to SAVE_TO_DB, matching
	// Publisher.PublishResult's behavior for SDP/SMPP. Retryable results aren't
	// saved here — this processor has no delay/retry-queue path of its own, so a
	// retryable send is already terminal by the time it reaches this point.
	// Deliberately done before the report call below — see the comment above.
	if p.publisher != nil && (result.IsSuccess() || result.IsPermanent()) {
		if err := p.retryInProcess(ctx, msg, "save_to_db_failed", func() error {
			return p.publisher.Publish(ctx, p.saveToDBQueue, result.Message)
		}); err != nil {
			return p.finalizeExhausted(ctx, msg, "save_to_db_failed", err)
		}
	}

	if err := p.retryInProcess(ctx, msg, "report_failed", func() error {
		return p.reporter.Report(ctx, result)
	}); err != nil {
		return p.finalizeExhausted(ctx, msg, "report_failed", err)
	}

	return outcomeAck
}

// retryInProcess retries fn — a downstream step that runs after AfricasTalking
// has already accepted or rejected the send — with exponential backoff, up to
// maxReportRetries additional attempts beyond the first. It never touches
// sender.Send; retrying via the queue instead would restart processMessage from
// the top and re-send the message via the real AfricasTalking API for every
// downstream retry. Returns nil on success, or the last error once attempts are
// exhausted (including ctx.Err() if ctx is cancelled mid-backoff).
func (p *AfricasTalkingProcessor) retryInProcess(ctx context.Context, msg *domain.Message, step string, fn func() error) error {
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		logFields := map[string]interface{}{
			"correlator": msg.Correlator,
			"outbox_id":  msg.OutboxID,
			"step":       step,
			"attempt":    attempt,
			"error":      err.Error(),
		}

		if attempt > p.maxReportRetries {
			return err
		}

		backoff := p.retryBackoff(attempt)
		p.log.WithFields(logFields).WithField("backoff_ms", backoff.Milliseconds()).
			Warn("AfricasTalking downstream step failed, backing off before retry")

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// finalizeExhausted handles a downstream step that never succeeded after all
// in-process retries. On ctx cancellation (shutdown mid-backoff) it lets the
// broker requeue the original delivery as-is rather than block shutdown
// further — accepting the same small risk of a duplicate real send already
// present elsewhere in this codebase for shutdown-time drains, rather than the
// certainty of one on every persistent-failure retry. Otherwise the message is
// dead-lettered and dropped instead of retried forever.
func (p *AfricasTalkingProcessor) finalizeExhausted(ctx context.Context, msg *domain.Message, reason string, err error) messageOutcome {
	if ctx.Err() != nil {
		return outcomeNackRequeue
	}

	p.log.WithFields(map[string]interface{}{
		"correlator": msg.Correlator,
		"outbox_id":  msg.OutboxID,
		"reason":     reason,
		"error":      err.Error(),
	}).Error("AfricasTalking downstream retries exhausted, dropping message")

	if p.publisher != nil && p.deadLetterQueue != "" {
		if dlqErr := p.publisher.Publish(ctx, p.deadLetterQueue, msg); dlqErr != nil {
			p.log.WithError(dlqErr).Error("Failed to publish exhausted AfricasTalking message to DLQ")
		}
	}
	return outcomeNackDrop
}

// retryBackoff computes exponential backoff for the given attempt
// (base * 2^(attempt-1)), capped at reportRetryMaxDelay.
func (p *AfricasTalkingProcessor) retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := p.reportRetryBaseDelay * time.Duration(uint(1)<<uint(attempt-1))
	if p.reportRetryMaxDelay > 0 && backoff > p.reportRetryMaxDelay {
		backoff = p.reportRetryMaxDelay
	}
	return backoff
}
