package service

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// AfricasTalkingProcessor processes deliveries from AFRICAS_TALKING_SMS_QUEUE.
// Unlike Processor, it does not publish results to internal RabbitMQ queues —
// it calls the AfricasTalking API directly, then reports the outcome to the PHP
// API over HTTP, and acks the delivery only once that report call succeeds.
// The queue is documented to carry exactly one message per delivery.
type AfricasTalkingProcessor struct {
	sender      ports.MNOSender
	reporter    ports.ResultReporter
	rateLimiter *ratelimit.Limiter
	metrics     ports.Metrics
	log         logger.Logger
}

// AfricasTalkingProcessorConfig holds configuration for the processor.
type AfricasTalkingProcessorConfig struct {
	Sender      ports.MNOSender
	Reporter    ports.ResultReporter
	RateLimiter *ratelimit.Limiter
	Metrics     ports.Metrics
	Logger      logger.Logger
}

// NewAfricasTalkingProcessor creates a new AfricasTalking processor.
func NewAfricasTalkingProcessor(cfg *AfricasTalkingProcessorConfig) *AfricasTalkingProcessor {
	return &AfricasTalkingProcessor{
		sender:      cfg.Sender,
		reporter:    cfg.Reporter,
		rateLimiter: cfg.RateLimiter,
		metrics:     cfg.Metrics,
		log:         cfg.Logger,
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
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"outbox_id":  msg.OutboxID,
			"send_ok":    result.IsSuccess(),
			"error":      err.Error(),
		}).Error("Failed to report AfricasTalking send result, requeueing")
		return outcomeNackRequeue
	}

	return outcomeAck
}
