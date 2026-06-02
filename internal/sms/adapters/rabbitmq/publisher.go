package rabbitmq

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Publisher implements the ports.QueuePublisher interface
type Publisher struct {
	conn             *Connection
	queues           ports.QueueConfig
	gatewayQueueName string
	log              logger.Logger
}

// PublisherConfig holds configuration for the publisher
type PublisherConfig struct {
	Connection *Connection
	Queues     ports.QueueConfig
	Logger     logger.Logger

	// GatewayQueueName is the input queue whose messages are treated as promotional.
	// Messages from any other queue are treated as transactional for retry routing.
	// Matches the GATEWAY_QUEUE_NAME env var.
	GatewayQueueName string

	// Delay queue TTL values — required to declare delay queues with correct x-message-ttl
	TransactionalDelayMs int
	PromotionalDelayMs   int
}

// NewPublisher creates a new RabbitMQ publisher and declares all output queues.
// Delay queues are declared with TTL + dead-letter args so failed messages are
// held for the configured delay before routing to the active retry queues via DLX.
func NewPublisher(cfg *PublisherConfig) (*Publisher, error) {
	p := &Publisher{
		conn:             cfg.Connection,
		queues:           cfg.Queues,
		gatewayQueueName: cfg.GatewayQueueName,
		log:              cfg.Logger,
	}

	ctx := context.Background()

	// Standard output queues
	for _, queue := range []string{cfg.Queues.SaveToDBQueue, cfg.Queues.RetryQueue, cfg.Queues.DeadLetterQueue} {
		if err := p.conn.DeclareQueue(ctx, queue); err != nil {
			return nil, err
		}
	}

	// Active retry queues (no special args — consume directly)
	for _, queue := range []string{cfg.Queues.TransactionalRetryQueue, cfg.Queues.PromotionalRetryQueue} {
		if err := p.conn.DeclareQueue(ctx, queue); err != nil {
			return nil, err
		}
	}

	// Delay queues: messages wait here for TTL ms, then DLX routes them to retry queues
	if err := p.conn.DeclareQueueWithArgs(ctx, cfg.Queues.TransactionalDelayQueue, amqp.Table{
		"x-message-ttl":             int32(cfg.TransactionalDelayMs),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": cfg.Queues.TransactionalRetryQueue,
	}); err != nil {
		return nil, err
	}

	if err := p.conn.DeclareQueueWithArgs(ctx, cfg.Queues.PromotionalDelayQueue, amqp.Table{
		"x-message-ttl":             int32(cfg.PromotionalDelayMs),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": cfg.Queues.PromotionalRetryQueue,
	}); err != nil {
		return nil, err
	}

	return p, nil
}

// Publish publishes a single message to a queue
func (p *Publisher) Publish(ctx context.Context, queueName string, msg *domain.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	channel := p.conn.Channel()
	return channel.PublishWithContext(
		ctx,
		"",        // exchange
		queueName, // routing key (queue name)
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
}

// PublishBatch publishes multiple messages as a single AMQP message to a queue.
// Consumers receive the full batch in one delivery, which is then unpacked.
func (p *Publisher) PublishBatch(ctx context.Context, queueName string, msgs []*domain.Message) error {
	body, err := json.Marshal(msgs)
	if err != nil {
		return err
	}

	channel := p.conn.Channel()
	return channel.PublishWithContext(
		ctx,
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
}

// PublishResult publishes a send result to the appropriate queue.
// Retryable messages are routed to the delay queue matching their type so they
// wait the configured TTL before entering the active retry queue via DLX.
func (p *Publisher) PublishResult(ctx context.Context, result *domain.SendResult) error {
	var queueName string

	switch result.Type {
	case domain.ResultSuccess:
		queueName = p.queues.SaveToDBQueue
	case domain.ResultRetryable:
		if result.Message.IsPromotional(p.gatewayQueueName) {
			queueName = p.queues.PromotionalDelayQueue
		} else {
			queueName = p.queues.TransactionalDelayQueue
		}
	case domain.ResultPermanent:
		queueName = p.queues.DeadLetterQueue
	default:
		queueName = p.queues.SaveToDBQueue
	}

	p.log.WithFields(map[string]interface{}{
		"correlator": result.Message.Correlator,
		"result":     result.Type.String(),
		"queue":      queueName,
	}).Debug("Publishing result")

	if err := p.Publish(ctx, queueName, result.Message); err != nil {
		return err
	}

	// Permanent failures are also saved to DB with status FAILED TO SEND
	if result.Type == domain.ResultPermanent {
		p.log.WithFields(map[string]interface{}{
			"correlator": result.Message.Correlator,
			"queue":      p.queues.SaveToDBQueue,
		}).Debug("Publishing permanent failure to SAVE_TO_DB")
		if err := p.Publish(ctx, p.queues.SaveToDBQueue, result.Message); err != nil {
			return err
		}
	}

	return nil
}

// PublishBatchResults publishes batch results to appropriate queues.
// Retryable messages are grouped by type and published as two batches (one per delay queue)
// rather than N individual publishes, reducing AMQP channel pressure under high error rates.
// DLQ publishes are also batched to prevent channel exhaustion during mass failure events.
func (p *Publisher) PublishBatchResults(ctx context.Context, results *domain.BatchResult) error {
	// Publish successful messages
	for _, result := range results.Successful {
		if err := p.Publish(ctx, p.queues.SaveToDBQueue, result.Message); err != nil {
			p.log.WithError(err).Error("Failed to publish successful message")
			return err
		}
	}

	// Group retryable messages by type, then publish each group as a single batch.
	// This reduces publish calls from N to at most 2.
	var transactionalRetries, promotionalRetries []*domain.Message
	for _, result := range results.Retryable {
		if result.Message.IsPromotional(p.gatewayQueueName) {
			promotionalRetries = append(promotionalRetries, result.Message)
		} else {
			transactionalRetries = append(transactionalRetries, result.Message)
		}
	}

	if len(transactionalRetries) > 0 {
		if err := p.PublishBatch(ctx, p.queues.TransactionalDelayQueue, transactionalRetries); err != nil {
			p.log.WithError(err).Error("Failed to publish transactional retries")
			return err
		}
	}
	if len(promotionalRetries) > 0 {
		if err := p.PublishBatch(ctx, p.queues.PromotionalDelayQueue, promotionalRetries); err != nil {
			p.log.WithError(err).Error("Failed to publish promotional retries")
			return err
		}
	}

	// Batch DLQ and DB publishes for failed messages
	if len(results.Failed) > 0 {
		failedMsgs := make([]*domain.Message, len(results.Failed))
		for i, result := range results.Failed {
			failedMsgs[i] = result.Message
		}

		if err := p.PublishBatch(ctx, p.queues.DeadLetterQueue, failedMsgs); err != nil {
			p.log.WithError(err).Error("Failed to publish failed messages to DLQ")
			return err
		}
		if err := p.PublishBatch(ctx, p.queues.SaveToDBQueue, failedMsgs); err != nil {
			p.log.WithError(err).Error("Failed to publish failed messages to SAVE_TO_DB")
			return err
		}
	}

	p.log.WithFields(map[string]interface{}{
		"successful": results.SuccessCount(),
		"retryable":  results.RetryableCount(),
		"failed":     results.FailedCount(),
	}).Info("Published batch results")

	return nil
}

// Close closes the publisher
func (p *Publisher) Close() error {
	return nil // Connection managed separately
}

// IsConnected returns true if connected
func (p *Publisher) IsConnected() bool {
	return p.conn.IsConnected()
}

// Ensure Publisher implements ports.QueuePublisher
var _ ports.QueuePublisher = (*Publisher)(nil)
