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
	conn   *Connection
	queues ports.QueueConfig
	log    logger.Logger
}

// PublisherConfig holds configuration for the publisher
type PublisherConfig struct {
	Connection *Connection
	Queues     ports.QueueConfig
	Logger     logger.Logger
}

// NewPublisher creates a new RabbitMQ publisher
func NewPublisher(cfg *PublisherConfig) (*Publisher, error) {
	p := &Publisher{
		conn:   cfg.Connection,
		queues: cfg.Queues,
		log:    cfg.Logger,
	}

	// Declare all output queues
	ctx := context.Background()
	for _, queue := range []string{cfg.Queues.SaveToDBQueue, cfg.Queues.RetryQueue, cfg.Queues.DeadLetterQueue} {
		if err := p.conn.DeclareQueue(ctx, queue); err != nil {
			return nil, err
		}
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

// PublishBatch publishes multiple messages to a queue
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

// PublishResult publishes a send result to the appropriate queue
func (p *Publisher) PublishResult(ctx context.Context, result *domain.SendResult) error {
	var queueName string

	switch result.Type {
	case domain.ResultSuccess:
		queueName = p.queues.SaveToDBQueue
	case domain.ResultRetryable:
		queueName = p.queues.RetryQueue
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

	return p.Publish(ctx, queueName, result.Message)
}

// PublishBatchResults publishes batch results to appropriate queues
func (p *Publisher) PublishBatchResults(ctx context.Context, results *domain.BatchResult) error {
	// Publish successful messages
	for _, result := range results.Successful {
		if err := p.Publish(ctx, p.queues.SaveToDBQueue, result.Message); err != nil {
			p.log.WithError(err).Error("Failed to publish successful message")
			return err
		}
	}

	// Publish retryable messages
	for _, result := range results.Retryable {
		if err := p.Publish(ctx, p.queues.RetryQueue, result.Message); err != nil {
			p.log.WithError(err).Error("Failed to publish retryable message")
			return err
		}
	}

	// Publish failed messages to DLQ
	for _, result := range results.Failed {
		if err := p.Publish(ctx, p.queues.DeadLetterQueue, result.Message); err != nil {
			p.log.WithError(err).Error("Failed to publish failed message to DLQ")
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
