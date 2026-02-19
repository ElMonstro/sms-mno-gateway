package ports

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// Delivery represents a message delivery from the queue
// It wraps the message batch and provides acknowledgment methods
type Delivery interface {
	// Messages returns the batch of messages
	Messages() []*domain.Message

	// Ack acknowledges the delivery, removing it from the queue
	Ack() error

	// Nack negatively acknowledges the delivery
	// If requeue is true, the message will be requeued
	Nack(requeue bool) error

	// DeliveryTag returns the unique identifier for this delivery
	DeliveryTag() uint64
}

// QueueConsumer defines the interface for consuming messages from a queue
type QueueConsumer interface {
	// Consume starts consuming messages from the queue
	// It returns a channel of deliveries
	// The consumer should handle reconnection internally
	Consume(ctx context.Context) (<-chan Delivery, error)

	// QueueName returns the name of the queue being consumed
	QueueName() string

	// Close gracefully closes the consumer
	Close() error

	// IsConnected returns true if the consumer is connected
	IsConnected() bool
}

// MultiQueueConsumer consumes from multiple queues
type MultiQueueConsumer interface {
	// AddQueue adds a queue to consume from
	AddQueue(queueName string) error

	// Consume starts consuming from all queues
	// Returns a unified channel of deliveries from all queues
	Consume(ctx context.Context) (<-chan Delivery, error)

	// Close gracefully closes all consumers
	Close() error
}
