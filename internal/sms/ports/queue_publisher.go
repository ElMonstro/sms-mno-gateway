package ports

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// QueuePublisher defines the interface for publishing messages to queues
type QueuePublisher interface {
	// Publish publishes a single message to a queue
	Publish(ctx context.Context, queueName string, msg *domain.Message) error

	// PublishBatch publishes multiple messages to a queue
	PublishBatch(ctx context.Context, queueName string, msgs []*domain.Message) error

	// PublishResult publishes a send result to the appropriate queue
	// based on the result type (success, retryable, permanent)
	PublishResult(ctx context.Context, result *domain.SendResult) error

	// PublishBatchResults publishes batch results to appropriate queues
	PublishBatchResults(ctx context.Context, results *domain.BatchResult) error

	// Close gracefully closes the publisher
	Close() error

	// IsConnected returns true if the publisher is connected
	IsConnected() bool
}

// QueueConfig holds queue configuration
type QueueConfig struct {
	// SaveToDBQueue is the queue for successful messages
	SaveToDBQueue string

	// RetryQueue is the queue for retryable messages
	RetryQueue string

	// DeadLetterQueue is the queue for permanently failed messages
	DeadLetterQueue string
}
