package ports

import (
	"context"
)

// PriorityStore defines the interface for managing queue priority weights
// Weights are stored in Redis and can be hot-reloaded without restart
type PriorityStore interface {
	// GetQueueWeights retrieves all queue weights as a map
	GetQueueWeights(ctx context.Context) (map[string]int, error)

	// GetQueueWeight retrieves the weight for a specific queue
	// Returns defaultWeight if the queue is not configured
	GetQueueWeight(ctx context.Context, queueName string, defaultWeight int) (int, error)

	// SetQueueWeight sets the weight for a specific queue
	SetQueueWeight(ctx context.Context, queueName string, weight int) error

	// SetQueueWeights sets multiple queue weights at once
	SetQueueWeights(ctx context.Context, weights map[string]int) error

	// WatchWeights returns a channel that receives updated weights when they change
	// The channel will receive the full weight map on each change
	WatchWeights(ctx context.Context) (<-chan map[string]int, error)

	// Close cleans up any resources (subscriptions, connections)
	Close() error
}
