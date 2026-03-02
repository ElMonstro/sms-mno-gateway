package redis

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// PriorityStore implements the ports.PriorityStore interface using Redis
// Queue weights are stored in a Redis hash for efficient retrieval
// Supports hot-reload via Redis Pub/Sub notifications
type PriorityStore struct {
	client         *redis.Client
	weightsKey     string
	pubsubKey      string
	log            logger.Logger
	defaultWeights map[string]int

	// For caching weights locally to reduce Redis calls
	mu            sync.RWMutex
	cachedWeights map[string]int
	cacheValid    bool

	// For cleanup
	cancelFunc context.CancelFunc
}

// PriorityStoreConfig holds configuration for the priority store
type PriorityStoreConfig struct {
	Client         *redis.Client
	WeightsKey     string         // Redis hash key for queue weights
	DefaultWeights map[string]int // Initial weights if Redis is empty
	Logger         logger.Logger
}

// NewPriorityStore creates a new Redis-backed priority store
func NewPriorityStore(cfg *PriorityStoreConfig) (*PriorityStore, error) {
	store := &PriorityStore{
		client:         cfg.Client,
		weightsKey:     cfg.WeightsKey,
		pubsubKey:      cfg.WeightsKey + ":notifications",
		log:            cfg.Logger,
		defaultWeights: cfg.DefaultWeights,
		cachedWeights:  make(map[string]int),
	}

	// Initialize with default weights if Redis is empty
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := cfg.Client.Exists(ctx, cfg.WeightsKey).Result()
	if err != nil {
		return nil, err
	}

	if exists == 0 && len(cfg.DefaultWeights) > 0 {
		store.log.Info("Initializing priority weights in Redis with defaults")
		if err := store.SetQueueWeights(ctx, cfg.DefaultWeights); err != nil {
			return nil, err
		}
	}

	// Load initial weights into cache
	if err := store.refreshCache(ctx); err != nil {
		store.log.WithError(err).Warn("Failed to load initial weights, using defaults")
		store.mu.Lock()
		store.cachedWeights = cfg.DefaultWeights
		store.cacheValid = true
		store.mu.Unlock()
	}

	return store, nil
}

// GetQueueWeights retrieves all queue weights as a map
func (s *PriorityStore) GetQueueWeights(ctx context.Context) (map[string]int, error) {
	// Try cache first
	s.mu.RLock()
	if s.cacheValid {
		weights := make(map[string]int, len(s.cachedWeights))
		for k, v := range s.cachedWeights {
			weights[k] = v
		}
		s.mu.RUnlock()
		return weights, nil
	}
	s.mu.RUnlock()

	// Cache miss - fetch from Redis
	return s.fetchWeightsFromRedis(ctx)
}

// GetQueueWeight retrieves the weight for a specific queue
func (s *PriorityStore) GetQueueWeight(ctx context.Context, queueName string, defaultWeight int) (int, error) {
	// Try cache first
	s.mu.RLock()
	if s.cacheValid {
		if weight, ok := s.cachedWeights[queueName]; ok {
			s.mu.RUnlock()
			return weight, nil
		}
		s.mu.RUnlock()
		return defaultWeight, nil
	}
	s.mu.RUnlock()

	// Cache miss - fetch from Redis
	result, err := s.client.HGet(ctx, s.weightsKey, queueName).Result()
	if err == redis.Nil {
		return defaultWeight, nil
	}
	if err != nil {
		s.log.WithError(err).WithField("queue", queueName).Error("Failed to get queue weight")
		return defaultWeight, err
	}

	weight, err := strconv.Atoi(result)
	if err != nil {
		s.log.WithError(err).WithField("queue", queueName).Error("Invalid weight value in Redis")
		return defaultWeight, nil
	}

	return weight, nil
}

// SetQueueWeight sets the weight for a specific queue
func (s *PriorityStore) SetQueueWeight(ctx context.Context, queueName string, weight int) error {
	err := s.client.HSet(ctx, s.weightsKey, queueName, weight).Err()
	if err != nil {
		s.log.WithError(err).WithFields(map[string]interface{}{
			"queue":  queueName,
			"weight": weight,
		}).Error("Failed to set queue weight")
		return err
	}

	// Invalidate cache
	s.mu.Lock()
	s.cacheValid = false
	s.mu.Unlock()

	// Notify watchers
	s.notifyChange(ctx)

	s.log.WithFields(map[string]interface{}{
		"queue":  queueName,
		"weight": weight,
	}).Info("Queue weight updated")

	return nil
}

// SetQueueWeights sets multiple queue weights at once
func (s *PriorityStore) SetQueueWeights(ctx context.Context, weights map[string]int) error {
	if len(weights) == 0 {
		return nil
	}

	// Convert to string map for Redis
	args := make([]interface{}, 0, len(weights)*2)
	for queue, weight := range weights {
		args = append(args, queue, weight)
	}

	err := s.client.HSet(ctx, s.weightsKey, args...).Err()
	if err != nil {
		s.log.WithError(err).Error("Failed to set queue weights")
		return err
	}

	// Invalidate cache
	s.mu.Lock()
	s.cacheValid = false
	s.mu.Unlock()

	// Notify watchers
	s.notifyChange(ctx)

	s.log.WithField("count", len(weights)).Info("Queue weights updated")

	return nil
}

// WatchWeights returns a channel that receives updated weights when they change
func (s *PriorityStore) WatchWeights(ctx context.Context) (<-chan map[string]int, error) {
	watchChan := make(chan map[string]int, 1)

	// Create a cancellable context for the subscription
	watchCtx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel

	// Subscribe to notifications
	pubsub := s.client.Subscribe(watchCtx, s.pubsubKey)

	// Verify subscription
	_, err := pubsub.Receive(watchCtx)
	if err != nil {
		cancel()
		return nil, err
	}

	// Start goroutine to watch for changes
	go func() {
		defer close(watchChan)
		defer pubsub.Close()

		ch := pubsub.Channel()
		for {
			select {
			case <-watchCtx.Done():
				return
			case msg := <-ch:
				if msg == nil {
					continue
				}
				// Fetch updated weights
				weights, err := s.fetchWeightsFromRedis(watchCtx)
				if err != nil {
					s.log.WithError(err).Error("Failed to fetch weights after notification")
					continue
				}
				// Send to channel (non-blocking)
				select {
				case watchChan <- weights:
					s.log.Info("Sent updated weights to watcher")
				default:
					s.log.Warn("Watcher channel full, skipping update")
				}
			}
		}
	}()

	s.log.Info("Started watching for priority weight changes")

	return watchChan, nil
}

// Close cleans up resources
func (s *PriorityStore) Close() error {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	return nil
}

// fetchWeightsFromRedis fetches all weights from Redis and updates cache
func (s *PriorityStore) fetchWeightsFromRedis(ctx context.Context) (map[string]int, error) {
	result, err := s.client.HGetAll(ctx, s.weightsKey).Result()
	if err != nil {
		return nil, err
	}

	weights := make(map[string]int, len(result))
	for queue, weightStr := range result {
		weight, err := strconv.Atoi(weightStr)
		if err != nil {
			s.log.WithError(err).WithField("queue", queue).Warn("Invalid weight value, skipping")
			continue
		}
		weights[queue] = weight
	}

	// Update cache
	s.mu.Lock()
	s.cachedWeights = weights
	s.cacheValid = true
	s.mu.Unlock()

	return weights, nil
}

// refreshCache refreshes the local cache from Redis
func (s *PriorityStore) refreshCache(ctx context.Context) error {
	_, err := s.fetchWeightsFromRedis(ctx)
	return err
}

// notifyChange publishes a notification that weights have changed
func (s *PriorityStore) notifyChange(ctx context.Context) {
	err := s.client.Publish(ctx, s.pubsubKey, "weights_updated").Err()
	if err != nil {
		s.log.WithError(err).Warn("Failed to publish weight change notification")
	}
}

// Ensure PriorityStore implements ports.PriorityStore
var _ ports.PriorityStore = (*PriorityStore)(nil)
