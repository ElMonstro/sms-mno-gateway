package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// TokenCache implements the ports.TokenCache interface using Redis
// CRITICAL: Properly returns errors instead of swallowing them (EM-145 fix)
type TokenCache struct {
	client *redis.Client
	log    logger.Logger
}

// TokenCacheConfig holds configuration for the token cache
type TokenCacheConfig struct {
	Addr     string
	Password string
	DB       int
	Logger   logger.Logger
}

// NewTokenCache creates a new Redis token cache
func NewTokenCache(cfg *TokenCacheConfig) (*TokenCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &TokenCache{
		client: client,
		log:    cfg.Logger,
	}, nil
}

// Get retrieves a token from the cache
// EM-145 fix: Returns errors instead of swallowing them
func (c *TokenCache) Get(ctx context.Context, key string) (string, bool, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		// Key doesn't exist - not an error
		return "", false, nil
	}
	if err != nil {
		// Actual error occurred - return it
		c.log.WithError(err).WithField("key", key).Error("Failed to get token from cache")
		return "", false, err
	}
	return val, true, nil
}

// Set stores a token in the cache with the specified TTL
// EM-145 fix: Returns errors instead of swallowing them
func (c *TokenCache) Set(ctx context.Context, key string, token string, ttl time.Duration) error {
	err := c.client.Set(ctx, key, token, ttl).Err()
	if err != nil {
		c.log.WithError(err).WithField("key", key).Error("Failed to set token in cache")
		return err
	}
	c.log.WithFields(map[string]interface{}{
		"key": key,
		"ttl": ttl.String(),
	}).Debug("Token cached successfully")
	return nil
}

// Delete removes a token from the cache
// EM-145 fix: Returns errors instead of swallowing them
func (c *TokenCache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		c.log.WithError(err).WithField("key", key).Error("Failed to delete token from cache")
		return err
	}
	c.log.WithField("key", key).Debug("Token deleted from cache")
	return nil
}

// Exists checks if a token exists in the cache
func (c *TokenCache) Exists(ctx context.Context, key string) (bool, error) {
	result, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		c.log.WithError(err).WithField("key", key).Error("Failed to check token existence")
		return false, err
	}
	return result > 0, nil
}

// Close closes the Redis client
func (c *TokenCache) Close() error {
	return c.client.Close()
}

// Ping checks the Redis connection
func (c *TokenCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Ensure TokenCache implements ports.TokenCache
var _ ports.TokenCache = (*TokenCache)(nil)
