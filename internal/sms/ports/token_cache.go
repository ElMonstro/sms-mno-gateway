package ports

import (
	"context"
	"time"
)

// TokenCache defines the interface for caching authentication tokens
type TokenCache interface {
	// Get retrieves a token from the cache
	// Returns the token and true if found, or empty string and false if not found
	Get(ctx context.Context, key string) (string, bool, error)

	// Set stores a token in the cache with the specified TTL
	Set(ctx context.Context, key string, token string, ttl time.Duration) error

	// Delete removes a token from the cache
	Delete(ctx context.Context, key string) error

	// Exists checks if a token exists in the cache
	Exists(ctx context.Context, key string) (bool, error)
}

// CacheKey constants for different token types
const (
	// SDPTokenKey is the cache key for Safaricom SDP authentication token
	SDPTokenKey = "SDP_TOKEN_KEY"
)

// DefaultTokenTTL is the default TTL for cached tokens (25 minutes)
const DefaultTokenTTL = 25 * time.Minute
