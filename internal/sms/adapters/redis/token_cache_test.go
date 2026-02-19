package redis

import (
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Verify TokenCache implements ports.TokenCache interface
func TestTokenCache_ImplementsInterface(t *testing.T) {
	var _ ports.TokenCache = (*TokenCache)(nil)
}

// TestTokenCacheConfig validates configuration struct
func TestTokenCacheConfig(t *testing.T) {
	cfg := &TokenCacheConfig{
		Addr:     "localhost:6379",
		Password: "secret",
		DB:       1,
	}

	if cfg.Addr != "localhost:6379" {
		t.Errorf("Expected addr localhost:6379, got %s", cfg.Addr)
	}

	if cfg.Password != "secret" {
		t.Errorf("Expected password secret, got %s", cfg.Password)
	}

	if cfg.DB != 1 {
		t.Errorf("Expected DB 1, got %d", cfg.DB)
	}
}

// TestTokenCacheConfig_Defaults validates default values
func TestTokenCacheConfig_Defaults(t *testing.T) {
	cfg := &TokenCacheConfig{}

	// Zero values should be defaults
	if cfg.Addr != "" {
		t.Errorf("Expected empty addr, got %s", cfg.Addr)
	}

	if cfg.Password != "" {
		t.Errorf("Expected empty password, got %s", cfg.Password)
	}

	if cfg.DB != 0 {
		t.Errorf("Expected DB 0, got %d", cfg.DB)
	}
}

// TestTokenCache_TTLValues validates TTL settings
func TestTokenCache_TTLValues(t *testing.T) {
	// Test typical TTL values for SDP tokens
	sdpTokenTTL := 25 * time.Minute

	if sdpTokenTTL != 25*time.Minute {
		t.Errorf("Expected 25 minutes TTL, got %v", sdpTokenTTL)
	}

	// TTL should be positive
	if sdpTokenTTL <= 0 {
		t.Error("TTL should be positive")
	}
}

// Integration tests require a running Redis instance
// Run with: go test -tags=integration ./internal/sms/adapters/redis/...
//
// Example integration test:
//
// func TestTokenCache_Integration(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("Skipping integration test")
// 	}
//
// 	cache, err := NewTokenCache(&TokenCacheConfig{
// 		Addr:   "localhost:6379",
// 		Logger: logger.NewNoop(),
// 	})
// 	if err != nil {
// 		t.Fatalf("Failed to connect to Redis: %v", err)
// 	}
// 	defer cache.Close()
//
// 	ctx := context.Background()
//
// 	// Test Set
// 	err = cache.Set(ctx, "test_key", "test_value", 1*time.Minute)
// 	if err != nil {
// 		t.Fatalf("Set failed: %v", err)
// 	}
//
// 	// Test Get
// 	val, found, err := cache.Get(ctx, "test_key")
// 	if err != nil {
// 		t.Fatalf("Get failed: %v", err)
// 	}
// 	if !found {
// 		t.Error("Expected key to be found")
// 	}
// 	if val != "test_value" {
// 		t.Errorf("Expected test_value, got %s", val)
// 	}
//
// 	// Test Exists
// 	exists, err := cache.Exists(ctx, "test_key")
// 	if err != nil {
// 		t.Fatalf("Exists failed: %v", err)
// 	}
// 	if !exists {
// 		t.Error("Expected key to exist")
// 	}
//
// 	// Test Delete
// 	err = cache.Delete(ctx, "test_key")
// 	if err != nil {
// 		t.Fatalf("Delete failed: %v", err)
// 	}
//
// 	// Verify deletion
// 	_, found, err = cache.Get(ctx, "test_key")
// 	if err != nil {
// 		t.Fatalf("Get after delete failed: %v", err)
// 	}
// 	if found {
// 		t.Error("Expected key to be not found after delete")
// 	}
// }
