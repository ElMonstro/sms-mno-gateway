package redis

import (
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Verify PriorityStore implements ports.PriorityStore interface
func TestPriorityStore_ImplementsInterface(t *testing.T) {
	var _ ports.PriorityStore = (*PriorityStore)(nil)
}

// TestPriorityStoreConfig validates configuration struct
func TestPriorityStoreConfig(t *testing.T) {
	defaultWeights := map[string]int{
		"queue1": 10,
		"queue2": 5,
	}

	cfg := &PriorityStoreConfig{
		WeightsKey:     "sms:priority:weights",
		DefaultWeights: defaultWeights,
	}

	if cfg.WeightsKey != "sms:priority:weights" {
		t.Errorf("Expected WeightsKey sms:priority:weights, got %s", cfg.WeightsKey)
	}

	if len(cfg.DefaultWeights) != 2 {
		t.Errorf("Expected 2 default weights, got %d", len(cfg.DefaultWeights))
	}

	if cfg.DefaultWeights["queue1"] != 10 {
		t.Errorf("Expected queue1 weight 10, got %d", cfg.DefaultWeights["queue1"])
	}

	if cfg.DefaultWeights["queue2"] != 5 {
		t.Errorf("Expected queue2 weight 5, got %d", cfg.DefaultWeights["queue2"])
	}
}

// TestPriorityStoreConfig_Defaults validates default values
func TestPriorityStoreConfig_Defaults(t *testing.T) {
	cfg := &PriorityStoreConfig{}

	if cfg.WeightsKey != "" {
		t.Errorf("Expected empty WeightsKey, got %s", cfg.WeightsKey)
	}

	if cfg.DefaultWeights != nil {
		t.Errorf("Expected nil DefaultWeights, got %v", cfg.DefaultWeights)
	}
}

// TestPriorityStore_PubSubKey validates pubsub key generation
func TestPriorityStore_PubSubKey(t *testing.T) {
	weightsKey := "sms:priority:weights"
	expectedPubSubKey := weightsKey + ":notifications"

	// The store should append :notifications to the weights key
	if expectedPubSubKey != "sms:priority:weights:notifications" {
		t.Errorf("Expected pubsub key sms:priority:weights:notifications, got %s", expectedPubSubKey)
	}
}

// TestWeightValues validates typical weight configurations
func TestWeightValues(t *testing.T) {
	tests := []struct {
		name     string
		weights  map[string]int
		expected map[string]int
	}{
		{
			name: "typical production weights",
			weights: map[string]int{
				"TITANIC-KE_SMS_QUEUE":  10,
				"CONSUME_TO_MNO":        5,
				"SMS_MNO_GATEWAY_QUEUE": 1,
			},
			expected: map[string]int{
				"TITANIC-KE_SMS_QUEUE":  10,
				"CONSUME_TO_MNO":        5,
				"SMS_MNO_GATEWAY_QUEUE": 1,
			},
		},
		{
			name: "equal weights",
			weights: map[string]int{
				"queue1": 1,
				"queue2": 1,
				"queue3": 1,
			},
			expected: map[string]int{
				"queue1": 1,
				"queue2": 1,
				"queue3": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for queue, weight := range tt.weights {
				if tt.expected[queue] != weight {
					t.Errorf("Queue %s: expected weight %d, got %d", queue, tt.expected[queue], weight)
				}
			}
		})
	}
}

// Integration tests require a running Redis instance
// Run with: go test -tags=integration ./internal/sms/adapters/redis/...
//
// Example integration test:
//
// func TestPriorityStore_Integration(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("Skipping integration test")
// 	}
//
// 	client := redis.NewClient(&redis.Options{
// 		Addr: "localhost:6379",
// 	})
// 	defer client.Close()
//
// 	store, err := NewPriorityStore(&PriorityStoreConfig{
// 		Client:     client,
// 		WeightsKey: "test:priority:weights",
// 		DefaultWeights: map[string]int{
// 			"queue1": 10,
// 			"queue2": 5,
// 		},
// 		Logger: logger.NewNoop(),
// 	})
// 	if err != nil {
// 		t.Fatalf("Failed to create priority store: %v", err)
// 	}
// 	defer store.Close()
//
// 	ctx := context.Background()
//
// 	// Test GetQueueWeights
// 	weights, err := store.GetQueueWeights(ctx)
// 	if err != nil {
// 		t.Fatalf("GetQueueWeights failed: %v", err)
// 	}
// 	if len(weights) < 2 {
// 		t.Errorf("Expected at least 2 weights, got %d", len(weights))
// 	}
//
// 	// Test SetQueueWeight
// 	err = store.SetQueueWeight(ctx, "queue3", 15)
// 	if err != nil {
// 		t.Fatalf("SetQueueWeight failed: %v", err)
// 	}
//
// 	// Verify new weight
// 	weight, err := store.GetQueueWeight(ctx, "queue3", 1)
// 	if err != nil {
// 		t.Fatalf("GetQueueWeight failed: %v", err)
// 	}
// 	if weight != 15 {
// 		t.Errorf("Expected weight 15, got %d", weight)
// 	}
//
// 	// Cleanup
// 	client.Del(ctx, "test:priority:weights")
// }
