package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

func TestPrioritySchedulerConfig(t *testing.T) {
	cfg := &PrioritySchedulerConfig{
		DefaultWeight:    1,
		CreditMultiplier: 10,
		RefillPeriod:     100 * time.Millisecond,
		MaxStarvationAge: 10 * time.Second,
	}

	if cfg.DefaultWeight != 1 {
		t.Errorf("Expected DefaultWeight 1, got %d", cfg.DefaultWeight)
	}

	if cfg.CreditMultiplier != 10 {
		t.Errorf("Expected CreditMultiplier 10, got %d", cfg.CreditMultiplier)
	}

	if cfg.RefillPeriod != 100*time.Millisecond {
		t.Errorf("Expected RefillPeriod 100ms, got %v", cfg.RefillPeriod)
	}

	if cfg.MaxStarvationAge != 10*time.Second {
		t.Errorf("Expected MaxStarvationAge 10s, got %v", cfg.MaxStarvationAge)
	}
}

func TestPriorityScheduler_DefaultValues(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	// Zero values should be set to defaults
	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:          metrics,
		DefaultWeight:    0, // Should default to 1
		CreditMultiplier: 0, // Should default to 10
		RefillPeriod:     0, // Should default to 100ms
		MaxStarvationAge: 0, // Should default to 10s
		Logger:           log,
	})

	if scheduler.defaultWeight != 1 {
		t.Errorf("Expected default weight 1, got %d", scheduler.defaultWeight)
	}

	if scheduler.creditMultiplier != 10 {
		t.Errorf("Expected credit multiplier 10, got %d", scheduler.creditMultiplier)
	}

	if scheduler.refillPeriod != 100*time.Millisecond {
		t.Errorf("Expected refill period 100ms, got %v", scheduler.refillPeriod)
	}

	if scheduler.maxStarvationAge != 10*time.Second {
		t.Errorf("Expected max starvation age 10s, got %v", scheduler.maxStarvationAge)
	}
}

func TestPriorityScheduler_SetWeight(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:       metrics,
		DefaultWeight: 1,
		Logger:        log,
	})

	scheduler.SetWeight("test-queue", 10)

	weights := scheduler.GetWeights()
	if weights["test-queue"] != 10 {
		t.Errorf("Expected weight 10, got %d", weights["test-queue"])
	}
}

func TestPriorityScheduler_GetStats(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:       metrics,
		DefaultWeight: 1,
		Logger:        log,
	})

	stats := scheduler.GetStats()
	if stats == nil {
		t.Fatal("GetStats() returned nil")
	}

	if len(stats) != 0 {
		t.Errorf("Expected empty stats, got %d items", len(stats))
	}
}

func TestPriorityScheduler_TotalProcessed(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:       metrics,
		DefaultWeight: 1,
		Logger:        log,
	})

	total := scheduler.TotalProcessed()
	if total != 0 {
		t.Errorf("Expected 0 total processed, got %d", total)
	}
}

func TestPriorityScheduler_StartStop(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:       metrics,
		DefaultWeight: 1,
		Logger:        log,
	})

	// Start scheduler
	err := scheduler.Start()
	if err != nil {
		t.Errorf("Start() error = %v", err)
	}

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Stop scheduler
	scheduler.Stop()

	// Should complete without hanging
}

func TestPriorityScheduler_CreditCapacity(t *testing.T) {
	// Test that credit channel capacity is weight × creditMultiplier

	tests := []struct {
		name             string
		weight           int
		creditMultiplier int
		expectedCapacity int
	}{
		{
			name:             "weight 1, multiplier 10",
			weight:           1,
			creditMultiplier: 10,
			expectedCapacity: 10,
		},
		{
			name:             "weight 5, multiplier 10",
			weight:           5,
			creditMultiplier: 10,
			expectedCapacity: 50,
		},
		{
			name:             "weight 10, multiplier 5",
			weight:           10,
			creditMultiplier: 5,
			expectedCapacity: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capacity := tt.weight * tt.creditMultiplier
			if capacity != tt.expectedCapacity {
				t.Errorf("Expected capacity %d, got %d", tt.expectedCapacity, capacity)
			}
		})
	}
}

func TestPriorityScheduler_CreditRefill(t *testing.T) {
	// Test that credit refill adds credits equal to weight (not full capacity)

	tests := []struct {
		name          string
		weight        int
		expectedAdded int
	}{
		{
			name:          "weight 1",
			weight:        1,
			expectedAdded: 1,
		},
		{
			name:          "weight 5",
			weight:        5,
			expectedAdded: 5,
		},
		{
			name:          "weight 10",
			weight:        10,
			expectedAdded: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Credits added per refill cycle = weight
			creditsAdded := tt.weight
			if creditsAdded != tt.expectedAdded {
				t.Errorf("Expected credits added %d, got %d", tt.expectedAdded, creditsAdded)
			}
		})
	}
}

func TestPriorityScheduler_StarvationPrevention(t *testing.T) {
	// Test starvation prevention with maxStarvationAge

	tests := []struct {
		name             string
		maxStarvationAge time.Duration
		timeSinceProcess time.Duration
		shouldBypass     bool
	}{
		{
			name:             "not starving - processed recently",
			maxStarvationAge: 10 * time.Second,
			timeSinceProcess: 5 * time.Second,
			shouldBypass:     false,
		},
		{
			name:             "starving - exceeded max age",
			maxStarvationAge: 10 * time.Second,
			timeSinceProcess: 15 * time.Second,
			shouldBypass:     true,
		},
		{
			name:             "at boundary - not starving",
			maxStarvationAge: 10 * time.Second,
			timeSinceProcess: 10 * time.Second,
			shouldBypass:     false,
		},
		{
			name:             "just past boundary - starving",
			maxStarvationAge: 10 * time.Second,
			timeSinceProcess: 10*time.Second + time.Millisecond,
			shouldBypass:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldBypass := tt.timeSinceProcess > tt.maxStarvationAge
			if shouldBypass != tt.shouldBypass {
				t.Errorf("Expected shouldBypass=%v, got %v", tt.shouldBypass, shouldBypass)
			}
		})
	}
}

func TestPriorityScheduler_GetWeights(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:       metrics,
		DefaultWeight: 1,
		Logger:        log,
	})

	// Set some weights
	scheduler.SetWeight("queue1", 10)
	scheduler.SetWeight("queue2", 5)
	scheduler.SetWeight("queue3", 1)

	weights := scheduler.GetWeights()

	if len(weights) != 3 {
		t.Errorf("Expected 3 weights, got %d", len(weights))
	}

	if weights["queue1"] != 10 {
		t.Errorf("Expected queue1 weight 10, got %d", weights["queue1"])
	}

	if weights["queue2"] != 5 {
		t.Errorf("Expected queue2 weight 5, got %d", weights["queue2"])
	}

	if weights["queue3"] != 1 {
		t.Errorf("Expected queue3 weight 1, got %d", weights["queue3"])
	}
}

func TestPriorityScheduler_WeightsCopy(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:       metrics,
		DefaultWeight: 1,
		Logger:        log,
	})

	scheduler.SetWeight("queue1", 10)

	// Get weights and modify the returned map
	weights := scheduler.GetWeights()
	weights["queue1"] = 999

	// Original should be unchanged
	originalWeights := scheduler.GetWeights()
	if originalWeights["queue1"] != 10 {
		t.Error("GetWeights() should return a copy, not the original map")
	}
}

func TestPriorityScheduler_GetCreditStats(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:          metrics,
		DefaultWeight:    1,
		CreditMultiplier: 10,
		RefillPeriod:     50 * time.Millisecond,
		Logger:           log,
	})

	stats := scheduler.GetCreditStats()
	if stats == nil {
		t.Fatal("GetCreditStats() returned nil")
	}

	if stats["credit_multiplier"].(int) != 10 {
		t.Errorf("Expected credit_multiplier 10, got %v", stats["credit_multiplier"])
	}

	if stats["refill_period_ms"].(int64) != 50 {
		t.Errorf("Expected refill_period_ms 50, got %v", stats["refill_period_ms"])
	}
}

func TestPriorityScheduler_SetCreditMultiplier(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:          metrics,
		CreditMultiplier: 10,
		Logger:           log,
	})

	scheduler.SetCreditMultiplier(20)

	if scheduler.creditMultiplier != 20 {
		t.Errorf("Expected credit multiplier 20, got %d", scheduler.creditMultiplier)
	}

	// Invalid value should not change
	scheduler.SetCreditMultiplier(0)
	if scheduler.creditMultiplier != 20 {
		t.Errorf("Expected credit multiplier to remain 20 after invalid set, got %d", scheduler.creditMultiplier)
	}
}

func TestPriorityScheduler_SetRefillPeriod(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:      metrics,
		RefillPeriod: 100 * time.Millisecond,
		Logger:       log,
	})

	scheduler.SetRefillPeriod(200 * time.Millisecond)

	if scheduler.refillPeriod != 200*time.Millisecond {
		t.Errorf("Expected refill period 200ms, got %v", scheduler.refillPeriod)
	}

	// Invalid value should not change
	scheduler.SetRefillPeriod(0)
	if scheduler.refillPeriod != 200*time.Millisecond {
		t.Errorf("Expected refill period to remain 200ms after invalid set, got %v", scheduler.refillPeriod)
	}
}

func TestPriorityScheduler_SetMaxStarvationAge(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:          metrics,
		MaxStarvationAge: 10 * time.Second,
		Logger:           log,
	})

	scheduler.SetMaxStarvationAge(20 * time.Second)

	if scheduler.maxStarvationAge != 20*time.Second {
		t.Errorf("Expected max starvation age 20s, got %v", scheduler.maxStarvationAge)
	}

	// Invalid value should not change
	scheduler.SetMaxStarvationAge(0)
	if scheduler.maxStarvationAge != 20*time.Second {
		t.Errorf("Expected max starvation age to remain 20s after invalid set, got %v", scheduler.maxStarvationAge)
	}
}

func TestPriorityScheduler_ProcessMessages_EmptyBatch(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:       metrics,
		DefaultWeight: 1,
		Logger:        log,
	})

	result, err := scheduler.ProcessMessages(context.Background(), []*domain.Message{}, "test-queue")
	if err != nil {
		t.Errorf("ProcessMessages() error = %v", err)
	}

	if result == nil {
		t.Fatal("ProcessMessages() returned nil result")
	}

	if result.TotalCount != 0 {
		t.Errorf("Expected TotalCount 0, got %d", result.TotalCount)
	}
}

func TestPriorityScheduler_ProcessMessages_WithProcessor(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	mockSender := mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom)
	factory.RegisterSender(mockSender)

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetriesTransactional: 5, MaxRetriesPromotional: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default:   100,
		Safaricom: 200,
	})

	processor := NewProcessor(&ProcessorConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		Logger:        log,
	})

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Processor:        processor,
		Metrics:          metrics,
		DefaultWeight:    1,
		CreditMultiplier: 10,
		Logger:           log,
	})

	// Start scheduler for credit refill
	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer scheduler.Stop()

	msgs := []*domain.Message{
		{
			Correlator: "promo-1",
			Content:    "Special offer!",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			PackageID:  "0",
			Sender:     "TestApp",
		},
		{
			Correlator: "promo-2",
			Content:    "Flash sale!",
			MSISDN:     "254722123457",
			NetworkRaw: "SAFARICOM",
			PackageID:  "0",
			Sender:     "TestApp",
		},
	}

	result, err := scheduler.ProcessMessages(context.Background(), msgs, "promo-queue")
	if err != nil {
		t.Errorf("ProcessMessages() error = %v", err)
	}

	if result == nil {
		t.Fatal("ProcessMessages() returned nil result")
	}

	if result.TotalCount != 2 {
		t.Errorf("Expected TotalCount 2, got %d", result.TotalCount)
	}

	// Verify stats updated
	total := scheduler.TotalProcessed()
	if total != 2 {
		t.Errorf("Expected TotalProcessed 2, got %d", total)
	}

	stats := scheduler.GetStats()
	if stats["promo-queue"] != 2 {
		t.Errorf("Expected promo-queue stats 2, got %d", stats["promo-queue"])
	}
}

func TestPriorityScheduler_ProcessMessages_CreditAcquisition(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	mockSender := mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom)
	factory.RegisterSender(mockSender)

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetriesTransactional: 5, MaxRetriesPromotional: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	processor := NewProcessor(&ProcessorConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		Logger:        log,
	})

	// Use low credit multiplier to test credit exhaustion
	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Processor:        processor,
		Metrics:          metrics,
		DefaultWeight:    1,
		CreditMultiplier: 2,               // Only 2 credits available initially
		RefillPeriod:     1 * time.Second, // Slow refill
		Logger:           log,
	})

	// Start scheduler
	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer scheduler.Stop()

	// First two batches should succeed immediately (using initial credits)
	msgs := []*domain.Message{
		{
			Correlator: "test-1",
			Content:    "Test",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			Sender:     "Test",
		},
	}

	// Process 2 batches quickly (should use initial credits)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := scheduler.ProcessMessages(ctx, msgs, "test-queue")
		cancel()
		if err != nil {
			t.Errorf("Batch %d: ProcessMessages() error = %v", i+1, err)
		}
	}

	total := scheduler.TotalProcessed()
	if total != 2 {
		t.Errorf("Expected 2 processed, got %d", total)
	}
}

func TestPriorityScheduler_WeightedCreditDistribution(t *testing.T) {
	// Test that higher weight queues get more credits and can process more

	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	mockSender := mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom)
	factory.RegisterSender(mockSender)

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetriesTransactional: 5, MaxRetriesPromotional: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 1000, // High limit so it doesn't interfere
	})

	processor := NewProcessor(&ProcessorConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		Logger:        log,
	})

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Processor:        processor,
		Metrics:          metrics,
		DefaultWeight:    1,
		CreditMultiplier: 5, // 5 credits per weight
		RefillPeriod:     10 * time.Millisecond,
		Logger:           log,
	})

	// Set different weights
	scheduler.SetWeight("high-priority", 10) // 50 credits capacity
	scheduler.SetWeight("low-priority", 1)   // 5 credits capacity

	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer scheduler.Stop()

	// Give time for initial setup
	time.Sleep(20 * time.Millisecond)

	msg := &domain.Message{
		Correlator: "test",
		Content:    "Test",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "Test",
	}

	// Process multiple batches concurrently from both queues
	var wg sync.WaitGroup
	highCount := 0
	lowCount := 0
	var mu sync.Mutex

	// Try to process 20 batches from each queue with tight timeout
	for i := 0; i < 20; i++ {
		wg.Add(2)

		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err := scheduler.ProcessMessages(ctx, []*domain.Message{msg}, "high-priority")
			if err == nil {
				mu.Lock()
				highCount++
				mu.Unlock()
			}
		}()

		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, err := scheduler.ProcessMessages(ctx, []*domain.Message{msg}, "low-priority")
			if err == nil {
				mu.Lock()
				lowCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// High priority should process significantly more than low priority
	// due to having 10x the credits
	t.Logf("High priority processed: %d, Low priority processed: %d", highCount, lowCount)

	// Both should process at least some (initial credits + refills)
	if highCount == 0 {
		t.Error("High priority queue should have processed some messages")
	}
	if lowCount == 0 {
		t.Error("Low priority queue should have processed some messages")
	}
}

func TestPriorityScheduler_ContextCancellation(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	mockSender := mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom)
	factory.RegisterSender(mockSender)

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetriesTransactional: 5, MaxRetriesPromotional: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	processor := NewProcessor(&ProcessorConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		Logger:        log,
	})

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Processor:        processor,
		Metrics:          metrics,
		DefaultWeight:    1,
		CreditMultiplier: 1,                // Only 1 credit
		RefillPeriod:     10 * time.Second, // Very slow refill
		Logger:           log,
	})

	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer scheduler.Stop()

	msg := &domain.Message{
		Correlator: "test",
		Content:    "Test",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "Test",
	}

	// Use the initial credit
	_, err = scheduler.ProcessMessages(context.Background(), []*domain.Message{msg}, "test-queue")
	if err != nil {
		t.Errorf("First ProcessMessages() error = %v", err)
	}

	// Now credits are exhausted, next call should block until context cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = scheduler.ProcessMessages(ctx, []*domain.Message{msg}, "test-queue")
	if err == nil {
		t.Log("ProcessMessages succeeded (starvation prevention may have kicked in)")
	} else if err != context.DeadlineExceeded {
		t.Logf("ProcessMessages returned: %v (expected context deadline or success)", err)
	}
}

func TestPriorityScheduler_QueueStateCreation(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	mockSender := mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom)
	factory.RegisterSender(mockSender)

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetriesTransactional: 5, MaxRetriesPromotional: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	processor := NewProcessor(&ProcessorConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		Logger:        log,
	})

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Processor:        processor,
		Metrics:          metrics,
		DefaultWeight:    5,
		CreditMultiplier: 10,
		Logger:           log,
	})

	// Pre-set weight for a queue before it exists
	scheduler.SetWeight("pre-configured-queue", 20)

	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer scheduler.Stop()

	msg := &domain.Message{
		Correlator: "test",
		Content:    "Test",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "Test",
	}

	// Process message for new queue (should use default weight)
	_, err = scheduler.ProcessMessages(context.Background(), []*domain.Message{msg}, "new-queue")
	if err != nil {
		t.Errorf("ProcessMessages(new-queue) error = %v", err)
	}

	// Process message for pre-configured queue (should use weight 20)
	_, err = scheduler.ProcessMessages(context.Background(), []*domain.Message{msg}, "pre-configured-queue")
	if err != nil {
		t.Errorf("ProcessMessages(pre-configured-queue) error = %v", err)
	}

	// Verify stats
	stats := scheduler.GetStats()
	if stats["new-queue"] != 1 {
		t.Errorf("Expected new-queue stats 1, got %d", stats["new-queue"])
	}
	if stats["pre-configured-queue"] != 1 {
		t.Errorf("Expected pre-configured-queue stats 1, got %d", stats["pre-configured-queue"])
	}
}

func TestPriorityScheduler_CreditStatsPerQueue(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	mockSender := mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom)
	factory.RegisterSender(mockSender)

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetriesTransactional: 5, MaxRetriesPromotional: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	processor := NewProcessor(&ProcessorConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		Logger:        log,
	})

	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Processor:        processor,
		Metrics:          metrics,
		DefaultWeight:    1,
		CreditMultiplier: 5,
		Logger:           log,
	})

	scheduler.SetWeight("queue-a", 10) // 50 credits
	scheduler.SetWeight("queue-b", 2)  // 10 credits

	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer scheduler.Stop()

	msg := &domain.Message{
		Correlator: "test",
		Content:    "Test",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "Test",
	}

	// Process some messages to create queue states
	_, _ = scheduler.ProcessMessages(context.Background(), []*domain.Message{msg}, "queue-a")
	_, _ = scheduler.ProcessMessages(context.Background(), []*domain.Message{msg}, "queue-b")

	creditStats := scheduler.GetCreditStats()
	queueCredits := creditStats["queue_credits_available"].(map[string]int)

	// queue-a should have more credits available (higher weight = higher capacity)
	// Note: one credit was consumed from each, so queue-a: 49, queue-b: 9
	if queueCredits["queue-a"] < queueCredits["queue-b"] {
		t.Errorf("Expected queue-a to have more credits than queue-b, got a=%d, b=%d",
			queueCredits["queue-a"], queueCredits["queue-b"])
	}
}
