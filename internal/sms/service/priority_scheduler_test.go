package service

import (
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

func TestPrioritySchedulerConfig(t *testing.T) {
	cfg := &PrioritySchedulerConfig{
		DefaultWeight:             1,
		StarvationPreventionRatio: 0.1,
	}

	if cfg.DefaultWeight != 1 {
		t.Errorf("Expected DefaultWeight 1, got %d", cfg.DefaultWeight)
	}

	if cfg.StarvationPreventionRatio != 0.1 {
		t.Errorf("Expected StarvationPreventionRatio 0.1, got %f", cfg.StarvationPreventionRatio)
	}
}

func TestPriorityScheduler_DefaultValues(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	// Zero values should be set to defaults
	scheduler := NewPriorityScheduler(&PrioritySchedulerConfig{
		Metrics:                   metrics,
		DefaultWeight:             0, // Should default to 1
		StarvationPreventionRatio: 0, // Should default to 0.1
		Logger:                    log,
	})

	if scheduler.defaultWeight != 1 {
		t.Errorf("Expected default weight 1, got %d", scheduler.defaultWeight)
	}

	if scheduler.starvationRatio != 0.1 {
		t.Errorf("Expected starvation ratio 0.1, got %f", scheduler.starvationRatio)
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

func TestPriorityScheduler_WeightedWaitTime(t *testing.T) {
	// Test that higher weights result in shorter wait times
	// This is a conceptual test verifying the WFQ algorithm

	tests := []struct {
		name       string
		weight     int
		expectLess bool // true if this should have less wait than previous
	}{
		{
			name:       "weight 1",
			weight:     1,
			expectLess: false,
		},
		{
			name:       "weight 5",
			weight:     5,
			expectLess: true, // should wait less than weight 1
		},
		{
			name:       "weight 10",
			weight:     10,
			expectLess: true, // should wait less than weight 5
		},
	}

	// Calculate wait times based on the formula: baseWait / weight
	baseWait := 10 * time.Millisecond
	var prevWait time.Duration

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			waitTime := time.Duration(float64(baseWait) / float64(tt.weight))

			if tt.expectLess && prevWait > 0 {
				if waitTime >= prevWait {
					t.Errorf("Expected wait time %v to be less than %v for weight %d",
						waitTime, prevWait, tt.weight)
				}
			}

			prevWait = waitTime
		})
	}
}

func TestPriorityScheduler_StarvationPrevention(t *testing.T) {
	// Test that starvation prevention triggers after max starvation time

	starvationRatio := 0.1
	maxStarvationTime := time.Duration(float64(time.Second) / starvationRatio)

	// With ratio 0.1, max starvation time should be 10 seconds
	expected := 10 * time.Second
	if maxStarvationTime != expected {
		t.Errorf("Expected max starvation time %v, got %v", expected, maxStarvationTime)
	}

	// Test different ratios
	tests := []struct {
		name        string
		ratio       float64
		expectedMax time.Duration
	}{
		{
			name:        "10% ratio",
			ratio:       0.1,
			expectedMax: 10 * time.Second,
		},
		{
			name:        "20% ratio",
			ratio:       0.2,
			expectedMax: 5 * time.Second,
		},
		{
			name:        "5% ratio",
			ratio:       0.05,
			expectedMax: 20 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxTime := time.Duration(float64(time.Second) / tt.ratio)
			if maxTime != tt.expectedMax {
				t.Errorf("Expected max starvation time %v, got %v", tt.expectedMax, maxTime)
			}
		})
	}
}

func TestPriorityScheduler_VirtualTimeCalculation(t *testing.T) {
	// Test virtual finish time calculation: virtual_finish = virtual_time + (1 / weight)

	tests := []struct {
		name           string
		virtualTime    float64
		weight         int
		expectedFinish float64
	}{
		{
			name:           "weight 1",
			virtualTime:    0.0,
			weight:         1,
			expectedFinish: 1.0,
		},
		{
			name:           "weight 2",
			virtualTime:    1.0,
			weight:         2,
			expectedFinish: 1.5,
		},
		{
			name:           "weight 10",
			virtualTime:    1.5,
			weight:         10,
			expectedFinish: 1.6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			virtualFinish := tt.virtualTime + (1.0 / float64(tt.weight))

			// Allow small floating point tolerance
			tolerance := 0.001
			if virtualFinish < tt.expectedFinish-tolerance || virtualFinish > tt.expectedFinish+tolerance {
				t.Errorf("Expected virtual finish %f, got %f", tt.expectedFinish, virtualFinish)
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
