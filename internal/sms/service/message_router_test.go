package service

import (
	"context"
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

func TestMessageRouter_RouteDelivery_EmptyDelivery(t *testing.T) {
	log := logger.NewNoop()

	router := NewMessageRouter(&MessageRouterConfig{
		Logger: log,
	})

	// Empty delivery should just ack
	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{})
	err := router.RouteDelivery(context.Background(), delivery, "test-queue")

	if err != nil {
		t.Errorf("RouteDelivery() error = %v, want nil", err)
	}

	if !delivery.AckCalled {
		t.Error("Expected Ack() to be called for empty delivery")
	}
}

func TestMessageRouter_RouteDelivery_TransactionalMessages(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	// Create transactional messages
	msgs := []*domain.Message{
		{
			Correlator: "tx-1",
			Content:    "OTP: 123456",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			PackageID:  "TRANSACTIONAL",
		},
		{
			Correlator: "tx-2",
			Content:    "Your code is 789",
			MSISDN:     "254722123457",
			NetworkRaw: "SAFARICOM",
			PackageID:  "TRANSACTIONAL",
		},
	}

	router := NewMessageRouter(&MessageRouterConfig{
		Metrics: metrics,
		Logger:  log,
	})

	delivery := mocks.NewMockDeliveryWithMessages(msgs)
	err := router.RouteDelivery(context.Background(), delivery, "test-queue")

	if err != nil {
		t.Errorf("RouteDelivery() error = %v, want nil", err)
	}

	// Without a transactional handler, messages won't be counted in transactionalCount
	// but they will be identified as transactional (no promotional count)
	_, promotional := router.GetStats()
	if promotional != 0 {
		t.Errorf("Expected 0 promotional messages, got %d", promotional)
	}

	// Verify messages are correctly identified as transactional
	for _, msg := range msgs {
		if !msg.IsTransactional() {
			t.Errorf("Message %s should be transactional", msg.Correlator)
		}
	}
}

func TestMessageRouter_RouteDelivery_PromotionalMessages(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	// Create promotional messages
	msgs := []*domain.Message{
		{
			Correlator: "promo-1",
			Content:    "Special offer!",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			PackageID:  "0",
		},
		{
			Correlator: "promo-2",
			Content:    "Flash sale today!",
			MSISDN:     "254722123457",
			NetworkRaw: "SAFARICOM",
			PackageID:  "",
		},
	}

	router := NewMessageRouter(&MessageRouterConfig{
		Metrics: metrics,
		Logger:  log,
	})

	delivery := mocks.NewMockDeliveryWithMessages(msgs)
	err := router.RouteDelivery(context.Background(), delivery, "test-queue")

	if err != nil {
		t.Errorf("RouteDelivery() error = %v, want nil", err)
	}

	// Check promotional count
	transactional, promotional := router.GetStats()
	if transactional != 0 {
		t.Errorf("Expected 0 transactional messages, got %d", transactional)
	}
	if promotional != 2 {
		t.Errorf("Expected 2 promotional messages, got %d", promotional)
	}
}

func TestMessageRouter_RouteDelivery_MixedMessages(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()

	// Create mixed messages
	msgs := []*domain.Message{
		{
			Correlator: "tx-1",
			Content:    "OTP: 123456",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			PackageID:  "TRANSACTIONAL",
		},
		{
			Correlator: "promo-1",
			Content:    "Special offer!",
			MSISDN:     "254722123457",
			NetworkRaw: "SAFARICOM",
			PackageID:  "0",
		},
		{
			Correlator: "tx-2",
			Content:    "Your code: 789",
			MSISDN:     "254722123458",
			NetworkRaw: "SAFARICOM",
			PackageID:  "TRANSACTIONAL",
		},
	}

	router := NewMessageRouter(&MessageRouterConfig{
		Metrics: metrics,
		Logger:  log,
	})

	delivery := mocks.NewMockDeliveryWithMessages(msgs)
	err := router.RouteDelivery(context.Background(), delivery, "test-queue")

	if err != nil {
		t.Errorf("RouteDelivery() error = %v, want nil", err)
	}

	// Without a transactional handler, transactional messages are identified but not counted
	// Only promotional messages are counted
	_, promotional := router.GetStats()
	if promotional != 1 {
		t.Errorf("Expected 1 promotional message, got %d", promotional)
	}

	// Verify messages are correctly classified
	transactionalCount := 0
	promotionalCount := 0
	for _, msg := range msgs {
		if msg.IsTransactional() {
			transactionalCount++
		} else {
			promotionalCount++
		}
	}
	if transactionalCount != 2 {
		t.Errorf("Expected 2 transactional messages, got %d", transactionalCount)
	}
	if promotionalCount != 1 {
		t.Errorf("Expected 1 promotional message, got %d", promotionalCount)
	}
}

func TestMessageRouter_Stats(t *testing.T) {
	log := logger.NewNoop()

	router := NewMessageRouter(&MessageRouterConfig{
		Logger: log,
	})

	stats := router.Stats()

	if stats == nil {
		t.Fatal("Stats() returned nil")
	}

	// Initial stats should be zero
	if stats["transactional_routed"] != 0 {
		t.Errorf("Expected transactional_routed 0, got %d", stats["transactional_routed"])
	}

	if stats["promotional_routed"] != 0 {
		t.Errorf("Expected promotional_routed 0, got %d", stats["promotional_routed"])
	}
}

func TestMessageRouter_ProcessDeliveryWithPriority(t *testing.T) {
	log := logger.NewNoop()

	router := NewMessageRouter(&MessageRouterConfig{
		Logger: log,
	})

	msgs := []*domain.Message{
		{
			Correlator: "test-1",
			Content:    "Test message",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			PackageID:  "0",
		},
	}

	delivery := mocks.NewMockDeliveryWithMessages(msgs)

	// ProcessDeliveryWithPriority should work the same as RouteDelivery
	err := router.ProcessDeliveryWithPriority(context.Background(), delivery, "test-queue")
	if err != nil {
		t.Errorf("ProcessDeliveryWithPriority() error = %v, want nil", err)
	}
}

func TestMessageRouterConfig(t *testing.T) {
	log := logger.NewNoop()

	cfg := &MessageRouterConfig{
		TransactionalHandler: nil,
		Scheduler:            nil,
		Processor:            nil,
		Logger:               log,
	}

	router := NewMessageRouter(cfg)

	if router == nil {
		t.Fatal("NewMessageRouter() returned nil")
	}

	if router.log == nil {
		t.Error("Router logger not set")
	}
}
