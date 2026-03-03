package service

import (
	"context"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

func TestTransactionalHandlerConfig(t *testing.T) {
	cfg := &TransactionalHandlerConfig{
		WorkerCount: 5,
	}

	if cfg.WorkerCount != 5 {
		t.Errorf("Expected WorkerCount 5, got %d", cfg.WorkerCount)
	}
}

func TestTransactionalHandlerConfig_DefaultWorkerCount(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	factory.RegisterSender(mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom))

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	// WorkerCount = 0 should default to 5
	handler := NewTransactionalHandler(&TransactionalHandlerConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		WorkerCount:   0,
		Logger:        log,
	})

	if handler.workerCount != 5 {
		t.Errorf("Expected default WorkerCount 5, got %d", handler.workerCount)
	}
}

func TestTransactionalHandler_Stats(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	factory.RegisterSender(mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom))

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	handler := NewTransactionalHandler(&TransactionalHandlerConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		WorkerCount:   2,
		Logger:        log,
	})

	processed, failed := handler.Stats()
	if processed != 0 {
		t.Errorf("Expected 0 processed, got %d", processed)
	}
	if failed != 0 {
		t.Errorf("Expected 0 failed, got %d", failed)
	}
}

func TestTransactionalHandler_QueueDepth(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	factory.RegisterSender(mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom))

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	handler := NewTransactionalHandler(&TransactionalHandlerConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		WorkerCount:   2,
		Logger:        log,
	})

	depth := handler.QueueDepth()
	if depth != 0 {
		t.Errorf("Expected queue depth 0, got %d", depth)
	}
}

func TestTransactionalHandler_HandleSync(t *testing.T) {
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
		MaxRetries: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default:   100,
		Safaricom: 200,
	})

	handler := NewTransactionalHandler(&TransactionalHandlerConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		WorkerCount:   2,
		Logger:        log,
	})

	msg := &domain.Message{
		Correlator: "tx-sync-1",
		Content:    "OTP: 123456",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		PackageID:  "TRANSACTIONAL",
		Sender:     "TestApp",
	}

	result := handler.HandleSync(context.Background(), msg)

	if result == nil {
		t.Fatal("HandleSync() returned nil result")
	}

	if result.Type != domain.ResultSuccess {
		t.Errorf("Expected success result, got %v", result.Type)
	}

	// Verify message was sent
	sentMsgs := mockSender.GetSentMessages()
	if len(sentMsgs) != 1 {
		t.Errorf("Expected 1 sent message, got %d", len(sentMsgs))
	}
}

func TestTransactionalHandler_HandleSync_InvalidMessage(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	factory.RegisterSender(mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom))

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	handler := NewTransactionalHandler(&TransactionalHandlerConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		WorkerCount:   2,
		Logger:        log,
	})

	// Invalid message - missing required fields
	msg := &domain.Message{
		Correlator: "tx-invalid",
		Content:    "", // Empty content
		MSISDN:     "", // Empty MSISDN
		NetworkRaw: "SAFARICOM",
	}

	result := handler.HandleSync(context.Background(), msg)

	if result == nil {
		t.Fatal("HandleSync() returned nil result")
	}

	if result.Type != domain.ResultPermanent {
		t.Errorf("Expected permanent failure for invalid message, got %v", result.Type)
	}
}

func TestTransactionalHandler_StartStop(t *testing.T) {
	log := logger.NewNoop()
	metrics := mocks.NewMockMetrics()
	factory := mocks.NewMockMNOSenderFactory()
	factory.RegisterSender(mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom))

	router := NewRouter(factory, log)
	publisher := mocks.NewMockQueuePublisher()
	resultHandler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default: 100,
	})

	handler := NewTransactionalHandler(&TransactionalHandlerConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		WorkerCount:   2,
		Logger:        log,
	})

	// Start handler
	handler.Start()

	// Give workers time to start
	time.Sleep(50 * time.Millisecond)

	// Stop handler
	handler.Stop()

	// Should complete without hanging
}

func TestTransactionalHandler_Handle(t *testing.T) {
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
		MaxRetries: 10,
		Logger:     log,
	})
	rateLimiter := ratelimit.New(&ratelimit.Config{
		Default:   100,
		Safaricom: 200,
	})

	handler := NewTransactionalHandler(&TransactionalHandlerConfig{
		Router:        router,
		ResultHandler: resultHandler,
		RateLimiter:   rateLimiter,
		Metrics:       metrics,
		WorkerCount:   2,
		Logger:        log,
	})

	// Start handler
	handler.Start()
	defer handler.Stop()

	msg := &domain.Message{
		Correlator: "tx-async-1",
		Content:    "OTP: 123456",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		PackageID:  "TRANSACTIONAL",
		Sender:     "TestApp",
	}

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{msg})

	err := handler.Handle(context.Background(), msg, delivery, "test-queue")
	if err != nil {
		t.Errorf("Handle() error = %v, want nil", err)
	}

	// Give worker time to process
	time.Sleep(100 * time.Millisecond)

	// Verify message was processed
	processed, _ := handler.Stats()
	if processed != 1 {
		t.Errorf("Expected 1 processed, got %d", processed)
	}
}
