package service

import (
	"context"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

func TestResultHandler_HandleResult_Success(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()
	log := logger.NewNoop()

	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
	}

	result := domain.NewSuccessResult(msg, "accepted", 100*time.Millisecond)

	err := handler.HandleResult(context.Background(), result)
	if err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	// Verify published to save_to_db
	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Errorf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "save_to_db" {
		t.Errorf("Expected queue type 'save_to_db', got %q", published[0].QueueType)
	}

	// Verify metrics
	if metrics.GetProcessedCount(domain.NetworkSafaricom, "success") != 1 {
		t.Error("Expected success metric to be incremented")
	}
}

func TestResultHandler_HandleResult_Retryable(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()
	log := logger.NewNoop()

	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
		RetryCount: 2,
	}

	result := domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 5*time.Second)

	err := handler.HandleResult(context.Background(), result)
	if err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	// Verify published to retry queue
	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Errorf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "retry" {
		t.Errorf("Expected queue type 'retry', got %q", published[0].QueueType)
	}

	// Verify retry metrics
	if metrics.RetryCounts[domain.NetworkSafaricom] != 1 {
		t.Error("Expected retry metric to be incremented")
	}
}

func TestResultHandler_HandleResult_RetryableExceedsMax(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()
	log := logger.NewNoop()

	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
		RetryCount: 10, // At max retries
	}

	result := domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 5*time.Second)
	// Note: NewRetryableResult increments retry count, so it becomes 11

	err := handler.HandleResult(context.Background(), result)
	if err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	// Verify published to DLQ
	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Errorf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "dlq" {
		t.Errorf("Expected queue type 'dlq', got %q", published[0].QueueType)
	}

	// Verify dead letter metrics
	if metrics.DeadLetterCounts[domain.NetworkSafaricom] != 1 {
		t.Error("Expected dead letter metric to be incremented")
	}
}

func TestResultHandler_HandleResult_Permanent(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()
	log := logger.NewNoop()

	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
	}

	result := domain.NewPermanentResult(msg, domain.ErrMNORejected, 50*time.Millisecond)

	err := handler.HandleResult(context.Background(), result)
	if err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	// Verify published to DLQ
	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Errorf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "dlq" {
		t.Errorf("Expected queue type 'dlq', got %q", published[0].QueueType)
	}

	// Verify metrics
	if metrics.DeadLetterCounts[domain.NetworkSafaricom] != 1 {
		t.Error("Expected dead letter metric to be incremented")
	}
	if metrics.GetProcessedCount(domain.NetworkSafaricom, "failed") != 1 {
		t.Error("Expected failed metric to be incremented")
	}
}

func TestResultHandler_HandleBatchResults(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()
	log := logger.NewNoop()

	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 10,
		Logger:     log,
	})

	batch := domain.NewBatchResult()

	// Add 3 successful results
	for i := 0; i < 3; i++ {
		msg := &domain.Message{
			Correlator: "success-" + string(rune('0'+i)),
			NetworkRaw: "SAFARICOM",
		}
		batch.AddResult(domain.NewSuccessResult(msg, "ok", 10*time.Millisecond))
	}

	// Add 2 retryable results (under max retries)
	for i := 0; i < 2; i++ {
		msg := &domain.Message{
			Correlator: "retry-" + string(rune('0'+i)),
			NetworkRaw: "AIRTEL",
			RetryCount: 2,
		}
		batch.AddResult(domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 5*time.Second))
	}

	// Add 1 failed result
	msg := &domain.Message{
		Correlator: "fail-1",
		NetworkRaw: "TELKOM",
	}
	batch.AddResult(domain.NewPermanentResult(msg, domain.ErrMNORejected, 50*time.Millisecond))

	err := handler.HandleBatchResults(context.Background(), batch)
	if err != nil {
		t.Fatalf("HandleBatchResults() error = %v", err)
	}

	// Verify published counts
	if publisher.CountByQueueType("save_to_db") != 3 {
		t.Errorf("Expected 3 published to save_to_db, got %d", publisher.CountByQueueType("save_to_db"))
	}
	if publisher.CountByQueueType("retry") != 2 {
		t.Errorf("Expected 2 published to retry, got %d", publisher.CountByQueueType("retry"))
	}
	if publisher.CountByQueueType("dlq") != 1 {
		t.Errorf("Expected 1 published to dlq, got %d", publisher.CountByQueueType("dlq"))
	}
}

func TestResultHandler_HandleBatchResults_MovesToDLQOnMaxRetries(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()
	log := logger.NewNoop()

	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:  publisher,
		Metrics:    metrics,
		MaxRetries: 5,
		Logger:     log,
	})

	batch := domain.NewBatchResult()

	// Add retryable result at max retries (will be moved to DLQ)
	msg := &domain.Message{
		Correlator: "retry-maxed",
		NetworkRaw: "SAFARICOM",
		RetryCount: 4, // NewRetryableResult increments to 5, which equals max
	}
	batch.AddResult(domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 5*time.Second))

	err := handler.HandleBatchResults(context.Background(), batch)
	if err != nil {
		t.Fatalf("HandleBatchResults() error = %v", err)
	}

	// Should be moved from retryable to failed
	if batch.RetryableCount() != 0 {
		t.Errorf("Expected 0 retryable, got %d", batch.RetryableCount())
	}
	if batch.FailedCount() != 1 {
		t.Errorf("Expected 1 failed, got %d", batch.FailedCount())
	}

	// Verify published to DLQ
	if publisher.CountByQueueType("dlq") != 1 {
		t.Errorf("Expected 1 published to dlq, got %d", publisher.CountByQueueType("dlq"))
	}
}
