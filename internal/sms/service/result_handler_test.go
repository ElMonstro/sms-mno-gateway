package service

import (
	"context"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

// newTestHandler returns a ResultHandler with sensible defaults for tests that
// don't care about the transactional/promotional split.
func newTestHandler(publisher *mocks.MockQueuePublisher, metrics *mocks.MockMetrics) *ResultHandler {
	return NewResultHandler(&ResultHandlerConfig{
		Publisher:               publisher,
		Metrics:                 metrics,
		MaxRetriesTransactional: 5,
		MaxRetriesPromotional:   10,
		Logger:                  logger.NewNoop(),
	})
}

func TestResultHandler_HandleResult_Success(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()

	handler := newTestHandler(publisher, metrics)

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
	}

	result := domain.NewSuccessResult(msg, "accepted", 100*time.Millisecond)

	if err := handler.HandleResult(context.Background(), result); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Fatalf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "save_to_db" {
		t.Errorf("Expected queue type 'save_to_db', got %q", published[0].QueueType)
	}
	if metrics.GetProcessedCount(domain.NetworkSafaricom, "success") != 1 {
		t.Error("Expected success metric to be incremented")
	}
}

func TestResultHandler_HandleResult_Retryable_Promotional(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()

	handler := newTestHandler(publisher, metrics)

	msg := &domain.Message{
		Correlator: "test-promo",
		Content:    "Sale!",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "BrandX",
		RetryCount: 2,
		// PackageID not set → promotional
	}

	result := domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 5*time.Second)

	if err := handler.HandleResult(context.Background(), result); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Fatalf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "promotional_delay" {
		t.Errorf("Expected 'promotional_delay', got %q", published[0].QueueType)
	}
	if metrics.RetryCounts[domain.NetworkSafaricom] != 1 {
		t.Error("Expected retry metric to be incremented")
	}
}

func TestResultHandler_HandleResult_Retryable_Transactional(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()

	handler := newTestHandler(publisher, metrics)

	msg := &domain.Message{
		Correlator: "test-tx",
		Content:    "OTP: 123456",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "BankX",
		RetryCount: 1,
		PackageID:  "TRANSACTIONAL",
	}

	result := domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 1*time.Second)

	if err := handler.HandleResult(context.Background(), result); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Fatalf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "transactional_delay" {
		t.Errorf("Expected 'transactional_delay', got %q", published[0].QueueType)
	}
}

func TestResultHandler_HandleResult_RetryableExceedsMax_Promotional(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()

	handler := newTestHandler(publisher, metrics)

	// RetryCount starts at 10; NewRetryableResult will increment it to 11 — above promotional limit of 10
	msg := &domain.Message{
		Correlator: "promo-maxed",
		Content:    "Sale!",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "BrandX",
		RetryCount: 10,
	}

	result := domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 5*time.Second)

	if err := handler.HandleResult(context.Background(), result); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Fatalf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "dlq" {
		t.Errorf("Expected 'dlq' after max retries, got %q", published[0].QueueType)
	}
	if metrics.DeadLetterCounts[domain.NetworkSafaricom] != 1 {
		t.Error("Expected dead letter metric to be incremented")
	}
}

func TestResultHandler_HandleResult_RetryableExceedsMax_Transactional(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()

	handler := newTestHandler(publisher, metrics)

	// Transactional limit is 5. RetryCount=5 → NewRetryableResult bumps to 6 → exceeds limit.
	msg := &domain.Message{
		Correlator: "tx-maxed",
		Content:    "OTP: 999",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "BankX",
		RetryCount: 5,
		PackageID:  "TRANSACTIONAL",
	}

	result := domain.NewRetryableResult(msg, domain.ErrMNOTimeout, 1*time.Second)

	if err := handler.HandleResult(context.Background(), result); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Fatalf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "dlq" {
		t.Errorf("Expected 'dlq' after transactional max retries, got %q", published[0].QueueType)
	}
}

// Verify that a transactional message is NOT expired at the promotional limit (10).
// With MaxRetriesTransactional=5 and MaxRetriesPromotional=10, a transactional message
// with RetryCount=7 is already past its 5-retry limit and should go to DLQ.
// A promotional message with RetryCount=7 should still retry.
func TestResultHandler_SplitMaxRetries_DifferentLimitsPerType(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	handler := newTestHandler(publisher, mocks.NewMockMetrics())

	retryCount := 7 // above transactional limit (5), below promotional limit (10)

	// Transactional with retryCount=7 → should DLQ
	txMsg := &domain.Message{
		Correlator: "tx-split",
		Content:    "OTP",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "Bank",
		RetryCount: retryCount,
		PackageID:  "TRANSACTIONAL",
	}
	txResult := domain.NewRetryableResult(txMsg, domain.ErrMNOTimeout, 1*time.Second)
	if err := handler.HandleResult(context.Background(), txResult); err != nil {
		t.Fatalf("transactional HandleResult error = %v", err)
	}

	// Promotional with same retryCount=7 → should delay-retry
	publisher2 := mocks.NewMockQueuePublisher()
	handler2 := newTestHandler(publisher2, mocks.NewMockMetrics())
	promoMsg := &domain.Message{
		Correlator: "promo-split",
		Content:    "Sale",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "Brand",
		RetryCount: retryCount,
		// no PackageID → promotional
	}
	promoResult := domain.NewRetryableResult(promoMsg, domain.ErrMNOTimeout, 5*time.Second)
	if err := handler2.HandleResult(context.Background(), promoResult); err != nil {
		t.Fatalf("promotional HandleResult error = %v", err)
	}

	txItems := publisher.GetPublishedItems()
	if len(txItems) == 0 || txItems[0].QueueType != "dlq" {
		t.Errorf("Transactional at retryCount=%d (limit=5): expected 'dlq', got %q", retryCount, txItems[0].QueueType)
	}

	promoItems := publisher2.GetPublishedItems()
	if len(promoItems) == 0 || promoItems[0].QueueType != "promotional_delay" {
		t.Errorf("Promotional at retryCount=%d (limit=10): expected 'promotional_delay', got %q", retryCount, promoItems[0].QueueType)
	}
}

func TestResultHandler_HandleResult_Permanent(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()
	metrics := mocks.NewMockMetrics()

	handler := newTestHandler(publisher, metrics)

	msg := &domain.Message{
		Correlator: "test-perm",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
	}

	result := domain.NewPermanentResult(msg, domain.ErrMNORejected, 50*time.Millisecond)

	if err := handler.HandleResult(context.Background(), result); err != nil {
		t.Fatalf("HandleResult() error = %v", err)
	}

	published := publisher.GetPublishedItems()
	if len(published) != 1 {
		t.Fatalf("Expected 1 published item, got %d", len(published))
	}
	if published[0].QueueType != "dlq" {
		t.Errorf("Expected 'dlq', got %q", published[0].QueueType)
	}
	if metrics.DeadLetterCounts[domain.NetworkSafaricom] != 1 {
		t.Error("Expected dead letter metric to be incremented")
	}
	if metrics.GetProcessedCount(domain.NetworkSafaricom, "failed") != 1 {
		t.Error("Expected failed metric to be incremented")
	}
}

func TestResultHandler_HandleBatchResults_RoutesRetryableByType(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()

	handler := newTestHandler(publisher, mocks.NewMockMetrics())

	batch := domain.NewBatchResult()

	// 2 successful
	for i := 0; i < 2; i++ {
		msg := &domain.Message{Correlator: "ok", NetworkRaw: "SAFARICOM"}
		batch.AddResult(domain.NewSuccessResult(msg, "ok", 10*time.Millisecond))
	}

	// 1 transactional retry
	batch.AddResult(domain.NewRetryableResult(&domain.Message{
		Correlator: "tx-retry",
		NetworkRaw: "SAFARICOM",
		RetryCount: 1,
		PackageID:  "TRANSACTIONAL",
	}, domain.ErrMNOTimeout, 1*time.Second))

	// 2 promotional retries
	for i := 0; i < 2; i++ {
		batch.AddResult(domain.NewRetryableResult(&domain.Message{
			Correlator: "promo-retry",
			NetworkRaw: "AIRTEL",
			RetryCount: 2,
		}, domain.ErrMNOTimeout, 5*time.Second))
	}

	// 1 permanent
	batch.AddResult(domain.NewPermanentResult(&domain.Message{
		Correlator: "fail",
		NetworkRaw: "TELKOM",
	}, domain.ErrMNORejected, 50*time.Millisecond))

	if err := handler.HandleBatchResults(context.Background(), batch); err != nil {
		t.Fatalf("HandleBatchResults() error = %v", err)
	}

	if got := publisher.CountByQueueType("save_to_db"); got != 2 {
		t.Errorf("Expected 2 save_to_db, got %d", got)
	}
	if got := publisher.CountByQueueType("transactional_delay"); got != 1 {
		t.Errorf("Expected 1 transactional_delay, got %d", got)
	}
	if got := publisher.CountByQueueType("promotional_delay"); got != 2 {
		t.Errorf("Expected 2 promotional_delay, got %d", got)
	}
	if got := publisher.CountByQueueType("dlq"); got != 1 {
		t.Errorf("Expected 1 dlq, got %d", got)
	}
}

func TestResultHandler_HandleBatchResults_MovesExceededRetriesToDLQ(t *testing.T) {
	publisher := mocks.NewMockQueuePublisher()

	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:               publisher,
		Metrics:                 mocks.NewMockMetrics(),
		MaxRetriesTransactional: 3,
		MaxRetriesPromotional:   5,
		Logger:                  logger.NewNoop(),
	})

	batch := domain.NewBatchResult()

	// Transactional at RetryCount=3 → NewRetryableResult bumps to 4 → exceeds limit 3 → DLQ
	batch.AddResult(domain.NewRetryableResult(&domain.Message{
		Correlator: "tx-maxed",
		NetworkRaw: "SAFARICOM",
		RetryCount: 3,
		PackageID:  "TRANSACTIONAL",
	}, domain.ErrMNOTimeout, 1*time.Second))

	// Promotional at RetryCount=5 → bumps to 6 → exceeds limit 5 → DLQ
	batch.AddResult(domain.NewRetryableResult(&domain.Message{
		Correlator: "promo-maxed",
		NetworkRaw: "AIRTEL",
		RetryCount: 5,
	}, domain.ErrMNOTimeout, 5*time.Second))

	// Promotional at RetryCount=3 → bumps to 4 → under limit 5 → delay retry
	batch.AddResult(domain.NewRetryableResult(&domain.Message{
		Correlator: "promo-ok",
		NetworkRaw: "AIRTEL",
		RetryCount: 3,
	}, domain.ErrMNOTimeout, 5*time.Second))

	if err := handler.HandleBatchResults(context.Background(), batch); err != nil {
		t.Fatalf("HandleBatchResults() error = %v", err)
	}

	if batch.RetryableCount() != 1 {
		t.Errorf("Expected 1 retryable remaining (promo-ok), got %d", batch.RetryableCount())
	}
	if batch.FailedCount() != 2 {
		t.Errorf("Expected 2 moved to failed (tx-maxed + promo-maxed), got %d", batch.FailedCount())
	}
	if got := publisher.CountByQueueType("dlq"); got != 2 {
		t.Errorf("Expected 2 dlq, got %d", got)
	}
	if got := publisher.CountByQueueType("promotional_delay"); got != 1 {
		t.Errorf("Expected 1 promotional_delay, got %d", got)
	}
}
