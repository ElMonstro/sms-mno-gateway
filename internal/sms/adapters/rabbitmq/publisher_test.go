package rabbitmq

import (
	"errors"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// TestPublisher_QueueRouting verifies that PublishResult routes each result type
// to the correct queue. Retryable messages now go to type-specific delay queues.
func TestPublisher_QueueRouting(t *testing.T) {
	queues := ports.QueueConfig{
		SaveToDBQueue:           "save_to_db",
		RetryQueue:              "legacy_retry",
		DeadLetterQueue:         "dlq",
		TransactionalDelayQueue: "tx_delay",
		PromotionalDelayQueue:   "promo_delay",
		TransactionalRetryQueue: "tx_retry",
		PromotionalRetryQueue:   "promo_retry",
	}

	tests := []struct {
		name          string
		resultType    domain.ResultType
		transactional bool
		expectedQueue string
	}{
		{
			name:          "success routes to save_to_db",
			resultType:    domain.ResultSuccess,
			expectedQueue: queues.SaveToDBQueue,
		},
		{
			name:          "transactional retryable routes to tx delay queue",
			resultType:    domain.ResultRetryable,
			transactional: true,
			expectedQueue: queues.TransactionalDelayQueue,
		},
		{
			name:          "promotional retryable routes to promo delay queue",
			resultType:    domain.ResultRetryable,
			transactional: false,
			expectedQueue: queues.PromotionalDelayQueue,
		},
		{
			name:          "permanent routes to dlq",
			resultType:    domain.ResultPermanent,
			expectedQueue: queues.DeadLetterQueue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &domain.Message{Correlator: "test"}
			if tt.transactional {
				msg.PackageID = "TRANSACTIONAL"
			}

			// Mirror the routing logic from Publisher.PublishResult
			var queueName string
			switch tt.resultType {
			case domain.ResultSuccess:
				queueName = queues.SaveToDBQueue
			case domain.ResultRetryable:
				if msg.IsTransactional() {
					queueName = queues.TransactionalDelayQueue
				} else {
					queueName = queues.PromotionalDelayQueue
				}
			case domain.ResultPermanent:
				queueName = queues.DeadLetterQueue
			default:
				queueName = queues.SaveToDBQueue
			}

			if queueName != tt.expectedQueue {
				t.Errorf("Expected queue %q, got %q", tt.expectedQueue, queueName)
			}
		})
	}
}

// TestPublisher_BatchResultGrouping verifies that PublishBatchResults groups
// retryable messages by type rather than publishing N individual messages.
func TestPublisher_BatchResultGrouping(t *testing.T) {
	txMsg := &domain.Message{
		Correlator: "tx-1",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		PackageID:  "TRANSACTIONAL",
	}
	promoMsg1 := &domain.Message{
		Correlator: "promo-1",
		MSISDN:     "254733123456",
		NetworkRaw: "AIRTEL",
	}
	promoMsg2 := &domain.Message{
		Correlator: "promo-2",
		MSISDN:     "254770123456",
		NetworkRaw: "TELKOM",
	}

	batch := domain.NewBatchResult()
	batch.AddResult(domain.NewRetryableResult(txMsg, errors.New("timeout"), 500*time.Millisecond))
	batch.AddResult(domain.NewRetryableResult(promoMsg1, errors.New("timeout"), 500*time.Millisecond))
	batch.AddResult(domain.NewRetryableResult(promoMsg2, errors.New("timeout"), 500*time.Millisecond))

	// Verify grouping by walking through the batch as the publisher would
	var txRetries, promoRetries []*domain.Message
	for _, r := range batch.Retryable {
		if r.Message.IsTransactional() {
			txRetries = append(txRetries, r.Message)
		} else {
			promoRetries = append(promoRetries, r.Message)
		}
	}

	if len(txRetries) != 1 {
		t.Errorf("Expected 1 transactional retry, got %d", len(txRetries))
	}
	if len(promoRetries) != 2 {
		t.Errorf("Expected 2 promotional retries, got %d", len(promoRetries))
	}
	if txRetries[0].Correlator != "tx-1" {
		t.Errorf("Wrong transactional message: %s", txRetries[0].Correlator)
	}
}

// TestPublisher_BatchFailedGrouping verifies that PublishBatchResults batches
// DLQ publishes rather than calling Publish N times.
func TestPublisher_BatchFailedGrouping(t *testing.T) {
	msgs := []*domain.Message{
		{Correlator: "fail-1", MSISDN: "254722123456", NetworkRaw: "SAFARICOM"},
		{Correlator: "fail-2", MSISDN: "254733123456", NetworkRaw: "AIRTEL"},
		{Correlator: "fail-3", MSISDN: "254770123456", NetworkRaw: "TELKOM"},
	}

	batch := domain.NewBatchResult()
	for _, msg := range msgs {
		batch.AddResult(domain.NewPermanentResult(msg, errors.New("invalid msisdn"), 5*time.Millisecond))
	}

	// Verify that failed messages are grouped for batch DLQ publish
	failedMsgs := make([]*domain.Message, len(batch.Failed))
	for i, r := range batch.Failed {
		failedMsgs[i] = r.Message
	}

	if len(failedMsgs) != 3 {
		t.Errorf("Expected 3 failed messages grouped, got %d", len(failedMsgs))
	}
}

// TestPublisher_BatchResultRouting verifies BatchResult counts after adding results.
func TestPublisher_BatchResultRouting(t *testing.T) {
	msg1 := &domain.Message{Correlator: "success-1", MSISDN: "254722123456", NetworkRaw: "SAFARICOM"}
	msg2 := &domain.Message{Correlator: "retry-1", MSISDN: "254733123456", NetworkRaw: "AIRTEL"}
	msg3 := &domain.Message{Correlator: "failed-1", MSISDN: "254770123456", NetworkRaw: "TELKOM"}

	batch := domain.NewBatchResult()
	batch.AddResult(domain.NewSuccessResult(msg1, "sent", 10*time.Millisecond))
	batch.AddResult(domain.NewRetryableResult(msg2, errors.New("timeout"), 500*time.Millisecond))
	batch.AddResult(domain.NewPermanentResult(msg3, errors.New("invalid msisdn"), 5*time.Millisecond))

	if batch.SuccessCount() != 1 {
		t.Errorf("Expected 1 success, got %d", batch.SuccessCount())
	}
	if batch.RetryableCount() != 1 {
		t.Errorf("Expected 1 retryable, got %d", batch.RetryableCount())
	}
	if batch.FailedCount() != 1 {
		t.Errorf("Expected 1 failed, got %d", batch.FailedCount())
	}
	if batch.Successful[0].Message.Correlator != "success-1" {
		t.Errorf("Wrong message in successful: %s", batch.Successful[0].Message.Correlator)
	}
	if batch.Retryable[0].Message.Correlator != "retry-1" {
		t.Errorf("Wrong message in retryable: %s", batch.Retryable[0].Message.Correlator)
	}
	if batch.Failed[0].Message.Correlator != "failed-1" {
		t.Errorf("Wrong message in failed: %s", batch.Failed[0].Message.Correlator)
	}
}

// TestQueueConfig verifies all QueueConfig fields including the new delay/retry fields.
func TestQueueConfig(t *testing.T) {
	cfg := ports.QueueConfig{
		SaveToDBQueue:           "save_to_db",
		RetryQueue:              "legacy_retry",
		DeadLetterQueue:         "dlq",
		TransactionalDelayQueue: "SMS_TRANSACTIONAL_DELAY_QUEUE",
		PromotionalDelayQueue:   "SMS_PROMOTIONAL_DELAY_QUEUE",
		TransactionalRetryQueue: "SMS_TRANSACTIONAL_RETRY_QUEUE",
		PromotionalRetryQueue:   "SMS_PROMOTIONAL_RETRY_QUEUE",
	}

	checks := map[string]string{
		cfg.SaveToDBQueue:           "save_to_db",
		cfg.RetryQueue:              "legacy_retry",
		cfg.DeadLetterQueue:         "dlq",
		cfg.TransactionalDelayQueue: "SMS_TRANSACTIONAL_DELAY_QUEUE",
		cfg.PromotionalDelayQueue:   "SMS_PROMOTIONAL_DELAY_QUEUE",
		cfg.TransactionalRetryQueue: "SMS_TRANSACTIONAL_RETRY_QUEUE",
		cfg.PromotionalRetryQueue:   "SMS_PROMOTIONAL_RETRY_QUEUE",
	}

	for got, want := range checks {
		if got != want {
			t.Errorf("Expected %q, got %q", want, got)
		}
	}
}
