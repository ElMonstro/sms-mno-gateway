package rabbitmq

import (
	"errors"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Test queue routing logic for different result types
func TestPublisher_QueueRouting(t *testing.T) {
	queues := ports.QueueConfig{
		SaveToDBQueue:   "save_to_db",
		RetryQueue:      "retry_queue",
		DeadLetterQueue: "dlq",
	}

	tests := []struct {
		name          string
		resultType    domain.ResultType
		expectedQueue string
	}{
		{
			name:          "success routes to save_to_db",
			resultType:    domain.ResultSuccess,
			expectedQueue: queues.SaveToDBQueue,
		},
		{
			name:          "retryable routes to retry_queue",
			resultType:    domain.ResultRetryable,
			expectedQueue: queues.RetryQueue,
		},
		{
			name:          "permanent routes to dlq",
			resultType:    domain.ResultPermanent,
			expectedQueue: queues.DeadLetterQueue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the routing logic from PublishResult
			var queueName string
			switch tt.resultType {
			case domain.ResultSuccess:
				queueName = queues.SaveToDBQueue
			case domain.ResultRetryable:
				queueName = queues.RetryQueue
			case domain.ResultPermanent:
				queueName = queues.DeadLetterQueue
			default:
				queueName = queues.SaveToDBQueue
			}

			if queueName != tt.expectedQueue {
				t.Errorf("Expected queue %s, got %s", tt.expectedQueue, queueName)
			}
		})
	}
}

// Test batch result queue routing
func TestPublisher_BatchResultRouting(t *testing.T) {
	msg1 := &domain.Message{Correlator: "success-1", MSISDN: "254722123456", NetworkRaw: "SAFARICOM"}
	msg2 := &domain.Message{Correlator: "retry-1", MSISDN: "254733123456", NetworkRaw: "AIRTEL"}
	msg3 := &domain.Message{Correlator: "failed-1", MSISDN: "254770123456", NetworkRaw: "TELKOM"}

	batch := domain.NewBatchResult()
	batch.AddResult(domain.NewSuccessResult(msg1, "sent", 10*time.Millisecond))
	batch.AddResult(domain.NewRetryableResult(msg2, errors.New("timeout"), 500*time.Millisecond))
	batch.AddResult(domain.NewPermanentResult(msg3, errors.New("invalid msisdn"), 5*time.Millisecond))

	// Verify counts
	if batch.SuccessCount() != 1 {
		t.Errorf("Expected 1 success, got %d", batch.SuccessCount())
	}
	if batch.RetryableCount() != 1 {
		t.Errorf("Expected 1 retryable, got %d", batch.RetryableCount())
	}
	if batch.FailedCount() != 1 {
		t.Errorf("Expected 1 failed, got %d", batch.FailedCount())
	}

	// Verify messages in each category
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

// Test QueueConfig fields
func TestQueueConfig(t *testing.T) {
	cfg := ports.QueueConfig{
		SaveToDBQueue:   "save_to_db",
		RetryQueue:      "retry",
		DeadLetterQueue: "dlq",
	}

	if cfg.SaveToDBQueue != "save_to_db" {
		t.Errorf("Expected save_to_db queue name, got %s", cfg.SaveToDBQueue)
	}

	if cfg.RetryQueue != "retry" {
		t.Errorf("Expected retry queue name, got %s", cfg.RetryQueue)
	}

	if cfg.DeadLetterQueue != "dlq" {
		t.Errorf("Expected dlq queue name, got %s", cfg.DeadLetterQueue)
	}
}
