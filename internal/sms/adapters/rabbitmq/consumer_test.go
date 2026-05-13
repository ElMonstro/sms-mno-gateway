package rabbitmq

import (
	"encoding/json"
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

func TestConsumer_ParseMessages_Batch(t *testing.T) {
	c := &Consumer{log: logger.NewNoop()}

	messages := []*domain.Message{
		{
			Correlator: "test-1",
			Content:    "Hello",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			Sender:     "Test",
		},
		{
			Correlator: "test-2",
			Content:    "World",
			MSISDN:     "254733123456",
			NetworkRaw: "AIRTEL",
			Sender:     "Test",
		},
	}

	body, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("Failed to marshal test messages: %v", err)
	}

	parsed, err := c.parseMessages(body, "test-queue")
	if err != nil {
		t.Fatalf("parseMessages() error = %v", err)
	}

	if len(parsed) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(parsed))
	}

	if parsed[0].Correlator != "test-1" {
		t.Errorf("Expected correlator 'test-1', got '%s'", parsed[0].Correlator)
	}

	if parsed[1].Correlator != "test-2" {
		t.Errorf("Expected correlator 'test-2', got '%s'", parsed[1].Correlator)
	}
}

func TestConsumer_ParseMessages_SingleMessage(t *testing.T) {
	c := &Consumer{log: logger.NewNoop()}

	message := &domain.Message{
		Correlator: "single-msg",
		Content:    "Single message",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "Test",
	}

	body, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("Failed to marshal test message: %v", err)
	}

	parsed, err := c.parseMessages(body, "test-queue")
	if err != nil {
		t.Fatalf("parseMessages() error = %v", err)
	}

	if len(parsed) != 1 {
		t.Errorf("Expected 1 message, got %d", len(parsed))
	}

	if parsed[0].Correlator != "single-msg" {
		t.Errorf("Expected correlator 'single-msg', got '%s'", parsed[0].Correlator)
	}
}

func TestConsumer_ParseMessages_InvalidJSON(t *testing.T) {
	c := &Consumer{log: logger.NewNoop()}

	_, err := c.parseMessages([]byte("not valid json"))
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestConsumer_ParseMessages_EmptyArray(t *testing.T) {
	c := &Consumer{log: logger.NewNoop()}

	parsed, err := c.parseMessages([]byte("[]"))
	if err != nil {
		t.Fatalf("parseMessages() error = %v", err)
	}

	if len(parsed) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(parsed))
	}
}

func TestConsumer_ParseMessages_WithAllFields(t *testing.T) {
	c := &Consumer{log: logger.NewNoop()}

	messages := []*domain.Message{
		{
			Correlator: "full-msg",
			Content:    "Full message with all fields",
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			Sender:     "MySender",
			PackageID:  "TRANSACTIONAL",
			Status:     domain.StatusPending,
			RetryCount: 2,
			LastError:  "previous error",
		},
	}

	body, err := json.Marshal(messages)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	parsed, err := c.parseMessages(body, "test-queue")
	if err != nil {
		t.Fatalf("parseMessages() error = %v", err)
	}

	if parsed[0].PackageID != "TRANSACTIONAL" {
		t.Errorf("Expected PackageID 'TRANSACTIONAL', got '%s'", parsed[0].PackageID)
	}

	if parsed[0].RetryCount != 2 {
		t.Errorf("Expected RetryCount 2, got %d", parsed[0].RetryCount)
	}

	if parsed[0].IsTransactional() != true {
		t.Error("Expected message to be transactional")
	}
}

func TestConsumer_QueueName(t *testing.T) {
	c := &Consumer{queueName: "test-queue"}

	if c.QueueName() != "test-queue" {
		t.Errorf("Expected 'test-queue', got '%s'", c.QueueName())
	}
}
