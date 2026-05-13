package domain

import (
	"testing"
)

func TestMessage_NormalizeMSISDN(t *testing.T) {
	tests := []struct {
		name     string
		msisdn   string
		expected string
	}{
		{
			name:     "local format 0722",
			msisdn:   "0722123456",
			expected: "254722123456",
		},
		{
			name:     "local format 0700",
			msisdn:   "0700123456",
			expected: "254700123456",
		},
		{
			name:     "already international 254",
			msisdn:   "254722123456",
			expected: "254722123456",
		},
		{
			name:     "plus international format",
			msisdn:   "+254722123456",
			expected: "254722123456",
		},
		{
			name:     "with leading spaces",
			msisdn:   "  0722123456",
			expected: "254722123456",
		},
		{
			name:     "with trailing spaces",
			msisdn:   "0722123456  ",
			expected: "254722123456",
		},
		{
			name:     "unknown format passed through",
			msisdn:   "1234567890",
			expected: "1234567890",
		},
		{
			name:     "empty string",
			msisdn:   "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{MSISDN: tt.msisdn}
			result := msg.NormalizeMSISDN()
			if result != tt.expected {
				t.Errorf("NormalizeMSISDN() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestMessage_IsTransactional(t *testing.T) {
	tests := []struct {
		name      string
		packageID string
		expected  bool
	}{
		{
			name:      "uppercase TRANSACTIONAL",
			packageID: "TRANSACTIONAL",
			expected:  true,
		},
		{
			name:      "lowercase transactional",
			packageID: "transactional",
			expected:  true,
		},
		{
			name:      "mixed case Transactional",
			packageID: "Transactional",
			expected:  true,
		},
		{
			name:      "with leading spaces",
			packageID: "  TRANSACTIONAL",
			expected:  true,
		},
		{
			name:      "with trailing spaces",
			packageID: "TRANSACTIONAL  ",
			expected:  true,
		},
		{
			name:      "numeric package ID",
			packageID: "0",
			expected:  false,
		},
		{
			name:      "other package ID",
			packageID: "PROMOTIONAL",
			expected:  false,
		},
		{
			name:      "empty string",
			packageID: "",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{PackageID: tt.packageID}
			result := msg.IsTransactional()
			if result != tt.expected {
				t.Errorf("IsTransactional() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMessage_IsPromotional(t *testing.T) {
	const gwQueue = "SMS_MNO_GATEWAY_QUEUE"

	tests := []struct {
		name             string
		packageID        string
		sourceQueue      string
		gatewayQueueName string
		expected         bool
	}{
		{
			name:             "gateway queue source is promotional",
			packageID:        "bulk",
			sourceQueue:      gwQueue,
			gatewayQueueName: gwQueue,
			expected:         true,
		},
		{
			name:             "api-v2 queue source is not promotional",
			packageID:        "bulk",
			sourceQueue:      "CONSUME_TO_MNO",
			gatewayQueueName: gwQueue,
			expected:         false,
		},
		{
			name:             "TRANSACTIONAL packageId overrides gateway source",
			packageID:        "TRANSACTIONAL",
			sourceQueue:      gwQueue,
			gatewayQueueName: gwQueue,
			expected:         false,
		},
		{
			name:             "TRANSACTIONAL packageId with api-v2 source is not promotional",
			packageID:        "TRANSACTIONAL",
			sourceQueue:      "CONSUME_TO_MNO",
			gatewayQueueName: gwQueue,
			expected:         false,
		},
		{
			name:             "no source queue falls back to promotional",
			packageID:        "bulk",
			sourceQueue:      "",
			gatewayQueueName: gwQueue,
			expected:         true,
		},
		{
			name:             "no gateway queue name configured falls back to promotional",
			packageID:        "bulk",
			sourceQueue:      "CONSUME_TO_MNO",
			gatewayQueueName: "",
			expected:         true,
		},
		{
			name:             "empty packageId from gateway queue is promotional",
			packageID:        "",
			sourceQueue:      gwQueue,
			gatewayQueueName: gwQueue,
			expected:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{PackageID: tt.packageID, SourceQueue: tt.sourceQueue}
			result := msg.IsPromotional(tt.gatewayQueueName)
			if result != tt.expected {
				t.Errorf("IsPromotional() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMessage_Network(t *testing.T) {
	tests := []struct {
		name       string
		networkRaw string
		expected   Network
	}{
		{
			name:       "SAFARICOM",
			networkRaw: "SAFARICOM",
			expected:   NetworkSafaricom,
		},
		{
			name:       "lowercase safaricom",
			networkRaw: "safaricom",
			expected:   NetworkSafaricom,
		},
		{
			name:       "AIRTEL",
			networkRaw: "AIRTEL",
			expected:   NetworkAirtel,
		},
		{
			name:       "UNKNOWN network",
			networkRaw: "VODAFONE",
			expected:   NetworkUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{NetworkRaw: tt.networkRaw}
			result := msg.Network()
			if result != tt.expected {
				t.Errorf("Network() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMessage_Validate(t *testing.T) {
	validMessage := &Message{
		Correlator: "test-123",
		Content:    "Hello World",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
	}

	tests := []struct {
		name        string
		modify      func(*Message)
		expectedErr error
	}{
		{
			name:        "valid message",
			modify:      func(m *Message) {},
			expectedErr: nil,
		},
		{
			name:        "missing correlator",
			modify:      func(m *Message) { m.Correlator = "" },
			expectedErr: ErrMissingCorrelator,
		},
		{
			name:        "missing content",
			modify:      func(m *Message) { m.Content = "" },
			expectedErr: ErrMissingContent,
		},
		{
			name:        "missing MSISDN",
			modify:      func(m *Message) { m.MSISDN = "" },
			expectedErr: ErrMissingMSISDN,
		},
		{
			name:        "unknown network",
			modify:      func(m *Message) { m.NetworkRaw = "UNKNOWN" },
			expectedErr: ErrUnknownNetwork,
		},
		{
			name:        "missing sender",
			modify:      func(m *Message) { m.Sender = "" },
			expectedErr: ErrMissingSender,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := validMessage.Clone()
			tt.modify(msg)
			err := msg.Validate()
			if err != tt.expectedErr {
				t.Errorf("Validate() error = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}

func TestMessage_Clone(t *testing.T) {
	original := &Message{
		Correlator: "test-123",
		Content:    "Hello World",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
		PackageID:  "TRANSACTIONAL",
		Status:     StatusPending,
		RetryCount: 3,
		LastError:  "previous error",
	}

	clone := original.Clone()

	// Verify all fields are copied
	if clone.Correlator != original.Correlator {
		t.Errorf("Clone Correlator = %v, want %v", clone.Correlator, original.Correlator)
	}
	if clone.Content != original.Content {
		t.Errorf("Clone Content = %v, want %v", clone.Content, original.Content)
	}
	if clone.MSISDN != original.MSISDN {
		t.Errorf("Clone MSISDN = %v, want %v", clone.MSISDN, original.MSISDN)
	}
	if clone.RetryCount != original.RetryCount {
		t.Errorf("Clone RetryCount = %v, want %v", clone.RetryCount, original.RetryCount)
	}

	// Verify it's a different pointer
	if clone == original {
		t.Error("Clone returned same pointer, expected different")
	}

	// Verify modifying clone doesn't affect original
	clone.Content = "Modified"
	if original.Content == clone.Content {
		t.Error("Modifying clone affected original")
	}
}

func TestMessage_CanRetry(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		maxRetries int
		expected   bool
	}{
		{
			name:       "zero retries",
			retryCount: 0,
			maxRetries: 10,
			expected:   true,
		},
		{
			name:       "under max",
			retryCount: 5,
			maxRetries: 10,
			expected:   true,
		},
		{
			name:       "at max",
			retryCount: 10,
			maxRetries: 10,
			expected:   false,
		},
		{
			name:       "over max",
			retryCount: 15,
			maxRetries: 10,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{RetryCount: tt.retryCount}
			result := msg.CanRetry(tt.maxRetries)
			if result != tt.expected {
				t.Errorf("CanRetry(%d) = %v, want %v", tt.maxRetries, result, tt.expected)
			}
		})
	}
}

func TestMessage_IncrementRetryCount(t *testing.T) {
	msg := &Message{RetryCount: 0}

	msg.IncrementRetryCount()
	if msg.RetryCount != 1 {
		t.Errorf("RetryCount after first increment = %d, want 1", msg.RetryCount)
	}

	msg.IncrementRetryCount()
	if msg.RetryCount != 2 {
		t.Errorf("RetryCount after second increment = %d, want 2", msg.RetryCount)
	}
}

func TestMessage_SetError(t *testing.T) {
	msg := &Message{Status: StatusPending}
	testErr := ErrMNOTimeout

	msg.SetError(testErr)

	if msg.LastError != testErr.Error() {
		t.Errorf("LastError = %v, want %v", msg.LastError, testErr.Error())
	}
	if msg.Status != StatusFailed {
		t.Errorf("Status = %v, want %v", msg.Status, StatusFailed)
	}
}

func TestMessage_SetStatus(t *testing.T) {
	msg := &Message{Status: StatusPending}

	msg.SetStatus(StatusSent)

	if msg.Status != StatusSent {
		t.Errorf("Status = %v, want %v", msg.Status, StatusSent)
	}
	if msg.ProcessedAt.IsZero() {
		t.Error("ProcessedAt should be set")
	}
}
