package domain

import (
	"testing"
	"time"
)

func TestResultType_String(t *testing.T) {
	tests := []struct {
		rt       ResultType
		expected string
	}{
		{ResultSuccess, "SUCCESS"},
		{ResultRetryable, "RETRYABLE"},
		{ResultPermanent, "PERMANENT"},
		{ResultType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.rt.String()
			if result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestNewSuccessResult(t *testing.T) {
	msg := &Message{
		Correlator: "test-123",
		Status:     StatusPending,
	}
	mnoResponse := "Message accepted"
	latency := 100 * time.Millisecond

	result := NewSuccessResult(msg, mnoResponse, latency)

	if result.Type != ResultSuccess {
		t.Errorf("Type = %v, want %v", result.Type, ResultSuccess)
	}
	if result.Message != msg {
		t.Error("Message pointer mismatch")
	}
	if result.MNOResponse != mnoResponse {
		t.Errorf("MNOResponse = %v, want %v", result.MNOResponse, mnoResponse)
	}
	if result.Latency != latency {
		t.Errorf("Latency = %v, want %v", result.Latency, latency)
	}
	if result.Timestamp.IsZero() {
		t.Error("Timestamp should be set")
	}
	if msg.Status != StatusSent {
		t.Errorf("Message status = %v, want %v", msg.Status, StatusSent)
	}
}

func TestNewRetryableResult(t *testing.T) {
	msg := &Message{
		Correlator: "test-123",
		Status:     StatusPending,
		RetryCount: 2,
	}
	err := ErrMNOTimeout
	latency := 5 * time.Second

	result := NewRetryableResult(msg, err, latency)

	if result.Type != ResultRetryable {
		t.Errorf("Type = %v, want %v", result.Type, ResultRetryable)
	}
	if result.Error != err {
		t.Errorf("Error = %v, want %v", result.Error, err)
	}
	if result.Latency != latency {
		t.Errorf("Latency = %v, want %v", result.Latency, latency)
	}
	if msg.RetryCount != 3 {
		t.Errorf("Message RetryCount = %d, want 3", msg.RetryCount)
	}
	if msg.Status != StatusFailed {
		t.Errorf("Message status = %v, want %v", msg.Status, StatusFailed)
	}
}

func TestNewPermanentResult(t *testing.T) {
	msg := &Message{
		Correlator: "test-123",
		Status:     StatusPending,
	}
	err := ErrMNORejected
	latency := 50 * time.Millisecond

	result := NewPermanentResult(msg, err, latency)

	if result.Type != ResultPermanent {
		t.Errorf("Type = %v, want %v", result.Type, ResultPermanent)
	}
	if result.Error != err {
		t.Errorf("Error = %v, want %v", result.Error, err)
	}
	if msg.Status != StatusFailed {
		t.Errorf("Message status = %v, want %v", msg.Status, StatusFailed)
	}
}

func TestSendResult_IsSuccess(t *testing.T) {
	tests := []struct {
		name     string
		rt       ResultType
		expected bool
	}{
		{"success", ResultSuccess, true},
		{"retryable", ResultRetryable, false},
		{"permanent", ResultPermanent, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &SendResult{Type: tt.rt}
			if result.IsSuccess() != tt.expected {
				t.Errorf("IsSuccess() = %v, want %v", result.IsSuccess(), tt.expected)
			}
		})
	}
}

func TestSendResult_IsRetryable(t *testing.T) {
	tests := []struct {
		name     string
		rt       ResultType
		expected bool
	}{
		{"success", ResultSuccess, false},
		{"retryable", ResultRetryable, true},
		{"permanent", ResultPermanent, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &SendResult{Type: tt.rt}
			if result.IsRetryable() != tt.expected {
				t.Errorf("IsRetryable() = %v, want %v", result.IsRetryable(), tt.expected)
			}
		})
	}
}

func TestSendResult_IsPermanent(t *testing.T) {
	tests := []struct {
		name     string
		rt       ResultType
		expected bool
	}{
		{"success", ResultSuccess, false},
		{"retryable", ResultRetryable, false},
		{"permanent", ResultPermanent, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &SendResult{Type: tt.rt}
			if result.IsPermanent() != tt.expected {
				t.Errorf("IsPermanent() = %v, want %v", result.IsPermanent(), tt.expected)
			}
		})
	}
}

func TestNewBatchResult(t *testing.T) {
	batch := NewBatchResult()

	if batch.Successful == nil {
		t.Error("Successful slice should be initialized")
	}
	if batch.Retryable == nil {
		t.Error("Retryable slice should be initialized")
	}
	if batch.Failed == nil {
		t.Error("Failed slice should be initialized")
	}
	if batch.TotalCount != 0 {
		t.Errorf("TotalCount = %d, want 0", batch.TotalCount)
	}
}

func TestBatchResult_AddResult(t *testing.T) {
	batch := NewBatchResult()

	// Add success
	successMsg := &Message{Correlator: "success-1"}
	successResult := &SendResult{Message: successMsg, Type: ResultSuccess}
	batch.AddResult(successResult)

	// Add retryable
	retryMsg := &Message{Correlator: "retry-1"}
	retryResult := &SendResult{Message: retryMsg, Type: ResultRetryable}
	batch.AddResult(retryResult)

	// Add permanent
	failMsg := &Message{Correlator: "fail-1"}
	failResult := &SendResult{Message: failMsg, Type: ResultPermanent}
	batch.AddResult(failResult)

	if batch.TotalCount != 3 {
		t.Errorf("TotalCount = %d, want 3", batch.TotalCount)
	}
	if len(batch.Successful) != 1 {
		t.Errorf("Successful count = %d, want 1", len(batch.Successful))
	}
	if len(batch.Retryable) != 1 {
		t.Errorf("Retryable count = %d, want 1", len(batch.Retryable))
	}
	if len(batch.Failed) != 1 {
		t.Errorf("Failed count = %d, want 1", len(batch.Failed))
	}
}

func TestBatchResult_SuccessCount(t *testing.T) {
	batch := NewBatchResult()
	batch.Successful = []*SendResult{{}, {}, {}}

	if batch.SuccessCount() != 3 {
		t.Errorf("SuccessCount() = %d, want 3", batch.SuccessCount())
	}
}

func TestBatchResult_RetryableCount(t *testing.T) {
	batch := NewBatchResult()
	batch.Retryable = []*SendResult{{}, {}}

	if batch.RetryableCount() != 2 {
		t.Errorf("RetryableCount() = %d, want 2", batch.RetryableCount())
	}
}

func TestBatchResult_FailedCount(t *testing.T) {
	batch := NewBatchResult()
	batch.Failed = []*SendResult{{}}

	if batch.FailedCount() != 1 {
		t.Errorf("FailedCount() = %d, want 1", batch.FailedCount())
	}
}

func TestBatchResult_SuccessRate(t *testing.T) {
	tests := []struct {
		name       string
		successful int
		total      int
		expected   float64
	}{
		{name: "empty batch", successful: 0, total: 0, expected: 0},
		{name: "100% success", successful: 10, total: 10, expected: 100},
		{name: "50% success", successful: 5, total: 10, expected: 50},
		{name: "75% success", successful: 3, total: 4, expected: 75},
		{name: "0% success", successful: 0, total: 10, expected: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := NewBatchResult()
			batch.TotalCount = tt.total
			for i := 0; i < tt.successful; i++ {
				batch.Successful = append(batch.Successful, &SendResult{})
			}

			result := batch.SuccessRate()
			if result != tt.expected {
				t.Errorf("SuccessRate() = %f, want %f", result, tt.expected)
			}
		})
	}
}

func TestBatchResult_MultipleAdditions(t *testing.T) {
	batch := NewBatchResult()

	// Add 5 successes
	for i := 0; i < 5; i++ {
		batch.AddResult(&SendResult{Type: ResultSuccess})
	}

	// Add 3 retryable
	for i := 0; i < 3; i++ {
		batch.AddResult(&SendResult{Type: ResultRetryable})
	}

	// Add 2 permanent failures
	for i := 0; i < 2; i++ {
		batch.AddResult(&SendResult{Type: ResultPermanent})
	}

	if batch.TotalCount != 10 {
		t.Errorf("TotalCount = %d, want 10", batch.TotalCount)
	}
	if batch.SuccessCount() != 5 {
		t.Errorf("SuccessCount = %d, want 5", batch.SuccessCount())
	}
	if batch.RetryableCount() != 3 {
		t.Errorf("RetryableCount = %d, want 3", batch.RetryableCount())
	}
	if batch.FailedCount() != 2 {
		t.Errorf("FailedCount = %d, want 2", batch.FailedCount())
	}
	if batch.SuccessRate() != 50 {
		t.Errorf("SuccessRate = %f, want 50", batch.SuccessRate())
	}
}
