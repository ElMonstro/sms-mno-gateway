package domain

import "time"

// ResultType indicates the outcome type of sending a message
type ResultType int

const (
	// ResultSuccess indicates the message was sent successfully
	ResultSuccess ResultType = iota
	// ResultRetryable indicates a temporary failure that can be retried
	ResultRetryable
	// ResultPermanent indicates a permanent failure that should not be retried
	ResultPermanent
)

// String returns the string representation of the result type
func (r ResultType) String() string {
	switch r {
	case ResultSuccess:
		return "SUCCESS"
	case ResultRetryable:
		return "RETRYABLE"
	case ResultPermanent:
		return "PERMANENT"
	default:
		return "UNKNOWN"
	}
}

// SendResult represents the outcome of sending a message to an MNO
type SendResult struct {
	Message     *Message
	Type        ResultType
	Error       error
	MNOResponse string
	Latency     time.Duration
	Timestamp   time.Time

	// ExternalMessageID and NetworkCode are populated only by the AfricasTalking
	// sender, for reporting back to the PHP API via ports.ResultReporter.
	ExternalMessageID string
	NetworkCode       string
}

// NewSuccessResult creates a new successful result
func NewSuccessResult(msg *Message, mnoResponse string, latency time.Duration) *SendResult {
	msg.SetStatus(StatusSent)
	return &SendResult{
		Message:     msg,
		Type:        ResultSuccess,
		MNOResponse: mnoResponse,
		Latency:     latency,
		Timestamp:   time.Now(),
	}
}

// NewRetryableResult creates a new retryable failure result
func NewRetryableResult(msg *Message, err error, latency time.Duration) *SendResult {
	msg.SetError(err)
	msg.IncrementRetryCount()
	return &SendResult{
		Message:   msg,
		Type:      ResultRetryable,
		Error:     err,
		Latency:   latency,
		Timestamp: time.Now(),
	}
}

// NewPermanentResult creates a new permanent failure result
func NewPermanentResult(msg *Message, err error, latency time.Duration) *SendResult {
	msg.SetError(err)
	return &SendResult{
		Message:   msg,
		Type:      ResultPermanent,
		Error:     err,
		Latency:   latency,
		Timestamp: time.Now(),
	}
}

// IsSuccess returns true if the result indicates success
func (r *SendResult) IsSuccess() bool {
	return r.Type == ResultSuccess
}

// IsRetryable returns true if the result indicates a retryable failure
func (r *SendResult) IsRetryable() bool {
	return r.Type == ResultRetryable
}

// IsPermanent returns true if the result indicates a permanent failure
func (r *SendResult) IsPermanent() bool {
	return r.Type == ResultPermanent
}

// BatchResult holds the results of processing a batch of messages
type BatchResult struct {
	Successful  []*SendResult
	Retryable   []*SendResult
	Failed      []*SendResult
	TotalCount  int
	ProcessTime time.Duration
}

// NewBatchResult creates a new empty batch result
func NewBatchResult() *BatchResult {
	return &BatchResult{
		Successful: make([]*SendResult, 0),
		Retryable:  make([]*SendResult, 0),
		Failed:     make([]*SendResult, 0),
	}
}

// AddResult adds a result to the appropriate category
func (b *BatchResult) AddResult(result *SendResult) {
	b.TotalCount++
	switch result.Type {
	case ResultSuccess:
		b.Successful = append(b.Successful, result)
	case ResultRetryable:
		b.Retryable = append(b.Retryable, result)
	case ResultPermanent:
		b.Failed = append(b.Failed, result)
	}
}

// SuccessCount returns the number of successful messages
func (b *BatchResult) SuccessCount() int {
	return len(b.Successful)
}

// RetryableCount returns the number of retryable messages
func (b *BatchResult) RetryableCount() int {
	return len(b.Retryable)
}

// FailedCount returns the number of failed messages
func (b *BatchResult) FailedCount() int {
	return len(b.Failed)
}

// SuccessRate returns the success rate as a percentage
func (b *BatchResult) SuccessRate() float64 {
	if b.TotalCount == 0 {
		return 0
	}
	return float64(len(b.Successful)) / float64(b.TotalCount) * 100
}
