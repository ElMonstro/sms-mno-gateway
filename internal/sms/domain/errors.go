package domain

import (
	"errors"
	"fmt"
	"net/http"
)

// Domain errors
var (
	// Validation errors
	ErrMissingCorrelator = errors.New("missing correlator")
	ErrMissingContent    = errors.New("missing message content")
	ErrMissingMSISDN     = errors.New("missing MSISDN")
	ErrMissingSender     = errors.New("missing sender")
	ErrUnknownNetwork    = errors.New("unknown network")
	ErrInvalidMSISDN     = errors.New("invalid MSISDN format")

	// MNO errors
	ErrMNOUnavailable     = errors.New("MNO service unavailable")
	ErrMNORejected        = errors.New("message rejected by MNO")
	ErrMNOTimeout         = errors.New("MNO request timed out")
	ErrMNOInvalidResponse = errors.New("invalid response from MNO")

	// Rate limiting errors
	ErrRateLimited = errors.New("rate limit exceeded")

	// Circuit breaker errors
	ErrCircuitOpen = errors.New("circuit breaker is open")

	// Token errors
	ErrTokenExpired   = errors.New("authentication token expired")
	ErrTokenFetchFail = errors.New("failed to fetch authentication token")

	// Queue errors
	ErrQueueUnavailable = errors.New("queue service unavailable")
	ErrPublishFailed    = errors.New("failed to publish message")
	ErrConsumeFailed    = errors.New("failed to consume message")

	// General errors
	ErrMaxRetriesExceeded = errors.New("maximum retry count exceeded")
)

// MNOError represents an error from an MNO with additional context
type MNOError struct {
	Network    Network
	StatusCode int
	Message    string
	Err        error
}

// Error implements the error interface
func (e *MNOError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s error (status %d): %s - %v", e.Network, e.StatusCode, e.Message, e.Err)
	}
	return fmt.Sprintf("%s error (status %d): %s", e.Network, e.StatusCode, e.Message)
}

// Unwrap returns the underlying error
func (e *MNOError) Unwrap() error {
	return e.Err
}

// IsRetryable determines if the MNO error is retryable
func (e *MNOError) IsRetryable() bool {
	// 5xx errors are typically retryable
	if e.StatusCode >= 500 && e.StatusCode < 600 {
		return true
	}

	// Specific retryable status codes
	switch e.StatusCode {
	case http.StatusTooManyRequests, // 429
		http.StatusRequestTimeout,     // 408
		http.StatusServiceUnavailable: // 503
		return true
	}

	// Check underlying error
	if errors.Is(e.Err, ErrMNOTimeout) ||
		errors.Is(e.Err, ErrMNOUnavailable) ||
		errors.Is(e.Err, ErrRateLimited) {
		return true
	}

	return false
}

// NewMNOError creates a new MNO error
func NewMNOError(network Network, statusCode int, message string, err error) *MNOError {
	return &MNOError{
		Network:    network,
		StatusCode: statusCode,
		Message:    message,
		Err:        err,
	}
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check if it's an MNO error
	var mnoErr *MNOError
	if errors.As(err, &mnoErr) {
		return mnoErr.IsRetryable()
	}

	// Check specific retryable errors
	if errors.Is(err, ErrMNOTimeout) ||
		errors.Is(err, ErrMNOUnavailable) ||
		errors.Is(err, ErrRateLimited) ||
		errors.Is(err, ErrQueueUnavailable) ||
		errors.Is(err, ErrTokenFetchFail) {
		return true
	}

	return false
}

// IsPermanentError checks if an error is permanent (should not be retried)
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}

	// Check if it's an MNO error
	var mnoErr *MNOError
	if errors.As(err, &mnoErr) {
		return !mnoErr.IsRetryable()
	}

	// Check specific permanent errors
	if errors.Is(err, ErrMissingCorrelator) ||
		errors.Is(err, ErrMissingContent) ||
		errors.Is(err, ErrMissingMSISDN) ||
		errors.Is(err, ErrMissingSender) ||
		errors.Is(err, ErrUnknownNetwork) ||
		errors.Is(err, ErrInvalidMSISDN) ||
		errors.Is(err, ErrMNORejected) ||
		errors.Is(err, ErrMaxRetriesExceeded) {
		return true
	}

	return false
}

// ClassifyError returns the result type based on the error
func ClassifyError(err error) ResultType {
	if err == nil {
		return ResultSuccess
	}
	if IsRetryableError(err) {
		return ResultRetryable
	}
	return ResultPermanent
}
