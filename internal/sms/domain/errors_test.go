package domain

import (
	"errors"
	"net/http"
	"testing"
)

func TestMNOError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *MNOError
		contains string
	}{
		{
			name: "with underlying error",
			err: &MNOError{
				Network:    NetworkSafaricom,
				StatusCode: 500,
				Message:    "internal server error",
				Err:        ErrMNOTimeout,
			},
			contains: "SAFARICOM error (status 500)",
		},
		{
			name: "without underlying error",
			err: &MNOError{
				Network:    NetworkAirtel,
				StatusCode: 400,
				Message:    "bad request",
				Err:        nil,
			},
			contains: "AIRTEL error (status 400): bad request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if len(result) == 0 {
				t.Error("Error() returned empty string")
			}
		})
	}
}

func TestMNOError_Unwrap(t *testing.T) {
	underlying := ErrMNOTimeout
	mnoErr := &MNOError{
		Network:    NetworkSafaricom,
		StatusCode: 500,
		Message:    "timeout",
		Err:        underlying,
	}

	result := mnoErr.Unwrap()
	if result != underlying {
		t.Errorf("Unwrap() = %v, want %v", result, underlying)
	}
}

func TestMNOError_IsRetryable(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		err        error
		expected   bool
	}{
		// 5xx errors are retryable
		{name: "500 Internal Server Error", statusCode: 500, err: nil, expected: true},
		{name: "502 Bad Gateway", statusCode: 502, err: nil, expected: true},
		{name: "503 Service Unavailable", statusCode: 503, err: nil, expected: true},
		{name: "504 Gateway Timeout", statusCode: 504, err: nil, expected: true},

		// Specific retryable status codes
		{name: "429 Too Many Requests", statusCode: 429, err: nil, expected: true},
		{name: "408 Request Timeout", statusCode: 408, err: nil, expected: true},

		// 4xx errors are not retryable (except specific ones)
		{name: "400 Bad Request", statusCode: 400, err: nil, expected: false},
		{name: "401 Unauthorized", statusCode: 401, err: nil, expected: false},
		{name: "403 Forbidden", statusCode: 403, err: nil, expected: false},
		{name: "404 Not Found", statusCode: 404, err: nil, expected: false},

		// Underlying retryable errors
		{name: "with ErrMNOTimeout", statusCode: 0, err: ErrMNOTimeout, expected: true},
		{name: "with ErrMNOUnavailable", statusCode: 0, err: ErrMNOUnavailable, expected: true},
		{name: "with ErrRateLimited", statusCode: 0, err: ErrRateLimited, expected: true},

		// Non-retryable errors
		{name: "with ErrMNORejected", statusCode: 400, err: ErrMNORejected, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mnoErr := &MNOError{
				Network:    NetworkSafaricom,
				StatusCode: tt.statusCode,
				Message:    "test",
				Err:        tt.err,
			}
			result := mnoErr.IsRetryable()
			if result != tt.expected {
				t.Errorf("IsRetryable() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNewMNOError(t *testing.T) {
	network := NetworkSafaricom
	statusCode := 500
	message := "test error"
	underlying := ErrMNOTimeout

	err := NewMNOError(network, statusCode, message, underlying)

	if err.Network != network {
		t.Errorf("Network = %v, want %v", err.Network, network)
	}
	if err.StatusCode != statusCode {
		t.Errorf("StatusCode = %v, want %v", err.StatusCode, statusCode)
	}
	if err.Message != message {
		t.Errorf("Message = %v, want %v", err.Message, message)
	}
	if err.Err != underlying {
		t.Errorf("Err = %v, want %v", err.Err, underlying)
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil error", err: nil, expected: false},
		{name: "ErrMNOTimeout", err: ErrMNOTimeout, expected: true},
		{name: "ErrMNOUnavailable", err: ErrMNOUnavailable, expected: true},
		{name: "ErrRateLimited", err: ErrRateLimited, expected: true},
		{name: "ErrQueueUnavailable", err: ErrQueueUnavailable, expected: true},
		{name: "ErrTokenFetchFail", err: ErrTokenFetchFail, expected: true},
		{name: "ErrMNORejected", err: ErrMNORejected, expected: false},
		{name: "ErrMissingCorrelator", err: ErrMissingCorrelator, expected: false},
		{name: "ErrUnknownNetwork", err: ErrUnknownNetwork, expected: false},
		{
			name: "MNOError with 500",
			err: &MNOError{
				Network:    NetworkSafaricom,
				StatusCode: http.StatusInternalServerError,
				Message:    "test",
			},
			expected: true,
		},
		{
			name: "MNOError with 400",
			err: &MNOError{
				Network:    NetworkSafaricom,
				StatusCode: http.StatusBadRequest,
				Message:    "test",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("IsRetryableError() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsPermanentError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil error", err: nil, expected: false},
		{name: "ErrMissingCorrelator", err: ErrMissingCorrelator, expected: true},
		{name: "ErrMissingContent", err: ErrMissingContent, expected: true},
		{name: "ErrMissingMSISDN", err: ErrMissingMSISDN, expected: true},
		{name: "ErrMissingSender", err: ErrMissingSender, expected: true},
		{name: "ErrUnknownNetwork", err: ErrUnknownNetwork, expected: true},
		{name: "ErrInvalidMSISDN", err: ErrInvalidMSISDN, expected: true},
		{name: "ErrMNORejected", err: ErrMNORejected, expected: true},
		{name: "ErrMaxRetriesExceeded", err: ErrMaxRetriesExceeded, expected: true},
		{name: "ErrMNOTimeout", err: ErrMNOTimeout, expected: false},
		{name: "ErrRateLimited", err: ErrRateLimited, expected: false},
		{
			name: "MNOError with 400",
			err: &MNOError{
				Network:    NetworkSafaricom,
				StatusCode: http.StatusBadRequest,
				Message:    "test",
			},
			expected: true,
		},
		{
			name: "MNOError with 500",
			err: &MNOError{
				Network:    NetworkSafaricom,
				StatusCode: http.StatusInternalServerError,
				Message:    "test",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPermanentError(tt.err)
			if result != tt.expected {
				t.Errorf("IsPermanentError() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected ResultType
	}{
		{name: "nil error", err: nil, expected: ResultSuccess},
		{name: "retryable error", err: ErrMNOTimeout, expected: ResultRetryable},
		{name: "permanent error", err: ErrMNORejected, expected: ResultPermanent},
		{name: "validation error", err: ErrMissingCorrelator, expected: ResultPermanent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifyError(tt.err)
			if result != tt.expected {
				t.Errorf("ClassifyError() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestMNOError_ErrorsAs(t *testing.T) {
	mnoErr := NewMNOError(NetworkSafaricom, 500, "test", nil)

	var target *MNOError
	if !errors.As(mnoErr, &target) {
		t.Error("errors.As failed to match MNOError")
	}

	if target.Network != NetworkSafaricom {
		t.Errorf("Network = %v, want %v", target.Network, NetworkSafaricom)
	}
}
