package domain

import (
	"strings"
	"time"
)

// Message represents an SMS message to be sent
type Message struct {
	Correlator  string    `json:"corelator"`
	Content     string    `json:"message"`
	MSISDN      string    `json:"msisdn"`
	NetworkRaw  string    `json:"network"`
	Sender      string    `json:"sender"`
	PackageID   string    `json:"packageId"`
	Status      string    `json:"status"`
	Description string    `json:"description,omitempty"`
	CreatedAt   string    `json:"createdAt"`
	RetryCount  int       `json:"retryCount,omitempty"`
	LastError   string    `json:"lastError,omitempty"`
	ProcessedAt time.Time `json:"processedAt,omitempty"`
	// SourceQueue is the RabbitMQ queue this message was consumed from.
	// Set at consumption time and used to select the correct DLR URL.
	// Not serialized — runtime only.
	SourceQueue string `json:"-"`
}

// Network returns the parsed Network type
func (m *Message) Network() Network {
	return ParseNetwork(m.NetworkRaw)
}

// IsTransactional checks if the message is explicitly marked as transactional via packageId.
// Used for Safaricom sender selection (SMPP vs SDP). Does not consider source queue.
func (m *Message) IsTransactional() bool {
	return strings.ToUpper(strings.TrimSpace(m.PackageID)) == "TRANSACTIONAL"
}

// IsPromotional returns true when the message should be treated as promotional for retry routing.
// A message is promotional when its source queue is the configured gateway queue.
// PackageID "TRANSACTIONAL" always overrides this — such messages are never promotional.
// When SourceQueue is unset (e.g. direct injection in tests), falls back to treating
// the message as promotional if packageId is not "TRANSACTIONAL".
func (m *Message) IsPromotional(gatewayQueueName string) bool {
	if strings.ToUpper(strings.TrimSpace(m.PackageID)) == "TRANSACTIONAL" {
		return false
	}
	if m.SourceQueue != "" && gatewayQueueName != "" {
		return m.SourceQueue == gatewayQueueName
	}
	return true
}

// NormalizeMSISDN converts local format (0xxx) to international format (254xxx)
// This is specific to Kenya phone numbers
func (m *Message) NormalizeMSISDN() string {
	msisdn := strings.TrimSpace(m.MSISDN)

	// Already in international format
	if strings.HasPrefix(msisdn, "254") {
		return msisdn
	}

	// Convert local format to international
	if strings.HasPrefix(msisdn, "0") {
		return "254" + strings.TrimPrefix(msisdn, "0")
	}

	// Handle +254 format
	if strings.HasPrefix(msisdn, "+254") {
		return strings.TrimPrefix(msisdn, "+")
	}

	// Return as-is if format is unknown
	return msisdn
}

// Validate checks if the message has all required fields
func (m *Message) Validate() error {
	if m.Correlator == "" {
		return ErrMissingCorrelator
	}
	if m.Content == "" {
		return ErrMissingContent
	}
	if m.MSISDN == "" {
		return ErrMissingMSISDN
	}
	if !m.Network().IsValid() {
		return ErrUnknownNetwork
	}
	if m.Sender == "" {
		return ErrMissingSender
	}
	return nil
}

// SetStatus updates the message status
func (m *Message) SetStatus(status string) {
	m.Status = status
	m.Description = "success"
	m.ProcessedAt = time.Now()
}

// SetError updates the message with error information
func (m *Message) SetError(err error) {
	m.LastError = err.Error()
	m.Description = err.Error()
	m.Status = StatusFailed
}

// IncrementRetryCount increments the retry count
func (m *Message) IncrementRetryCount() {
	m.RetryCount++
}

// CanRetry checks if the message can be retried
func (m *Message) CanRetry(maxRetries int) bool {
	return m.RetryCount < maxRetries
}

// Clone creates a deep copy of the message
func (m *Message) Clone() *Message {
	return &Message{
		Correlator:  m.Correlator,
		Content:     m.Content,
		MSISDN:      m.MSISDN,
		NetworkRaw:  m.NetworkRaw,
		Sender:      m.Sender,
		PackageID:   m.PackageID,
		Status:      m.Status,
		Description: m.Description,
		CreatedAt:   m.CreatedAt,
		RetryCount:  m.RetryCount,
		LastError:   m.LastError,
		ProcessedAt: m.ProcessedAt,
		SourceQueue: m.SourceQueue,
	}
}

// Message status constants
const (
	StatusPending   = "PENDING"
	StatusSent      = "SENT"
	StatusFailed    = "FAILED TO SEND"
	StatusRetrying  = "RETRYING"
	StatusDelivered = "DELIVERED"
	StatusUnknown   = "UNKNOWN"
)
