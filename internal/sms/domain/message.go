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
	CreatedAt   string    `json:"createdAt"`
	RetryCount  int       `json:"retryCount,omitempty"`
	LastError   string    `json:"lastError,omitempty"`
	ProcessedAt time.Time `json:"processedAt,omitempty"`
}

// Network returns the parsed Network type
func (m *Message) Network() Network {
	return ParseNetwork(m.NetworkRaw)
}

// IsTransactional checks if the message is marked as transactional
// Transactional messages for Safaricom are routed via SMPP instead of SDP
func (m *Message) IsTransactional() bool {
	return strings.ToUpper(strings.TrimSpace(m.PackageID)) == "TRANSACTIONAL"
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
	m.ProcessedAt = time.Now()
}

// SetError updates the message with error information
func (m *Message) SetError(err error) {
	m.LastError = err.Error()
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
		CreatedAt:   m.CreatedAt,
		RetryCount:  m.RetryCount,
		LastError:   m.LastError,
		ProcessedAt: m.ProcessedAt,
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
