package ports

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// MNOSender defines the interface for sending SMS messages to an MNO
type MNOSender interface {
	// Send sends a message to the MNO and returns the result
	Send(ctx context.Context, msg *domain.Message) *domain.SendResult

	// Network returns the network this sender handles
	Network() domain.Network

	// Name returns a human-readable name for this sender
	Name() string

	// IsHealthy returns true if the sender is healthy and can accept messages
	IsHealthy() bool
}

// BatchSender is an optional capability for MNO senders that support sending
// multiple messages in a single API call. Processors detect this capability
// via a type assertion and use it for eligible messages only.
// A single API response (success or failure) applies to all messages in the batch.
type BatchSender interface {
	SendBatch(ctx context.Context, msgs []*domain.Message) []*domain.SendResult
}

// MNOSenderFactory creates MNO senders based on network and message type
type MNOSenderFactory interface {
	// GetSender returns the appropriate sender for the given message
	// It considers the network and whether the message is transactional
	GetSender(msg *domain.Message) (MNOSender, error)

	// GetSenderByNetwork returns the default sender for a network
	GetSenderByNetwork(network domain.Network) (MNOSender, error)

	// ListSenders returns all available senders
	ListSenders() []MNOSender
}
