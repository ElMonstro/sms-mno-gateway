package service

import (
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Router routes messages to the appropriate MNO sender
// Handles transactional routing for Safaricom
type Router struct {
	factory ports.MNOSenderFactory
	log     logger.Logger
}

// NewRouter creates a new router
func NewRouter(factory ports.MNOSenderFactory, log logger.Logger) *Router {
	return &Router{
		factory: factory,
		log:     log,
	}
}

// GetSender returns the appropriate sender for a message
// Considers network and whether the message is transactional
func (r *Router) GetSender(msg *domain.Message) (ports.MNOSender, error) {
	sender, err := r.factory.GetSender(msg)
	if err != nil {
		r.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"network":    msg.NetworkRaw,
			"error":      err.Error(),
		}).Error("Failed to get sender for message")
		return nil, err
	}

	r.log.WithFields(map[string]interface{}{
		"correlator":    msg.Correlator,
		"network":       msg.Network().String(),
		"sender":        sender.Name(),
		"transactional": msg.IsTransactional(),
	}).Debug("Routed message to sender")

	return sender, nil
}

// GetSenderByNetwork returns the default sender for a network
func (r *Router) GetSenderByNetwork(network domain.Network) (ports.MNOSender, error) {
	return r.factory.GetSenderByNetwork(network)
}

// ListSenders returns all available senders
func (r *Router) ListSenders() []ports.MNOSender {
	return r.factory.ListSenders()
}

// IsNetworkHealthy checks if the network's sender is healthy
func (r *Router) IsNetworkHealthy(network domain.Network) bool {
	sender, err := r.factory.GetSenderByNetwork(network)
	if err != nil {
		return false
	}
	return sender.IsHealthy()
}
