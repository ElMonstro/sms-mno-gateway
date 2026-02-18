package mno

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// SafaricomSMPPSender sends SMS via Safaricom SMPP (Kannel)
// Used for transactional SMS that require faster delivery
type SafaricomSMPPSender struct {
	*BaseSMPPSender
}

// NewSafaricomSMPPSender creates a new Safaricom SMPP sender
func NewSafaricomSMPPSender(
	baseURL, smsc, username, password, dlrURL string,
	httpClient *httpclient.Client,
	circuitBreaker *circuitbreaker.CircuitBreaker,
	metrics ports.Metrics,
	log logger.Logger,
) *SafaricomSMPPSender {
	return &SafaricomSMPPSender{
		BaseSMPPSender: NewBaseSMPPSender(&SMPPConfig{
			Network:        domain.NetworkSafaricom,
			Name:           "Safaricom SMPP (Transactional)",
			BaseURL:        baseURL,
			SMSC:           smsc,
			Username:       username,
			Password:       password,
			DLRURL:         dlrURL,
			HTTPClient:     httpClient,
			CircuitBreaker: circuitBreaker,
			Metrics:        metrics,
			Logger:         log,
		}),
	}
}

// Send sends a message via Safaricom SMPP gateway
func (s *SafaricomSMPPSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	return s.BaseSMPPSender.Send(ctx, msg)
}

// Name returns the name of this sender
func (s *SafaricomSMPPSender) Name() string {
	return "Safaricom SMPP (Transactional)"
}
