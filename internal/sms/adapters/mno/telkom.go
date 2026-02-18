package mno

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// TelkomSender sends SMS via Telkom SMPP gateway (Kannel)
type TelkomSender struct {
	*BaseSMPPSender
}

// NewTelkomSender creates a new Telkom SMPP sender
func NewTelkomSender(
	baseURL, smsc, username, password, dlrURL string,
	httpClient *httpclient.Client,
	circuitBreaker *circuitbreaker.CircuitBreaker,
	metrics ports.Metrics,
	log logger.Logger,
) *TelkomSender {
	return &TelkomSender{
		BaseSMPPSender: NewBaseSMPPSender(&SMPPConfig{
			Network:        domain.NetworkTelkom,
			Name:           "Telkom SMPP",
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

// Send sends a message via Telkom SMPP gateway
func (s *TelkomSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	return s.BaseSMPPSender.Send(ctx, msg)
}

// Name returns the name of this sender
func (s *TelkomSender) Name() string {
	return "Telkom SMPP"
}
