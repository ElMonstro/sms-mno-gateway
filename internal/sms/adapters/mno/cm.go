package mno

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// CMSender sends SMS via CM (International) SMPP gateway (Kannel)
// Also used for INTNL network
type CMSender struct {
	*BaseSMPPSender
}

// NewCMSender creates a new CM SMPP sender
func NewCMSender(
	baseURL, smsc, username, password, dlrURL string,
	httpClient *httpclient.Client,
	circuitBreaker *circuitbreaker.CircuitBreaker,
	metrics ports.Metrics,
	log logger.Logger,
) *CMSender {
	return &CMSender{
		BaseSMPPSender: NewBaseSMPPSender(&SMPPConfig{
			Network:        domain.NetworkCM,
			Name:           "CM International SMPP",
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

// Send sends a message via CM SMPP gateway
func (s *CMSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	return s.BaseSMPPSender.Send(ctx, msg)
}

// Name returns the name of this sender
func (s *CMSender) Name() string {
	return "CM International SMPP"
}
