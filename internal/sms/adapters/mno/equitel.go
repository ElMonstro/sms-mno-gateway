package mno

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// EquitelSender sends SMS via Equitel SMPP gateway (Kannel)
type EquitelSender struct {
	*BaseSMPPSender
}

// NewEquitelSender creates a new Equitel SMPP sender
func NewEquitelSender(
	baseURL, smsc, username, password, dlrURL, dlrURLApiV2 string,
	httpClient *httpclient.Client,
	circuitBreaker *circuitbreaker.CircuitBreaker,
	metrics ports.Metrics,
	log logger.Logger,
) *EquitelSender {
	return &EquitelSender{
		BaseSMPPSender: NewBaseSMPPSender(&SMPPConfig{
			Network:        domain.NetworkEquitel,
			Name:           "Equitel SMPP",
			BaseURL:        baseURL,
			SMSC:           smsc,
			Username:       username,
			Password:       password,
			DLRURL:         dlrURL,
			DLRURLApiV2:    dlrURLApiV2,
			HTTPClient:     httpClient,
			CircuitBreaker: circuitBreaker,
			Metrics:        metrics,
			Logger:         log,
		}),
	}
}

// Send sends a message via Equitel SMPP gateway
func (s *EquitelSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	return s.BaseSMPPSender.Send(ctx, msg)
}

// Name returns the name of this sender
func (s *EquitelSender) Name() string {
	return "Equitel SMPP"
}
