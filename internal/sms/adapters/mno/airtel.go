package mno

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// AirtelSender sends SMS via Airtel SMPP gateway (Kannel)
type AirtelSender struct {
	*BaseSMPPSender
}

// NewAirtelSender creates a new Airtel SMPP sender
func NewAirtelSender(
	baseURL, smsc, username, password, dlrURL, dlrURLApiV2 string,
	httpClient *httpclient.Client,
	circuitBreaker *circuitbreaker.CircuitBreaker,
	metrics ports.Metrics,
	log logger.Logger,
) *AirtelSender {
	return &AirtelSender{
		BaseSMPPSender: NewBaseSMPPSender(&SMPPConfig{
			Network:        domain.NetworkAirtel,
			Name:           "Airtel SMPP",
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

// Send sends a message via Airtel SMPP gateway
func (s *AirtelSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	return s.BaseSMPPSender.Send(ctx, msg)
}

// Name returns the name of this sender
func (s *AirtelSender) Name() string {
	return "Airtel SMPP"
}
