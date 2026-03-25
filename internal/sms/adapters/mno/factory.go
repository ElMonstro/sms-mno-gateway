package mno

import (
	"fmt"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/config"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Factory creates and manages MNO senders
type Factory struct {
	safaricomSDP  *SafaricomSDPSender
	safaricomSMPP *SafaricomSMPPSender
	airtel        *AirtelSender
	airtelPromo   *AirtelSender
	telkom        *TelkomSender
	equitel       *EquitelSender
	cm            *CMSender
	log           logger.Logger
}

// FactoryConfig holds configuration for the MNO factory
type FactoryConfig struct {
	Config          *config.Config
	HTTPClient      *httpclient.Client
	TokenCache      ports.TokenCache
	BreakerRegistry *circuitbreaker.BreakerRegistry
	Metrics         ports.Metrics
	Logger          logger.Logger
}

// NewFactory creates a new MNO sender factory
func NewFactory(cfg *FactoryConfig) *Factory {
	log := cfg.Logger

	// Configure the DLR gateway queue name so BaseSMPPSender can select
	// the correct DLR URL based on the message's source queue.
	SetGatewayQueueName(cfg.Config.Queues.GatewayQueueName)

	// Get circuit breakers
	safCB, _ := cfg.BreakerRegistry.Get(domain.NetworkSafaricom)
	airCB, _ := cfg.BreakerRegistry.Get(domain.NetworkAirtel)
	telCB, _ := cfg.BreakerRegistry.Get(domain.NetworkTelkom)
	equCB, _ := cfg.BreakerRegistry.Get(domain.NetworkEquitel)
	cmCB, _ := cfg.BreakerRegistry.Get(domain.NetworkCM)

	mnoCfg := cfg.Config.MNO

	return &Factory{
		safaricomSDP: NewSafaricomSDPSender(&SDPConfig{
			AuthURL:        mnoCfg.SafaricomSDP.AuthURL,
			SendURL:        mnoCfg.SafaricomSDP.SendURL,
			AuthUsername:   mnoCfg.SafaricomSDP.AuthUser,
			Username:       mnoCfg.SafaricomSDP.Username,
			Password:       mnoCfg.SafaricomSDP.Password,
			DLRURL:         mnoCfg.SafaricomSDP.DLRURL,
			DLRURLApiV2:    mnoCfg.SafaricomSDP.DLRURLApiV2,
			TokenKey:       mnoCfg.SafaricomSDP.TokenKey,
			TokenTTL:       mnoCfg.SafaricomSDP.TokenTTL,
			TokenCache:     cfg.TokenCache,
			HTTPClient:     cfg.HTTPClient,
			CircuitBreaker: safCB,
			Metrics:        cfg.Metrics,
			Logger:         log,
		}),
		safaricomSMPP: NewSafaricomSMPPSender(
			mnoCfg.SafaricomSMPP.URL,
			mnoCfg.SafaricomSMPP.SMSC,
			mnoCfg.SafaricomSMPP.Username,
			mnoCfg.SafaricomSMPP.Password,
			mnoCfg.SafaricomSMPP.DLRURL,
			mnoCfg.SafaricomSMPP.DLRURLApiV2,
			cfg.HTTPClient,
			safCB,
			cfg.Metrics,
			log,
		),
		airtel: NewAirtelSender(
			mnoCfg.Airtel.URL,
			mnoCfg.Airtel.SMSC,
			mnoCfg.Airtel.Username,
			mnoCfg.Airtel.Password,
			mnoCfg.Airtel.DLRURL,
			mnoCfg.Airtel.DLRURLApiV2,
			cfg.HTTPClient,
			airCB,
			cfg.Metrics,
			log,
		),
		airtelPromo: NewAirtelSender(
			mnoCfg.AirtelPromo.URL,
			mnoCfg.AirtelPromo.SMSC,
			mnoCfg.AirtelPromo.Username,
			mnoCfg.AirtelPromo.Password,
			mnoCfg.AirtelPromo.DLRURL,
			mnoCfg.AirtelPromo.DLRURLApiV2,
			cfg.HTTPClient,
			airCB,
			cfg.Metrics,
			log,
		),
		telkom: NewTelkomSender(
			mnoCfg.Telkom.URL,
			mnoCfg.Telkom.SMSC,
			mnoCfg.Telkom.Username,
			mnoCfg.Telkom.Password,
			mnoCfg.Telkom.DLRURL,
			mnoCfg.Telkom.DLRURLApiV2,
			cfg.HTTPClient,
			telCB,
			cfg.Metrics,
			log,
		),
		equitel: NewEquitelSender(
			mnoCfg.Equitel.URL,
			mnoCfg.Equitel.SMSC,
			mnoCfg.Equitel.Username,
			mnoCfg.Equitel.Password,
			mnoCfg.Equitel.DLRURL,
			mnoCfg.Equitel.DLRURLApiV2,
			cfg.HTTPClient,
			equCB,
			cfg.Metrics,
			log,
		),
		cm: NewCMSender(
			mnoCfg.CM.URL,
			mnoCfg.CM.SMSC,
			mnoCfg.CM.Username,
			mnoCfg.CM.Password,
			mnoCfg.CM.DLRURL,
			cfg.HTTPClient,
			cmCB,
			cfg.Metrics,
			log,
		),
		log: log,
	}
}

// GetSender returns the appropriate sender for the given message
// It considers the network and whether the message is transactional
func (f *Factory) GetSender(msg *domain.Message) (ports.MNOSender, error) {
	network := msg.Network()

	// Safaricom transactional routing
	if network.IsSafaricom() && msg.IsTransactional() {
		f.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"packageId":  msg.PackageID,
		}).Debug("Routing to Safaricom SMPP (transactional)")
		return f.safaricomSMPP, nil
	}

	// Airtel traffic split by packageId — transactional and promotional
	// use separate SMPP credentials
	if network == domain.NetworkAirtel {
		if msg.IsTransactional() {
			f.log.WithFields(map[string]interface{}{
				"correlator": msg.Correlator,
				"packageId":  msg.PackageID,
			}).Debug("Routing to Airtel SMPP (transactional)")
			return f.airtel, nil
		}
		f.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"packageId":  msg.PackageID,
		}).Debug("Routing to Airtel SMPP (promotional)")
		return f.airtelPromo, nil
	}

	return f.GetSenderByNetwork(network)
}

// GetSenderByNetwork returns the default sender for a network
func (f *Factory) GetSenderByNetwork(network domain.Network) (ports.MNOSender, error) {
	switch network {
	case domain.NetworkSafaricom:
		return f.safaricomSDP, nil
	case domain.NetworkAirtel:
		return f.airtel, nil
	case domain.NetworkTelkom:
		return f.telkom, nil
	case domain.NetworkEquitel:
		return f.equitel, nil
	case domain.NetworkCM, domain.NetworkINTNL:
		return f.cm, nil
	default:
		return nil, fmt.Errorf("%w: %s", domain.ErrUnknownNetwork, network)
	}
}

// ListSenders returns all available senders
func (f *Factory) ListSenders() []ports.MNOSender {
	return []ports.MNOSender{
		f.safaricomSDP,
		f.safaricomSMPP,
		f.airtel,
		f.airtelPromo,
		f.telkom,
		f.equitel,
		f.cm,
	}
}

// Ensure Factory implements ports.MNOSenderFactory
var _ ports.MNOSenderFactory = (*Factory)(nil)
