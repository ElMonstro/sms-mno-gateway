package service

import (
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

func TestRouter_GetSender(t *testing.T) {
	factory := mocks.NewMockMNOSenderFactory()
	log := logger.NewNoop()

	// Register mock senders
	safaricomSender := mocks.NewMockMNOSender("safaricom-sdp", domain.NetworkSafaricom)
	airtelSender := mocks.NewMockMNOSender("airtel-smpp", domain.NetworkAirtel)
	telkomSender := mocks.NewMockMNOSender("telkom-smpp", domain.NetworkTelkom)

	factory.RegisterSender(safaricomSender)
	factory.RegisterSender(airtelSender)
	factory.RegisterSender(telkomSender)

	router := NewRouter(factory, log)

	tests := []struct {
		name           string
		network        string
		expectedSender string
	}{
		{
			name:           "safaricom message",
			network:        "SAFARICOM",
			expectedSender: "safaricom-sdp",
		},
		{
			name:           "airtel message",
			network:        "AIRTEL",
			expectedSender: "airtel-smpp",
		},
		{
			name:           "telkom message",
			network:        "TELKOM",
			expectedSender: "telkom-smpp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &domain.Message{
				Correlator: "test-123",
				Content:    "Hello",
				MSISDN:     "254722123456",
				NetworkRaw: tt.network,
				Sender:     "TestSender",
			}

			sender, err := router.GetSender(msg)
			if err != nil {
				t.Fatalf("GetSender() error = %v", err)
			}

			if sender.Name() != tt.expectedSender {
				t.Errorf("GetSender() sender name = %q, want %q", sender.Name(), tt.expectedSender)
			}
		})
	}
}

func TestRouter_GetSender_UnknownNetwork(t *testing.T) {
	factory := mocks.NewMockMNOSenderFactory()
	log := logger.NewNoop()

	router := NewRouter(factory, log)

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "UNKNOWN",
		Sender:     "TestSender",
	}

	_, err := router.GetSender(msg)
	if err == nil {
		t.Error("GetSender() expected error for unknown network, got nil")
	}
}

func TestRouter_GetSenderByNetwork(t *testing.T) {
	factory := mocks.NewMockMNOSenderFactory()
	log := logger.NewNoop()

	safaricomSender := mocks.NewMockMNOSender("safaricom-sdp", domain.NetworkSafaricom)
	factory.RegisterSender(safaricomSender)

	router := NewRouter(factory, log)

	sender, err := router.GetSenderByNetwork(domain.NetworkSafaricom)
	if err != nil {
		t.Fatalf("GetSenderByNetwork() error = %v", err)
	}

	if sender.Name() != "safaricom-sdp" {
		t.Errorf("GetSenderByNetwork() name = %q, want %q", sender.Name(), "safaricom-sdp")
	}
}

func TestRouter_GetSenderByNetwork_NotFound(t *testing.T) {
	factory := mocks.NewMockMNOSenderFactory()
	log := logger.NewNoop()

	router := NewRouter(factory, log)

	_, err := router.GetSenderByNetwork(domain.NetworkSafaricom)
	if err == nil {
		t.Error("GetSenderByNetwork() expected error for missing network, got nil")
	}
}

func TestRouter_ListSenders(t *testing.T) {
	factory := mocks.NewMockMNOSenderFactory()
	log := logger.NewNoop()

	factory.RegisterSender(mocks.NewMockMNOSender("safaricom-sdp", domain.NetworkSafaricom))
	factory.RegisterSender(mocks.NewMockMNOSender("airtel-smpp", domain.NetworkAirtel))
	factory.RegisterSender(mocks.NewMockMNOSender("telkom-smpp", domain.NetworkTelkom))

	router := NewRouter(factory, log)

	senders := router.ListSenders()
	if len(senders) != 3 {
		t.Errorf("ListSenders() returned %d senders, want 3", len(senders))
	}
}

func TestRouter_IsNetworkHealthy(t *testing.T) {
	factory := mocks.NewMockMNOSenderFactory()
	log := logger.NewNoop()

	healthySender := mocks.NewMockMNOSender("safaricom-sdp", domain.NetworkSafaricom)
	healthySender.SetHealthy(true)

	unhealthySender := mocks.NewMockMNOSender("airtel-smpp", domain.NetworkAirtel)
	unhealthySender.SetHealthy(false)

	factory.RegisterSender(healthySender)
	factory.RegisterSender(unhealthySender)

	router := NewRouter(factory, log)

	tests := []struct {
		name     string
		network  domain.Network
		expected bool
	}{
		{
			name:     "healthy network",
			network:  domain.NetworkSafaricom,
			expected: true,
		},
		{
			name:     "unhealthy network",
			network:  domain.NetworkAirtel,
			expected: false,
		},
		{
			name:     "missing network",
			network:  domain.NetworkTelkom,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := router.IsNetworkHealthy(tt.network)
			if result != tt.expected {
				t.Errorf("IsNetworkHealthy() = %v, want %v", result, tt.expected)
			}
		})
	}
}
