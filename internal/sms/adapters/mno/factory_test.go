package mno

import (
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/config"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

func createTestFactory() *Factory {
	cfg := createTestConfig()
	registry := circuitbreaker.NewRegistry()
	// Register breakers for all networks
	networks := []domain.Network{
		domain.NetworkSafaricom,
		domain.NetworkAirtel,
		domain.NetworkTelkom,
		domain.NetworkEquitel,
		domain.NetworkCM,
	}
	for _, n := range networks {
		registry.Register(n, &circuitbreaker.Config{
			Name:                n.String(),
			MaxRequests:         3,
			Timeout:             30000000000,
			ConsecutiveFailures: 5,
		})
	}

	return NewFactory(&FactoryConfig{
		Config:          cfg,
		HTTPClient:      httpclient.New(httpclient.DefaultConfig()),
		TokenCache:      NewMockTokenCache(),
		BreakerRegistry: registry,
		Logger:          logger.NewNoop(),
	})
}

func TestFactory_GetSender_NonTransactionalSafaricom(t *testing.T) {
	factory := createTestFactory()

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
		PackageID:  "0", // Non-transactional
	}

	sender, err := factory.GetSender(msg)
	if err != nil {
		t.Fatalf("GetSender() error = %v", err)
	}

	if sender.Name() != "Safaricom SDP" {
		t.Errorf("Expected SDP sender for non-transactional, got %s", sender.Name())
	}
}

func TestFactory_GetSender_TransactionalSafaricom(t *testing.T) {
	factory := createTestFactory()

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
		PackageID:  "TRANSACTIONAL", // Transactional
	}

	sender, err := factory.GetSender(msg)
	if err != nil {
		t.Fatalf("GetSender() error = %v", err)
	}

	// Name includes "(Transactional)" suffix
	if sender.Name() != "Safaricom SMPP (Transactional)" {
		t.Errorf("Expected SMPP sender for transactional, got %s", sender.Name())
	}
}

func TestFactory_GetSender_Airtel(t *testing.T) {
	factory := createTestFactory()

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254733123456",
		NetworkRaw: "AIRTEL",
		Sender:     "TestSender",
	}

	sender, err := factory.GetSender(msg)
	if err != nil {
		t.Fatalf("GetSender() error = %v", err)
	}

	if sender.Network() != domain.NetworkAirtel {
		t.Errorf("Expected AIRTEL sender, got %s", sender.Network())
	}
}

func TestFactory_GetSender_Telkom(t *testing.T) {
	factory := createTestFactory()

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254770123456",
		NetworkRaw: "TELKOM",
		Sender:     "TestSender",
	}

	sender, err := factory.GetSender(msg)
	if err != nil {
		t.Fatalf("GetSender() error = %v", err)
	}

	if sender.Network() != domain.NetworkTelkom {
		t.Errorf("Expected TELKOM sender, got %s", sender.Network())
	}
}

func TestFactory_GetSender_Equitel(t *testing.T) {
	factory := createTestFactory()

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254760123456",
		NetworkRaw: "EQUITEL",
		Sender:     "TestSender",
	}

	sender, err := factory.GetSender(msg)
	if err != nil {
		t.Fatalf("GetSender() error = %v", err)
	}

	if sender.Network() != domain.NetworkEquitel {
		t.Errorf("Expected EQUITEL sender, got %s", sender.Network())
	}
}

func TestFactory_GetSender_CM(t *testing.T) {
	factory := createTestFactory()

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "+447700123456",
		NetworkRaw: "CM",
		Sender:     "TestSender",
	}

	sender, err := factory.GetSender(msg)
	if err != nil {
		t.Fatalf("GetSender() error = %v", err)
	}

	if sender.Network() != domain.NetworkCM {
		t.Errorf("Expected CM sender, got %s", sender.Network())
	}
}

func TestFactory_GetSender_UnknownNetwork(t *testing.T) {
	factory := createTestFactory()

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello",
		MSISDN:     "254722123456",
		NetworkRaw: "VODAFONE", // Unknown
		Sender:     "TestSender",
	}

	_, err := factory.GetSender(msg)
	if err == nil {
		t.Error("Expected error for unknown network")
	}
}

func TestFactory_GetSenderByNetwork(t *testing.T) {
	factory := createTestFactory()

	tests := []struct {
		network      domain.Network
		expectedName string
	}{
		{domain.NetworkSafaricom, "Safaricom SDP"},
		{domain.NetworkAirtel, "Airtel SMPP"},
		{domain.NetworkTelkom, "Telkom SMPP"},
		{domain.NetworkEquitel, "Equitel SMPP"},
		{domain.NetworkCM, "CM International SMPP"},
	}

	for _, tt := range tests {
		t.Run(tt.network.String(), func(t *testing.T) {
			sender, err := factory.GetSenderByNetwork(tt.network)
			if err != nil {
				t.Fatalf("GetSenderByNetwork(%s) error = %v", tt.network, err)
			}
			if sender.Name() != tt.expectedName {
				t.Errorf("Expected %s, got %s", tt.expectedName, sender.Name())
			}
		})
	}
}

func TestFactory_ListSenders(t *testing.T) {
	factory := createTestFactory()

	senders := factory.ListSenders()

	// Should have 6 senders: SDP, Safaricom SMPP, Airtel, Telkom, Equitel, CM
	if len(senders) != 6 {
		t.Errorf("Expected 6 senders, got %d", len(senders))
	}

	// Verify all expected senders are present
	names := make(map[string]bool)
	for _, s := range senders {
		names[s.Name()] = true
	}

	expected := []string{"Safaricom SDP", "Safaricom SMPP (Transactional)", "Airtel SMPP", "Telkom SMPP", "Equitel SMPP", "CM International SMPP"}
	for _, name := range expected {
		if !names[name] {
			t.Errorf("Missing sender: %s", name)
		}
	}
}

// Helper to create test config
func createTestConfig() *config.Config {
	return &config.Config{
		MNO: config.MNOConfig{
			SafaricomSDP: config.SDPConfig{
				AuthURL:  "http://localhost/auth",
				SendURL:  "http://localhost/send",
				Username: "test",
				Password: "test",
				DLRURL:   "http://localhost/dlr",
				TokenKey: "test_key",
			},
			SafaricomSMPP: config.SMPPConfig{
				URL:      "http://localhost/smpp",
				SMSC:     "SAFARICOM",
				Username: "test",
				Password: "test",
				DLRURL:   "http://localhost/dlr",
			},
			Airtel: config.SMPPConfig{
				URL:      "http://localhost/airtel",
				SMSC:     "AIRTEL",
				Username: "test",
				Password: "test",
				DLRURL:   "http://localhost/dlr",
			},
			Telkom: config.SMPPConfig{
				URL:      "http://localhost/telkom",
				SMSC:     "TELKOM",
				Username: "test",
				Password: "test",
				DLRURL:   "http://localhost/dlr",
			},
			Equitel: config.SMPPConfig{
				URL:      "http://localhost/equitel",
				SMSC:     "EQUITEL",
				Username: "test",
				Password: "test",
				DLRURL:   "http://localhost/dlr",
			},
			CM: config.SMPPConfig{
				URL:      "http://localhost/cm",
				SMSC:     "CM",
				Username: "test",
				Password: "test",
				DLRURL:   "http://localhost/dlr",
			},
		},
	}
}
