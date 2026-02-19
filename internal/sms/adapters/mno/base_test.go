package mno

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

func newTestSMPPSender(serverURL string) *BaseSMPPSender {
	return &BaseSMPPSender{
		network:    domain.NetworkAirtel,
		name:       "test-smpp",
		baseURL:    serverURL + "/cgi-bin/sendsms",
		smsc:       "TEST_SMSC",
		username:   "testuser",
		password:   "testpass",
		dlrURL:     "http://localhost:8088/dlr",
		httpClient: httpclient.New(httpclient.DefaultConfig()),
		log:        logger.NewNoop(),
	}
}

func newTestMessage() *domain.Message {
	return &domain.Message{
		Correlator: "test-correlator-123",
		Content:    "Hello World",
		MSISDN:     "254722123456",
		NetworkRaw: "AIRTEL",
		Sender:     "TestSender",
	}
}

func TestBaseSMPPSender_Send_Success(t *testing.T) {
	// Create test server that returns success
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify query parameters
		if r.URL.Query().Get("smsc") != "TEST_SMSC" {
			t.Errorf("Expected smsc=TEST_SMSC, got %s", r.URL.Query().Get("smsc"))
		}
		if r.URL.Query().Get("to") != "254722123456" {
			t.Errorf("Expected to=254722123456, got %s", r.URL.Query().Get("to"))
		}
		if r.URL.Query().Get("from") != "TestSender" {
			t.Errorf("Expected from=TestSender, got %s", r.URL.Query().Get("from"))
		}
		if r.URL.Query().Get("dlr-mask") != "31" {
			t.Errorf("Expected dlr-mask=31, got %s", r.URL.Query().Get("dlr-mask"))
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("0: Accepted for delivery"))
	}))
	defer server.Close()

	sender := newTestSMPPSender(server.URL)
	msg := newTestMessage()

	result := sender.Send(context.Background(), msg)

	if !result.IsSuccess() {
		t.Errorf("Expected success, got %v with error: %v", result.Type, result.Error)
	}
	if result.MNOResponse != "0: Accepted for delivery" {
		t.Errorf("Expected response '0: Accepted for delivery', got %q", result.MNOResponse)
	}
	if msg.Status != domain.StatusSent {
		t.Errorf("Expected message status %s, got %s", domain.StatusSent, msg.Status)
	}
}

func TestBaseSMPPSender_Send_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	sender := newTestSMPPSender(server.URL)
	msg := newTestMessage()

	result := sender.Send(context.Background(), msg)

	// 5xx errors should be retryable
	if !result.IsRetryable() {
		t.Errorf("Expected retryable result for 500, got %v", result.Type)
	}
}

func TestBaseSMPPSender_Send_BadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad Request: Invalid MSISDN"))
	}))
	defer server.Close()

	sender := newTestSMPPSender(server.URL)
	msg := newTestMessage()

	result := sender.Send(context.Background(), msg)

	// 4xx errors should be permanent
	if !result.IsPermanent() {
		t.Errorf("Expected permanent result for 400, got %v", result.Type)
	}
}

func TestBaseSMPPSender_Send_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay longer than context timeout
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("0: Accepted for delivery"))
	}))
	defer server.Close()

	sender := newTestSMPPSender(server.URL)
	msg := newTestMessage()

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result := sender.Send(ctx, msg)

	// Timeouts should be retryable
	if !result.IsRetryable() {
		t.Errorf("Expected retryable result for timeout, got %v", result.Type)
	}
}

func TestBaseSMPPSender_Send_ConnectionRefused(t *testing.T) {
	// Use a server that's immediately closed
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	serverURL := server.URL
	server.Close()

	sender := newTestSMPPSender(serverURL)
	msg := newTestMessage()

	result := sender.Send(context.Background(), msg)

	// Connection errors should be retryable
	if !result.IsRetryable() {
		t.Errorf("Expected retryable result for connection error, got %v", result.Type)
	}
}

func TestBaseSMPPSender_Send_InvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("1: Rejected by carrier"))
	}))
	defer server.Close()

	sender := newTestSMPPSender(server.URL)
	msg := newTestMessage()

	result := sender.Send(context.Background(), msg)

	// 200 without "0: Accepted" should be permanent failure
	if !result.IsPermanent() {
		t.Errorf("Expected permanent result for rejected message, got %v", result.Type)
	}
}

func TestBaseSMPPSender_Send_RateLimited(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("Rate limit exceeded"))
	}))
	defer server.Close()

	sender := newTestSMPPSender(server.URL)
	msg := newTestMessage()

	result := sender.Send(context.Background(), msg)

	// 429 should be retryable
	if !result.IsRetryable() {
		t.Errorf("Expected retryable result for 429, got %v", result.Type)
	}
}

func TestBaseSMPPSender_buildRequestURL(t *testing.T) {
	sender := &BaseSMPPSender{
		baseURL:  "http://example.com/sendsms",
		smsc:     "MY_SMSC",
		username: "user123",
		password: "pass456",
		dlrURL:   "http://dlr.example.com/callback",
	}

	msg := &domain.Message{
		Correlator: "corr-123",
		Content:    "Hello World",
		MSISDN:     "0722123456", // Local format
		Sender:     "MySender",
	}

	url := sender.buildRequestURL(msg)

	// Check URL contains expected components
	if !strings.Contains(url, "http://example.com/sendsms?") {
		t.Errorf("URL should start with base URL, got %s", url)
	}
	if !strings.Contains(url, "smsc=MY_SMSC") {
		t.Errorf("URL should contain smsc, got %s", url)
	}
	if !strings.Contains(url, "username=user123") {
		t.Errorf("URL should contain username, got %s", url)
	}
	if !strings.Contains(url, "to=254722123456") {
		t.Errorf("URL should contain normalized MSISDN, got %s", url)
	}
	if !strings.Contains(url, "from=MySender") {
		t.Errorf("URL should contain sender, got %s", url)
	}
	if !strings.Contains(url, "dlr-mask=31") {
		t.Errorf("URL should contain dlr-mask=31, got %s", url)
	}
}

func TestBaseSMPPSender_Network(t *testing.T) {
	sender := &BaseSMPPSender{network: domain.NetworkTelkom}

	if sender.Network() != domain.NetworkTelkom {
		t.Errorf("Expected TELKOM, got %s", sender.Network())
	}
}

func TestBaseSMPPSender_Name(t *testing.T) {
	sender := &BaseSMPPSender{name: "telkom-smpp"}

	if sender.Name() != "telkom-smpp" {
		t.Errorf("Expected telkom-smpp, got %s", sender.Name())
	}
}

func TestBaseSMPPSender_IsHealthy_NoCircuitBreaker(t *testing.T) {
	sender := &BaseSMPPSender{circuitBreaker: nil}

	if !sender.IsHealthy() {
		t.Error("Sender without circuit breaker should be healthy")
	}
}

func TestBaseSMPPSender_Send_MSISDNNormalization(t *testing.T) {
	tests := []struct {
		name           string
		inputMSISDN    string
		expectedMSISDN string
	}{
		{"local format", "0722123456", "254722123456"},
		{"international format", "254722123456", "254722123456"},
		{"plus format", "+254722123456", "254722123456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var receivedTo string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedTo = r.URL.Query().Get("to")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("0: Accepted for delivery"))
			}))
			defer server.Close()

			sender := newTestSMPPSender(server.URL)
			msg := &domain.Message{
				Correlator: "test-123",
				Content:    "Test",
				MSISDN:     tt.inputMSISDN,
				NetworkRaw: "AIRTEL",
				Sender:     "Test",
			}

			sender.Send(context.Background(), msg)

			if receivedTo != tt.expectedMSISDN {
				t.Errorf("Expected MSISDN %s, got %s", tt.expectedMSISDN, receivedTo)
			}
		})
	}
}
