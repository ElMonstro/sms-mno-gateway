package mno

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

func newTestAfricasTalkingSender(t *testing.T, serverURL string) *AfricasTalkingSender {
	t.Helper()
	return NewAfricasTalkingSender(&AfricasTalkingConfig{
		SandboxURL:    serverURL,
		ProductionURL: serverURL,
		Mode:          "sandbox",
		APIKey:        "sandbox-key",
		APIKeyProd:    "prod-key",
		Username:      "sandbox",
		HTTPClient:    httpclient.New(httpclient.DefaultConfig()),
		Logger:        logger.NewNoop(),
	})
}

func testMessage() *domain.Message {
	return &domain.Message{
		Correlator: "123",
		Content:    "hello",
		MSISDN:     "233241234567",
		NetworkRaw: "Intnl",
		Sender:     "EMALIFY",
		OutboxID:   123,
	}
}

func TestAfricasTalkingSender_Send_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("apiKey") != "sandbox-key" {
			t.Errorf("expected apiKey header 'sandbox-key', got %q", r.Header.Get("apiKey"))
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("unexpected Content-Type: %q", r.Header.Get("Content-Type"))
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse form: %v", err)
		}
		if got := r.PostForm.Get("to"); got != "+233241234567" {
			t.Errorf("expected to=+233241234567, got %q", got)
		}
		if got := r.PostForm.Get("from"); got != "EMALIFY" {
			t.Errorf("expected from=EMALIFY, got %q", got)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"SMSMessageData":{"Message":"Sent","Recipients":[
			{"statusCode":101,"number":"+233241234567","status":"Success","cost":"KES 0.8","messageId":"ATXid_123"}
		]}}`))
	}))
	defer server.Close()

	sender := newTestAfricasTalkingSender(t, server.URL)
	result := sender.Send(context.Background(), testMessage())

	if !result.IsSuccess() {
		t.Fatalf("expected success, got %v (err=%v)", result.Type, result.Error)
	}
	if result.ExternalMessageID != "ATXid_123" {
		t.Errorf("expected ExternalMessageID ATXid_123, got %q", result.ExternalMessageID)
	}
	if result.NetworkCode != "101" {
		t.Errorf("expected NetworkCode 101, got %q", result.NetworkCode)
	}
}

func TestAfricasTalkingSender_Send_RejectedByAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"SMSMessageData":{"Message":"Failed","Recipients":[
			{"statusCode":401,"number":"+233241234567","status":"InvalidSenderId","cost":"0","messageId":"None"}
		]}}`))
	}))
	defer server.Close()

	sender := newTestAfricasTalkingSender(t, server.URL)
	result := sender.Send(context.Background(), testMessage())

	if result.IsSuccess() {
		t.Fatalf("expected failure, got success")
	}
	if !result.IsPermanent() {
		t.Errorf("expected permanent failure for API-level rejection, got %v", result.Type)
	}
}

func TestAfricasTalkingSender_Send_ServerError_Retryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`upstream unavailable`))
	}))
	defer server.Close()

	sender := newTestAfricasTalkingSender(t, server.URL)
	result := sender.Send(context.Background(), testMessage())

	if !result.IsRetryable() {
		t.Fatalf("expected retryable failure for 503, got %v", result.Type)
	}
}

func TestAfricasTalkingSender_Send_BadRequest_Permanent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`bad request`))
	}))
	defer server.Close()

	sender := newTestAfricasTalkingSender(t, server.URL)
	result := sender.Send(context.Background(), testMessage())

	if !result.IsPermanent() {
		t.Fatalf("expected permanent failure for 400, got %v", result.Type)
	}
}

func TestAfricasTalkingSender_Send_ProductionMode_UsesProdKeyAndURL(t *testing.T) {
	var gotAPIKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("apiKey")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"SMSMessageData":{"Message":"Sent","Recipients":[
			{"statusCode":101,"number":"+233241234567","status":"Success","cost":"0","messageId":"id"}
		]}}`))
	}))
	defer server.Close()

	sandboxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("sandbox URL should not be called in production mode")
	}))
	defer sandboxServer.Close()

	sender := NewAfricasTalkingSender(&AfricasTalkingConfig{
		SandboxURL:    sandboxServer.URL,
		ProductionURL: server.URL,
		Mode:          "production",
		APIKey:        "sandbox-key",
		APIKeyProd:    "prod-key",
		Username:      "prod-user",
		HTTPClient:    httpclient.New(httpclient.DefaultConfig()),
		Logger:        logger.NewNoop(),
	})

	result := sender.Send(context.Background(), testMessage())
	if !result.IsSuccess() {
		t.Fatalf("expected success, got %v (err=%v)", result.Type, result.Error)
	}
	if gotAPIKey != "prod-key" {
		t.Errorf("expected production apiKey to be used, got %q", gotAPIKey)
	}
}

func TestAfricasTalkingSender_Send_InsufficientBalance_TripsCooldown(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"SMSMessageData":{"Message":"Sent to 0/1 Total Cost: 0","Recipients":[
			{"statusCode":405,"number":"+233241234567","status":"InsufficientBalance","cost":"0","messageId":"None"}
		]}}`))
	}))
	defer server.Close()

	sender := NewAfricasTalkingSender(&AfricasTalkingConfig{
		SandboxURL:      server.URL,
		ProductionURL:   server.URL,
		Mode:            "sandbox",
		APIKey:          "sandbox-key",
		Username:        "sandbox",
		HTTPClient:      httpclient.New(httpclient.DefaultConfig()),
		Logger:          logger.NewNoop(),
		BalanceCooldown: time.Minute,
	})

	first := sender.Send(context.Background(), testMessage())
	if !first.IsPermanent() {
		t.Fatalf("expected permanent failure for InsufficientBalance, got %v", first.Type)
	}
	if callCount != 1 {
		t.Fatalf("expected exactly 1 real API call for the first send, got %d", callCount)
	}

	// A second send, still within the cooldown window, must not hit the API at all —
	// this is what stops a persistent InsufficientBalance from turning into a tight
	// loop of real AfricasTalking API calls.
	second := sender.Send(context.Background(), testMessage())
	if !second.IsPermanent() || !errors.Is(second.Error, domain.ErrInsufficientBalance) {
		t.Fatalf("expected second send to short-circuit with ErrInsufficientBalance, got %v (%v)", second.Type, second.Error)
	}
	if callCount != 1 {
		t.Fatalf("expected cooldown to prevent a second real API call, but callCount=%d", callCount)
	}
}

func TestAfricasTalkingSender_Send_InternalServerError_Retryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"SMSMessageData":{"Message":"Failed","Recipients":[
			{"statusCode":500,"number":"+233241234567","status":"InternalServerError","cost":"0","messageId":"None"}
		]}}`))
	}))
	defer server.Close()

	sender := newTestAfricasTalkingSender(t, server.URL)
	result := sender.Send(context.Background(), testMessage())

	if !result.IsRetryable() {
		t.Fatalf("expected retryable failure for AT-side InternalServerError (500), got %v", result.Type)
	}
}

func TestAfricasTalkingSender_Send_GatewayError_Retryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"SMSMessageData":{"Message":"Failed","Recipients":[
			{"statusCode":501,"number":"+233241234567","status":"GatewayError","cost":"0","messageId":"None"}
		]}}`))
	}))
	defer server.Close()

	sender := newTestAfricasTalkingSender(t, server.URL)
	result := sender.Send(context.Background(), testMessage())

	if !result.IsRetryable() {
		t.Fatalf("expected retryable failure for AT-side GatewayError (501), got %v", result.Type)
	}
}

func TestAfricasTalkingSender_Send_RejectedByGateway_Permanent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"SMSMessageData":{"Message":"Failed","Recipients":[
			{"statusCode":502,"number":"+233241234567","status":"RejectedByGateway","cost":"0","messageId":"None"}
		]}}`))
	}))
	defer server.Close()

	sender := newTestAfricasTalkingSender(t, server.URL)
	result := sender.Send(context.Background(), testMessage())

	// 502 sits in the 5xx range but AT's docs describe it as an unmapped
	// senderId/shortcode — a permanent config issue, not a transient gateway error.
	if !result.IsPermanent() {
		t.Fatalf("expected permanent failure for RejectedByGateway (502), got %v", result.Type)
	}
}

func TestAfricasTalkingSender_Send_ProcessedAndQueuedCodes_TreatedAsSuccess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		status     string
	}{
		{"processed", 100, "Processed"},
		{"queued", 102, "Queued"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				fmt.Fprintf(w, `{"SMSMessageData":{"Message":"Sent","Recipients":[
					{"statusCode":%d,"number":"+233241234567","status":"%s","cost":"0","messageId":"ATXid_x"}
				]}}`, tc.statusCode, tc.status)
			}))
			defer server.Close()

			sender := newTestAfricasTalkingSender(t, server.URL)
			result := sender.Send(context.Background(), testMessage())

			if !result.IsSuccess() {
				t.Fatalf("expected success for statusCode %d (%s), got %v", tc.statusCode, tc.status, result.Type)
			}
		})
	}
}

func TestAfricasTalkingSender_NetworkAndName(t *testing.T) {
	sender := newTestAfricasTalkingSender(t, "http://example.invalid")
	if sender.Network() != domain.NetworkINTNL {
		t.Errorf("expected Network() to be NetworkINTNL, got %v", sender.Network())
	}
	if sender.Name() == "" {
		t.Error("expected non-empty Name()")
	}
	if !sender.IsHealthy() {
		t.Error("expected sender to be healthy without a circuit breaker")
	}
}
