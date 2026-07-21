package mno

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

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
