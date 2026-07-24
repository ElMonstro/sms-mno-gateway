package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

func newTestReporter(t *testing.T, url, token string) *AfricasTalkingReporter {
	t.Helper()
	return NewAfricasTalkingReporter(&AfricasTalkingReporterConfig{
		URL:        url,
		Token:      token,
		HTTPClient: httpclient.New(httpclient.DefaultConfig()),
		Logger:     logger.NewNoop(),
	})
}

func TestAfricasTalkingReporter_Report_Success(t *testing.T) {
	var gotToken string
	var gotPayload sendResultPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-AT-Consumer-Token")
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("failed to decode payload: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := newTestReporter(t, server.URL, "shared-secret")

	msg := &domain.Message{Correlator: "123", OutboxID: 456}
	result := domain.NewSuccessResult(msg, "ok", time.Millisecond)
	result.ExternalMessageID = "ATXid_1"
	result.NetworkCode = "101"

	if err := reporter.Report(context.Background(), result); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gotToken != "shared-secret" {
		t.Errorf("expected token 'shared-secret', got %q", gotToken)
	}
	if gotPayload.OutboxID != 456 {
		t.Errorf("expected outboxId 456, got %d", gotPayload.OutboxID)
	}
	if !gotPayload.Success {
		t.Error("expected success=true")
	}
	if gotPayload.MessageID != "ATXid_1" {
		t.Errorf("expected messageId ATXid_1, got %q", gotPayload.MessageID)
	}
}

// TestAfricasTalkingReporter_Report_IncludesMSISDNAndSender guards against a
// gap gateway-dlr-handler flagged: our payload previously had no phone number
// or sender at all, so its handler had nothing to populate those fields with
// unless it already had the data from its own outbox row.
func TestAfricasTalkingReporter_Report_IncludesMSISDNAndSender(t *testing.T) {
	var gotPayload sendResultPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("failed to decode payload: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := newTestReporter(t, server.URL, "shared-secret")

	msg := &domain.Message{Correlator: "123", OutboxID: 456, MSISDN: "260761567057", Sender: "Betika"}
	result := domain.NewSuccessResult(msg, "ok", time.Millisecond)
	result.ExternalMessageID = "ATXid_1"
	result.NetworkCode = "101"

	if err := reporter.Report(context.Background(), result); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gotPayload.MSISDN != "260761567057" {
		t.Errorf("expected msisdn 260761567057, got %q", gotPayload.MSISDN)
	}
	if gotPayload.Sender != "Betika" {
		t.Errorf("expected sender Betika, got %q", gotPayload.Sender)
	}
}

// TestAfricasTalkingReporter_Report_SerializesMessageIDAsID guards against a
// regression to the "messageId" key: gateway-dlr-handler's /save/international
// endpoint requires the field to be named "id" (matching AfricasTalking's own
// native DLR webhook), and silently rejects every callback with "missing id"
// if it isn't — decoding into the same struct on both ends of a test wouldn't
// catch that, so this asserts on the raw wire body instead.
func TestAfricasTalkingReporter_Report_SerializesMessageIDAsID(t *testing.T) {
	var rawBody string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read raw body: %v", err)
		}
		rawBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := newTestReporter(t, server.URL, "shared-secret")

	msg := &domain.Message{Correlator: "123", OutboxID: 456}
	result := domain.NewSuccessResult(msg, "ok", time.Millisecond)
	result.ExternalMessageID = "ATXid_1"
	result.NetworkCode = "101"

	if err := reporter.Report(context.Background(), result); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !strings.Contains(rawBody, `"id":"ATXid_1"`) {
		t.Errorf("expected raw payload to contain \"id\":\"ATXid_1\", got %s", rawBody)
	}
	if strings.Contains(rawBody, "messageId") {
		t.Errorf("expected raw payload not to contain the old \"messageId\" key, got %s", rawBody)
	}
}

func TestAfricasTalkingReporter_Report_FailureResult_OmitsMessageID(t *testing.T) {
	var gotPayload sendResultPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Fatalf("failed to decode payload: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reporter := newTestReporter(t, server.URL, "shared-secret")

	msg := &domain.Message{Correlator: "123", OutboxID: 456}
	result := domain.NewPermanentResult(msg, domain.ErrMNORejected, time.Millisecond)

	if err := reporter.Report(context.Background(), result); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gotPayload.Success {
		t.Error("expected success=false")
	}
	if gotPayload.MessageID != "" {
		t.Errorf("expected empty messageId on failure, got %q", gotPayload.MessageID)
	}
	if gotPayload.Status == "" {
		t.Error("expected non-empty status on failure")
	}
}

func TestAfricasTalkingReporter_Report_NonOKStatus_ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`invalid token`))
	}))
	defer server.Close()

	reporter := newTestReporter(t, server.URL, "wrong-secret")
	msg := &domain.Message{Correlator: "123", OutboxID: 456}
	result := domain.NewSuccessResult(msg, "ok", time.Millisecond)

	if err := reporter.Report(context.Background(), result); err == nil {
		t.Fatal("expected an error for non-2xx response")
	}
}

func TestAfricasTalkingReporter_Report_TransportError_ReturnsError(t *testing.T) {
	reporter := newTestReporter(t, "http://127.0.0.1:0", "secret")
	msg := &domain.Message{Correlator: "123", OutboxID: 456}
	result := domain.NewSuccessResult(msg, "ok", time.Millisecond)

	if err := reporter.Report(context.Background(), result); err == nil {
		t.Fatal("expected an error for an unreachable server")
	}
}
