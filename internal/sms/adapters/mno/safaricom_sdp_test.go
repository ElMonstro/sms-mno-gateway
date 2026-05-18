package mno

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// MockTokenCache implements ports.TokenCache for testing
type MockTokenCache struct {
	mu     sync.Mutex
	tokens map[string]string
	ttls   map[string]time.Duration
	GetErr error
	SetErr error
}

func NewMockTokenCache() *MockTokenCache {
	return &MockTokenCache{
		tokens: make(map[string]string),
		ttls:   make(map[string]time.Duration),
	}
}

func (m *MockTokenCache) Get(ctx context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.GetErr != nil {
		return "", false, m.GetErr
	}
	token, found := m.tokens[key]
	return token, found, nil
}

func (m *MockTokenCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SetErr != nil {
		return m.SetErr
	}
	m.tokens[key] = value
	m.ttls[key] = ttl
	return nil
}

func (m *MockTokenCache) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tokens, key)
	return nil
}

func (m *MockTokenCache) Ping(ctx context.Context) error {
	return nil
}

func (m *MockTokenCache) Close() error {
	return nil
}

func (m *MockTokenCache) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, found := m.tokens[key]
	return found, nil
}

func (m *MockTokenCache) SetToken(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens[key] = value
}

func (m *MockTokenCache) GetStoredToken(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	token, found := m.tokens[key]
	return token, found
}

func TestSafaricomSDPSender_Send_Success(t *testing.T) {
	tokenCache := NewMockTokenCache()
	authCalls := 0
	sendCalls := 0

	// Create test server for auth and send
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth":
			authCalls++
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(SDPAuthResponse{
				Token:     "test-token-123",
				ExpiresIn: 1500, // 25 minutes
			})
		case "/send":
			sendCalls++
			// Verify auth header
			authHeader := r.Header.Get("X-Authorization")
			if authHeader != "Bearer test-token-123" {
				t.Errorf("Expected Bearer token, got %s", authHeader)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if countryHeader := r.Header.Get("X-Country"); countryHeader != "KEN" {
				t.Errorf("Expected X-Country KEN, got %s", countryHeader)
			}

			// Verify request body
			var req SDPSendRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("Failed to decode request: %v", err)
			}
			if len(req.DataSet) != 1 {
				t.Errorf("Expected 1 SMS record, got %d", len(req.DataSet))
			}
			if req.DataSet[0].MSISDN != "254722123456" {
				t.Errorf("Expected MSISDN 254722123456, got %s", req.DataSet[0].MSISDN)
			}
			if req.DataSet[0].PackageID != 0 {
				t.Errorf("Expected package_id 0, got %d", req.DataSet[0].PackageID)
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"success"}`))
		default:
			t.Errorf("Unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	sender := NewSafaricomSDPSender(&SDPConfig{
		AuthURL:       server.URL + "/auth",
		SendURL:       server.URL + "/send",
		CountryPrefix: "KEN",
		Username:      "testuser",
		Password:      "testpass",
		DLRURL:        "http://dlr.example.com",
		TokenKey:      "test_token_key",
		TokenTTL:      25 * time.Minute,
		TokenCache:    tokenCache,
		HTTPClient:    httpclient.New(httpclient.DefaultConfig()),
		Logger:        logger.NewNoop(),
	})

	msg := &domain.Message{
		Correlator: "test-123",
		Content:    "Hello from SDP",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
		PackageID:  "6179",
	}

	result := sender.Send(context.Background(), msg)

	if !result.IsSuccess() {
		t.Errorf("Expected success, got %v with error: %v", result.Type, result.Error)
	}

	// Verify auth was called (no cached token)
	if authCalls != 1 {
		t.Errorf("Expected 1 auth call, got %d", authCalls)
	}

	// Verify send was called
	if sendCalls != 1 {
		t.Errorf("Expected 1 send call, got %d", sendCalls)
	}

	// Verify token was cached
	if token, found := tokenCache.GetStoredToken("test_token_key"); !found || token != "test-token-123" {
		t.Errorf("Token should be cached, found=%v, token=%s", found, token)
	}
}

func TestSafaricomSDPSender_Send_UseCachedToken(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "cached-token-abc")

	authCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth":
			authCalls++
			t.Error("Auth should not be called when token is cached")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(SDPAuthResponse{Token: "new-token"})
		case "/send":
			// Verify cached token is used
			authHeader := r.Header.Get("X-Authorization")
			if authHeader != "Bearer cached-token-abc" {
				t.Errorf("Expected cached token, got %s", authHeader)
			}
			if countryHeader := r.Header.Get("X-Country"); countryHeader != "KEN" {
				t.Errorf("Expected X-Country KEN, got %s", countryHeader)
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"success"}`))
		}
	}))
	defer server.Close()

	sender := NewSafaricomSDPSender(&SDPConfig{
		AuthURL:       server.URL + "/auth",
		SendURL:       server.URL + "/send",
		CountryPrefix: "KEN",
		Username:      "testuser",
		Password:      "testpass",
		DLRURL:        "http://dlr.example.com",
		TokenKey:      "test_token_key",
		TokenTTL:      25 * time.Minute,
		TokenCache:    tokenCache,
		HTTPClient:    httpclient.New(httpclient.DefaultConfig()),
		Logger:        logger.NewNoop(),
	})

	msg := newTestMessage()
	result := sender.Send(context.Background(), msg)

	if !result.IsSuccess() {
		t.Errorf("Expected success, got %v", result.Type)
	}

	// Verify auth was NOT called (token was cached)
	if authCalls != 0 {
		t.Errorf("Expected 0 auth calls (cached token), got %d", authCalls)
	}
}

func TestSafaricomSDPSender_Send_TokenExpired(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "expired-token")

	sendCalls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(SDPAuthResponse{Token: "new-token"})
		case "/send":
			sendCalls++
			if sendCalls == 1 {
				// First call returns 401 (token expired)
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte("Token expired"))
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"status":"success"}`))
			}
		}
	}))
	defer server.Close()

	sender := NewSafaricomSDPSender(&SDPConfig{
		AuthURL:       server.URL + "/auth",
		SendURL:       server.URL + "/send",
		CountryPrefix: "KEN",
		Username:      "testuser",
		Password:      "testpass",
		DLRURL:        "http://dlr.example.com",
		TokenKey:      "test_token_key",
		TokenTTL:      25 * time.Minute,
		TokenCache:    tokenCache,
		HTTPClient:    httpclient.New(httpclient.DefaultConfig()),
		Logger:        logger.NewNoop(),
	})

	msg := newTestMessage()
	result := sender.Send(context.Background(), msg)

	// 401 should be retryable (token needs refresh)
	if !result.IsRetryable() {
		t.Errorf("Expected retryable for token expired, got %v", result.Type)
	}

	// Verify token was deleted from cache
	if _, found := tokenCache.GetStoredToken("test_token_key"); found {
		t.Error("Expired token should be deleted from cache")
	}
}

func TestSafaricomSDPSender_Send_ServerError(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/send":
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Internal Server Error"))
		}
	}))
	defer server.Close()

	sender := NewSafaricomSDPSender(&SDPConfig{
		AuthURL:    server.URL + "/auth",
		SendURL:    server.URL + "/send",
		Username:   "testuser",
		Password:   "testpass",
		DLRURL:     "http://dlr.example.com",
		TokenKey:   "test_token_key",
		TokenTTL:   25 * time.Minute,
		TokenCache: tokenCache,
		HTTPClient: httpclient.New(httpclient.DefaultConfig()),
		Logger:     logger.NewNoop(),
	})

	msg := newTestMessage()
	result := sender.Send(context.Background(), msg)

	// 5xx should be retryable
	if !result.IsRetryable() {
		t.Errorf("Expected retryable for 500, got %v", result.Type)
	}
}

func TestSafaricomSDPSender_Send_BadRequest(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/send":
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Invalid MSISDN format"))
		}
	}))
	defer server.Close()

	sender := NewSafaricomSDPSender(&SDPConfig{
		AuthURL:    server.URL + "/auth",
		SendURL:    server.URL + "/send",
		Username:   "testuser",
		Password:   "testpass",
		DLRURL:     "http://dlr.example.com",
		TokenKey:   "test_token_key",
		TokenTTL:   25 * time.Minute,
		TokenCache: tokenCache,
		HTTPClient: httpclient.New(httpclient.DefaultConfig()),
		Logger:     logger.NewNoop(),
	})

	msg := newTestMessage()
	result := sender.Send(context.Background(), msg)

	// 400 should be permanent
	if !result.IsPermanent() {
		t.Errorf("Expected permanent for 400, got %v", result.Type)
	}
}

func TestSafaricomSDPSender_Network(t *testing.T) {
	sender := &SafaricomSDPSender{}
	if sender.Network() != domain.NetworkSafaricom {
		t.Errorf("Expected SAFARICOM, got %s", sender.Network())
	}
}

func TestSafaricomSDPSender_Name(t *testing.T) {
	sender := &SafaricomSDPSender{}
	if sender.Name() != "Safaricom SDP" {
		t.Errorf("Expected 'Safaricom SDP', got %s", sender.Name())
	}
}

func TestSafaricomSDPSender_IsHealthy_NoCircuitBreaker(t *testing.T) {
	sender := &SafaricomSDPSender{circuitBreaker: nil}
	if !sender.IsHealthy() {
		t.Error("Sender without circuit breaker should be healthy")
	}
}

// newSDPTestMessages returns n promotional Safaricom messages for batch tests.
func newSDPTestMessages(n int) []*domain.Message {
	msgs := make([]*domain.Message, n)
	for i := range msgs {
		msgs[i] = &domain.Message{
			Correlator: fmt.Sprintf("test-sdp-%d", i),
			Content:    fmt.Sprintf("Batch message %d", i),
			MSISDN:     "254722123456",
			NetworkRaw: "SAFARICOM",
			Sender:     "TestSender",
			PackageID:  "PROMOTIONAL",
		}
	}
	return msgs
}

// newSDPSenderForTest creates a SafaricomSDPSender pointing at the given test server.
func newSDPSenderForTest(serverURL string, tokenCache *MockTokenCache, batchSize int) *SafaricomSDPSender {
	return NewSafaricomSDPSender(&SDPConfig{
		AuthURL:       serverURL + "/auth",
		SendURL:       serverURL + "/send",
		CountryPrefix: "KEN",
		Username:      "testuser",
		Password:      "testpass",
		DLRURL:        "http://dlr.example.com",
		TokenKey:      "test_token_key",
		TokenTTL:      25 * time.Minute,
		BatchSize:     batchSize,
		TokenCache:    tokenCache,
		HTTPClient:    httpclient.New(httpclient.DefaultConfig()),
		Logger:        logger.NewNoop(),
	})
}

func TestSendBatch_Success(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	var capturedRecords int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req SDPSendRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request: %v", err)
		}
		capturedRecords = len(req.DataSet)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()

	sender := newSDPSenderForTest(server.URL, tokenCache, 4)
	msgs := newSDPTestMessages(4)

	results := sender.SendBatch(context.Background(), msgs)

	if len(results) != 4 {
		t.Fatalf("Expected 4 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.IsSuccess() {
			t.Errorf("result[%d] expected success, got %v", i, r.Type)
		}
	}
	if capturedRecords != 4 {
		t.Errorf("Expected 4 records in DataSet, got %d", capturedRecords)
	}
}

func TestSendBatch_EmptySlice(t *testing.T) {
	tokenCache := NewMockTokenCache()
	sender := newSDPSenderForTest("http://unused", tokenCache, 4)

	results := sender.SendBatch(context.Background(), nil)
	if results != nil {
		t.Errorf("Expected nil for empty slice, got %v", results)
	}
}

func TestSendBatch_GatewayError_AllRetryable(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("Bad Gateway"))
	}))
	defer server.Close()

	sender := newSDPSenderForTest(server.URL, tokenCache, 3)
	msgs := newSDPTestMessages(3)

	results := sender.SendBatch(context.Background(), msgs)

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.IsRetryable() {
			t.Errorf("result[%d] expected retryable for 502, got %v", i, r.Type)
		}
	}
}

func TestSendBatch_PermanentError_AllPermanent(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request"))
	}))
	defer server.Close()

	sender := newSDPSenderForTest(server.URL, tokenCache, 2)
	msgs := newSDPTestMessages(2)

	results := sender.SendBatch(context.Background(), msgs)

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.IsPermanent() {
			t.Errorf("result[%d] expected permanent for 400, got %v", i, r.Type)
		}
	}
}

func TestSendBatch_Unauthorized_TokenCleared(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "expired-token")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("Unauthorized"))
	}))
	defer server.Close()

	sender := newSDPSenderForTest(server.URL, tokenCache, 3)
	msgs := newSDPTestMessages(3)

	results := sender.SendBatch(context.Background(), msgs)

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.IsRetryable() {
			t.Errorf("result[%d] expected retryable for 401, got %v", i, r.Type)
		}
	}
	if _, found := tokenCache.GetStoredToken("test_token_key"); found {
		t.Error("Token should be cleared from cache after 401")
	}
}

func TestSendBatch_DataSetContainsAllMessages(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	var capturedDataSet []SDPSMSRecord
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req SDPSendRequest
		json.NewDecoder(r.Body).Decode(&req)
		capturedDataSet = req.DataSet
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := newSDPSenderForTest(server.URL, tokenCache, 5)
	msgs := newSDPTestMessages(5)
	for i, m := range msgs {
		m.MSISDN = fmt.Sprintf("25472200000%d", i)
	}

	sender.SendBatch(context.Background(), msgs)

	if len(capturedDataSet) != 5 {
		t.Fatalf("Expected 5 records in DataSet, got %d", len(capturedDataSet))
	}
	for i, record := range capturedDataSet {
		expected := fmt.Sprintf("25472200000%d", i)
		if record.MSISDN != expected {
			t.Errorf("record[%d] MSISDN: expected %s, got %s", i, expected, record.MSISDN)
		}
		if record.UniqueID != fmt.Sprintf("test-sdp-%d", i) {
			t.Errorf("record[%d] UniqueID: expected test-sdp-%d, got %s", i, i, record.UniqueID)
		}
	}
}

func TestSendBatch_BatchSizeAccessor(t *testing.T) {
	tokenCache := NewMockTokenCache()
	sender := newSDPSenderForTest("http://unused", tokenCache, 10)
	if sender.BatchSize() != 10 {
		t.Errorf("Expected BatchSize 10, got %d", sender.BatchSize())
	}
}

func TestSendBatch_DefaultBatchSizeIsOne(t *testing.T) {
	tokenCache := NewMockTokenCache()
	// BatchSize 0 should clamp to 1
	sender := newSDPSenderForTest("http://unused", tokenCache, 0)
	if sender.BatchSize() != 1 {
		t.Errorf("Expected BatchSize 1 for zero input, got %d", sender.BatchSize())
	}
}

// TestSendBatch_MixedSenders_AllPermanent verifies that SendBatch rejects batches
// where messages have different Sender (oa) values, returning permanent errors for
// all messages without making an HTTP call.
func TestSendBatch_MixedSenders_AllPermanent(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	httpCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := newSDPSenderForTest(server.URL, tokenCache, 4)

	msgs := []*domain.Message{
		{Correlator: "m1", Content: "msg", MSISDN: "254722000001", NetworkRaw: "SAFARICOM", Sender: "SenderA"},
		{Correlator: "m2", Content: "msg", MSISDN: "254722000002", NetworkRaw: "SAFARICOM", Sender: "SenderB"},
		{Correlator: "m3", Content: "msg", MSISDN: "254722000003", NetworkRaw: "SAFARICOM", Sender: "SenderA"},
	}

	results := sender.SendBatch(context.Background(), msgs)

	if httpCalled {
		t.Error("HTTP request must not be made when senders are mixed")
	}
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.IsPermanent() {
			t.Errorf("result[%d] expected permanent for mixed-sender batch, got %v", i, r.Type)
		}
	}
}

// TestSendBatch_SameSender_Accepted verifies that a batch where all messages share
// the same Sender is accepted and succeeds normally.
func TestSendBatch_SameSender_Accepted(t *testing.T) {
	tokenCache := NewMockTokenCache()
	tokenCache.SetToken("test_token_key", "valid-token")

	var capturedOAs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req SDPSendRequest
		json.NewDecoder(r.Body).Decode(&req)
		for _, rec := range req.DataSet {
			capturedOAs = append(capturedOAs, rec.OA)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()

	sender := newSDPSenderForTest(server.URL, tokenCache, 4)
	msgs := newSDPTestMessages(3) // all use Sender: "TestSender"

	results := sender.SendBatch(context.Background(), msgs)

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.IsSuccess() {
			t.Errorf("result[%d] expected success, got %v", i, r.Type)
		}
	}
	for i, oa := range capturedOAs {
		if oa != "TestSender" {
			t.Errorf("DataSet[%d].OA = %q, want %q", i, oa, "TestSender")
		}
	}
}
