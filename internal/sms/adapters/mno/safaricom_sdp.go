package mno

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// SafaricomSDPSender sends SMS via Safaricom SDP (Service Delivery Platform)
// Used for non-transactional bulk SMS
type SafaricomSDPSender struct {
	authURL        string
	sendURL        string
	authUsername   string
	countryPrefix  string
	username       string
	password       string
	dlrURL         string
	dlrURLApiV2    string
	tokenKey       string
	tokenTTL       time.Duration
	tokenCache     ports.TokenCache
	httpClient     *httpclient.Client
	circuitBreaker *circuitbreaker.CircuitBreaker
	metrics        ports.Metrics
	log            logger.Logger
}

// SDPConfig holds configuration for Safaricom SDP sender
type SDPConfig struct {
	AuthURL        string
	SendURL        string
	AuthUsername   string
	CountryPrefix  string
	Username       string
	Password       string
	DLRURL         string
	DLRURLApiV2    string
	TokenKey       string
	TokenTTL       time.Duration
	TokenCache     ports.TokenCache
	HTTPClient     *httpclient.Client
	CircuitBreaker *circuitbreaker.CircuitBreaker
	Metrics        ports.Metrics
	Logger         logger.Logger
}

// SDPAuthRequest is the authentication request payload
type SDPAuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// SDPAuthResponse is the authentication response
type SDPAuthResponse struct {
	Token     string `json:"token"`
	ExpiresIn int64  `json:"expiresIn"`
}

// SDPSendRequest is the SMS send request payload
type SDPSendRequest struct {
	TimeStamp int64          `json:"timeStamp"`
	DataSet   []SDPSMSRecord `json:"dataSet"`
}

// SDPSMSRecord represents a single SMS in the SDP request
type SDPSMSRecord struct {
	UserName          string `json:"userName"`
	Channel           string `json:"channel"`
	PackageID         uint64 `json:"package_id"`
	OA                string `json:"oa"`
	MSISDN            string `json:"msisdn"`
	Message           string `json:"message"`
	UniqueID          string `json:"uniqueId"`
	ActionResponseURL string `json:"actionResponseURL"`
}

// NewSafaricomSDPSender creates a new Safaricom SDP sender
func NewSafaricomSDPSender(cfg *SDPConfig) *SafaricomSDPSender {
	authUsername := cfg.AuthUsername
	if authUsername == "" {
		authUsername = cfg.Username
	}

	return &SafaricomSDPSender{
		authURL:        cfg.AuthURL,
		sendURL:        cfg.SendURL,
		authUsername:   authUsername,
		countryPrefix:  cfg.CountryPrefix,
		username:       cfg.Username,
		password:       cfg.Password,
		dlrURL:         cfg.DLRURL,
		dlrURLApiV2:    cfg.DLRURLApiV2,
		tokenKey:       cfg.TokenKey,
		tokenTTL:       cfg.TokenTTL,
		tokenCache:     cfg.TokenCache,
		httpClient:     cfg.HTTPClient,
		circuitBreaker: cfg.CircuitBreaker,
		metrics:        cfg.Metrics,
		log:            cfg.Logger,
	}
}

// Send sends a message via Safaricom SDP
func (s *SafaricomSDPSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	start := time.Now()

	// Check circuit breaker
	if s.circuitBreaker != nil && s.circuitBreaker.IsOpen() {
		s.log.WithField("network", domain.NetworkSafaricom).Warn("Circuit breaker is open")
		return domain.NewPermanentResult(msg, domain.ErrCircuitOpen, time.Since(start))
	}

	s.log.WithFields(map[string]interface{}{
		"network":    domain.NetworkSafaricom,
		"correlator": msg.Correlator,
		"msisdn":     msg.NormalizeMSISDN(),
	}).Debug("Sending message via SDP")

	// Execute through circuit breaker
	var result *domain.SendResult
	if s.circuitBreaker != nil {
		_, err := s.circuitBreaker.Execute(func() (interface{}, error) {
			result = s.executeSend(ctx, msg, start)
			if !result.IsSuccess() {
				return nil, result.Error
			}
			return result, nil
		})
		if err != nil && result == nil {
			result = domain.NewRetryableResult(msg, err, time.Since(start))
		}
	} else {
		result = s.executeSend(ctx, msg, start)
	}

	// Record metrics
	if s.metrics != nil {
		s.metrics.ObserveSendLatency(domain.NetworkSafaricom, result.Latency)
		status := "success"
		if !result.IsSuccess() {
			status = "failed"
		}
		s.metrics.IncMessagesProcessed(domain.NetworkSafaricom, status)
	}

	return result
}

// executeSend performs the actual send operation
func (s *SafaricomSDPSender) executeSend(ctx context.Context, msg *domain.Message, start time.Time) *domain.SendResult {
	// Get authentication token
	token, err := s.getToken(ctx)
	if err != nil {
		s.log.WithError(err).Error("Failed to get SDP token")
		return domain.NewRetryableResult(msg, domain.ErrTokenFetchFail, time.Since(start))
	}

	// Build request payload
	packageID := uint64(0)

	payload := SDPSendRequest{
		TimeStamp: time.Now().Unix(),
		DataSet: []SDPSMSRecord{
			{
				UserName:          s.username,
				Channel:           "sms",
				PackageID:         packageID,
				OA:                msg.Sender,
				MSISDN:            msg.NormalizeMSISDN(),
				Message:           msg.Content,
				UniqueID:          msg.Correlator,
				ActionResponseURL: s.resolveDLRURL(msg),
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		s.log.WithError(err).Error("Failed to marshal SDP payload")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	s.log.WithFields(map[string]interface{}{
		"correlator": msg.Correlator,
		"url":        s.sendURL,
		"payload":    string(payloadBytes),
	}).Debug("SDP send request payload")

	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.sendURL, bytes.NewReader(payloadBytes))
	if err != nil {
		s.log.WithError(err).Error("Failed to create SDP request")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Authorization", "Bearer "+token)
	if s.countryPrefix != "" {
		req.Header.Set("X-Country", s.countryPrefix)
	}

	s.log.WithFields(map[string]interface{}{
		"correlator":   msg.Correlator,
		"content_type": req.Header.Get("Content-Type"),
		"auth_scheme":  "Bearer",
		"x_country":    req.Header.Get("X-Country"),
	}).Debug("SDP send request headers")

	// Execute request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.log.WithError(err).Error("SDP request failed")
		return domain.NewRetryableResult(msg, domain.ErrMNOUnavailable, time.Since(start))
	}
	defer httpclient.DrainBody(resp) // EM-139 fix

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.log.WithError(err).Error("Failed to read SDP response")
		return domain.NewRetryableResult(msg, err, time.Since(start))
	}

	responseStr := string(body)
	latency := time.Since(start)

	// Check response status
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.log.WithFields(map[string]interface{}{
			"network":     domain.NetworkSafaricom,
			"correlator":  msg.Correlator,
			"status_code": resp.StatusCode,
			"response":    responseStr,
			"latency_ms":  latency.Milliseconds(),
		}).Info("Message sent successfully via SDP")
		return domain.NewSuccessResult(msg, responseStr, latency)
	}

	// Handle error response
	mnoErr := domain.NewMNOError(domain.NetworkSafaricom, resp.StatusCode, responseStr, nil)

	// Check if token expired (401)
	if resp.StatusCode == http.StatusUnauthorized {
		s.log.Warn("SDP token expired, clearing cache")
		if err := s.tokenCache.Delete(ctx, s.tokenKey); err != nil {
			s.log.WithError(err).Warn("Failed to delete expired token from cache")
		}
		return domain.NewRetryableResult(msg, domain.ErrTokenExpired, latency)
	}

	if mnoErr.IsRetryable() {
		return domain.NewRetryableResult(msg, mnoErr, latency)
	}

	s.log.WithFields(map[string]interface{}{
		"network":     domain.NetworkSafaricom,
		"correlator":  msg.Correlator,
		"status_code": resp.StatusCode,
		"response":    responseStr,
	}).Error("Message rejected by SDP")

	return domain.NewPermanentResult(msg, mnoErr, latency)
}

// resolveDLRURL selects the correct DLR URL based on the message source queue.
// Messages from the gateway queue use the primary DLR URL; all others use the API v2 URL.
func (s *SafaricomSDPSender) resolveDLRURL(msg *domain.Message) string {
	if s.dlrURLApiV2 != "" && msg.SourceQueue != "" && msg.SourceQueue != gatewayQueueName {
		s.log.WithFields(map[string]interface{}{
			"correlator":   msg.Correlator,
			"source_queue": msg.SourceQueue,
			"dlr_url":      s.dlrURLApiV2,
		}).Debug("SDP using API v2 DLR URL")
		return s.dlrURLApiV2
	}
	s.log.WithFields(map[string]interface{}{
		"correlator":   msg.Correlator,
		"source_queue": msg.SourceQueue,
		"dlr_url":      s.dlrURL,
	}).Debug("SDP using primary DLR URL")
	return s.dlrURL
}

// getToken retrieves the authentication token, using cache when possible
func (s *SafaricomSDPSender) getToken(ctx context.Context) (string, error) {
	// Try to get from cache - EM-145 fix: properly handle cache errors
	token, found, err := s.tokenCache.Get(ctx, s.tokenKey)
	if err != nil {
		s.log.WithError(err).Warn("Failed to get token from cache, fetching new token")
	}
	if found && token != "" {
		return token, nil
	}

	// Fetch new token with retry
	var lastErr error
	for i := 0; i < 10; i++ {
		token, err = s.fetchToken(ctx)
		if err == nil {
			// Cache the token
			if cacheErr := s.tokenCache.Set(ctx, s.tokenKey, token, s.tokenTTL); cacheErr != nil {
				s.log.WithError(cacheErr).Warn("Failed to cache SDP token")
			}
			return token, nil
		}
		lastErr = err
		s.log.WithError(err).WithField("attempt", i+1).Warn("Token fetch failed, retrying")
		time.Sleep(time.Duration(i+1) * time.Second) // Linear backoff
	}

	return "", fmt.Errorf("failed to fetch token after 10 attempts: %w", lastErr)
}

// fetchToken fetches a new authentication token from SDP
func (s *SafaricomSDPSender) fetchToken(ctx context.Context) (string, error) {
	authReq := SDPAuthRequest{
		Username: s.authUsername,
		Password: s.password,
	}

	payload, err := json.Marshal(authReq)
	if err != nil {
		return "", fmt.Errorf("failed to marshal auth request: %w", err)
	}

	s.log.WithFields(map[string]interface{}{
		"url":          s.authURL,
		"username":     s.username,
		"password_set": s.password != "",
		"payload":      string(payload),
	}).Debug("SDP auth request details")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.authURL, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("failed to create auth request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	s.log.WithFields(map[string]interface{}{
		"Content-Type":     req.Header.Get("Content-Type"),
		"X-Requested-With": req.Header.Get("X-Requested-With"),
	}).Debug("SDP auth request headers")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("auth request failed: %w", err)
	}
	defer httpclient.DrainBody(resp)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("auth failed with status %d: %s", resp.StatusCode, string(body))
	}

	var authResp SDPAuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return "", fmt.Errorf("failed to decode auth response: %w", err)
	}

	if authResp.Token == "" {
		return "", fmt.Errorf("received empty token from SDP")
	}

	return authResp.Token, nil
}

// Network returns the network this sender handles
func (s *SafaricomSDPSender) Network() domain.Network {
	return domain.NetworkSafaricom
}

// Name returns the name of this sender
func (s *SafaricomSDPSender) Name() string {
	return "Safaricom SDP"
}

// IsHealthy returns true if the sender is healthy
func (s *SafaricomSDPSender) IsHealthy() bool {
	if s.circuitBreaker == nil {
		return true
	}
	return !s.circuitBreaker.IsOpen()
}
