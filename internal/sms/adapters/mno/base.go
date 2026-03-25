package mno

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// gatewayQueueName is the input queue that uses primary DLR URLs.
// Messages from any other queue use DLRURLApiV2. Set via SetGatewayQueueName.
var gatewayQueueName = "SMS_MNO_GATEWAY_QUEUE"

// SetGatewayQueueName configures the queue name that maps to primary DLR URLs.
// Call this at app startup with the value from config.
func SetGatewayQueueName(name string) {
	if name != "" {
		gatewayQueueName = name
	}
}

// BaseSMPPSender provides common SMPP (Kannel) gateway functionality
// Used by Airtel, Telkom, Equitel, CM, and Safaricom transactional
type BaseSMPPSender struct {
	network        domain.Network
	name           string
	baseURL        string
	smsc           string
	username       string
	password       string
	dlrURL         string
	dlrURLApiV2    string
	httpClient     *httpclient.Client
	circuitBreaker *circuitbreaker.CircuitBreaker
	metrics        ports.Metrics
	log            logger.Logger
}

// SMPPConfig holds configuration for SMPP senders
type SMPPConfig struct {
	Network        domain.Network
	Name           string
	BaseURL        string
	SMSC           string
	Username       string
	Password       string
	DLRURL         string
	DLRURLApiV2    string
	HTTPClient     *httpclient.Client
	CircuitBreaker *circuitbreaker.CircuitBreaker
	Metrics        ports.Metrics
	Logger         logger.Logger
}

// NewBaseSMPPSender creates a new base SMPP sender
func NewBaseSMPPSender(cfg *SMPPConfig) *BaseSMPPSender {
	return &BaseSMPPSender{
		network:        cfg.Network,
		name:           cfg.Name,
		baseURL:        cfg.BaseURL,
		smsc:           cfg.SMSC,
		username:       cfg.Username,
		password:       cfg.Password,
		dlrURL:         cfg.DLRURL,
		dlrURLApiV2:    cfg.DLRURLApiV2,
		httpClient:     cfg.HTTPClient,
		circuitBreaker: cfg.CircuitBreaker,
		metrics:        cfg.Metrics,
		log:            cfg.Logger,
	}
}

// Send sends a message via SMPP gateway (Kannel)
func (s *BaseSMPPSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	start := time.Now()

	// Check circuit breaker
	if s.circuitBreaker != nil && s.circuitBreaker.IsOpen() {
		s.log.WithField("network", s.network).Warn("Circuit breaker is open")
		return domain.NewPermanentResult(msg, domain.ErrCircuitOpen, time.Since(start))
	}

	// Build request URL
	requestURL := s.buildRequestURL(msg)

	s.log.WithFields(map[string]interface{}{
		"network":    s.network,
		"correlator": msg.Correlator,
		"msisdn":     msg.NormalizeMSISDN(),
	}).Debug("Sending message via SMPP")

	// Execute request through circuit breaker
	var result *domain.SendResult
	if s.circuitBreaker != nil {
		_, err := s.circuitBreaker.Execute(func() (interface{}, error) {
			result = s.executeRequest(ctx, msg, requestURL, start)
			if !result.IsSuccess() {
				return nil, result.Error
			}
			return result, nil
		})
		if err != nil && result == nil {
			result = domain.NewRetryableResult(msg, err, time.Since(start))
		}
	} else {
		result = s.executeRequest(ctx, msg, requestURL, start)
	}

	// Record metrics
	if s.metrics != nil {
		s.metrics.ObserveSendLatency(s.network, result.Latency)
		status := "success"
		if !result.IsSuccess() {
			status = "failed"
		}
		s.metrics.IncMessagesProcessed(s.network, status)
	}

	return result
}

// executeRequest performs the HTTP request
func (s *BaseSMPPSender) executeRequest(ctx context.Context, msg *domain.Message, requestURL string, start time.Time) *domain.SendResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		s.log.WithError(err).Error("Failed to create request")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.log.WithError(err).Error("Request failed")
		return domain.NewRetryableResult(msg, domain.ErrMNOUnavailable, time.Since(start))
	}
	defer httpclient.DrainBody(resp) // EM-139 fix: proper body handling

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.log.WithError(err).Error("Failed to read response body")
		return domain.NewRetryableResult(msg, err, time.Since(start))
	}

	responseStr := string(body)
	latency := time.Since(start)

	// Check response
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// SMPP success response contains "0: Accepted for delivery"
		if strings.Contains(responseStr, "0: Accepted for delivery") {
			s.log.WithFields(map[string]interface{}{
				"network":    s.network,
				"correlator": msg.Correlator,
				"latency_ms": latency.Milliseconds(),
			}).Info("Message sent successfully")
			return domain.NewSuccessResult(msg, responseStr, latency)
		}
	}

	// Check for specific error responses
	mnoErr := domain.NewMNOError(s.network, resp.StatusCode, responseStr, nil)
	if mnoErr.IsRetryable() {
		return domain.NewRetryableResult(msg, mnoErr, latency)
	}

	s.log.WithFields(map[string]interface{}{
		"network":     s.network,
		"correlator":  msg.Correlator,
		"status_code": resp.StatusCode,
		"response":    responseStr,
	}).Error("Message rejected by MNO")

	return domain.NewPermanentResult(msg, mnoErr, latency)
}

// buildRequestURL constructs the Kannel SMPP gateway URL
func (s *BaseSMPPSender) buildRequestURL(msg *domain.Message) string {
	// Build DLR callback URL with message tracking parameters
	dlrURL := s.buildDLRURL(msg)

	// Build query parameters
	params := url.Values{}
	params.Set("smsc", s.smsc)
	params.Set("username", s.username)
	params.Set("password", s.password)
	params.Set("to", msg.NormalizeMSISDN())
	params.Set("from", msg.Sender)
	params.Set("text", msg.Content)
	params.Set("dlr-mask", "31") // Request all DLR types
	params.Set("dlr-url", dlrURL)

	return fmt.Sprintf("%s?%s", s.baseURL, params.Encode())
}

// buildDLRURL constructs the DLR callback URL with tracking parameters.
// Selects between the primary DLR URL (for GATEWAY_QUEUE_NAME) and the
// API v2 DLR URL (for all other input queues).
//
// Kannel placeholders (%d, %A, %P, %p, %t, %n, %b) must NOT be URL-encoded —
// url.Values.Encode() would turn %d into %25d, breaking Kannel's substitution.
// Dynamic values (correlator, username, password) are encoded via url.QueryEscape.
func (s *BaseSMPPSender) buildDLRURL(msg *domain.Message) string {
	baseURL := s.dlrURL
	if s.dlrURLApiV2 != "" && msg.SourceQueue != "" && msg.SourceQueue != gatewayQueueName {
		baseURL = s.dlrURLApiV2
		s.log.WithFields(map[string]interface{}{
			"correlator":   msg.Correlator,
			"network":      s.network,
			"source_queue": msg.SourceQueue,
			"dlr_url":      baseURL,
		}).Debug("SMPP using API v2 DLR URL")
	} else {
		s.log.WithFields(map[string]interface{}{
			"correlator":   msg.Correlator,
			"network":      s.network,
			"source_queue": msg.SourceQueue,
			"dlr_url":      baseURL,
		}).Debug("SMPP using primary DLR URL")
	}

	// Kannel placeholders: %d=status, %A=answer, %P=sender, %p=receiver, %t=time, %n=user ref, %b=message
	// Note: "reciever" typo is intentional (matching existing system)
	return fmt.Sprintf(
		"%s?correlator=%s&user=%s&passwd=%s&status=%%d&answer=%%A&sender=%%P&reciever=%%p&time=%%t&usr=%%n&message=%%b",
		baseURL,
		url.QueryEscape(msg.Correlator),
		url.QueryEscape(s.username),
		url.QueryEscape(s.password),
	)
}

// Network returns the network this sender handles
func (s *BaseSMPPSender) Network() domain.Network {
	return s.network
}

// Name returns the name of this sender
func (s *BaseSMPPSender) Name() string {
	return s.name
}

// IsHealthy returns true if the sender is healthy
func (s *BaseSMPPSender) IsHealthy() bool {
	if s.circuitBreaker == nil {
		return true
	}
	return !s.circuitBreaker.IsOpen()
}
