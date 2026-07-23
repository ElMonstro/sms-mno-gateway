package mno

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// AfricasTalking SMS status codes returned in Recipients[].statusCode.
// Reference: https://help.africastalking.com/en/articles/742491-why-did-my-messages-fail
const (
	atStatusProcessed = 100 // success
	atStatusSent      = 101 // success
	atStatusQueued    = 102 // success — accepted, queued at carrier

	atStatusRiskHold              = 401 // permanent — account flagged for risk review
	atStatusInvalidSenderID       = 402 // permanent — sender ID not registered/mapped
	atStatusInvalidPhoneNumber    = 403 // permanent — malformed number
	atStatusUnsupportedNumberType = 404 // permanent — number type not supported
	atStatusInsufficientBalance   = 405 // account-level, not per-message — see tripBalanceCooldown
	atStatusUserInBlacklist       = 406 // permanent
	atStatusCouldNotRoute         = 407 // permanent — no route to destination

	atStatusInternalServerError = 500 // transient — AT-side error, worth a later retry
	atStatusGatewayError        = 501 // transient — AT-side error

	// atStatusRejectedByGateway sits in the 5xx range but AT's own docs describe it
	// specifically as "senderId/shortcode not mapped to your account" — a permanent
	// config issue, not a transient gateway error.
	atStatusRejectedByGateway = 502
)

// classifyATRejection maps a non-success AfricasTalking Recipients[].statusCode to a
// ResultType. Only 500/501 are genuinely transient AT-side errors worth a later
// retry — every other documented code is permanent for this specific message (even
// InsufficientBalance, which is account-level: tripBalanceCooldown handles backing
// the whole sender off separately, but this attempt itself is still done).
func classifyATRejection(statusCode int) domain.ResultType {
	switch statusCode {
	case atStatusInternalServerError, atStatusGatewayError:
		return domain.ResultRetryable
	case atStatusRiskHold, atStatusInvalidSenderID, atStatusInvalidPhoneNumber,
		atStatusUnsupportedNumberType, atStatusInsufficientBalance,
		atStatusUserInBlacklist, atStatusCouldNotRoute, atStatusRejectedByGateway:
		return domain.ResultPermanent
	default:
		// Unknown/undocumented code — treat conservatively as permanent rather than
		// risk an unbounded retry loop over a code we don't understand.
		return domain.ResultPermanent
	}
}

// AfricasTalkingSender sends international SMS via the AfricasTalking REST API.
// Unlike the Kenyan MNO adapters, its result is not published to internal RabbitMQ
// queues — it's reported back to the PHP API by a separate ports.ResultReporter,
// wired up by service.AfricasTalkingProcessor rather than the standard Processor.
type AfricasTalkingSender struct {
	sandboxURL     string
	productionURL  string
	mode           string // "sandbox" or "production"
	apiKey         string
	apiKeyProd     string
	username       string
	httpClient     *httpclient.Client
	circuitBreaker *circuitbreaker.CircuitBreaker
	metrics        ports.Metrics
	log            logger.Logger

	// balanceCooldown gates sends after an InsufficientBalance response — an
	// account-level condition where retrying immediately (or per-message) never
	// helps. See tripBalanceCooldown.
	balanceCooldown      time.Duration
	mu                   sync.Mutex
	balanceDepletedUntil time.Time
}

// AfricasTalkingConfig holds configuration for the AfricasTalking sender.
type AfricasTalkingConfig struct {
	SandboxURL     string
	ProductionURL  string
	Mode           string
	APIKey         string
	APIKeyProd     string
	Username       string
	HTTPClient     *httpclient.Client
	CircuitBreaker *circuitbreaker.CircuitBreaker
	Metrics        ports.Metrics
	Logger         logger.Logger

	// BalanceCooldown is how long the sender stops attempting sends after seeing
	// InsufficientBalance. Defaults to 5 minutes if unset.
	BalanceCooldown time.Duration
}

// NewAfricasTalkingSender creates a new AfricasTalking sender.
func NewAfricasTalkingSender(cfg *AfricasTalkingConfig) *AfricasTalkingSender {
	cooldown := cfg.BalanceCooldown
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	return &AfricasTalkingSender{
		sandboxURL:      cfg.SandboxURL,
		productionURL:   cfg.ProductionURL,
		mode:            cfg.Mode,
		apiKey:          cfg.APIKey,
		apiKeyProd:      cfg.APIKeyProd,
		username:        cfg.Username,
		httpClient:      cfg.HTTPClient,
		circuitBreaker:  cfg.CircuitBreaker,
		metrics:         cfg.Metrics,
		log:             cfg.Logger,
		balanceCooldown: cooldown,
	}
}

// balanceCooldownRemaining returns how long is left on the balance-depleted
// cooldown, or zero if it isn't active.
func (s *AfricasTalkingSender) balanceCooldownRemaining() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.balanceDepletedUntil.IsZero() {
		return 0
	}
	if remaining := time.Until(s.balanceDepletedUntil); remaining > 0 {
		return remaining
	}
	return 0
}

// tripBalanceCooldown stops further sends for balanceCooldown. Called after
// AfricasTalking reports InsufficientBalance, so the sender stops making calls it
// already knows will fail until the account is expected to have been topped up.
func (s *AfricasTalkingSender) tripBalanceCooldown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.balanceDepletedUntil = time.Now().Add(s.balanceCooldown)
}

// atRecipient is a single recipient entry in the AfricasTalking send response.
type atRecipient struct {
	StatusCode int    `json:"statusCode"`
	Number     string `json:"number"`
	Status     string `json:"status"`
	Cost       string `json:"cost"`
	MessageID  string `json:"messageId"`
}

// atSendResponse is the AfricasTalking SMS send API response shape.
type atSendResponse struct {
	SMSMessageData struct {
		Message    string        `json:"Message"`
		Recipients []atRecipient `json:"Recipients"`
	} `json:"SMSMessageData"`
}

// baseURLAndKey resolves the send URL and API key for the configured mode.
func (s *AfricasTalkingSender) baseURLAndKey() (string, string) {
	if strings.EqualFold(s.mode, "production") {
		return s.productionURL, s.apiKeyProd
	}
	return s.sandboxURL, s.apiKey
}

// Send sends a message via the AfricasTalking API.
func (s *AfricasTalkingSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	start := time.Now()

	if remaining := s.balanceCooldownRemaining(); remaining > 0 {
		s.log.WithFields(map[string]interface{}{
			"correlator":    msg.Correlator,
			"cooldown_left": remaining.String(),
		}).Warn("AfricasTalking balance depleted, skipping send until cooldown expires")
		return domain.NewPermanentResult(msg, domain.ErrInsufficientBalance, time.Since(start))
	}

	if s.circuitBreaker != nil && s.circuitBreaker.IsOpen() {
		s.log.WithField("network", domain.NetworkINTNL).Warn("Circuit breaker is open")
		return domain.NewPermanentResult(msg, domain.ErrCircuitOpen, time.Since(start))
	}

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

	if s.metrics != nil {
		s.metrics.ObserveSendLatency(domain.NetworkINTNL, result.Latency)
		status := "success"
		if !result.IsSuccess() {
			status = "failed"
		}
		s.metrics.IncMessagesProcessed(domain.NetworkINTNL, status)
	}

	return result
}

// executeSend performs the actual AfricasTalking API call.
func (s *AfricasTalkingSender) executeSend(ctx context.Context, msg *domain.Message, start time.Time) *domain.SendResult {
	sendURL, apiKey := s.baseURLAndKey()

	// AfricasTalking MSISDNs arrive as digits-only, already in full international
	// format (no leading 0) — unlike the Kenyan MNOs, NormalizeMSISDN() does not apply.
	to := "+" + strings.TrimPrefix(strings.TrimSpace(msg.MSISDN), "+")

	form := url.Values{}
	form.Set("username", s.username)
	form.Set("to", to)
	form.Set("message", msg.Content)
	form.Set("from", msg.Sender)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sendURL, strings.NewReader(form.Encode()))
	if err != nil {
		s.log.WithError(err).Error("Failed to create AfricasTalking request")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("apiKey", apiKey)

	s.log.WithFields(map[string]interface{}{
		"correlator": msg.Correlator,
		"msisdn":     to,
		"url":        sendURL,
	}).Info("Sending message via AfricasTalking")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		elapsed := time.Since(start)
		s.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"msisdn":     to,
			"url":        sendURL,
		}).WithError(err).Error("AfricasTalking request failed")
		return domain.NewRetryableResult(msg, domain.ErrMNOUnavailable, elapsed)
	}
	defer httpclient.DrainBody(resp)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.log.WithError(err).Error("Failed to read AfricasTalking response")
		return domain.NewRetryableResult(msg, err, time.Since(start))
	}

	latency := time.Since(start)
	responseStr := string(body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		mnoErr := domain.NewMNOError(domain.NetworkINTNL, resp.StatusCode, responseStr, nil)
		s.log.WithFields(map[string]interface{}{
			"correlator":  msg.Correlator,
			"status_code": resp.StatusCode,
			"response":    responseStr,
		}).Error("Message rejected by AfricasTalking")
		if mnoErr.IsRetryable() {
			return domain.NewRetryableResult(msg, mnoErr, latency)
		}
		return domain.NewPermanentResult(msg, mnoErr, latency)
	}

	var parsed atSendResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		s.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"response":   responseStr,
		}).WithError(err).Error("Failed to parse AfricasTalking response")
		return domain.NewRetryableResult(msg, domain.ErrMNOInvalidResponse, latency)
	}

	recipients := parsed.SMSMessageData.Recipients
	if len(recipients) == 0 {
		s.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"response":   responseStr,
		}).Error("AfricasTalking response has no recipients")
		return domain.NewPermanentResult(msg, domain.ErrMNOInvalidResponse, latency)
	}

	first := recipients[0]
	if first.Status == "Success" || first.StatusCode == atStatusSent ||
		first.StatusCode == atStatusProcessed || first.StatusCode == atStatusQueued {
		s.log.WithFields(map[string]interface{}{
			"correlator":  msg.Correlator,
			"message_id":  first.MessageID,
			"status_code": first.StatusCode,
			"latency_ms":  latency.Milliseconds(),
		}).Info("Message sent successfully via AfricasTalking")

		result := domain.NewSuccessResult(msg, responseStr, latency)
		result.ExternalMessageID = first.MessageID
		result.NetworkCode = fmt.Sprintf("%d", first.StatusCode)
		return result
	}

	s.log.WithFields(map[string]interface{}{
		"correlator":  msg.Correlator,
		"status":      first.Status,
		"status_code": first.StatusCode,
		"response":    responseStr,
	}).Error("Message rejected by AfricasTalking")

	if first.StatusCode == atStatusInsufficientBalance {
		s.tripBalanceCooldown()
	}

	rejectionErr := fmt.Errorf("%w: status=%s statusCode=%d", domain.ErrMNORejected, first.Status, first.StatusCode)

	var result *domain.SendResult
	if classifyATRejection(first.StatusCode) == domain.ResultRetryable {
		result = domain.NewRetryableResult(msg, rejectionErr, latency)
	} else {
		result = domain.NewPermanentResult(msg, rejectionErr, latency)
	}
	result.NetworkCode = fmt.Sprintf("%d", first.StatusCode)
	return result
}

// Network returns the network this sender handles.
func (s *AfricasTalkingSender) Network() domain.Network {
	return domain.NetworkINTNL
}

// Name returns the name of this sender.
func (s *AfricasTalkingSender) Name() string {
	return "AfricasTalking"
}

// IsHealthy returns true if the sender is healthy.
func (s *AfricasTalkingSender) IsHealthy() bool {
	if s.circuitBreaker == nil {
		return true
	}
	return !s.circuitBreaker.IsOpen()
}

// Ensure AfricasTalkingSender implements ports.MNOSender
var _ ports.MNOSender = (*AfricasTalkingSender)(nil)
