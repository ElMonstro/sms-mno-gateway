// Package httpapi holds adapters that call this service's own upstream PHP API,
// as opposed to the internal/sms/adapters/mno package which calls MNO/aggregator APIs.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// AfricasTalkingReporter reports AfricasTalking send outcomes back to the PHP API's
// POST /api/internal/africas-talking/send-result endpoint, so it can update the
// outbox row (externalMessageId + mq_publish_status). Implements ports.ResultReporter.
type AfricasTalkingReporter struct {
	url        string
	token      string
	httpClient *httpclient.Client
	log        logger.Logger
}

// AfricasTalkingReporterConfig holds configuration for the reporter.
type AfricasTalkingReporterConfig struct {
	URL        string
	Token      string
	HTTPClient *httpclient.Client
	Logger     logger.Logger
}

// NewAfricasTalkingReporter creates a new AfricasTalking result reporter.
func NewAfricasTalkingReporter(cfg *AfricasTalkingReporterConfig) *AfricasTalkingReporter {
	return &AfricasTalkingReporter{
		url:        cfg.URL,
		token:      cfg.Token,
		httpClient: cfg.HTTPClient,
		log:        cfg.Logger,
	}
}

// sendResultPayload is the request body for the send-result callback.
type sendResultPayload struct {
	OutboxID    int64  `json:"outboxId"`
	Success     bool   `json:"success"`
	MessageID   string `json:"messageId,omitempty"`
	Status      string `json:"status,omitempty"`
	NetworkCode string `json:"networkCode,omitempty"`
}

// Report POSTs the outcome of result to the PHP API. A non-nil error means the
// report call itself failed (network error or non-2xx) — the caller should treat
// the originating delivery as retryable, independent of result.IsSuccess().
func (r *AfricasTalkingReporter) Report(ctx context.Context, result *domain.SendResult) error {
	success := result.IsSuccess()

	status := result.NetworkCode
	if !success && result.Error != nil {
		status = result.Error.Error()
	} else if success {
		status = "Success"
	}

	payload := sendResultPayload{
		OutboxID:    int64(result.Message.OutboxID),
		Success:     success,
		Status:      status,
		NetworkCode: result.NetworkCode,
	}
	if success {
		payload.MessageID = result.ExternalMessageID
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal send-result payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create send-result request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-AT-Consumer-Token", r.token)

	r.log.WithFields(map[string]interface{}{
		"correlator": result.Message.Correlator,
		"outbox_id":  result.Message.OutboxID,
		"success":    success,
	}).Info("Reporting AfricasTalking send result to API v2")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		r.log.WithFields(map[string]interface{}{
			"correlator": result.Message.Correlator,
			"outbox_id":  result.Message.OutboxID,
		}).WithError(err).Error("Failed to report AfricasTalking send result")
		return fmt.Errorf("send-result request failed: %w", err)
	}
	defer httpclient.DrainBody(resp)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		r.log.WithFields(map[string]interface{}{
			"correlator":  result.Message.Correlator,
			"outbox_id":   result.Message.OutboxID,
			"status_code": resp.StatusCode,
			"response":    string(respBody),
		}).Error("AfricasTalking send-result callback rejected")
		return fmt.Errorf("send-result callback returned status %d", resp.StatusCode)
	}

	r.log.WithFields(map[string]interface{}{
		"correlator": result.Message.Correlator,
		"outbox_id":  result.Message.OutboxID,
	}).Info("AfricasTalking send result reported to API v2 successfully")

	return nil
}

// Ensure AfricasTalkingReporter implements ports.ResultReporter
var _ ports.ResultReporter = (*AfricasTalkingReporter)(nil)
