package service

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// ResultHandler handles the routing of send results to appropriate queues
// This addresses EM-149: Route permanent failures to DLQ
type ResultHandler struct {
	publisher  ports.QueuePublisher
	metrics    ports.Metrics
	maxRetries int
	log        logger.Logger
}

// ResultHandlerConfig holds configuration for the result handler
type ResultHandlerConfig struct {
	Publisher  ports.QueuePublisher
	Metrics    ports.Metrics
	MaxRetries int
	Logger     logger.Logger
}

// NewResultHandler creates a new result handler
func NewResultHandler(cfg *ResultHandlerConfig) *ResultHandler {
	return &ResultHandler{
		publisher:  cfg.Publisher,
		metrics:    cfg.Metrics,
		maxRetries: cfg.MaxRetries,
		log:        cfg.Logger,
	}
}

// HandleResult processes a single send result and publishes to the appropriate queue
func (h *ResultHandler) HandleResult(ctx context.Context, result *domain.SendResult) error {
	network := result.Message.Network()

	switch result.Type {
	case domain.ResultSuccess:
		h.log.WithFields(map[string]interface{}{
			"correlator": result.Message.Correlator,
			"network":    network.String(),
			"latency_ms": result.Latency.Milliseconds(),
		}).Info("Message sent successfully")

		if h.metrics != nil {
			h.metrics.IncMessagesProcessed(network, ports.MetricStatusSuccess)
		}

	case domain.ResultRetryable:
		// Check if we've exceeded max retries
		if result.Message.RetryCount >= h.maxRetries {
			h.log.WithFields(map[string]interface{}{
				"correlator":  result.Message.Correlator,
				"network":     network.String(),
				"retry_count": result.Message.RetryCount,
				"max_retries": h.maxRetries,
				"error":       result.Error.Error(),
			}).Warn("Message exceeded max retries, routing to DLQ")

			// Change result type to permanent failure
			result.Type = domain.ResultPermanent
			result.Message.SetError(domain.ErrMaxRetriesExceeded)

			if h.metrics != nil {
				h.metrics.IncDeadLetters(network)
			}
		} else {
			h.log.WithFields(map[string]interface{}{
				"correlator":  result.Message.Correlator,
				"network":     network.String(),
				"retry_count": result.Message.RetryCount,
				"error":       result.Error.Error(),
			}).Warn("Message failed, will retry")

			if h.metrics != nil {
				h.metrics.IncRetries(network)
			}
		}

	case domain.ResultPermanent:
		h.log.WithFields(map[string]interface{}{
			"correlator": result.Message.Correlator,
			"network":    network.String(),
			"error":      result.Error.Error(),
		}).Error("Message permanently failed, routing to DLQ")

		if h.metrics != nil {
			h.metrics.IncDeadLetters(network)
			h.metrics.IncMessagesProcessed(network, ports.MetricStatusFailed)
		}
	}

	// Publish to appropriate queue
	return h.publisher.PublishResult(ctx, result)
}

// HandleBatchResults processes a batch of send results
func (h *ResultHandler) HandleBatchResults(ctx context.Context, results *domain.BatchResult) error {
	// Check retryable messages against max retries
	for i := len(results.Retryable) - 1; i >= 0; i-- {
		result := results.Retryable[i]
		if result.Message.RetryCount >= h.maxRetries {
			// Move to failed
			result.Type = domain.ResultPermanent
			result.Message.SetError(domain.ErrMaxRetriesExceeded)
			results.Failed = append(results.Failed, result)

			// Remove from retryable
			results.Retryable = append(results.Retryable[:i], results.Retryable[i+1:]...)

			h.log.WithFields(map[string]interface{}{
				"correlator":  result.Message.Correlator,
				"retry_count": result.Message.RetryCount,
			}).Warn("Message exceeded max retries, moving to DLQ")
		}
	}

	// Log summary
	h.log.WithFields(map[string]interface{}{
		"successful":   results.SuccessCount(),
		"retryable":    results.RetryableCount(),
		"failed":       results.FailedCount(),
		"total":        results.TotalCount,
		"success_rate": results.SuccessRate(),
	}).Info("Processing batch results")

	// Publish all results
	return h.publisher.PublishBatchResults(ctx, results)
}
