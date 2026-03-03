package service

import (
	"context"
	"sync/atomic"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// MessageRouter routes incoming messages to the appropriate processing path
// - Transactional messages (packageId="TRANSACTIONAL") -> TransactionalHandler (fast-path)
// - Promotional messages -> PriorityScheduler (WFQ)
type MessageRouter struct {
	transactionalHandler *TransactionalHandler
	scheduler            *PriorityScheduler
	processor            *Processor
	metrics              ports.Metrics
	log                  logger.Logger

	// Stats
	transactionalCount atomic.Uint64
	promotionalCount   atomic.Uint64
}

// MessageRouterConfig holds configuration for the message router
type MessageRouterConfig struct {
	TransactionalHandler *TransactionalHandler
	Scheduler            *PriorityScheduler
	Processor            *Processor // Fallback processor when scheduler is nil
	Metrics              ports.Metrics
	Logger               logger.Logger
}

// NewMessageRouter creates a new message router
func NewMessageRouter(cfg *MessageRouterConfig) *MessageRouter {
	return &MessageRouter{
		transactionalHandler: cfg.TransactionalHandler,
		scheduler:            cfg.Scheduler,
		processor:            cfg.Processor,
		metrics:              cfg.Metrics,
		log:                  cfg.Logger,
	}
}

// RouteDelivery routes a delivery to the appropriate processing path
// This is the main entry point for processing messages with priority routing
// It processes all messages (transactional and promotional) and acks the delivery when done
func (r *MessageRouter) RouteDelivery(ctx context.Context, delivery ports.Delivery, queueName string) error {
	messages := delivery.Messages()
	if len(messages) == 0 {
		r.log.Warn("Received empty delivery")
		return delivery.Ack()
	}

	// Separate transactional from promotional messages
	var transactional []*domain.Message
	var promotional []*domain.Message

	for _, msg := range messages {
		if msg.IsTransactional() {
			transactional = append(transactional, msg)
		} else {
			promotional = append(promotional, msg)
		}
	}

	r.log.WithFields(map[string]interface{}{
		"queue":         queueName,
		"total":         len(messages),
		"transactional": len(transactional),
		"promotional":   len(promotional),
	}).Debug("Routing delivery")

	var processingFailed bool

	// Process transactional messages via fast-path (synchronous batch processing)
	if len(transactional) > 0 {
		r.transactionalCount.Add(uint64(len(transactional)))
		if r.metrics != nil {
			for range transactional {
				r.metrics.IncPriorityRouted(ports.MessageTypeTransactional, queueName)
			}
		}

		if r.transactionalHandler != nil {
			// Use ProcessBatch for synchronous processing with completion tracking
			result := r.transactionalHandler.ProcessBatch(ctx, transactional)
			r.log.WithFields(map[string]interface{}{
				"transactional_total":   result.TotalCount,
				"transactional_success": result.SuccessCount(),
				"transactional_failed":  result.FailedCount(),
			}).Debug("Transactional batch processed")
		} else {
			// No transactional handler - process via standard processor
			if r.processor != nil {
				if _, err := r.processor.ProcessMessages(ctx, transactional); err != nil {
					r.log.WithError(err).Error("Failed to process transactional messages via processor")
					processingFailed = true
				}
			}
		}
	}

	// Process promotional messages
	if len(promotional) > 0 {
		r.promotionalCount.Add(uint64(len(promotional)))
		if r.metrics != nil {
			for range promotional {
				r.metrics.IncPriorityRouted(ports.MessageTypePromotional, queueName)
			}
		}

		// Use scheduler if available, otherwise fall back to processor
		if r.scheduler != nil {
			// Process via scheduler (applies WFQ delays)
			if _, err := r.scheduler.ProcessMessages(ctx, promotional, queueName); err != nil {
				r.log.WithError(err).Error("Failed to process promotional messages via scheduler")
				processingFailed = true
			}
		} else if r.processor != nil {
			// No scheduler - process directly
			if _, err := r.processor.ProcessMessages(ctx, promotional); err != nil {
				r.log.WithError(err).Error("Failed to process promotional messages via processor")
				processingFailed = true
			}
		}
	}

	// Ack or Nack the delivery based on processing outcome
	if processingFailed {
		r.log.Warn("Processing had failures, nacking delivery for requeue")
		return delivery.Nack(true)
	}

	if err := delivery.Ack(); err != nil {
		r.log.WithError(err).Error("Failed to acknowledge delivery")
		return err
	}

	r.log.WithFields(map[string]interface{}{
		"queue":         queueName,
		"transactional": len(transactional),
		"promotional":   len(promotional),
	}).Debug("Delivery processed and acknowledged")

	return nil
}

// ProcessDeliveryWithPriority is a convenience method for processing with full priority routing
// Use this when you want to process a delivery with both transactional fast-path and WFQ
func (r *MessageRouter) ProcessDeliveryWithPriority(ctx context.Context, delivery ports.Delivery, queueName string) error {
	return r.RouteDelivery(ctx, delivery, queueName)
}

// GetStats returns routing statistics
func (r *MessageRouter) GetStats() (transactional, promotional uint64) {
	return r.transactionalCount.Load(), r.promotionalCount.Load()
}

// Stats returns a map of all stats
func (r *MessageRouter) Stats() map[string]uint64 {
	stats := map[string]uint64{
		"transactional_routed": r.transactionalCount.Load(),
		"promotional_routed":   r.promotionalCount.Load(),
	}

	// Add transactional handler stats
	if r.transactionalHandler != nil {
		processed, failed := r.transactionalHandler.Stats()
		stats["transactional_processed"] = processed
		stats["transactional_failed"] = failed
		stats["transactional_queue_depth"] = uint64(r.transactionalHandler.QueueDepth())
	}

	// Add scheduler stats
	if r.scheduler != nil {
		for queue, count := range r.scheduler.GetStats() {
			stats["scheduler_"+queue] = count
		}
		stats["scheduler_total"] = r.scheduler.TotalProcessed()
	}

	return stats
}
