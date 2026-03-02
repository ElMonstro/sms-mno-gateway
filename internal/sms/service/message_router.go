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

	// Route transactional messages to fast-path
	if len(transactional) > 0 && r.transactionalHandler != nil {
		r.transactionalCount.Add(uint64(len(transactional)))
		for _, msg := range transactional {
			if r.metrics != nil {
				r.metrics.IncPriorityRouted(ports.MessageTypeTransactional, queueName)
			}
			if err := r.transactionalHandler.Handle(ctx, msg, delivery, queueName); err != nil {
				r.log.WithError(err).WithField("correlator", msg.Correlator).Error("Failed to route transactional message")
			}
		}
	}

	// Route promotional messages
	// If scheduler is enabled, it will handle via WFQ
	// Otherwise, process directly with the standard processor
	if len(promotional) > 0 {
		r.promotionalCount.Add(uint64(len(promotional)))
		if r.metrics != nil {
			for range promotional {
				r.metrics.IncPriorityRouted(ports.MessageTypePromotional, queueName)
			}
		}

		// Create a filtered delivery with only promotional messages
		// The scheduler/processor will handle these
		// For now, if we have a scheduler, let it handle via its registered queue channels
		// If no scheduler, use the standard processor
		if r.scheduler == nil && r.processor != nil {
			// No priority scheduling - use standard processor
			return r.processor.ProcessDelivery(ctx, delivery)
		}
		// With scheduler, messages flow through the registered queue channels
		// which are handled by PriorityScheduler.processQueue()
	}

	// If all messages were transactional and handled, ack the delivery
	if len(promotional) == 0 && len(transactional) > 0 {
		// Transactional messages are handled async, so we can ack
		// Note: This assumes transactional handler queues the messages
		// and handles ack/nack internally per message
		return nil // Don't ack here - transactional handler will manage its own lifecycle
	}

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
