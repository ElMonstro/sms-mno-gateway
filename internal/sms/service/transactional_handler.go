package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// TransactionalHandler provides a fast-path for transactional messages
// Transactional messages (packageId="TRANSACTIONAL") bypass the priority scheduler
// and are processed immediately with dedicated workers
type TransactionalHandler struct {
	router        *Router
	resultHandler *ResultHandler
	rateLimiter   *ratelimit.Limiter
	metrics       ports.Metrics
	workerCount   int
	log           logger.Logger

	// Message channel for fast-path processing
	msgChan chan *transactionalWork

	// Stats
	processed atomic.Uint64
	failed    atomic.Uint64

	// Lifecycle
	wg       sync.WaitGroup
	ctx      context.Context
	cancelFn context.CancelFunc
}

// transactionalWork wraps a message with its delivery for ack handling
type transactionalWork struct {
	msg       *domain.Message
	delivery  ports.Delivery
	queueName string
}

// TransactionalHandlerConfig holds configuration for the handler
type TransactionalHandlerConfig struct {
	Router        *Router
	ResultHandler *ResultHandler
	RateLimiter   *ratelimit.Limiter
	Metrics       ports.Metrics
	WorkerCount   int
	Logger        logger.Logger
}

// NewTransactionalHandler creates a new transactional message handler
func NewTransactionalHandler(cfg *TransactionalHandlerConfig) *TransactionalHandler {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 5 // Default dedicated workers for transactional
	}

	ctx, cancel := context.WithCancel(context.Background())

	handler := &TransactionalHandler{
		router:        cfg.Router,
		resultHandler: cfg.ResultHandler,
		rateLimiter:   cfg.RateLimiter,
		metrics:       cfg.Metrics,
		workerCount:   cfg.WorkerCount,
		log:           cfg.Logger,
		msgChan:       make(chan *transactionalWork, cfg.WorkerCount*10), // Buffered channel
		ctx:           ctx,
		cancelFn:      cancel,
	}

	return handler
}

// Start starts the transactional handler workers
func (h *TransactionalHandler) Start() {
	h.log.WithField("workers", h.workerCount).Info("Starting transactional handler")

	for i := 0; i < h.workerCount; i++ {
		h.wg.Add(1)
		go h.worker(i)
	}
}

// Stop gracefully stops the handler and waits for workers to finish
func (h *TransactionalHandler) Stop() {
	h.log.Info("Stopping transactional handler")
	h.cancelFn()
	close(h.msgChan)
	h.wg.Wait()
	h.log.WithFields(map[string]interface{}{
		"processed": h.processed.Load(),
		"failed":    h.failed.Load(),
	}).Info("Transactional handler stopped")
}

// Handle submits a transactional message for immediate processing
// This is the fast-path entry point - messages are queued to dedicated workers
func (h *TransactionalHandler) Handle(ctx context.Context, msg *domain.Message, delivery ports.Delivery, queueName string) error {
	work := &transactionalWork{
		msg:       msg,
		delivery:  delivery,
		queueName: queueName,
	}

	select {
	case h.msgChan <- work:
		h.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"queue":      queueName,
		}).Debug("Transactional message queued for fast-path processing")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
}

// HandleSync processes a transactional message synchronously
// Use this when you need to wait for the result
func (h *TransactionalHandler) HandleSync(ctx context.Context, msg *domain.Message) *domain.SendResult {
	return h.processMessage(ctx, msg)
}

// worker processes transactional messages from the channel
func (h *TransactionalHandler) worker(id int) {
	defer h.wg.Done()

	h.log.WithField("worker_id", id).Debug("Transactional worker started")

	for {
		select {
		case <-h.ctx.Done():
			h.log.WithField("worker_id", id).Debug("Transactional worker stopping")
			return
		case work, ok := <-h.msgChan:
			if !ok {
				return
			}
			h.processWork(work, id)
		}
	}
}

// processWork processes a single transactional work item
func (h *TransactionalHandler) processWork(work *transactionalWork, workerID int) {
	start := time.Now()
	msg := work.msg

	// Update queue depth metric
	if h.metrics != nil {
		h.metrics.SetTransactionalQueueDepth(len(h.msgChan))
	}

	h.log.WithFields(map[string]interface{}{
		"correlator": msg.Correlator,
		"worker_id":  workerID,
		"queue":      work.queueName,
	}).Debug("Processing transactional message")

	result := h.processMessage(h.ctx, msg)

	// Track stats
	h.processed.Add(1)
	if result.Type == domain.ResultPermanent || result.Type == domain.ResultRetryable {
		h.failed.Add(1)
		if h.metrics != nil {
			h.metrics.IncTransactionalProcessed(ports.MetricStatusFailed)
		}
	} else {
		if h.metrics != nil {
			h.metrics.IncTransactionalProcessed(ports.MetricStatusSuccess)
		}
	}

	// Handle result (publish to appropriate queue)
	batchResult := domain.NewBatchResult()
	batchResult.AddResult(result)
	batchResult.ProcessTime = time.Since(start)

	if err := h.resultHandler.HandleBatchResults(h.ctx, batchResult); err != nil {
		h.log.WithError(err).WithField("correlator", msg.Correlator).Error("Failed to handle transactional result")
	}

	// Log completion
	h.log.WithFields(map[string]interface{}{
		"correlator":  msg.Correlator,
		"result_type": result.Type.String(),
		"latency_ms":  result.Latency.Milliseconds(),
		"worker_id":   workerID,
	}).Debug("Transactional message processed")
}

// processMessage processes a single message (similar to Processor.processMessage)
func (h *TransactionalHandler) processMessage(ctx context.Context, msg *domain.Message) *domain.SendResult {
	start := time.Now()
	network := msg.Network()

	// Normalize MSISDN
	msg.MSISDN = msg.NormalizeMSISDN()

	// Validate message
	if err := msg.Validate(); err != nil {
		h.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"error":      err.Error(),
		}).Warn("Transactional message validation failed")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	// Apply rate limiting (transactional still respects rate limits)
	if err := h.rateLimiter.Wait(ctx, network); err != nil {
		h.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"network":    network.String(),
			"error":      err.Error(),
		}).Warn("Transactional rate limit wait failed")

		if h.metrics != nil {
			h.metrics.IncRateLimitHits(network)
		}
		return domain.NewRetryableResult(msg, domain.ErrRateLimited, time.Since(start))
	}

	// Get sender (for Safaricom transactional, this returns SMPP sender)
	sender, err := h.router.GetSender(msg)
	if err != nil {
		h.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"network":    network.String(),
			"error":      err.Error(),
		}).Error("Failed to get sender for transactional message")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	// Check sender health
	if !sender.IsHealthy() {
		h.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"network":    network.String(),
			"sender":     sender.Name(),
		}).Warn("Transactional sender is unhealthy (circuit breaker open)")
		return domain.NewRetryableResult(msg, domain.ErrCircuitOpen, time.Since(start))
	}

	// Send message
	result := sender.Send(ctx, msg)

	// Record metrics
	if h.metrics != nil {
		h.metrics.ObserveSendLatency(network, result.Latency)
	}

	return result
}

// Stats returns processing statistics
func (h *TransactionalHandler) Stats() (processed, failed uint64) {
	return h.processed.Load(), h.failed.Load()
}

// QueueDepth returns the current number of pending transactional messages
func (h *TransactionalHandler) QueueDepth() int {
	return len(h.msgChan)
}

// ProcessBatch processes a batch of transactional messages and waits for completion
// This is the preferred method when you need to know when all messages are done
// (e.g., for proper delivery acknowledgment)
func (h *TransactionalHandler) ProcessBatch(ctx context.Context, messages []*domain.Message) *domain.BatchResult {
	if len(messages) == 0 {
		return domain.NewBatchResult()
	}

	result := domain.NewBatchResult()
	resultChan := make(chan *domain.SendResult, len(messages))

	// Process all messages concurrently using the worker pool
	var wg sync.WaitGroup
	for _, msg := range messages {
		wg.Add(1)
		go func(m *domain.Message) {
			defer wg.Done()
			sendResult := h.processMessage(h.ctx, m)

			// Track stats
			h.processed.Add(1)
			if sendResult.Type == domain.ResultPermanent || sendResult.Type == domain.ResultRetryable {
				h.failed.Add(1)
				if h.metrics != nil {
					h.metrics.IncTransactionalProcessed(ports.MetricStatusFailed)
				}
			} else {
				if h.metrics != nil {
					h.metrics.IncTransactionalProcessed(ports.MetricStatusSuccess)
				}
			}

			resultChan <- sendResult
		}(msg)
	}

	// Wait for all processing to complete and close channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for sendResult := range resultChan {
		result.AddResult(sendResult)
	}

	// Handle results (publish to appropriate queues)
	if err := h.resultHandler.HandleBatchResults(ctx, result); err != nil {
		h.log.WithError(err).Error("Failed to handle transactional batch results")
	}

	return result
}
