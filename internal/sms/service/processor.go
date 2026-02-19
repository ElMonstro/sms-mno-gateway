package service

import (
	"context"
	"sync"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Processor processes batches of messages with a worker pool
// CRITICAL: Implements per-message retry, NOT batch retry
type Processor struct {
	router        *Router
	resultHandler *ResultHandler
	rateLimiter   *ratelimit.Limiter
	metrics       ports.Metrics
	workerCount   int
	log           logger.Logger
}

// ProcessorConfig holds configuration for the processor
type ProcessorConfig struct {
	Router        *Router
	ResultHandler *ResultHandler
	RateLimiter   *ratelimit.Limiter
	Metrics       ports.Metrics
	WorkerCount   int
	Logger        logger.Logger
}

// NewProcessor creates a new message processor
func NewProcessor(cfg *ProcessorConfig) *Processor {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 10
	}

	return &Processor{
		router:        cfg.Router,
		resultHandler: cfg.ResultHandler,
		rateLimiter:   cfg.RateLimiter,
		metrics:       cfg.Metrics,
		workerCount:   cfg.WorkerCount,
		log:           cfg.Logger,
	}
}

// ProcessDelivery processes a delivery from the queue
// Returns true if all messages were processed successfully (for ack decision)
func (p *Processor) ProcessDelivery(ctx context.Context, delivery ports.Delivery) error {
	messages := delivery.Messages()
	if len(messages) == 0 {
		p.log.Warn("Received empty delivery")
		return delivery.Ack()
	}

	start := time.Now()
	p.log.WithField("count", len(messages)).Info("Processing batch")

	// Process all messages
	batchResult := p.processBatch(ctx, messages)
	batchResult.ProcessTime = time.Since(start)

	// Handle results (publish to appropriate queues)
	if err := p.resultHandler.HandleBatchResults(ctx, batchResult); err != nil {
		p.log.WithError(err).Error("Failed to handle batch results")
		// Nack with requeue on failure to publish results
		return delivery.Nack(true)
	}

	// Acknowledge the original delivery ONLY after all messages are processed
	// and results are published (EM-143 fix)
	if err := delivery.Ack(); err != nil {
		p.log.WithError(err).Error("Failed to acknowledge delivery")
		return err
	}

	p.log.WithFields(map[string]interface{}{
		"total":        batchResult.TotalCount,
		"successful":   batchResult.SuccessCount(),
		"retryable":    batchResult.RetryableCount(),
		"failed":       batchResult.FailedCount(),
		"success_rate": batchResult.SuccessRate(),
		"duration_ms":  batchResult.ProcessTime.Milliseconds(),
	}).Info("Batch processing complete")

	return nil
}

// processBatch processes a batch of messages using a worker pool
func (p *Processor) processBatch(ctx context.Context, messages []*domain.Message) *domain.BatchResult {
	result := domain.NewBatchResult()

	// Create channels for work distribution
	msgChan := make(chan *domain.Message, len(messages))
	resultChan := make(chan *domain.SendResult, len(messages))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go p.worker(ctx, i, msgChan, resultChan, &wg)
	}

	// Send messages to workers
	go func() {
		for _, msg := range messages {
			select {
			case msgChan <- msg:
			case <-ctx.Done():
				return
			}
		}
		close(msgChan)
	}()

	// Wait for workers in a separate goroutine
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for sendResult := range resultChan {
		result.AddResult(sendResult)
	}

	return result
}

// worker processes messages from the channel
func (p *Processor) worker(ctx context.Context, id int, msgs <-chan *domain.Message, results chan<- *domain.SendResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			result := p.processMessage(ctx, msg)
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

// processMessage processes a single message
func (p *Processor) processMessage(ctx context.Context, msg *domain.Message) *domain.SendResult {
	start := time.Now()
	network := msg.Network()

	// Normalize MSISDN
	msg.MSISDN = msg.NormalizeMSISDN()

	// Validate message
	if err := msg.Validate(); err != nil {
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"error":      err.Error(),
		}).Warn("Message validation failed")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	// Apply rate limiting
	if err := p.rateLimiter.Wait(ctx, network); err != nil {
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"network":    network.String(),
			"error":      err.Error(),
		}).Warn("Rate limit wait failed")

		if p.metrics != nil {
			p.metrics.IncRateLimitHits(network)
		}
		return domain.NewRetryableResult(msg, domain.ErrRateLimited, time.Since(start))
	}

	// Get sender
	sender, err := p.router.GetSender(msg)
	if err != nil {
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"network":    network.String(),
			"error":      err.Error(),
		}).Error("Failed to get sender")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	// Check sender health
	if !sender.IsHealthy() {
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"network":    network.String(),
			"sender":     sender.Name(),
		}).Warn("Sender is unhealthy (circuit breaker open)")
		return domain.NewRetryableResult(msg, domain.ErrCircuitOpen, time.Since(start))
	}

	// Send message
	result := sender.Send(ctx, msg)

	// Record metrics
	if p.metrics != nil {
		p.metrics.ObserveSendLatency(network, result.Latency)
	}

	return result
}

// ProcessRetryQueue processes messages from the retry queue
// This is called separately to handle messages that failed previously
func (p *Processor) ProcessRetryQueue(ctx context.Context, delivery ports.Delivery) error {
	messages := delivery.Messages()

	p.log.WithField("count", len(messages)).Info("Processing retry batch")

	// Process with the same logic as regular processing
	return p.ProcessDelivery(ctx, delivery)
}
