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
	sdpBatchSize  int  // messages per SDP API call for promotional Safaricom; 0/1 = no batching
	isRetry       bool // when true, uses WaitRetry() instead of Wait() for rate limiting
	log           logger.Logger
}

// ProcessorConfig holds configuration for the processor
type ProcessorConfig struct {
	Router        *Router
	ResultHandler *ResultHandler
	RateLimiter   *ratelimit.Limiter
	Metrics       ports.Metrics
	WorkerCount   int
	SDPBatchSize  int  // messages per SDP API call for promotional Safaricom; 0/1 = no batching
	IsRetry       bool // set true for retry consumer processors
	Logger        logger.Logger
}

// NewProcessor creates a new message processor
func NewProcessor(cfg *ProcessorConfig) *Processor {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 10
	}
	batchSize := cfg.SDPBatchSize
	if batchSize < 1 {
		batchSize = 1
	}

	return &Processor{
		router:        cfg.Router,
		resultHandler: cfg.ResultHandler,
		rateLimiter:   cfg.RateLimiter,
		metrics:       cfg.Metrics,
		workerCount:   cfg.WorkerCount,
		sdpBatchSize:  batchSize,
		isRetry:       cfg.IsRetry,
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

// processBatch routes messages through either the SDP batch path (promotional Safaricom)
// or the regular per-message worker pool, depending on configuration.
func (p *Processor) processBatch(ctx context.Context, messages []*domain.Message) *domain.BatchResult {
	result := domain.NewBatchResult()

	// When batching is disabled (size 1) skip classification overhead entirely.
	if p.sdpBatchSize <= 1 {
		return p.processRegular(ctx, messages)
	}

	// Separate promotional Safaricom messages — these go to the SDP batch path.
	// All others use the regular per-message worker pool.
	var sdpPromo []*domain.Message
	var regular []*domain.Message

	for _, msg := range messages {
		if msg.Network() == domain.NetworkSafaricom && !msg.IsTransactional() {
			sdpPromo = append(sdpPromo, msg)
		} else {
			regular = append(regular, msg)
		}
	}

	// SDP promotional batch path
	if len(sdpPromo) > 0 {
		sender, err := p.router.GetSenderByNetwork(domain.NetworkSafaricom)
		if err != nil {
			p.log.WithError(err).Error("Failed to get Safaricom SDP sender for batch")
			for _, msg := range sdpPromo {
				result.AddResult(domain.NewRetryableResult(msg, err, 0))
			}
		} else if bs, ok := sender.(ports.BatchSender); ok {
			p.processSdpBatch(ctx, sdpPromo, bs, result)
		} else {
			// Sender does not implement BatchSender — fall back to regular path
			p.log.Debug("Safaricom SDP sender does not implement BatchSender, falling back to per-message processing")
			regular = append(regular, sdpPromo...)
		}
	}

	// Regular per-message worker pool for everything else
	if len(regular) > 0 {
		regularResult := p.processRegular(ctx, regular)
		for _, r := range regularResult.Successful {
			result.AddResult(r)
		}
		for _, r := range regularResult.Retryable {
			result.AddResult(r)
		}
		for _, r := range regularResult.Failed {
			result.AddResult(r)
		}
	}

	return result
}

// processSdpBatch sends promotional Safaricom messages in sub-batches via a single SDP API call each.
// Messages are first grouped by Sender so each SDP DataSet contains a single oa value.
// Rate limit tokens are consumed per message (N tokens per N-message batch) to preserve the TPS contract.
func (p *Processor) processSdpBatch(
	ctx context.Context,
	msgs []*domain.Message,
	sender ports.BatchSender,
	result *domain.BatchResult,
) {
	// Group by sender so each SDP API call has a homogeneous oa field.
	senderGroups := groupBySender(msgs)

	for _, group := range senderGroups {
		for i := 0; i < len(group); i += p.sdpBatchSize {
			end := i + p.sdpBatchSize
			if end > len(group) {
				end = len(group)
			}
			chunk := group[i:end]

			// Consume N rate limit tokens — one per message — before the batch call.
			var rateLimitErr error
			if p.isRetry {
				rateLimitErr = p.rateLimiter.WaitRetryN(ctx, domain.NetworkSafaricom, len(chunk))
			} else {
				rateLimitErr = p.rateLimiter.WaitN(ctx, domain.NetworkSafaricom, len(chunk))
			}
			if rateLimitErr != nil {
				p.log.WithFields(map[string]interface{}{
					"chunk_size": len(chunk),
					"error":      rateLimitErr.Error(),
				}).Warn("Rate limit wait failed for SDP batch chunk")
				if p.metrics != nil {
					p.metrics.IncRateLimitHits(domain.NetworkSafaricom)
				}
				for _, msg := range chunk {
					result.AddResult(domain.NewRetryableResult(msg, domain.ErrRateLimited, 0))
				}
				continue
			}

			results := sender.SendBatch(ctx, chunk)
			for _, r := range results {
				result.AddResult(r)
			}

			p.log.WithFields(map[string]interface{}{
				"chunk_size": len(chunk),
				"offset":     i,
				"total":      len(group),
				"sender":     group[0].Sender,
			}).Debug("SDP promotional batch chunk sent")
		}
	}
}

// groupBySender partitions msgs into slices that share the same Sender value,
// preserving message order within each group and first-seen sender ordering across groups.
func groupBySender(msgs []*domain.Message) [][]*domain.Message {
	var order []string
	groups := make(map[string][]*domain.Message)
	for _, msg := range msgs {
		if _, ok := groups[msg.Sender]; !ok {
			order = append(order, msg.Sender)
		}
		groups[msg.Sender] = append(groups[msg.Sender], msg)
	}
	result := make([][]*domain.Message, len(order))
	for i, sender := range order {
		result[i] = groups[sender]
	}
	return result
}

// processRegular processes messages individually via the existing worker pool.
func (p *Processor) processRegular(ctx context.Context, messages []*domain.Message) *domain.BatchResult {
	result := domain.NewBatchResult()

	// Create channels for work distribution
	msgChan := make(chan *domain.Message, len(messages))
	resultChan := make(chan *domain.SendResult, len(messages))

	// Cap workers at actual message count — avoids idle goroutines on small retry batches
	var wg sync.WaitGroup
	workers := p.workerCount
	if workers > len(messages) {
		workers = len(messages)
	}
	for i := 0; i < workers; i++ {
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
	// Clone so this worker has exclusive ownership of the message pointer.
	// NewRetryableResult / NewSuccessResult mutate the message (SetError, SetStatus),
	// and two workers can receive the same *Message when callers reuse slices.
	msg = msg.Clone()
	start := time.Now()
	network := msg.Network()

	// Validate message
	if err := msg.Validate(); err != nil {
		p.log.WithFields(map[string]interface{}{
			"correlator": msg.Correlator,
			"error":      err.Error(),
		}).Warn("Message validation failed")
		return domain.NewPermanentResult(msg, err, time.Since(start))
	}

	// Apply rate limiting — retry processors use the reserved retry budget
	var rateLimitErr error
	if p.isRetry {
		rateLimitErr = p.rateLimiter.WaitRetry(ctx, network)
	} else {
		rateLimitErr = p.rateLimiter.Wait(ctx, network)
	}
	if err := rateLimitErr; err != nil {
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

// ProcessMessages processes a slice of messages without handling delivery ack/nack
// This is used by MessageRouter when it needs to control ack timing
// Returns the batch result for the caller to handle
func (p *Processor) ProcessMessages(ctx context.Context, messages []*domain.Message) (*domain.BatchResult, error) {
	if len(messages) == 0 {
		return domain.NewBatchResult(), nil
	}

	start := time.Now()
	p.log.WithField("count", len(messages)).Debug("Processing messages (no ack)")

	// Process all messages
	batchResult := p.processBatch(ctx, messages)
	batchResult.ProcessTime = time.Since(start)

	// Handle results (publish to appropriate queues)
	if err := p.resultHandler.HandleBatchResults(ctx, batchResult); err != nil {
		p.log.WithError(err).Error("Failed to handle batch results")
		return batchResult, err
	}

	p.log.WithFields(map[string]interface{}{
		"total":        batchResult.TotalCount,
		"successful":   batchResult.SuccessCount(),
		"retryable":    batchResult.RetryableCount(),
		"failed":       batchResult.FailedCount(),
		"success_rate": batchResult.SuccessRate(),
		"duration_ms":  batchResult.ProcessTime.Milliseconds(),
	}).Debug("Messages processing complete (no ack)")

	return batchResult, nil
}
