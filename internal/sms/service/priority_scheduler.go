package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// PriorityScheduler implements Credit-Based Weighted Round Robin for promotional messages.
// Higher weight queues receive more credits per refill cycle, allowing more messages to be processed.
//
// Algorithm:
//   - Each queue has a credit channel (buffered) acting as a semaphore
//   - Credit channel buffer size = weight × creditMultiplier
//   - Processing requires acquiring a credit (reading from channel)
//   - A background goroutine refills credits every refillPeriod
//   - Starvation prevention grants immediate credits to starving queues
//
// Benefits over time-based delays:
//   - Zero latency when system is idle (credits available immediately)
//   - Fair distribution under contention (credits consumed proportionally)
//   - Hot-reloadable weights (credit channels resized on weight change)
type PriorityScheduler struct {
	priorityStore ports.PriorityStore
	processor     *Processor
	metrics       ports.Metrics
	log           logger.Logger

	// Queue state
	mu            sync.RWMutex
	queues        map[string]*queueState
	weights       map[string]int
	defaultWeight int

	// Credit-based WRR configuration (all protected by mu for live updates)
	creditMultiplier int           // Credits per weight unit (e.g., 10 means weight=5 gets 50 credits)
	refillPeriod     time.Duration // How often to refill credits
	maxStarvationAge time.Duration // Max time without processing before starvation prevention kicks in

	// refillPeriodCh carries period changes from watchSchedulerConfig to creditRefillLoop.
	refillPeriodCh chan time.Duration

	// dynConfig enables hot-reload of scheduler tuning from Redis.
	dynConfig ports.DynamicConfigStore

	// Stats
	processedByQueue map[string]*atomic.Uint64
	totalProcessed   atomic.Uint64
	creditsGranted   atomic.Uint64
	starvationEvents atomic.Uint64

	// Lifecycle
	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
}

// queueState tracks per-queue scheduling state
type queueState struct {
	name              string
	weight            int
	credits           chan struct{} // Buffered channel as credit semaphore
	lastProcessedNano atomic.Int64 // UnixNano of last processed time; updated atomically
	pendingCount      atomic.Int64
	creditsMu         sync.Mutex // Protects credit channel recreation
}

// PrioritySchedulerConfig holds configuration for the scheduler
type PrioritySchedulerConfig struct {
	PriorityStore    ports.PriorityStore
	Processor        *Processor
	Metrics          ports.Metrics
	DefaultWeight    int
	CreditMultiplier int           // Credits per weight unit (default: 10)
	RefillPeriod     time.Duration // Credit refill interval (default: 100ms)
	MaxStarvationAge time.Duration // Starvation threshold (default: 10s)
	// DynamicConfig enables hot-reload of CreditMultiplier, RefillPeriod, and
	// MaxStarvationAge from Redis without a restart. Reads from NSSchedulerPromotional.
	DynamicConfig ports.DynamicConfigStore
	Logger        logger.Logger
}

// NewPriorityScheduler creates a new priority scheduler with Credit-Based WRR
func NewPriorityScheduler(cfg *PrioritySchedulerConfig) *PriorityScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	// Apply defaults
	if cfg.DefaultWeight <= 0 {
		cfg.DefaultWeight = 1
	}
	if cfg.CreditMultiplier <= 0 {
		cfg.CreditMultiplier = 10 // 10 credits per weight unit
	}
	if cfg.RefillPeriod <= 0 {
		cfg.RefillPeriod = 100 * time.Millisecond // Refill every 100ms
	}
	if cfg.MaxStarvationAge <= 0 {
		cfg.MaxStarvationAge = 10 * time.Second // 10 seconds starvation threshold
	}

	scheduler := &PriorityScheduler{
		priorityStore:    cfg.PriorityStore,
		processor:        cfg.Processor,
		metrics:          cfg.Metrics,
		log:              cfg.Logger,
		queues:           make(map[string]*queueState),
		weights:          make(map[string]int),
		defaultWeight:    cfg.DefaultWeight,
		creditMultiplier: cfg.CreditMultiplier,
		refillPeriod:     cfg.RefillPeriod,
		maxStarvationAge: cfg.MaxStarvationAge,
		refillPeriodCh:   make(chan time.Duration, 1),
		dynConfig:        cfg.DynamicConfig,
		processedByQueue: make(map[string]*atomic.Uint64),
		ctx:              ctx,
		cancelFn:         cancel,
	}

	return scheduler
}

// Start starts the scheduler, weight watcher, and credit refill loop
func (s *PriorityScheduler) Start() error {
	s.log.Info("Starting priority scheduler with Credit-Based WRR")

	// Load initial weights
	if err := s.loadWeights(); err != nil {
		s.log.WithError(err).Warn("Failed to load initial weights, using defaults")
	}

	// Start credit refill loop
	s.wg.Add(1)
	go s.creditRefillLoop()

	// Start weight watcher for hot-reload
	if s.priorityStore != nil {
		s.wg.Add(1)
		go s.watchWeights()
	}

	// Start dynamic config watcher (scheduler tuning: multiplier, period, starvation age)
	if s.dynConfig != nil {
		s.wg.Add(1)
		go s.watchSchedulerConfig()
	}

	s.log.WithFields(map[string]interface{}{
		"credit_multiplier":  s.creditMultiplier,
		"refill_period_ms":   s.refillPeriod.Milliseconds(),
		"max_starvation_sec": s.maxStarvationAge.Seconds(),
		"default_weight":     s.defaultWeight,
	}).Info("Priority scheduler started")

	return nil
}

// Stop gracefully stops the scheduler
func (s *PriorityScheduler) Stop() {
	s.log.Info("Stopping priority scheduler")
	s.cancelFn()
	s.wg.Wait()

	s.log.WithFields(map[string]interface{}{
		"total_processed":   s.totalProcessed.Load(),
		"credits_granted":   s.creditsGranted.Load(),
		"starvation_events": s.starvationEvents.Load(),
	}).Info("Priority scheduler stopped")
}

// loadWeights loads queue weights from the priority store
func (s *PriorityScheduler) loadWeights() error {
	if s.priorityStore == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	weights, err := s.priorityStore.GetQueueWeights(ctx)
	if err != nil {
		return err
	}

	s.updateWeights(weights)
	return nil
}

// updateWeights updates the internal weights and resizes credit channels
func (s *PriorityScheduler) updateWeights(weights map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldWeights := s.weights
	s.weights = weights

	// Update existing queue states
	for name, qs := range s.queues {
		newWeight := s.defaultWeight
		if w, ok := weights[name]; ok {
			newWeight = w
		}

		oldWeight := qs.weight
		if oldWeight != newWeight {
			qs.weight = newWeight
			// Resize credit channel if weight changed
			s.resizeCreditChannel(qs, newWeight)
			s.log.WithFields(map[string]interface{}{
				"queue":      name,
				"old_weight": oldWeight,
				"new_weight": newWeight,
			}).Info("Queue weight changed, credit channel resized")
		}

		// Emit weight metric
		if s.metrics != nil {
			s.metrics.SetSchedulerWeight(name, newWeight)
		}
	}

	// Log weight changes
	if len(oldWeights) > 0 || len(weights) > 0 {
		s.log.WithField("weights", weights).Info("Priority weights updated")
	}
}

// resizeCreditChannel recreates the credit channel with new capacity
// Must be called with s.mu held
func (s *PriorityScheduler) resizeCreditChannel(qs *queueState, newWeight int) {
	qs.creditsMu.Lock()
	defer qs.creditsMu.Unlock()

	newCapacity := newWeight * s.creditMultiplier
	if newCapacity <= 0 {
		newCapacity = s.creditMultiplier
	}

	// Create new channel with new capacity
	newCredits := make(chan struct{}, newCapacity)

	// Transfer existing credits (up to new capacity)
	if qs.credits != nil {
		transferred := 0
		for transferred < newCapacity {
			select {
			case <-qs.credits:
				newCredits <- struct{}{}
				transferred++
			default:
				// No more credits to transfer
				goto done
			}
		}
	done:
	}

	qs.credits = newCredits
}

// watchWeights watches for weight changes and updates the scheduler
func (s *PriorityScheduler) watchWeights() {
	defer s.wg.Done()

	watchChan, err := s.priorityStore.WatchWeights(s.ctx)
	if err != nil {
		s.log.WithError(err).Error("Failed to start weight watcher")
		return
	}

	s.log.Info("Weight watcher started")

	for {
		select {
		case <-s.ctx.Done():
			return
		case weights, ok := <-watchChan:
			if !ok {
				return
			}
			s.updateWeights(weights)
		}
	}
}

// creditRefillLoop periodically refills credits for all queues.
// It listens on refillPeriodCh to pick up live period changes from watchSchedulerConfig.
func (s *PriorityScheduler) creditRefillLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.refillPeriod)
	defer ticker.Stop()

	s.log.WithField("period_ms", s.refillPeriod.Milliseconds()).Info("Credit refill loop started")

	for {
		select {
		case <-s.ctx.Done():
			return
		case newPeriod := <-s.refillPeriodCh:
			ticker.Reset(newPeriod)
			s.log.WithField("period_ms", newPeriod.Milliseconds()).Info("Credit refill period updated")
		case <-ticker.C:
			s.refillAllCredits()
		}
	}
}

// watchSchedulerConfig watches NSSchedulerPromotional in DynamicConfig and applies
// live updates to creditMultiplier, refillPeriod, and maxStarvationAge.
func (s *PriorityScheduler) watchSchedulerConfig() {
	defer s.wg.Done()

	ch, err := s.dynConfig.Watch(s.ctx, ports.NSSchedulerPromotional)
	if err != nil {
		s.log.WithError(err).Error("Failed to start scheduler config watcher")
		return
	}

	s.log.Info("Scheduler config watcher started")

	for {
		select {
		case <-s.ctx.Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
			s.mu.Lock()
			newMultiplier := s.dynConfig.GetInt(ports.NSSchedulerPromotional, ports.FieldCreditMultiplier, s.creditMultiplier)
			newPeriodMs := s.dynConfig.GetInt(ports.NSSchedulerPromotional, ports.FieldRefillPeriodMs, int(s.refillPeriod.Milliseconds()))
			newStarvationSec := s.dynConfig.GetInt(ports.NSSchedulerPromotional, ports.FieldMaxStarvationAgeSec, int(s.maxStarvationAge.Seconds()))

			periodChanged := newPeriodMs != int(s.refillPeriod.Milliseconds())

			if newMultiplier > 0 {
				s.creditMultiplier = newMultiplier
			}
			newPeriod := time.Duration(newPeriodMs) * time.Millisecond
			if newPeriod > 0 {
				s.refillPeriod = newPeriod
			}
			newAge := time.Duration(newStarvationSec) * time.Second
			if newAge > 0 {
				s.maxStarvationAge = newAge
			}
			s.mu.Unlock()

			if periodChanged && newPeriod > 0 {
				select {
				case s.refillPeriodCh <- newPeriod:
				default:
				}
			}

			s.log.WithFields(map[string]any{
				"credit_multiplier":  newMultiplier,
				"refill_period_ms":   newPeriodMs,
				"max_starvation_sec": newStarvationSec,
			}).Info("Scheduler config updated from Redis")
		}
	}
}

// refillAllCredits adds credits to all queues based on their weights
func (s *PriorityScheduler) refillAllCredits() {
	s.mu.RLock()
	queues := make([]*queueState, 0, len(s.queues))
	for _, qs := range s.queues {
		queues = append(queues, qs)
	}
	s.mu.RUnlock()

	for _, qs := range queues {
		s.refillQueueCredits(qs)
	}
}

// refillQueueCredits adds credits to a single queue based on its weight
func (s *PriorityScheduler) refillQueueCredits(qs *queueState) {
	qs.creditsMu.Lock()
	defer qs.creditsMu.Unlock()

	if qs.credits == nil {
		return
	}

	// Add credits equal to weight (not full capacity, to allow gradual accumulation)
	creditsToAdd := qs.weight
	if creditsToAdd <= 0 {
		creditsToAdd = s.defaultWeight
	}

	added := 0
	for i := 0; i < creditsToAdd; i++ {
		select {
		case qs.credits <- struct{}{}:
			added++
		default:
			// Channel full, stop adding
			break
		}
	}

	if added > 0 {
		s.creditsGranted.Add(uint64(added))
	}
}

// acquireCredit attempts to acquire a processing credit for a queue
// Returns nil on success, context error if cancelled, or grants immediate credit on starvation
func (s *PriorityScheduler) acquireCredit(ctx context.Context, qs *queueState) error {
	// Read maxStarvationAge under RLock — watchSchedulerConfig may update it concurrently.
	s.mu.RLock()
	maxAge := s.maxStarvationAge
	s.mu.RUnlock()

	// Check for starvation - if queue hasn't processed in too long, grant immediate credit
	timeSinceLastProcess := time.Since(time.Unix(0, qs.lastProcessedNano.Load()))
	if timeSinceLastProcess > maxAge {
		s.starvationEvents.Add(1)
		if s.metrics != nil {
			s.metrics.IncStarvationTriggers(qs.name)
		}
		s.log.WithFields(map[string]interface{}{
			"queue":            qs.name,
			"time_since_ms":    timeSinceLastProcess.Milliseconds(),
			"max_starvation_s": maxAge.Seconds(),
		}).Debug("Starvation prevention: granting immediate credit")
		return nil
	}

	qs.creditsMu.Lock()
	creditChan := qs.credits
	qs.creditsMu.Unlock()

	if creditChan == nil {
		// No credit channel yet, allow processing (will be created on first use)
		return nil
	}

	// Try to acquire credit
	select {
	case <-creditChan:
		// Got credit, proceed
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// No credit available immediately, wait with timeout
	}

	// Wait for credit with context awareness
	select {
	case <-creditChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// getOrCreateQueueState gets or creates queue state for a queue name
func (s *PriorityScheduler) getOrCreateQueueState(queueName string) *queueState {
	s.mu.Lock()
	defer s.mu.Unlock()

	qs, exists := s.queues[queueName]
	if !exists {
		weight := s.defaultWeight
		if w, ok := s.weights[queueName]; ok {
			weight = w
		}

		// Calculate credit channel capacity
		capacity := weight * s.creditMultiplier
		if capacity <= 0 {
			capacity = s.creditMultiplier
		}

		// Create credit channel and pre-fill it
		credits := make(chan struct{}, capacity)
		for i := 0; i < capacity; i++ {
			credits <- struct{}{}
		}

		qs = &queueState{
			name:    queueName,
			weight:  weight,
			credits: credits,
		}
		qs.lastProcessedNano.Store(time.Now().UnixNano())
		s.queues[queueName] = qs
		s.processedByQueue[queueName] = &atomic.Uint64{}

		s.log.WithFields(map[string]interface{}{
			"queue":    queueName,
			"weight":   weight,
			"capacity": capacity,
		}).Info("Created new queue state with credit channel")

		// Emit initial weight metric
		if s.metrics != nil {
			s.metrics.SetSchedulerWeight(queueName, weight)
		}
	}

	return qs
}

// ProcessMessages processes a batch of messages with Credit-Based WRR scheduling.
// This is called by MessageRouter for promotional messages.
// It acquires credits before processing, ensuring fair resource allocation.
func (s *PriorityScheduler) ProcessMessages(ctx context.Context, messages []*domain.Message, queueName string) (*domain.BatchResult, error) {
	if len(messages) == 0 {
		return domain.NewBatchResult(), nil
	}

	// Get or create queue state
	qs := s.getOrCreateQueueState(queueName)

	// Acquire credit for this batch (1 credit per batch, not per message)
	if err := s.acquireCredit(ctx, qs); err != nil {
		s.log.WithError(err).WithField("queue", queueName).Warn("Failed to acquire credit, context cancelled")
		return domain.NewBatchResult(), err
	}

	// Process via the processor
	result, err := s.processor.ProcessMessages(ctx, messages)

	// Update stats — RLock protects the map read; the counter itself is atomic.
	s.mu.RLock()
	counter, ok := s.processedByQueue[queueName]
	s.mu.RUnlock()
	if ok {
		counter.Add(uint64(len(messages)))
	}
	s.totalProcessed.Add(uint64(len(messages)))
	qs.lastProcessedNano.Store(time.Now().UnixNano())

	// Emit metrics
	if s.metrics != nil {
		for range messages {
			s.metrics.IncSchedulerProcessed(queueName)
		}
	}

	// Log results (handle nil result if processor returned error)
	logFields := map[string]interface{}{
		"queue":  queueName,
		"count":  len(messages),
		"weight": qs.weight,
	}
	if result != nil {
		logFields["success"] = result.SuccessCount()
		logFields["failed"] = result.FailedCount()
	}
	s.log.WithFields(logFields).Debug("Scheduler processed messages")

	return result, err
}

// GetStats returns processing statistics per queue
func (s *PriorityScheduler) GetStats() map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[string]uint64)
	for name, counter := range s.processedByQueue {
		stats[name] = counter.Load()
	}
	return stats
}

// GetWeights returns the current queue weights
func (s *PriorityScheduler) GetWeights() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	weights := make(map[string]int, len(s.weights))
	for k, v := range s.weights {
		weights[k] = v
	}
	return weights
}

// TotalProcessed returns the total number of messages processed
func (s *PriorityScheduler) TotalProcessed() uint64 {
	return s.totalProcessed.Load()
}

// GetCreditStats returns credit-related statistics
func (s *PriorityScheduler) GetCreditStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := map[string]interface{}{
		"credits_granted":   s.creditsGranted.Load(),
		"starvation_events": s.starvationEvents.Load(),
		"credit_multiplier": s.creditMultiplier,
		"refill_period_ms":  s.refillPeriod.Milliseconds(),
	}

	// Add per-queue credit availability
	queueCredits := make(map[string]int)
	for name, qs := range s.queues {
		qs.creditsMu.Lock()
		if qs.credits != nil {
			queueCredits[name] = len(qs.credits)
		}
		qs.creditsMu.Unlock()
	}
	stats["queue_credits_available"] = queueCredits

	return stats
}

// SetWeight sets the weight for a queue (for testing)
func (s *PriorityScheduler) SetWeight(queueName string, weight int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.weights[queueName] = weight
	if qs, ok := s.queues[queueName]; ok {
		oldWeight := qs.weight
		qs.weight = weight
		if oldWeight != weight {
			s.resizeCreditChannel(qs, weight)
		}
	}
}

// SetCreditMultiplier sets the credit multiplier (for testing)
func (s *PriorityScheduler) SetCreditMultiplier(multiplier int) {
	if multiplier > 0 {
		s.creditMultiplier = multiplier
	}
}

// SetRefillPeriod sets the refill period (for testing)
func (s *PriorityScheduler) SetRefillPeriod(period time.Duration) {
	if period > 0 {
		s.refillPeriod = period
	}
}

// SetMaxStarvationAge sets the max starvation age (for testing)
func (s *PriorityScheduler) SetMaxStarvationAge(age time.Duration) {
	if age > 0 {
		s.maxStarvationAge = age
	}
}
