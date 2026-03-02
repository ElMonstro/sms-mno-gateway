package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// PriorityScheduler implements Weighted Fair Queuing for promotional messages
// Higher weight queues get more processing slots proportional to their weight
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

	// WFQ state - virtual time based scheduling
	virtualTime float64

	// Starvation prevention
	starvationRatio float64

	// Stats
	processedByQueue map[string]*atomic.Uint64
	totalProcessed   atomic.Uint64

	// Lifecycle
	ctx      context.Context
	cancelFn context.CancelFunc
	wg       sync.WaitGroup
}

// queueState tracks per-queue scheduling state
type queueState struct {
	name          string
	weight        int
	deliveryChan  <-chan ports.Delivery
	virtualFinish float64
	lastProcessed time.Time
	pendingCount  atomic.Int64
}

// PrioritySchedulerConfig holds configuration for the scheduler
type PrioritySchedulerConfig struct {
	PriorityStore             ports.PriorityStore
	Processor                 *Processor
	Metrics                   ports.Metrics
	DefaultWeight             int
	StarvationPreventionRatio float64
	Logger                    logger.Logger
}

// NewPriorityScheduler creates a new priority scheduler
func NewPriorityScheduler(cfg *PrioritySchedulerConfig) *PriorityScheduler {
	ctx, cancel := context.WithCancel(context.Background())

	if cfg.DefaultWeight <= 0 {
		cfg.DefaultWeight = 1
	}
	if cfg.StarvationPreventionRatio <= 0 {
		cfg.StarvationPreventionRatio = 0.1 // 10% minimum for lowest priority
	}

	scheduler := &PriorityScheduler{
		priorityStore:    cfg.PriorityStore,
		processor:        cfg.Processor,
		metrics:          cfg.Metrics,
		log:              cfg.Logger,
		queues:           make(map[string]*queueState),
		weights:          make(map[string]int),
		defaultWeight:    cfg.DefaultWeight,
		starvationRatio:  cfg.StarvationPreventionRatio,
		processedByQueue: make(map[string]*atomic.Uint64),
		ctx:              ctx,
		cancelFn:         cancel,
	}

	return scheduler
}

// RegisterQueue adds a queue to the scheduler with its delivery channel
func (s *PriorityScheduler) RegisterQueue(name string, deliveries <-chan ports.Delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()

	weight := s.defaultWeight
	if w, ok := s.weights[name]; ok {
		weight = w
	}

	s.queues[name] = &queueState{
		name:          name,
		weight:        weight,
		deliveryChan:  deliveries,
		lastProcessed: time.Now(),
	}
	s.processedByQueue[name] = &atomic.Uint64{}

	s.log.WithFields(map[string]interface{}{
		"queue":  name,
		"weight": weight,
	}).Info("Registered queue with priority scheduler")
}

// Start starts the scheduler and weight watcher
func (s *PriorityScheduler) Start() error {
	s.log.Info("Starting priority scheduler")

	// Load initial weights
	if err := s.loadWeights(); err != nil {
		s.log.WithError(err).Warn("Failed to load initial weights, using defaults")
	}

	// Start weight watcher for hot-reload
	if s.priorityStore != nil {
		s.wg.Add(1)
		go s.watchWeights()
	}

	// Start scheduler goroutines for each registered queue
	s.mu.RLock()
	for name, qs := range s.queues {
		s.wg.Add(1)
		go s.processQueue(name, qs)
	}
	s.mu.RUnlock()

	return nil
}

// Stop gracefully stops the scheduler
func (s *PriorityScheduler) Stop() {
	s.log.Info("Stopping priority scheduler")
	s.cancelFn()
	s.wg.Wait()

	s.log.WithFields(map[string]interface{}{
		"total_processed": s.totalProcessed.Load(),
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

// updateWeights updates the internal weights and queue states
func (s *PriorityScheduler) updateWeights(weights map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.weights = weights

	// Update queue states and metrics
	for name, qs := range s.queues {
		if weight, ok := weights[name]; ok {
			qs.weight = weight
		} else {
			qs.weight = s.defaultWeight
		}
		// Emit weight metric
		if s.metrics != nil {
			s.metrics.SetSchedulerWeight(name, qs.weight)
		}
	}

	s.log.WithField("weights", weights).Info("Priority weights updated")
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

// processQueue processes deliveries from a single queue with WFQ scheduling
func (s *PriorityScheduler) processQueue(name string, qs *queueState) {
	defer s.wg.Done()

	s.log.WithField("queue", name).Info("Started processing queue")

	for {
		select {
		case <-s.ctx.Done():
			s.log.WithField("queue", name).Info("Queue processor stopping")
			return

		case delivery, ok := <-qs.deliveryChan:
			if !ok {
				s.log.WithField("queue", name).Info("Queue channel closed")
				return
			}

			// Wait for scheduling slot based on WFQ
			s.waitForSlot(qs)

			// Process the delivery
			if err := s.processor.ProcessDelivery(s.ctx, delivery); err != nil {
				s.log.WithError(err).WithField("queue", name).Error("Failed to process delivery")
			}

			// Update stats
			s.processedByQueue[name].Add(1)
			s.totalProcessed.Add(1)
			qs.lastProcessed = time.Now()

			// Emit metrics
			if s.metrics != nil {
				s.metrics.IncSchedulerProcessed(name)
			}

			// Update virtual time for WFQ
			s.updateVirtualTime(qs)
		}
	}
}

// waitForSlot implements WFQ scheduling - higher weight queues wait less
func (s *PriorityScheduler) waitForSlot(qs *queueState) {
	s.mu.RLock()
	weight := qs.weight
	s.mu.RUnlock()

	if weight <= 0 {
		weight = s.defaultWeight
	}

	// Calculate wait time inversely proportional to weight
	// Higher weight = shorter wait
	// Base wait time is 10ms for weight 1
	baseWait := 10 * time.Millisecond
	waitTime := time.Duration(float64(baseWait) / float64(weight))

	// Apply starvation prevention - ensure minimum processing rate
	// Check if this queue has been starved
	timeSinceLastProcess := time.Since(qs.lastProcessed)
	maxStarvationTime := time.Duration(float64(time.Second) / s.starvationRatio)

	if timeSinceLastProcess > maxStarvationTime {
		// This queue is starving, skip wait
		s.log.WithFields(map[string]interface{}{
			"queue":         qs.name,
			"time_since_ms": timeSinceLastProcess.Milliseconds(),
			"max_starve_ms": maxStarvationTime.Milliseconds(),
		}).Debug("Starvation prevention triggered")
		if s.metrics != nil {
			s.metrics.IncStarvationTriggers(qs.name)
		}
		return
	}

	// Wait with context awareness
	select {
	case <-s.ctx.Done():
		return
	case <-time.After(waitTime):
		return
	}
}

// updateVirtualTime updates the WFQ virtual time based on processed packet
func (s *PriorityScheduler) updateVirtualTime(qs *queueState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// WFQ: virtual_finish = virtual_time + (packet_size / weight)
	// Since all messages are equal size (1), formula simplifies
	if qs.weight > 0 {
		qs.virtualFinish = s.virtualTime + (1.0 / float64(qs.weight))
	} else {
		qs.virtualFinish = s.virtualTime + 1.0
	}
	s.virtualTime = qs.virtualFinish
}

// GetStats returns processing statistics per queue
func (s *PriorityScheduler) GetStats() map[string]uint64 {
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

// SetWeight sets the weight for a queue (for testing)
func (s *PriorityScheduler) SetWeight(queueName string, weight int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.weights[queueName] = weight
	if qs, ok := s.queues[queueName]; ok {
		qs.weight = weight
	}
}
