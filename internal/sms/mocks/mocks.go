package mocks

import (
	"context"
	"sync"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// MockMNOSender is a mock implementation of ports.MNOSender
type MockMNOSender struct {
	mu           sync.Mutex
	name         string
	network      domain.Network
	healthy      bool
	SendFunc     func(ctx context.Context, msg *domain.Message) *domain.SendResult
	SentMessages []*domain.Message
}

func NewMockMNOSender(name string, network domain.Network) *MockMNOSender {
	return &MockMNOSender{
		name:         name,
		network:      network,
		healthy:      true,
		SentMessages: make([]*domain.Message, 0),
	}
}

func (m *MockMNOSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	m.mu.Lock()
	m.SentMessages = append(m.SentMessages, msg)
	m.mu.Unlock()

	if m.SendFunc != nil {
		return m.SendFunc(ctx, msg)
	}

	// Default: return success
	return domain.NewSuccessResult(msg, "mock-response", 10*time.Millisecond)
}

func (m *MockMNOSender) Name() string {
	return m.name
}

func (m *MockMNOSender) Network() domain.Network {
	return m.network
}

func (m *MockMNOSender) IsHealthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthy
}

func (m *MockMNOSender) SetHealthy(healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = healthy
}

func (m *MockMNOSender) GetSentMessages() []*domain.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SentMessages
}

// MockBatchSender implements both ports.MNOSender and ports.BatchSender.
// Use it to test the SDP promotional batch path in the processor.
type MockBatchSender struct {
	mu            sync.Mutex
	name          string
	network       domain.Network
	healthy       bool
	SendCalls     []*domain.Message
	BatchCalls    []int               // size of each SendBatch call
	BatchMessages [][]*domain.Message // messages passed in each SendBatch call
	SendBatchFunc func(ctx context.Context, msgs []*domain.Message) []*domain.SendResult
}

func NewMockBatchSender(name string, network domain.Network) *MockBatchSender {
	return &MockBatchSender{name: name, network: network, healthy: true}
}

func (m *MockBatchSender) Send(ctx context.Context, msg *domain.Message) *domain.SendResult {
	m.mu.Lock()
	m.SendCalls = append(m.SendCalls, msg)
	m.mu.Unlock()
	return domain.NewSuccessResult(msg, "mock-batch-send", 10*time.Millisecond)
}

func (m *MockBatchSender) Name() string             { return m.name }
func (m *MockBatchSender) Network() domain.Network  { return m.network }
func (m *MockBatchSender) IsHealthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthy
}

func (m *MockBatchSender) SendBatch(ctx context.Context, msgs []*domain.Message) []*domain.SendResult {
	m.mu.Lock()
	m.BatchCalls = append(m.BatchCalls, len(msgs))
	batch := make([]*domain.Message, len(msgs))
	copy(batch, msgs)
	m.BatchMessages = append(m.BatchMessages, batch)
	m.mu.Unlock()

	if m.SendBatchFunc != nil {
		return m.SendBatchFunc(ctx, msgs)
	}
	results := make([]*domain.SendResult, len(msgs))
	for i, msg := range msgs {
		results[i] = domain.NewSuccessResult(msg, "mock-batch", 10*time.Millisecond)
	}
	return results
}

func (m *MockBatchSender) GetBatchCalls() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.BatchCalls...)
}

func (m *MockBatchSender) GetBatchMessages() [][]*domain.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]*domain.Message, len(m.BatchMessages))
	for i, batch := range m.BatchMessages {
		result[i] = append([]*domain.Message(nil), batch...)
	}
	return result
}

func (m *MockBatchSender) GetSendCalls() []*domain.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*domain.Message(nil), m.SendCalls...)
}

// MockMNOSenderFactory is a mock implementation of ports.MNOSenderFactory
type MockMNOSenderFactory struct {
	mu      sync.Mutex
	senders map[domain.Network]ports.MNOSender
}

func NewMockMNOSenderFactory() *MockMNOSenderFactory {
	return &MockMNOSenderFactory{
		senders: make(map[domain.Network]ports.MNOSender),
	}
}

func (f *MockMNOSenderFactory) RegisterSender(sender *MockMNOSender) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.senders[sender.network] = sender
}

func (f *MockMNOSenderFactory) RegisterBatchSender(sender *MockBatchSender) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.senders[sender.network] = sender
}

func (f *MockMNOSenderFactory) GetSender(msg *domain.Message) (ports.MNOSender, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	network := msg.Network()
	if sender, ok := f.senders[network]; ok {
		return sender, nil
	}
	return nil, domain.ErrUnknownNetwork
}

func (f *MockMNOSenderFactory) GetSenderByNetwork(network domain.Network) (ports.MNOSender, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sender, ok := f.senders[network]; ok {
		return sender, nil
	}
	return nil, domain.ErrUnknownNetwork
}

func (f *MockMNOSenderFactory) ListSenders() []ports.MNOSender {
	f.mu.Lock()
	defer f.mu.Unlock()
	senders := make([]ports.MNOSender, 0, len(f.senders))
	for _, s := range f.senders {
		senders = append(senders, s)
	}
	return senders
}

// MockQueuePublisher is a mock implementation of ports.QueuePublisher
type MockQueuePublisher struct {
	mu               sync.Mutex
	connected        bool
	PublishedItems   []*PublishedItem
	PublishFunc      func(ctx context.Context, result *domain.SendResult) error
	PublishErr       error
	GatewayQueueName string
}

type PublishedItem struct {
	Result    *domain.SendResult
	Message   *domain.Message
	QueueName string
	QueueType string
}

func NewMockQueuePublisher() *MockQueuePublisher {
	return &MockQueuePublisher{
		connected:      true,
		PublishedItems: make([]*PublishedItem, 0),
	}
}

func (p *MockQueuePublisher) Publish(ctx context.Context, queueName string, msg *domain.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.PublishErr != nil {
		return p.PublishErr
	}
	p.PublishedItems = append(p.PublishedItems, &PublishedItem{
		Message:   msg,
		QueueName: queueName,
	})
	return nil
}

func (p *MockQueuePublisher) PublishBatch(ctx context.Context, queueName string, msgs []*domain.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, msg := range msgs {
		p.PublishedItems = append(p.PublishedItems, &PublishedItem{
			Message:   msg,
			QueueName: queueName,
		})
	}
	return nil
}

func (p *MockQueuePublisher) PublishResult(ctx context.Context, result *domain.SendResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	queueType := "save_to_db"
	switch result.Type {
	case domain.ResultRetryable:
		// Mirror real publisher: route to type-specific delay queue
		if result.Message.IsPromotional(p.GatewayQueueName) {
			queueType = "promotional_delay"
		} else {
			queueType = "transactional_delay"
		}
	case domain.ResultPermanent:
		queueType = "dlq"
	}

	p.PublishedItems = append(p.PublishedItems, &PublishedItem{
		Result:    result,
		QueueType: queueType,
	})

	if p.PublishFunc != nil {
		return p.PublishFunc(ctx, result)
	}
	return nil
}

func (p *MockQueuePublisher) PublishBatchResults(ctx context.Context, batch *domain.BatchResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, r := range batch.Successful {
		p.PublishedItems = append(p.PublishedItems, &PublishedItem{
			Result:    r,
			QueueType: "save_to_db",
		})
	}

	for _, r := range batch.Retryable {
		queueType := "transactional_delay"
		if r.Message.IsPromotional(p.GatewayQueueName) {
			queueType = "promotional_delay"
		}
		p.PublishedItems = append(p.PublishedItems, &PublishedItem{
			Result:    r,
			QueueType: queueType,
		})
	}

	for _, r := range batch.Failed {
		p.PublishedItems = append(p.PublishedItems, &PublishedItem{
			Result:    r,
			QueueType: "dlq",
		})
	}

	return nil
}

func (p *MockQueuePublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connected = false
	return nil
}

func (p *MockQueuePublisher) IsConnected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connected
}

func (p *MockQueuePublisher) GetPublishedItems() []*PublishedItem {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.PublishedItems
}

func (p *MockQueuePublisher) CountByQueueType(queueType string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, item := range p.PublishedItems {
		if item.QueueType == queueType {
			count++
		}
	}
	return count
}

// MockMetrics is a mock implementation of ports.Metrics
type MockMetrics struct {
	mu                  sync.Mutex
	ProcessedCounts     map[string]int
	RetryCounts         map[domain.Network]int
	DeadLetterCounts    map[domain.Network]int
	LatencyObservations []LatencyObservation
	CircuitBreakerState map[domain.Network]string
	CircuitBreakerTrips map[domain.Network]int
}

type LatencyObservation struct {
	Network domain.Network
	Latency time.Duration
}

func NewMockMetrics() *MockMetrics {
	return &MockMetrics{
		ProcessedCounts:     make(map[string]int),
		RetryCounts:         make(map[domain.Network]int),
		DeadLetterCounts:    make(map[domain.Network]int),
		LatencyObservations: make([]LatencyObservation, 0),
		CircuitBreakerState: make(map[domain.Network]string),
		CircuitBreakerTrips: make(map[domain.Network]int),
	}
}

func (m *MockMetrics) IncMessagesProcessed(network domain.Network, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := network.String() + "_" + status
	m.ProcessedCounts[key]++
}

func (m *MockMetrics) ObserveSendLatency(network domain.Network, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LatencyObservations = append(m.LatencyObservations, LatencyObservation{
		Network: network,
		Latency: latency,
	})
}

func (m *MockMetrics) IncRetries(network domain.Network) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RetryCounts[network]++
}

func (m *MockMetrics) IncDeadLetters(network domain.Network) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeadLetterCounts[network]++
}

func (m *MockMetrics) SetCircuitBreakerState(network domain.Network, state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CircuitBreakerState[network] = state
}

func (m *MockMetrics) IncCircuitBreakerTrips(network domain.Network) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CircuitBreakerTrips[network]++
}

func (m *MockMetrics) GetProcessedCount(network domain.Network, status string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := network.String() + "_" + status
	return m.ProcessedCounts[key]
}

// Additional methods to satisfy ports.Metrics interface
func (m *MockMetrics) SetQueueDepth(queueName string, depth int) {}
func (m *MockMetrics) IncQueuePublished(queueName string)        {}
func (m *MockMetrics) IncQueueConsumed(queueName string)         {}
func (m *MockMetrics) IncRateLimitHits(network domain.Network)   {}
func (m *MockMetrics) ObserveHTTPRequestDuration(method, path string, statusCode int, d time.Duration) {
}
func (m *MockMetrics) SetHealthy(component string, healthy bool) {}

func (m *MockMetrics) IncPriorityRouted(messageType, queue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := "priority_routed_" + messageType + "_" + queue
	m.ProcessedCounts[key]++
}

func (m *MockMetrics) IncTransactionalProcessed(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := "transactional_" + status
	m.ProcessedCounts[key]++
}

func (m *MockMetrics) SetTransactionalQueueDepth(depth int) {}

func (m *MockMetrics) IncSchedulerProcessed(queue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := "scheduler_" + queue
	m.ProcessedCounts[key]++
}

func (m *MockMetrics) SetSchedulerWeight(queue string, weight int) {}

func (m *MockMetrics) IncStarvationTriggers(queue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := "starvation_" + queue
	m.ProcessedCounts[key]++
}

func (m *MockMetrics) IncDLQMigratorForwarded(dest string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessedCounts["dlq_forwarded_"+dest]++
}

func (m *MockMetrics) IncDLQMigratorPublishError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessedCounts["dlq_publish_errors"]++
}

func (m *MockMetrics) IncDLQMigratorChannelRestart() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessedCounts["dlq_channel_restarts"]++
}

func (m *MockMetrics) IncSaveToDBDLQMigratorForwarded()      {}
func (m *MockMetrics) IncSaveToDBDLQMigratorPublishError()   {}
func (m *MockMetrics) IncSaveToDBDLQMigratorChannelRestart() {}

// MockDelivery is a mock implementation of ports.Delivery
type MockDelivery struct {
	Data       []byte
	MockMsgs   []*domain.Message
	Tag        uint64
	AckCalled  bool
	NackParam  bool
	NackCalled bool
}

func NewMockDelivery(data []byte) *MockDelivery {
	return &MockDelivery{
		Data: data,
		Tag:  1,
	}
}

// NewMockDeliveryWithMessages creates a mock delivery with pre-parsed messages
func NewMockDeliveryWithMessages(msgs []*domain.Message) *MockDelivery {
	return &MockDelivery{
		MockMsgs: msgs,
		Tag:      1,
	}
}

func (d *MockDelivery) Body() []byte {
	return d.Data
}

func (d *MockDelivery) Messages() []*domain.Message {
	return d.MockMsgs
}

func (d *MockDelivery) DeliveryTag() uint64 {
	return d.Tag
}

func (d *MockDelivery) Ack() error {
	d.AckCalled = true
	return nil
}

func (d *MockDelivery) Nack(requeue bool) error {
	d.NackCalled = true
	d.NackParam = requeue
	return nil
}

// MockResultReporter is a mock implementation of ports.ResultReporter
type MockResultReporter struct {
	mu          sync.Mutex
	ReportFunc  func(ctx context.Context, result *domain.SendResult) error
	ReportCalls []*domain.SendResult
}

func NewMockResultReporter() *MockResultReporter {
	return &MockResultReporter{}
}

func (r *MockResultReporter) Report(ctx context.Context, result *domain.SendResult) error {
	r.mu.Lock()
	r.ReportCalls = append(r.ReportCalls, result)
	r.mu.Unlock()

	if r.ReportFunc != nil {
		return r.ReportFunc(ctx, result)
	}
	return nil
}

func (r *MockResultReporter) GetReportCalls() []*domain.SendResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*domain.SendResult(nil), r.ReportCalls...)
}

// MockRateLimiter is a mock rate limiter for testing
type MockRateLimiter struct {
	AllowFunc func(network domain.Network) bool
}

func NewMockRateLimiter() *MockRateLimiter {
	return &MockRateLimiter{
		AllowFunc: func(network domain.Network) bool { return true },
	}
}

func (r *MockRateLimiter) Allow(network domain.Network) bool {
	if r.AllowFunc != nil {
		return r.AllowFunc(network)
	}
	return true
}

func (r *MockRateLimiter) SetLimit(network domain.Network, limit int) {}
