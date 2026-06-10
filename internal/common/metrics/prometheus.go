package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// PrometheusMetrics implements the ports.Metrics interface using Prometheus
// This addresses EM-147: Prometheus metrics
type PrometheusMetrics struct {
	// Message metrics
	messagesProcessed *prometheus.CounterVec
	sendLatency       *prometheus.HistogramVec

	// Queue metrics
	queueDepth     *prometheus.GaugeVec
	queuePublished *prometheus.CounterVec
	queueConsumed  *prometheus.CounterVec

	// Circuit breaker metrics
	circuitBreakerTrips *prometheus.CounterVec
	circuitBreakerState *prometheus.GaugeVec

	// Retry metrics
	retries     *prometheus.CounterVec
	deadLetters *prometheus.CounterVec

	// Rate limiter metrics
	rateLimitHits *prometheus.CounterVec

	// HTTP metrics
	httpRequestDuration *prometheus.HistogramVec

	// Health metrics
	healthStatus *prometheus.GaugeVec

	// Priority routing metrics (EM-155)
	priorityRouted          *prometheus.CounterVec
	transactionalProcessed  *prometheus.CounterVec
	transactionalQueueDepth prometheus.Gauge
	schedulerProcessed      *prometheus.CounterVec
	schedulerWeight         *prometheus.GaugeVec
	starvationTriggers      *prometheus.CounterVec

	// DLQ migrator metrics
	dlqMigratorForwarded       *prometheus.CounterVec
	dlqMigratorPublishErrors   prometheus.Counter
	dlqMigratorChannelRestarts prometheus.Counter
}

// New creates a new Prometheus metrics instance
func New(namespace string) *PrometheusMetrics {
	if namespace == "" {
		namespace = "emalify_sms"
	}

	return &PrometheusMetrics{
		messagesProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "messages_processed_total",
				Help:      "Total number of messages processed by network and status",
			},
			[]string{"network", "status"},
		),
		sendLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "send_latency_seconds",
				Help:      "Latency of sending messages to MNOs in seconds",
				Buckets:   []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"network"},
		),
		queueDepth: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "queue_depth",
				Help:      "Current depth of message queues",
			},
			[]string{"queue"},
		),
		queuePublished: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "queue_published_total",
				Help:      "Total number of messages published to queues",
			},
			[]string{"queue"},
		),
		queueConsumed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "queue_consumed_total",
				Help:      "Total number of messages consumed from queues",
			},
			[]string{"queue"},
		),
		circuitBreakerTrips: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "circuit_breaker_trips_total",
				Help:      "Total number of circuit breaker trips by network",
			},
			[]string{"network"},
		),
		circuitBreakerState: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "circuit_breaker_state",
				Help:      "Current state of circuit breakers (0=closed, 1=half-open, 2=open)",
			},
			[]string{"network"},
		),
		retries: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "retries_total",
				Help:      "Total number of message retries by network",
			},
			[]string{"network"},
		),
		deadLetters: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dead_letters_total",
				Help:      "Total number of messages sent to dead letter queue by network",
			},
			[]string{"network"},
		),
		rateLimitHits: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rate_limit_hits_total",
				Help:      "Total number of rate limit hits by network",
			},
			[]string{"network"},
		),
		httpRequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "http_request_duration_seconds",
				Help:      "Duration of HTTP requests in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method", "path", "status_code"},
		),
		healthStatus: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "health_status",
				Help:      "Health status of components (1=healthy, 0=unhealthy)",
			},
			[]string{"component"},
		),

		// Priority routing metrics (EM-155)
		priorityRouted: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "priority_messages_routed_total",
				Help:      "Total messages routed by message type and queue",
			},
			[]string{"type", "queue"},
		),
		transactionalProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "priority_transactional_processed_total",
				Help:      "Total transactional messages processed by status",
			},
			[]string{"status"},
		),
		transactionalQueueDepth: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "priority_transactional_queue_depth",
				Help:      "Current depth of transactional message queue",
			},
		),
		schedulerProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "priority_scheduler_processed_total",
				Help:      "Total messages processed by scheduler per queue",
			},
			[]string{"queue"},
		),
		schedulerWeight: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "priority_scheduler_weight",
				Help:      "Current weight assigned to each queue",
			},
			[]string{"queue"},
		),
		starvationTriggers: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "priority_starvation_triggers_total",
				Help:      "Number of times starvation prevention was triggered per queue",
			},
			[]string{"queue"},
		),
		dlqMigratorForwarded: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dlq_migrator_forwarded_total",
				Help:      "Total deliveries forwarded by the DLQ migrator, by destination queue",
			},
			[]string{"dest"},
		),
		dlqMigratorPublishErrors: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dlq_migrator_publish_errors_total",
				Help:      "Total publish errors encountered by the DLQ migrator",
			},
		),
		dlqMigratorChannelRestarts: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dlq_migrator_channel_restarts_total",
				Help:      "Total channel restarts due to connection loss in the DLQ migrator",
			},
		),
	}
}

// IncMessagesProcessed increments the messages processed counter
func (m *PrometheusMetrics) IncMessagesProcessed(network domain.Network, status string) {
	m.messagesProcessed.WithLabelValues(network.String(), status).Inc()
}

// ObserveSendLatency records the send latency
func (m *PrometheusMetrics) ObserveSendLatency(network domain.Network, duration time.Duration) {
	m.sendLatency.WithLabelValues(network.String()).Observe(duration.Seconds())
}

// SetQueueDepth sets the current queue depth
func (m *PrometheusMetrics) SetQueueDepth(queueName string, depth int) {
	m.queueDepth.WithLabelValues(queueName).Set(float64(depth))
}

// IncQueuePublished increments the queue published counter
func (m *PrometheusMetrics) IncQueuePublished(queueName string) {
	m.queuePublished.WithLabelValues(queueName).Inc()
}

// IncQueueConsumed increments the queue consumed counter
func (m *PrometheusMetrics) IncQueueConsumed(queueName string) {
	m.queueConsumed.WithLabelValues(queueName).Inc()
}

// IncCircuitBreakerTrips increments the circuit breaker trips counter
func (m *PrometheusMetrics) IncCircuitBreakerTrips(network domain.Network) {
	m.circuitBreakerTrips.WithLabelValues(network.String()).Inc()
}

// SetCircuitBreakerState sets the circuit breaker state gauge
func (m *PrometheusMetrics) SetCircuitBreakerState(network domain.Network, state string) {
	var stateValue float64
	switch state {
	case "closed":
		stateValue = 0
	case "half-open":
		stateValue = 1
	case "open":
		stateValue = 2
	}
	m.circuitBreakerState.WithLabelValues(network.String()).Set(stateValue)
}

// IncRetries increments the retries counter
func (m *PrometheusMetrics) IncRetries(network domain.Network) {
	m.retries.WithLabelValues(network.String()).Inc()
}

// IncDeadLetters increments the dead letters counter
func (m *PrometheusMetrics) IncDeadLetters(network domain.Network) {
	m.deadLetters.WithLabelValues(network.String()).Inc()
}

// IncRateLimitHits increments the rate limit hits counter
func (m *PrometheusMetrics) IncRateLimitHits(network domain.Network) {
	m.rateLimitHits.WithLabelValues(network.String()).Inc()
}

// ObserveHTTPRequestDuration records HTTP request duration
func (m *PrometheusMetrics) ObserveHTTPRequestDuration(method, path string, statusCode int, duration time.Duration) {
	m.httpRequestDuration.WithLabelValues(method, path, string(rune(statusCode))).Observe(duration.Seconds())
}

// SetHealthy sets the health status for a component
func (m *PrometheusMetrics) SetHealthy(component string, healthy bool) {
	var value float64
	if healthy {
		value = 1
	}
	m.healthStatus.WithLabelValues(component).Set(value)
}

// Priority routing metrics (EM-155)

// IncPriorityRouted increments the priority routed counter
func (m *PrometheusMetrics) IncPriorityRouted(messageType, queue string) {
	m.priorityRouted.WithLabelValues(messageType, queue).Inc()
}

// IncTransactionalProcessed increments the transactional processed counter
func (m *PrometheusMetrics) IncTransactionalProcessed(status string) {
	m.transactionalProcessed.WithLabelValues(status).Inc()
}

// SetTransactionalQueueDepth sets the transactional queue depth
func (m *PrometheusMetrics) SetTransactionalQueueDepth(depth int) {
	m.transactionalQueueDepth.Set(float64(depth))
}

// IncSchedulerProcessed increments the scheduler processed counter
func (m *PrometheusMetrics) IncSchedulerProcessed(queue string) {
	m.schedulerProcessed.WithLabelValues(queue).Inc()
}

// SetSchedulerWeight sets the scheduler weight for a queue
func (m *PrometheusMetrics) SetSchedulerWeight(queue string, weight int) {
	m.schedulerWeight.WithLabelValues(queue).Set(float64(weight))
}

// IncStarvationTriggers increments the starvation triggers counter
func (m *PrometheusMetrics) IncStarvationTriggers(queue string) {
	m.starvationTriggers.WithLabelValues(queue).Inc()
}

// IncDLQMigratorForwarded increments the DLQ migrator forwarded counter for the given destination
func (m *PrometheusMetrics) IncDLQMigratorForwarded(dest string) {
	m.dlqMigratorForwarded.WithLabelValues(dest).Inc()
}

// IncDLQMigratorPublishError increments the DLQ migrator publish error counter
func (m *PrometheusMetrics) IncDLQMigratorPublishError() {
	m.dlqMigratorPublishErrors.Inc()
}

// IncDLQMigratorChannelRestart increments the DLQ migrator channel restart counter
func (m *PrometheusMetrics) IncDLQMigratorChannelRestart() {
	m.dlqMigratorChannelRestarts.Inc()
}

// Ensure PrometheusMetrics implements ports.Metrics
var _ ports.Metrics = (*PrometheusMetrics)(nil)

// NoopMetrics is a no-op implementation for testing
type NoopMetrics struct{}

func (m *NoopMetrics) IncMessagesProcessed(network domain.Network, status string)        {}
func (m *NoopMetrics) ObserveSendLatency(network domain.Network, duration time.Duration) {}
func (m *NoopMetrics) SetQueueDepth(queueName string, depth int)                         {}
func (m *NoopMetrics) IncQueuePublished(queueName string)                                {}
func (m *NoopMetrics) IncQueueConsumed(queueName string)                                 {}
func (m *NoopMetrics) IncCircuitBreakerTrips(network domain.Network)                     {}
func (m *NoopMetrics) SetCircuitBreakerState(network domain.Network, state string)       {}
func (m *NoopMetrics) IncRetries(network domain.Network)                                 {}
func (m *NoopMetrics) IncDeadLetters(network domain.Network)                             {}
func (m *NoopMetrics) IncRateLimitHits(network domain.Network)                           {}
func (m *NoopMetrics) ObserveHTTPRequestDuration(method, path string, statusCode int, duration time.Duration) {
}
func (m *NoopMetrics) SetHealthy(component string, healthy bool)   {}
func (m *NoopMetrics) IncPriorityRouted(messageType, queue string) {}
func (m *NoopMetrics) IncTransactionalProcessed(status string)     {}
func (m *NoopMetrics) SetTransactionalQueueDepth(depth int)        {}
func (m *NoopMetrics) IncSchedulerProcessed(queue string)          {}
func (m *NoopMetrics) SetSchedulerWeight(queue string, weight int) {}
func (m *NoopMetrics) IncStarvationTriggers(queue string)     {}
func (m *NoopMetrics) IncDLQMigratorForwarded(dest string)    {}
func (m *NoopMetrics) IncDLQMigratorPublishError()            {}
func (m *NoopMetrics) IncDLQMigratorChannelRestart()          {}

var _ ports.Metrics = (*NoopMetrics)(nil)
