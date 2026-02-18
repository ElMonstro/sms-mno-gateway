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
func (m *NoopMetrics) SetHealthy(component string, healthy bool) {}

var _ ports.Metrics = (*NoopMetrics)(nil)
