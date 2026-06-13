package ports

import (
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// Metrics defines the interface for collecting application metrics
type Metrics interface {
	// Message metrics
	IncMessagesProcessed(network domain.Network, status string)
	ObserveSendLatency(network domain.Network, duration time.Duration)

	// Queue metrics
	SetQueueDepth(queueName string, depth int)
	IncQueuePublished(queueName string)
	IncQueueConsumed(queueName string)

	// Circuit breaker metrics
	IncCircuitBreakerTrips(network domain.Network)
	SetCircuitBreakerState(network domain.Network, state string)

	// Retry metrics
	IncRetries(network domain.Network)
	IncDeadLetters(network domain.Network)

	// Rate limiter metrics
	IncRateLimitHits(network domain.Network)

	// HTTP metrics
	ObserveHTTPRequestDuration(method, path string, statusCode int, duration time.Duration)

	// Health metrics
	SetHealthy(component string, healthy bool)

	// Priority routing metrics (EM-155)
	IncPriorityRouted(messageType, queue string)
	IncTransactionalProcessed(status string)
	SetTransactionalQueueDepth(depth int)
	IncSchedulerProcessed(queue string)
	SetSchedulerWeight(queue string, weight int)
	IncStarvationTriggers(queue string)

	// DLQ migrator metrics
	IncDLQMigratorForwarded(dest string)
	IncDLQMigratorPublishError()
	IncDLQMigratorChannelRestart()

	// SaveToDB DLQ migrator metrics
	IncSaveToDBDLQMigratorForwarded()
	IncSaveToDBDLQMigratorPublishError()
	IncSaveToDBDLQMigratorChannelRestart()
}

// MetricLabels contains common labels for metrics
type MetricLabels struct {
	Network string
	Status  string
	Queue   string
}

// Status constants for metrics
const (
	MetricStatusSuccess   = "success"
	MetricStatusRetryable = "retryable"
	MetricStatusFailed    = "failed"
	MetricStatusTimeout   = "timeout"
	MetricStatusRejected  = "rejected"
)

// Component names for health metrics
const (
	ComponentRabbitMQ      = "rabbitmq"
	ComponentRedis         = "redis"
	ComponentSDP           = "sdp"
	ComponentSMPP          = "smpp"
	ComponentPriorityStore = "priority_store"
	ComponentTransactional = "transactional_handler"
	ComponentScheduler     = "priority_scheduler"
)

// Message type constants for priority routing metrics
const (
	MessageTypeTransactional = "transactional"
	MessageTypePromotional   = "promotional"
)
