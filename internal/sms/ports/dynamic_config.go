package ports

import "context"

// DynamicConfigStore provides hot-reloadable configuration backed by Redis.
// Values are cached locally for zero-RTT reads on the hot path and invalidated
// via Redis Pub/Sub whenever a field changes.
type DynamicConfigStore interface {
	// GetInt returns the int value for field in namespace, or defaultVal if unset.
	GetInt(namespace, field string, defaultVal int) int

	// SetInt writes a single int value and notifies all watchers.
	SetInt(ctx context.Context, namespace, field string, value int) error

	// GetAll returns all fields in a namespace directly from Redis.
	GetAll(ctx context.Context, namespace string) (map[string]string, error)

	// SetAll writes multiple fields atomically and notifies watchers.
	SetAll(ctx context.Context, namespace string, values map[string]any) error

	// Watch returns a channel that fires when namespace changes.
	// Callers should re-read via GetInt/GetAll after receiving.
	Watch(ctx context.Context, namespace string) (<-chan struct{}, error)

	// Seed writes default values only if the namespace hash is empty (no-op otherwise).
	Seed(ctx context.Context, namespace string, defaults map[string]any) error

	Close() error
}

// Namespace constants — prevents raw string literals in callers.
const (
	NSRetry                = "retry"
	NSRateLimits           = "rate_limits"
	NSSchedulerPromotional = "scheduler:promotional"
	// NSSchedulerTransactional is intentionally omitted until TransactionalHandler
	// has a dynConfig field and a running watcher. Seeding it without a watcher
	// creates a misleading key in Redis that ops cannot actually hot-reload.
)

// Field constants per namespace.
const (
	// NSRetry fields
	FieldMaxRetriesTransactional = "max_retries_transactional"
	FieldMaxRetriesPromotional   = "max_retries_promotional"

	// NSRateLimits fields
	FieldRateSafaricom = "safaricom"
	FieldRateAirtel    = "airtel"
	FieldRateTelkom    = "telkom"
	FieldRateEquitel   = "equitel"
	FieldRateCM        = "cm"
	FieldRateDefault   = "default"

	// NSScheduler* fields
	FieldCreditMultiplier    = "credit_multiplier"
	FieldRefillPeriodMs      = "refill_period_ms"
	FieldMaxStarvationAgeSec = "max_starvation_age_sec"
)
