package redis

import (
	"context"
	"strconv"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

const dynConfigKeyPrefix = "sms:config:"
const dynConfigChannel = "sms:config:__changes__"

// DynamicConfig stores hot-reloadable config in Redis hashes and propagates
// changes to all pods via Pub/Sub. Local cache means zero Redis RTT on the hot path.
type DynamicConfig struct {
	client *goredis.Client
	log    logger.Logger

	mu    sync.RWMutex
	cache map[string]map[string]string // namespace -> field -> value

	cancelsMu sync.Mutex
	cancels   []context.CancelFunc // one per Watch() call, all cancelled by Close()
}

// NewDynamicConfig creates a DynamicConfig and pre-warms the local cache for all
// known namespaces. Cache misses are tolerated — watchers will fill them on first change.
func NewDynamicConfig(client *goredis.Client, log logger.Logger) (*DynamicConfig, error) {
	dc := &DynamicConfig{
		client: client,
		log:    log,
		cache:  make(map[string]map[string]string),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, ns := range []string{
		ports.NSRetry,
		ports.NSRateLimits,
		ports.NSSchedulerPromotional,
	} {
		if err := dc.refreshNamespace(ctx, ns); err != nil {
			dc.log.WithError(err).WithField("namespace", ns).Warn("Failed to pre-warm dynamic config cache")
		}
	}

	return dc, nil
}

// GetInt returns the cached int for (namespace, field), falling back to defaultVal.
// This is the hot path — no Redis call.
func (dc *DynamicConfig) GetInt(namespace, field string, defaultVal int) int {
	dc.mu.RLock()
	ns, ok := dc.cache[namespace]
	if ok {
		if raw, found := ns[field]; found {
			dc.mu.RUnlock()
			if v, err := strconv.Atoi(raw); err == nil {
				return v
			}
		}
	}
	dc.mu.RUnlock()
	return defaultVal
}

// SetInt writes field=value into the namespace hash, updates the local cache,
// and publishes a change notification to all pods.
func (dc *DynamicConfig) SetInt(ctx context.Context, namespace, field string, value int) error {
	key := dynConfigKeyPrefix + namespace
	if err := dc.client.HSet(ctx, key, field, value).Err(); err != nil {
		return err
	}
	dc.mu.Lock()
	if ns, ok := dc.cache[namespace]; ok {
		ns[field] = strconv.Itoa(value)
	}
	dc.mu.Unlock()
	return dc.client.Publish(ctx, dynConfigChannel, namespace).Err()
}

// GetAll fetches all fields for a namespace directly from Redis, bypassing the
// local cache intentionally. Its callers (admin HTTP handlers) need authoritative
// data, not a potentially stale snapshot — the extra Redis RTT is acceptable there
// and is not on the hot message-processing path.
func (dc *DynamicConfig) GetAll(ctx context.Context, namespace string) (map[string]string, error) {
	return dc.client.HGetAll(ctx, dynConfigKeyPrefix+namespace).Result()
}

// SetAll writes multiple fields atomically and notifies watchers.
func (dc *DynamicConfig) SetAll(ctx context.Context, namespace string, values map[string]any) error {
	key := dynConfigKeyPrefix + namespace
	args := make([]any, 0, len(values)*2)
	for k, v := range values {
		args = append(args, k, v)
	}
	if err := dc.client.HSet(ctx, key, args...).Err(); err != nil {
		return err
	}
	_ = dc.refreshNamespace(ctx, namespace)
	return dc.client.Publish(ctx, dynConfigChannel, namespace).Err()
}

// Watch subscribes to the global change channel and forwards notifications for
// the given namespace onto the returned channel. The channel is closed when ctx
// is cancelled or Close() is called.
func (dc *DynamicConfig) Watch(ctx context.Context, namespace string) (<-chan struct{}, error) {
	ch := make(chan struct{}, 1)

	watchCtx, cancel := context.WithCancel(ctx)
	dc.cancelsMu.Lock()
	dc.cancels = append(dc.cancels, cancel)
	dc.cancelsMu.Unlock()

	pubsub := dc.client.Subscribe(watchCtx, dynConfigChannel)
	if _, err := pubsub.Receive(watchCtx); err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer close(ch)
		defer pubsub.Close()

		msgCh := pubsub.Channel()
		for {
			select {
			case <-watchCtx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				if msg.Payload != namespace {
					continue
				}
				_ = dc.refreshNamespace(watchCtx, namespace)
				select {
				case ch <- struct{}{}:
				default:
					// Coalesce rapid changes — the next read will get the latest value.
				}
			}
		}
	}()

	return ch, nil
}

// Seed writes default values into a namespace using HSETNX per field, which is
// atomic at the field level. Unlike the previous EXISTS→HSET pattern, this avoids
// the TOCTOU window where two pods starting simultaneously both see a missing key
// and the second overwrites any operator-set values written in between. Fields that
// already exist in Redis are left untouched; only genuinely absent fields are added.
func (dc *DynamicConfig) Seed(ctx context.Context, namespace string, defaults map[string]any) error {
	key := dynConfigKeyPrefix + namespace
	pipe := dc.client.Pipeline()
	for field, value := range defaults {
		pipe.HSetNX(ctx, key, field, value)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	// Refresh cache so GetInt() reflects any newly written fields immediately.
	return dc.refreshNamespace(ctx, namespace)
}

// Close cancels all active Watch goroutines.
func (dc *DynamicConfig) Close() error {
	dc.cancelsMu.Lock()
	defer dc.cancelsMu.Unlock()
	for _, cancel := range dc.cancels {
		cancel()
	}
	dc.cancels = nil
	return nil
}

// refreshNamespace fetches all fields for namespace from Redis and updates the local cache.
func (dc *DynamicConfig) refreshNamespace(ctx context.Context, namespace string) error {
	result, err := dc.client.HGetAll(ctx, dynConfigKeyPrefix+namespace).Result()
	if err != nil {
		return err
	}
	dc.mu.Lock()
	dc.cache[namespace] = result
	dc.mu.Unlock()
	return nil
}

var _ ports.DynamicConfigStore = (*DynamicConfig)(nil)
