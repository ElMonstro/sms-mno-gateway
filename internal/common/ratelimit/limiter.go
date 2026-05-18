package ratelimit

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// Limiter provides per-network rate limiting with separate budgets for main and retry queues.
type Limiter struct {
	main     map[domain.Network]*rate.Limiter // budget for main input queues
	retry    map[domain.Network]*rate.Limiter // reserved budget for retry queues
	mu       sync.RWMutex
	defaults map[domain.Network]int
}

// Config holds rate limiter configuration
type Config struct {
	Safaricom int
	Airtel    int
	Telkom    int
	Equitel   int
	CM        int
	Default   int
}

// RetryConfig holds the reserved retry budget per network.
// BurstFactor allows retry to exceed its reservation when main queues are idle.
// Effective burst cap = RPS * BurstFactor. BurstFactor=1 means strict reservation.
type RetryConfig struct {
	SafaricomSDP  int
	SafaricomSMPP int
	Airtel        int
	Equitel       int
	Telkom        int
	CM            int
	BurstFactor   int
}

// DefaultConfig returns the default rate limit configuration
func DefaultConfig() *Config {
	return &Config{
		Safaricom: 200,
		Airtel:    50,
		Telkom:    100,
		Equitel:   20,
		CM:        20,
		Default:   20,
	}
}

// New creates a new per-network rate limiter with main-only budgets.
// Call WithRetryConfig to add retry budgets.
func New(cfg *Config) *Limiter {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	l := &Limiter{
		main:     make(map[domain.Network]*rate.Limiter),
		retry:    make(map[domain.Network]*rate.Limiter),
		defaults: make(map[domain.Network]int),
	}

	l.setMainLimiter(domain.NetworkSafaricom, cfg.Safaricom)
	l.setMainLimiter(domain.NetworkAirtel, cfg.Airtel)
	l.setMainLimiter(domain.NetworkTelkom, cfg.Telkom)
	l.setMainLimiter(domain.NetworkEquitel, cfg.Equitel)
	l.setMainLimiter(domain.NetworkCM, cfg.CM)
	l.setMainLimiter(domain.NetworkINTNL, cfg.CM)
	l.setMainLimiter(domain.NetworkUnknown, cfg.Default)

	return l
}

// WithRetryConfig attaches reserved retry budgets to the limiter.
// Must be called before the service starts consuming retry queues.
func (l *Limiter) WithRetryConfig(cfg *RetryConfig) *Limiter {
	if cfg == nil {
		return l
	}

	burst := cfg.BurstFactor
	if burst < 1 {
		burst = 1
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	setRetry := func(network domain.Network, rps int) {
		if rps <= 0 {
			rps = 1
		}
		l.retry[network] = rate.NewLimiter(rate.Limit(rps), rps*burst)
	}

	// Safaricom: SDP (promotional) and SMPP (transactional) share the same
	// network key. We reserve the larger of the two values so both paths fit
	// within budget. Fine-grained per-sender splitting is handled by the
	// separate transactional/promotional processor pools.
	safaricomRetry := cfg.SafaricomSDP
	if cfg.SafaricomSMPP > safaricomRetry {
		safaricomRetry = cfg.SafaricomSMPP
	}
	setRetry(domain.NetworkSafaricom, safaricomRetry)
	setRetry(domain.NetworkAirtel, cfg.Airtel)
	setRetry(domain.NetworkTelkom, cfg.Telkom)
	setRetry(domain.NetworkEquitel, cfg.Equitel)
	setRetry(domain.NetworkCM, cfg.CM)
	setRetry(domain.NetworkINTNL, cfg.CM)
	setRetry(domain.NetworkUnknown, 1)

	return l
}

// setMainLimiter creates a rate limiter for the main queue of a network.
func (l *Limiter) setMainLimiter(network domain.Network, ratePerSecond int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.main[network] = rate.NewLimiter(rate.Limit(ratePerSecond), ratePerSecond)
	l.defaults[network] = ratePerSecond
}

// Wait blocks until the main rate limiter allows an event for the network.
// Returns an error if the context is cancelled.
func (l *Limiter) Wait(ctx context.Context, network domain.Network) error {
	limiter := l.getMainLimiter(network)
	return limiter.Wait(ctx)
}

// AllowRetry returns true if an event may happen now for the network on the retry queue.
// This is non-blocking. Returns false if the retry budget is exhausted.
func (l *Limiter) AllowRetry(network domain.Network) bool {
	l.mu.RLock()
	limiter, ok := l.retry[network]
	if !ok {
		limiter = l.retry[domain.NetworkUnknown]
	}
	l.mu.RUnlock()
	return limiter.Allow()
}

// WaitRetry blocks until the retry rate limiter allows an event for the network.
// Returns an error if the context is cancelled.
func (l *Limiter) WaitRetry(ctx context.Context, network domain.Network) error {
	l.mu.RLock()
	limiter, ok := l.retry[network]
	if !ok {
		limiter = l.retry[domain.NetworkUnknown]
	}
	l.mu.RUnlock()
	return limiter.Wait(ctx)
}

// WaitN blocks until the main rate limiter allows n events for the network.
// It consumes n tokens in one call, which is equivalent to calling Wait n times
// but more efficient for batch sends. Callers must ensure n does not exceed the
// burst cap (equal to the per-second rate limit); use min(n, burstCap) if unsure.
func (l *Limiter) WaitN(ctx context.Context, network domain.Network, n int) error {
	limiter := l.getMainLimiter(network)
	return limiter.WaitN(ctx, n)
}

// WaitRetryN is WaitN for the retry budget.
func (l *Limiter) WaitRetryN(ctx context.Context, network domain.Network, n int) error {
	l.mu.RLock()
	limiter, ok := l.retry[network]
	if !ok {
		limiter = l.retry[domain.NetworkUnknown]
	}
	l.mu.RUnlock()
	return limiter.WaitN(ctx, n)
}

// Allow returns true if an event may happen now for the network on the main queue.
// This is non-blocking.
func (l *Limiter) Allow(network domain.Network) bool {
	limiter := l.getMainLimiter(network)
	return limiter.Allow()
}

// Reserve returns a reservation for the network on the main queue.
// The caller must call Cancel() on the reservation if not used.
func (l *Limiter) Reserve(network domain.Network) *rate.Reservation {
	limiter := l.getMainLimiter(network)
	return limiter.Reserve()
}

// getMainLimiter returns the main limiter for a network, defaulting to unknown if not found.
func (l *Limiter) getMainLimiter(network domain.Network) *rate.Limiter {
	l.mu.RLock()
	defer l.mu.RUnlock()

	limiter, ok := l.main[network]
	if !ok {
		return l.main[domain.NetworkUnknown]
	}
	return limiter
}

// SetRate dynamically updates the main rate limit for a network.
func (l *Limiter) SetRate(network domain.Network, ratePerSecond int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if limiter, ok := l.main[network]; ok {
		limiter.SetLimit(rate.Limit(ratePerSecond))
		l.defaults[network] = ratePerSecond
	}
}

// GetRate returns the current main rate limit for a network.
func (l *Limiter) GetRate(network domain.Network) int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if rps, ok := l.defaults[network]; ok {
		return rps
	}
	return l.defaults[domain.NetworkUnknown]
}

// Reset resets all main limiters to their initial state.
func (l *Limiter) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for network, rps := range l.defaults {
		l.main[network] = rate.NewLimiter(rate.Limit(rps), rps)
	}
}

// Tokens returns the number of tokens currently available for a network on the main queue.
func (l *Limiter) Tokens(network domain.Network) float64 {
	limiter := l.getMainLimiter(network)
	return limiter.Tokens()
}

// RetryTokens returns the number of tokens currently available for a network on the retry queue.
func (l *Limiter) RetryTokens(network domain.Network) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if limiter, ok := l.retry[network]; ok {
		return limiter.Tokens()
	}
	if limiter, ok := l.retry[domain.NetworkUnknown]; ok {
		return limiter.Tokens()
	}
	return 0
}
