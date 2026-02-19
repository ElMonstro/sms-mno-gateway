package ratelimit

import (
	"context"
	"sync"

	"golang.org/x/time/rate"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// Limiter provides per-network rate limiting
type Limiter struct {
	limiters map[domain.Network]*rate.Limiter
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

// New creates a new per-network rate limiter
func New(cfg *Config) *Limiter {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	l := &Limiter{
		limiters: make(map[domain.Network]*rate.Limiter),
		defaults: make(map[domain.Network]int),
	}

	// Initialize limiters for each network
	l.setLimiter(domain.NetworkSafaricom, cfg.Safaricom)
	l.setLimiter(domain.NetworkAirtel, cfg.Airtel)
	l.setLimiter(domain.NetworkTelkom, cfg.Telkom)
	l.setLimiter(domain.NetworkEquitel, cfg.Equitel)
	l.setLimiter(domain.NetworkCM, cfg.CM)
	l.setLimiter(domain.NetworkINTNL, cfg.CM) // INTNL uses same as CM
	l.setLimiter(domain.NetworkUnknown, cfg.Default)

	return l
}

// setLimiter creates a rate limiter for a network
func (l *Limiter) setLimiter(network domain.Network, ratePerSecond int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.limiters[network] = rate.NewLimiter(rate.Limit(ratePerSecond), 1)
	l.defaults[network] = ratePerSecond
}

// Wait blocks until the rate limiter allows an event for the network
// Returns an error if the context is cancelled
func (l *Limiter) Wait(ctx context.Context, network domain.Network) error {
	limiter := l.getLimiter(network)
	return limiter.Wait(ctx)
}

// Allow returns true if an event may happen now for the network
// This is non-blocking
func (l *Limiter) Allow(network domain.Network) bool {
	limiter := l.getLimiter(network)
	return limiter.Allow()
}

// Reserve returns a reservation for the network
// The caller must call Cancel() on the reservation if not used
func (l *Limiter) Reserve(network domain.Network) *rate.Reservation {
	limiter := l.getLimiter(network)
	return limiter.Reserve()
}

// getLimiter returns the limiter for a network, defaulting to unknown if not found
func (l *Limiter) getLimiter(network domain.Network) *rate.Limiter {
	l.mu.RLock()
	defer l.mu.RUnlock()

	limiter, ok := l.limiters[network]
	if !ok {
		return l.limiters[domain.NetworkUnknown]
	}
	return limiter
}

// SetRate dynamically updates the rate limit for a network
func (l *Limiter) SetRate(network domain.Network, ratePerSecond int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if limiter, ok := l.limiters[network]; ok {
		limiter.SetLimit(rate.Limit(ratePerSecond))
		l.defaults[network] = ratePerSecond
	}
}

// GetRate returns the current rate limit for a network
func (l *Limiter) GetRate(network domain.Network) int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if rps, ok := l.defaults[network]; ok {
		return rps
	}
	return l.defaults[domain.NetworkUnknown]
}

// Reset resets all limiters to their initial state
func (l *Limiter) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for network, rps := range l.defaults {
		l.limiters[network] = rate.NewLimiter(rate.Limit(rps), 1)
	}
}

// Tokens returns the number of tokens currently available for a network
func (l *Limiter) Tokens(network domain.Network) float64 {
	limiter := l.getLimiter(network)
	return limiter.Tokens()
}
