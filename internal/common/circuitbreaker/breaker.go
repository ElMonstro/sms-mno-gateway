package circuitbreaker

import (
	"errors"
	"time"

	"github.com/sony/gobreaker"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// CircuitBreaker wraps gobreaker for MNO-specific circuit breaking
// This addresses EM-148: Circuit breaker for MNO connections
type CircuitBreaker struct {
	breaker *gobreaker.CircuitBreaker
	network domain.Network
}

// Config holds circuit breaker configuration
type Config struct {
	// Name is the circuit breaker name (usually MNO name)
	Name string

	// MaxRequests is the maximum number of requests allowed when half-open
	MaxRequests uint32

	// Interval is the cyclic period of the closed state
	// During this interval, the circuit breaker clears internal counts
	Interval time.Duration

	// Timeout is how long the circuit breaker stays open before transitioning to half-open
	Timeout time.Duration

	// ConsecutiveFailures is the number of consecutive failures before opening
	ConsecutiveFailures uint32

	// FailureRatio is the failure ratio threshold (0.0 to 1.0)
	// If the ratio of failures to total requests exceeds this, the circuit opens
	FailureRatio float64

	// MinRequests is the minimum number of requests before the failure ratio is considered
	MinRequests uint32

	// OnStateChange is called when the circuit breaker state changes
	OnStateChange func(name string, from, to gobreaker.State)
}

// DefaultConfig returns the default circuit breaker configuration
func DefaultConfig(name string) *Config {
	return &Config{
		Name:                name,
		MaxRequests:         3,
		Interval:            60 * time.Second,
		Timeout:             10 * time.Second,
		ConsecutiveFailures: 20,
		FailureRatio:        0.5,
		MinRequests:         50,
	}
}

// New creates a new circuit breaker for an MNO
func New(network domain.Network, cfg *Config) *CircuitBreaker {
	if cfg == nil {
		cfg = DefaultConfig(network.String())
	}

	settings := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Trip if consecutive failures exceed threshold
			if counts.ConsecutiveFailures >= cfg.ConsecutiveFailures {
				return true
			}
			// Trip if failure ratio exceeds threshold (with minimum requests)
			if counts.Requests >= cfg.MinRequests {
				failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
				return failureRatio >= cfg.FailureRatio
			}
			return false
		},
		OnStateChange: cfg.OnStateChange,
	}

	return &CircuitBreaker{
		breaker: gobreaker.NewCircuitBreaker(settings),
		network: network,
	}
}

// Execute runs the given function through the circuit breaker
func (cb *CircuitBreaker) Execute(fn func() (interface{}, error)) (interface{}, error) {
	return cb.breaker.Execute(fn)
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() gobreaker.State {
	return cb.breaker.State()
}

// IsOpen returns true if the circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.breaker.State() == gobreaker.StateOpen
}

// IsClosed returns true if the circuit is closed
func (cb *CircuitBreaker) IsClosed() bool {
	return cb.breaker.State() == gobreaker.StateClosed
}

// IsHalfOpen returns true if the circuit is half-open
func (cb *CircuitBreaker) IsHalfOpen() bool {
	return cb.breaker.State() == gobreaker.StateHalfOpen
}

// Network returns the network this breaker is for
func (cb *CircuitBreaker) Network() domain.Network {
	return cb.network
}

// Counts returns the current counts
func (cb *CircuitBreaker) Counts() gobreaker.Counts {
	return cb.breaker.Counts()
}

// BreakerRegistry manages circuit breakers for all MNOs
type BreakerRegistry struct {
	breakers map[domain.Network]*CircuitBreaker
}

// NewRegistry creates a new circuit breaker registry
func NewRegistry() *BreakerRegistry {
	return &BreakerRegistry{
		breakers: make(map[domain.Network]*CircuitBreaker),
	}
}

// Register adds a circuit breaker to the registry
func (r *BreakerRegistry) Register(network domain.Network, cfg *Config) {
	if cfg == nil {
		cfg = DefaultConfig(network.String())
	}
	r.breakers[network] = New(network, cfg)
}

// Get returns the circuit breaker for a network
func (r *BreakerRegistry) Get(network domain.Network) (*CircuitBreaker, error) {
	cb, ok := r.breakers[network]
	if !ok {
		return nil, errors.New("circuit breaker not found for network: " + network.String())
	}
	return cb, nil
}

// Execute runs a function through the appropriate circuit breaker
func (r *BreakerRegistry) Execute(network domain.Network, fn func() (interface{}, error)) (interface{}, error) {
	cb, err := r.Get(network)
	if err != nil {
		return nil, err
	}
	return cb.Execute(fn)
}

// StateString returns a string representation of the circuit breaker state
func StateString(state gobreaker.State) string {
	switch state {
	case gobreaker.StateClosed:
		return "closed"
	case gobreaker.StateHalfOpen:
		return "half-open"
	case gobreaker.StateOpen:
		return "open"
	default:
		return "unknown"
	}
}
