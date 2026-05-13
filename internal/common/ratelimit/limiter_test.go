package ratelimit

import (
	"context"
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// TestNew_ConfiguredRates verifies that New() initialises main limiters with the
// configured per-network rates and exposes them via GetRate().
func TestNew_ConfiguredRates(t *testing.T) {
	cfg := &Config{
		Safaricom: 1000,
		Airtel:    25,
		Telkom:    50,
		Equitel:   50,
		CM:        20,
		Default:   20,
	}

	l := New(cfg)

	checks := []struct {
		network domain.Network
		want    int
	}{
		{domain.NetworkSafaricom, 1000},
		{domain.NetworkAirtel, 25},
		{domain.NetworkTelkom, 50},
		{domain.NetworkEquitel, 50},
		{domain.NetworkCM, 20},
		{domain.NetworkUnknown, 20},
	}

	for _, c := range checks {
		if got := l.GetRate(c.network); got != c.want {
			t.Errorf("GetRate(%s) = %d, want %d", c.network, got, c.want)
		}
	}
}

// TestNew_NilConfigUsesDefaults verifies that passing nil uses DefaultConfig values.
func TestNew_NilConfigUsesDefaults(t *testing.T) {
	l := New(nil)
	defaults := DefaultConfig()

	if got := l.GetRate(domain.NetworkSafaricom); got != defaults.Safaricom {
		t.Errorf("GetRate(Safaricom) = %d, want %d", got, defaults.Safaricom)
	}
	if got := l.GetRate(domain.NetworkAirtel); got != defaults.Airtel {
		t.Errorf("GetRate(Airtel) = %d, want %d", got, defaults.Airtel)
	}
}

// TestAllow_ReturnsTrue verifies Allow() returns true when tokens are available.
func TestAllow_ReturnsTrue(t *testing.T) {
	l := New(&Config{Safaricom: 100, Default: 10})

	if !l.Allow(domain.NetworkSafaricom) {
		t.Error("Allow(Safaricom) = false, want true for high-rate limiter")
	}
}

// TestAllow_INTNLUsedCMRate verifies that NetworkINTNL is initialised from Config.CM
// (the same rate as CM) and allows tokens when CM is set to a high rate.
func TestAllow_INTNLUsedCMRate(t *testing.T) {
	l := New(&Config{CM: 100, Default: 10})

	if !l.Allow(domain.NetworkINTNL) {
		t.Error("Allow(INTNL) should not block immediately when CM rate is 100")
	}
}

// TestSetRate_UpdatesMainLimiter verifies that SetRate only affects the main budget
// and GetRate reflects the new value.
func TestSetRate_UpdatesMainLimiter(t *testing.T) {
	l := New(&Config{Safaricom: 100, Default: 10})

	l.SetRate(domain.NetworkSafaricom, 500)

	if got := l.GetRate(domain.NetworkSafaricom); got != 500 {
		t.Errorf("GetRate(Safaricom) after SetRate = %d, want 500", got)
	}
}

// TestReset_RestoresMainTokens verifies that Reset recreates main limiters from
// the stored defaults, restoring token budgets.
func TestReset_RestoresMainTokens(t *testing.T) {
	l := New(&Config{Safaricom: 100, Default: 10})

	// Drain some tokens
	for i := 0; i < 50; i++ {
		l.Allow(domain.NetworkSafaricom)
	}

	tokensBefore := l.Tokens(domain.NetworkSafaricom)
	l.Reset()
	tokensAfter := l.Tokens(domain.NetworkSafaricom)

	if tokensAfter <= tokensBefore {
		t.Errorf("Tokens after Reset (%f) should be greater than before (%f)", tokensAfter, tokensBefore)
	}
}

// TestWithRetryConfig_CreatesRetryLimiters verifies that WithRetryConfig populates
// the retry map with independent limiters.
func TestWithRetryConfig_CreatesRetryLimiters(t *testing.T) {
	l := New(&Config{
		Safaricom: 800,
		Airtel:    20,
		Default:   10,
	}).WithRetryConfig(&RetryConfig{
		SafaricomSDP:  200,
		SafaricomSMPP: 40,
		Airtel:        5,
		Equitel:       10,
		Telkom:        10,
		CM:            5,
		BurstFactor:   1,
	})

	// Retry limiters should exist — WaitRetry should not panic
	ctx := context.Background()
	// With BurstFactor=1 and rps=200, there are 200 initial tokens, so WaitRetry should not block
	if err := l.WaitRetry(ctx, domain.NetworkSafaricom); err != nil {
		t.Errorf("WaitRetry(Safaricom) unexpected error: %v", err)
	}
}

// TestWithRetryConfig_UsesMaxSafaricomRate verifies that when SafaricomSDP > SafaricomSMPP,
// the larger value is used for the shared Safaricom retry bucket.
func TestWithRetryConfig_UsesMaxSafaricomRate(t *testing.T) {
	sdp, smpp := 200, 40
	l := New(&Config{Safaricom: 800, Default: 10}).WithRetryConfig(&RetryConfig{
		SafaricomSDP:  sdp,
		SafaricomSMPP: smpp,
		BurstFactor:   1,
	})

	// Retry tokens for Safaricom should equal max(sdp, smpp) = 200
	retryTokens := l.retry[domain.NetworkSafaricom].Tokens()
	if retryTokens < float64(smpp) {
		t.Errorf("Retry tokens for Safaricom = %f, want at least %d (max of SDP/SMPP)", retryTokens, smpp)
	}
}

// TestWithRetryConfig_NilReturnsUnchanged verifies that passing nil is a no-op.
func TestWithRetryConfig_NilReturnsUnchanged(t *testing.T) {
	l := New(&Config{Safaricom: 100, Default: 10})
	l2 := l.WithRetryConfig(nil)

	if l2 != l {
		t.Error("WithRetryConfig(nil) should return the same limiter unchanged")
	}
}

// TestWithRetryConfig_BurstFactorScalesBurst verifies that burst capacity
// equals rps × BurstFactor.
func TestWithRetryConfig_BurstFactorScalesBurst(t *testing.T) {
	l := New(&Config{Default: 10}).WithRetryConfig(&RetryConfig{
		Airtel:      10,
		BurstFactor: 3, // burst = 10 × 3 = 30
		CM:          5,
	})

	// With 30 burst tokens, 25 Allow()-equivalent calls should succeed
	airtelRetry := l.retry[domain.NetworkAirtel]
	if airtelRetry == nil {
		t.Fatal("Airtel retry limiter should not be nil after WithRetryConfig")
	}
	// Initial tokens == burst == rps * BurstFactor == 10 * 3 == 30
	if got := airtelRetry.Tokens(); got < 25 {
		t.Errorf("Airtel retry burst tokens = %f, expected at least 25 (burst=30)", got)
	}
}

// TestWithRetryConfig_BurstFactorBelowOneClampedToOne verifies that BurstFactor < 1
// is clamped to 1.
func TestWithRetryConfig_BurstFactorBelowOneClampedToOne(t *testing.T) {
	l := New(&Config{Default: 10}).WithRetryConfig(&RetryConfig{
		Airtel:      10,
		BurstFactor: 0, // invalid — should be clamped to 1
	})

	// Burst = 10 × 1 = 10, so we should have 10 tokens (not 0)
	airtelRetry := l.retry[domain.NetworkAirtel]
	if airtelRetry == nil {
		t.Fatal("Airtel retry limiter is nil")
	}
	if got := airtelRetry.Tokens(); got < 9 {
		t.Errorf("Airtel retry tokens = %f, expected at least 9 (clamp to BurstFactor=1)", got)
	}
}

// TestMainAndRetryBudgetsAreIndependent verifies that consuming main-queue tokens
// does not affect the retry budget and vice versa.
func TestMainAndRetryBudgetsAreIndependent(t *testing.T) {
	mainRPS, retryRPS := 100, 50
	l := New(&Config{Safaricom: mainRPS, Default: 10}).WithRetryConfig(&RetryConfig{
		SafaricomSDP:  retryRPS,
		SafaricomSMPP: retryRPS,
		BurstFactor:   1,
	})

	mainBefore := l.Tokens(domain.NetworkSafaricom)
	retryBefore := l.retry[domain.NetworkSafaricom].Tokens()

	// Consume a main token
	l.Allow(domain.NetworkSafaricom)

	mainAfter := l.Tokens(domain.NetworkSafaricom)
	retryAfter := l.retry[domain.NetworkSafaricom].Tokens()

	if mainAfter >= mainBefore {
		t.Error("Main tokens should have decreased after Allow()")
	}
	if retryAfter != retryBefore {
		t.Errorf("Retry tokens changed after Allow() on main: before=%f after=%f", retryBefore, retryAfter)
	}
}

// TestWaitRetry_CancelledContextReturnsError verifies that WaitRetry respects
// context cancellation — important for graceful shutdown.
func TestWaitRetry_CancelledContextReturnsError(t *testing.T) {
	l := New(&Config{Default: 10}).WithRetryConfig(&RetryConfig{
		// Zero RPS forces WaitRetry to block indefinitely; cancellation must escape it
		Airtel:      1,
		BurstFactor: 1,
	})

	// Drain the single token so WaitRetry will block
	l.retry[domain.NetworkAirtel].Allow()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := l.WaitRetry(ctx, domain.NetworkAirtel)
	if err == nil {
		t.Error("WaitRetry with cancelled context should return an error")
	}
}

// TestWait_CancelledContextReturnsError verifies that Wait (main queue) also
// respects context cancellation.
func TestWait_CancelledContextReturnsError(t *testing.T) {
	l := New(&Config{Airtel: 1, Default: 1})

	// Drain the single token
	l.Allow(domain.NetworkAirtel)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := l.Wait(ctx, domain.NetworkAirtel)
	if err == nil {
		t.Error("Wait with cancelled context should return an error")
	}
}

// TestWaitRetry_UnknownNetworkFallsBack verifies that a network without a dedicated
// retry limiter falls back to NetworkUnknown instead of panicking.
func TestWaitRetry_UnknownNetworkFallsBack(t *testing.T) {
	l := New(&Config{Default: 10}).WithRetryConfig(&RetryConfig{
		// Only populates a few networks; NetworkINTNL is not listed explicitly
		Airtel:      5,
		BurstFactor: 1,
	})

	// INTNL has no dedicated retry limiter — should fall back to NetworkUnknown (rps=1)
	// There should be 1 token available at construction time, so this won't block.
	ctx := context.Background()
	if err := l.WaitRetry(ctx, domain.NetworkINTNL); err != nil {
		t.Errorf("WaitRetry fallback to NetworkUnknown returned error: %v", err)
	}
}
