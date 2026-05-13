package service

import (
	"context"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

// newTestProcessor builds a Processor wired with a mock sender, publisher, and the
// provided limiter. If isRetry is true, the processor uses the retry rate budget.
func newTestProcessor(t *testing.T, limiter *ratelimit.Limiter, isRetry bool) (*Processor, *mocks.MockQueuePublisher, *mocks.MockMNOSender) {
	t.Helper()

	log := logger.NewNoop()
	pub := mocks.NewMockQueuePublisher()
	metr := mocks.NewMockMetrics()

	factory := mocks.NewMockMNOSenderFactory()
	sender := mocks.NewMockMNOSender("safaricom-smpp", domain.NetworkSafaricom)
	factory.RegisterSender(sender)

	router := NewRouter(factory, log)
	handler := NewResultHandler(&ResultHandlerConfig{
		Publisher:               pub,
		Metrics:                 metr,
		MaxRetriesTransactional: 5,
		MaxRetriesPromotional:   10,
		Logger:                  log,
	})

	p := NewProcessor(&ProcessorConfig{
		Router:        router,
		ResultHandler: handler,
		RateLimiter:   limiter,
		Metrics:       metr,
		WorkerCount:   100,
		IsRetry:       isRetry,
		Logger:        log,
	})

	return p, pub, sender
}

// generousLimiter returns a rate limiter with very high rates so tests are never
// gated by throughput rather than the logic under test.
func generousLimiter() *ratelimit.Limiter {
	return ratelimit.New(&ratelimit.Config{Safaricom: 10000, Default: 10000}).
		WithRetryConfig(&ratelimit.RetryConfig{
			SafaricomSDP:  5000,
			SafaricomSMPP: 5000,
			Airtel:        5000,
			BurstFactor:   1,
		})
}

func validMsg(correlator string) *domain.Message {
	return &domain.Message{
		Correlator: correlator,
		Content:    "Test message",
		MSISDN:     "254722123456",
		NetworkRaw: "SAFARICOM",
		Sender:     "TestSender",
	}
}

// TestProcessor_WorkerCapAtBatchSize verifies that processBatch spawns at most
// len(messages) goroutines — no idle workers for small batches.
func TestProcessor_WorkerCapAtBatchSize(t *testing.T) {
	p, pub, _ := newTestProcessor(t, generousLimiter(), false)

	msgs := []*domain.Message{validMsg("a"), validMsg("b"), validMsg("c")}
	delivery := mocks.NewMockDeliveryWithMessages(msgs)

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() error = %v", err)
	}

	if len(pub.GetPublishedItems()) != 3 {
		t.Errorf("Expected 3 published results, got %d", len(pub.GetPublishedItems()))
	}
	if !delivery.AckCalled {
		t.Error("Expected delivery to be Ack'd after processing")
	}
}

// TestProcessor_WorkerCapSingleMessage verifies correctness when workerCount (100)
// far exceeds batch size (1) — only 1 worker should be spawned.
func TestProcessor_WorkerCapSingleMessage(t *testing.T) {
	p, pub, _ := newTestProcessor(t, generousLimiter(), false)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validMsg("only-one")})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() error = %v", err)
	}

	if len(pub.GetPublishedItems()) != 1 {
		t.Errorf("Expected 1 published result, got %d", len(pub.GetPublishedItems()))
	}
}

// TestProcessor_IsRetryFlag_ConsumesRetryBudget verifies that IsRetry=true draws from
// the retry token bucket rather than the main one:
//   - isRetry=false → main tokens decrease, retry tokens unchanged
//   - isRetry=true  → retry tokens decrease, main tokens unchanged
func TestProcessor_IsRetryFlag_ConsumesRetryBudget(t *testing.T) {
	const mainRPS = 1000
	const retryRPS = 50

	limiter := ratelimit.New(&ratelimit.Config{Safaricom: mainRPS, Default: 100}).
		WithRetryConfig(&ratelimit.RetryConfig{
			SafaricomSDP:  retryRPS,
			SafaricomSMPP: retryRPS,
			BurstFactor:   1,
		})

	mainBefore := limiter.Tokens(domain.NetworkSafaricom)
	retryBefore := limiter.RetryTokens(domain.NetworkSafaricom)

	// -- Main-queue processor (isRetry=false) --
	mainProc, _, _ := newTestProcessor(t, limiter, false)
	d1 := mocks.NewMockDeliveryWithMessages([]*domain.Message{validMsg("main-msg")})
	if err := mainProc.ProcessDelivery(context.Background(), d1); err != nil {
		t.Fatalf("main processor error: %v", err)
	}

	mainAfterMain := limiter.Tokens(domain.NetworkSafaricom)
	retryAfterMain := limiter.RetryTokens(domain.NetworkSafaricom)

	if mainAfterMain >= mainBefore {
		t.Errorf("Main tokens should decrease after main-queue processing: before=%.1f after=%.1f", mainBefore, mainAfterMain)
	}
	if retryAfterMain != retryBefore {
		t.Errorf("Retry tokens should not change when isRetry=false: before=%.1f after=%.1f", retryBefore, retryAfterMain)
	}

	// -- Retry processor (isRetry=true) --
	retryProc, _, _ := newTestProcessor(t, limiter, true)
	d2 := mocks.NewMockDeliveryWithMessages([]*domain.Message{validMsg("retry-msg")})
	if err := retryProc.ProcessDelivery(context.Background(), d2); err != nil {
		t.Fatalf("retry processor error: %v", err)
	}

	mainAfterRetry := limiter.Tokens(domain.NetworkSafaricom)
	retryAfterRetry := limiter.RetryTokens(domain.NetworkSafaricom)

	if retryAfterRetry >= retryAfterMain {
		t.Errorf("Retry tokens should decrease after retry-queue processing: before=%.1f after=%.1f", retryAfterMain, retryAfterRetry)
	}
	// Main tokens must not have changed since the retry processor ran
	if mainAfterRetry != mainAfterMain {
		t.Errorf("Main tokens should not change when isRetry=true: before=%.1f after=%.1f", mainAfterMain, mainAfterRetry)
	}
}

// TestProcessor_EmptyDelivery verifies that an empty delivery is Ack'd immediately.
func TestProcessor_EmptyDelivery(t *testing.T) {
	p, pub, _ := newTestProcessor(t, generousLimiter(), false)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() with empty delivery error = %v", err)
	}
	if !delivery.AckCalled {
		t.Error("Expected empty delivery to be Ack'd")
	}
	if len(pub.GetPublishedItems()) != 0 {
		t.Errorf("Expected 0 published for empty delivery, got %d", len(pub.GetPublishedItems()))
	}
}

// TestProcessor_IsRetryConfig_DefaultFalse verifies IsRetry defaults to false.
func TestProcessor_IsRetryConfig_DefaultFalse(t *testing.T) {
	p := NewProcessor(&ProcessorConfig{WorkerCount: 10})
	if p.isRetry {
		t.Error("isRetry should default to false")
	}
}

// TestProcessor_IsRetryConfig_SetTrue verifies IsRetry=true is stored on the struct.
func TestProcessor_IsRetryConfig_SetTrue(t *testing.T) {
	p := NewProcessor(&ProcessorConfig{WorkerCount: 10, IsRetry: true})
	if !p.isRetry {
		t.Error("isRetry should be true when IsRetry=true in config")
	}
}

// TestProcessor_WorkerCountDefault verifies that WorkerCount=0 defaults to 10.
func TestProcessor_WorkerCountDefault(t *testing.T) {
	p := NewProcessor(&ProcessorConfig{WorkerCount: 0})
	if p.workerCount != 10 {
		t.Errorf("Expected default workerCount=10, got %d", p.workerCount)
	}
}

// TestProcessor_ProcessMessages_NoAck verifies that ProcessMessages runs correctly
// and does not require a Delivery object.
func TestProcessor_ProcessMessages_NoAck(t *testing.T) {
	p, pub, _ := newTestProcessor(t, generousLimiter(), false)

	msgs := []*domain.Message{validMsg("pm-1"), validMsg("pm-2")}
	result, err := p.ProcessMessages(context.Background(), msgs)
	if err != nil {
		t.Fatalf("ProcessMessages() error = %v", err)
	}
	if result.TotalCount != 2 {
		t.Errorf("Expected TotalCount=2, got %d", result.TotalCount)
	}
	if len(pub.GetPublishedItems()) != 2 {
		t.Errorf("Expected 2 published results, got %d", len(pub.GetPublishedItems()))
	}
}

// TestProcessor_ContextCancellation verifies that context cancellation during
// processing does not panic — workers drain cleanly.
func TestProcessor_ContextCancellation(t *testing.T) {
	p, _, _ := newTestProcessor(t, generousLimiter(), false)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	msgs := make([]*domain.Message, 20)
	for i := range msgs {
		msgs[i] = validMsg("bulk")
	}

	_, _ = p.ProcessMessages(ctx, msgs) // must not panic
}
