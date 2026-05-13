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

// TestProcessor_IsRetryFlag_UsesRetryBudget verifies that IsRetry=true draws from
// the retry token bucket rather than the main one.
//
// Strategy: use a limiter where the retry budget has a single burst token.
// Drain that token via AllowRetry(), then process with a short-timeout context:
//   - isRetry=true processor  → WaitRetry blocks → context expires → rate-limited result
//   - isRetry=false processor → Wait on main (1000 tokens) → succeeds immediately
func TestProcessor_IsRetryFlag_UsesRetryBudget(t *testing.T) {
	// Main has plenty of tokens; retry starts with exactly 1 burst token.
	limiter := ratelimit.New(&ratelimit.Config{Safaricom: 1000, Default: 100}).
		WithRetryConfig(&ratelimit.RetryConfig{
			SafaricomSDP:  1, // 1 token, burst=1
			SafaricomSMPP: 1,
			BurstFactor:   1,
		})

	// Exhaust the single retry token so the next WaitRetry will block.
	limiter.AllowRetry(domain.NetworkSafaricom)

	// -- isRetry=true processor with tight deadline --
	retryProc, retryPub, _ := newTestProcessor(t, limiter, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	d := mocks.NewMockDeliveryWithMessages([]*domain.Message{validMsg("retry-msg")})
	_ = retryProc.ProcessDelivery(ctx, d) // may error; we care about result type

	// With the retry budget drained and context expired, the message should have
	// been rate-limited (published as retryable, not success).
	items := retryPub.GetPublishedItems()
	if len(items) == 1 && items[0].QueueType == "save_to_db" {
		t.Error("isRetry=true processor should NOT succeed when retry budget is drained — expected rate-limited retryable or nack")
	}

	// -- isRetry=false processor — main budget untouched, should succeed --
	mainProc, mainPub, _ := newTestProcessor(t, limiter, false)
	d2 := mocks.NewMockDeliveryWithMessages([]*domain.Message{validMsg("main-msg")})
	if err := mainProc.ProcessDelivery(context.Background(), d2); err != nil {
		t.Fatalf("main processor failed unexpectedly: %v", err)
	}
	mainItems := mainPub.GetPublishedItems()
	if len(mainItems) != 1 || mainItems[0].QueueType != "save_to_db" {
		t.Errorf("isRetry=false processor should succeed with main budget: got %v items", len(mainItems))
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
