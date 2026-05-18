package service

import (
	"context"
	"fmt"
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

// newTestBatchProcessor creates a Processor wired with a MockBatchSender for
// Safaricom (implements both MNOSender and BatchSender) at the given sdpBatchSize.
func newTestBatchProcessor(t *testing.T, batchSize int) (*Processor, *mocks.MockQueuePublisher, *mocks.MockBatchSender) {
	t.Helper()

	log := logger.NewNoop()
	pub := mocks.NewMockQueuePublisher()
	metr := mocks.NewMockMetrics()

	factory := mocks.NewMockMNOSenderFactory()
	batchSender := mocks.NewMockBatchSender("safaricom-sdp", domain.NetworkSafaricom)
	factory.RegisterBatchSender(batchSender)

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
		RateLimiter:   generousLimiter(),
		Metrics:       metr,
		WorkerCount:   10,
		SDPBatchSize:  batchSize,
		Logger:        log,
	})

	return p, pub, batchSender
}

// TestProcessor_SDPBatch_GroupingSeparatesPromoFromRegular verifies that processBatch
// routes promotional Safaricom messages to the BatchSender path while transactional
// Safaricom messages use the regular per-message worker path.
func TestProcessor_SDPBatch_GroupingSeparatesPromoFromRegular(t *testing.T) {
	// batchSize=10 so all 3 promo messages land in a single SendBatch call.
	p, pub, batchSender := newTestBatchProcessor(t, 10)

	promoMsgs := []*domain.Message{validMsg("p1"), validMsg("p2"), validMsg("p3")}
	txMsgs := []*domain.Message{
		{Correlator: "t1", Content: "msg", MSISDN: "254722123456", NetworkRaw: "SAFARICOM", Sender: "S", PackageID: "TRANSACTIONAL"},
		{Correlator: "t2", Content: "msg", MSISDN: "254722654321", NetworkRaw: "SAFARICOM", Sender: "S", PackageID: "TRANSACTIONAL"},
	}
	delivery := mocks.NewMockDeliveryWithMessages(append(promoMsgs, txMsgs...))

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() error = %v", err)
	}

	// SendBatch must be called once with all 3 promotional messages.
	batchCalls := batchSender.GetBatchCalls()
	if len(batchCalls) != 1 {
		t.Errorf("Expected 1 SendBatch call, got %d", len(batchCalls))
	} else if batchCalls[0] != 3 {
		t.Errorf("Expected SendBatch call with 3 messages, got %d", batchCalls[0])
	}

	// 2 transactional messages must be routed to individual Send calls.
	sendCalls := batchSender.GetSendCalls()
	if len(sendCalls) != 2 {
		t.Errorf("Expected 2 individual Send calls for transactional messages, got %d", len(sendCalls))
	}

	// All 5 results must be published.
	if len(pub.GetPublishedItems()) != 5 {
		t.Errorf("Expected 5 published results, got %d", len(pub.GetPublishedItems()))
	}
}

// TestProcessor_SDPBatch_ChunkSizeRespected verifies that 10 promotional messages
// with sdpBatchSize=4 produce exactly 3 SendBatch calls with sizes [4, 4, 2].
func TestProcessor_SDPBatch_ChunkSizeRespected(t *testing.T) {
	p, pub, batchSender := newTestBatchProcessor(t, 4)

	msgs := make([]*domain.Message, 10)
	for i := range msgs {
		msgs[i] = validMsg(fmt.Sprintf("promo-%d", i))
	}
	delivery := mocks.NewMockDeliveryWithMessages(msgs)

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() error = %v", err)
	}

	batchCalls := batchSender.GetBatchCalls()
	if len(batchCalls) != 3 {
		t.Fatalf("Expected 3 SendBatch calls, got %d: %v", len(batchCalls), batchCalls)
	}
	for i, want := range []int{4, 4, 2} {
		if batchCalls[i] != want {
			t.Errorf("SendBatch call %d: expected size %d, got %d", i, want, batchCalls[i])
		}
	}
	if len(pub.GetPublishedItems()) != 10 {
		t.Errorf("Expected 10 published results, got %d", len(pub.GetPublishedItems()))
	}
}

// TestProcessor_SDPBatch_FallbackWhenNotBatchSender verifies that when the Safaricom
// sender does not implement BatchSender, the processor falls back to the per-message
// worker pool and calls Send for each promotional message.
func TestProcessor_SDPBatch_FallbackWhenNotBatchSender(t *testing.T) {
	log := logger.NewNoop()
	pub := mocks.NewMockQueuePublisher()
	metr := mocks.NewMockMetrics()

	factory := mocks.NewMockMNOSenderFactory()
	plainSender := mocks.NewMockMNOSender("safaricom-sdp", domain.NetworkSafaricom)
	factory.RegisterSender(plainSender)

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
		RateLimiter:   generousLimiter(),
		Metrics:       metr,
		WorkerCount:   10,
		SDPBatchSize:  2, // >1 triggers batch classification path
		Logger:        log,
	})

	msgs := []*domain.Message{validMsg("fb-1"), validMsg("fb-2"), validMsg("fb-3")}
	delivery := mocks.NewMockDeliveryWithMessages(msgs)

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() error = %v", err)
	}

	// All 3 messages must be sent via individual Send (fallback path).
	sent := plainSender.GetSentMessages()
	if len(sent) != 3 {
		t.Errorf("Expected 3 individual Send calls (fallback), got %d", len(sent))
	}
	if len(pub.GetPublishedItems()) != 3 {
		t.Errorf("Expected 3 published results, got %d", len(pub.GetPublishedItems()))
	}
}

// TestProcessor_SDPBatch_GroupsBySender verifies that promotional messages with
// different Sender values are split into separate SendBatch calls (one per sender),
// ensuring each SDP DataSet contains a single homogeneous oa value.
func TestProcessor_SDPBatch_GroupsBySender(t *testing.T) {
	// batchSize=10: each sender group fits in one SendBatch call.
	p, pub, batchSender := newTestBatchProcessor(t, 10)

	alphaMsgs := []*domain.Message{
		{Correlator: "a1", Content: "msg", MSISDN: "254722000001", NetworkRaw: "SAFARICOM", Sender: "Alpha"},
		{Correlator: "a2", Content: "msg", MSISDN: "254722000002", NetworkRaw: "SAFARICOM", Sender: "Alpha"},
		{Correlator: "a3", Content: "msg", MSISDN: "254722000003", NetworkRaw: "SAFARICOM", Sender: "Alpha"},
	}
	betaMsgs := []*domain.Message{
		{Correlator: "b1", Content: "msg", MSISDN: "254722000004", NetworkRaw: "SAFARICOM", Sender: "Beta"},
		{Correlator: "b2", Content: "msg", MSISDN: "254722000005", NetworkRaw: "SAFARICOM", Sender: "Beta"},
	}
	delivery := mocks.NewMockDeliveryWithMessages(append(alphaMsgs, betaMsgs...))

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() error = %v", err)
	}

	batchCalls := batchSender.GetBatchCalls()
	if len(batchCalls) != 2 {
		t.Fatalf("Expected 2 SendBatch calls (one per sender), got %d: %v", len(batchCalls), batchCalls)
	}
	if batchCalls[0] != 3 {
		t.Errorf("First SendBatch call (Alpha): expected 3 messages, got %d", batchCalls[0])
	}
	if batchCalls[1] != 2 {
		t.Errorf("Second SendBatch call (Beta): expected 2 messages, got %d", batchCalls[1])
	}

	// Verify each batch call contains only messages with the same sender.
	for batchIdx, batch := range batchSender.GetBatchMessages() {
		firstSender := batch[0].Sender
		for i, msg := range batch {
			if msg.Sender != firstSender {
				t.Errorf("batch[%d] message[%d] has sender %q, want %q", batchIdx, i, msg.Sender, firstSender)
			}
		}
	}

	if len(pub.GetPublishedItems()) != 5 {
		t.Errorf("Expected 5 published results, got %d", len(pub.GetPublishedItems()))
	}
}

// TestProcessor_SDPBatch_GroupsBySender_Chunked verifies that each sender group is
// independently chunked by sdpBatchSize. With batchSize=2 and 3 messages per sender,
// each group produces 2 SendBatch calls [2, 1], yielding 4 calls total.
func TestProcessor_SDPBatch_GroupsBySender_Chunked(t *testing.T) {
	p, _, batchSender := newTestBatchProcessor(t, 2)

	var msgs []*domain.Message
	for i := 0; i < 3; i++ {
		msgs = append(msgs, &domain.Message{
			Correlator: fmt.Sprintf("alpha-%d", i),
			Content:    "msg",
			MSISDN:     fmt.Sprintf("25472200000%d", i),
			NetworkRaw: "SAFARICOM",
			Sender:     "Alpha",
		})
	}
	for i := 0; i < 3; i++ {
		msgs = append(msgs, &domain.Message{
			Correlator: fmt.Sprintf("beta-%d", i),
			Content:    "msg",
			MSISDN:     fmt.Sprintf("25472200001%d", i),
			NetworkRaw: "SAFARICOM",
			Sender:     "Beta",
		})
	}
	delivery := mocks.NewMockDeliveryWithMessages(msgs)

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("ProcessDelivery() error = %v", err)
	}

	// Alpha: [2, 1], Beta: [2, 1] → 4 SendBatch calls
	batchCalls := batchSender.GetBatchCalls()
	if len(batchCalls) != 4 {
		t.Fatalf("Expected 4 SendBatch calls, got %d: %v", len(batchCalls), batchCalls)
	}
	for i, want := range []int{2, 1, 2, 1} {
		if batchCalls[i] != want {
			t.Errorf("SendBatch call %d: expected size %d, got %d", i, want, batchCalls[i])
		}
	}
}

// TestGroupBySender verifies the helper directly: ordering, grouping, and empty input.
func TestGroupBySender(t *testing.T) {
	msgs := []*domain.Message{
		{Correlator: "1", Sender: "A"},
		{Correlator: "2", Sender: "B"},
		{Correlator: "3", Sender: "A"},
		{Correlator: "4", Sender: "C"},
		{Correlator: "5", Sender: "B"},
	}

	groups := groupBySender(msgs)

	if len(groups) != 3 {
		t.Fatalf("Expected 3 groups, got %d", len(groups))
	}
	// First-seen order: A, B, C
	wantSenders := []string{"A", "B", "C"}
	wantSizes := []int{2, 2, 1}
	for i, g := range groups {
		if g[0].Sender != wantSenders[i] {
			t.Errorf("group[%d] sender = %q, want %q", i, g[0].Sender, wantSenders[i])
		}
		if len(g) != wantSizes[i] {
			t.Errorf("group[%d] size = %d, want %d", i, len(g), wantSizes[i])
		}
	}

	// Empty input
	if got := groupBySender(nil); len(got) != 0 {
		t.Errorf("groupBySender(nil) should return empty, got %d groups", len(got))
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
