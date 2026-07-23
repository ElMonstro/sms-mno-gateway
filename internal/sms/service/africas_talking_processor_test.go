package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

// testReportRetryBaseDelay/testReportRetryMaxDelay keep the backoff-driven tests
// fast and deterministic instead of sleeping for the production defaults.
const (
	testReportRetryBaseDelay = time.Millisecond
	testReportRetryMaxDelay  = 5 * time.Millisecond
)

func newTestAfricasTalkingProcessor(sender *mocks.MockMNOSender, reporter *mocks.MockResultReporter) *AfricasTalkingProcessor {
	return NewAfricasTalkingProcessor(&AfricasTalkingProcessorConfig{
		Sender:               sender,
		Reporter:             reporter,
		RateLimiter:          nil,
		Metrics:              mocks.NewMockMetrics(),
		ReportRetryBaseDelay: testReportRetryBaseDelay,
		ReportRetryMaxDelay:  testReportRetryMaxDelay,
		Logger:               logger.NewNoop(),
	})
}

func newTestAfricasTalkingProcessorWithPublisher(sender *mocks.MockMNOSender, reporter *mocks.MockResultReporter, publisher *mocks.MockQueuePublisher) *AfricasTalkingProcessor {
	return NewAfricasTalkingProcessor(&AfricasTalkingProcessorConfig{
		Sender:               sender,
		Reporter:             reporter,
		RateLimiter:          nil,
		Metrics:              mocks.NewMockMetrics(),
		Publisher:            publisher,
		SaveToDBQueue:        "SAVE_TO_DB",
		ReportRetryBaseDelay: testReportRetryBaseDelay,
		ReportRetryMaxDelay:  testReportRetryMaxDelay,
		Logger:               logger.NewNoop(),
	})
}

// newTestAfricasTalkingProcessorFull is for tests exercising the bounded
// in-process-retry-on-downstream-failure path, which needs DeadLetterQueue/
// MaxReportRetries wired up.
func newTestAfricasTalkingProcessorFull(sender *mocks.MockMNOSender, reporter *mocks.MockResultReporter, publisher *mocks.MockQueuePublisher, maxRetries int) *AfricasTalkingProcessor {
	return NewAfricasTalkingProcessor(&AfricasTalkingProcessorConfig{
		Sender:               sender,
		Reporter:             reporter,
		RateLimiter:          nil,
		Metrics:              mocks.NewMockMetrics(),
		Publisher:            publisher,
		SaveToDBQueue:        "SAVE_TO_DB",
		DeadLetterQueue:      "SMS_DEAD_LETTER_QUEUE",
		MaxReportRetries:     maxRetries,
		ReportRetryBaseDelay: testReportRetryBaseDelay,
		ReportRetryMaxDelay:  testReportRetryMaxDelay,
		Logger:               logger.NewNoop(),
	})
}

func validATMessage() *domain.Message {
	return &domain.Message{
		Correlator: "1",
		Content:    "hi",
		MSISDN:     "233241234567",
		NetworkRaw: "Intnl",
		Sender:     "EMALIFY",
		OutboxID:   42,
	}
}

func TestAfricasTalkingProcessor_SendSuccess_ReportSuccess_Acks(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	p := newTestAfricasTalkingProcessor(sender, reporter)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected delivery to be acked")
	}
	if delivery.NackCalled {
		t.Error("expected delivery not to be nacked")
	}
	if len(reporter.GetReportCalls()) != 1 {
		t.Fatalf("expected 1 report call, got %d", len(reporter.GetReportCalls()))
	}
}

func TestAfricasTalkingProcessor_SendFails_ReportSucceeds_StillAcks(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	sender.SendFunc = func(ctx context.Context, msg *domain.Message) *domain.SendResult {
		return domain.NewPermanentResult(msg, domain.ErrMNORejected, 0)
	}
	reporter := mocks.NewMockResultReporter()
	p := newTestAfricasTalkingProcessor(sender, reporter)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected delivery to be acked even though the AT send failed — PHP now owns the terminal state")
	}
	calls := reporter.GetReportCalls()
	if len(calls) != 1 || calls[0].IsSuccess() {
		t.Fatalf("expected 1 report call reporting failure, got %+v", calls)
	}
}

func TestAfricasTalkingProcessor_ReportFails_ThenSucceeds_RetriesInProcessAndAcks(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	attempts := 0
	reporter.ReportFunc = func(ctx context.Context, result *domain.SendResult) error {
		attempts++
		if attempts == 1 {
			return errors.New("report endpoint unreachable")
		}
		return nil
	}
	p := newTestAfricasTalkingProcessor(sender, reporter)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected delivery to be acked once the retried report call succeeds")
	}
	if delivery.NackCalled {
		t.Error("expected delivery not to be nacked")
	}
	// The critical assertion: AfricasTalking must never be re-invoked just
	// because a downstream step is being retried.
	if got := len(sender.GetSentMessages()); got != 1 {
		t.Errorf("expected AfricasTalking Send to be called exactly once, got %d", got)
	}
	if attempts != 2 {
		t.Errorf("expected 2 report attempts, got %d", attempts)
	}
}

func TestAfricasTalkingProcessor_ReportAlwaysFails_ExhaustsRetries_DeadLettersAndDrops(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	reporter.ReportFunc = func(ctx context.Context, result *domain.SendResult) error {
		return errors.New("report endpoint unreachable")
	}
	publisher := mocks.NewMockQueuePublisher()
	p := newTestAfricasTalkingProcessorFull(sender, reporter, publisher, 2) // 1 initial + 2 retries = 3 attempts

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if delivery.AckCalled {
		t.Error("expected delivery not to be acked once retries are exhausted")
	}
	// Dropped without requeue — requeueing here would restart processMessage
	// from the top and re-send the message via AfricasTalking again.
	if !delivery.NackCalled || delivery.NackParam {
		t.Error("expected delivery to be nacked without requeue (dropped) once retries are exhausted")
	}
	if got := len(sender.GetSentMessages()); got != 1 {
		t.Errorf("expected AfricasTalking Send to be called exactly once regardless of report retries, got %d", got)
	}
	if got := len(reporter.GetReportCalls()); got != 3 {
		t.Errorf("expected 3 report attempts (1 initial + 2 retries), got %d", got)
	}

	// SAVE_TO_DB is published before the report call (see processMessage), so it
	// still succeeds even though the report keeps failing — the record just also
	// ends up dead-lettered once report retries are exhausted.
	items := publisher.GetPublishedItems()
	if len(items) != 2 {
		t.Fatalf("expected 2 published items (SAVE_TO_DB, then dead-letter), got %d", len(items))
	}
	if items[0].QueueName != "SAVE_TO_DB" {
		t.Errorf("items[0].QueueName = %v, want SAVE_TO_DB", items[0].QueueName)
	}
	if items[1].QueueName != "SMS_DEAD_LETTER_QUEUE" {
		t.Errorf("items[1].QueueName = %v, want SMS_DEAD_LETTER_QUEUE", items[1].QueueName)
	}
}

func TestAfricasTalkingProcessor_SaveToDBPublishFails_ThenSucceeds_RetriesInProcessAndAcks(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	publisher := mocks.NewMockQueuePublisher()
	attempts := 0
	publisher.PublishHook = func(ctx context.Context, queueName string, msg *domain.Message) error {
		if queueName != "SAVE_TO_DB" {
			return nil
		}
		attempts++
		if attempts == 1 {
			return errors.New("save_to_db publish failed")
		}
		return nil
	}
	p := newTestAfricasTalkingProcessorWithPublisher(sender, reporter, publisher)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected delivery to be acked once the retried SAVE_TO_DB publish succeeds")
	}
	if delivery.NackCalled {
		t.Error("expected delivery not to be nacked")
	}
	if got := len(sender.GetSentMessages()); got != 1 {
		t.Errorf("expected AfricasTalking Send to be called exactly once, got %d", got)
	}
	if attempts != 2 {
		t.Errorf("expected 2 SAVE_TO_DB publish attempts, got %d", attempts)
	}

	items := publisher.GetPublishedItems()
	if len(items) != 1 {
		t.Fatalf("expected exactly 1 recorded published item (the failed attempt isn't recorded), got %d", len(items))
	}
	if items[0].QueueName != "SAVE_TO_DB" {
		t.Errorf("QueueName = %v, want SAVE_TO_DB", items[0].QueueName)
	}
}

func TestAfricasTalkingProcessor_InvalidOutboxID_DropsWithoutSending(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	p := newTestAfricasTalkingProcessor(sender, reporter)

	msg := validATMessage()
	msg.OutboxID = 0
	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{msg})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(sender.GetSentMessages()) != 0 {
		t.Error("expected sender not to be called for a message with invalid outboxId")
	}
	if len(reporter.GetReportCalls()) != 0 {
		t.Error("expected reporter not to be called for a message with invalid outboxId")
	}
	if delivery.AckCalled {
		t.Error("expected delivery not to be acked")
	}
	if !delivery.NackCalled || delivery.NackParam {
		t.Error("expected delivery to be nacked without requeue (dropped) for invalid outboxId")
	}
}

func TestAfricasTalkingProcessor_Success_PublishesToSaveToDB(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	publisher := mocks.NewMockQueuePublisher()
	p := newTestAfricasTalkingProcessorWithPublisher(sender, reporter, publisher)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected delivery to be acked")
	}
	items := publisher.GetPublishedItems()
	if len(items) != 1 {
		t.Fatalf("expected 1 published item, got %d", len(items))
	}
	if items[0].QueueName != "SAVE_TO_DB" {
		t.Errorf("QueueName = %v, want SAVE_TO_DB", items[0].QueueName)
	}
}

func TestAfricasTalkingProcessor_SaveToDBPublishedBeforeReportCall(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	publisher := mocks.NewMockQueuePublisher()

	var order []string
	reporter.ReportFunc = func(ctx context.Context, result *domain.SendResult) error {
		order = append(order, "report")
		return nil
	}
	publisher.PublishHook = func(ctx context.Context, queueName string, msg *domain.Message) error {
		order = append(order, "publish:"+queueName)
		return nil
	}
	p := newTestAfricasTalkingProcessorWithPublisher(sender, reporter, publisher)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	want := []string{"publish:SAVE_TO_DB", "report"}
	if len(order) != len(want) || order[0] != want[0] || order[1] != want[1] {
		t.Errorf("call order = %v, want %v (SAVE_TO_DB must be published before the report call reaches gateway-dlr-handler)", order, want)
	}
}

func TestAfricasTalkingProcessor_PermanentFailure_PublishesToSaveToDB(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	sender.SendFunc = func(ctx context.Context, msg *domain.Message) *domain.SendResult {
		return domain.NewPermanentResult(msg, domain.ErrMNORejected, 0)
	}
	reporter := mocks.NewMockResultReporter()
	publisher := mocks.NewMockQueuePublisher()
	p := newTestAfricasTalkingProcessorWithPublisher(sender, reporter, publisher)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected delivery to be acked")
	}
	if len(publisher.GetPublishedItems()) != 1 {
		t.Fatalf("expected permanent failure to still be published to SAVE_TO_DB, got %d items", len(publisher.GetPublishedItems()))
	}
}

func TestAfricasTalkingProcessor_RetryableResult_DoesNotPublishToSaveToDB(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	sender.SendFunc = func(ctx context.Context, msg *domain.Message) *domain.SendResult {
		return domain.NewRetryableResult(msg, domain.ErrMNOUnavailable, 0)
	}
	reporter := mocks.NewMockResultReporter()
	publisher := mocks.NewMockQueuePublisher()
	p := newTestAfricasTalkingProcessorWithPublisher(sender, reporter, publisher)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected delivery to be acked (reporter still succeeded)")
	}
	if len(publisher.GetPublishedItems()) != 0 {
		t.Errorf("expected retryable result not to be published to SAVE_TO_DB, got %d items", len(publisher.GetPublishedItems()))
	}
}

func TestAfricasTalkingProcessor_SaveToDBPublishAlwaysFails_ExhaustsRetries_Drops(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	publisher := mocks.NewMockQueuePublisher()
	publisher.PublishErr = errors.New("save_to_db publish failed")
	p := newTestAfricasTalkingProcessorWithPublisher(sender, reporter, publisher)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if delivery.AckCalled {
		t.Error("expected delivery not to be acked when the SAVE_TO_DB publish keeps failing")
	}
	// Dropped without requeue — requeueing here would restart processMessage
	// from the top and re-send the message via AfricasTalking again.
	if !delivery.NackCalled || delivery.NackParam {
		t.Error("expected delivery to be nacked without requeue (dropped) once retries are exhausted")
	}
	if got := len(sender.GetSentMessages()); got != 1 {
		t.Errorf("expected AfricasTalking Send to be called exactly once, got %d", got)
	}
}

func TestAfricasTalkingProcessor_EmptyDelivery_Acks(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	p := newTestAfricasTalkingProcessor(sender, reporter)

	delivery := mocks.NewMockDeliveryWithMessages(nil)

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !delivery.AckCalled {
		t.Error("expected empty delivery to be acked")
	}
}
