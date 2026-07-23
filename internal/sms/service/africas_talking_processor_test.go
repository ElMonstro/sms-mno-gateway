package service

import (
	"context"
	"errors"
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
)

func newTestAfricasTalkingProcessor(sender *mocks.MockMNOSender, reporter *mocks.MockResultReporter) *AfricasTalkingProcessor {
	return NewAfricasTalkingProcessor(&AfricasTalkingProcessorConfig{
		Sender:      sender,
		Reporter:    reporter,
		RateLimiter: nil,
		Metrics:     mocks.NewMockMetrics(),
		Logger:      logger.NewNoop(),
	})
}

func newTestAfricasTalkingProcessorWithPublisher(sender *mocks.MockMNOSender, reporter *mocks.MockResultReporter, publisher *mocks.MockQueuePublisher) *AfricasTalkingProcessor {
	return NewAfricasTalkingProcessor(&AfricasTalkingProcessorConfig{
		Sender:        sender,
		Reporter:      reporter,
		RateLimiter:   nil,
		Metrics:       mocks.NewMockMetrics(),
		Publisher:     publisher,
		SaveToDBQueue: "SAVE_TO_DB",
		Logger:        logger.NewNoop(),
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

func TestAfricasTalkingProcessor_ReportFails_NacksWithRequeue(t *testing.T) {
	sender := mocks.NewMockMNOSender("AfricasTalking", domain.NetworkINTNL)
	reporter := mocks.NewMockResultReporter()
	reporter.ReportFunc = func(ctx context.Context, result *domain.SendResult) error {
		return errors.New("report endpoint unreachable")
	}
	p := newTestAfricasTalkingProcessor(sender, reporter)

	delivery := mocks.NewMockDeliveryWithMessages([]*domain.Message{validATMessage()})

	if err := p.ProcessDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if delivery.AckCalled {
		t.Error("expected delivery not to be acked when the report call fails")
	}
	if !delivery.NackCalled || !delivery.NackParam {
		t.Error("expected delivery to be nacked with requeue=true when the report call fails")
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

func TestAfricasTalkingProcessor_SaveToDBPublishFails_NacksWithRequeue(t *testing.T) {
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
		t.Error("expected delivery not to be acked when the SAVE_TO_DB publish fails")
	}
	if !delivery.NackCalled || !delivery.NackParam {
		t.Error("expected delivery to be nacked with requeue=true when the SAVE_TO_DB publish fails")
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
