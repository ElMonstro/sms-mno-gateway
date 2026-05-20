package bootstrap

import (
	"context"
	"testing"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/mocks"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

func newDelivery(msgCount int) ports.Delivery {
	msgs := make([]*domain.Message, msgCount)
	for i := range msgs {
		msgs[i] = &domain.Message{Correlator: "test"}
	}
	return mocks.NewMockDeliveryWithMessages(msgs)
}

func newDeliveryChan(buf int, items ...ports.Delivery) chan ports.Delivery {
	ch := make(chan ports.Delivery, buf)
	for _, d := range items {
		ch <- d
	}
	return ch
}

func TestAccumulateBatch_DrainsSingleDelivery(t *testing.T) {
	ch := newDeliveryChan(5, newDelivery(1))

	msgs, batch := accumulateBatch(context.Background(), ch, 50)
	if len(msgs) != 1 {
		t.Errorf("expected 1 message, got %d", len(msgs))
	}
	if len(batch) != 1 {
		t.Errorf("expected 1 delivery, got %d", len(batch))
	}
}

func TestAccumulateBatch_AccumulatesUpToMax(t *testing.T) {
	ch := newDeliveryChan(10,
		newDelivery(1),
		newDelivery(1),
		newDelivery(1),
	)

	msgs, batch := accumulateBatch(context.Background(), ch, 3)
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages, got %d", len(msgs))
	}
	if len(batch) != 3 {
		t.Errorf("expected 3 deliveries, got %d", len(batch))
	}
}

func TestAccumulateBatch_CapsAtMaxMsgs(t *testing.T) {
	ch := newDeliveryChan(10)
	for i := 0; i < 10; i++ {
		ch <- newDelivery(1)
	}

	msgs, batch := accumulateBatch(context.Background(), ch, 3)
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages (capped), got %d", len(msgs))
	}
	if len(batch) != 3 {
		t.Errorf("expected 3 deliveries, got %d", len(batch))
	}
}

func TestAccumulateBatch_ContextCancelledBeforeFirst(t *testing.T) {
	ch := newDeliveryChan(5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msgs, batch := accumulateBatch(ctx, ch, 10)
	if msgs != nil || batch != nil {
		t.Errorf("expected nil slices on cancelled ctx, got msgs=%v batch=%v", msgs, batch)
	}
}

func TestAccumulateBatch_ChannelClosedBeforeFirst(t *testing.T) {
	ch := make(chan ports.Delivery)
	close(ch)

	msgs, batch := accumulateBatch(context.Background(), ch, 10)
	if msgs != nil || batch != nil {
		t.Errorf("expected nil slices on closed channel, got msgs=%v batch=%v", msgs, batch)
	}
}

func TestAccumulateBatch_MultiMessageDeliveryCountsTowardMax(t *testing.T) {
	// Each delivery carries 3 messages. maxMsgs=4.
	// First delivery: 3 msgs (< 4, drain continues). Second delivery: +3 = 6 (>= 4, stop).
	ch := newDeliveryChan(5, newDelivery(3), newDelivery(3))

	msgs, batch := accumulateBatch(context.Background(), ch, 4)
	if len(batch) != 2 {
		t.Errorf("expected 2 deliveries, got %d", len(batch))
	}
	if len(msgs) != 6 {
		t.Errorf("expected 6 messages, got %d", len(msgs))
	}
}
