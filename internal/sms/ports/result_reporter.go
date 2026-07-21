package ports

import (
	"context"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
)

// ResultReporter reports the outcome of a send attempt back to an external system.
// Used by AfricasTalking, whose result is not published to internal RabbitMQ queues
// like the Kenyan MNOs but instead reported synchronously over HTTP to the PHP API.
type ResultReporter interface {
	// Report sends the outcome of result to the external system. A non-nil error
	// means the report itself failed (e.g. network error, non-2xx) — callers should
	// treat this as retryable (nack/requeue the originating delivery), independent
	// of whether the underlying SendResult was a success or failure.
	Report(ctx context.Context, result *domain.SendResult) error
}
