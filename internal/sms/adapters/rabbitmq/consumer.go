package rabbitmq

import (
	"context"
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Consumer implements the ports.QueueConsumer interface
// CRITICAL: Uses manual acknowledgment to fix EM-143
type Consumer struct {
	conn          *Connection
	queueName     string
	prefetch      int
	reconnectWait time.Duration
	log           logger.Logger
}

// ConsumerConfig holds configuration for the consumer
type ConsumerConfig struct {
	Connection    *Connection
	QueueName     string
	Prefetch      int
	ReconnectWait time.Duration
	Logger        logger.Logger
}

// NewConsumer creates a new RabbitMQ consumer
func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	rw := cfg.ReconnectWait
	if rw <= 0 {
		rw = 5 * time.Second
	}

	c := &Consumer{
		conn:          cfg.Connection,
		queueName:     cfg.QueueName,
		prefetch:      cfg.Prefetch,
		reconnectWait: rw,
		log:           cfg.Logger,
	}

	// Declare the queue (idempotent; uses shared channel for init only)
	if err := c.conn.DeclareQueue(context.Background(), cfg.QueueName); err != nil {
		return nil, err
	}

	return c, nil
}

// openChannel opens a fresh dedicated AMQP channel for this consumer and registers
// it as a subscriber on c.queueName. Each consumer owns its own channel so that
// Qos settings are isolated and concurrent Ack/Nack calls are safe.
func (c *Consumer) openChannel() (*amqp.Channel, <-chan amqp.Delivery, error) {
	channel, err := c.conn.NewChannel()
	if err != nil {
		return nil, nil, err
	}

	if err := channel.Qos(c.prefetch, 0, false); err != nil {
		channel.Close()
		return nil, nil, err
	}

	msgs, err := channel.Consume(
		c.queueName,
		"",    // consumer tag (auto-assigned)
		false, // auto-ack = FALSE (manual ack)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		channel.Close()
		return nil, nil, err
	}

	return channel, msgs, nil
}

// Consume starts consuming messages from the queue.
// The returned deliveries channel remains open for the lifetime of ctx — the
// consumer automatically reopens its AMQP channel when it is closed (e.g. after
// a RabbitMQ reconnect), so callers do not need to restart consumption.
func (c *Consumer) Consume(ctx context.Context) (<-chan ports.Delivery, error) {
	channel, msgs, err := c.openChannel()
	if err != nil {
		return nil, err
	}

	deliveries := make(chan ports.Delivery)

	go func() {
		defer close(deliveries)

		for {
			channelClosed := c.drainChannel(ctx, msgs, deliveries)
			channel.Close()

			if !channelClosed {
				// ctx cancelled — clean shutdown
				return
			}

			// AMQP channel was closed underneath us (connection drop).
			// Wait for the connection-level reconnect to complete, then reopen.
			c.log.WithField("queue", c.queueName).Warn("Consumer channel closed, reconnecting...")

			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(c.reconnectWait):
				}

				var openErr error
				channel, msgs, openErr = c.openChannel()
				if openErr != nil {
					c.log.WithError(openErr).WithField("queue", c.queueName).
						Warn("Failed to reopen consumer channel, retrying...")
					continue
				}

				c.log.WithField("queue", c.queueName).Info("Consumer channel reconnected")
				break
			}
		}
	}()

	c.log.WithField("queue", c.queueName).Info("Started consuming messages")
	return deliveries, nil
}

// drainChannel reads from msgs until the channel closes or ctx is cancelled.
// Returns true if the AMQP channel was closed (caller should reconnect),
// false if ctx was cancelled (caller should stop).
func (c *Consumer) drainChannel(ctx context.Context, msgs <-chan amqp.Delivery, deliveries chan<- ports.Delivery) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case msg, ok := <-msgs:
			if !ok {
				return true // channel closed — reconnect
			}

			messages, err := c.parseMessages(msg.Body, c.queueName)
			if err != nil {
				c.log.WithError(err).Error("Failed to parse messages")
				// Nack with no requeue for invalid messages
				msg.Nack(false, false)
				continue
			}

			c.log.WithFields(map[string]any{
				"queue":   c.queueName,
				"payload": string(msg.Body),
			}).Debug("message consumed")

			delivery := &Delivery{
				messages: messages,
				amqpMsg:  msg,
				log:      c.log,
			}

			select {
			case deliveries <- delivery:
			case <-ctx.Done():
				// Nack with requeue on shutdown so the message is not lost
				msg.Nack(false, true)
				return false
			}
		}
	}
}

// parseMessages parses the message body into a slice of domain.Message
// and stamps each message with the source queue name.
func (c *Consumer) parseMessages(body []byte, queueName string) ([]*domain.Message, error) {
	var messages []*domain.Message
	if err := json.Unmarshal(body, &messages); err != nil {
		// Try single message format
		var single domain.Message
		if err := json.Unmarshal(body, &single); err != nil {
			return nil, err
		}
		messages = []*domain.Message{&single}
	}
	for _, msg := range messages {
		msg.SourceQueue = queueName
	}
	return messages, nil
}

// QueueName returns the queue name
func (c *Consumer) QueueName() string {
	return c.queueName
}

// Close closes the consumer
func (c *Consumer) Close() error {
	return nil // Connection managed separately
}

// IsConnected returns true if connected
func (c *Consumer) IsConnected() bool {
	return c.conn.IsConnected()
}

// Delivery wraps an AMQP delivery with message parsing
type Delivery struct {
	messages []*domain.Message
	amqpMsg  amqp.Delivery
	log      logger.Logger
}

// Messages returns the batch of messages
func (d *Delivery) Messages() []*domain.Message {
	return d.messages
}

// Ack acknowledges the delivery
// CRITICAL: Only call this AFTER successfully processing all messages
func (d *Delivery) Ack() error {
	if err := d.amqpMsg.Ack(false); err != nil {
		d.log.WithError(err).Error("Failed to acknowledge message")
		return err
	}
	d.log.WithField("delivery_tag", d.amqpMsg.DeliveryTag).Debug("Acknowledged message")
	return nil
}

// Nack negatively acknowledges the delivery
func (d *Delivery) Nack(requeue bool) error {
	if err := d.amqpMsg.Nack(false, requeue); err != nil {
		d.log.WithError(err).Error("Failed to nack message")
		return err
	}
	d.log.WithFields(map[string]any{
		"delivery_tag": d.amqpMsg.DeliveryTag,
		"requeue":      requeue,
	}).Debug("Nacked message")
	return nil
}

// DeliveryTag returns the delivery tag
func (d *Delivery) DeliveryTag() uint64 {
	return d.amqpMsg.DeliveryTag
}

// Ensure Consumer implements ports.QueueConsumer
var _ ports.QueueConsumer = (*Consumer)(nil)

// Ensure Delivery implements ports.Delivery
var _ ports.Delivery = (*Delivery)(nil)
