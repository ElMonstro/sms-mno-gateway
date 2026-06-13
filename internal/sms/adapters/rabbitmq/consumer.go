package rabbitmq

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
)

// Consumer implements the ports.QueueConsumer interface
// CRITICAL: Uses manual acknowledgment to fix EM-143
type Consumer struct {
	conn      *Connection
	queueName string
	prefetch  int
	log       logger.Logger
}

// ConsumerConfig holds configuration for the consumer
type ConsumerConfig struct {
	Connection *Connection
	QueueName  string
	Prefetch   int
	Logger     logger.Logger
}

// NewConsumer creates a new RabbitMQ consumer
func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	c := &Consumer{
		conn:      cfg.Connection,
		queueName: cfg.QueueName,
		prefetch:  cfg.Prefetch,
		log:       cfg.Logger,
	}

	// Declare the queue
	if err := c.conn.DeclareQueue(context.Background(), cfg.QueueName); err != nil {
		return nil, err
	}

	return c, nil
}

// Consume starts consuming messages from the queue
// CRITICAL: auto-ack is FALSE - this is the fix for EM-143
func (c *Consumer) Consume(ctx context.Context) (<-chan ports.Delivery, error) {
	channel := c.conn.Channel()

	// Set prefetch count (QoS)
	if err := channel.Qos(c.prefetch, 0, false); err != nil {
		return nil, err
	}

	// Start consuming with auto-ack = FALSE (EM-143 fix)
	msgs, err := channel.Consume(
		c.queueName, // queue
		"",          // consumer tag
		false,       // auto-ack = FALSE (CRITICAL: manual ack)
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return nil, err
	}

	deliveries := make(chan ports.Delivery)

	go func() {
		defer close(deliveries)

		for {
			select {
			case <-ctx.Done():
				c.log.Info("Consumer context cancelled")
				return
			case msg, ok := <-msgs:
				if !ok {
					c.log.Warn("Consumer channel closed")
					return
				}

				// Parse messages and stamp the source queue so downstream
				// components (e.g. DLR URL selection) can branch per queue.
				messages, err := c.parseMessages(msg.Body, c.queueName)
				if err != nil {
					c.log.WithError(err).Error("Failed to parse messages")
					// Nack with no requeue for invalid messages
					msg.Nack(false, false)
					continue
				}

				c.log.WithFields(map[string]interface{}{
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
					// Nack with requeue on shutdown
					msg.Nack(false, true)
					return
				}
			}
		}
	}()

	c.log.WithField("queue", c.queueName).Info("Started consuming messages")
	return deliveries, nil
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
	d.log.WithFields(map[string]interface{}{
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
