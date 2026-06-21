package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
)

// Connection manages a RabbitMQ connection with automatic reconnection
type Connection struct {
	url           string
	conn          *amqp.Connection
	channel       *amqp.Channel
	reconnectWait time.Duration
	log           logger.Logger
	mu            sync.RWMutex
	closed        bool
	notifyClose   chan *amqp.Error
}

// ConnectionConfig holds configuration for RabbitMQ connection
type ConnectionConfig struct {
	URL           string
	ReconnectWait time.Duration
	Logger        logger.Logger
}

// NewConnection creates a new RabbitMQ connection
func NewConnection(cfg *ConnectionConfig) (*Connection, error) {
	c := &Connection{
		url:           cfg.URL,
		reconnectWait: cfg.ReconnectWait,
		log:           cfg.Logger,
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	return c, nil
}

// connect establishes the connection and channel
func (c *Connection) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close the old connection before replacing it so we don't leak TCP sockets.
	// The old connection may already be closed (that's what triggered a reconnect),
	// but calling Close() on a closed amqp.Connection is safe — it's a no-op.
	if c.conn != nil {
		c.conn.Close()
	}

	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	c.conn = conn
	c.channel = channel
	c.notifyClose = make(chan *amqp.Error)
	c.conn.NotifyClose(c.notifyClose)

	c.log.Info("Connected to RabbitMQ")

	// Start reconnection handler
	go c.handleReconnect()

	return nil
}

// handleReconnect listens for connection close events and reconnects
func (c *Connection) handleReconnect() {
	for {
		select {
		case err, ok := <-c.notifyClose:
			if !ok {
				return
			}
			if c.closed {
				return
			}

			c.log.WithError(err).Warn("RabbitMQ connection lost, reconnecting...")

			for {
				if c.closed {
					return
				}

				time.Sleep(c.reconnectWait)

				if err := c.connect(); err != nil {
					c.log.WithError(err).Warn("Failed to reconnect to RabbitMQ")
					continue
				}

				c.log.Info("Reconnected to RabbitMQ")
				return
			}
		}
	}
}

// Channel returns the current channel
func (c *Connection) Channel() *amqp.Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.channel
}

// IsConnected returns true if connected
func (c *Connection) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil && !c.conn.IsClosed()
}

// Close closes the connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true

	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			c.log.WithError(err).Warn("Error closing channel")
		}
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			c.log.WithError(err).Warn("Error closing connection")
			return err
		}
	}

	c.log.Info("RabbitMQ connection closed")
	return nil
}

// NewChannel opens a fresh, dedicated AMQP channel on the current connection.
// Use this when a component needs its own channel isolated from the shared one
// (e.g. a background consumer that also publishes).
//
// Note: may return an error if called during a reconnect window while the
// underlying connection is being replaced. Callers should retry with backoff.
func (c *Connection) NewChannel() (*amqp.Channel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn.Channel()
}

// DeclareQueue declares a durable queue with no special arguments.
func (c *Connection) DeclareQueue(ctx context.Context, name string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, err := c.channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	return err
}

// DeclareQueueWithArgs declares a durable queue with custom AMQP arguments.
// Use this for delay queues that require x-message-ttl and x-dead-letter-routing-key.
// Declaration is idempotent — safe to call on every restart provided args are stable.
func (c *Connection) DeclareQueueWithArgs(ctx context.Context, name string, args amqp.Table) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, err := c.channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		args,
	)
	return err
}
