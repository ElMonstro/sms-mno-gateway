package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	sourceQueue  = "SMS_DEAD_LETTER_QUEUE"
	promoQueue   = "sms_transactional"
	permQueue    = "SMS_DEAD_LETTER_QUEUE_PERM"
	consumerTag  = "dlq-migrator"
	idleTimeout  = 3 * time.Second
	tickInterval = time.Minute
)

func rabbitMQURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/",
		os.Getenv("RABBITMQ_USER"),
		os.Getenv("RABBITMQ_PASS"),
		os.Getenv("RABBITMQ_HOST"),
		os.Getenv("RABBITMQ_PORT"),
	)
}

type MessagePayload struct {
	Corelator   string    `json:"corelator"`
	Message     string    `json:"message"`
	Msisdn      string    `json:"msisdn"`
	Network     string    `json:"network"`
	Sender      string    `json:"sender"`
	PackageID   string    `json:"packageId"`
	Status      string    `json:"status"`
	Description string    `json:"description"`
	CreatedAt   string    `json:"createdAt"`
	RetryCount  int       `json:"retryCount"`
	LastError   string    `json:"lastError"`
	ProcessedAt time.Time `json:"processedAt"`
}

type migrator struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func newMigrator() (*migrator, error) {
	conn, err := amqp.Dial(rabbitMQURL())
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	m := &migrator{conn: conn, ch: ch}
	if err := m.declareQueues(); err != nil {
		m.close()
		return nil, err
	}
	return m, nil
}

func (m *migrator) declareQueues() error {
	for _, q := range []string{sourceQueue, promoQueue, permQueue} {
		if _, err := m.ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// reconnect replaces the connection and channel if the broker dropped them.
func (m *migrator) reconnect() error {
	if !m.conn.IsClosed() {
		return nil
	}
	conn, err := amqp.Dial(rabbitMQURL())
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}
	m.conn = conn
	m.ch = ch
	return m.declareQueues()
}

// run drains the source queue once, routing each message to the appropriate target.
func (m *migrator) run() {
	if err := m.reconnect(); err != nil {
		log.Printf("[ERROR] reconnect failed: %s", err)
		return
	}

	// Cancel any consumer left over from a previous tick before registering a new one.
	_ = m.ch.Cancel(consumerTag, false)

	msgs, err := m.ch.Consume(sourceQueue, consumerTag, false, false, false, false, nil)
	if err != nil {
		log.Printf("[ERROR] consume failed: %s", err)
		return
	}
	defer m.ch.Cancel(consumerTag, false)

	log.Printf("[*] Tick: draining %s", sourceQueue)

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return
			}

			var payloads []MessagePayload
			if err := json.Unmarshal(d.Body, &payloads); err != nil {
				log.Printf("[ERROR] unmarshal failed — routing raw body to %s", permQueue)
				publishToQueue(m.ch, permQueue, d.Body)
				d.Ack(false)
				continue
			}

			target := permQueue
			for _, p := range payloads {
				if p.Description == "maximum retry count exceeded" {
					target = promoQueue
					break
				}
			}

			if err := publishToQueue(m.ch, target, d.Body); err != nil {
				log.Printf("[ERROR] publish failed: %s — requeuing", err)
				d.Nack(false, true)
			} else {
				log.Printf("[SUCCESS] moved batch to %s", target)
				d.Ack(false)
			}

		case <-time.After(idleTimeout):
			log.Println("[*] Queue drained — tick complete")
			return
		}
	}
}

func (m *migrator) close() {
	if m.ch != nil {
		m.ch.Close()
	}
	if m.conn != nil {
		m.conn.Close()
	}
}

func publishToQueue(ch *amqp.Channel, queue string, body []byte) error {
	return ch.Publish("", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func main() {
	m, err := newMigrator()
	if err != nil {
		log.Fatalf("failed to initialise migrator: %s", err)
	}
	defer m.close()

	// Run once immediately on startup, then every minute.
	m.run()

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("[*] Migrator running — tick every %s", tickInterval)

	for {
		select {
		case <-ticker.C:
			m.run()
		case <-sigChan:
			log.Println("[!] Shutdown signal received — exiting")
			return
		}
	}
}
