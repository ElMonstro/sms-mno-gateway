package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		logrus.Warn("No .env file found, using environment variables")
	}

	// Initialize logger
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	log.SetLevel(level)

	log.Info("Starting emalify-sms-mno-gateway...")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO: Initialize components
	// 1. Config
	// 2. Redis client
	// 3. RabbitMQ connection
	// 4. HTTP client (pooled)
	// 5. Metrics
	// 6. Rate limiter
	// 7. Circuit breakers
	// 8. MNO sender factory
	// 9. Router
	// 10. Result handler
	// 11. Processor
	// 12. HTTP server
	// 13. Start consumers
	// 14. Start HTTP server

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.WithField("signal", sig.String()).Info("Received shutdown signal")
		cancel()
	case <-ctx.Done():
		log.Info("Context cancelled")
	}

	// TODO: Graceful shutdown
	// 1. Stop accepting new messages
	// 2. Wait for in-flight workers (30s timeout)
	// 3. Close connections

	log.Info("Shutdown complete")
}
