package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"

	"github.com/emalify/emalify-sms-mno-gateway/internal/bootstrap"
	"github.com/emalify/emalify-sms-mno-gateway/internal/config"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		logrus.Warn("No .env file found, using environment variables")
	}

	// Initialize early logger for startup
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

	// Load configuration
	cfg := config.Load()

	// Create and initialize the application
	app, err := bootstrap.New(cfg)
	if err != nil {
		log.WithError(err).Fatal("Failed to initialize application")
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the application (consumers and HTTP server)
	if err := app.Start(ctx); err != nil {
		log.WithError(err).Fatal("Failed to start application")
	}

	log.Info("Application started successfully")

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	log.WithField("signal", sig.String()).Info("Received shutdown signal, initiating graceful shutdown...")

	// Cancel context to signal all goroutines to stop
	cancel()

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Graceful shutdown
	if err := app.Shutdown(shutdownCtx); err != nil {
		log.WithError(err).Error("Error during shutdown")
	}

	log.Info("Shutdown complete")
}
