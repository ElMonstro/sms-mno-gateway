package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/sony/gobreaker"

	"github.com/emalify/emalify-sms-mno-gateway/internal/api"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/circuitbreaker"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/httpclient"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/metrics"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/ratelimit"
	"github.com/emalify/emalify-sms-mno-gateway/internal/config"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/adapters/mno"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/adapters/rabbitmq"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/adapters/redis"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/domain"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/ports"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/service"
)

// App holds all application components
type App struct {
	Config          *config.Config
	Logger          logger.Logger
	Metrics         *metrics.PrometheusMetrics
	HTTPClient      *httpclient.Client
	RateLimiter     *ratelimit.Limiter
	BreakerRegistry *circuitbreaker.BreakerRegistry
	RedisCache      *redis.TokenCache
	RabbitConn      *rabbitmq.Connection
	Publisher       *rabbitmq.Publisher
	Consumers       []*rabbitmq.Consumer
	MNOFactory      *mno.Factory
	Router          *service.Router
	ResultHandler   *service.ResultHandler
	Processor       *service.Processor
	HTTPServer      *api.Server

	// Priority routing components (optional, enabled via PRIORITY_ROUTING_ENABLED)
	PriorityStore        ports.PriorityStore
	TransactionalHandler *service.TransactionalHandler
	PriorityScheduler    *service.PriorityScheduler
	MessageRouter        *service.MessageRouter
}

// New creates and initializes all application components
func New(cfg *config.Config) (*App, error) {
	app := &App{
		Config: cfg,
	}

	var err error

	// 1. Initialize logger
	app.Logger = logger.New(cfg.App.LogLevel)
	app.Logger.Info("Initializing application...")

	// 2. Initialize metrics
	app.Metrics = metrics.New("emalify_sms")

	// 3. Initialize HTTP client (pooled)
	app.HTTPClient = httpclient.New(httpclient.DefaultConfig())
	app.Logger.Info("HTTP client initialized with connection pooling")

	// 4. Initialize rate limiter
	app.RateLimiter = ratelimit.New(&ratelimit.Config{
		Safaricom: cfg.RateLimit.Safaricom,
		Airtel:    cfg.RateLimit.Airtel,
		Telkom:    cfg.RateLimit.Telkom,
		Equitel:   cfg.RateLimit.Equitel,
		CM:        cfg.RateLimit.CM,
		Default:   cfg.RateLimit.Default,
	})
	app.Logger.Info("Rate limiter initialized")

	// 5. Initialize circuit breakers
	app.BreakerRegistry = circuitbreaker.NewRegistry()
	networks := []domain.Network{
		domain.NetworkSafaricom,
		domain.NetworkAirtel,
		domain.NetworkTelkom,
		domain.NetworkEquitel,
		domain.NetworkCM,
	}
	for _, network := range networks {
		app.BreakerRegistry.Register(network, &circuitbreaker.Config{
			Name:                network.String(),
			MaxRequests:         3,
			Timeout:             30000000000, // 30 seconds in nanoseconds
			ConsecutiveFailures: 5,
			OnStateChange: func(name string, from, to gobreaker.State) {
				app.Logger.WithFields(map[string]interface{}{
					"circuit":    name,
					"from_state": circuitbreaker.StateString(from),
					"to_state":   circuitbreaker.StateString(to),
				}).Warn("Circuit breaker state changed")
				app.Metrics.SetCircuitBreakerState(domain.ParseNetwork(name), circuitbreaker.StateString(to))
				if to == gobreaker.StateOpen {
					app.Metrics.IncCircuitBreakerTrips(domain.ParseNetwork(name))
				}
			},
		})
	}
	app.Logger.Info("Circuit breakers initialized for all networks")

	// 6. Initialize Redis (token cache)
	app.RedisCache, err = redis.NewTokenCache(&redis.TokenCacheConfig{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		Logger:   app.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Redis: %w", err)
	}
	app.Logger.Info("Redis token cache initialized")

	// 7. Initialize RabbitMQ connection
	app.RabbitConn, err = rabbitmq.NewConnection(&rabbitmq.ConnectionConfig{
		URL:           cfg.RabbitMQ.URL,
		ReconnectWait: cfg.RabbitMQ.ReconnectWait,
		Logger:        app.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	app.Logger.Info("RabbitMQ connection established")

	// 8. Initialize RabbitMQ publisher
	app.Publisher, err = rabbitmq.NewPublisher(&rabbitmq.PublisherConfig{
		Connection: app.RabbitConn,
		Queues: ports.QueueConfig{
			SaveToDBQueue:   cfg.Queues.SaveToDBQueue,
			RetryQueue:      cfg.Queues.RetryQueue,
			DeadLetterQueue: cfg.Queues.DeadLetterQueue,
		},
		Logger: app.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize publisher: %w", err)
	}
	app.Logger.Info("RabbitMQ publisher initialized")

	// 9. Initialize MNO sender factory
	app.MNOFactory = mno.NewFactory(&mno.FactoryConfig{
		Config:          cfg,
		HTTPClient:      app.HTTPClient,
		TokenCache:      app.RedisCache,
		BreakerRegistry: app.BreakerRegistry,
		Metrics:         app.Metrics,
		Logger:          app.Logger,
	})
	app.Logger.Info("MNO sender factory initialized with all networks")

	// 10. Initialize router
	app.Router = service.NewRouter(app.MNOFactory, app.Logger)

	// 11. Initialize result handler
	app.ResultHandler = service.NewResultHandler(&service.ResultHandlerConfig{
		Publisher:  app.Publisher,
		Metrics:    app.Metrics,
		MaxRetries: 10,
		Logger:     app.Logger,
	})

	// 12. Initialize processor
	app.Processor = service.NewProcessor(&service.ProcessorConfig{
		Router:        app.Router,
		ResultHandler: app.ResultHandler,
		RateLimiter:   app.RateLimiter,
		Metrics:       app.Metrics,
		WorkerCount:   cfg.App.WorkerCount,
		Logger:        app.Logger,
	})
	app.Logger.Infof("Message processor initialized with %d workers", cfg.App.WorkerCount)

	// 13. Initialize priority routing (if enabled)
	if cfg.Priority.Enabled {
		app.Logger.Info("Priority routing enabled, initializing components...")

		// Initialize priority store (Redis-backed)
		app.PriorityStore, err = redis.NewPriorityStore(&redis.PriorityStoreConfig{
			Client:         app.RedisCache.Client(),
			WeightsKey:     cfg.Priority.RedisWeightsKey,
			DefaultWeights: cfg.Priority.DefaultQueueWeights,
			Logger:         app.Logger,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize priority store: %w", err)
		}
		app.Logger.Info("Priority store initialized")

		// Initialize transactional handler (fast-path)
		app.TransactionalHandler = service.NewTransactionalHandler(&service.TransactionalHandlerConfig{
			Router:        app.Router,
			ResultHandler: app.ResultHandler,
			RateLimiter:   app.RateLimiter,
			Metrics:       app.Metrics,
			WorkerCount:   cfg.Priority.TransactionalWorkers,
			Logger:        app.Logger,
		})
		app.Logger.Infof("Transactional handler initialized with %d workers", cfg.Priority.TransactionalWorkers)

		// Initialize priority scheduler (Credit-Based WRR for promotional)
		app.PriorityScheduler = service.NewPriorityScheduler(&service.PrioritySchedulerConfig{
			PriorityStore:    app.PriorityStore,
			Processor:        app.Processor,
			Metrics:          app.Metrics,
			DefaultWeight:    cfg.Priority.DefaultWeight,
			CreditMultiplier: cfg.Priority.CreditMultiplier,
			RefillPeriod:     time.Duration(cfg.Priority.RefillPeriodMs) * time.Millisecond,
			MaxStarvationAge: time.Duration(cfg.Priority.MaxStarvationAgeSec) * time.Second,
			Logger:           app.Logger,
		})
		app.Logger.WithFields(map[string]interface{}{
			"credit_multiplier":  cfg.Priority.CreditMultiplier,
			"refill_period_ms":   cfg.Priority.RefillPeriodMs,
			"max_starvation_sec": cfg.Priority.MaxStarvationAgeSec,
		}).Info("Priority scheduler initialized with Credit-Based WRR")

		// Initialize message router
		app.MessageRouter = service.NewMessageRouter(&service.MessageRouterConfig{
			TransactionalHandler: app.TransactionalHandler,
			Scheduler:            app.PriorityScheduler,
			Processor:            app.Processor,
			Metrics:              app.Metrics,
			Logger:               app.Logger,
		})
		app.Logger.Info("Message router initialized")
	}

	// 14. Initialize consumers for input queues
	for _, queueName := range cfg.Queues.InputQueues {
		consumer, err := rabbitmq.NewConsumer(&rabbitmq.ConsumerConfig{
			Connection: app.RabbitConn,
			QueueName:  queueName,
			Prefetch:   cfg.RabbitMQ.PrefetchCount,
			Logger:     app.Logger,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer for %s: %w", queueName, err)
		}
		app.Consumers = append(app.Consumers, consumer)
		app.Logger.Infof("Consumer initialized for queue: %s", queueName)
	}

	// 14. Initialize HTTP server
	app.HTTPServer = api.NewServer(&api.ServerConfig{
		Port:         cfg.HTTP.Port,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
		RabbitConn:   app.RabbitConn,
		RedisCache:   app.RedisCache,
		Logger:       app.Logger,
	})
	app.Logger.Info("HTTP server initialized")

	app.Logger.Info("Application initialization complete")
	return app, nil
}

// Start starts all consumers and the HTTP server
func (app *App) Start(ctx context.Context) error {
	// Start priority components if enabled
	if app.Config.Priority.Enabled {
		app.Logger.Info("Starting priority routing components...")

		// Start transactional handler workers
		app.TransactionalHandler.Start()

		// Start priority scheduler (weight watcher only - no queue consumption)
		// Note: The scheduler no longer consumes from queue channels directly.
		// Instead, MessageRouter calls scheduler.ProcessMessages() for promotional messages.
		if err := app.PriorityScheduler.Start(); err != nil {
			return fmt.Errorf("failed to start priority scheduler: %w", err)
		}
	}

	// Start consuming from all queues
	for _, consumer := range app.Consumers {
		deliveries, err := consumer.Consume(ctx)
		if err != nil {
			return fmt.Errorf("failed to start consumer for %s: %w", consumer.QueueName(), err)
		}

		queueName := consumer.QueueName()

		// Fan out delivery processing across WorkerCount goroutines per queue.
		// All goroutines read from the same deliveries channel (safe — Go channels are concurrent-safe).
		// This ensures the worker pool is actually used concurrently rather than fed sequentially.
		concurrency := app.Config.App.WorkerCount
		if app.Config.Priority.Enabled && app.MessageRouter != nil {
			// Priority routing enabled - use MessageRouter
			app.Logger.Infof("Starting %d delivery processors for queue: %s (priority routing)", concurrency, queueName)
			for i := 0; i < concurrency; i++ {
				go func(queueName string, deliveries <-chan ports.Delivery) {
					for delivery := range deliveries {
						if err := app.MessageRouter.RouteDelivery(ctx, delivery, queueName); err != nil {
							app.Logger.WithError(err).Error("Failed to route delivery")
						}
					}
					app.Logger.Infof("Delivery processor stopped for queue: %s", queueName)
				}(queueName, deliveries)
			}
		} else {
			// Standard processing - use Processor directly
			app.Logger.Infof("Starting %d delivery processors for queue: %s", concurrency, queueName)
			for i := 0; i < concurrency; i++ {
				go func(queueName string, deliveries <-chan ports.Delivery) {
					for delivery := range deliveries {
						if err := app.Processor.ProcessDelivery(ctx, delivery); err != nil {
							app.Logger.WithError(err).Error("Failed to process delivery")
						}
					}
					app.Logger.Infof("Delivery processor stopped for queue: %s", queueName)
				}(queueName, deliveries)
			}
		}
	}

	// Start HTTP server in a goroutine
	go func() {
		if err := app.HTTPServer.Start(); err != nil {
			app.Logger.WithError(err).Error("HTTP server error")
		}
	}()

	return nil
}

// Shutdown gracefully shuts down all components
func (app *App) Shutdown(ctx context.Context) error {
	app.Logger.Info("Shutting down application...")

	// Shutdown HTTP server
	if app.HTTPServer != nil {
		if err := app.HTTPServer.Shutdown(ctx); err != nil {
			app.Logger.WithError(err).Warn("Error shutting down HTTP server")
		}
	}

	// Stop priority components first (before closing consumers)
	if app.TransactionalHandler != nil {
		app.TransactionalHandler.Stop()
		app.Logger.Info("Transactional handler stopped")
	}
	if app.PriorityScheduler != nil {
		app.PriorityScheduler.Stop()
		app.Logger.Info("Priority scheduler stopped")
	}

	// Close consumers
	for _, consumer := range app.Consumers {
		if err := consumer.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing consumer")
		}
	}

	// Close publisher
	if app.Publisher != nil {
		if err := app.Publisher.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing publisher")
		}
	}

	// Close RabbitMQ connection
	if app.RabbitConn != nil {
		if err := app.RabbitConn.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing RabbitMQ connection")
		}
	}

	// Close priority store (before Redis since it may use the shared client)
	if app.PriorityStore != nil {
		if err := app.PriorityStore.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing priority store")
		}
	}

	// Close Redis
	if app.RedisCache != nil {
		if err := app.RedisCache.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing Redis connection")
		}
	}

	// Close HTTP client
	if app.HTTPClient != nil {
		app.HTTPClient.Close()
	}

	app.Logger.Info("Application shutdown complete")
	return nil
}
