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

	// Dedicated retry consumers and processors — isolated from main queue processing
	TransactionalRetryConsumer *rabbitmq.Consumer
	PromotionalRetryConsumer   *rabbitmq.Consumer
	TransactionalRetryProcessor *service.Processor
	PromotionalRetryProcessor   *service.Processor

	// Priority routing components (optional, enabled via PRIORITY_ROUTING_ENABLED)
	PriorityStore        ports.PriorityStore
	TransactionalHandler *service.TransactionalHandler
	PriorityScheduler    *service.PriorityScheduler
	MessageRouter        *service.MessageRouter

	// Per-queue processors keyed by queue name (em-1102).
	// Queues listed in QUEUE_SDP_BATCH_SIZES get a dedicated processor with that batch size.
	// All other queues fall back to Processor.
	QueueProcessors map[string]*service.Processor
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

	// 4. Initialize rate limiter with main budgets, then attach retry budgets
	app.RateLimiter = ratelimit.New(&ratelimit.Config{
		Safaricom: cfg.RateLimit.Safaricom,
		Airtel:    cfg.RateLimit.Airtel,
		Telkom:    cfg.RateLimit.Telkom,
		Equitel:   cfg.RateLimit.Equitel,
		CM:        cfg.RateLimit.CM,
		Default:   cfg.RateLimit.Default,
	}).WithRetryConfig(&ratelimit.RetryConfig{
		SafaricomSDP:  cfg.Retry.RateLimitSafaricomSDP,
		SafaricomSMPP: cfg.Retry.RateLimitSafaricomSMPP,
		Airtel:        cfg.Retry.RateLimitAirtel,
		Equitel:       cfg.Retry.RateLimitEquitel,
		Telkom:        cfg.Retry.RateLimitTelkom,
		CM:            cfg.Retry.RateLimitCM,
		BurstFactor:   cfg.Retry.BurstFactor,
	})
	app.Logger.Info("Rate limiter initialized with main and retry budgets")

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

	// 8. Initialize RabbitMQ publisher (declares all queues including delay queues with TTL+DLX)
	app.Publisher, err = rabbitmq.NewPublisher(&rabbitmq.PublisherConfig{
		Connection: app.RabbitConn,
		Queues: ports.QueueConfig{
			SaveToDBQueue:           cfg.Queues.SaveToDBQueue,
			RetryQueue:              cfg.Queues.RetryQueue,
			DeadLetterQueue:         cfg.Queues.DeadLetterQueue,
			TransactionalDelayQueue: cfg.Queues.TransactionalDelayQueue,
			PromotionalDelayQueue:   cfg.Queues.PromotionalDelayQueue,
			TransactionalRetryQueue: cfg.Queues.TransactionalRetryQueue,
			PromotionalRetryQueue:   cfg.Queues.PromotionalRetryQueue,
		},
		GatewayQueueName:     cfg.Queues.GatewayQueueName,
		TransactionalDelayMs: cfg.Retry.TransactionalDelayMs,
		PromotionalDelayMs:   cfg.Retry.PromotionalDelayMs,
		Logger:               app.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize publisher: %w", err)
	}
	app.Logger.Info("RabbitMQ publisher initialized with delay queues")

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

	// 11. Initialize result handler with per-type retry limits
	app.ResultHandler = service.NewResultHandler(&service.ResultHandlerConfig{
		Publisher:               app.Publisher,
		Metrics:                 app.Metrics,
		MaxRetriesTransactional: cfg.Retry.MaxRetriesTransactional,
		MaxRetriesPromotional:   cfg.Retry.MaxRetriesPromotional,
		Logger:                  app.Logger,
	})

	// 12. Initialize processor (default — uses global SDP_PROMO_BATCH_SIZE)
	app.Processor = service.NewProcessor(&service.ProcessorConfig{
		Router:        app.Router,
		ResultHandler: app.ResultHandler,
		RateLimiter:   app.RateLimiter,
		Metrics:       app.Metrics,
		WorkerCount:   cfg.App.WorkerCount,
		SDPBatchSize:  cfg.MNO.SafaricomSDP.PromoSDPBatchSize,
		Logger:        app.Logger,
	})
	app.Logger.Infof("Message processor initialized with %d workers", cfg.App.WorkerCount)

	// 12a. Per-queue processors for queues with explicit SDP batch sizes (QUEUE_SDP_BATCH_SIZES).
	app.QueueProcessors = make(map[string]*service.Processor)
	for queueName, batchSize := range cfg.Queues.SDPBatchSizes {
		app.QueueProcessors[queueName] = service.NewProcessor(&service.ProcessorConfig{
			Router:        app.Router,
			ResultHandler: app.ResultHandler,
			RateLimiter:   app.RateLimiter,
			Metrics:       app.Metrics,
			WorkerCount:   cfg.App.WorkerCount,
			SDPBatchSize:  batchSize,
			Logger:        app.Logger,
		})
		app.Logger.WithFields(map[string]interface{}{
			"queue":      queueName,
			"batch_size": batchSize,
		}).Info("Per-queue SDP processor initialized")
	}

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

	// 15. Initialize dedicated retry consumers and processors
	// Each retry pool has its own prefetch and worker budget, isolated from main queues.
	app.TransactionalRetryConsumer, err = rabbitmq.NewConsumer(&rabbitmq.ConsumerConfig{
		Connection: app.RabbitConn,
		QueueName:  cfg.Queues.TransactionalRetryQueue,
		Prefetch:   cfg.Retry.TransactionalPrefetch,
		Logger:     app.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional retry consumer: %w", err)
	}
	app.Logger.Infof("Transactional retry consumer initialized (queue: %s, prefetch: %d)",
		cfg.Queues.TransactionalRetryQueue, cfg.Retry.TransactionalPrefetch)

	app.PromotionalRetryConsumer, err = rabbitmq.NewConsumer(&rabbitmq.ConsumerConfig{
		Connection: app.RabbitConn,
		QueueName:  cfg.Queues.PromotionalRetryQueue,
		Prefetch:   cfg.Retry.PromotionalPrefetch,
		Logger:     app.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create promotional retry consumer: %w", err)
	}
	app.Logger.Infof("Promotional retry consumer initialized (queue: %s, prefetch: %d)",
		cfg.Queues.PromotionalRetryQueue, cfg.Retry.PromotionalPrefetch)

	app.TransactionalRetryProcessor = service.NewProcessor(&service.ProcessorConfig{
		Router:        app.Router,
		ResultHandler: app.ResultHandler,
		RateLimiter:   app.RateLimiter,
		Metrics:       app.Metrics,
		WorkerCount:   cfg.Retry.TransactionalWorkerCount,
		IsRetry:       true,
		Logger:        app.Logger,
	})
	app.Logger.Infof("Transactional retry processor initialized with %d workers", cfg.Retry.TransactionalWorkerCount)

	app.PromotionalRetryProcessor = service.NewProcessor(&service.ProcessorConfig{
		Router:        app.Router,
		ResultHandler: app.ResultHandler,
		RateLimiter:   app.RateLimiter,
		Metrics:       app.Metrics,
		WorkerCount:   cfg.Retry.PromotionalWorkerCount,
		IsRetry:       true,
		Logger:        app.Logger,
	})
	app.Logger.Infof("Promotional retry processor initialized with %d workers", cfg.Retry.PromotionalWorkerCount)

	// 17. Initialize HTTP server
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

	// processorFor returns the queue-specific processor if one was configured via
	// QUEUE_SDP_BATCH_SIZES; otherwise falls back to the default processor.
	processorFor := func(queueName string) *service.Processor {
		if p, ok := app.QueueProcessors[queueName]; ok {
			return p
		}
		return app.Processor
	}

	// Start consuming from all queues
	for _, consumer := range app.Consumers {
		deliveries, err := consumer.Consume(ctx)
		if err != nil {
			return fmt.Errorf("failed to start consumer for %s: %w", consumer.QueueName(), err)
		}

		queueName := consumer.QueueName()
		processor := processorFor(queueName)

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
		} else if batchSize := app.Config.Queues.SDPBatchSizes[queueName]; batchSize > 1 {
			// SDP batching path: accumulate up to batchSize deliveries before making one API call.
			// Each upstream publish is one AMQP message, so accumulation must happen here —
			// the Processor alone cannot see across delivery boundaries.
			app.Logger.WithFields(map[string]interface{}{
				"queue":      queueName,
				"batch_size": batchSize,
				"workers":    concurrency,
			}).Info("Starting SDP batching processors")
			for i := 0; i < concurrency; i++ {
				go func(queueName string, deliveries <-chan ports.Delivery, p *service.Processor, bs int) {
					for {
						msgs, deliveryBatch := accumulateBatch(ctx, deliveries, bs)
						if len(msgs) == 0 {
							app.Logger.Infof("SDP batching processor stopped for queue: %s", queueName)
							return
						}

						_, err := p.ProcessMessages(ctx, msgs)
						if err != nil {
							app.Logger.WithError(err).Errorf("Failed to handle SDP batch results for %s, nacking %d deliveries", queueName, len(deliveryBatch))
							for _, d := range deliveryBatch {
								_ = d.Nack(true)
							}
							continue
						}

						for _, d := range deliveryBatch {
							if err := d.Ack(); err != nil {
								app.Logger.WithError(err).Errorf("Failed to ack delivery from %s", queueName)
							}
						}
					}
				}(queueName, deliveries, processor, batchSize)
			}
		} else {
			// Standard single-delivery processing
			app.Logger.Infof("Starting %d delivery processors for queue: %s", concurrency, queueName)
			for i := 0; i < concurrency; i++ {
				go func(queueName string, deliveries <-chan ports.Delivery, p *service.Processor) {
					for delivery := range deliveries {
						if err := p.ProcessDelivery(ctx, delivery); err != nil {
							app.Logger.WithError(err).Error("Failed to process delivery")
						}
					}
					app.Logger.Infof("Delivery processor stopped for queue: %s", queueName)
				}(queueName, deliveries, processor)
			}
		}
	}

	// Start retry consumers with their dedicated processors
	type retryConsumerSpec struct {
		consumer  *rabbitmq.Consumer
		processor *service.Processor
		workers   int
	}
	retrySpecs := []retryConsumerSpec{
		{app.TransactionalRetryConsumer, app.TransactionalRetryProcessor, app.Config.Retry.TransactionalWorkerCount},
		{app.PromotionalRetryConsumer, app.PromotionalRetryProcessor, app.Config.Retry.PromotionalWorkerCount},
	}
	for _, spec := range retrySpecs {
		deliveries, err := spec.consumer.Consume(ctx)
		if err != nil {
			return fmt.Errorf("failed to start retry consumer for %s: %w", spec.consumer.QueueName(), err)
		}
		queueName := spec.consumer.QueueName()
		processor := spec.processor
		app.Logger.Infof("Starting %d retry delivery processors for queue: %s", spec.workers, queueName)
		for i := 0; i < spec.workers; i++ {
			go func(queueName string, deliveries <-chan ports.Delivery) {
				for delivery := range deliveries {
					if err := processor.ProcessDelivery(ctx, delivery); err != nil {
						app.Logger.WithError(err).Errorf("Failed to process retry delivery from %s", queueName)
					}
				}
				app.Logger.Infof("Retry delivery processor stopped for queue: %s", queueName)
			}(queueName, deliveries)
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

// accumulateBatch collects up to maxMsgs messages across successive deliveries.
// It blocks on the first delivery, then non-blocking drains further deliveries until
// maxMsgs is reached or the channel has no immediately-available messages.
// Returns nil slices when ctx is cancelled or the deliveries channel is closed.
func accumulateBatch(ctx context.Context, deliveries <-chan ports.Delivery, maxMsgs int) ([]*domain.Message, []ports.Delivery) {
	var msgs []*domain.Message
	var batch []ports.Delivery

	// Block until at least one delivery arrives.
	select {
	case d, ok := <-deliveries:
		if !ok {
			return nil, nil
		}
		msgs = append(msgs, d.Messages()...)
		batch = append(batch, d)
	case <-ctx.Done():
		return nil, nil
	}

	// Non-blocking drain: accumulate additional deliveries that are already queued.
	for len(msgs) < maxMsgs {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return msgs, batch
			}
			msgs = append(msgs, d.Messages()...)
			batch = append(batch, d)
		default:
			return msgs, batch
		}
	}

	return msgs, batch
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

	// Close main consumers
	for _, consumer := range app.Consumers {
		if err := consumer.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing consumer")
		}
	}

	// Close retry consumers
	for _, consumer := range []*rabbitmq.Consumer{app.TransactionalRetryConsumer, app.PromotionalRetryConsumer} {
		if consumer != nil {
			if err := consumer.Close(); err != nil {
				app.Logger.WithError(err).Warn("Error closing retry consumer")
			}
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
