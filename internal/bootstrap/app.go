package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
	SDPHTTPClient   *httpclient.Client
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
	TransactionalRetryConsumer  *rabbitmq.Consumer
	PromotionalRetryConsumer    *rabbitmq.Consumer
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

	// DynamicConfig provides hot-reloadable config stored in Redis.
	// See internal/sms/ports/dynamic_config.go for namespaces and fields.
	DynamicConfig ports.DynamicConfigStore
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

	// 3. Initialize HTTP clients (pooled)
	app.HTTPClient = httpclient.New(httpclient.DefaultConfig())
	sdpCfg := httpclient.DefaultConfig()
	sdpCfg.ResponseHeaderTimeout = cfg.MNO.SafaricomSDP.ResponseHeaderTimeout
	sdpCfg.Timeout = cfg.MNO.SafaricomSDP.RequestTimeout
	app.SDPHTTPClient = httpclient.New(sdpCfg)
	app.Logger.WithFields(map[string]interface{}{
		"response_header_timeout": cfg.MNO.SafaricomSDP.ResponseHeaderTimeout,
		"request_timeout":         cfg.MNO.SafaricomSDP.RequestTimeout,
	}).Info("HTTP clients initialized (shared + SDP-specific)")

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

	// 6.5. Initialize dynamic config store (hot-reloadable settings backed by Redis)
	dc, err := redis.NewDynamicConfig(app.RedisCache.Client(), app.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to init dynamic config: %w", err)
	}
	app.DynamicConfig = dc

	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer seedCancel()
	_ = dc.Seed(seedCtx, ports.NSRetry, map[string]any{
		ports.FieldMaxRetriesTransactional: cfg.Retry.MaxRetriesTransactional,
		ports.FieldMaxRetriesPromotional:   cfg.Retry.MaxRetriesPromotional,
	})
	_ = dc.Seed(seedCtx, ports.NSRateLimits, map[string]any{
		ports.FieldRateSafaricom: cfg.RateLimit.Safaricom,
		ports.FieldRateAirtel:    cfg.RateLimit.Airtel,
		ports.FieldRateTelkom:    cfg.RateLimit.Telkom,
		ports.FieldRateEquitel:   cfg.RateLimit.Equitel,
		ports.FieldRateCM:        cfg.RateLimit.CM,
		ports.FieldRateDefault:   cfg.RateLimit.Default,
	})
	_ = dc.Seed(seedCtx, ports.NSSchedulerPromotional, map[string]any{
		ports.FieldCreditMultiplier:    cfg.Priority.CreditMultiplier,
		ports.FieldRefillPeriodMs:      cfg.Priority.RefillPeriodMs,
		ports.FieldMaxStarvationAgeSec: cfg.Priority.MaxStarvationAgeSec,
	})
	app.Logger.Info("Dynamic config store initialized and defaults seeded")

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
		SDPHTTPClient:   app.SDPHTTPClient,
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
		DynamicConfig:           dc,
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
			DynamicConfig:    dc,
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
	// Wire dynamic config watchers (rate limiter + scheduler config hot-reload)
	if app.DynamicConfig != nil {
		app.RateLimiter.WatchDynamicConfig(ctx, app.DynamicConfig)
	}

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

	// Start DLQ migrator as an isolated background goroutine
	app.startDLQMigrator(ctx)

	// Start HTTP server in a goroutine
	go func() {
		if err := app.HTTPServer.Start(); err != nil {
			app.Logger.WithError(err).Error("HTTP server error")
		}
	}()

	return nil
}

// startDLQMigrator launches the DLQ migrator as an isolated background goroutine.
// It restarts automatically on channel/connection loss. Disabled via DLQ_MIGRATOR_ENABLED=false.
func (app *App) startDLQMigrator(ctx context.Context) {
	if !app.Config.Queues.DLQMigratorEnabled {
		app.Logger.Info("DLQ migrator disabled — skipping")
		return
	}

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			if err := app.runDLQMigratorOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				app.Logger.WithError(err).Warn("DLQ migrator: restarting in 5s")
				app.Metrics.IncDLQMigratorChannelRestart()
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}

// runDLQMigratorOnce opens a channel, consumes from the DLQ, and routes each
// delivery until the channel closes or ctx is cancelled.
// Returns nil on clean shutdown, non-nil on any connection/channel error.
func (app *App) runDLQMigratorOnce(ctx context.Context) error {
	ch, err := app.RabbitConn.NewChannel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	destQueue := app.Config.Queues.DLQMigratorDestQueue
	permQueue := app.Config.Queues.DLQPermQueue
	for _, q := range []string{destQueue, permQueue} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare %s: %w", q, err)
		}
	}

	dlq := app.Config.Queues.DeadLetterQueue
	msgs, err := ch.Consume(dlq, "dlq-migrator", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", dlq, err)
	}

	app.Logger.Infof("DLQ migrator started — listening on %s", dlq)

	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("channel closed")
			}

			dest := permQueue
			// Try single-message path first (common case: Publish marshals one *domain.Message as object)
			var single domain.Message
			if err := json.Unmarshal(d.Body, &single); err == nil {
				if single.Description == domain.ErrMaxRetriesExceeded.Error() {
					dest = destQueue
				}
			} else {
				// Fall back to batch path (PublishBatch marshals []*domain.Message as array)
				var batch []domain.Message
				if err := json.Unmarshal(d.Body, &batch); err == nil {
					for _, m := range batch {
						if m.Description == domain.ErrMaxRetriesExceeded.Error() {
							dest = destQueue
							break
						}
					}
				}
			}

			if err := ch.PublishWithContext(ctx, "", dest, false, false, amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Body:         d.Body,
			}); err != nil {
				app.Logger.WithError(err).Error("DLQ migrator: publish failed — requeuing")
				app.Metrics.IncDLQMigratorPublishError()
				d.Nack(false, true)
				continue
			}

			app.Logger.Infof("DLQ migrator: forwarded delivery to %s", dest)
			app.Metrics.IncDLQMigratorForwarded(dest)
			d.Ack(false)

		case <-ctx.Done():
			app.Logger.Info("DLQ migrator stopped")
			return nil
		}
	}
}

// accumulateBatch collects up to maxMsgs messages across successive deliveries.
// It blocks on the first delivery, then non-blocking drains further deliveries until
// maxMsgs is reached or the channel has no immediately-available messages.
// Returns nil slices when ctx is cancelled or the deliveries channel is closed.
//
// Note: if a single delivery contains more messages than maxMsgs, the returned
// batch will exceed maxMsgs. Individual deliveries are never split — all messages
// inside one AMQP delivery must be acked or nacked together.
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

	// Close dynamic config watchers
	if app.DynamicConfig != nil {
		if err := app.DynamicConfig.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing dynamic config store")
		}
	}

	// Close Redis
	if app.RedisCache != nil {
		if err := app.RedisCache.Close(); err != nil {
			app.Logger.WithError(err).Warn("Error closing Redis connection")
		}
	}

	// Close HTTP clients
	if app.HTTPClient != nil {
		app.HTTPClient.Close()
	}
	if app.SDPHTTPClient != nil {
		app.SDPHTTPClient.Close()
	}

	app.Logger.Info("Application shutdown complete")
	return nil
}
