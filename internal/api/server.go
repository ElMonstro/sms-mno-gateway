package api

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/emalify/emalify-sms-mno-gateway/internal/api/handlers"
	"github.com/emalify/emalify-sms-mno-gateway/internal/common/logger"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/adapters/rabbitmq"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/adapters/redis"
)

// Server represents the HTTP API server
type Server struct {
	router     *chi.Mux
	httpServer *http.Server
	log        logger.Logger
}

// ServerConfig holds configuration for the HTTP server
type ServerConfig struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	RabbitConn   *rabbitmq.Connection
	RedisCache   *redis.TokenCache
	Logger       logger.Logger
}

// NewServer creates a new HTTP server
func NewServer(cfg *ServerConfig) *Server {
	router := chi.NewRouter()

	// Middleware
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(30 * time.Second))

	// Health handlers
	healthHandler := handlers.NewHealthHandler(cfg.RabbitConn, cfg.RedisCache)

	// Routes
	router.Get("/health", healthHandler.Health)
	router.Get("/ready", healthHandler.Ready)
	router.Handle("/metrics", promhttp.Handler())

	addr := ":" + string(rune(cfg.Port))
	if cfg.Port > 0 {
		addr = ":" + itoa(cfg.Port)
	}

	return &Server{
		router: router,
		httpServer: &http.Server{
			Addr:         addr,
			Handler:      router,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			IdleTimeout:  cfg.IdleTimeout,
		},
		log: cfg.Logger,
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.log.Infof("Starting HTTP server on %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("Shutting down HTTP server")
	return s.httpServer.Shutdown(ctx)
}

// Router returns the chi router for additional route registration
func (s *Server) Router() *chi.Mux {
	return s.router
}

// itoa converts int to string (avoiding strconv import for simple case)
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}
