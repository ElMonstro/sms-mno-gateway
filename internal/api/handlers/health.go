package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/adapters/rabbitmq"
	"github.com/emalify/emalify-sms-mno-gateway/internal/sms/adapters/redis"
)

// HealthHandler handles health check endpoints
type HealthHandler struct {
	rabbitConn *rabbitmq.Connection
	redisCache *redis.TokenCache
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(rabbitConn *rabbitmq.Connection, redisCache *redis.TokenCache) *HealthHandler {
	return &HealthHandler{
		rabbitConn: rabbitConn,
		redisCache: redisCache,
	}
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components,omitempty"`
}

// Health handles the /health endpoint (basic liveness check)
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HealthResponse{Status: "ok"})
}

// Ready handles the /ready endpoint (readiness check with dependencies)
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	components := make(map[string]string)
	allHealthy := true

	// Check RabbitMQ
	if h.rabbitConn != nil && h.rabbitConn.IsConnected() {
		components["rabbitmq"] = "ok"
	} else {
		components["rabbitmq"] = "unhealthy"
		allHealthy = false
	}

	// Check Redis
	if h.redisCache != nil {
		if err := h.redisCache.Ping(r.Context()); err == nil {
			components["redis"] = "ok"
		} else {
			components["redis"] = "unhealthy"
			allHealthy = false
		}
	} else {
		components["redis"] = "not configured"
	}

	w.Header().Set("Content-Type", "application/json")

	resp := HealthResponse{
		Components: components,
	}

	if allHealthy {
		resp.Status = "ok"
		w.WriteHeader(http.StatusOK)
	} else {
		resp.Status = "unhealthy"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(resp)
}
