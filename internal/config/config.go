package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration
type Config struct {
	App       AppConfig
	HTTP      HTTPConfig
	Redis     RedisConfig
	RabbitMQ  RabbitMQConfig
	Queues    QueuesConfig
	MNO       MNOConfig
	RateLimit RateLimitConfig
	Priority  PriorityConfig
}

// AppConfig holds application-level configuration
type AppConfig struct {
	Env         string
	LogLevel    string
	WorkerCount int
}

// HTTPConfig holds HTTP server configuration
type HTTPConfig struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	Host     string
	Port     int
	Password string
	DB       int
}

// RabbitMQConfig holds RabbitMQ connection configuration
type RabbitMQConfig struct {
	URL           string
	PrefetchCount int
	ReconnectWait time.Duration
}

// QueuesConfig holds queue names configuration
type QueuesConfig struct {
	// Input queues (comma-separated list from env)
	InputQueues []string

	// Output queues
	SaveToDBQueue   string
	RetryQueue      string
	DeadLetterQueue string

	// GatewayQueueName is the queue that uses the primary DLR URLs.
	// Messages from any other input queue use the _API_V2 DLR URLs.
	GatewayQueueName string
}

// MNOConfig holds all MNO-specific configuration
type MNOConfig struct {
	SafaricomSDP  SDPConfig
	SafaricomSMPP SMPPConfig
	Airtel        SMPPConfig
	AirtelPromo   SMPPConfig
	Telkom        SMPPConfig
	Equitel       SMPPConfig
	CM            SMPPConfig
}

// SDPConfig holds Safaricom SDP configuration
type SDPConfig struct {
	AuthURL     string
	SendURL     string
	AuthUser    string
	Username    string
	Password    string
	DLRURL      string
	DLRURLApiV2 string
	TokenKey    string
	TokenTTL    time.Duration
}

// SMPPConfig holds SMPP (Kannel) gateway configuration
type SMPPConfig struct {
	URL         string
	SMSC        string
	Username    string
	Password    string
	DLRURL      string
	DLRURLApiV2 string
}

// RateLimitConfig holds per-network rate limit configuration
type RateLimitConfig struct {
	Safaricom int
	Airtel    int
	Telkom    int
	Equitel   int
	CM        int
	Default   int
}

// PriorityConfig holds message prioritization settings
type PriorityConfig struct {
	// Enabled turns on priority scheduling
	Enabled bool

	// RedisWeightsKey is the Redis key for queue weights (hash)
	RedisWeightsKey string

	// DefaultQueueWeights are initial weights if Redis is empty
	// Format from env: "QUEUE1:10,QUEUE2:5,QUEUE3:1"
	DefaultQueueWeights map[string]int

	// DefaultWeight for queues not explicitly configured
	DefaultWeight int

	// TransactionalWorkers is the number of dedicated workers for transactional fast-path
	TransactionalWorkers int

	// Credit-Based Weighted Round Robin (WRR) Configuration
	// CreditMultiplier determines credits per weight unit (e.g., 10 means weight=5 gets 50 credits)
	CreditMultiplier int

	// RefillPeriodMs is the credit refill interval in milliseconds
	RefillPeriodMs int

	// MaxStarvationAgeSec is the max seconds without processing before starvation prevention kicks in
	MaxStarvationAgeSec int
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		App: AppConfig{
			Env:         getEnv("APP_ENV", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
			WorkerCount: getEnvAsInt("WORKER_COUNT", 10),
		},
		HTTP: HTTPConfig{
			Port:         getEnvAsInt("HTTP_PORT", 8080),
			ReadTimeout:  getEnvAsDuration("HTTP_READ_TIMEOUT", 30*time.Second),
			WriteTimeout: getEnvAsDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
			IdleTimeout:  getEnvAsDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		},
		Redis: RedisConfig{
			Host:     getEnv("REDIS_HOST", "localhost"),
			Port:     getEnvAsInt("REDIS_PORT", 6379),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvAsInt("REDIS_DB", 0),
		},
		RabbitMQ: RabbitMQConfig{
			URL:           getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			PrefetchCount: getEnvAsInt("RABBITMQ_PREFETCH", 10),
			ReconnectWait: getEnvAsDuration("RABBITMQ_RECONNECT_WAIT", 5*time.Second),
		},
		Queues: QueuesConfig{
			InputQueues:      getEnvAsStringSlice("INPUT_QUEUES", []string{"TITANIC-KE_SMS_QUEUE", "CONSUME_TO_MNO"}),
			SaveToDBQueue:    getEnv("SAVE_TO_DB_QUEUE", "SAVE_TO_DB"),
			RetryQueue:       getEnv("SMS_RETRY_QUEUE", "SMS_RETRY_QUEUE"),
			DeadLetterQueue:  getEnv("SMS_DEAD_LETTER_QUEUE", "SMS_DEAD_LETTER_QUEUE"),
			GatewayQueueName: getEnv("GATEWAY_QUEUE_NAME", "SMS_MNO_GATEWAY_QUEUE"),
		},
		MNO: MNOConfig{
			SafaricomSDP: SDPConfig{
				AuthURL:  getEnv("SDP_AUTH_URL", "https://dsvc2.safaricom.com:9480/api/auth/login"),
				SendURL:  getEnv("SDP_SEND_URL", "https://dsvc2.safaricom.com:9480/api/public/CMS/bulksms"),
				AuthUser: getEnv("SDP_USERNAME", getEnv("SDP_USER", "")),
				Username: getEnv("SDP_USER", getEnv("SDP_USERNAME", "")),
				Password: getEnv("SDP_PASSWORD", ""),
				DLRURL:      getEnv("SDP_DLR_URL", "https://smsdlr.emalify.com/save"),
				DLRURLApiV2: getEnv("SDP_DLR_URL_API_V2", getEnv("SDP_DLR_URL", "https://smsdlr.emalify.com/save")),
				TokenKey:    getEnv("SDP_TOKEN_KEY", "SDP_TOKEN_KEY"),
				TokenTTL: getEnvAsDuration("SDP_TOKEN_TTL", 25*time.Minute),
			},
			SafaricomSMPP: SMPPConfig{
				URL:         getEnv("SAFARICOM_SMPP_URL", "http://10.0.0.87:80/cgi-bin/sendsms"),
				SMSC:        getEnv("SAFARICOM_SMPP_SMSC", "SAFARICOM"),
				Username:    getEnv("SAFARICOM_SMPP_USERNAME", ""),
				Password:    getEnv("SAFARICOM_SMPP_PASSWORD", ""),
				DLRURL:      getEnv("SAFARICOM_SMPP_DLR_URL", "http://10.0.0.100:8088/save"),
				DLRURLApiV2: getEnv("SAFARICOM_SMPP_DLR_URL_API_V2", getEnv("SAFARICOM_SMPP_DLR_URL", "http://10.0.0.100:8088/save")),
			},
			Airtel: SMPPConfig{
				URL:         getEnv("AIRTEL_SMPP_URL", "http://10.0.0.88:14013/cgi-bin/sendsms"),
				SMSC:        getEnv("AIRTEL_SMPP_SMSC", "AIRTEL"),
				Username:    getEnv("AIRTEL_SMPP_USERNAME", ""),
				Password:    getEnv("AIRTEL_SMPP_PASSWORD", ""),
				DLRURL:      getEnv("AIRTEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save"),
				DLRURLApiV2: getEnv("AIRTEL_SMPP_DLR_URL_API_V2", getEnv("AIRTEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save")),
			},
			AirtelPromo: SMPPConfig{
				URL:         getEnv("AIRTEL_SMPP_URL_PROMO", getEnv("AIRTEL_SMPP_URL", "http://10.0.0.88:14013/cgi-bin/sendsms")),
				SMSC:        getEnv("AIRTEL_SMPP_SMSC_PROMO", getEnv("AIRTEL_SMPP_SMSC", "AIRTEL")),
				Username:    getEnv("AIRTEL_SMPP_USERNAME_PROMO", getEnv("AIRTEL_SMPP_USERNAME", "")),
				Password:    getEnv("AIRTEL_SMPP_PASSWORD_PROMO", getEnv("AIRTEL_SMPP_PASSWORD", "")),
				DLRURL:      getEnv("AIRTEL_SMPP_DLR_URL_PROMO", getEnv("AIRTEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save")),
				DLRURLApiV2: getEnv("AIRTEL_SMPP_DLR_URL_PROMO_API_V2", getEnv("AIRTEL_SMPP_DLR_URL_PROMO", getEnv("AIRTEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save"))),
			},
			Telkom: SMPPConfig{
				URL:         getEnv("TELKOM_SMPP_URL", "http://34.77.25.98:14013/cgi-bin/sendsms"),
				SMSC:        getEnv("TELKOM_SMPP_SMSC", "TELKOM"),
				Username:    getEnv("TELKOM_SMPP_USERNAME", ""),
				Password:    getEnv("TELKOM_SMPP_PASSWORD", ""),
				DLRURL:      getEnv("TELKOM_SMPP_DLR_URL", "http://197.248.69.107:48088/save"),
				DLRURLApiV2: getEnv("TELKOM_SMPP_DLR_URL_API_V2", getEnv("TELKOM_SMPP_DLR_URL", "http://197.248.69.107:48088/save")),
			},
			Equitel: SMPPConfig{
				URL:         getEnv("EQUITEL_SMPP_URL", "http://10.0.0.87:80/cgi-bin/sendsms"),
				SMSC:        getEnv("EQUITEL_SMPP_SMSC", "EQUITEL"),
				Username:    getEnv("EQUITEL_SMPP_USERNAME", ""),
				Password:    getEnv("EQUITEL_SMPP_PASSWORD", ""),
				DLRURL:      getEnv("EQUITEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save"),
				DLRURLApiV2: getEnv("EQUITEL_SMPP_DLR_URL_API_V2", getEnv("EQUITEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save")),
			},
			CM: SMPPConfig{
				URL:      getEnv("CM_SMPP_URL", "http://34.77.25.98:14013/cgi-bin/sendsms"),
				SMSC:     getEnv("CM_SMPP_SMSC", "CM"),
				Username: getEnv("CM_SMPP_USERNAME", ""),
				Password: getEnv("CM_SMPP_PASSWORD", ""),
				DLRURL:   getEnv("CM_SMPP_DLR_URL", "http://10.0.0.100:8088/save"),
			},
		},
		RateLimit: RateLimitConfig{
			Safaricom: getEnvAsInt("RATE_LIMIT_SAFARICOM", 200),
			Airtel:    getEnvAsInt("RATE_LIMIT_AIRTEL", 50),
			Telkom:    getEnvAsInt("RATE_LIMIT_TELKOM", 100),
			Equitel:   getEnvAsInt("RATE_LIMIT_EQUITEL", 20),
			CM:        getEnvAsInt("RATE_LIMIT_CM", 20),
			Default:   getEnvAsInt("RATE_LIMIT_DEFAULT", 20),
		},
		Priority: PriorityConfig{
			Enabled:         getEnvAsBool("PRIORITY_ROUTING_ENABLED", false),
			RedisWeightsKey: getEnv("PRIORITY_REDIS_WEIGHTS_KEY", "sms:priority:queue_weights"),
			DefaultQueueWeights: getEnvAsQueueWeights("PRIORITY_DEFAULT_WEIGHTS", map[string]int{
				"GOLD_PARTNERS_QUEUE":   10,
				"BETIKA_GOLD":           10,
				"PEPETA_GOLD":           10,
				"TITANIC-KE_SMS_QUEUE":  1,
				"CONSUME_TO_MNO":        1,
				"SMS_MNO_GATEWAY_QUEUE": 1,
			}),
			DefaultWeight:        getEnvAsInt("PRIORITY_DEFAULT_WEIGHT", 1),
			TransactionalWorkers: getEnvAsInt("PRIORITY_TRANSACTIONAL_WORKERS", 5),
			// Credit-Based WRR configuration
			CreditMultiplier:    getEnvAsInt("PRIORITY_CREDIT_MULTIPLIER", 10),      // 10 credits per weight unit
			RefillPeriodMs:      getEnvAsInt("PRIORITY_REFILL_PERIOD_MS", 100),      // 100ms refill interval
			MaxStarvationAgeSec: getEnvAsInt("PRIORITY_MAX_STARVATION_AGE_SEC", 10), // 10 seconds
		},
	}
}

// Helper functions for environment variables

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvAsStringSlice(key string, defaultValue []string) []string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		parts := strings.Split(value, ",")
		result := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				result = append(result, trimmed)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

// getEnvAsQueueWeights parses queue weights from env var format: "QUEUE1:10,QUEUE2:5"
func getEnvAsQueueWeights(key string, defaultValue map[string]int) map[string]int {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		result := make(map[string]int)
		pairs := strings.Split(value, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(parts) == 2 {
				queueName := strings.TrimSpace(parts[0])
				if weight, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil && queueName != "" {
					result[queueName] = weight
				}
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultValue
}

// RedisAddr returns the Redis address in host:port format
func (c *RedisConfig) Addr() string {
	return c.Host + ":" + strconv.Itoa(c.Port)
}

// IsProduction returns true if running in production environment
func (c *AppConfig) IsProduction() bool {
	return c.Env == "production"
}

// IsDevelopment returns true if running in development environment
func (c *AppConfig) IsDevelopment() bool {
	return c.Env == "development"
}
