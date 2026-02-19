package config

import (
	"os"
	"strconv"
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
	// Input queues
	TitanicKESMSQueue string
	ConsumeToMNOQueue string

	// Output queues
	SaveToDBQueue   string
	RetryQueue      string
	DeadLetterQueue string
}

// MNOConfig holds all MNO-specific configuration
type MNOConfig struct {
	SafaricomSDP  SDPConfig
	SafaricomSMPP SMPPConfig
	Airtel        SMPPConfig
	Telkom        SMPPConfig
	Equitel       SMPPConfig
	CM            SMPPConfig
}

// SDPConfig holds Safaricom SDP configuration
type SDPConfig struct {
	AuthURL  string
	SendURL  string
	Username string
	Password string
	DLRURL   string
	TokenKey string
	TokenTTL time.Duration
}

// SMPPConfig holds SMPP (Kannel) gateway configuration
type SMPPConfig struct {
	URL      string
	SMSC     string
	Username string
	Password string
	DLRURL   string
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
			TitanicKESMSQueue: getEnv("TITANIC_KE_SMS_QUEUE", "TITANIC-KE_SMS_QUEUE"),
			ConsumeToMNOQueue: getEnv("CONSUME_TO_MNO_QUEUE", "CONSUME_TO_MNO"),
			SaveToDBQueue:     getEnv("SAVE_TO_DB_QUEUE", "SAVE_TO_DB"),
			RetryQueue:        getEnv("SMS_RETRY_QUEUE", "SMS_RETRY_QUEUE"),
			DeadLetterQueue:   getEnv("SMS_DEAD_LETTER_QUEUE", "SMS_DEAD_LETTER_QUEUE"),
		},
		MNO: MNOConfig{
			SafaricomSDP: SDPConfig{
				AuthURL:  getEnv("SDP_AUTH_URL", "https://dsvc2.safaricom.com:9480/api/auth/login"),
				SendURL:  getEnv("SDP_SEND_URL", "https://dsvc2.safaricom.com:9480/api/public/CMS/bulksms"),
				Username: getEnv("SDP_USERNAME", ""),
				Password: getEnv("SDP_PASSWORD", ""),
				DLRURL:   getEnv("SDP_DLR_URL", "https://smsdlr.emalify.com/save"),
				TokenKey: getEnv("SDP_TOKEN_KEY", "SDP_TOKEN_KEY"),
				TokenTTL: getEnvAsDuration("SDP_TOKEN_TTL", 25*time.Minute),
			},
			SafaricomSMPP: SMPPConfig{
				URL:      getEnv("SAFARICOM_SMPP_URL", "http://10.0.0.87:80/cgi-bin/sendsms"),
				SMSC:     getEnv("SAFARICOM_SMPP_SMSC", "SAFARICOM"),
				Username: getEnv("SAFARICOM_SMPP_USERNAME", ""),
				Password: getEnv("SAFARICOM_SMPP_PASSWORD", ""),
				DLRURL:   getEnv("SAFARICOM_SMPP_DLR_URL", "http://10.0.0.100:8088/save"),
			},
			Airtel: SMPPConfig{
				URL:      getEnv("AIRTEL_SMPP_URL", "http://10.0.0.88:14013/cgi-bin/sendsms"),
				SMSC:     getEnv("AIRTEL_SMPP_SMSC", "AIRTEL"),
				Username: getEnv("AIRTEL_SMPP_USERNAME", ""),
				Password: getEnv("AIRTEL_SMPP_PASSWORD", ""),
				DLRURL:   getEnv("AIRTEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save"),
			},
			Telkom: SMPPConfig{
				URL:      getEnv("TELKOM_SMPP_URL", "http://34.77.25.98:14013/cgi-bin/sendsms"),
				SMSC:     getEnv("TELKOM_SMPP_SMSC", "TELKOM"),
				Username: getEnv("TELKOM_SMPP_USERNAME", ""),
				Password: getEnv("TELKOM_SMPP_PASSWORD", ""),
				DLRURL:   getEnv("TELKOM_SMPP_DLR_URL", "http://197.248.69.107:48088/save"),
			},
			Equitel: SMPPConfig{
				URL:      getEnv("EQUITEL_SMPP_URL", "http://10.0.0.87:80/cgi-bin/sendsms"),
				SMSC:     getEnv("EQUITEL_SMPP_SMSC", "EQUITEL"),
				Username: getEnv("EQUITEL_SMPP_USERNAME", ""),
				Password: getEnv("EQUITEL_SMPP_PASSWORD", ""),
				DLRURL:   getEnv("EQUITEL_SMPP_DLR_URL", "http://10.0.0.100:8088/save"),
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

func getEnvAsBool(key string, defaultValue bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
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
