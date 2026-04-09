package config

import (
	"os"
	"strconv"
)

// Config holds all application configuration loaded from environment variables.
type Config struct {
	// Redis
	RedisHost     string
	RedisPort     string
	RedisPassword string

	// Kafka
	KafkaBroker     string
	KafkaTopicOrders string
	KafkaTopicDLQ    string
	KafkaGroupID     string

	// Database (via PgBouncer)
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string

	// API Gateway
	APIPort          string
	TicketInventory  int
	LockTTLSeconds   int

	// Worker
	PaymentSuccessRate int
	MaxRetries         int
	ConsumerCount      int
	WorkerMetricsPort  string

	// Observability
	JaegerEndpoint string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	return &Config{
		// Redis
		RedisHost:     getEnv("REDIS_HOST", "localhost"),
		RedisPort:     getEnv("REDIS_PORT", "6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),

		// Kafka
		KafkaBroker:      getEnv("KAFKA_BROKER", "localhost:9092"),
		KafkaTopicOrders:  getEnv("KAFKA_TOPIC_ORDERS", "order-pending"),
		KafkaTopicDLQ:     getEnv("KAFKA_TOPIC_DLQ", "order-dlq"),
		KafkaGroupID:      getEnv("KAFKA_GROUP_ID", "order-workers"),

		// Database
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "6432"),
		DBUser:     getEnv("DB_USER", "ticketer"),
		DBPassword: getEnv("DB_PASSWORD", "ticketer_secret"),
		DBName:     getEnv("DB_NAME", "ticketer_db"),

		// API Gateway
		APIPort:         getEnv("API_PORT", "8080"),
		TicketInventory: getEnvInt("TICKET_INVENTORY", 100),
		LockTTLSeconds:  getEnvInt("LOCK_TTL_SECONDS", 300),

		// Worker
		PaymentSuccessRate: getEnvInt("WORKER_PAYMENT_SUCCESS_RATE", 80),
		MaxRetries:         getEnvInt("WORKER_MAX_RETRIES", 3),
		ConsumerCount:      getEnvInt("WORKER_CONSUMER_COUNT", 5),
		WorkerMetricsPort:  getEnv("WORKER_METRICS_PORT", "9091"),

		// Observability
		JaegerEndpoint: getEnv("JAEGER_ENDPOINT", "http://localhost:4317"),
	}
}

// RedisAddr returns the full Redis address.
func (c *Config) RedisAddr() string {
	return c.RedisHost + ":" + c.RedisPort
}

// DatabaseDSN returns the PostgreSQL connection string (via PgBouncer).
func (c *Config) DatabaseDSN() string {
	return "postgres://" + c.DBUser + ":" + c.DBPassword +
		"@" + c.DBHost + ":" + c.DBPort + "/" + c.DBName + "?sslmode=disable"
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
