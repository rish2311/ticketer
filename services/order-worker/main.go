package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/ticketer/order-worker/consumer"
	"github.com/ticketer/order-worker/db"
	"github.com/ticketer/order-worker/payment"
	"github.com/ticketer/order-worker/telemetry"
	"github.com/ticketer/shared/config"
)

func main() {
	log.Println("🚀 Starting Order Worker...")

	// ── Load Config ──
	cfg := config.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Initialize OpenTelemetry Tracer ──
	tp, err := telemetry.InitTracer(ctx, cfg.JaegerEndpoint)
	if err != nil {
		log.Printf("[WARNING] Failed to initialize tracer: %v (continuing without tracing)", err)
	}

	// ── Connect to PostgreSQL (via PgBouncer) ──
	repo, err := db.NewRepository(ctx, cfg.DatabaseDSN())
	if err != nil {
		log.Fatalf("❌ Failed to connect to database: %v", err)
	}
	defer repo.Close()

	// ── Connect to Redis ──
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr(),
		Password: cfg.RedisPassword,
		DB:       0,
		PoolSize: 20,
	})

	for i := 0; i < 30; i++ {
		if err := redisClient.Ping(ctx).Err(); err == nil {
			log.Println("✅ Connected to Redis")
			break
		}
		log.Printf("⏳ Waiting for Redis... (%d/30)", i+1)
		time.Sleep(2 * time.Second)
	}

	// ── Initialize Metrics ──
	metrics := consumer.NewMetrics()

	// ── Initialize Payment Simulator ──
	paymentSim := payment.NewSimulator(cfg.PaymentSuccessRate)

	// ── Create Kafka components ──
	// Main consumer (reading order-pending topic)
	orderReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaBroker},
		Topic:          cfg.KafkaTopicOrders,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	// DLQ writer
	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBroker),
		Topic:        cfg.KafkaTopicDLQ,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}

	// Retry writer (back to the same topic)
	retryWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBroker),
		Topic:        cfg.KafkaTopicOrders,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
	}

	// DLQ consumer
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaBroker},
		Topic:          cfg.KafkaTopicDLQ,
		GroupID:        cfg.KafkaGroupID + "-dlq",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	// ── Start Main Consumer ──
	orderConsumer := consumer.NewOrderConsumer(
		orderReader, dlqWriter, retryWriter,
		repo, redisClient, paymentSim, cfg, metrics,
	)

	// Start multiple consumer goroutines
	for i := 0; i < cfg.ConsumerCount; i++ {
		go orderConsumer.Start(ctx)
	}
	log.Printf("✅ Started %d order consumer goroutines", cfg.ConsumerCount)

	// ── Start DLQ Consumer ──
	dlqConsumer := consumer.NewDLQConsumer(dlqReader, repo, redisClient, metrics)
	go dlqConsumer.Start(ctx)
	log.Println("✅ DLQ consumer started")

	// ── Expose Metrics Server ──
	metricsServer := &http.Server{
		Addr:    ":" + cfg.WorkerMetricsPort,
		Handler: promhttp.Handler(),
	}
	go func() {
		log.Printf("📊 Metrics server listening on :%s", cfg.WorkerMetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// ── Graceful Shutdown ──
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Shutting down Order Worker...")
	cancel()

	if err := orderReader.Close(); err != nil {
		log.Printf("Error closing order reader: %v", err)
	}
	if err := dlqReader.Close(); err != nil {
		log.Printf("Error closing DLQ reader: %v", err)
	}
	if err := dlqWriter.Close(); err != nil {
		log.Printf("Error closing DLQ writer: %v", err)
	}
	if err := retryWriter.Close(); err != nil {
		log.Printf("Error closing retry writer: %v", err)
	}
	if err := redisClient.Close(); err != nil {
		log.Printf("Error closing Redis: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	metricsServer.Shutdown(shutdownCtx)

	if tp != nil {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error shutting down tracer: %v", err)
		}
	}

	log.Println("👋 Order Worker stopped")
}
