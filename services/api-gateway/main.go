package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/ticketer/api-gateway/handlers"
	apimiddleware "github.com/ticketer/api-gateway/middleware"
	redisclient "github.com/ticketer/api-gateway/redis"
	"github.com/ticketer/api-gateway/telemetry"
	ws "github.com/ticketer/api-gateway/websocket"
	"github.com/ticketer/shared/config"
	"github.com/ticketer/shared/events"
)

func main() {
	log.Println("🚀 Starting API Gateway...")

	// ── Load Config ──
	cfg := config.Load()

	// ── Initialize OpenTelemetry Tracer ──
	ctx := context.Background()
	tp, err := telemetry.InitTracer(ctx, cfg.JaegerEndpoint)
	if err != nil {
		log.Printf("[WARNING] Failed to initialize tracer: %v (continuing without tracing)", err)
	}

	// ── Connect to Redis ──
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr(),
		Password: cfg.RedisPassword,
		DB:       0,
		PoolSize: 50,
	})

	// Retry Redis connection
	for i := 0; i < 30; i++ {
		if err := redisClient.Ping(ctx).Err(); err == nil {
			log.Println("✅ Connected to Redis")
			break
		}
		log.Printf("⏳ Waiting for Redis... (%d/30)", i+1)
		time.Sleep(2 * time.Second)
	}

	// ── Initialize Redis Gatekeeper ──
	gatekeeper, err := redisclient.NewGatekeeper(redisClient)
	if err != nil {
		log.Fatalf("❌ Failed to initialize gatekeeper: %v", err)
	}

	// ── Initialize Kafka Writer ──
	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBroker),
		Topic:        cfg.KafkaTopicOrders,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	// ── Set up WebSocket Hub ──
	wsMsgChan := make(chan events.WebSocketMessage, 1000)
	hub := ws.NewHub(wsMsgChan)
	go hub.Run()

	// ── Start Redis Subscriber ──
	subCtx, subCancel := context.WithCancel(ctx)
	subscriber := redisclient.NewSubscriber(redisClient, wsMsgChan)
	go subscriber.Start(subCtx)

	// ── Start periodic inventory metric updater ──
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		eventID := "550e8400-e29b-41d4-a716-446655440000"
		for range ticker.C {
			inv, err := gatekeeper.GetInventory(ctx, eventID)
			if err == nil {
				apimiddleware.RedisInventoryCurrent.Set(float64(inv))
			}
			apimiddleware.WebSocketConnectionsActive.Set(float64(hub.ConnectionCount()))
		}
	}()

	// ── Create Fiber App ──
	app := fiber.New(fiber.Config{
		AppName:      "Ticketer API Gateway",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	})

	// ── Middleware ──
	app.Use(recover.New())
	app.Use(logger.New(logger.Config{
		Format: "${time} | ${status} | ${latency} | ${method} | ${path}\n",
	}))
	app.Use(apimiddleware.SetupCORS())
	app.Use(apimiddleware.RequestID())
	app.Use(apimiddleware.PrometheusMiddleware())

	// ── Routes ──
	bookingHandler := handlers.NewBookingHandler(gatekeeper, kafkaWriter, cfg)
	eventHandler := handlers.NewEventHandler(gatekeeper, cfg)
	healthHandler := handlers.NewHealthHandler(redisClient)

	// Health & Metrics
	app.Get("/health", healthHandler.HealthCheck)
	app.Get("/metrics", apimiddleware.MetricsHandler())

	// API v1
	v1 := app.Group("/api/v1")
	v1.Post("/bookings", bookingHandler.CreateBooking)
	v1.Get("/events", eventHandler.GetEvents)
	v1.Get("/events/:id", eventHandler.GetEventByID)
	v1.Post("/events/:id/start", eventHandler.StartFlashSale)

	// WebSocket endpoint
	app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	app.Get("/ws", websocket.New(func(c *websocket.Conn) {
		userID := c.Query("user_id", "anonymous")
		client := ws.NewClient(hub, c, userID)
		hub.Register() <- client

		// Start write pump in goroutine
		go client.WritePump()

		// ReadPump blocks until connection closes
		client.ReadPump()
	}))

	// ── Graceful Shutdown ──
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("🛑 Shutting down API Gateway...")

		subCancel()

		if err := kafkaWriter.Close(); err != nil {
			log.Printf("Error closing Kafka writer: %v", err)
		}

		if tp != nil {
			if err := tp.Shutdown(ctx); err != nil {
				log.Printf("Error shutting down tracer: %v", err)
			}
		}

		if err := redisClient.Close(); err != nil {
			log.Printf("Error closing Redis: %v", err)
		}

		if err := app.Shutdown(); err != nil {
			log.Printf("Error shutting down server: %v", err)
		}
	}()

	// ── Start Server ──
	addr := ":" + cfg.APIPort
	log.Printf("🌐 API Gateway listening on %s", addr)

	if err := app.Listen(addr); err != nil {
		log.Fatalf("❌ Server error: %v", err)
	}
}
