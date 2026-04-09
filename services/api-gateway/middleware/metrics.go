package middleware

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

var (
	// BookingRequestsTotal counts total booking requests by status.
	BookingRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "booking_requests_total",
			Help: "Total number of booking requests by status",
		},
		[]string{"status"},
	)

	// BookingRequestDuration measures booking request latency.
	BookingRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "booking_request_duration_seconds",
			Help:    "Duration of booking requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status_code"},
	)

	// RedisGatekeeperDuration measures Redis gatekeeper operation latency.
	RedisGatekeeperDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "redis_gatekeeper_duration_seconds",
			Help:    "Duration of Redis gatekeeper operations in seconds",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// KafkaPublishDuration measures Kafka publish latency.
	KafkaPublishDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kafka_publish_duration_seconds",
			Help:    "Duration of Kafka publish operations in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		},
	)

	// WebSocketConnectionsActive tracks active WebSocket connections.
	WebSocketConnectionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "websocket_connections_active",
			Help: "Number of active WebSocket connections",
		},
	)

	// RedisInventoryCurrent tracks current Redis inventory.
	RedisInventoryCurrent = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "redis_inventory_current",
			Help: "Current ticket inventory in Redis",
		},
	)

	// HTTPRequestsInFlight tracks requests currently being processed.
	HTTPRequestsInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Number of HTTP requests currently being processed",
		},
	)
)

// PrometheusMiddleware records HTTP request metrics.
func PrometheusMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		HTTPRequestsInFlight.Inc()

		err := c.Next()

		HTTPRequestsInFlight.Dec()
		duration := time.Since(start).Seconds()

		statusCode := strconv.Itoa(c.Response().StatusCode())
		BookingRequestDuration.WithLabelValues(
			c.Method(),
			c.Path(),
			statusCode,
		).Observe(duration)

		return err
	}
}

// MetricsHandler serves the Prometheus metrics endpoint.
func MetricsHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		handler := fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
		handler(c.Context())
		return nil
	}
}
