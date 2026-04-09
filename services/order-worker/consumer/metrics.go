package consumer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the Order Worker.
type Metrics struct {
	OrdersProcessed    *prometheus.CounterVec
	ProcessingDuration prometheus.Histogram
	PaymentDuration    prometheus.Histogram
	DLQMessages        prometheus.Counter
	CompensationOps    prometheus.Counter
}

// NewMetrics creates and registers all worker metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		OrdersProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "orders_processed_total",
				Help: "Total orders processed by status (confirmed, retry, dlq, duplicate, poison)",
			},
			[]string{"status"},
		),

		ProcessingDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "order_processing_duration_seconds",
				Help:    "Duration of order processing in seconds",
				Buckets: prometheus.DefBuckets,
			},
		),

		PaymentDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "payment_simulation_duration_seconds",
				Help:    "Duration of payment simulation in seconds",
				Buckets: []float64{0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0},
			},
		),

		DLQMessages: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "dlq_messages_total",
				Help: "Total messages sent to the Dead Letter Queue",
			},
		),

		CompensationOps: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "compensation_operations_total",
				Help: "Total compensation operations performed (ticket releases)",
			},
		),
	}
}
