package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/ticketer/order-worker/db"
	"github.com/ticketer/order-worker/payment"
	"github.com/ticketer/shared/config"
	"github.com/ticketer/shared/events"
	"github.com/ticketer/shared/models"
)

// OrderConsumer consumes OrderPending events from Kafka and processes them.
type OrderConsumer struct {
	reader      *kafka.Reader
	dlqWriter   *kafka.Writer
	retryWriter *kafka.Writer
	repo        *db.Repository
	redis       *redis.Client
	payment     *payment.Simulator
	cfg         *config.Config
	metrics     *Metrics
}

// NewOrderConsumer creates a new Kafka consumer for order processing.
func NewOrderConsumer(
	reader *kafka.Reader,
	dlqWriter *kafka.Writer,
	retryWriter *kafka.Writer,
	repo *db.Repository,
	redisClient *redis.Client,
	paymentSim *payment.Simulator,
	cfg *config.Config,
	metrics *Metrics,
) *OrderConsumer {
	return &OrderConsumer{
		reader:      reader,
		dlqWriter:   dlqWriter,
		retryWriter: retryWriter,
		repo:        repo,
		redis:       redisClient,
		payment:     paymentSim,
		cfg:         cfg,
		metrics:     metrics,
	}
}

// Start begins consuming messages. Should be run as a goroutine.
func (c *OrderConsumer) Start(ctx context.Context) {
	log.Println("[Consumer] Started consuming from topic:", c.cfg.KafkaTopicOrders)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Consumer] Context cancelled, shutting down")
			return
		default:
		}

		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Consumer] Error reading message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *OrderConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	start := time.Now()

	// Deserialize event
	var event events.OrderEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("[Consumer] Failed to unmarshal message (poison): %v", err)
		// Poison message → send to DLQ directly
		c.sendToDLQ(ctx, msg.Value)
		c.metrics.OrdersProcessed.WithLabelValues("poison").Inc()
		return
	}

	log.Printf("[Consumer] Processing order: orderID=%s userID=%s attempt=%d",
		event.OrderID, event.UserID, event.RetryCount+1)

	// ── Step 1: Idempotency Check ──
	existing, err := c.repo.GetOrderByIdempotencyKey(ctx, event.IdempotencyKey)
	if err != nil {
		log.Printf("[Consumer] DB idempotency check failed: %v", err)
		// Don't commit; retry later
		return
	}
	if existing != nil && existing.Status == models.StatusConfirmed {
		log.Printf("[Consumer] Order already confirmed (idempotent skip): %s", existing.ID)
		c.metrics.OrdersProcessed.WithLabelValues("duplicate").Inc()
		return
	}

	// ── Step 2: Create Order Record (pending) ──
	order := &models.Order{
		ID:             event.OrderID,
		EventID:        event.EventID,
		UserID:         event.UserID,
		IdempotencyKey: event.IdempotencyKey,
		Status:         models.StatusPending,
		RetryCount:     event.RetryCount,
	}
	c.repo.CreateOrder(ctx, order)

	// ── Step 3: Simulate Payment ──
	paymentCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	paymentStart := time.Now()
	result := c.payment.ProcessPayment(paymentCtx, event.OrderID)
	c.metrics.PaymentDuration.Observe(time.Since(paymentStart).Seconds())

	if result.Success {
		// ── SUCCESS PATH ──
		if err := c.repo.UpdateOrderStatus(ctx, event.OrderID, models.StatusConfirmed, nil); err != nil {
			log.Printf("[Consumer] Failed to update order to confirmed: %v", err)
			return
		}

		// Publish confirmation via Redis Pub/Sub
		c.publishStatusUpdate(ctx, event, models.StatusConfirmed, "Payment successful! Your ticket is confirmed. 🎉")

		c.metrics.OrdersProcessed.WithLabelValues("confirmed").Inc()
		log.Printf("[Consumer] ✅ Order confirmed: %s (%.2fms)", event.OrderID, time.Since(start).Seconds()*1000)

	} else {
		// ── FAILURE PATH ──
		failureReason := result.Reason
		event.RetryCount++

		if event.RetryCount < c.cfg.MaxRetries {
			// Retry with exponential backoff
			backoff := time.Duration(500*(1<<event.RetryCount)) * time.Millisecond
			log.Printf("[Consumer] ⚠️ Payment failed (attempt %d/%d), retrying in %v: %s",
				event.RetryCount, c.cfg.MaxRetries, backoff, failureReason)

			time.Sleep(backoff)

			// Re-publish to the same topic for retry
			retryEvent, _ := json.Marshal(event)
			if err := c.retryWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(event.EventID),
				Value: retryEvent,
			}); err != nil {
				log.Printf("[Consumer] Failed to publish retry: %v", err)
			}

			c.metrics.OrdersProcessed.WithLabelValues("retry").Inc()
		} else {
			// Max retries exceeded → DLQ
			log.Printf("[Consumer] ❌ Max retries exceeded, sending to DLQ: orderID=%s reason=%s",
				event.OrderID, failureReason)

			if err := c.repo.UpdateOrderStatus(ctx, event.OrderID, models.StatusFailed, &failureReason); err != nil {
				log.Printf("[Consumer] Failed to update order to failed: %v", err)
			}

			dlqEvent, _ := json.Marshal(event)
			c.sendToDLQ(ctx, dlqEvent)

			c.metrics.OrdersProcessed.WithLabelValues("dlq").Inc()
			c.metrics.DLQMessages.Inc()
		}
	}

	c.metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
}

func (c *OrderConsumer) sendToDLQ(ctx context.Context, data []byte) {
	if err := c.dlqWriter.WriteMessages(ctx, kafka.Message{
		Value: data,
	}); err != nil {
		log.Printf("[Consumer] Failed to send to DLQ: %v", err)
	}
}

func (c *OrderConsumer) publishStatusUpdate(ctx context.Context, event events.OrderEvent, status, message string) {
	wsMsg := events.WebSocketMessage{
		Type:    fmt.Sprintf("order_%s", status),
		OrderID: event.OrderID,
		UserID:  event.UserID,
		Status:  status,
		Message: message,
	}

	data, err := json.Marshal(wsMsg)
	if err != nil {
		log.Printf("[Consumer] Failed to marshal WS message: %v", err)
		return
	}

	channel := events.ChannelOrderConfirmed
	if status == models.StatusFailed {
		channel = events.ChannelOrderFailed
	}

	if err := c.redis.Publish(ctx, channel, string(data)).Err(); err != nil {
		log.Printf("[Consumer] Failed to publish to Redis: %v", err)
	}
}
