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
	"github.com/ticketer/shared/events"
	"github.com/ticketer/shared/models"
)

// DLQConsumer consumes failed orders from the DLQ and performs compensation.
type DLQConsumer struct {
	reader  *kafka.Reader
	repo    *db.Repository
	redis   *redis.Client
	metrics *Metrics
}

// NewDLQConsumer creates a new DLQ consumer.
func NewDLQConsumer(
	reader *kafka.Reader,
	repo *db.Repository,
	redisClient *redis.Client,
	metrics *Metrics,
) *DLQConsumer {
	return &DLQConsumer{
		reader:  reader,
		repo:    repo,
		redis:   redisClient,
		metrics: metrics,
	}
}

// Start begins consuming DLQ messages. Should be run as a goroutine.
func (c *DLQConsumer) Start(ctx context.Context) {
	log.Println("[DLQ Consumer] Started consuming from DLQ topic")

	for {
		select {
		case <-ctx.Done():
			log.Println("[DLQ Consumer] Context cancelled, shutting down")
			return
		default:
		}

		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[DLQ Consumer] Error reading message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		c.processCompensation(ctx, msg)
	}
}

func (c *DLQConsumer) processCompensation(ctx context.Context, msg kafka.Message) {
	var event events.OrderEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("[DLQ Consumer] Failed to unmarshal DLQ message: %v", err)
		return
	}

	log.Printf("[DLQ Consumer] 🔄 Processing compensation: orderID=%s userID=%s eventID=%s",
		event.OrderID, event.UserID, event.EventID)

	// ── Step 1: Mark order as compensated in DB ──
	reason := "payment_failed_permanently"
	if err := c.repo.UpdateOrderStatus(ctx, event.OrderID, models.StatusCompensated, &reason); err != nil {
		log.Printf("[DLQ Consumer] ⚠️ Failed to update order status (will still release ticket): %v", err)
	}

	// ── Step 2: Release ticket back to inventory (CRITICAL) ──
	inventoryKey := events.InventoryKey(event.EventID)
	if err := c.redis.Incr(ctx, inventoryKey).Err(); err != nil {
		log.Printf("[DLQ Consumer] ❌ CRITICAL: Failed to release ticket to Redis: %v", err)
		// This is a serious issue — log at error level for alerting
	} else {
		log.Printf("[DLQ Consumer] ✅ Ticket released back to inventory: eventID=%s", event.EventID)
	}

	// ── Step 3: Remove user lock ──
	userLockKey := events.UserLockKey(event.EventID, event.UserID)
	if err := c.redis.Del(ctx, userLockKey).Err(); err != nil {
		log.Printf("[DLQ Consumer] ⚠️ Failed to remove user lock: %v", err)
	}

	// ── Step 4: Notify user via Redis Pub/Sub ──
	wsMsg := events.WebSocketMessage{
		Type:    "order_failed",
		OrderID: event.OrderID,
		UserID:  event.UserID,
		Status:  models.StatusFailed,
		Message: "Payment failed after multiple attempts. Your ticket has been released. You can try again.",
	}

	data, _ := json.Marshal(wsMsg)
	if err := c.redis.Publish(ctx, events.ChannelOrderFailed, string(data)).Err(); err != nil {
		log.Printf("[DLQ Consumer] Failed to publish failure notification: %v", err)
	}

	// ── Step 5: Publish inventory update ──
	remaining, _ := c.redis.Get(ctx, inventoryKey).Int()
	invMsg := events.WebSocketMessage{
		Type:    "inventory_update",
		Message: fmt.Sprintf("Ticket released! %d tickets now available.", remaining),
		Data:    map[string]int{"remaining": remaining},
	}
	invData, _ := json.Marshal(invMsg)
	c.redis.Publish(ctx, events.ChannelInventoryUpdate, string(invData))

	c.metrics.CompensationOps.Inc()
	log.Printf("[DLQ Consumer] ✅ Compensation complete for order %s", event.OrderID)
}
