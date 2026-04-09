package events

import "time"

// Event types
const (
	OrderPending   = "OrderPending"
	OrderConfirmed = "OrderConfirmed"
	OrderFailed    = "OrderFailed"
)

// OrderEvent is the message contract between API Gateway and Order Worker.
// Published to Kafka and consumed by workers.
type OrderEvent struct {
	EventType      string    `json:"event_type"`
	OrderID        string    `json:"order_id"`
	UserID         string    `json:"user_id"`
	EventID        string    `json:"event_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	RetryCount     int       `json:"retry_count"`
	Timestamp      time.Time `json:"timestamp"`
	TraceID        string    `json:"trace_id"`
}

// WebSocketMessage is sent from the server to connected clients
// via WebSocket for real-time order status updates.
type WebSocketMessage struct {
	Type    string      `json:"type"`
	OrderID string      `json:"order_id"`
	UserID  string      `json:"user_id"`
	Status  string      `json:"status"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Redis Pub/Sub channels
const (
	ChannelOrderConfirmed = "order:confirmed"
	ChannelOrderFailed    = "order:failed"
	ChannelInventoryUpdate = "inventory:update"
)

// Redis key patterns
const (
	InventoryKeyPrefix = "inventory:"
	UserLockKeyPrefix  = "lock:"
)

// InventoryKey returns the Redis key for an event's inventory.
func InventoryKey(eventID string) string {
	return InventoryKeyPrefix + eventID
}

// UserLockKey returns the Redis key for a per-user lock on an event.
func UserLockKey(eventID, userID string) string {
	return UserLockKeyPrefix + eventID + ":" + userID
}
