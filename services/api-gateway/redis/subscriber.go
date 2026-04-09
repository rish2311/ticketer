package redisclient

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/ticketer/shared/events"
)

// Subscriber listens to Redis Pub/Sub for order status updates
// and forwards them to a message channel.
type Subscriber struct {
	client  *redis.Client
	msgChan chan events.WebSocketMessage
}

// NewSubscriber creates a Redis Pub/Sub subscriber.
func NewSubscriber(client *redis.Client, msgChan chan events.WebSocketMessage) *Subscriber {
	return &Subscriber{
		client:  client,
		msgChan: msgChan,
	}
}

// Start begins listening on Redis Pub/Sub channels.
// This should be run as a goroutine.
func (s *Subscriber) Start(ctx context.Context) {
	pubsub := s.client.Subscribe(ctx,
		events.ChannelOrderConfirmed,
		events.ChannelOrderFailed,
		events.ChannelInventoryUpdate,
	)
	defer pubsub.Close()

	log.Println("[Redis Subscriber] Listening on channels:", events.ChannelOrderConfirmed, events.ChannelOrderFailed, events.ChannelInventoryUpdate)

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			log.Println("[Redis Subscriber] Context cancelled, shutting down")
			return
		case msg, ok := <-ch:
			if !ok {
				log.Println("[Redis Subscriber] Channel closed")
				return
			}

			var wsMsg events.WebSocketMessage
			if err := json.Unmarshal([]byte(msg.Payload), &wsMsg); err != nil {
				log.Printf("[Redis Subscriber] Failed to unmarshal message: %v", err)
				continue
			}

			// Forward to WebSocket hub
			select {
			case s.msgChan <- wsMsg:
			default:
				log.Println("[Redis Subscriber] Message channel full, dropping message")
			}
		}
	}
}
