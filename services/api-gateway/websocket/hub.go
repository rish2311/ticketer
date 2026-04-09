package websocket

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	ws "github.com/gofiber/contrib/websocket"
	"github.com/ticketer/shared/events"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 30 * time.Second

	// Send pings to peer with this period (must be less than pongWait)
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer
	maxMessageSize = 512

	// Buffer size for the send channel
	sendBufferSize = 256
)

// Client represents a single WebSocket connection.
type Client struct {
	hub    *Hub
	conn   *ws.Conn
	send   chan []byte
	userID string
}

// Hub maintains the set of active clients and broadcasts messages to them.
type Hub struct {
	// Registered clients mapped by user ID
	clients map[string]*Client

	// Mutex for thread-safe client map access
	mu sync.RWMutex

	// Channel for incoming messages from Redis Pub/Sub
	inbound chan events.WebSocketMessage

	// Registration and unregistration channels
	register   chan *Client
	unregister chan *Client
}

// NewHub creates a new Hub instance.
func NewHub(inbound chan events.WebSocketMessage) *Hub {
	return &Hub{
		clients:    make(map[string]*Client),
		inbound:    inbound,
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the hub's main event loop. Must be called as a goroutine.
func (h *Hub) Run() {
	log.Println("[WebSocket Hub] Started")

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			// Close existing connection for same user (new tab replaces old)
			if existing, ok := h.clients[client.userID]; ok {
				close(existing.send)
				delete(h.clients, client.userID)
			}
			h.clients[client.userID] = client
			count := len(h.clients)
			h.mu.Unlock()
			log.Printf("[WebSocket Hub] Client registered: %s (total: %d)", client.userID, count)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.userID]; ok {
				close(client.send)
				delete(h.clients, client.userID)
			}
			count := len(h.clients)
			h.mu.Unlock()
			log.Printf("[WebSocket Hub] Client unregistered: %s (total: %d)", client.userID, count)

		case msg := <-h.inbound:
			data, err := json.Marshal(msg)
			if err != nil {
				log.Printf("[WebSocket Hub] Failed to marshal message: %v", err)
				continue
			}

			if msg.Type == "inventory_update" {
				// Broadcast inventory updates to ALL clients
				h.broadcast(data)
			} else {
				// Send order status updates to specific user
				h.sendToUser(msg.UserID, data)
				// Also broadcast as a feed event (anonymized)
				feedMsg := events.WebSocketMessage{
					Type:    "feed",
					OrderID: msg.OrderID,
					Status:  msg.Status,
					Message: msg.Message,
				}
				if feedData, err := json.Marshal(feedMsg); err == nil {
					h.broadcast(feedData)
				}
			}
		}
	}
}

// sendToUser sends a message to a specific user's WebSocket connection.
func (h *Hub) sendToUser(userID string, data []byte) {
	h.mu.RLock()
	client, ok := h.clients[userID]
	h.mu.RUnlock()

	if ok {
		select {
		case client.send <- data:
		default:
			log.Printf("[WebSocket Hub] Send buffer full for user %s, dropping", userID)
		}
	}
}

// broadcast sends a message to all connected clients.
func (h *Hub) broadcast(data []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, client := range h.clients {
		select {
		case client.send <- data:
		default:
			// Buffer full, skip
		}
	}
}

// ConnectionCount returns the number of active WebSocket connections.
func (h *Hub) ConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Register returns the registration channel.
func (h *Hub) Register() chan<- *Client {
	return h.register
}

// Unregister returns the unregistration channel.
func (h *Hub) Unregister() chan<- *Client {
	return h.unregister
}

// NewClient creates a new Client for the given connection and user.
func NewClient(hub *Hub, conn *ws.Conn, userID string) *Client {
	return &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, sendBufferSize),
		userID: userID,
	}
}

// WritePump pumps messages from the hub to the WebSocket connection.
// Must be run as a goroutine.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel
				c.conn.WriteMessage(ws.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(ws.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(ws.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ReadPump pumps messages from the WebSocket connection to the hub.
// Mainly used for ping/pong keepalive detection.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
