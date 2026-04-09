package redisclient

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/ticketer/shared/events"
)

// Gatekeeper handles atomic Redis operations for the flash sale.
type Gatekeeper struct {
	client    *redis.Client
	scriptSHA string
}

// Lua script for atomic inventory check + decrement + user lock.
// KEYS[1] = inventory key (e.g., "inventory:event_123")
// KEYS[2] = user lock key (e.g., "lock:event_123:user_456")
// ARGV[1] = lock TTL in seconds
const gatekeeperScript = `
-- 1. Idempotency: Check if the user already holds a lock
if redis.call('EXISTS', KEYS[2]) == 1 then
    return -2
end

-- 2. Check inventory
local current_inventory = tonumber(redis.call('GET', KEYS[1]) or "0")
if current_inventory > 0 then
    -- 3. Atomic decrement
    redis.call('DECR', KEYS[1])
    -- 4. Set per-user lock with TTL
    redis.call('SETEX', KEYS[2], ARGV[1], "locked")
    return 1
else
    return 0
end
`

// GatekeeperResult codes
const (
	ResultSuccess   = 1
	ResultSoldOut   = 0
	ResultDuplicate = -2
)

// NewGatekeeper creates a new Gatekeeper and loads the Lua script into Redis.
func NewGatekeeper(client *redis.Client) (*Gatekeeper, error) {
	ctx := context.Background()

	// Load the script into Redis and cache its SHA
	sha, err := client.ScriptLoad(ctx, gatekeeperScript).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load gatekeeper Lua script: %w", err)
	}

	log.Printf("[Gatekeeper] Lua script loaded, SHA: %s", sha)

	return &Gatekeeper{
		client:    client,
		scriptSHA: sha,
	}, nil
}

// TryAcquireTicket atomically attempts to reserve a ticket for a user.
// Returns: 1 (success), 0 (sold out), -2 (user already locked/duplicate)
func (g *Gatekeeper) TryAcquireTicket(ctx context.Context, eventID, userID string, lockTTL int) (int, error) {
	inventoryKey := events.InventoryKey(eventID)
	userLockKey := events.UserLockKey(eventID, userID)

	result, err := g.client.EvalSha(ctx, g.scriptSHA, []string{inventoryKey, userLockKey}, lockTTL).Int()
	if err != nil {
		// If SHA not found (Redis restarted), reload the script
		if err.Error() == "NOSCRIPT No matching script. Please use EVAL." {
			log.Println("[Gatekeeper] Script not found, reloading...")
			sha, loadErr := g.client.ScriptLoad(ctx, gatekeeperScript).Result()
			if loadErr != nil {
				return -1, fmt.Errorf("failed to reload script: %w", loadErr)
			}
			g.scriptSHA = sha
			result, err = g.client.EvalSha(ctx, g.scriptSHA, []string{inventoryKey, userLockKey}, lockTTL).Int()
			if err != nil {
				return -1, fmt.Errorf("gatekeeper eval failed after reload: %w", err)
			}
		} else {
			return -1, fmt.Errorf("gatekeeper eval failed: %w", err)
		}
	}

	return result, nil
}

// InitInventory sets the initial inventory for a flash sale event in Redis.
func (g *Gatekeeper) InitInventory(ctx context.Context, eventID string, totalTickets int) error {
	key := events.InventoryKey(eventID)
	return g.client.Set(ctx, key, totalTickets, 0).Err()
}

// GetInventory returns the current remaining inventory for an event.
func (g *Gatekeeper) GetInventory(ctx context.Context, eventID string) (int, error) {
	key := events.InventoryKey(eventID)
	val, err := g.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(val)
}

// ReleaseTicket increments inventory and removes the user lock (compensation).
func (g *Gatekeeper) ReleaseTicket(ctx context.Context, eventID, userID string) error {
	inventoryKey := events.InventoryKey(eventID)
	userLockKey := events.UserLockKey(eventID, userID)

	pipe := g.client.Pipeline()
	pipe.Incr(ctx, inventoryKey)
	pipe.Del(ctx, userLockKey)
	_, err := pipe.Exec(ctx)
	return err
}
