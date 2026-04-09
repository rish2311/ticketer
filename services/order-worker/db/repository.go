package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ticketer/shared/models"
)

// Repository handles PostgreSQL operations for orders.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new Repository with a connection pool.
func NewRepository(ctx context.Context, dsn string) (*Repository, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	// Retry connection
	var pool *pgxpool.Pool
	for i := 0; i < 30; i++ {
		pool, err = pgxpool.NewWithConfig(ctx, config)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				log.Println("✅ Connected to PostgreSQL (via PgBouncer)")
				break
			}
		}
		log.Printf("⏳ Waiting for PostgreSQL... (%d/30)", i+1)
		time.Sleep(2 * time.Second)
	}
	if pool == nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL after 30 attempts")
	}

	return &Repository{pool: pool}, nil
}

// CreateOrder inserts a new order. Uses ON CONFLICT for idempotency.
// Returns true if a new row was inserted, false if it already existed.
func (r *Repository) CreateOrder(ctx context.Context, order *models.Order) (bool, error) {
	query := `
		INSERT INTO orders (id, event_id, user_id, idempotency_key, status, retry_count, failure_reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id
	`

	var returnedID string
	err := r.pool.QueryRow(ctx, query,
		order.ID,
		order.EventID,
		order.UserID,
		order.IdempotencyKey,
		order.Status,
		order.RetryCount,
		order.FailureReason,
	).Scan(&returnedID)

	if err != nil {
		// If no rows returned, it means the idempotency key already exists
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, fmt.Errorf("failed to create order: %w", err)
	}

	return true, nil
}

// UpdateOrderStatus updates the status of an order.
func (r *Repository) UpdateOrderStatus(ctx context.Context, orderID, status string, failureReason *string) error {
	query := `
		UPDATE orders
		SET status = $2, failure_reason = $3, updated_at = NOW()
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, query, orderID, status, failureReason)
	if err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}
	return nil
}

// GetOrderByIdempotencyKey retrieves an order by its idempotency key.
func (r *Repository) GetOrderByIdempotencyKey(ctx context.Context, key string) (*models.Order, error) {
	query := `
		SELECT id, event_id, user_id, idempotency_key, status, retry_count, failure_reason, created_at, updated_at
		FROM orders
		WHERE idempotency_key = $1
	`

	order := &models.Order{}
	err := r.pool.QueryRow(ctx, query, key).Scan(
		&order.ID,
		&order.EventID,
		&order.UserID,
		&order.IdempotencyKey,
		&order.Status,
		&order.RetryCount,
		&order.FailureReason,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get order: %w", err)
	}

	return order, nil
}

// CountConfirmedOrders returns the count of confirmed orders for an event.
func (r *Repository) CountConfirmedOrders(ctx context.Context, eventID string) (int, error) {
	query := `SELECT COUNT(*) FROM orders WHERE event_id = $1 AND status = 'confirmed'`
	var count int
	err := r.pool.QueryRow(ctx, query, eventID).Scan(&count)
	return count, err
}

// Close closes the connection pool.
func (r *Repository) Close() {
	r.pool.Close()
}
