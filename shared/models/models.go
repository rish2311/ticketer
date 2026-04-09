package models

import "time"

// Order status constants
const (
	StatusPending     = "pending"
	StatusConfirmed   = "confirmed"
	StatusFailed      = "failed"
	StatusCompensated = "compensated"
)

// Event represents a flash sale event.
type Event struct {
	ID           string    `json:"id" db:"id"`
	Name         string    `json:"name" db:"name"`
	Description  string    `json:"description" db:"description"`
	TotalTickets int       `json:"total_tickets" db:"total_tickets"`
	TicketPrice  float64   `json:"ticket_price" db:"ticket_price"`
	SaleStartAt  time.Time `json:"sale_start_at" db:"sale_start_at"`
	SaleEndAt    time.Time `json:"sale_end_at" db:"sale_end_at"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" db:"updated_at"`
}

// Order represents a ticket booking order.
type Order struct {
	ID             string    `json:"id" db:"id"`
	EventID        string    `json:"event_id" db:"event_id"`
	UserID         string    `json:"user_id" db:"user_id"`
	IdempotencyKey string    `json:"idempotency_key" db:"idempotency_key"`
	Status         string    `json:"status" db:"status"`
	RetryCount     int       `json:"retry_count" db:"retry_count"`
	FailureReason  *string   `json:"failure_reason,omitempty" db:"failure_reason"`
	CreatedAt      time.Time `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time `json:"updated_at" db:"updated_at"`
}

// BookingRequest is the API request payload for creating a booking.
type BookingRequest struct {
	UserID         string `json:"user_id" validate:"required,uuid"`
	EventID        string `json:"event_id" validate:"required,uuid"`
	IdempotencyKey string `json:"idempotency_key" validate:"required,min=1,max=255"`
}

// BookingResponse is the API response for a booking request.
type BookingResponse struct {
	Status  string `json:"status"`
	OrderID string `json:"order_id,omitempty"`
	Message string `json:"message"`
}

// EventWithAvailability is an Event with live ticket availability from Redis.
type EventWithAvailability struct {
	Event
	TicketsRemaining int `json:"tickets_remaining"`
	TicketsSold      int `json:"tickets_sold"`
}

// HealthStatus represents the system health check response.
type HealthStatus struct {
	Status   string            `json:"status"`
	Services map[string]string `json:"services"`
}

// ErrorResponse is a standard error response.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}
