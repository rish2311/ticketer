package handlers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	redisclient "github.com/ticketer/api-gateway/redis"
	"github.com/ticketer/shared/config"
	"github.com/ticketer/shared/events"
	"github.com/ticketer/shared/models"
)

// BookingHandler handles ticket booking requests.
type BookingHandler struct {
	gatekeeper  *redisclient.Gatekeeper
	kafkaWriter *kafka.Writer
	cfg         *config.Config
}

// NewBookingHandler creates a new BookingHandler.
func NewBookingHandler(gk *redisclient.Gatekeeper, kw *kafka.Writer, cfg *config.Config) *BookingHandler {
	return &BookingHandler{
		gatekeeper:  gk,
		kafkaWriter: kw,
		cfg:         cfg,
	}
}

// CreateBooking handles POST /api/v1/bookings
func (h *BookingHandler) CreateBooking(c *fiber.Ctx) error {
	// Parse request body
	var req models.BookingRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "invalid_request",
			Code:    400,
			Message: "Invalid request body: " + err.Error(),
		})
	}

	// Validate required fields
	if req.UserID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "missing_field",
			Code:    400,
			Message: "user_id is required",
		})
	}
	if req.EventID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "missing_field",
			Code:    400,
			Message: "event_id is required",
		})
	}
	if req.IdempotencyKey == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "missing_field",
			Code:    400,
			Message: "idempotency_key is required",
		})
	}

	// Validate UUID format
	if _, err := uuid.Parse(req.UserID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "invalid_format",
			Code:    400,
			Message: "user_id must be a valid UUID",
		})
	}
	if _, err := uuid.Parse(req.EventID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "invalid_format",
			Code:    400,
			Message: "event_id must be a valid UUID",
		})
	}

	ctx := c.UserContext()

	// ── Step 1: Redis Gatekeeper ──
	result, err := h.gatekeeper.TryAcquireTicket(ctx, req.EventID, req.UserID, h.cfg.LockTTLSeconds)
	if err != nil {
		log.Printf("[Booking] Gatekeeper error: %v", err)
		return c.Status(fiber.StatusServiceUnavailable).JSON(models.ErrorResponse{
			Error:   "service_unavailable",
			Code:    503,
			Message: "Ticket service temporarily unavailable. Please retry.",
		})
	}

	switch result {
	case redisclient.ResultSoldOut:
		return c.Status(fiber.StatusConflict).JSON(models.BookingResponse{
			Status:  "sold_out",
			Message: "Sorry, all tickets have been sold out!",
		})
	case redisclient.ResultDuplicate:
		return c.Status(fiber.StatusConflict).JSON(models.BookingResponse{
			Status:  "duplicate",
			Message: "You already have a pending booking for this event.",
		})
	}

	// ── Step 2: Publish to Kafka ──
	orderID := uuid.New().String()

	event := events.OrderEvent{
		EventType:      events.OrderPending,
		OrderID:        orderID,
		UserID:         req.UserID,
		EventID:        req.EventID,
		IdempotencyKey: req.IdempotencyKey,
		RetryCount:     0,
		Timestamp:      time.Now().UTC(),
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("[Booking] Failed to marshal event: %v", err)
		// Release the ticket since we can't publish
		h.gatekeeper.ReleaseTicket(ctx, req.EventID, req.UserID)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error:   "internal_error",
			Code:    500,
			Message: "Failed to process booking",
		})
	}

	err = h.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(req.EventID), // Partition by event ID for ordering
		Value: eventBytes,
	})
	if err != nil {
		log.Printf("[Booking] Failed to publish to Kafka: %v", err)
		// Release the ticket since Kafka write failed
		h.gatekeeper.ReleaseTicket(ctx, req.EventID, req.UserID)
		return c.Status(fiber.StatusServiceUnavailable).JSON(models.ErrorResponse{
			Error:   "service_unavailable",
			Code:    503,
			Message: "Booking service temporarily unavailable. Please retry.",
		})
	}

	log.Printf("[Booking] Order published: orderID=%s userID=%s eventID=%s", orderID, req.UserID, req.EventID)

	// ── Step 3: Return 202 Accepted ──
	return c.Status(fiber.StatusAccepted).JSON(models.BookingResponse{
		Status:  "processing",
		OrderID: orderID,
		Message: "Ticket secured in queue. Awaiting payment confirmation.",
	})
}
