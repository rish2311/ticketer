package handlers

import (
	"log"

	"github.com/gofiber/fiber/v2"

	redisclient "github.com/ticketer/api-gateway/redis"
	"github.com/ticketer/shared/config"
	"github.com/ticketer/shared/models"
)

// EventHandler handles event-related API requests.
type EventHandler struct {
	gatekeeper *redisclient.Gatekeeper
	cfg        *config.Config
}

// NewEventHandler creates a new EventHandler.
func NewEventHandler(gk *redisclient.Gatekeeper, cfg *config.Config) *EventHandler {
	return &EventHandler{
		gatekeeper: gk,
		cfg:        cfg,
	}
}

// GetEvents handles GET /api/v1/events
// Returns the hardcoded seed event for the demo.
func (h *EventHandler) GetEvents(c *fiber.Ctx) error {
	// For the demo, return the seeded event with live availability
	eventID := "550e8400-e29b-41d4-a716-446655440000"
	remaining, err := h.gatekeeper.GetInventory(c.Context(), eventID)
	if err != nil {
		log.Printf("[Events] Failed to get inventory: %v", err)
		remaining = 0
	}

	event := models.EventWithAvailability{
		Event: models.Event{
			ID:           eventID,
			Name:         "Mega Concert 2026",
			Description:  "The biggest concert of the year! Limited tickets available. First come, first served.",
			TotalTickets: h.cfg.TicketInventory,
			TicketPrice:  49.99,
		},
		TicketsRemaining: remaining,
		TicketsSold:      h.cfg.TicketInventory - remaining,
	}

	return c.JSON(fiber.Map{
		"events": []models.EventWithAvailability{event},
	})
}

// GetEventByID handles GET /api/v1/events/:id
func (h *EventHandler) GetEventByID(c *fiber.Ctx) error {
	eventID := c.Params("id")
	if eventID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "missing_param",
			Code:    400,
			Message: "Event ID is required",
		})
	}

	remaining, err := h.gatekeeper.GetInventory(c.Context(), eventID)
	if err != nil {
		log.Printf("[Events] Failed to get inventory for %s: %v", eventID, err)
		remaining = 0
	}

	event := models.EventWithAvailability{
		Event: models.Event{
			ID:           eventID,
			Name:         "Mega Concert 2026",
			Description:  "The biggest concert of the year! Limited tickets available. First come, first served.",
			TotalTickets: h.cfg.TicketInventory,
			TicketPrice:  49.99,
		},
		TicketsRemaining: remaining,
		TicketsSold:      h.cfg.TicketInventory - remaining,
	}

	return c.JSON(event)
}

// StartFlashSale handles POST /api/v1/events/:id/start
// Initializes the Redis inventory for a flash sale event.
func (h *EventHandler) StartFlashSale(c *fiber.Ctx) error {
	eventID := c.Params("id")
	if eventID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(models.ErrorResponse{
			Error:   "missing_param",
			Code:    400,
			Message: "Event ID is required",
		})
	}

	// Check if inventory is already initialized
	existing, err := h.gatekeeper.GetInventory(c.Context(), eventID)
	if err == nil && existing > 0 {
		return c.JSON(fiber.Map{
			"status":  "already_started",
			"message": "Flash sale is already active",
			"tickets_remaining": existing,
		})
	}

	// Initialize inventory in Redis
	if err := h.gatekeeper.InitInventory(c.Context(), eventID, h.cfg.TicketInventory); err != nil {
		log.Printf("[Events] Failed to initialize inventory: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(models.ErrorResponse{
			Error:   "init_failed",
			Code:    500,
			Message: "Failed to initialize flash sale",
		})
	}

	log.Printf("[Events] Flash sale started: eventID=%s inventory=%d", eventID, h.cfg.TicketInventory)

	return c.JSON(fiber.Map{
		"status":  "started",
		"message": "Flash sale initialized!",
		"event_id": eventID,
		"total_tickets": h.cfg.TicketInventory,
	})
}
