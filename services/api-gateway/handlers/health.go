package handlers

import (
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"github.com/ticketer/shared/models"
)

// HealthHandler handles health check requests.
type HealthHandler struct {
	redisClient *redis.Client
}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler(rc *redis.Client) *HealthHandler {
	return &HealthHandler{redisClient: rc}
}

// HealthCheck handles GET /health
func (h *HealthHandler) HealthCheck(c *fiber.Ctx) error {
	ctx := c.Context()
	services := make(map[string]string)

	// Check Redis
	if err := h.redisClient.Ping(ctx).Err(); err != nil {
		services["redis"] = "unhealthy: " + err.Error()
	} else {
		services["redis"] = "healthy"
	}

	// Determine overall status
	overallStatus := "healthy"
	for _, status := range services {
		if status != "healthy" {
			overallStatus = "degraded"
			break
		}
	}

	statusCode := fiber.StatusOK
	if overallStatus != "healthy" {
		statusCode = fiber.StatusServiceUnavailable
	}

	return c.Status(statusCode).JSON(models.HealthStatus{
		Status:   overallStatus,
		Services: services,
	})
}
