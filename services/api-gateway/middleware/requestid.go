package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// RequestID injects a unique request ID into each request's headers and locals.
func RequestID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Check if request ID already present (forwarded from load balancer)
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Set in response headers
		c.Set("X-Request-ID", requestID)

		// Store in locals for use in handlers
		c.Locals("requestID", requestID)

		return c.Next()
	}
}
