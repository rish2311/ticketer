package payment

import (
	"context"
	"math/rand"
	"time"
)

// Result represents the outcome of a payment simulation.
type Result struct {
	Success bool
	Reason  string
}

// Simulator simulates payment processing with a configurable success rate.
type Simulator struct {
	successRate int // percentage (0-100)
}

// NewSimulator creates a new payment Simulator.
func NewSimulator(successRate int) *Simulator {
	if successRate < 0 {
		successRate = 0
	}
	if successRate > 100 {
		successRate = 100
	}
	return &Simulator{successRate: successRate}
}

// ProcessPayment simulates a payment with random latency and failure.
// Returns the result and respects context cancellation.
func (s *Simulator) ProcessPayment(ctx context.Context, orderID string) Result {
	// Simulate processing latency (100ms - 500ms)
	delay := time.Duration(100+rand.Intn(400)) * time.Millisecond

	select {
	case <-time.After(delay):
		// Continue processing
	case <-ctx.Done():
		return Result{
			Success: false,
			Reason:  "payment processing timed out",
		}
	}

	// Determine success/failure based on configured rate
	if rand.Intn(100) < s.successRate {
		return Result{
			Success: true,
			Reason:  "",
		}
	}

	// Simulate different failure reasons
	failures := []string{
		"insufficient_funds",
		"card_declined",
		"payment_gateway_timeout",
		"fraud_detection_triggered",
	}
	return Result{
		Success: false,
		Reason:  failures[rand.Intn(len(failures))],
	}
}
