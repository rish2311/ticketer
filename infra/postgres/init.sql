-- ============================================
-- Flash Sale Ticket Booking Engine
-- PostgreSQL Schema Initialization
-- ============================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- Events Table: Flash sale events
-- ============================================
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    total_tickets INT NOT NULL CHECK (total_tickets > 0),
    ticket_price DECIMAL(10, 2) NOT NULL CHECK (ticket_price > 0),
    sale_start_at TIMESTAMPTZ NOT NULL,
    sale_end_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_sale_dates CHECK (sale_end_at > sale_start_at)
);

-- ============================================
-- Orders Table: Source of truth for bookings
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'confirmed', 'failed', 'compensated')),
    retry_count INT NOT NULL DEFAULT 0,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_idempotency UNIQUE (idempotency_key)
);

-- ============================================
-- Indexes for performance
-- ============================================
CREATE INDEX IF NOT EXISTS idx_orders_event_id ON orders(event_id);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_event_user ON orders(event_id, user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);

-- ============================================
-- Seed: Default flash sale event
-- ============================================
INSERT INTO events (id, name, description, total_tickets, ticket_price, sale_start_at, sale_end_at)
VALUES (
    '550e8400-e29b-41d4-a716-446655440000',
    'Mega Concert 2026',
    'The biggest concert of the year! Limited tickets available. First come, first served.',
    100,
    49.99,
    NOW(),
    NOW() + INTERVAL '24 hours'
) ON CONFLICT (id) DO NOTHING;

-- Print confirmation
DO $$
BEGIN
    RAISE NOTICE 'Database initialized successfully!';
    RAISE NOTICE 'Seeded event: Mega Concert 2026 (100 tickets @ $49.99)';
END $$;
