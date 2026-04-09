#!/bin/bash
# ============================================
# Seed Flash Sale Event & Initialize Inventory
# ============================================

set -e

API_URL=${API_URL:-http://localhost:8080}
EVENT_ID="550e8400-e29b-41d4-a716-446655440000"

echo ""
echo "🎟️  Seeding Flash Sale Event..."
echo ""

# Start the flash sale (initializes Redis inventory from config)
echo "📦 Initializing inventory in Redis..."
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/events/$EVENT_ID/start")
echo "   Response: $RESPONSE"

echo ""
echo "🔍 Verifying event..."
EVENT=$(curl -s "$API_URL/api/v1/events/$EVENT_ID")
echo "   Event details: $EVENT"

echo ""
echo "✅ Flash sale ready! Visit http://localhost:3000 to see the UI."
echo "   Run 'k6 run scripts/load-test/k6-flash-sale.js' to start load testing."
echo ""
