#!/bin/bash
# ============================================
# Post-Load-Test Validation Script
# Verifies invariants after a load test run
# ============================================

set -e

EVENT_ID="550e8400-e29b-41d4-a716-446655440000"
TOTAL_TICKETS=100

echo ""
echo "============================================"
echo "  POST-LOAD-TEST VALIDATION"
echo "============================================"
echo ""

# ── 1. Query PostgreSQL for confirmed orders ──
echo "📊 Querying PostgreSQL for confirmed orders..."
CONFIRMED=$(docker exec ticketer-postgres psql -U ticketer -d ticketer_db -t -c \
  "SELECT COUNT(*) FROM orders WHERE event_id='$EVENT_ID' AND status='confirmed';" | tr -d ' ')
echo "   Confirmed orders: $CONFIRMED"

# ── 2. Query PostgreSQL for failed/compensated orders ──
FAILED=$(docker exec ticketer-postgres psql -U ticketer -d ticketer_db -t -c \
  "SELECT COUNT(*) FROM orders WHERE event_id='$EVENT_ID' AND status IN ('failed', 'compensated');" | tr -d ' ')
echo "   Failed/Compensated orders: $FAILED"

# ── 3. Query Redis for remaining inventory ──
echo "📦 Querying Redis for remaining inventory..."
REMAINING=$(docker exec ticketer-redis redis-cli -a redis_secret GET "inventory:$EVENT_ID" 2>/dev/null | tr -d ' ')
echo "   Remaining inventory: $REMAINING"

# ── 4. Check for duplicate idempotency keys ──
echo "🔍 Checking for duplicate idempotency keys..."
DUPLICATES=$(docker exec ticketer-postgres psql -U ticketer -d ticketer_db -t -c \
  "SELECT COUNT(*) FROM (SELECT idempotency_key FROM orders GROUP BY idempotency_key HAVING COUNT(*) > 1) AS dups;" | tr -d ' ')
echo "   Duplicate keys found: $DUPLICATES"

echo ""
echo "============================================"
echo "  VALIDATION RESULTS"
echo "============================================"
echo ""

# ── Assertions ──
PASS=true

# Check: confirmed + remaining == total
SUM=$((CONFIRMED + REMAINING))
if [ "$SUM" -eq "$TOTAL_TICKETS" ]; then
  echo "  ✅ Inventory Invariant: confirmed($CONFIRMED) + remaining($REMAINING) = $TOTAL_TICKETS ✓"
else
  echo "  ❌ Inventory Invariant FAILED: confirmed($CONFIRMED) + remaining($REMAINING) = $SUM ≠ $TOTAL_TICKETS"
  PASS=false
fi

# Check: no overselling
if [ "$CONFIRMED" -le "$TOTAL_TICKETS" ]; then
  echo "  ✅ No Overselling: $CONFIRMED ≤ $TOTAL_TICKETS ✓"
else
  echo "  ❌ OVERSELLING DETECTED: $CONFIRMED > $TOTAL_TICKETS"
  PASS=false
fi

# Check: no duplicate idempotency keys
if [ "$DUPLICATES" -eq "0" ]; then
  echo "  ✅ No Duplicate Keys ✓"
else
  echo "  ❌ DUPLICATE KEYS DETECTED: $DUPLICATES"
  PASS=false
fi

echo ""
if [ "$PASS" = true ]; then
  echo "  🎉 ALL VALIDATIONS PASSED!"
else
  echo "  💥 SOME VALIDATIONS FAILED!"
  exit 1
fi
echo ""
