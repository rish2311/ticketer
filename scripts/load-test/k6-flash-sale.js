import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Custom metrics
const ticketsSecured = new Counter('tickets_secured');
const soldOutResponses = new Counter('sold_out_responses');
const duplicateResponses = new Counter('duplicate_responses');
const bookingLatency = new Trend('booking_latency', true);

// Test configuration
const EVENT_ID = '550e8400-e29b-41d4-a716-446655440000';
const API_URL = __ENV.API_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    flash_sale: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: 100 },   // Ramp up to 100 users
        { duration: '10s', target: 500 },   // Ramp up to 500 users
        { duration: '20s', target: 500 },   // Hold at 500 users
        { duration: '5s', target: 0 },      // Ramp down
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.01'],           // < 1% server errors
    'http_req_duration': ['p(95)<500'],          // p95 < 500ms
    'tickets_secured': ['count<=100'],           // Never oversell (max 100 tickets)
  },
};

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

export default function () {
  const userId = generateUUID();
  const idempotencyKey = `${userId}-${EVENT_ID}-${Date.now()}`;

  const payload = JSON.stringify({
    user_id: userId,
    event_id: EVENT_ID,
    idempotency_key: idempotencyKey,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const start = Date.now();
  const res = http.post(`${API_URL}/api/v1/bookings`, payload, params);
  const latency = Date.now() - start;

  bookingLatency.add(latency);

  // Validate response
  check(res, {
    'status is 202 or 409': (r) => r.status === 202 || r.status === 409,
    'no server errors': (r) => r.status < 500,
    'has response body': (r) => r.body && r.body.length > 0,
  });

  // Track outcomes
  if (res.status === 202) {
    ticketsSecured.add(1);
  } else if (res.status === 409) {
    const body = JSON.parse(res.body);
    if (body.status === 'sold_out') {
      soldOutResponses.add(1);
    } else if (body.status === 'duplicate') {
      duplicateResponses.add(1);
    }
  }

  // Small random delay between requests
  sleep(Math.random() * 0.5);
}

export function handleSummary(data) {
  const secured = data.metrics.tickets_secured ? data.metrics.tickets_secured.values.count : 0;
  const soldOut = data.metrics.sold_out_responses ? data.metrics.sold_out_responses.values.count : 0;
  const duplicates = data.metrics.duplicate_responses ? data.metrics.duplicate_responses.values.count : 0;

  console.log('\n' + '='.repeat(60));
  console.log('  FLASH SALE LOAD TEST RESULTS');
  console.log('='.repeat(60));
  console.log(`  🎟️  Tickets Secured:    ${secured}`);
  console.log(`  🚫  Sold Out Responses: ${soldOut}`);
  console.log(`  ⚠️  Duplicate Blocked:  ${duplicates}`);
  console.log(`  📊  Total Requests:     ${secured + soldOut + duplicates}`);
  console.log('='.repeat(60));

  if (secured > 100) {
    console.log('  ❌ OVERSELLING DETECTED! Secured more than 100 tickets!');
  } else {
    console.log('  ✅ No overselling detected');
  }

  console.log('='.repeat(60) + '\n');

  return {};
}
