// Latency load test: a fixed request rate against one target, for scripts/latency.sh.
//
// It sends RATE requests per second for DURATION, each a GET / with a random key among
// load-0 ... load-99 (keys with huge limits, so the gateway never rejects one and every
// request takes the full path to the backend). The same script runs against the backend
// directly and through the gateway, so the difference is what the gateway adds.
//
//	k6 run -e TARGET=http://nginx:8080 -e RATE=1000 -e DURATION=60s -e SUMMARY=/out/x.json latency.js
//
// The JSON written to SUMMARY is what latency.sh reads; durations in it are milliseconds.
import http from 'k6/http';
import { check } from 'k6';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';

const TARGET = __ENV.TARGET || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '1000', 10);
const DURATION = __ENV.DURATION || '60s';
const SUMMARY = __ENV.SUMMARY || '/tmp/k6-summary.json';

// Built once per VU rather than per request, so the load generator spends its time sending.
const PARAMS = [];
for (let i = 0; i < 100; i++) {
  PARAMS.push({ headers: { 'X-API-Key': `load-${i}` } });
}

export const options = {
  // A constant arrival rate, not a fixed number of looping users: a slow response must not
  // slow down the next request, or a slow target would quietly get less load (coordinated
  // omission) and look faster than it is.
  scenarios: {
    latency: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(99)', 'max'],
  // The bodies are a few bytes of "ok"; keeping them would only cost memory.
  discardResponseBodies: true,
};

export default function () {
  const res = http.get(`${TARGET}/`, PARAMS[Math.floor(Math.random() * PARAMS.length)]);
  check(res, { 'status is 200': (r) => r.status === 200 });
}

// Reads one value of a metric, or null when k6 didn't record it (dropped_iterations exists
// only if some iterations were dropped).
function value(data, metric, stat) {
  const m = data.metrics[metric];
  if (!m || !m.values || m.values[stat] === undefined) {
    return null;
  }
  return m.values[stat];
}

export function handleSummary(data) {
  const out = {
    target: TARGET,
    rate_requested: RATE,
    duration: DURATION,
    http_req_duration_ms: {
      avg: value(data, 'http_req_duration', 'avg'),
      med: value(data, 'http_req_duration', 'med'),
      p90: value(data, 'http_req_duration', 'p(90)'),
      p99: value(data, 'http_req_duration', 'p(99)'),
      max: value(data, 'http_req_duration', 'max'),
    },
    iterations: {
      count: value(data, 'iterations', 'count'),
      rate: value(data, 'iterations', 'rate'),
    },
    http_req_failed_rate: value(data, 'http_req_failed', 'rate'),
    checks_rate: value(data, 'checks', 'rate'),
    dropped_iterations: value(data, 'dropped_iterations', 'count') || 0,
    vus_max: value(data, 'vus_max', 'max'),
  };
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: false }),
    [SUMMARY]: JSON.stringify(out, null, 2) + '\n',
  };
}
