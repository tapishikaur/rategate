# RateGate: metrics in Prometheus

Checked 2026-10-28 02:01 UTC by `scripts/prom_check.py`: 20 s of traffic through nginx (both gateways), then queries to the Prometheus HTTP API (`/api/v1/query`), which scrapes each gateway's `/metrics` every 2 s.

## Traffic sent

| Key | Asked rate (req/s) | Responses |
|---|---|---|
| `load-0` | 100 | 200: 2005 |
| `demo` | 150 | 200: 1047, 429: 1958 |

`load-0` has a limit too high to reach; `demo` has 50 requests/s with a burst of 50, so at 150/s most of its requests should get 429.

## Queries

**Rejection ratio (limited / all requests)**

```promql
sum(rate(rategate_requests_total{outcome="limited"}[30s])) / sum(rate(rategate_requests_total[30s]))
```

Result: `0.390692`

**p99 latency of allowed requests, seconds**

```promql
histogram_quantile(0.99, sum by (le) (rate(rategate_request_duration_seconds_bucket{outcome="allowed"}[30s])))
```

Result: a number, so the latency histogram reaches Prometheus and can be queried. The value isn't a latency
measurement: it includes the backend and came from a 20 s check on a shared 2-core machine.

**Requests/s per gateway**

```promql
sum by (instance) (rate(rategate_requests_total[30s]))
```

- `{instance="gateway-1:9091"}`: `89.008`
- `{instance="gateway-2:9091"}`: `87.0935`

**Scrape target up (1) or down (0)**

```promql
up{job="rategate"}
```

- `{__name__="up", instance="gateway-1:9091", job="rategate"}`: `1`
- `{__name__="up", instance="gateway-2:9091", job="rategate"}`: `1`

## Checks

- PASS: both gateways are scrape targets and up (up == 1 for 2 instances)
- PASS: both gateways report requests (2 instances, rate > 0)
- PASS: rejection ratio > 0
- PASS: p99 of allowed requests is a number
- PASS: the Prometheus graph rendered for the screenshot

So Prometheus saw 39.1% of requests refused (1,958 of 5,010 responses were 429), and the latency histogram of
allowed requests is exported and queryable.

Screenshot of the rejection ratio in the Prometheus UI: `prometheus.png`.
