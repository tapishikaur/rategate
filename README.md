# RateGate

[![CI](https://github.com/tapishikaur/rategate/actions/workflows/ci.yml/badge.svg)](https://github.com/tapishikaur/rategate/actions/workflows/ci.yml)

A rate-limiting API gateway in Go. Two (or more) gateway instances behind a load balancer enforce **one shared limit
per client**, without Redis. Each instance decides in memory, and every 100 ms it syncs the tokens it used to
PostgreSQL in one atomic, batched SQL statement. Prometheus gets request counts, rejections and latency histograms.

## Results

| Claim | Result | Record |
|---|---|---|
| One limit shared across 2 gateways | A key limited to 50/s, sent 200/s through nginx. CI checks on every push that sync mode lets through at most 1.25× what one bucket allows, and that local mode (no sharing) lets through at least 1.6×. | [scripts/demo.sh](scripts/demo.sh), [CI](https://github.com/tapishikaur/rategate/actions/workflows/ci.yml) |
| Over-admission vs sync interval, 2 gateways | Steady overload: within **±1%** of one exact bucket at every interval from 10 ms to 1 s; local mode **+99.9%**. The worst 1-second window grows from about 200 to 325 at 1 s. Bursts after a quiet spell: **+4% at 10 ms**, +25% at 100 ms, +61% at 1 s. | [results/over-admission.md](results/over-admission.md) |
| Rejections and latency exported to Prometheus | Both gateways scraped; rejection ratio and latency histogram queried from Prometheus's API | [results/prometheus-check.md](results/prometheus-check.md) |

All measured on a 2-core GitHub Codespace, with the whole stack and the load tool on that one machine.

![Prometheus graph of the rejection ratio across both gateways](results/prometheus.png)

**The trade-off in one sentence:** over a minute, batch sync stays within 1% of an exact limit at every interval
measured. The interval decides how big a burst can slip through between syncs. 100 ms is a sensible default, and 10 ms
buys tighter bursts for 10× the sync traffic, which is still one SQL statement per gateway per interval.

## The problem

A client may make, say, 100 requests per second. With one gateway that's easy: keep a counter in memory. With two
gateways behind a load balancer, the limit belongs to the client, not to a gateway. If each gateway counts on its own,
the client gets twice its limit.

| Approach | Per request | Accuracy |
|---|---|---|
| **Exact**: ask the shared database every time | one atomic database update | exact, but every request waits for a round trip, and all of one busy client's requests queue on one database row |
| **Local only**: each gateway counts alone | nothing shared | N gateways let through N× the limit |
| **Batch sync** (RateGate's default) | in memory; usage synced every 100 ms | off by a short burst at most, and the overspend is paid back |

RateGate implements all three as modes (`MODE=exact | sync | local`), so the trade-off can be measured rather than
argued.

## How it works

```mermaid
flowchart LR
  C[client] -->|"HTTP + X-API-Key"| N[nginx: round robin]
  N --> G1[gateway 1]
  N --> G2[gateway 2]
  G1 -->|"allowed requests"| B[backend]
  G2 -->|"allowed requests"| B
  G1 <-->|"every 100 ms: tokens used, shared count"| PG[(PostgreSQL)]
  G2 <-->|"every 100 ms"| PG
  P[Prometheus] -->|"scrapes /metrics"| G1
  P --> G2
```

1. **A request** carries an API key (`X-API-Key`). Its limit comes from a config file
   ([deploy/keys.json](deploy/keys.json)): a rate R (tokens per second) and a burst B (bucket size).
2. **The decision is local.** The gateway takes a token from its in-memory **token bucket** for that key. The bucket
   refills at R and holds at most B. No token means **429 Too Many Requests**, with `Retry-After`. Otherwise the request
   is forwarded to the backend over a kept-alive connection. An unknown key gets 401.
3. **The sync.** Every 100 ms, the gateway sends every key's usage since the last sync to PostgreSQL in **one
   statement**, whatever the number of keys. For each key, PostgreSQL atomically refills the shared bucket, subtracts
   the usage and returns the shared count. The gateway then adopts that count, minus what it used while the query ran.
4. **Debt.** If both gateways spend in the same interval, the shared count goes negative. Both see the debt at their
   next sync and refuse until refill pays it back. So over time the limit holds, and the error is a short burst.

## Why this choice

- **Batch sync instead of exact.** Exact global limits need coordination on every request: the decision depends on
  the latest global count. That puts a database round trip on every request's path, and all requests for one busy
  key serialize on a single row lock. Batching makes the decision local (microseconds) and the coordination cost
  fixed: one statement per gateway per interval (about 20 a second here), regardless of traffic.
- **A token bucket** rather than a fixed window. A fixed window lets 2× the limit through around the window edge. A
  token bucket allows a controlled burst, then a steady rate. That's what `Retry-After` can describe precisely.
- **Counts in whole nano-tokens, not floats.** Float sums drift: refills of 0.4 + 0.3 + 0.299 + 0.001 tokens add up to
  just under 1, and a request arriving exactly on time was refused. Integer billionths of a token are exact for
  whole-number rates ([internal/bucket](internal/bucket/bucket.go)).
- **One `INSERT … ON CONFLICT DO UPDATE` per sync, sorted by key** ([internal/store/postgres.go](internal/store/postgres.go)).
  - Each row's update is atomic, so concurrent gateways never lose each other's usage.
  - Sorting means every sync locks rows in the same order, so two syncs can't deadlock.
  - New keys are inserted by the same statement.
- **The database's clock for refills.** Gateways' clocks differ slightly; refilling by PostgreSQL's `now()` counts each
  refill once. `GREATEST(0, now() - updated_at)` covers an update that resumes on a row committed after its own
  transaction began.
- **Debt is never forgiven in normal operation.** The first version capped it at -burst. The simulator showed that this
  forgave debt again and again with 3 gateways syncing every second (27% over the limit), so the cap was removed.
- **An `UNLOGGED` table.** Rate-limit state is soft state. Skipping the write-ahead log makes updates cheap. The cost is
  that after a database crash every bucket starts full, which is one extra burst per key.
- **Fail-open.** If PostgreSQL is unreachable, each gateway keeps limiting on its own (at worst N× the limit) and
  `rategate_store_errors_total` rises; a rate limiter shouldn't take the API down. Unreported usage is kept up to one
  burst, so a blip loses nothing and a long outage doesn't lock clients out afterwards for as long.
- **Exact mode reads "allowed" from `RETURNING old/new`** (PostgreSQL 18): one round trip, no read-modify-write.
- **No per-key Prometheus labels.** Thousands of keys would mean thousands of time series per metric. Per-key usage
  belongs in logs.
- **Metrics and health on a separate admin port**, so they can't be reached through the proxy.

## Metrics

On each gateway's admin port, `:9091/metrics`:

| Metric | What |
|---|---|
| `rategate_requests_total{outcome}` | requests by outcome: `allowed`, `limited` (429), `unauthorized` (401), `upstream_error` (502) |
| `rategate_request_duration_seconds{outcome}` | histogram, fine-grained around 1–5 ms |
| `rategate_limiter_decision_seconds` | the limit decision alone: microseconds in memory, a round trip in exact mode |
| `rategate_store_duration_seconds{op}`, `rategate_store_errors_total{op}` | calls to PostgreSQL: `sync` (batched) or `take` (exact mode) |
| `rategate_sync_keys` | keys per sync |
| `rategate_info{mode}` | which mode the gateway runs in |

Rejection rate: `sum(rate(rategate_requests_total{outcome="limited"}[1m])) / sum(rate(rategate_requests_total[1m]))`.

## Run it

You need Docker with Compose. A GitHub Codespace on this repo has everything.

```bash
docker compose up --build -d --wait      # PostgreSQL, backend, 2 gateways, nginx :8080, Prometheus :9090
curl -i -H 'X-API-Key: demo' http://localhost:8080/
bash scripts/demo.sh                     # one limit shared by both gateways (sync) vs 2× (local)
bash scripts/overadmit.sh                # the over-admission sweep: exact, 10 ms … 1 s, local
bash scripts/latency.sh                  # k6 at 1,000 req/s: direct vs 1 gateway vs nginx + 2 gateways
python3 -I scripts/prom_check.py --out /tmp/prom.md --screenshot /tmp/prom.png
MODE=exact docker compose up -d --wait gateway-1 gateway-2   # switch mode (or MODE=local, SYNC_INTERVAL=1s)
```

Tests need Go 1.26 and a PostgreSQL server; each test creates and drops its own database:

```bash
RATEGATE_TEST_DB=postgres://user:pass@localhost:5432/postgres bash scripts/test.sh -race -count=1
```

Without `RATEGATE_TEST_DB`, `scripts/test.sh` starts a throwaway local PostgreSQL ([scripts/pg_local.sh](scripts/pg_local.sh)).
The tests cover:
- the bucket, against an exact reference over a million random requests;
- the limiter's modes, fail-open, and usage taken during a sync;
- the store under concurrent syncs and deadlock-prone orders;
- two real gateways sharing a limit through PostgreSQL;
- a deterministic simulator of 2 and 3 gateways for every interval.

## Layout

| Path | What |
|---|---|
| `cmd/rategate` | the single binary: `gateway`, `backend`, `overadmit`, `health` |
| `internal/bucket` | the token bucket |
| `internal/limiter` | per-key decisions, the three modes, the sync loop; `sim_test.go` is the simulator |
| `internal/store` | the shared buckets: PostgreSQL (`schema.sql`, the batched SQL) and in-memory |
| `internal/gateway` | the HTTP front door: keys, 401/429, reverse proxy |
| `internal/metrics` | Prometheus metrics |
| `internal/backend` | the test backend; its per-key counts are the ground truth for what got through |
| `internal/overadmit` | the open-loop load tool for the over-admission experiment |
| `deploy/`, `docker-compose.yml` | Dockerfile, nginx, Prometheus, keys |
| `scripts/`, `loadtest/` | demo, sweep, latency (k6), Prometheus check, tests |

## Limitations

- **Bursts after a quiet spell** are the main error: every gateway's local view is a full bucket, so a burst spread over
  N gateways can get N bursts through before the next sync. The resulting debt is paid back afterwards.
- Limits come from a config file read at start-up. All gateways must get the same file, and changing a limit means a
  restart.
- API keys are compared as plain strings from the config file; a real deployment would store hashes.
- One PostgreSQL instance is a single point of coordination (not of failure: the gateways fail open).
- Docker Compose only; no cluster.

## Next steps

- **A per-gateway spending cap** between syncs (each gateway spends at most its share of the shared count). It makes
  bursts accurate but needs the gateways to know how many of them are alive.
- **An adaptive sync interval:** sync sooner when a key is close to its limit, and rarely when it's far from it.
- **Live limit changes:** read limits from the database instead of a file.

## License

MIT
