# RateGate

[![CI](https://github.com/tapishikaur/rategate/actions/workflows/ci.yml/badge.svg)](https://github.com/tapishikaur/rategate/actions/workflows/ci.yml)

A rate-limiting API gateway in Go. Several gateway instances behind a load balancer enforce **one shared per-client
limit** without Redis. Each instance decides from an in-memory token bucket, and every 100 ms it syncs the tokens it
used to PostgreSQL in one atomic, batched update.

> Work in progress: measurements (added latency, over-admission vs sync interval) are being taken and will be added
> here.

```bash
docker compose up --build -d --wait     # PostgreSQL, backend, 2 gateways, nginx, Prometheus
curl -H 'X-API-Key: demo' http://localhost:8080/
bash scripts/demo.sh                    # the limit is shared across both gateways
```

## License

MIT
