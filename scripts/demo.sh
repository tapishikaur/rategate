#!/usr/bin/env bash
# The multi-instance demo: one API key limited to 50 requests/s gets 200 requests/s through
# nginx, which spreads them over the 2 gateways. With the gateways sharing their buckets
# (sync mode) about 50/s gets through in total; with no sharing (local mode) each gateway
# allows its own 50/s, so about twice that. The backend's own count is the proof.
#
# Usage:  scripts/demo.sh [report.md=/tmp/multi-instance-demo.md]      Env: DURATION (10s)
# Fails (exit 1) if sync mode lets through more than 1.25× what one bucket allows, or local
# mode less than 1.6×. CI runs it on every push.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
REPORT="${1:-/tmp/multi-instance-demo.md}"
DURATION="${DURATION:-10s}"
RESULTS="$(mktemp)"

docker compose up --build -d --wait
# Whatever happens, leave the gateways in the default mode.
trap 'docker compose up -d --wait --force-recreate gateway-1 gateway-2 > /dev/null 2>&1 || true' EXIT

for mode in sync local; do
  echo "demo: MODE=$mode"
  MODE="$mode" docker compose up -d --wait --force-recreate gateway-1 gateway-2 > /dev/null
  docker compose exec -T postgres psql -q -U rategate -d rategate -c 'TRUNCATE buckets' > /dev/null 2>&1 || true
  docker run --rm --network rategate_default rategate:dev overadmit \
    -targets http://nginx:8080 -keys demo -rate 200 -duration "$DURATION" -idle 2s \
    -backend http://backend:8081 -limit-rate 50 -limit-burst 50 -json |
    grep '^{' | tail -1 | sed "s/^{/{\"mode\": \"$mode\", /" >> "$RESULTS"
done

python3 - "$RESULTS" "$REPORT" "$(nproc)" "$(uname -srm)" <<'EOF'
import json, sys
results, report, cpus, kernel = sys.argv[1:]
rows = {r["mode"]: r for r in map(json.loads, open(results))}
out = [
    "# The limit is shared across 2 gateways",
    "",
    "One API key, limited to 50 requests/s with a burst of 50, sends 200 requests/s to nginx, which",
    "alternates between 2 gateways. **Allowed** is what one token bucket admits over the run:",
    "burst + rate x T. **Admitted** is the backend's own count.",
    f"Machine: {cpus} CPUs, {kernel}; everything on it.",
    "",
    "| Mode | Offered/s | Allowed by one bucket | Admitted | Admitted/s | Admitted ÷ allowed |",
    "|---|---|---|---|---|---|",
]
ratios = {}
for mode in ("sync", "local"):
    r = rows[mode]
    t = r["duration_s"]
    ratios[mode] = r["backend_admitted"] / r["allowed"]
    out.append(f"| {mode} | {r['sent'] / t:.0f} | {r['allowed']:.0f} | {r['backend_admitted']} | "
               f"{r['backend_admitted'] / t:.0f} | {ratios[mode]:.2f} |")
out += ["", "In sync mode the two gateways together stay close to one limit; in local mode each "
        "enforces the full limit on its own, so the client gets about twice as much."]
open(report, "w").write("\n".join(out) + "\n")
print("\n".join(out))
failed = []
if ratios["sync"] > 1.25:
    failed.append(f"sync mode admitted {ratios['sync']:.2f}x the limit (want at most 1.25x)")
if ratios["local"] < 1.6:
    failed.append(f"local mode admitted only {ratios['local']:.2f}x the limit (want at least 1.6x)")
if failed:
    sys.exit("demo FAILED: " + "; ".join(failed))
EOF
