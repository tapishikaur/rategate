#!/usr/bin/env bash
# The over-admission sweep: how far past the limit traffic gets through when one key's
# requests are spread over 2 gateways, for each mode and sync interval. Runs against the
# Docker Compose stack (a Codespace has Docker), recreating the gateways for each setting.
#
# Settings: exact; sync every 10ms, 50ms, 100ms, 250ms, 500ms, 1s; local.
# Scenarios per setting (keys oa-0..oa-3, each limited to 100/s with a burst of 100):
#   steady-50/50  3× the limit for DURATION, traffic split evenly between the gateways
#   steady-90/10  the same, 90% to gateway-1
#   burst         500 requests per key at once after a quiet spell
#
# Definitions (fixed before measuring): allowed = burst + rate × T, what one perfect bucket
# starting full admits; over-admission = (admitted - allowed) / allowed, admitted counted by
# the backend; worst 1 s = most admitted in any 1-second window of send time.
#
# Usage:  scripts/overadmit.sh [results.jsonl=/tmp/overadmit.jsonl] [report.md=/tmp/over-admission.md]
# Env:    DURATION (60s), RATE per key (300), BURST (500), IDLE (3s)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
JSONL="${1:-/tmp/overadmit.jsonl}"
REPORT="${2:-/tmp/over-admission.md}"
DURATION="${DURATION:-60s}"
RATE="${RATE:-300}"
BURST="${BURST:-500}"
IDLE="${IDLE:-3s}"
KEYS="oa-0,oa-1,oa-2,oa-3"
LIMIT=(-limit-rate 100 -limit-burst 100)
GATEWAYS="http://gateway-1:8080,http://gateway-2:8080"
: > "$JSONL"

docker compose up --build -d --wait

restore() { docker compose up -d --wait --force-recreate gateway-1 gateway-2 > /dev/null 2>&1 || true; }
trap restore EXIT

overadmit() { docker run --rm --network rategate_default rategate:dev overadmit "$@" -json; }

# run SETTING SCENARIO overadmit-flags...: one run, its JSON line tagged and appended.
run() {
  local setting="$1" scenario="$2"
  shift 2
  echo "$setting / $scenario"
  local line
  line="$(overadmit -targets "$GATEWAYS" -keys "$KEYS" -backend http://backend:8081 -idle "$IDLE" "${LIMIT[@]}" "$@" |
    grep '^{' | tail -1)"
  echo "{\"setting\": \"$setting\", \"scenario\": \"$scenario\", \"result\": $line}" >> "$JSONL"
}

for setting in exact 10ms 50ms 100ms 250ms 500ms 1s local; do
  case "$setting" in
    exact | local) mode="$setting" interval=100ms ;;
    *) mode=sync interval="$setting" ;;
  esac
  MODE="$mode" SYNC_INTERVAL="$interval" docker compose up -d --wait --force-recreate gateway-1 gateway-2 > /dev/null
  label="$mode"
  [ "$mode" = sync ] && label="sync $interval"
  for scenario in steady-50/50 steady-90/10 burst; do
    # A clean start for every run: the shared buckets persist in PostgreSQL between runs.
    docker compose exec -T postgres psql -q -U rategate -d rategate -c 'TRUNCATE buckets' > /dev/null
    case "$scenario" in
      steady-50/50) run "$label" "$scenario" -weights 1,1 -rate "$RATE" -duration "$DURATION" ;;
      steady-90/10) run "$label" "$scenario" -weights 9,1 -rate "$RATE" -duration "$DURATION" ;;
      burst) run "$label" "$scenario" -weights 1,1 -rate 0 -duration 0 -burst "$BURST" ;;
    esac
  done
done

python3 - "$JSONL" "$REPORT" "$(nproc)" "$(uname -srm)" "$(date -u +%Y-%m-%d)" "$DURATION" "$RATE" "$BURST" <<'EOF'
import json, sys
jsonl, report, cpus, kernel, day, duration, rate, burst = sys.argv[1:]
rows = [json.loads(l) for l in open(jsonl) if l.strip()]
out = [
    "# Over-admission vs sync interval, 2 gateways",
    "",
    f"Measured {day} on {cpus} CPUs ({kernel}), the whole Docker Compose stack and the load tool on the same machine.",
    f"4 API keys, each limited to 100 requests/s with a burst of 100. Steady scenarios offer {rate}/s per key (3x the limit)",
    f"for {duration}; the burst scenario sends {burst} requests per key at once after a quiet spell.",
    "",
    "- **Allowed** = burst + rate x T: what one perfect token bucket, starting full, would admit (summed over the keys).",
    "- **Admitted** = what reached the backend (its own count).",
    "- **Over-admission** = (admitted - allowed) / allowed.",
    "- **Worst 1 s** = most requests admitted for one key in any 1-second window; one exact bucket admits at most rate + burst = 200.",
    "- **Burst overshoot** = requests admitted per key in the burst beyond the burst size.",
    "",
    "| Setting | Scenario | Allowed | Admitted | Over-admission | Worst 1 s (exact: 200) | Burst overshoot per key |",
    "|---|---|---|---|---|---|---|",
]
for r in rows:
    x = r["result"]
    admitted = x["backend_admitted"] if x["backend_admitted"] >= 0 else x["admitted"]
    steady = r["scenario"] != "burst"
    out.append("| {} | {} | {:.0f} | {} | {:+.1f}% | {} | {} |".format(
        r["setting"], r["scenario"], x["allowed"], admitted, 100 * x["over_admission"],
        x["worst_1s_per_key"] if steady else "",
        "" if steady else "{:.0f}".format(x["burst_overshoot_per_key"])))
open(report, "w").write("\n".join(out) + "\n")
print(f"wrote {report}")
EOF
