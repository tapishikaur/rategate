#!/usr/bin/env bash
# Measures the latency the gateway adds, at a fixed rate, with k6 (loadtest/latency.js) in a
# container on the Compose network, so every path is measured from the same place.
#
# The method is fixed before measuring (PLAN.md, section 6):
#   - each run: a warm-up (not reported), then a measured run, at the same rate;
#   - paths: direct = the backend, gateway = one gateway without nginx,
#     nginx = nginx + 2 gateways in sync mode (the headline);
#   - 3 rounds, the order of the paths alternating between rounds, so a machine that gets
#     slower (or faster) during the test doesn't favour one path;
#   - added p99 = p99(nginx) - p99(direct) of the same round; the headline is the median of
#     the per-round differences;
#   - then exact mode: the gateways recreated with MODE=exact, nginx measured 3 times, and
#     the gateways put back in sync mode.
#
# Usage: scripts/latency.sh [report.md]       (default /tmp/latency.md)
#   Env overrides: RATE (1000 req/s), DURATION (60s), WARMUP (15s, 0 to skip), ROUNDS (3).
#   e.g. a quick smoke test: RATE=200 DURATION=10s WARMUP=0 ROUNDS=1 scripts/latency.sh
#
# The report goes to /tmp by default, not results/: the number may miss the target, and a
# result below target must not end up committed by accident. Copy it to results/ by hand.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="${1:-/tmp/latency.md}"
RATE="${RATE:-1000}"
DURATION="${DURATION:-60s}"
WARMUP="${WARMUP:-15s}"
ROUNDS="${ROUNDS:-3}"
# Pinned, so a later k6 release can't change the numbers between runs.
K6_IMAGE="grafana/k6:2.3.0"
NETWORK="rategate_default"
PROM="${PROM:-http://localhost:9090}"
PYTHON="$(command -v python3 || command -v python)"

# Git Bash would rewrite the container paths (/scripts, /out) into Windows paths; this turns
# that off. It does nothing on Linux, where the script normally runs.
export MSYS_NO_PATHCONV=1

# A host folder as Docker wants it: a Windows path under Git Bash, the plain path on Linux.
host_path() {
  (cd "$1" && pwd -W 2>/dev/null) || (cd "$1" && pwd)
}

# The raw k6 summaries. They stay in /tmp (listed at the end) for anyone who wants to check
# the report against them.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/rategate-latency.XXXXXX")"
# k6 runs as its own user inside the container and must be able to write here.
chmod a+rwx "$WORK"
SCRIPTS_MOUNT="$(host_path "$ROOT/loadtest")"
WORK_MOUNT="$(host_path "$WORK")"

# k6 TARGET DURATION SUMMARY_NAME: one k6 run; its JSON summary lands in $WORK/SUMMARY_NAME.
k6() {
  docker run --rm --network "$NETWORK" \
    -v "$SCRIPTS_MOUNT:/scripts:ro" -v "$WORK_MOUNT:/out" \
    "$K6_IMAGE" run --quiet \
    -e TARGET="$1" -e RATE="$RATE" -e DURATION="$2" -e SUMMARY="/out/$3" \
    /scripts/latency.js
}

target_of() {
  case "$1" in
    direct) echo "http://backend:8081" ;;
    gateway) echo "http://gateway-1:8080" ;;
    nginx) echo "http://nginx:8080" ;;
  esac
}

# measure PATH NAME: warm-up, then the measured run saved as NAME.json. The warm-up fills
# connection pools and lets Go and the CPU settle; its numbers are thrown away.
measure() {
  local target
  target="$(target_of "$1")"
  case "$WARMUP" in
    0 | 0s) ;;
    *)
      echo "== $2: warm-up, $1 ($target), $WARMUP"
      k6 "$target" "$WARMUP" "warmup.json" >/dev/null
      ;;
  esac
  echo "== $2: measured, $1 ($target), $DURATION at $RATE req/s"
  k6 "$target" "$DURATION" "$2.json"
}

# wait_mode MODE: waits until both gateways report MODE on rategate_info in Prometheus. The
# gateway itself says which mode it runs in, so this proves the switch really happened.
wait_mode() {
  "$PYTHON" -I - "$PROM" "$1" <<'PY'
import json, sys, time, urllib.parse, urllib.request
prom, mode = sys.argv[1], sys.argv[2]
def count(q):
    url = prom + "/api/v1/query?" + urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(url, timeout=5) as r:
        res = json.load(r)["data"]["result"]
    return int(float(res[0]["value"][1])) if res else 0
deadline = time.time() + 60
while True:
    try:
        good = count('count(rategate_info{job="rategate",mode="%s"})' % mode)
        bad = count('count(rategate_info{job="rategate",mode!="%s"})' % mode)
        if good == 2 and bad == 0:
            print("both gateways report mode=%s" % mode)
            sys.exit(0)
    except Exception as e:  # Prometheus may still be starting
        good, bad = "?", str(e)
    if time.time() > deadline:
        sys.exit("gateways not in mode %s after 60 s (mode=%s: %s, other: %s)" % (mode, mode, good, bad))
    time.sleep(1)
PY
}

# If the script stops during the exact-mode pass, put the gateways back in sync mode, the
# state every other script expects.
IN_EXACT=0
restore_sync() {
  if [ "$IN_EXACT" = 1 ]; then
    echo "== putting the gateways back in sync mode"
    MODE=sync docker compose up -d --wait gateway-1 gateway-2 || true
  fi
}
trap restore_sync EXIT

echo "== starting the stack (sync mode)"
# MODE is set explicitly: a MODE left exported in the shell would otherwise leak into the
# headline run.
MODE=sync docker compose up --build -d --wait
wait_mode sync

# The machine the numbers come from. No hostname: it identifies the machine, not its size.
MACHINE_CPUS="$(nproc)"
MACHINE_MEM_GB="$(free -g | awk '/^Mem:/ {print $2}')"
MACHINE_OS="$(uname -srm)"
DOCKER_VERSION="$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unknown)"
STARTED="$(date -u '+%Y-%m-%d %H:%M UTC')"

for ((r = 1; r <= ROUNDS; r++)); do
  if ((r % 2 == 1)); then
    order="direct gateway nginx"
  else
    order="nginx gateway direct"
  fi
  for p in $order; do
    measure "$p" "r$r-$p"
  done
done

echo "== exact mode"
IN_EXACT=1
# nginx keeps running: it looks the gateways' names up again (deploy/nginx.conf), so it
# follows the recreated containers.
MODE=exact docker compose up -d --wait gateway-1 gateway-2
wait_mode exact
for ((r = 1; r <= ROUNDS; r++)); do
  measure nginx "exact-$r"
done
echo "== putting the gateways back in sync mode"
MODE=sync docker compose up -d --wait gateway-1 gateway-2
IN_EXACT=0
wait_mode sync

mkdir -p "$(dirname "$OUT")"
WORK="$WORK" OUT="$OUT" RATE="$RATE" DURATION="$DURATION" WARMUP="$WARMUP" ROUNDS="$ROUNDS" \
  K6_IMAGE="$K6_IMAGE" MACHINE_CPUS="$MACHINE_CPUS" MACHINE_MEM_GB="$MACHINE_MEM_GB" \
  MACHINE_OS="$MACHINE_OS" DOCKER_VERSION="$DOCKER_VERSION" STARTED="$STARTED" \
  "$PYTHON" -I - <<'PY'
# Renders the report from the k6 summaries. Every number in it comes from those files.
import json
import os
import statistics

env = os.environ
work, out = env["WORK"], env["OUT"]
rounds = int(env["ROUNDS"])
rate = float(env["RATE"])
warmup = ("no warm-up" if env["WARMUP"] in ("0", "0s")
          else "%s warm-up (not reported)" % env["WARMUP"])
PATHS = ["direct", "gateway", "nginx"]
LABEL = {
    "direct": "direct (backend)",
    "gateway": "one gateway",
    "nginx": "nginx + 2 gateways",
    "exact": "nginx + 2 gateways, exact",
}


def load(name):
    with open(os.path.join(work, name + ".json")) as f:
        return json.load(f)


def ms(x):
    return "n/a" if x is None else "%.3f" % x


def signed(x):
    return "%+.3f" % x


runs = {}  # (round, path) -> summary
for r in range(1, rounds + 1):
    for p in PATHS:
        runs[(r, p)] = load("r%d-%s" % (r, p))
exact = [load("exact-%d" % r) for r in range(1, rounds + 1)]


def p99(s):
    return s["http_req_duration_ms"]["p99"]


# A run only counts if k6 really sent the rate asked for and the target answered; these are
# listed rather than hidden, so the reader can judge the numbers.
warnings = []


def row(label, path, name, s):
    d = s["http_req_duration_ms"]
    achieved = s["iterations"]["rate"] or 0.0
    failed = s["http_req_failed_rate"] or 0.0
    dropped = s.get("dropped_iterations") or 0
    if achieved < 0.99 * rate:
        warnings.append("%s: achieved %.1f req/s of %.0f asked" % (name, achieved, rate))
    if failed > 0:
        warnings.append("%s: %.3f%% of requests failed" % (name, failed * 100))
    if dropped:
        warnings.append("%s: k6 dropped %d iterations (not enough VUs)" % (name, dropped))
    return "| %s | %s | %s | %s | %s | %s | %.1f | %.3f%% | %d |" % (
        label, LABEL[path], ms(d["med"]), ms(d["p90"]), ms(d["p99"]),
        ms(d["max"]), achieved, failed * 100, dropped)


lines = []
add = lines.append
add("# RateGate: added latency at %.0f requests/s" % rate)
add("")
add("Measured %s by `scripts/latency.sh`." % env["STARTED"])
add("")
add("**Machine:** %s CPUs, %s GB memory, %s, Docker %s. k6, nginx, both gateways, the backend, "
    "PostgreSQL and Prometheus all run on this one machine." % (
        env["MACHINE_CPUS"], env["MACHINE_MEM_GB"], env["MACHINE_OS"], env["DOCKER_VERSION"]))
add("")
add("**Load:** k6 (`%s`, `loadtest/latency.js`) in a container on the Compose network, constant "
    "arrival rate of %.0f requests/s, `GET /` with a random key among `load-0` ... `load-99` "
    "(limits too high to ever reject). Each run: %s, then %s measured. "
    "%d rounds; the order of the paths alternates between rounds." % (
        env["K6_IMAGE"], rate, warmup, env["DURATION"], rounds))
add("")
add("**Paths:** direct = the backend (`backend:8081`); one gateway = `gateway-1:8080`, no nginx; "
    "nginx + 2 gateways = `nginx:8080`, round robin over both gateways, sync mode (the headline).")
add("")
add("**Rule (fixed before measuring):** added p99 = p99 through the path minus p99 direct, "
    "in the same round. The headline is the **median of the per-round differences**.")
add("")

add("## Every measured run")
add("")
add("Latencies in ms, as k6 measures `http_req_duration` (request sent to response received).")
add("")
add("| Round | Path | p50 | p90 | p99 | max | achieved req/s | failed | dropped |")
add("|---|---|---|---|---|---|---|---|---|")
for r in range(1, rounds + 1):
    for p in PATHS:
        add(row(str(r), p, "round %d, %s" % (r, p), runs[(r, p)]))
add("")

add("## Added p99, per round")
add("")
add("| Round | direct p99 | one gateway p99 | nginx + 2 gateways p99 | added: one gateway | added: nginx + 2 gateways |")
add("|---|---|---|---|---|---|")
add_gw, add_ng = [], []
for r in range(1, rounds + 1):
    d, g, n = (p99(runs[(r, p)]) for p in PATHS)
    add_gw.append(g - d)
    add_ng.append(n - d)
    add("| %d | %s | %s | %s | %s | %s |" % (r, ms(d), ms(g), ms(n), signed(g - d), signed(n - d)))
add("")
head_ng = statistics.median(add_ng)
head_gw = statistics.median(add_gw)
headline = ("nginx + 2 gateways (sync mode) added p99 = %.3f ms at %.0f req/s "
            "(median of %d per-round differences: %s ms)" % (
                head_ng, rate, rounds, ", ".join(signed(x) for x in add_ng)))

add("## Result")
add("")
add("- **Headline: %s.**" % headline)
add("- One gateway without nginx: added p99 = %.3f ms (median of %s ms)." % (
    head_gw, ", ".join(signed(x) for x in add_gw)))
add("- Target, for comparison: about 2 ms added p99 at 1,000 requests/s.")
add("")

add("## Exact mode")
add("")
add("The gateways recreated with `MODE=exact` (one PostgreSQL round trip per request), nginx "
    "measured %d times, each the same way (%s, then %s measured). There is no direct run in "
    "this pass, so each run is compared with the median direct p99 of the rounds above." % (
        rounds, warmup, env["DURATION"]))
add("")
direct_med = statistics.median(p99(runs[(r, "direct")]) for r in range(1, rounds + 1))
add("| Run | Path | p50 | p90 | p99 | max | achieved req/s | failed | dropped |")
add("|---|---|---|---|---|---|---|---|---|")
for i, s in enumerate(exact, 1):
    add(row(str(i), "exact", "exact run %d" % i, s))
add("")
add_ex = [p99(s) - direct_med for s in exact]
add("- Median direct p99 (rounds above): %.3f ms." % direct_med)
add("- Added p99 per run: %s ms; **median %.3f ms**." % (
    ", ".join(signed(x) for x in add_ex), statistics.median(add_ex)))
add("")

if warnings:
    add("## Warnings")
    add("")
    for w in warnings:
        add("- " + w)
    add("")

with open(out, "w", newline="\n") as f:
    f.write("\n".join(lines))
print("headline: " + headline)
print("exact mode: added p99 = %.3f ms (median)" % statistics.median(add_ex))
for w in warnings:
    print("warning: " + w)
PY

echo "== report: $OUT (raw k6 summaries: $WORK)"
