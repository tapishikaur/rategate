"""Proves that the gateways' metrics reach Prometheus: sends traffic, then asks Prometheus.

It sends about 20 s of traffic through nginx: key load-0 (a huge limit, always allowed) and
key demo at about 150 requests/s, three times its limit of 50/s, so many requests get a 429.
Then it waits for Prometheus to scrape (every 2 s) and queries its HTTP API for the
rejection ratio, the p99 latency of allowed requests, the request rate per gateway and
whether both gateways are up. The queries and answers go into a Markdown file. It exits 1
if a check fails: both gateways up and reporting, a rejection ratio above 0, a p99 that is
a number.

Usage (with the Compose stack running, `docker compose up --build -d --wait`):

    python3 -I scripts/prom_check.py --out /tmp/prometheus-check.md
    python3 -I scripts/prom_check.py --out results/prometheus-check.md --screenshot results/prometheus.png

Options: --prom (default http://localhost:9090), --gateway-url (nginx, default
http://localhost:8080), --duration (seconds of traffic, default 20).

--screenshot also saves a picture of the Prometheus graph of the rejection ratio. It needs
Playwright, which is not in the standard library:

    pip install playwright
    python -m playwright install --with-deps chromium

Everything else is the standard library only.
"""

import argparse
import datetime
import http.client
import json
import math
import os
import sys
import threading
import time
import urllib.parse
import urllib.request

QUERIES = {
    "rejection_ratio": (
        "Rejection ratio (limited / all requests)",
        'sum(rate(rategate_requests_total{outcome="limited"}[30s])) / sum(rate(rategate_requests_total[30s]))',
    ),
    "p99": (
        "p99 latency of allowed requests, seconds",
        'histogram_quantile(0.99, sum by (le) (rate(rategate_request_duration_seconds_bucket{outcome="allowed"}[30s])))',
    ),
    "per_instance": (
        "Requests/s per gateway",
        "sum by (instance) (rate(rategate_requests_total[30s]))",
    ),
    "up": (
        "Scrape target up (1) or down (0)",
        'up{job="rategate"}',
    ),
}

# (key, requests per second). demo's limit is 50/s with a burst of 50, so at 150/s about
# two thirds of its requests should be refused; load-0 is never refused.
TRAFFIC = [("load-0", 100), ("demo", 150)]
# Several senders per key, so one slow response doesn't hold up the schedule.
SENDERS_PER_KEY = 5


def send(base, key, rate, stop_at, counts, lock):
    """Sends `rate` requests/s with `key` until stop_at, on one kept-alive connection.

    Requests go out on a fixed schedule rather than one after another, so the rate doesn't
    depend on how fast the gateway answers.
    """
    u = urllib.parse.urlsplit(base)
    conn_class = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
    path = (u.path.rstrip("/") or "") + "/"
    conn = None
    interval = 1.0 / rate
    next_at = time.monotonic()
    while True:
        now = time.monotonic()
        if now >= stop_at:
            break
        if next_at > now:
            time.sleep(next_at - now)
        next_at += interval
        try:
            if conn is None:
                conn = conn_class(u.hostname, u.port, timeout=5)
            conn.request("GET", path, headers={"X-API-Key": key})
            resp = conn.getresponse()
            resp.read()
            outcome = str(resp.status)
        except (OSError, http.client.HTTPException):
            outcome = "error"
            if conn is not None:
                conn.close()
            conn = None
        with lock:
            counts[key][outcome] = counts[key].get(outcome, 0) + 1
    if conn is not None:
        conn.close()


def traffic(base, seconds):
    """Runs the traffic mix for `seconds`; returns {key: {status: count}}."""
    counts = {key: {} for key, _ in TRAFFIC}
    lock = threading.Lock()
    stop_at = time.monotonic() + seconds
    threads = []
    for key, rate in TRAFFIC:
        for _ in range(SENDERS_PER_KEY):
            t = threading.Thread(target=send, args=(base, key, rate / SENDERS_PER_KEY, stop_at, counts, lock))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()
    return counts


def query(prom, expr):
    """One instant query; returns the result list ([{metric, value: [time, "number"]}])."""
    url = prom.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=10) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError("Prometheus: %s" % body.get("error", body))
    return body["data"]["result"]


def number(series):
    return float(series["value"][1])  # Prometheus sends "NaN" and "+Inf" as strings too


def scalar(result):
    """The single value of a one-series result, or None when there is none."""
    return number(result[0]) if len(result) == 1 else None


def graph_url(prom, expr):
    # The Prometheus 3 UI's query page; /graph with the same parameters also works and is
    # what Prometheus 2 used.
    return prom.rstrip("/") + "/query?" + urllib.parse.urlencode(
        {"g0.expr": expr, "g0.tab": "graph", "g0.range_input": "5m"})


def screenshot(prom, expr, path):
    """Saves a full-page PNG of the Prometheus graph of expr; True if a chart was drawn."""
    # Imported here so the rest of the script works without Playwright installed.
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeout
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit("--screenshot needs Playwright: pip install playwright && "
                 "python -m playwright install --with-deps chromium")

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            # "load", not "networkidle": the Prometheus 3 UI keeps a live notifications stream
            # open, so the network never goes idle.
            page.goto(graph_url(prom, expr), wait_until="load")
            drawn = True
            try:
                # Both UIs draw the graph on a canvas (uPlot in Prometheus 3, Flot in 2).
                page.wait_for_selector("canvas", state="visible", timeout=30000)
            except PlaywrightTimeout:
                drawn = False
            # Let the chart finish drawing (and its animation end) before the picture.
            page.wait_for_timeout(2000)
            page.screenshot(path=path, full_page=True)
        finally:
            browser.close()
    return drawn


def fmt(x):
    if x is None:
        return "no result"
    if math.isnan(x):
        return "NaN"  # what histogram_quantile gives when there were no observations
    return "%.6g" % x


def main():
    ap = argparse.ArgumentParser(description="Check that RateGate's metrics reach Prometheus.")
    ap.add_argument("--prom", default="http://localhost:9090", help="Prometheus base URL")
    ap.add_argument("--gateway-url", default="http://localhost:8080", help="nginx, in front of both gateways")
    ap.add_argument("--out", required=True, help="Markdown report to write")
    ap.add_argument("--screenshot", help="also save a PNG of the rejection-ratio graph (needs Playwright)")
    ap.add_argument("--duration", type=float, default=20, help="seconds of traffic")
    args = ap.parse_args()

    started = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("sending %.0f s of traffic through %s ..." % (args.duration, args.gateway_url))
    counts = traffic(args.gateway_url, args.duration)
    # Three scrape intervals (2 s each), so the last of the traffic is in Prometheus.
    time.sleep(6)

    results = {}
    for name, (_, expr) in QUERIES.items():
        results[name] = query(args.prom, expr)

    checks = []  # (description, passed)
    up = results["up"]
    checks.append(("both gateways are scrape targets and up (up == 1 for 2 instances)",
                   len(up) == 2 and all(number(s) == 1 for s in up)))
    per = results["per_instance"]
    checks.append(("both gateways report requests (2 instances, rate > 0)",
                   len(per) == 2 and all(number(s) > 0 for s in per)))
    ratio = scalar(results["rejection_ratio"])
    checks.append(("rejection ratio > 0", ratio is not None and math.isfinite(ratio) and ratio > 0))
    p99 = scalar(results["p99"])
    checks.append(("p99 of allowed requests is a number", p99 is not None and math.isfinite(p99)))

    shot_note = None
    if args.screenshot:
        drawn = screenshot(args.prom, QUERIES["rejection_ratio"][1], args.screenshot)
        checks.append(("the Prometheus graph rendered for the screenshot", drawn))
        shot_note = args.screenshot

    lines = []
    add = lines.append
    add("# RateGate: metrics in Prometheus")
    add("")
    add("Checked %s by `scripts/prom_check.py`: %.0f s of traffic through nginx (both gateways), "
        "then queries to the Prometheus HTTP API (`/api/v1/query`), which scrapes each gateway's "
        "`/metrics` every 2 s." % (started, args.duration))
    add("")
    add("## Traffic sent")
    add("")
    add("| Key | Asked rate (req/s) | Responses |")
    add("|---|---|---|")
    for key, rate in TRAFFIC:
        got = ", ".join("%s: %d" % (k, v) for k, v in sorted(counts[key].items())) or "none"
        add("| `%s` | %d | %s |" % (key, rate, got))
    add("")
    add("`load-0` has a limit too high to reach; `demo` has 50 requests/s with a burst of 50, so "
        "at 150/s most of its requests should get 429.")
    add("")
    add("## Queries")
    add("")
    for name, (title, expr) in QUERIES.items():
        add("**%s**" % title)
        add("")
        add("```promql")
        add(expr)
        add("```")
        add("")
        res = results[name]
        if not res:
            add("Result: no series.")
        elif len(res) == 1 and not res[0]["metric"]:
            add("Result: `%s`" % fmt(number(res[0])))
        else:
            for s in res:
                labels = ", ".join('%s="%s"' % (k, v) for k, v in sorted(s["metric"].items()))
                add("- `{%s}`: `%s`" % (labels, fmt(number(s))))
        add("")
    add("## Checks")
    add("")
    for desc, ok in checks:
        add("- %s: %s" % ("PASS" if ok else "FAIL", desc))
    add("")
    if ratio is not None and p99 is not None and math.isfinite(ratio) and math.isfinite(p99):
        add("So Prometheus saw %.1f%% of requests refused, and a p99 of %.3f ms for allowed "
            "requests (from the histogram's buckets, so an estimate)." % (ratio * 100, p99 * 1000))
        add("")
    if shot_note:
        add("Screenshot of the rejection ratio in the Prometheus UI: `%s`." % os.path.basename(shot_note))
        add("")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="\n") as f:
        f.write("\n".join(lines))

    for desc, ok in checks:
        print("%s: %s" % ("PASS" if ok else "FAIL", desc))
    print("rejection ratio %s, p99 %s s; report: %s" % (fmt(ratio), fmt(p99), args.out))
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
