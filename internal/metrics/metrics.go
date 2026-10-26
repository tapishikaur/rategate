// Package metrics is what each gateway exports to Prometheus on its admin port, /metrics.
//
// No metric has a per-key label. With thousands of API keys that would be thousands of time
// series per metric, which is the classic way to overload a Prometheus server. Per-key counts
// belong in logs or a separate usage pipeline.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The outcomes of a request through the gateway.
const (
	Allowed       = "allowed"        // forwarded to the backend
	Limited       = "limited"        // refused with 429
	Unauthorized  = "unauthorized"   // no API key, or an unknown one: 401
	UpstreamError = "upstream_error" // allowed, but the backend couldn't be reached: 502
)

type Metrics struct {
	registry   *prometheus.Registry
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	decision   prometheus.Histogram
	storeTime  *prometheus.HistogramVec
	storeFails *prometheus.CounterVec
	storeKeys  prometheus.Histogram
}

// New registers every metric. mode (sync, exact or local) is exported as a constant label on
// rategate_info, so dashboards can tell which mode a gateway runs in.
func New(mode string) *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{
		registry: r,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rategate_requests_total",
			Help: "Requests through the gateway, by outcome: allowed, limited (429), unauthorized (401), upstream_error (502).",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "rategate_request_duration_seconds",
			Help: "Time from receiving a request to finishing the response, including the backend for allowed requests.",
			// Fine-grained around the 1-5 ms that matter for a gateway's own overhead.
			Buckets: []float64{.0001, .00025, .0005, .001, .0015, .002, .003, .005, .0075, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"outcome"}),
		decision: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "rategate_limiter_decision_seconds",
			Help: "Time to decide whether a request may pass: in memory in sync and local mode, a database round trip in exact mode.",
			Buckets: []float64{.000001, .0000025, .000005, .00001, .000025, .00005, .0001, .00025, .0005,
				.001, .0025, .005, .01, .025, .1},
		}),
		storeTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rategate_store_duration_seconds",
			Help:    "Duration of calls to the shared PostgreSQL store: op=sync (one batch per interval) or op=take (exact mode, one per request).",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"op"}),
		storeFails: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rategate_store_errors_total",
			Help: "Failed calls to the shared store. While they fail, the gateway limits on its own (fail-open).",
		}, []string{"op"}),
		storeKeys: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "rategate_sync_keys",
			Help:    "API keys carried by one sync.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 8), // 1 to 16384
		}),
	}
	info := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "rategate_info",
		Help:        "Always 1; the labels describe the gateway.",
		ConstLabels: prometheus.Labels{"mode": mode},
	})
	info.Set(1)
	r.MustRegister(m.requests, m.duration, m.decision, m.storeTime, m.storeFails, m.storeKeys, info,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	// Create every labelled series now, at 0, so rates and ratios work from the first scrape
	// instead of appearing only after the first event of each kind.
	for _, o := range []string{Allowed, Limited, Unauthorized, UpstreamError} {
		m.requests.WithLabelValues(o)
		m.duration.WithLabelValues(o)
	}
	for _, op := range []string{"sync", "take"} {
		m.storeTime.WithLabelValues(op)
		m.storeFails.WithLabelValues(op)
	}
	return m
}

// Request records one finished request.
func (m *Metrics) Request(outcome string, took time.Duration) {
	m.requests.WithLabelValues(outcome).Inc()
	m.duration.WithLabelValues(outcome).Observe(took.Seconds())
}

// Decision records the time one limiter decision took.
func (m *Metrics) Decision(took time.Duration) { m.decision.Observe(took.Seconds()) }

// Store records one call to the shared store; its signature matches limiter.Config.OnStore.
func (m *Metrics) Store(op string, keys int, took time.Duration, err error) {
	m.storeTime.WithLabelValues(op).Observe(took.Seconds())
	if err != nil {
		m.storeFails.WithLabelValues(op).Inc()
	}
	if op == "sync" {
		m.storeKeys.Observe(float64(keys))
	}
}

// Handler serves the metrics in Prometheus's text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}
