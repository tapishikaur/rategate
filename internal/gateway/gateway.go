// Package gateway is the HTTP front door: it identifies the client by its API key, asks the
// limiter, and either refuses the request (401 or 429) or forwards it to the backend.
package gateway

import (
	"context"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/metrics"
)

// KeyHeader carries the client's API key. It is forwarded to the backend unchanged, so the
// backend knows which client a request belongs to.
const KeyHeader = "X-API-Key"

type Gateway struct {
	limiter *limiter.Limiter
	metrics *metrics.Metrics
	proxy   *httputil.ReverseProxy
}

// failedKey marks, in a request's context, that the proxy couldn't reach the backend.
type failedKey struct{}

func New(upstream *url.URL, l *limiter.Limiter, m *metrics.Metrics) *Gateway {
	g := &Gateway{limiter: l, metrics: m}
	g.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			pr.SetXForwarded()
		},
		// The gateway's own latency depends mostly on reusing connections to the backend:
		// a new TCP connection per request would cost a round trip and a lot of CPU. So
		// keep plenty of idle connections (Go's default keeps only 2 per host).
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:          1024,
			MaxIdleConnsPerHost:   1024,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			DisableCompression:    true, // pass bodies through as they are
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if failed, ok := r.Context().Value(failedKey{}).(*bool); ok {
				*failed = true
			}
			slog.Warn("backend unreachable", "err", err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return g
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	outcome := g.serve(w, r)
	g.metrics.Request(outcome, time.Since(start))
}

func (g *Gateway) serve(w http.ResponseWriter, r *http.Request) string {
	t := time.Now()
	d, known := g.limiter.Allow(r.Context(), r.Header.Get(KeyHeader))
	g.metrics.Decision(time.Since(t))
	if !known {
		http.Error(w, "missing or unknown API key", http.StatusUnauthorized)
		return metrics.Unauthorized
	}
	h := w.Header()
	h.Set("X-RateLimit-Limit", strconv.FormatFloat(d.Limit.Rate, 'f', -1, 64)) // per second
	h.Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	if !d.Allowed {
		// Retry-After is in whole seconds (RFC 9110), so round up, and never say 0.
		h.Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(d.RetryAfter.Seconds())))))
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return metrics.Limited
	}
	failed := false
	g.proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), failedKey{}, &failed)))
	if failed {
		return metrics.UpstreamError
	}
	return metrics.Allowed
}
