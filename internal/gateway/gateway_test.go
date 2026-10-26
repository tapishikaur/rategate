package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tapishikaur/rategate/internal/backend"
	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/metrics"
)

type rig struct {
	gateway  *httptest.Server
	backend  *backend.Server
	metrics  *metrics.Metrics
	upstream *httptest.Server
}

// newRig starts a backend and a gateway in front of it (local mode: these tests are about
// HTTP, the limiter has its own tests).
func newRig(t *testing.T, limits map[string]limiter.Limit) *rig {
	t.Helper()
	be := &backend.Server{}
	up := httptest.NewServer(be)
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	m := metrics.New("local")
	gw := httptest.NewServer(New(u, limiter.New(nil, limits, limiter.Config{Mode: limiter.Local}), m))
	t.Cleanup(gw.Close)
	return &rig{gateway: gw, backend: be, metrics: m, upstream: up}
}

func get(t *testing.T, url, key string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if key != "" {
		req.Header.Set(KeyHeader, key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp
}

// scrape returns the gateway's /metrics text.
func (r *rig) scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func TestAllowedRequestsReachTheBackend(t *testing.T) {
	r := newRig(t, map[string]limiter.Limit{"demo": {Rate: 1, Burst: 5}})
	resp := get(t, r.gateway.URL+"/orders?id=7", "demo")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-RateLimit-Limit"); got != "1" {
		t.Errorf("X-RateLimit-Limit %q, want 1", got)
	}
	if got := resp.Header.Get("X-RateLimit-Remaining"); got != "4" {
		t.Errorf("X-RateLimit-Remaining %q, want 4", got)
	}
	if got := r.backend.Stats().Keys["demo"]; got != 1 {
		t.Errorf("backend saw %d requests for demo, want 1 (the key is forwarded)", got)
	}
}

func TestMethodPathAndBodyAreForwarded(t *testing.T) {
	var got struct {
		method, path, body, forwardedFor string
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.method, got.path, got.body = r.Method, r.URL.RequestURI(), string(b)
		got.forwardedFor = r.Header.Get("X-Forwarded-For")
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "created") //nolint:errcheck
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	gw := httptest.NewServer(New(u, limiter.New(nil, map[string]limiter.Limit{"k": {Rate: 1, Burst: 1}},
		limiter.Config{Mode: limiter.Local}), metrics.New("local")))
	defer gw.Close()

	req, _ := http.NewRequest(http.MethodPost, gw.URL+"/a/b?c=d", strings.NewReader(`{"x":1}`))
	req.Header.Set(KeyHeader, "k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || string(body) != "created" {
		t.Errorf("response %d %q, want the backend's 201 \"created\"", resp.StatusCode, body)
	}
	if got.method != "POST" || got.path != "/a/b?c=d" || got.body != `{"x":1}` {
		t.Errorf("backend got %s %s %q", got.method, got.path, got.body)
	}
	if got.forwardedFor == "" {
		t.Error("X-Forwarded-For not set")
	}
}

func TestMissingOrUnknownKeyIs401(t *testing.T) {
	r := newRig(t, map[string]limiter.Limit{"demo": {Rate: 1, Burst: 5}})
	for _, key := range []string{"", "stolen"} {
		if resp := get(t, r.gateway.URL, key); resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("key %q: status %d, want 401", key, resp.StatusCode)
		}
	}
	if total := r.backend.Stats().Total; total != 0 {
		t.Errorf("backend saw %d requests, want 0", total)
	}
}

func TestOverTheLimitIs429AndNeverReachesTheBackend(t *testing.T) {
	r := newRig(t, map[string]limiter.Limit{"demo": {Rate: 0.25, Burst: 2}}) // a token every 4 s
	codes := map[int]int{}
	var last *http.Response
	for i := 0; i < 5; i++ {
		last = get(t, r.gateway.URL, "demo")
		codes[last.StatusCode]++
	}
	if codes[200] != 2 || codes[429] != 3 {
		t.Fatalf("status codes %v, want 2×200 and 3×429", codes)
	}
	if got := last.Header.Get("Retry-After"); got != "4" {
		t.Errorf("Retry-After %q, want 4 (seconds until the next token)", got)
	}
	if got := r.backend.Stats().Total; got != 2 {
		t.Errorf("backend saw %d requests, want 2", got)
	}
}

func TestBackendDownIs502(t *testing.T) {
	r := newRig(t, map[string]limiter.Limit{"demo": {Rate: 1, Burst: 5}})
	r.upstream.Close()
	if resp := get(t, r.gateway.URL, "demo"); resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", resp.StatusCode)
	}
	if !strings.Contains(r.scrape(t), `rategate_requests_total{outcome="upstream_error"} 1`) {
		t.Error("the failure isn't counted as upstream_error")
	}
}

func TestMetricsCountEveryOutcome(t *testing.T) {
	r := newRig(t, map[string]limiter.Limit{"demo": {Rate: 0.001, Burst: 3}})
	for i := 0; i < 5; i++ {
		get(t, r.gateway.URL, "demo")
	}
	get(t, r.gateway.URL, "nobody")
	text := r.scrape(t)
	for _, want := range []string{
		`rategate_requests_total{outcome="allowed"} 3`,
		`rategate_requests_total{outcome="limited"} 2`,
		`rategate_requests_total{outcome="unauthorized"} 1`,
		`rategate_requests_total{outcome="upstream_error"} 0`,
		`rategate_request_duration_seconds_count{outcome="allowed"} 3`,
		`rategate_limiter_decision_seconds_count 6`,
		`rategate_info{mode="local"} 1`,
		`rategate_store_errors_total{op="sync"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

func TestConcurrentRequestsAdmitExactlyTheBurst(t *testing.T) {
	r := newRig(t, map[string]limiter.Limit{"demo": {Rate: 0.001, Burst: 40}})
	var ok atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if get(t, r.gateway.URL, "demo").StatusCode == http.StatusOK {
					ok.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 40 || r.backend.Stats().Total != 40 {
		t.Fatalf("admitted %d, backend saw %d; want 40 each", ok.Load(), r.backend.Stats().Total)
	}
}
