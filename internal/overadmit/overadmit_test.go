package overadmit

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/tapishikaur/rategate/internal/backend"
	"github.com/tapishikaur/rategate/internal/gateway"
	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/metrics"
	"github.com/tapishikaur/rategate/internal/store"
)

// The end-to-end tests run the whole path in one process: the tool sends real HTTP requests
// to two real gateways, which share one in-memory store (in sync and exact mode) and forward
// to one real backend, whose counts are the ground truth. Only PostgreSQL, Docker and the
// network are missing. Durations are short and rates small, and the bounds leave room for a
// slow, busy machine: what they check is the shape (local ≈ 2×, sync near 1×), not the third
// digit.

const interval = 50 * time.Millisecond

var (
	limit50 = limiter.Limit{Rate: 50, Burst: 50}
	keys2   = []string{"k1", "k2"}
)

// setup starts a backend and n gateways in front of it, and returns their URLs.
func setup(t *testing.T, mode limiter.Mode, n int, lim limiter.Limit, keys []string) (targets []string, backendURL string) {
	t.Helper()
	be := httptest.NewServer(&backend.Server{})
	t.Cleanup(be.Close)
	u, _ := url.Parse(be.URL)
	limits := map[string]limiter.Limit{}
	for _, k := range keys {
		limits[k] = lim
	}
	shared := &store.Memory{} // real clock, like the database's
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // runs last; the one per gateway below stops the sync loops in order
	for range n {
		var st limiter.Store
		if mode != limiter.Local {
			st = shared
		}
		l := limiter.New(st, limits, limiter.Config{Mode: mode, Interval: interval})
		done := make(chan struct{})
		go func() {
			l.Run(ctx)
			close(done)
		}()
		gw := httptest.NewServer(gateway.New(u, l, metrics.New(string(mode))))
		t.Cleanup(func() { <-done }) // cleanups run last-in first-out: close, cancel, then wait
		t.Cleanup(cancel)
		t.Cleanup(gw.Close)
		targets = append(targets, gw.URL)
	}
	return targets, be.URL
}

// steady is 6× the limit per key (300/s against 50/s), so each of two gateways alone gets 3×.
func steady(targets []string, backendURL string) Config {
	return Config{Targets: targets, Keys: keys2, Rate: 300, Duration: 3 * time.Second,
		Backend: backendURL, LimitRate: limit50.Rate, LimitBurst: limit50.Burst, Seed: 1}
}

func run(t *testing.T, cfg Config) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("sent %d, admitted %d (backend %d), limited %d, errors %d; allowed %.1f over %.3f s: over-admission %+.1f%%, worst 1 s %d, max lag %v",
		res.Sent, res.Admitted, res.BackendAdmitted, res.Limited, res.Errors, res.Allowed, res.DurationS,
		100*res.OverAdmission, res.Worst1sPerKey, res.MaxLag)
	if res.Errors != 0 {
		t.Errorf("%d requests failed", res.Errors)
	}
	if res.BackendAdmitted != res.Admitted {
		t.Errorf("the backend counted %d, the tool saw %d responses with 200", res.BackendAdmitted, res.Admitted)
	}
	if res.Sent != res.Admitted+res.Limited+res.Errors {
		t.Errorf("sent %d ≠ admitted + limited + errors", res.Sent)
	}
	return res
}

func TestLocalModeAdmitsTwiceTheLimit(t *testing.T) {
	targets, be := setup(t, limiter.Local, 2, limit50, keys2)
	res := run(t, steady(targets, be))
	// No sharing: each gateway enforces the whole limit on its own half of the traffic.
	if ratio := float64(res.BackendAdmitted) / res.Allowed; ratio < 1.8 || ratio > 2.05 {
		t.Errorf("admitted %.2f× allowed, want about 2×", ratio)
	}
	if got, want := res.OverAdmission, float64(res.BackendAdmitted)/res.Allowed-1; math.Abs(got-want) > 1e-9 {
		t.Errorf("over_admission %v, want %v", got, want)
	}
}

func TestSyncModeStaysNearTheLimit(t *testing.T) {
	targets, be := setup(t, limiter.Sync, 2, limit50, keys2)
	res := run(t, steady(targets, be))
	// The design's bound (limiter/sim_test.go): with n gateways the count is off by at most
	// (n-1) × (Burst + Rate × Interval) per key. The slack is for real time: a sync tick that
	// fires late on a busy machine is a longer interval, so allow a quarter second more.
	bound := 1 * (limit50.Burst + limit50.Rate*interval.Seconds())
	slack := limit50.Rate*0.25 + 5
	off := float64(res.BackendAdmitted) - res.Allowed
	if limit := float64(len(keys2)) * (bound + slack); math.Abs(off) > limit {
		t.Errorf("admitted %d, %+.1f from the %.1f allowed, beyond the bound of ±%.1f", res.BackendAdmitted, off, res.Allowed, limit)
	}
}

func TestExactModeIsExact(t *testing.T) {
	targets, be := setup(t, limiter.Exact, 2, limit50, keys2)
	res := run(t, steady(targets, be))
	// One shared bucket, so admitted ≈ allowed. Each decision happens a little after its
	// send (the request's trip to the gateway), so the bucket refills a little past the last
	// send: allow for 0.2 s of that, per key.
	slack := float64(len(keys2)) * (limit50.Rate*0.2 + 2)
	if off := float64(res.BackendAdmitted) - res.Allowed; math.Abs(off) > slack {
		t.Errorf("admitted %d, %+.1f from the %.1f allowed", res.BackendAdmitted, off, res.Allowed)
	}
	// No check on the worst 1 s here: it is measured by send time, and on a busy machine the
	// gateway decides some requests well after they were sent, which bunches admitted send
	// times together. The real stack, with spare CPU, is where that number means something.
}

func TestBurstOverTwoLocalGatewaysOvershootsByOneBurst(t *testing.T) {
	// Almost no refill, so the count doesn't depend on how fast this machine sends.
	lim := limiter.Limit{Rate: 0.5, Burst: 50}
	targets, be := setup(t, limiter.Local, 2, lim, keys2)
	res := run(t, Config{Targets: targets, Keys: keys2, Burst: 200, Backend: be,
		LimitRate: lim.Rate, LimitBurst: lim.Burst, Seed: 1})
	if res.Sent != 400 {
		t.Errorf("sent %d, want 2 keys × 200", res.Sent)
	}
	// Each gateway gets 100 of each key's 200 and lets its own full bucket through: 2 × 50.
	if res.BurstAdmittedPerKey < 100 || res.BurstAdmittedPerKey > 102 {
		t.Errorf("%.1f admitted per key in the burst, want 100 (a full bucket per gateway)", res.BurstAdmittedPerKey)
	}
	if res.BurstOvershootPerKey < lim.Burst || res.BurstOvershootPerKey > lim.Burst+2 {
		t.Errorf("burst overshoot %.1f per key, want about %v (one extra burst)", res.BurstOvershootPerKey, lim.Burst)
	}
	if res.Worst1sPerKey != int(res.BurstAdmittedPerKey) {
		t.Errorf("worst 1 s %d, want the whole burst, %v", res.Worst1sPerKey, res.BurstAdmittedPerKey)
	}

	// The JSON is a contract with the sweep script: every field, by these exact names.
	b, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	want := []string{"targets", "weights", "keys", "rate_per_key", "duration_s", "burst_per_key",
		"sent", "admitted", "limited", "errors", "backend_admitted",
		"allowed_per_key", "allowed", "over_admission", "worst_1s_per_key", "exact_worst_1s",
		"burst_admitted_per_key", "burst_overshoot_per_key"}
	for _, f := range want {
		if _, ok := fields[f]; !ok {
			t.Errorf("JSON has no %q: %s", f, b)
		}
	}
	if len(fields) != len(want) {
		t.Errorf("JSON has %d fields, want %d: %s", len(fields), len(want), b)
	}
	if fields["keys"] != 2.0 || fields["burst_per_key"] != 200.0 || fields["backend_admitted"] != float64(res.Admitted) {
		t.Errorf("JSON values don't match the result: %s", b)
	}
	if w, _ := fields["weights"].([]any); len(w) != 2 || w[0] != 1.0 || w[1] != 1.0 {
		t.Errorf("weights %v, want the default [1 1]", fields["weights"])
	}
}

func TestWithoutABackendTheToolsCountIsUsed(t *testing.T) {
	lim := limiter.Limit{Rate: 0.5, Burst: 10}
	targets, _ := setup(t, limiter.Local, 1, lim, []string{"k"})
	res, err := Run(context.Background(), Config{Targets: targets, Keys: []string{"k"}, Burst: 30,
		LimitRate: lim.Rate, LimitBurst: lim.Burst})
	if err != nil {
		t.Fatal(err)
	}
	if res.BackendAdmitted != -1 {
		t.Errorf("backend_admitted %d without a backend, want -1", res.BackendAdmitted)
	}
	if want := (float64(res.Admitted) - res.Allowed) / res.Allowed; res.OverAdmission != want {
		t.Errorf("over_admission %v, want %v from the tool's own count", res.OverAdmission, want)
	}
}

func TestWorstWindow(t *testing.T) {
	ms := func(v ...int) []time.Duration {
		var out []time.Duration
		for _, x := range v {
			out = append(out, time.Duration(x)*time.Millisecond)
		}
		return out
	}
	for _, tc := range []struct {
		times []time.Duration
		want  int
	}{
		{nil, 0},
		{ms(0), 1},
		{ms(0, 0, 0), 3},
		// [1000, 2000) holds 1000, 1500, 1600, 1700, 1990; any window from 0 ends before 1000.
		{ms(0, 100, 500, 990, 1000, 1500, 1600, 1700, 1990), 5},
		// The window is half-open: 0 and 1000 are a full second apart, so never together.
		{ms(0, 1000, 2000, 3000), 1},
		{ms(0, 999), 2},
		// Order doesn't matter.
		{ms(1990, 0, 1500, 100, 1000, 990, 1700, 500, 1600), 5},
	} {
		in := slices.Clone(tc.times)
		if got := WorstWindow(tc.times, time.Second); got != tc.want {
			t.Errorf("WorstWindow(%v) = %d, want %d", tc.times, got, tc.want)
		}
		if !slices.Equal(in, tc.times) {
			t.Errorf("WorstWindow changed its input")
		}
	}
}

func TestPlan(t *testing.T) {
	cfg := Config{Targets: []string{"a", "b"}, Weights: []float64{9, 1}, Keys: []string{"x", "y"},
		Rate: 300, Duration: 10 * time.Second, Burst: 500, LimitRate: 1, LimitBurst: 1, Seed: 1}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	items := Plan(cfg)
	burst := map[[2]int]int{} // [key, target] → requests
	steady := map[int]int{}   // target → requests
	perKey := map[int]int{}
	for i, it := range items {
		if i > 0 && it.At < items[i-1].At {
			t.Fatalf("item %d is out of order", i)
		}
		if it.Burst {
			if it.At != 0 || i >= 2*cfg.Burst {
				t.Fatalf("burst item %d at %v: the burst must come first, at 0", i, it.At)
			}
			burst[[2]int{it.Key, it.Target}]++
			continue
		}
		if it.At <= 0 || it.At >= cfg.Duration {
			t.Fatalf("arrival at %v, outside (0, %v)", it.At, cfg.Duration)
		}
		steady[it.Target]++
		perKey[it.Key]++
	}
	// The burst is split exactly by weight, for every key.
	for k := range cfg.Keys {
		if burst[[2]int{k, 0}] != 450 || burst[[2]int{k, 1}] != 50 {
			t.Errorf("key %d's burst went %d/%d, want 450/50", k, burst[[2]int{k, 0}], burst[[2]int{k, 1}])
		}
	}
	// Poisson: 3,000 arrivals per key expected, standard deviation √3000 ≈ 55.
	for k, n := range perKey {
		if math.Abs(float64(n)-3000) > 5*55 {
			t.Errorf("key %d: %d arrivals in 10 s at 300/s", k, n)
		}
	}
	if share := float64(steady[0]) / float64(steady[0]+steady[1]); share < 0.88 || share > 0.92 {
		t.Errorf("target a got %.3f of the steady traffic, want 0.9", share)
	}
	// Reproducible with the same seed, different with another.
	if !slices.Equal(items, Plan(cfg)) {
		t.Error("the same seed gave a different plan")
	}
	cfg.Seed = 2
	if slices.Equal(items, Plan(cfg)) {
		t.Error("a different seed gave the same plan")
	}
}

func TestValidate(t *testing.T) {
	ok := Config{Targets: []string{"a"}, Keys: []string{"k"}, Rate: 10, Duration: time.Second, LimitRate: 1, LimitBurst: 1}
	for name, change := range map[string]func(*Config){
		"no targets":         func(c *Config) { c.Targets = nil },
		"no keys":            func(c *Config) { c.Keys = nil },
		"rate, no duration":  func(c *Config) { c.Duration = 0 },
		"nothing to send":    func(c *Config) { c.Rate, c.Duration = 0, 0 },
		"no limit":           func(c *Config) { c.LimitRate = 0 },
		"weights mismatch":   func(c *Config) { c.Weights = []float64{1, 1} },
		"zero weights":       func(c *Config) { c.Weights = []float64{0} },
		"negative burst":     func(c *Config) { c.Burst = -1 },
		"negative weight":    func(c *Config) { c.Targets, c.Weights = []string{"a", "b"}, []float64{2, -1} },
		"negative idle time": func(c *Config) { c.Idle = -time.Second },
	} {
		c := ok
		change(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	burstOnly := Config{Targets: []string{"a", "b"}, Keys: []string{"k"}, Burst: 500, LimitRate: 1, LimitBurst: 1}
	if err := burstOnly.Validate(); err != nil {
		t.Errorf("a burst alone (rate 0, duration 0): %v", err)
	}
	if !slices.Equal(burstOnly.Weights, []float64{1, 1}) {
		t.Errorf("default weights %v, want equal", burstOnly.Weights)
	}
}
