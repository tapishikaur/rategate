// Package overadmit is the over-admission experiment: it sends traffic for some API keys to
// several gateways that share one limit per key, and measures how much more gets through than
// one perfect token bucket would let through.
//
// The load is open loop: requests are sent on a schedule fixed in advance (Poisson arrivals at
// the offered rate), never waiting for earlier responses. A closed loop (send, wait, send
// again) slows down exactly when the gateway does, which hides the behaviour being measured.
// A pool of workers does the sending, so the number of requests in flight stays bounded; if
// every worker is busy, a request goes out late, and its actual send time is what's recorded.
//
// Definitions (PLAN.md, section 6):
//
//   - allowed = Burst + Rate × T per key: what one perfect bucket, starting full, admits while
//     the tool sends for T seconds.
//   - over-admission = (admitted − allowed) / allowed, admitted as counted by the backend when
//     it's available (the ground truth), else the 200 responses the tool saw.
//   - worst 1 s = the most 200 responses whose send times fall in any 1-second window. One
//     exact bucket never admits more than Rate + Burst in a second.
//   - burst overshoot = requests admitted in the burst beyond Burst.
package overadmit

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tapishikaur/rategate/internal/gateway"
)

type Config struct {
	Targets    []string      // gateway base URLs
	Weights    []float64     // share of the traffic per target; nil means equal shares
	Keys       []string      // API keys; each gets the same load, independently
	Rate       float64       // offered requests per second, per key; 0 means the burst only
	Duration   time.Duration // how long to send at Rate
	Burst      int           // requests per key sent all at once, before the steady phase
	Idle       time.Duration // wait before sending, so the buckets refill to full
	Backend    string        // the backend's base URL; "" to skip its counts
	LimitRate  float64       // the keys' configured limit, for allowed and over-admission
	LimitBurst float64
	Seed       uint64 // for reproducible arrival times and target choices
	Workers    int    // requests in flight at most (default 256)
	Client     *http.Client
}

// Result is the experiment's outcome. The JSON field names are a contract: the sweep script
// and the results table read them.
type Result struct {
	Targets     []string  `json:"targets"`
	Weights     []float64 `json:"weights"`
	Keys        int       `json:"keys"`
	RatePerKey  float64   `json:"rate_per_key"`
	DurationS   float64   `json:"duration_s"` // T: from the first send to the last
	BurstPerKey int       `json:"burst_per_key"`

	Sent            int64 `json:"sent"`
	Admitted        int64 `json:"admitted"`         // 200 responses
	Limited         int64 `json:"limited"`          // 429 responses
	Errors          int64 `json:"errors"`           // anything else, including transport errors
	BackendAdmitted int64 `json:"backend_admitted"` // the backend's count for these keys; -1 if not asked

	AllowedPerKey float64 `json:"allowed_per_key"`
	Allowed       float64 `json:"allowed"`
	OverAdmission float64 `json:"over_admission"`
	Worst1sPerKey int     `json:"worst_1s_per_key"` // the worst key's
	ExactWorst1s  float64 `json:"exact_worst_1s"`

	BurstAdmittedPerKey  float64 `json:"burst_admitted_per_key"` // averaged over keys
	BurstOvershootPerKey float64 `json:"burst_overshoot_per_key"`

	// MaxLag is how late the latest request went out compared with its schedule. Large values
	// mean this machine couldn't keep up with the offered load; T and allowed already account
	// for it, but the load wasn't the one asked for.
	MaxLag time.Duration `json:"-"`
}

// Validate checks a configuration, and fills in equal weights if none are given.
func (c *Config) Validate() error {
	switch {
	case len(c.Targets) == 0:
		return errors.New("no targets")
	case len(c.Keys) == 0:
		return errors.New("no keys")
	case c.Rate < 0 || c.Duration < 0 || c.Burst < 0 || c.Idle < 0:
		return errors.New("rate, duration, burst and idle can't be negative")
	case c.Rate > 0 && c.Duration == 0:
		return errors.New("a rate needs a duration")
	case c.Rate == 0 && c.Burst == 0:
		return errors.New("nothing to send: give a rate and duration, a burst, or both")
	case c.LimitRate <= 0 || c.LimitBurst <= 0:
		return errors.New("the keys' limit (rate and burst) must be positive")
	}
	if c.Weights == nil {
		for range c.Targets {
			c.Weights = append(c.Weights, 1)
		}
	}
	if len(c.Weights) != len(c.Targets) {
		return fmt.Errorf("%d weights for %d targets", len(c.Weights), len(c.Targets))
	}
	total := 0.0
	for _, w := range c.Weights {
		if w < 0 {
			return errors.New("weights can't be negative")
		}
		total += w
	}
	if total == 0 {
		return errors.New("the weights add up to 0")
	}
	if c.Workers <= 0 {
		c.Workers = 256
	}
	return nil
}

// Run sends the load and measures what got through.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}
	client := cfg.Client
	if client == nil {
		client = newClient(cfg.Workers)
	}
	if cfg.Backend != "" {
		if err := resetBackend(ctx, client, cfg.Backend); err != nil {
			return Result{}, err
		}
	}
	if err := sleep(ctx, cfg.Idle); err != nil {
		return Result{}, err
	}

	items := Plan(cfg)
	recs := make([]record, len(items))
	jobs := make(chan int)
	start := time.Now()
	var wg sync.WaitGroup
	for range cfg.Workers {
		wg.Go(func() {
			for i := range jobs {
				it := items[i]
				recs[i] = send(ctx, client, cfg.Targets[it.Target], cfg.Keys[it.Key])
				recs[i].lag = recs[i].sent.Sub(start.Add(it.At))
			}
		})
	}
	// The dispatcher only waits for the clock, never for a response: a worker that is still
	// waiting for one just leaves the next request to another worker.
dispatch:
	for i, it := range items {
		if d := time.Until(start.Add(it.At)); d > 0 {
			select {
			case <-ctx.Done():
				break dispatch
			case <-time.After(d):
			}
		}
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	res := summarize(cfg, items, recs)
	res.BackendAdmitted = -1
	if cfg.Backend != "" {
		n, err := backendCount(ctx, client, cfg.Backend, cfg.Keys)
		if err != nil {
			return Result{}, err
		}
		res.BackendAdmitted = n
	}
	admitted := float64(res.Admitted)
	if res.BackendAdmitted >= 0 {
		admitted = float64(res.BackendAdmitted)
	}
	res.OverAdmission = (admitted - res.Allowed) / res.Allowed
	return res, nil
}

// Item is one planned request.
type Item struct {
	At     time.Duration // when to send it, from the start
	Key    int           // index into Config.Keys
	Target int           // index into Config.Targets
	Burst  bool          // part of the initial burst
}

// Plan is the schedule: first the burst (every key's, at time 0, spread over the targets by
// weight, exactly), then each key's Poisson arrivals at Rate for Duration, each sent to a
// target chosen at random by weight. Random choice splits a Poisson stream into independent
// Poisson streams, one per gateway, which is what a load balancer in front of them sees.
// cfg must be valid.
func Plan(cfg Config) []Item {
	var items []Item
	spread := make([]*roundRobin, len(cfg.Keys))
	for k := range cfg.Keys {
		spread[k] = newRoundRobin(cfg.Weights)
	}
	for range cfg.Burst {
		for k := range cfg.Keys { // interleaved, so no key's burst goes first
			items = append(items, Item{Key: k, Target: spread[k].next(), Burst: true})
		}
	}
	if cfg.Rate > 0 {
		mean := float64(time.Second) / cfg.Rate
		for k := range cfg.Keys {
			arrivals := rand.New(rand.NewPCG(cfg.Seed, uint64(2*k)))
			targets := rand.New(rand.NewPCG(cfg.Seed, uint64(2*k+1)))
			for at := time.Duration(0); ; {
				at += time.Duration(arrivals.ExpFloat64() * mean)
				if at >= cfg.Duration {
					break
				}
				items = append(items, Item{At: at, Key: k, Target: pick(targets, cfg.Weights)})
			}
		}
	}
	// Stable, so the burst stays first even if an arrival lands at 0.
	slices.SortStableFunc(items, func(a, b Item) int { return cmp.Compare(a.At, b.At) })
	return items
}

// roundRobin is smooth weighted round robin (as in nginx): each target is chosen in
// proportion to its weight, as evenly interleaved as possible. A burst of 500 with weights
// 9,1 goes exactly 450 and 50.
type roundRobin struct {
	weights, current []float64
	total            float64
}

func newRoundRobin(weights []float64) *roundRobin {
	r := &roundRobin{weights: weights, current: make([]float64, len(weights))}
	for _, w := range weights {
		r.total += w
	}
	return r
}

func (r *roundRobin) next() int {
	best := 0
	for i, w := range r.weights {
		r.current[i] += w
		if r.current[i] > r.current[best] {
			best = i
		}
	}
	r.current[best] -= r.total
	return best
}

func pick(rng *rand.Rand, weights []float64) int {
	total := 0.0
	for _, w := range weights {
		total += w
	}
	x := rng.Float64() * total
	for i, w := range weights {
		if x < w {
			return i
		}
		x -= w
	}
	return len(weights) - 1 // only reached through rounding
}

type record struct {
	sent   time.Time
	lag    time.Duration
	status int // 0: no response (a transport error)
}

func send(ctx context.Context, client *http.Client, target, key string) record {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(target, "/")+"/overadmit", nil)
	if err != nil {
		return record{sent: time.Now()}
	}
	req.Header.Set(gateway.KeyHeader, key)
	r := record{sent: time.Now()}
	resp, err := client.Do(req)
	if err != nil {
		return r
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck // read to the end, so the connection is reused
	resp.Body.Close()
	r.status = resp.StatusCode
	return r
}

func summarize(cfg Config, items []Item, recs []record) Result {
	res := Result{
		Targets: cfg.Targets, Weights: cfg.Weights, Keys: len(cfg.Keys),
		RatePerKey: cfg.Rate, BurstPerKey: cfg.Burst,
		ExactWorst1s: cfg.LimitRate + cfg.LimitBurst,
	}
	first, last := recs[0].sent, recs[0].sent
	for _, r := range recs {
		if r.sent.Before(first) {
			first = r.sent
		}
		if r.sent.After(last) {
			last = r.sent
		}
	}
	admittedAt := make([][]time.Duration, len(cfg.Keys)) // send times, from the first send
	burstAdmitted := 0
	for i, r := range recs {
		res.Sent++
		res.MaxLag = max(res.MaxLag, r.lag)
		switch r.status {
		case http.StatusOK:
			res.Admitted++
			admittedAt[items[i].Key] = append(admittedAt[items[i].Key], r.sent.Sub(first))
			if items[i].Burst {
				burstAdmitted++
			}
		case http.StatusTooManyRequests:
			res.Limited++
		default:
			res.Errors++
		}
	}
	// T is measured, not taken from the configuration: it matches -duration up to scheduling,
	// and if this machine fell behind, the longer send time is refill the perfect bucket would
	// have had too, so allowed isn't understated.
	res.DurationS = last.Sub(first).Seconds()
	res.AllowedPerKey = cfg.LimitBurst + cfg.LimitRate*res.DurationS
	res.Allowed = res.AllowedPerKey * float64(len(cfg.Keys))
	for _, at := range admittedAt {
		res.Worst1sPerKey = max(res.Worst1sPerKey, WorstWindow(at, time.Second))
	}
	if cfg.Burst > 0 {
		res.BurstAdmittedPerKey = float64(burstAdmitted) / float64(len(cfg.Keys))
		res.BurstOvershootPerKey = res.BurstAdmittedPerKey - cfg.LimitBurst // beyond B, not beyond what was sent
	}
	return res
}

// WorstWindow is the most times that fall in any window [t, t+w). times needn't be sorted.
func WorstWindow(times []time.Duration, w time.Duration) int {
	sorted := slices.Clone(times)
	slices.Sort(sorted)
	worst, lo := 0, 0
	for hi, at := range sorted {
		for at-sorted[lo] >= w {
			lo++
		}
		worst = max(worst, hi-lo+1)
	}
	return worst
}

func newClient(workers int) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Proxy:       nil, // straight to the gateways, whatever the environment says
			DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			// One idle connection per worker and target, so steady load reuses connections
			// instead of opening one per request (Go's default keeps only 2 per host).
			MaxIdleConns:        0,
			MaxIdleConnsPerHost: workers,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
}

func resetBackend(ctx context.Context, client *http.Client, base string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/stats/reset", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("resetting the backend's counts: %w", err)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resetting the backend's counts: status %d", resp.StatusCode)
	}
	return nil
}

// backendCount is the backend's count of requests for these keys only, so other traffic
// through the same backend doesn't count.
func backendCount(ctx context.Context, client *http.Client, base string, keys []string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/stats", nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("reading the backend's counts: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("reading the backend's counts: status %d", resp.StatusCode)
	}
	var stats struct {
		Keys map[string]int64 `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return 0, fmt.Errorf("reading the backend's counts: %w", err)
	}
	var n int64
	for _, k := range keys {
		n += stats.Keys[k]
	}
	return n, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
