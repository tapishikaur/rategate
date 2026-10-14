// Package limiter decides, per API key, whether a request may pass, and keeps that decision
// roughly consistent across several gateways that share one limit.
//
// Three modes, the two ends and the middle of the accuracy/cost trade-off:
//
//   - Sync (the default): decide from a local token bucket, in memory, with no network call.
//     Every Interval, send the tokens taken since the last sync to the shared store in one
//     batch, and replace each local count with the shared one. Between syncs every gateway
//     refills at the full rate, so together they can over-admit; the shared count then goes
//     into debt, which every gateway sees at its next sync and pays back before admitting
//     more. Over time the limit holds; the error is a short burst, bounded by the interval.
//   - Exact: one atomic update in the shared store per request. Never over-admits, but every
//     request waits for a database round trip.
//   - Local: no sharing at all. N gateways let through up to N times the limit.
//
// If the store fails, the limiter fails open: it keeps deciding from the local buckets (each
// gateway then enforces the full limit on its own), keeps up to one burst of unreported usage,
// and reports it once the store answers again.
package limiter

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/tapishikaur/rategate/internal/bucket"
)

type Mode string

const (
	Sync  Mode = "sync"
	Exact Mode = "exact"
	Local Mode = "local"
)

func ParseMode(s string) (Mode, error) {
	switch m := Mode(s); m {
	case Sync, Exact, Local:
		return m, nil
	}
	return "", fmt.Errorf("mode %q: want sync, exact or local", s)
}

// Limit is one key's rate limit: Rate tokens per second, at most Burst at once.
type Limit struct {
	Rate  float64
	Burst float64
}

// Usage is what one gateway took from one key's bucket since its last sync.
type Usage struct {
	Key   string
	Used  float64
	Limit Limit
}

// Store holds the buckets every gateway shares. Both methods refill a key's shared bucket
// first (at its rate, since its last update, up to its burst), atomically per key.
type Store interface {
	// Sync subtracts each key's usage from its shared bucket and returns every key's shared
	// count afterwards. The count may be negative: debt, paid back by refill. Keys must be
	// unique within one call.
	Sync(ctx context.Context, usage []Usage) (map[string]float64, error)
	// Take takes one token from the key's shared bucket if a whole one is there.
	Take(ctx context.Context, key string, limit Limit) (allowed bool, tokens float64, err error)
}

// Decision is the answer for one request.
type Decision struct {
	Allowed    bool
	Remaining  int64         // whole tokens left, as far as this gateway knows
	RetryAfter time.Duration // when refused: how long until a token is available
	Limit      Limit
}

type Config struct {
	Mode     Mode
	Interval time.Duration    // Sync mode: time between syncs (default 100 ms)
	Now      func() time.Time // the clock (default time.Now); tests pass a fake one
	// OnStore, if set, is called after every store call: op is "sync" or "take", keys is the
	// number of keys in it. The gateway feeds it into Prometheus.
	OnStore func(op string, keys int, took time.Duration, err error)
}

type Limiter struct {
	store    Store
	mode     Mode
	interval time.Duration
	now      func() time.Time
	onStore  func(op string, keys int, took time.Duration, err error)
	byKey    map[string]*entry // built once in New and never changed, so reads need no lock
	sorted   []*entry          // the same entries in key order, so syncs are reproducible
}

// entry is one key's local state. Each key has its own mutex, so requests for different keys
// never wait for each other.
type entry struct {
	mu      sync.Mutex
	key     string
	limit   Limit
	bucket  bucket.Bucket
	pending float64 // tokens taken since the last sync began
	touched bool    // a request (allowed or not) since the last sync began
}

// New makes a limiter for a fixed set of keys. store may be nil in Local mode.
func New(store Store, limits map[string]Limit, cfg Config) *Limiter {
	l := &Limiter{
		store:    store,
		mode:     cfg.Mode,
		interval: cfg.Interval,
		now:      cfg.Now,
		onStore:  cfg.OnStore,
		byKey:    make(map[string]*entry, len(limits)),
	}
	if l.mode == "" {
		l.mode = Sync
	}
	if l.interval <= 0 {
		l.interval = 100 * time.Millisecond
	}
	if l.now == nil {
		l.now = time.Now
	}
	start := l.now()
	for key, lim := range limits {
		e := &entry{key: key, limit: lim, bucket: bucket.New(lim.Rate, lim.Burst, start)}
		l.byKey[key] = e
		l.sorted = append(l.sorted, e)
	}
	sort.Slice(l.sorted, func(i, j int) bool { return l.sorted[i].key < l.sorted[j].key })
	return l
}

func (l *Limiter) Mode() Mode { return l.mode }

// Allow decides one request for key. ok is false if the key is unknown.
func (l *Limiter) Allow(ctx context.Context, key string) (d Decision, ok bool) {
	e := l.byKey[key]
	if e == nil {
		return Decision{}, false
	}
	if l.mode == Exact {
		if d, err := l.allowExact(ctx, e); err == nil {
			return d, true
		}
		// The store failed: fail open, and decide from the local bucket below.
	}
	now := l.now()
	e.mu.Lock()
	defer e.mu.Unlock()
	allowed := e.bucket.Take(now)
	if allowed && l.mode == Sync {
		e.pending++
	}
	e.touched = true
	d = Decision{Allowed: allowed, Limit: e.limit, Remaining: whole(e.bucket.Tokens(now))}
	if !allowed {
		d.RetryAfter = e.bucket.Wait(now)
	}
	return d, true
}

func (l *Limiter) allowExact(ctx context.Context, e *entry) (Decision, error) {
	start := time.Now()
	allowed, tokens, err := l.store.Take(ctx, e.key, e.limit)
	l.report("take", 1, time.Since(start), err)
	if err != nil {
		return Decision{}, err
	}
	now := l.now()
	e.mu.Lock()
	e.bucket.Set(tokens, now) // keeps the fail-open fallback close to the shared count
	e.mu.Unlock()
	d := Decision{Allowed: allowed, Limit: e.limit, Remaining: whole(tokens)}
	if !allowed {
		d.RetryAfter = time.Duration(math.Ceil((1 - tokens) / e.limit.Rate * float64(time.Second)))
	}
	return d, nil
}

// Run syncs every Interval until ctx is cancelled, then makes one last sync so the usage of
// the final interval isn't lost. It returns at once in Exact and Local modes.
func (l *Limiter) Run(ctx context.Context) {
	if l.mode != Sync {
		return
	}
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			l.SyncOnce(final) //nolint:errcheck // already reported through OnStore
			cancel()
			return
		case <-ticker.C:
			l.SyncOnce(ctx) //nolint:errcheck // reported through OnStore; the next tick retries
		}
	}
}

// SyncOnce sends the usage of every key touched since the last sync and adopts the shared
// counts. Keys that were only refused are included too (with usage 0), so a gateway that is
// refusing learns promptly when the shared bucket has refilled.
func (l *Limiter) SyncOnce(ctx context.Context) error {
	start := l.now()
	var usage []Usage
	var entries []*entry
	for _, e := range l.sorted {
		e.mu.Lock()
		if e.touched {
			usage = append(usage, Usage{Key: e.key, Used: e.pending, Limit: e.limit})
			entries = append(entries, e)
			e.pending, e.touched = 0, false
		}
		e.mu.Unlock()
	}
	if len(usage) == 0 {
		return nil
	}
	t := time.Now()
	shared, err := l.store.Sync(ctx, usage)
	l.report("sync", len(usage), time.Since(t), err)
	for i, e := range entries {
		e.mu.Lock()
		if err != nil {
			// Keep the usage for the next sync, up to one burst; meanwhile the local bucket
			// goes on deciding. One failed sync loses nothing. After a long outage, though,
			// reporting everything would put the shared bucket deep in debt and lock the key
			// out for about as long as the outage lasted. Over-admitting during an outage is
			// the price of failing open; the cap keeps it from being repaid as an outage.
			e.pending = min(e.pending+usage[i].Used, e.limit.Burst)
			e.touched = true
		} else if tokens, ok := shared[e.key]; ok {
			// The shared count is from the start of the sync (the database's clock reads at
			// the start of its transaction). What this gateway took since then is in pending.
			e.bucket.Set(tokens-e.pending, start)
		}
		e.mu.Unlock()
	}
	return err
}

func (l *Limiter) report(op string, keys int, took time.Duration, err error) {
	if l.onStore != nil {
		l.onStore(op, keys, took, err)
	}
}

// whole is the number of whole tokens in a count, 0 if it's negative.
func whole(tokens float64) int64 {
	if tokens < 0 {
		return 0
	}
	return int64(tokens)
}
