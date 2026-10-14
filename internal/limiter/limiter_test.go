package limiter_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/store"
)

const still = 1e-9 // a rate so small that refill during a test is invisible

// clock is a fake clock shared by limiters and the Memory store.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

func (c *clock) Advance(d time.Duration) { c.Set(c.Now().Add(d)) }

func one(key string, lim limiter.Limit) map[string]limiter.Limit {
	return map[string]limiter.Limit{key: lim}
}

func allow(t *testing.T, l *limiter.Limiter, key string) limiter.Decision {
	t.Helper()
	d, ok := l.Allow(context.Background(), key)
	if !ok {
		t.Fatalf("key %q unknown", key)
	}
	return d
}

func count(t *testing.T, l *limiter.Limiter, key string, n int) (allowed int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if allow(t, l, key).Allowed {
			allowed++
		}
	}
	return allowed
}

func TestUnknownKey(t *testing.T) {
	l := limiter.New(nil, one("k", limiter.Limit{Rate: 1, Burst: 1}), limiter.Config{Mode: limiter.Local})
	if _, ok := l.Allow(context.Background(), "nope"); ok {
		t.Fatal("an unknown key was accepted")
	}
}

func TestLocalModeIsOneBucket(t *testing.T) {
	c := newClock()
	l := limiter.New(nil, one("k", limiter.Limit{Rate: 2, Burst: 3}), limiter.Config{Mode: limiter.Local, Now: c.Now})
	for want := int64(2); want >= 0; want-- {
		d := allow(t, l, "k")
		if !d.Allowed || d.Remaining != want {
			t.Fatalf("got %+v, want allowed with %d remaining", d, want)
		}
	}
	d := allow(t, l, "k")
	if d.Allowed || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("4th request: %+v, want refused with RetryAfter 500ms (2 tokens/s)", d)
	}
	c.Advance(500 * time.Millisecond)
	if !allow(t, l, "k").Allowed {
		t.Fatal("refused after RetryAfter")
	}
}

func TestSyncModeSharesTheLimit(t *testing.T) {
	c := newClock()
	mem := &store.Memory{Now: c.Now}
	lim := limiter.Limit{Rate: 10, Burst: 10}
	cfg := limiter.Config{Mode: limiter.Sync, Now: c.Now}
	a := limiter.New(mem, one("k", lim), cfg)
	b := limiter.New(mem, one("k", lim), cfg)

	// Before any sync both gateways believe the bucket is full: together they admit two
	// bursts. This is the overshoot the design accepts (and the README measures).
	if got := count(t, a, "k", 10) + count(t, b, "k", 10); got != 20 {
		t.Fatalf("admitted %d before the first sync, want 20 (a full burst each)", got)
	}
	ctx := context.Background()
	if err := a.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if mem.Taken("k") != 20 {
		t.Fatalf("store has %v taken, want 20", mem.Taken("k"))
	}
	// The shared bucket is now 10 in debt (10 - 20), and b knows it: it refuses until the
	// debt is paid back, at 10 tokens/s that's 1 s, plus 0.1 s for a whole token.
	if d := allow(t, b, "k"); d.Allowed || d.RetryAfter != 1100*time.Millisecond {
		t.Fatalf("b in debt: %+v, want refused with RetryAfter 1.1s", d)
	}
	// a synced before b's usage arrived; its next sync (it's been touched now) catches up.
	allow(t, a, "k")
	a.SyncOnce(ctx) //nolint:errcheck
	if d := allow(t, a, "k"); d.Allowed {
		t.Fatalf("a still admits after learning of the debt: %+v", d)
	}
	c.Advance(1100 * time.Millisecond)
	if !allow(t, a, "k").Allowed {
		t.Fatal("a still refuses after the debt was paid back")
	}
}

// blockingStore lets a test run code while a Sync is in flight: each Sync signals entered
// (buffered, so later syncs don't wait for a reader) and then waits until release is closed.
type blockingStore struct {
	limiter.Store
	entered, release chan struct{}
}

func (s *blockingStore) Sync(ctx context.Context, usage []limiter.Usage) (map[string]float64, error) {
	s.entered <- struct{}{}
	<-s.release
	return s.Store.Sync(ctx, usage)
}

func TestUsageTakenDuringASyncIsKept(t *testing.T) {
	mem := &store.Memory{}
	bs := &blockingStore{Store: mem, entered: make(chan struct{}, 8), release: make(chan struct{})}
	l := limiter.New(bs, one("k", limiter.Limit{Rate: still, Burst: 10}), limiter.Config{Mode: limiter.Sync})
	count(t, l, "k", 4)
	done := make(chan error)
	go func() { done <- l.SyncOnce(context.Background()) }()
	<-bs.entered
	count(t, l, "k", 2) // while the sync carrying the first 4 is in flight
	close(bs.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Shared: 10 - 4 = 6. Locally 2 more were taken since, so 4 are left.
	if d := allow(t, l, "k"); !d.Allowed || d.Remaining != 3 {
		t.Fatalf("got %+v, want allowed with 3 remaining", d)
	}
	l.SyncOnce(context.Background()) //nolint:errcheck
	if got := mem.Taken("k"); got != 7 {
		t.Fatalf("store has %v taken, want 7: 4 + the 2 taken mid-sync + 1", got)
	}
}

// flakyStore fails while down is set.
type flakyStore struct {
	limiter.Store
	down atomic.Bool
}

var errDown = errors.New("database unreachable")

func (s *flakyStore) Sync(ctx context.Context, usage []limiter.Usage) (map[string]float64, error) {
	if s.down.Load() {
		return nil, errDown
	}
	return s.Store.Sync(ctx, usage)
}

func (s *flakyStore) Take(ctx context.Context, key string, lim limiter.Limit) (bool, float64, error) {
	if s.down.Load() {
		return false, 0, errDown
	}
	return s.Store.Take(ctx, key, lim)
}

func TestSyncModeFailsOpenAndReportsLater(t *testing.T) {
	mem := &store.Memory{}
	fs := &flakyStore{Store: mem}
	var failures atomic.Int32
	l := limiter.New(fs, one("k", limiter.Limit{Rate: still, Burst: 10}), limiter.Config{
		Mode: limiter.Sync,
		OnStore: func(op string, keys int, took time.Duration, err error) {
			if err != nil {
				failures.Add(1)
			}
		},
	})
	fs.down.Store(true)
	if got := count(t, l, "k", 6); got != 6 {
		t.Fatalf("admitted %d of 6 with the database down, want 6 (local bucket)", got)
	}
	if err := l.SyncOnce(context.Background()); !errors.Is(err, errDown) {
		t.Fatalf("SyncOnce with the database down: %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("OnStore saw %d failures, want 1", failures.Load())
	}
	count(t, l, "k", 2)
	fs.down.Store(false)
	if err := l.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := mem.Taken("k"); got != 8 {
		t.Fatalf("store has %v taken after recovery, want 8: nothing may be lost", got)
	}
}

func TestALongOutageCarriesAtMostOneBurst(t *testing.T) {
	c := newClock()
	mem := &store.Memory{Now: c.Now}
	fs := &flakyStore{Store: mem}
	l := limiter.New(fs, one("k", limiter.Limit{Rate: 10, Burst: 10}), limiter.Config{Mode: limiter.Sync, Now: c.Now})
	fs.down.Store(true)
	admitted := 0
	for i := 0; i < 5; i++ { // 5 s of outage, the client using everything it's allowed
		admitted += count(t, l, "k", 20)
		l.SyncOnce(context.Background()) //nolint:errcheck
		c.Advance(time.Second)
	}
	if admitted != 50 {
		t.Fatalf("admitted %d during the outage, want 50 (burst 10, then 10/s)", admitted)
	}
	fs.down.Store(false)
	if err := l.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := mem.Taken("k"); got != 10 {
		t.Fatalf("reported %v after the outage, want 10 (one burst), not all 50", got)
	}
}

func TestExactModeNeverOverAdmitsAcrossGateways(t *testing.T) {
	mem := &store.Memory{}
	lim := limiter.Limit{Rate: still, Burst: 10}
	cfg := limiter.Config{Mode: limiter.Exact}
	a := limiter.New(mem, one("k", lim), cfg)
	b := limiter.New(mem, one("k", lim), cfg)
	allowed := 0
	for i := 0; i < 20; i++ {
		l := a
		if i%2 == 1 {
			l = b
		}
		if allow(t, l, "k").Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("exact mode admitted %d with burst 10 over two gateways", allowed)
	}
}

func TestExactModeFailsOpenToTheLocalBucket(t *testing.T) {
	fs := &flakyStore{Store: &store.Memory{}}
	fs.down.Store(true)
	l := limiter.New(fs, one("k", limiter.Limit{Rate: still, Burst: 3}), limiter.Config{Mode: limiter.Exact})
	if got := count(t, l, "k", 5); got != 3 {
		t.Fatalf("admitted %d of 5 with the database down, want 3 (the local burst)", got)
	}
}

func TestConcurrentUseConservesTokens(t *testing.T) {
	// Two gateways, eight goroutines each hammering four keys, syncs running all the while,
	// real clock: every admitted request must reach the shared store exactly once.
	mem := &store.Memory{}
	limits := map[string]limiter.Limit{}
	for i := 0; i < 4; i++ {
		limits[fmt.Sprintf("k%d", i)] = limiter.Limit{Rate: 5000, Burst: 50}
	}
	gws := []*limiter.Limiter{
		limiter.New(mem, limits, limiter.Config{Mode: limiter.Sync}),
		limiter.New(mem, limits, limiter.Config{Mode: limiter.Sync}),
	}
	var admitted [4]atomic.Int64
	stop := make(chan struct{})
	var syncs sync.WaitGroup
	for _, l := range gws {
		syncs.Add(1)
		go func() {
			defer syncs.Done()
			for {
				select {
				case <-stop:
					return
				default:
					l.SyncOnce(context.Background()) //nolint:errcheck
				}
			}
		}()
	}
	var workers sync.WaitGroup
	for _, l := range gws {
		for w := 0; w < 8; w++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for i := 0; i < 2000; i++ {
					k := (w + i) % 4
					if d, _ := l.Allow(context.Background(), fmt.Sprintf("k%d", k)); d.Allowed {
						admitted[k].Add(1)
					}
				}
			}()
		}
	}
	workers.Wait()
	close(stop)
	syncs.Wait()
	for _, l := range gws {
		if err := l.SyncOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for k := range admitted {
		key := fmt.Sprintf("k%d", k)
		if got, want := mem.Taken(key), float64(admitted[k].Load()); got != want {
			t.Errorf("%s: store has %v taken, gateways admitted %v", key, got, want)
		}
	}
}

func TestRunSyncsUntilCancelledThenFlushes(t *testing.T) {
	mem := &store.Memory{}
	l := limiter.New(mem, one("k", limiter.Limit{Rate: still, Burst: 100}),
		limiter.Config{Mode: limiter.Sync, Interval: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()
	count(t, l, "k", 5)
	deadline := time.Now().Add(10 * time.Second)
	for mem.Taken("k") != 5 {
		if time.Now().After(deadline) {
			t.Fatalf("no sync within 10 s: store has %v taken", mem.Taken("k"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	count(t, l, "k", 3)
	cancel()
	<-done
	if got := mem.Taken("k"); got != 8 {
		t.Fatalf("store has %v taken after shutdown, want 8: the final sync was lost", got)
	}
}
