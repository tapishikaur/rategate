package store_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/store"
	"github.com/tapishikaur/rategate/internal/testdb"
)

// A rate so small that refill during a test is invisible, for tests about the arithmetic.
const still = 1e-9

type testStore interface {
	limiter.Store
	taken(t *testing.T, key string) float64
}

type memStore struct{ *store.Memory }

func (m memStore) taken(_ *testing.T, key string) float64 { return m.Taken(key) }

type pgStore struct{ *store.Postgres }

func (p pgStore) taken(t *testing.T, key string) float64 {
	var n float64
	if err := p.Pool.QueryRow(context.Background(), `SELECT taken FROM buckets WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func newPostgres(t *testing.T) *store.Postgres {
	pg := &store.Postgres{Pool: testdb.Pool(t, 16)}
	if err := pg.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return pg
}

// both runs a test against Memory and against Postgres: they must behave the same.
func both(t *testing.T, test func(t *testing.T, s testStore)) {
	t.Run("memory", func(t *testing.T) { test(t, memStore{&store.Memory{}}) })
	t.Run("postgres", func(t *testing.T) { test(t, pgStore{newPostgres(t)}) })
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func syncOne(t *testing.T, s limiter.Store, key string, used float64, lim limiter.Limit) float64 {
	t.Helper()
	shared, err := s.Sync(context.Background(), []limiter.Usage{{Key: key, Used: used, Limit: lim}})
	if err != nil {
		t.Fatal(err)
	}
	return shared[key]
}

func TestNewKeyStartsFull(t *testing.T) {
	both(t, func(t *testing.T, s testStore) {
		if got := syncOne(t, s, "k", 3, limiter.Limit{Rate: still, Burst: 10}); !near(got, 7) {
			t.Fatalf("a new key's first sync of 3 left %v, want 7 of 10", got)
		}
	})
}

func TestDebtIsKept(t *testing.T) {
	both(t, func(t *testing.T, s testStore) {
		lim := limiter.Limit{Rate: still, Burst: 10}
		for _, step := range []struct{ used, want float64 }{{4, 6}, {12, -6}, {30, -36}} {
			if got := syncOne(t, s, "k", step.used, lim); !near(got, step.want) {
				t.Fatalf("after using %v more: %v, want %v", step.used, got, step.want)
			}
		}
		if got := s.taken(t, "k"); got != 46 {
			t.Fatalf("taken = %v, want 46", got)
		}
	})
}

func TestDebtIsPaidBackByRefill(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := &store.Memory{Now: func() time.Time { return now }}
	lim := limiter.Limit{Rate: 10, Burst: 10}
	syncOne(t, m, "k", 25, lim) // 15 in debt
	now = now.Add(2 * time.Second)
	if got := syncOne(t, m, "k", 0, lim); !near(got, 5) {
		t.Fatalf("15 in debt, then 2 s at 10/s: %v, want 5", got)
	}
}

func TestConcurrentSyncsLoseNothing(t *testing.T) {
	both(t, func(t *testing.T, s testStore) {
		// 8 "gateways" sync the same 3 keys at once, 25 times each, 1 token per key per sync.
		// Each update must apply on top of the others: no usage may be lost or doubled.
		lim := limiter.Limit{Rate: still, Burst: 1000}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					batch := []limiter.Usage{{Key: "a", Used: 1, Limit: lim}, {Key: "b", Used: 1, Limit: lim}, {Key: "c", Used: 1, Limit: lim}}
					if _, err := s.Sync(context.Background(), batch); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		for _, key := range []string{"a", "b", "c"} {
			if got := s.taken(t, key); got != 200 {
				t.Errorf("key %s: taken = %v, want 200", key, got)
			}
			if got := syncOne(t, s, key, 0, lim); !near(got, 800) {
				t.Errorf("key %s: tokens = %v, want 800", key, got)
			}
		}
	})
}

func TestTakeNeverOverAdmits(t *testing.T) {
	both(t, func(t *testing.T, s testStore) {
		// 100 concurrent requests for a bucket of 50 with no refill: exactly 50 get through.
		lim := limiter.Limit{Rate: still, Burst: 50}
		var mu sync.Mutex
		allowed := 0
		var wg sync.WaitGroup
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					ok, _, err := s.Take(context.Background(), "k", lim)
					if err != nil {
						t.Error(err)
						return
					}
					if ok {
						mu.Lock()
						allowed++
						mu.Unlock()
					}
				}
			}()
		}
		wg.Wait()
		if allowed != 50 {
			t.Fatalf("allowed %d of 100 with burst 50", allowed)
		}
		if got := s.taken(t, "k"); got != 50 {
			t.Fatalf("taken = %v, want 50", got)
		}
	})
}

func TestTakeRefusesWhileInDebt(t *testing.T) {
	both(t, func(t *testing.T, s testStore) {
		lim := limiter.Limit{Rate: still, Burst: 10}
		syncOne(t, s, "k", 15, lim) // two gateways overspent: 5 tokens of debt
		ok, tokens, err := s.Take(context.Background(), "k", lim)
		if err != nil {
			t.Fatal(err)
		}
		if ok || !near(tokens, -5) {
			t.Fatalf("Take in debt: allowed=%v tokens=%v, want refused at -5", ok, tokens)
		}
	})
}

func TestMemoryRefillFollowsItsClock(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := &store.Memory{Now: func() time.Time { return now }}
	lim := limiter.Limit{Rate: 10, Burst: 20}
	syncOne(t, m, "k", 20, lim)
	now = now.Add(500 * time.Millisecond)
	if got := syncOne(t, m, "k", 0, lim); !near(got, 5) {
		t.Fatalf("after 0.5 s at 10/s: %v, want 5", got)
	}
	now = now.Add(-300 * time.Millisecond) // a sync whose clock reading is older than the row
	if got := syncOne(t, m, "k", 1, lim); !near(got, 4) {
		t.Fatalf("an older clock reading changed the refill: %v, want 4", got)
	}
	now = now.Add(400 * time.Millisecond) // 0.1 s after the latest update, not 0.4 s
	if got := syncOne(t, m, "k", 0, lim); !near(got, 5) {
		t.Fatalf("refill after a backwards reading: %v, want 5", got)
	}
}

func TestPostgresRefillsByTheDatabaseClock(t *testing.T) {
	s := newPostgres(t)
	lim := limiter.Limit{Rate: 100, Burst: 100}
	syncOne(t, s, "k", 100, lim)
	time.Sleep(200 * time.Millisecond)
	// About 20 tokens; more if this machine is slow between the two statements, never fewer.
	if got := syncOne(t, s, "k", 0, lim); got < 20 || got > 60 {
		t.Fatalf("after 200 ms at 100/s: %v tokens, want about 20", got)
	}
}

func TestPostgresMigrateIsSafeToRunConcurrently(t *testing.T) {
	pool := testdb.Pool(t, 8)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- (&store.Postgres{Pool: pool}).Migrate(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostgresSyncsInOppositeOrderDontDeadlock(t *testing.T) {
	// Two gateways syncing the same keys must lock them in the same order. Store.Sync
	// requires sorted input; syncSQL sorts again with ORDER BY, so even unsorted batches
	// running concurrently finish (a deadlock would surface as an error after 1 s).
	s := newPostgres(t)
	lim := limiter.Limit{Rate: still, Burst: 1e6}
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				batch := make([]limiter.Usage, len(keys))
				for j := range keys {
					k := keys[j]
					if g == 1 {
						k = keys[len(keys)-1-j] // reversed
					}
					batch[j] = limiter.Usage{Key: k, Used: 1, Limit: lim}
				}
				if _, err := s.Sync(context.Background(), batch); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
