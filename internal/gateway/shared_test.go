package gateway

import (
	"context"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/tapishikaur/rategate/internal/backend"
	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/metrics"
	"github.com/tapishikaur/rategate/internal/store"
	"github.com/tapishikaur/rategate/internal/testdb"
)

// Two gateways in front of one backend, sharing their buckets through a real PostgreSQL
// database: the whole multi-instance path except the load balancer and Docker.
func TestTwoGatewaysShareOneLimitThroughPostgres(t *testing.T) {
	pg := &store.Postgres{Pool: testdb.Pool(t, 8)}
	if err := pg.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	be := &backend.Server{}
	up := httptest.NewServer(be)
	defer up.Close()
	u, _ := url.Parse(up.URL)

	// A burst of 20 and almost no refill, so the expected count doesn't depend on how fast
	// this machine is.
	limits := map[string]limiter.Limit{"demo": {Rate: 0.001, Burst: 20}}
	ctx, cancel := context.WithCancel(context.Background())
	var gateways []*httptest.Server
	var lims []*limiter.Limiter
	var running sync.WaitGroup
	for i := 0; i < 2; i++ {
		l := limiter.New(pg, limits, limiter.Config{Mode: limiter.Sync, Interval: 20 * time.Millisecond})
		lims = append(lims, l)
		running.Add(1)
		go func() {
			defer running.Done()
			l.Run(ctx)
		}()
		gw := httptest.NewServer(New(u, l, metrics.New("sync")))
		defer gw.Close()
		gateways = append(gateways, gw)
	}
	defer cancel()

	// Phase 1: 15 requests alternating between the gateways. Each starts with a full local
	// view, so all 15 pass.
	for i := 0; i < 15; i++ {
		get(t, gateways[i%2].URL, "demo")
	}
	// Let both sync (the database now knows 15 of 20 are used), then send 40 more.
	waitFor(t, func() bool { return taken(t, pg) == 15 })
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 40; i++ {
		get(t, gateways[i%2].URL, "demo")
	}
	got := be.Stats().Keys["demo"]
	// One bucket would admit exactly 20; two separate limits, 40. Shared, the worst case is 33:
	// both gateways' views include the 5 tokens left after phase 1, so both may spend them
	// (5 + 5), and the gateway whose last sync came first may not yet know about up to 8 of
	// the other's phase-1 requests (+8), until its next sync: 15 + 5 + 5 + 8 = 33.
	if got < 20 || got > 33 {
		t.Fatalf("the backend saw %d requests, want 20 to 33 (one shared burst), not 40", got)
	}
	// After each gateway's final sync, every admitted request is recorded in the shared
	// bucket. Wait for those to finish, then sync once more in case one failed (a failed sync
	// keeps its usage for the next one).
	cancel()
	running.Wait()
	for _, l := range lims {
		l.SyncOnce(context.Background()) //nolint:errcheck
	}
	if n := taken(t, pg); n != float64(got) {
		t.Fatalf("the database recorded %v taken, the backend saw %d", n, got)
	}
}

func taken(t *testing.T, pg *store.Postgres) float64 {
	t.Helper()
	var n float64
	err := pg.Pool.QueryRow(context.Background(), `SELECT COALESCE(sum(taken), 0) FROM buckets`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
