package limiter_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/store"
)

// The simulator: several gateways share one Memory store on a fake clock. Requests for one key
// arrive at random (a Poisson process) at a multiple of its limit, the load balancer sends each
// to a gateway according to a split, and each gateway syncs on its own schedule (a random phase
// within the interval). Syncs are instantaneous here; on a real network the interval is
// effectively the interval plus the sync's round trip.
//
// Expected error, from the design: between two syncs, each other gateway can spend its local
// view of the bucket (up to a burst) plus what refills locally in one interval. So with n
// gateways the count is off by at most about (n-1) × (Burst + Rate × Interval), in either
// direction, and debt pays any overspend back.

type scenario struct {
	mode     limiter.Mode
	interval time.Duration
	split    []float64 // share of the traffic each gateway gets
}

func (sc scenario) String() string {
	split := ""
	for i, s := range sc.split {
		if i > 0 {
			split += "/"
		}
		split += fmt.Sprintf("%.0f", 100*s)
	}
	if sc.mode != limiter.Sync {
		return fmt.Sprintf("%s, split %s", sc.mode, split)
	}
	return fmt.Sprintf("sync every %v, split %s", sc.interval, split)
}

func (sc scenario) bound(lim limiter.Limit) float64 {
	return float64(len(sc.split)-1) * (lim.Burst + lim.Rate*sc.interval.Seconds())
}

type load struct {
	limit    limiter.Limit
	overload float64       // offered rate ÷ limit rate
	duration time.Duration // of the overload, starting with full buckets
	burst    int           // requests sent all at once at the start, round robin
}

// allowed is what one perfect bucket admits over the whole run: the burst plus the refill.
func (ld load) allowed() float64 { return ld.limit.Burst + ld.limit.Rate*ld.duration.Seconds() }

type result struct {
	admitted []time.Duration // when each admitted request arrived, from the start
}

func (r result) total() int { return len(r.admitted) }

// worstWindow is the most requests admitted in any window of length w.
func (r result) worstWindow(w time.Duration) int {
	worst, lo := 0, 0
	for hi, at := range r.admitted {
		for at-r.admitted[lo] >= w {
			lo++
		}
		worst = max(worst, hi-lo+1)
	}
	return worst
}

func simulate(sc scenario, ld load, seed uint64) result {
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 99))
	c := newClock()
	t0 := c.Now()
	mem := &store.Memory{Now: c.Now}
	n := len(sc.split)
	gws := make([]*limiter.Limiter, n)
	next := make([]time.Time, n) // each gateway's next sync
	for i := range gws {
		gws[i] = limiter.New(mem, one("k", ld.limit), limiter.Config{Mode: sc.mode, Interval: sc.interval, Now: c.Now})
		next[i] = t0.Add(time.Duration(rng.Float64() * float64(sc.interval)))
	}
	var r result
	send := func(g int, now time.Time) {
		if d, _ := gws[g].Allow(ctx, "k"); d.Allowed {
			r.admitted = append(r.admitted, now.Sub(t0))
		}
	}
	for i := 0; i < ld.burst; i++ {
		send(i%n, t0)
	}
	if ld.overload == 0 {
		return r // a burst only
	}
	end := t0.Add(ld.duration)
	mean := float64(time.Second) / (ld.limit.Rate * ld.overload)
	for now := t0; ; {
		now = now.Add(time.Duration(rng.ExpFloat64() * mean))
		if !now.Before(end) {
			return r
		}
		for sc.mode == limiter.Sync { // every sync due before this request, earliest first
			i := 0
			for j := range next {
				if next[j].Before(next[i]) {
					i = j
				}
			}
			if next[i].After(now) {
				break
			}
			c.Set(next[i])
			gws[i].SyncOnce(ctx) //nolint:errcheck // Memory doesn't fail
			next[i] = next[i].Add(sc.interval)
		}
		c.Set(now)
		g, x := 0, rng.Float64()
		for g < n-1 && x >= sc.split[g] {
			x -= sc.split[g]
			g++
		}
		send(g, now)
	}
}

var (
	limit100  = limiter.Limit{Rate: 100, Burst: 100}
	splits    = [][]float64{{0.5, 0.5}, {0.9, 0.1}, {1.0 / 3, 1.0 / 3, 1.0 / 3}, {0.8, 0.1, 0.1}}
	intervals = []time.Duration{10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
		250 * time.Millisecond, 500 * time.Millisecond, time.Second}
)

func TestSimulatedSteadyOverloadStaysWithinTheBound(t *testing.T) {
	// 3× the limit for 60 s, through 2 or 3 gateways, every split and interval, 5 seeds each.
	ld := load{limit: limit100, overload: 3, duration: 60 * time.Second}
	for _, split := range splits {
		for _, iv := range intervals {
			sc := scenario{mode: limiter.Sync, interval: iv, split: split}
			lo, hi, worst := math.Inf(1), math.Inf(-1), 0
			for seed := uint64(1); seed <= 5; seed++ {
				r := simulate(sc, ld, seed)
				over := float64(r.total()) - ld.allowed()
				lo, hi = min(lo, over), max(hi, over)
				worst = max(worst, r.worstWindow(time.Second))
				if math.Abs(over) > sc.bound(ld.limit) {
					t.Errorf("%v, seed %d: off by %v, beyond the bound %v", sc, seed, over, sc.bound(ld.limit))
				}
			}
			t.Logf("%-32s 60 s: %+5.0f to %+5.0f of %v allowed; worst 1 s: %d (one exact bucket: at most %v)",
				sc, lo, hi, ld.allowed(), worst, ld.limit.Burst+ld.limit.Rate)
		}
	}
}

func TestSimulatedBurstOvershootIsPaidBack(t *testing.T) {
	// After a quiet spell every gateway's local view is a full bucket, so a burst spread over
	// n gateways gets n bursts through: the overshoot is (n-1) × Burst (decision 3: measured
	// and documented, not prevented). The debt it leaves is then paid back, so 10 s later the
	// total is back within the steady-state bound.
	for _, split := range [][]float64{{0.5, 0.5}, {1.0 / 3, 1.0 / 3, 1.0 / 3}} {
		sc := scenario{mode: limiter.Sync, interval: 100 * time.Millisecond, split: split}
		n := len(split)
		burstOnly := simulate(sc, load{limit: limit100, burst: 5 * 100}, 1)
		if got, want := burstOnly.total(), n*100; got != want {
			t.Errorf("%v: a burst of 500 admitted %d, want %d (a full bucket per gateway)", sc, got, want)
		}
		ld := load{limit: limit100, overload: 3, duration: 10 * time.Second, burst: 5 * 100}
		for seed := uint64(1); seed <= 5; seed++ {
			over := float64(simulate(sc, ld, seed).total()) - ld.allowed()
			if over > float64(n-1)*limit100.Rate*sc.interval.Seconds()+1 {
				t.Errorf("%v, seed %d: still %v over 10 s after the burst", sc, seed, over)
			}
		}
	}
}

func TestSimulatedExactModeIsExact(t *testing.T) {
	ld := load{limit: limit100, overload: 3, duration: 60 * time.Second, burst: 500}
	for _, split := range splits {
		sc := scenario{mode: limiter.Exact, split: split}
		got := float64(simulate(sc, ld, 1).total())
		if got > ld.allowed() || got < ld.allowed()-1 {
			t.Errorf("%v: admitted %v, want %v", sc, got, ld.allowed())
		}
	}
}

func TestSimulatedLocalModeMultipliesTheLimit(t *testing.T) {
	// No sharing: each gateway enforces the full limit, so with traffic spread evenly and
	// heavy enough to saturate every gateway, n gateways admit about n times the limit.
	ld := load{limit: limit100, overload: 6, duration: 60 * time.Second}
	for _, split := range [][]float64{{0.5, 0.5}, {1.0 / 3, 1.0 / 3, 1.0 / 3}} {
		sc := scenario{mode: limiter.Local, split: split}
		ratio := float64(simulate(sc, ld, 1).total()) / ld.allowed()
		if want := float64(len(split)); ratio < want*0.97 || ratio > want*1.01 {
			t.Errorf("%v: admitted %.2f× the limit, want about %v×", sc, ratio, want)
		}
	}
}
