package bucket

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }

func TestStartsFullAndAllowsExactlyTheBurst(t *testing.T) {
	b := New(10, 5, t0)
	for i := 0; i < 5; i++ {
		if !b.Take(t0) {
			t.Fatalf("request %d of the burst refused", i+1)
		}
	}
	if b.Take(t0) {
		t.Fatal("a 6th request at the same instant was allowed with burst 5")
	}
}

func TestRefillsAtTheRate(t *testing.T) {
	b := New(10, 5, t0) // one token every 100 ms
	for b.Take(t0) {
	}
	if b.Take(at(99 * time.Millisecond)) {
		t.Fatal("allowed before a whole token had refilled")
	}
	if !b.Take(at(100 * time.Millisecond)) {
		t.Fatal("refused after exactly one token's worth of time")
	}
	if b.Take(at(100 * time.Millisecond)) {
		t.Fatal("allowed twice on one token")
	}
	// 1 s later: 10 tokens earned, but the bucket holds only 5.
	if got := b.Tokens(at(1100 * time.Millisecond)); got != 5 {
		t.Fatalf("tokens after a long pause = %v, want the capacity 5", got)
	}
}

func TestFractionalTokensAccumulate(t *testing.T) {
	b := New(1, 1, t0)
	b.Take(t0)
	b.Tokens(at(400 * time.Millisecond))
	b.Tokens(at(700 * time.Millisecond))
	if b.Take(at(999 * time.Millisecond)) {
		t.Fatal("allowed at 0.999 tokens")
	}
	if !b.Take(at(time.Second)) {
		t.Fatal("refused at 1 token built from three partial refills")
	}
}

func TestTimeGoingBackwardsMintsNothing(t *testing.T) {
	b := New(10, 5, t0)
	for b.Take(at(time.Second)) {
	}
	// An earlier timestamp must neither refill nor move the clock back; otherwise the
	// interval up to 1 s would be credited a second time on the next call.
	if b.Take(at(500 * time.Millisecond)) {
		t.Fatal("an earlier timestamp produced a token")
	}
	if got := b.Tokens(at(time.Second + 50*time.Millisecond)); math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("tokens = %v, want 0.5 (only the 50 ms after the latest time seen)", got)
	}
}

func TestWaitIsTheTimeToTheNextToken(t *testing.T) {
	b := New(4, 2, t0) // one token every 250 ms
	if w := b.Wait(t0); w != 0 {
		t.Fatalf("Wait on a full bucket = %v, want 0", w)
	}
	b.Take(t0)
	b.Take(t0)
	w := b.Wait(at(100 * time.Millisecond))
	if w != 150*time.Millisecond {
		t.Fatalf("Wait = %v, want 150ms", w)
	}
	if !b.Take(at(100*time.Millisecond + w)) {
		t.Fatal("Take at now+Wait was refused")
	}
}

func TestSetKeepsDebtAndCapsAtTheBurst(t *testing.T) {
	b := New(10, 5, t0)
	b.Set(-3, t0) // two gateways overspent: 3 tokens of debt
	if w := b.Wait(t0); w != 400*time.Millisecond {
		t.Fatalf("Wait with 3 tokens of debt = %v, want 400ms (4 tokens at 10/s)", w)
	}
	if b.Take(at(399 * time.Millisecond)) {
		t.Fatal("allowed while still in debt")
	}
	if !b.Take(at(400 * time.Millisecond)) {
		t.Fatal("refused once the debt was paid back")
	}
	b.Set(50, at(time.Second))
	if got := b.Tokens(at(time.Second)); got != 5 {
		t.Fatalf("Set above the capacity left %v tokens, want 5", got)
	}
}

func TestSetMayMoveTheClockBack(t *testing.T) {
	// A sync started at 1.0 s and answered at 1.2 s: the shared count is as of 1.0 s, so
	// refill must continue from 1.0 s even though requests at 1.1 s already moved b.last.
	b := New(10, 100, t0)
	b.Take(at(1100 * time.Millisecond))
	b.Set(0, at(time.Second))
	if got := b.Tokens(at(1200 * time.Millisecond)); math.Abs(got-2) > 1e-9 {
		t.Fatalf("tokens = %v, want 2 (refill from 1.0 s to 1.2 s)", got)
	}
}

// reference is the textbook definition, written independently and as plainly as possible: the
// count at any moment is what was there plus rate × elapsed time, capped, in exact integers.
type reference struct {
	rate, burst int64 // tokens per second, capacity
	nano        int64 // tokens × 1e9
	last        int64 // ns
}

func (r *reference) take(now int64) bool {
	if now > r.last {
		r.nano = min(r.burst*1e9, r.nano+r.rate*(now-r.last))
		r.last = now
	}
	if r.nano >= 1e9 {
		r.nano -= 1e9
		return true
	}
	return false
}

func TestMatchesAnExactReference(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		rate := int64(1 + rng.IntN(1000))
		burst := int64(1 + rng.IntN(100))
		b := New(float64(rate), float64(burst), t0)
		ref := &reference{rate: rate, burst: burst, nano: burst * 1e9}
		mean := float64(time.Second) / float64(rate) / 2 // offered load about 2× the rate
		var now int64
		for i := 0; i < 5000; i++ {
			now += int64(rng.ExpFloat64() * mean)
			if rng.IntN(20) == 0 {
				now += int64(rng.IntN(int(time.Second))) // an occasional quiet spell
			}
			want := ref.take(now)
			if got := b.Take(at(time.Duration(now))); got != want {
				t.Fatalf("seed %d, request %d at %d ns: bucket said %v, exact reference %v",
					seed, i, now, got, want)
			}
		}
	}
}
