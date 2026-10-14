// Package bucket is the token bucket: the counting rule behind every rate limit in RateGate.
//
// A bucket holds at most Burst tokens and refills continuously at Rate tokens per second. Each
// request takes one token; with no whole token left, the request is refused. So a client can
// send Burst requests at once after a quiet spell, and Rate per second on average.
//
// The caller passes the time to every method instead of the bucket reading a clock, so tests
// can drive it with exact, made-up times. A Bucket is not safe for concurrent use; the limiter
// guards each one with its own mutex.
package bucket

import (
	"math"
	"time"
)

// nano is one billionth of a token. Counts are kept as whole nano-tokens because float64
// sums drift: refills of 0.4 + 0.3 + 0.299 + 0.001 tokens add up to just under 1, and a
// request arriving exactly on time would be refused. A rate of R tokens per second is R
// nano-tokens per nanosecond, so for whole-number rates every count is exact.
const nano = 1e9

type Bucket struct {
	rate   float64   // tokens per second = nano-tokens per nanosecond
	burst  int64     // capacity, in nano-tokens
	tokens int64     // in nano-tokens; negative after Set means debt (see Set)
	last   time.Time // the time tokens was last brought up to date
}

// New returns a full bucket. rate must be positive and burst at least 1 (config validates both).
func New(rate, burst float64, now time.Time) Bucket {
	b := Bucket{rate: rate, burst: toNano(burst), last: now}
	b.tokens = b.burst
	return b
}

func (b *Bucket) Rate() float64  { return b.rate }
func (b *Bucket) Burst() float64 { return float64(b.burst) / nano }

// advance adds the tokens earned since b.last, up to the capacity. If now is earlier than
// b.last (two goroutines read the clock in one order and lock in the other), nothing changes:
// that time was already credited, and crediting it again would mint tokens.
func (b *Bucket) advance(now time.Time) {
	if !now.After(b.last) {
		return
	}
	earned := b.rate * float64(now.Sub(b.last)) // nano-tokens
	if room := b.burst - b.tokens; earned >= float64(room) {
		b.tokens = b.burst
	} else {
		b.tokens += int64(earned) // rounds down: never mints a fraction it didn't earn
	}
	b.last = now
}

// Take takes one token if a whole one is available.
func (b *Bucket) Take(now time.Time) bool {
	b.advance(now)
	if b.tokens >= nano {
		b.tokens -= nano
		return true
	}
	return false
}

// Tokens is the current count, possibly fractional or negative.
func (b *Bucket) Tokens(now time.Time) float64 {
	b.advance(now)
	return float64(b.tokens) / nano
}

// Wait is how long until a whole token is available: 0 if one is now. Rounded up to the next
// nanosecond, so a Take at now+Wait succeeds.
func (b *Bucket) Wait(now time.Time) time.Duration {
	b.advance(now)
	if b.tokens >= nano {
		return 0
	}
	return time.Duration(math.Ceil(float64(nano-b.tokens) / b.rate))
}

// Set replaces the count as of time at. The limiter calls it after a sync, with the shared
// count from the database minus what this gateway took while the sync was running. Unlike
// advance, at may be earlier than the last update: the new count replaces the old state, and
// refill continues from at. A count above the capacity is cut to the capacity; a negative one
// (debt) is kept and paid back by refill.
func (b *Bucket) Set(tokens float64, at time.Time) {
	b.tokens = min(b.burst, toNano(tokens))
	b.last = at
}

func toNano(tokens float64) int64 { return int64(math.Round(tokens * nano)) }
