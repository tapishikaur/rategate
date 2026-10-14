package store

import (
	"context"
	"sync"
	"time"

	"github.com/tapishikaur/rategate/internal/limiter"
)

// Memory is the shared store inside one process, with the same arithmetic as Postgres (see
// schema.sql and syncSQL). Tests and the simulator run several limiters against one Memory
// with a fake clock, to check the multi-gateway behaviour exactly and quickly.
type Memory struct {
	Now func() time.Time // the "database clock" (default time.Now)

	mu      sync.Mutex
	buckets map[string]*memBucket
}

type memBucket struct {
	tokens, taken float64
	updated       time.Time
}

// Taken is how many tokens all gateways have taken from key's bucket, as Postgres's column.
func (m *Memory) Taken(key string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b := m.buckets[key]; b != nil {
		return b.taken
	}
	return 0
}

func (m *Memory) Sync(_ context.Context, usage []limiter.Usage) (map[string]float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	shared := make(map[string]float64, len(usage))
	for _, u := range usage {
		b := m.bucket(u.Key, u.Limit, now)
		b.tokens = refill(b, u.Limit, now) - u.Used
		b.taken += u.Used
		b.updated = later(b.updated, now)
		shared[u.Key] = b.tokens
	}
	return shared, nil
}

func (m *Memory) Take(_ context.Context, key string, limit limiter.Limit) (bool, float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	b := m.bucket(key, limit, now)
	b.tokens = refill(b, limit, now)
	b.updated = later(b.updated, now)
	if b.tokens < 1 {
		return false, b.tokens, nil
	}
	b.tokens--
	b.taken++
	return true, b.tokens, nil
}

// bucket returns key's bucket, creating it full (as the INSERT branch of the SQL does).
func (m *Memory) bucket(key string, limit limiter.Limit, now time.Time) *memBucket {
	if m.buckets == nil {
		m.buckets = make(map[string]*memBucket)
	}
	b := m.buckets[key]
	if b == nil {
		b = &memBucket{tokens: limit.Burst, updated: now}
		m.buckets[key] = b
	}
	return b
}

func (m *Memory) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// refill is rategate_refill in schema.sql.
func refill(b *memBucket, limit limiter.Limit, now time.Time) float64 {
	elapsed := max(0, now.Sub(b.updated).Seconds())
	return min(limit.Burst, b.tokens+limit.Rate*elapsed)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
