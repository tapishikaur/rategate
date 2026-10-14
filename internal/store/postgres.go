// Package store holds the shared token buckets: Postgres for real, Memory for tests and the
// simulator. Both implement limiter.Store with the same arithmetic.
package store

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tapishikaur/rategate/internal/limiter"
)

//go:embed schema.sql
var schema string

type Postgres struct {
	Pool *pgxpool.Pool
}

// Open connects to PostgreSQL, retrying for up to wait (the database may still be starting).
func Open(ctx context.Context, url string, maxConns int32, wait time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = min(2, maxConns) // keep connections open: a new one takes several round trips
	deadline := time.Now().Add(wait)
	for {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connecting to PostgreSQL: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Migrate creates the table and function if they don't exist. The advisory lock makes
// gateways that start at the same moment take turns, instead of racing to create the table.
func (p *Postgres) Migrate(ctx context.Context) error {
	return pgx.BeginFunc(ctx, p.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(820820)`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
}

// syncSQL applies a whole batch in one statement, so a sync is one round trip however many
// keys it carries. For each key:
//   - a new key's row starts full, minus what was used;
//   - an existing row is refilled, then the usage is subtracted. The result may be negative:
//     debt, when gateways together spent more than there was. It is never forgiven, so every
//     overspend is paid back by refill, and over time the limit holds.
//
// Each row's update is atomic: if two gateways sync the same key at once, the second waits for
// the first's row lock and then applies its usage to the updated row, so no usage is lost.
// ORDER BY key makes every sync lock rows in the same order, so two syncs can't deadlock.
// The inserted row's "taken" column carries the usage into the update as EXCLUDED.taken.
const syncSQL = `
INSERT INTO buckets AS b (key, rate, burst, tokens, taken, updated_at)
SELECT u.key, u.rate, u.burst, u.burst - u.used, u.used, now()
  FROM unnest($1::text[], $2::float8[], $3::float8[], $4::float8[]) AS u(key, used, rate, burst)
 ORDER BY u.key
ON CONFLICT (key) DO UPDATE SET
  rate       = EXCLUDED.rate,
  burst      = EXCLUDED.burst,
  tokens     = rategate_refill(b.tokens, b.updated_at, EXCLUDED.rate, EXCLUDED.burst) - EXCLUDED.taken,
  taken      = b.taken + EXCLUDED.taken,
  updated_at = GREATEST(b.updated_at, now())
RETURNING key, tokens`

func (p *Postgres) Sync(ctx context.Context, usage []limiter.Usage) (map[string]float64, error) {
	keys := make([]string, len(usage))
	used := make([]float64, len(usage))
	rates := make([]float64, len(usage))
	bursts := make([]float64, len(usage))
	for i, u := range usage {
		keys[i], used[i], rates[i], bursts[i] = u.Key, u.Used, u.Limit.Rate, u.Limit.Burst
	}
	rows, err := p.Pool.Query(ctx, syncSQL, keys, used, rates, bursts)
	if err != nil {
		return nil, err
	}
	shared := make(map[string]float64, len(usage))
	var key string
	var tokens float64
	_, err = pgx.ForEachRow(rows, []any{&key, &tokens}, func() error {
		shared[key] = tokens
		return nil
	})
	if err != nil {
		return nil, err
	}
	return shared, nil
}

// takeSQL is exact mode: take one token only if a whole one is there, in one atomic update.
// Whether it was taken comes from comparing the row before and after the update (RETURNING
// old/new, new in PostgreSQL 18): "taken" goes up by 1 exactly when a token was taken.
const takeSQL = `
INSERT INTO buckets AS b (key, rate, burst, tokens, taken, updated_at)
VALUES ($1, $2::float8, $3::float8, $3::float8 - 1, 1, now())
ON CONFLICT (key) DO UPDATE SET
  rate       = EXCLUDED.rate,
  burst      = EXCLUDED.burst,
  tokens     = rategate_refill(b.tokens, b.updated_at, EXCLUDED.rate, EXCLUDED.burst)
               - CASE WHEN rategate_refill(b.tokens, b.updated_at, EXCLUDED.rate, EXCLUDED.burst) >= 1
                      THEN 1 ELSE 0 END,
  taken      = b.taken
               + CASE WHEN rategate_refill(b.tokens, b.updated_at, EXCLUDED.rate, EXCLUDED.burst) >= 1
                      THEN 1 ELSE 0 END,
  updated_at = GREATEST(b.updated_at, now())
RETURNING old.taken IS NULL OR new.taken > old.taken, new.tokens`

func (p *Postgres) Take(ctx context.Context, key string, limit limiter.Limit) (bool, float64, error) {
	var allowed bool
	var tokens float64
	err := p.Pool.QueryRow(ctx, takeSQL, key, limit.Rate, limit.Burst).Scan(&allowed, &tokens)
	return allowed, tokens, err
}
