// Package testdb gives each test its own PostgreSQL database, created on the server named by
// RATEGATE_TEST_DB and dropped when the test ends. Tests that need it are skipped when the
// variable isn't set (scripts/test.sh sets it, starting a throwaway local server if needed).
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// URL returns the URL of a new, empty database that is dropped at the end of the test.
func URL(t testing.TB) string {
	t.Helper()
	server := os.Getenv("RATEGATE_TEST_DB")
	if server == "" {
		t.Skip("RATEGATE_TEST_DB is not set (scripts/test.sh sets it)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, server)
	if err != nil {
		t.Fatalf("connecting to RATEGATE_TEST_DB: %v", err)
	}
	defer admin.Close(context.Background())

	suffix := make([]byte, 6)
	rand.Read(suffix)
	name := "rategate_test_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("creating test database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if conn, err := pgx.Connect(ctx, server); err == nil {
			conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") //nolint:errcheck
			conn.Close(ctx)
		}
	})
	u, err := url.Parse(server)
	if err != nil {
		t.Fatalf("RATEGATE_TEST_DB must be a postgres:// URL: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

// Pool opens a pool on a new test database.
func Pool(t testing.TB, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(URL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
