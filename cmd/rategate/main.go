// rategate: one binary for every part of the demo, chosen by the first argument.
//
//	rategate gateway          the rate-limiting gateway (configured by environment variables)
//	rategate backend          the test backend: a fixed tiny response, per-key request counts
//	rategate overadmit [...]  the over-admission experiment (see overadmit.go)
//	rategate health URL       exit 0 if URL answers 200 (for container health checks)
//
// One binary means one small Docker image; Compose runs it with different commands.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/tapishikaur/rategate/internal/backend"
	"github.com/tapishikaur/rategate/internal/config"
	"github.com/tapishikaur/rategate/internal/gateway"
	"github.com/tapishikaur/rategate/internal/limiter"
	"github.com/tapishikaur/rategate/internal/metrics"
	"github.com/tapishikaur/rategate/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "gateway":
		err = runGateway(ctx)
	case "backend":
		err = runBackend(ctx)
	case "overadmit":
		err = runOveradmit(ctx, os.Args[2:])
	case "health":
		err = runHealth(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "rategate %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: rategate gateway | backend | overadmit [flags] | health URL")
	os.Exit(2)
}

// runGateway serves the proxy on LISTEN_ADDR and /metrics and /healthz on ADMIN_ADDR, a
// separate port, so neither is reachable through the proxy.
func runGateway(ctx context.Context) error {
	limits, err := config.Load(env("CONFIG", "/etc/rategate/keys.json"))
	if err != nil {
		return err
	}
	upstream, err := url.Parse(env("UPSTREAM", ""))
	if err != nil || upstream.Host == "" {
		return fmt.Errorf("UPSTREAM must be the backend's URL, e.g. http://backend:8081")
	}
	mode, err := limiter.ParseMode(env("MODE", "sync"))
	if err != nil {
		return err
	}
	interval, err := time.ParseDuration(env("SYNC_INTERVAL", "100ms"))
	if err != nil {
		return fmt.Errorf("SYNC_INTERVAL: %w", err)
	}

	m := metrics.New(string(mode))
	var st limiter.Store
	if mode != limiter.Local {
		maxConns, err := strconv.Atoi(env("DB_MAX_CONNS", "16"))
		if err != nil {
			return fmt.Errorf("DB_MAX_CONNS: %w", err)
		}
		pool, err := store.Open(ctx, env("DATABASE_URL", ""), int32(maxConns), time.Minute)
		if err != nil {
			return err
		}
		defer pool.Close()
		pg := &store.Postgres{Pool: pool}
		if err := pg.Migrate(ctx); err != nil {
			return fmt.Errorf("creating the buckets table: %w", err)
		}
		st = pg
	}
	lim := limiter.New(st, limits, limiter.Config{Mode: mode, Interval: interval, OnStore: m.Store})

	syncCtx, stopSync := context.WithCancel(context.WithoutCancel(ctx))
	defer stopSync() // also on the error path; calling it twice is harmless
	syncDone := make(chan struct{})
	go func() {
		lim.Run(syncCtx)
		close(syncDone)
	}()

	admin := http.NewServeMux()
	admin.Handle("/metrics", m.Handler())
	admin.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) }) //nolint:errcheck
	proxy := &http.Server{Addr: env("LISTEN_ADDR", ":8080"), Handler: gateway.New(upstream, lim, m),
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 2 * time.Minute}
	adminSrv := &http.Server{Addr: env("ADMIN_ADDR", ":9091"), Handler: admin, ReadHeaderTimeout: 5 * time.Second}

	slog.Info("gateway starting", "listen", proxy.Addr, "admin", adminSrv.Addr, "upstream", upstream.String(),
		"mode", mode, "sync_interval", interval, "keys", len(limits))
	errs := make(chan error, 2)
	go func() { errs <- proxy.ListenAndServe() }()
	go func() { errs <- adminSrv.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err := <-errs:
		return err
	}
	// Shutdown order: stop taking requests, let those in flight finish, then make the final
	// sync, so the usage of the last interval reaches the shared buckets.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = proxy.Shutdown(shutdownCtx)
	stopSync()
	<-syncDone
	adminSrv.Shutdown(shutdownCtx) //nolint:errcheck
	slog.Info("gateway stopped")
	return err
}

func runBackend(ctx context.Context) error {
	srv := &http.Server{Addr: env("LISTEN_ADDR", ":8081"), Handler: &backend.Server{},
		ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 2 * time.Minute}
	slog.Info("backend starting", "listen", srv.Addr)
	errs := make(chan error, 1)
	go func() { errs <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errs:
		return err
	}
}

func runHealth(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: rategate health URL")
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(args[0])
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", args[0], resp.StatusCode)
	}
	return nil
}

// env returns an environment variable, or fallback if it's unset or empty.
func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
