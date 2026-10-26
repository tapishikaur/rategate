package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tapishikaur/rategate/internal/overadmit"
)

// runOveradmit is "rategate overadmit [flags]": the over-admission experiment (see
// internal/overadmit). It sends straight to each gateway, not through the load balancer, so
// the split of traffic between them is exactly the one asked for.
func runOveradmit(ctx context.Context, args []string) error {
	return overadmitCmd(ctx, args, os.Stdout, os.Stderr)
}

func overadmitCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("overadmit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	targets := fs.String("targets", "", "gateway base URLs, comma-separated (required)")
	weights := fs.String("weights", "", "share of the traffic per target, comma-separated, e.g. 9,1 (default equal)")
	keys := fs.String("keys", "", "API keys to drive, comma-separated; each gets the same load (required)")
	rate := fs.Float64("rate", 0, "offered requests per second, per key, Poisson arrivals; 0 sends the burst only")
	duration := fs.Duration("duration", 0, "how long to send at -rate, e.g. 60s")
	burst := fs.Int("burst", 0, "requests per key sent all at once before the steady phase")
	idle := fs.Duration("idle", 0, "wait this long before sending, so the buckets refill to full")
	backendURL := fs.String("backend", "", "backend base URL: reset its counts first, read them at the end")
	limitRate := fs.Float64("limit-rate", 0, "the keys' configured rate, tokens per second (required)")
	limitBurst := fs.Float64("limit-burst", 0, "the keys' configured burst (required)")
	seed := fs.Uint64("seed", 1, "seed for arrival times and target choices")
	asJSON := fs.Bool("json", false, "print one JSON object instead of a summary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *targets == "" || *keys == "" || *limitRate == 0 || *limitBurst == 0 {
		return errors.New("-targets, -keys, -limit-rate and -limit-burst are required")
	}
	cfg := overadmit.Config{
		Targets: splitList(*targets), Keys: splitList(*keys),
		Rate: *rate, Duration: *duration, Burst: *burst, Idle: *idle,
		Backend: *backendURL, LimitRate: *limitRate, LimitBurst: *limitBurst, Seed: *seed,
	}
	if *weights != "" {
		for _, w := range splitList(*weights) {
			f, err := strconv.ParseFloat(w, 64)
			if err != nil {
				return fmt.Errorf("-weights: %w", err)
			}
			cfg.Weights = append(cfg.Weights, f)
		}
	}
	// Check now, so a usage error doesn't wait for -idle or reset the backend first.
	if err := cfg.Validate(); err != nil {
		return err
	}

	res, err := overadmit.Run(ctx, cfg)
	if err != nil {
		return err
	}
	if res.MaxLag > 50*time.Millisecond {
		fmt.Fprintf(stderr, "warning: some requests went out up to %v late; this machine couldn't keep up with the offered load\n",
			res.MaxLag.Round(time.Millisecond))
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(res)
	}
	printOveradmit(stdout, res, cfg)
	return nil
}

func printOveradmit(w io.Writer, r overadmit.Result, cfg overadmit.Config) {
	var to []string
	for i, t := range r.Targets {
		to = append(to, fmt.Sprintf("%s (weight %g)", t, r.Weights[i]))
	}
	fmt.Fprintf(w, "targets         %s\n", strings.Join(to, ", "))
	load := fmt.Sprintf("a burst of %d only", r.BurstPerKey)
	if r.RatePerKey > 0 {
		load = fmt.Sprintf("a burst of %d, then %g/s for %v", r.BurstPerKey, r.RatePerKey, cfg.Duration)
	}
	fmt.Fprintf(w, "offered         %d key(s), each %s; %.3f s from first send to last\n", r.Keys, load, r.DurationS)
	fmt.Fprintf(w, "responses       %d sent: %d admitted (200), %d limited (429), %d other\n",
		r.Sent, r.Admitted, r.Limited, r.Errors)
	admitted := r.Admitted
	if r.BackendAdmitted >= 0 {
		admitted = r.BackendAdmitted
		fmt.Fprintf(w, "backend         %d requests counted for these keys\n", r.BackendAdmitted)
	}
	fmt.Fprintf(w, "allowed         %.1f (%.1f per key: burst %g + %g/s × %.3f s)\n",
		r.Allowed, r.AllowedPerKey, cfg.LimitBurst, cfg.LimitRate, r.DurationS)
	fmt.Fprintf(w, "over-admission  %+.2f%% (%d admitted vs %.1f allowed)\n", 100*r.OverAdmission, admitted, r.Allowed)
	fmt.Fprintf(w, "worst 1 s       %d admitted for one key (one exact bucket: at most %g)\n", r.Worst1sPerKey, r.ExactWorst1s)
	if r.BurstPerKey > 0 {
		fmt.Fprintf(w, "burst           %.1f admitted per key, %+.1f beyond the burst of %g\n",
			r.BurstAdmittedPerKey, r.BurstOvershootPerKey, cfg.LimitBurst)
	}
	fmt.Fprintf(w, "send lag        at most %v behind schedule\n", r.MaxLag.Round(time.Millisecond))
}

// splitList splits a comma-separated list, dropping blanks.
func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
