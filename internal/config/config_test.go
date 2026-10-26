package config

import (
	"strings"
	"testing"

	"github.com/tapishikaur/rategate/internal/limiter"
)

func TestParseListedAndGeneratedKeys(t *testing.T) {
	limits, err := Parse([]byte(`{
		"keys": [{"key": "demo", "rate": 50, "burst": 25}],
		"generated": [{"prefix": "load-", "count": 3, "rate": 1000, "burst": 1000}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]limiter.Limit{
		"demo":   {Rate: 50, Burst: 25},
		"load-0": {Rate: 1000, Burst: 1000},
		"load-1": {Rate: 1000, Burst: 1000},
		"load-2": {Rate: 1000, Burst: 1000},
	}
	if len(limits) != len(want) {
		t.Fatalf("got %d keys, want %d", len(limits), len(want))
	}
	for k, w := range want {
		if limits[k] != w {
			t.Errorf("%s: got %+v, want %+v", k, limits[k], w)
		}
	}
}

func TestParseRejectsBadConfigs(t *testing.T) {
	for _, tc := range []struct{ config, wantErr string }{
		{`{"keys": [{"key": "a", "rate": 1, "burst": 1}, {"key": "a", "rate": 2, "burst": 2}]}`, "twice"},
		{`{"keys": [{"key": "a0", "rate": 1, "burst": 1}], "generated": [{"prefix": "a", "count": 2, "rate": 1, "burst": 1}]}`, "twice"},
		{`{"keys": [{"key": "a", "rate": 0, "burst": 1}]}`, "rate"},
		{`{"keys": [{"key": "a", "rate": 1, "burst": 0.5}]}`, "burst"},
		{`{"keys": [{"key": "", "rate": 1, "burst": 1}]}`, "empty"},
		{`{"keys": [{"key": "a", "rate": 1, "brust": 1}]}`, "unknown field"},
		{`{"generated": [{"prefix": "a", "count": 0, "rate": 1, "burst": 1}]}`, "count"},
		{`{}`, "no keys"},
	} {
		_, err := Parse([]byte(tc.config))
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: error %v, want one mentioning %q", tc.config, err, tc.wantErr)
		}
	}
}
