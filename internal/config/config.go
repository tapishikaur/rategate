// Package config reads the API keys and their rate limits from a JSON file that every gateway
// is given. Example:
//
//	{
//	  "keys":      [{"key": "demo", "rate": 50, "burst": 50}],
//	  "generated": [{"prefix": "load-", "count": 100, "rate": 100000, "burst": 100000}]
//	}
//
// "generated" makes count keys named prefix0, prefix1, …, so a load test can use many keys
// without listing each one. The database holds only bucket state, never the limits: a gateway
// sends a key's limit with every sync, so all gateways must be given the same file.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/tapishikaur/rategate/internal/limiter"
)

type file struct {
	Keys      []key   `json:"keys"`
	Generated []group `json:"generated"`
}

type key struct {
	Key   string  `json:"key"`
	Rate  float64 `json:"rate"`
	Burst float64 `json:"burst"`
}

type group struct {
	Prefix string  `json:"prefix"`
	Count  int     `json:"count"`
	Rate   float64 `json:"rate"`
	Burst  float64 `json:"burst"`
}

// Load reads and checks a config file.
func Load(path string) (map[string]limiter.Limit, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	limits, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return limits, nil
}

// Parse checks a config and returns each key's limit.
func Parse(data []byte) (map[string]limiter.Limit, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // a misspelt field must be an error, not a silently missing limit
	var f file
	if err := dec.Decode(&f); err != nil {
		return nil, err
	}
	limits := map[string]limiter.Limit{}
	add := func(name string, rate, burst float64) error {
		switch {
		case name == "":
			return fmt.Errorf("a key is empty")
		case !(rate > 0): // also rejects NaN
			return fmt.Errorf("key %q: rate must be positive", name)
		case !(burst >= 1):
			return fmt.Errorf("key %q: burst must be at least 1, or no request could ever pass", name)
		}
		if _, dup := limits[name]; dup {
			return fmt.Errorf("key %q appears twice", name)
		}
		limits[name] = limiter.Limit{Rate: rate, Burst: burst}
		return nil
	}
	for _, k := range f.Keys {
		if err := add(k.Key, k.Rate, k.Burst); err != nil {
			return nil, err
		}
	}
	for _, g := range f.Generated {
		if g.Count < 1 {
			return nil, fmt.Errorf("generated keys %q: count must be at least 1", g.Prefix)
		}
		for i := 0; i < g.Count; i++ {
			if err := add(fmt.Sprintf("%s%d", g.Prefix, i), g.Rate, g.Burst); err != nil {
				return nil, err
			}
		}
	}
	if len(limits) == 0 {
		return nil, fmt.Errorf("no keys")
	}
	return limits, nil
}
