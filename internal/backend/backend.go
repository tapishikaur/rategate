// Package backend is the service behind the gateway in the demo and the measurements: it
// answers every request at once with a tiny fixed body, so latency measured through the
// gateway is the gateway's, not the backend's. It also counts requests per API key. Those
// counts are the ground truth for over-admission: what actually got through.
//
//	GET  /stats        the counts, as JSON: {"total": N, "keys": {"demo": n, ...}}
//	POST /stats/reset  set every count to 0
//	anything else      200 "ok", counted under the request's X-API-Key
package backend

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
)

type Server struct {
	total  atomic.Int64
	counts sync.Map // API key → *atomic.Int64
}

type Stats struct {
	Total int64            `json:"total"`
	Keys  map[string]int64 `json:"keys"`
}

var okBody = []byte("ok\n")

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/stats":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Stats()) //nolint:errcheck
		return
	case "/stats/reset":
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		s.Reset()
		return
	}
	key := r.Header.Get("X-API-Key")
	c, ok := s.counts.Load(key)
	if !ok {
		c, _ = s.counts.LoadOrStore(key, new(atomic.Int64))
	}
	c.(*atomic.Int64).Add(1)
	s.total.Add(1)
	w.Header().Set("Content-Type", "text/plain")
	w.Write(okBody) //nolint:errcheck
}

func (s *Server) Stats() Stats {
	st := Stats{Total: s.total.Load(), Keys: map[string]int64{}}
	s.counts.Range(func(k, v any) bool {
		st.Keys[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return st
}

func (s *Server) Reset() {
	s.counts.Range(func(k, v any) bool {
		v.(*atomic.Int64).Store(0)
		return true
	})
	s.total.Store(0)
}
