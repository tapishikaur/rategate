package backend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func call(s *Server, method, path, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestCountsPerKeyAndResets(t *testing.T) {
	s := &Server{}
	for _, key := range []string{"a", "a", "b"} {
		if rec := call(s, "GET", "/anything", key); rec.Code != 200 || rec.Body.String() != "ok\n" {
			t.Fatalf("got %d %q", rec.Code, rec.Body.String())
		}
	}
	var st Stats
	if err := json.Unmarshal(call(s, "GET", "/stats", "").Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.Total != 3 || st.Keys["a"] != 2 || st.Keys["b"] != 1 {
		t.Fatalf("stats %+v, want total 3, a 2, b 1 (the /stats call itself isn't counted)", st)
	}
	if rec := call(s, "GET", "/stats/reset", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /stats/reset: %d, want 405", rec.Code)
	}
	call(s, "POST", "/stats/reset", "")
	if st := s.Stats(); st.Total != 0 || st.Keys["a"] != 0 {
		t.Fatalf("after reset: %+v", st)
	}
}
