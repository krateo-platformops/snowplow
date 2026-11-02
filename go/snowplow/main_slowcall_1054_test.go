package main

// main_slowcall_1054_test.go — #1054: the slow-call recorder is in the handler
// composition the pod serves (newServerHandler), at a position where the
// route is already known: every API route's handler sees a record on its
// context; the SSE stream, the probes and /debug do not. Over a real listener.
//
// MUTATIONS (RED, shown in the PR): slowcall.Middleware moved OUTSIDE
// tracing.NewServerHandler → r.Pattern is not set yet, every route is
// untracked and no request is recorded; slowcall.Body dropped from inside the
// compressor → an API handler's writes are not timed.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

func TestS1054_ServedCompositionRecordsAPIRoutesOnly(t *testing.T) {
	var mu sync.Mutex
	seen, timed := map[string]bool{}, map[string]bool{}
	mux := http.NewServeMux()
	for _, p := range []string{"GET /call", "POST /call/read", "GET /list", "GET /export", "GET /refreshes", "GET /health", "GET /readyz", "GET /debug/vars"} {
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen[p] = slowcall.RecordingForTest(r.Context())
			timed[p] = slowcall.BodyTimedForTest(w)
			mu.Unlock()
			ok200(w, r)
		})
	}
	srv := httptest.NewServer(newServerHandler(mux))
	defer srv.Close()
	for _, p := range []string{"GET /call", "POST /call/read", "GET /list", "GET /export", "GET /refreshes", "GET /health", "GET /readyz", "GET /debug/vars"} {
		method, path, _ := strings.Cut(p, " ")
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		resp.Body.Close()
	}
	want := map[string]bool{
		"GET /call": true, "POST /call/read": true, "GET /list": true, "GET /export": true,
		"GET /refreshes": false, "GET /health": false, "GET /readyz": false, "GET /debug/vars": false,
	}
	mu.Lock()
	defer mu.Unlock()
	for p, w := range want {
		got, ok := seen[p]
		if !ok {
			t.Fatalf("PRECONDITION: %s never reached its handler", p)
		}
		if got != w {
			t.Errorf("S1054 RED: %s recorded=%v, want %v", p, got, w)
		}
		if timed[p] != w {
			t.Errorf("S1054 RED: %s body writes timed=%v, want %v", p, timed[p], w)
		}
	}
}
