package handlers

// call_slowcall_1054_test.go — #1054: a /call passthrough (no resolver) is one
// dial of the passthrough branch, every attempt of credhttp.Do's retry client
// recorded on it. Through Call() and the production slow-call middleware.

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

func TestS1054_CallPassthroughIsOneDialWithEveryAttempt(t *testing.T) {
	prev := xenv.TestMode()
	xenv.SetTestMode(true) // plumbing's UserConfig keeps the injected ServerURL
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	t.Setenv("CLIENT_MAX_RETRIES", "1")
	t.Setenv("CLIENT_BASE_BACKOFF", "50ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "50ms")
	var hits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"cm"}}`))
	}))
	defer origin.Close()

	const bound = 20 * time.Millisecond
	var buf bytes.Buffer
	mon := slowcall.NewMonitorForTest(bound, slog.New(slog.NewJSONHandler(&buf, nil)))
	h := mon.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		Call().ServeHTTP(w, withCaller443(r, endpoints.Endpoint{ServerURL: origin.URL, Token: "s1054-token"}))
		time.Sleep(2 * bound)
	}))
	req := httptest.NewRequest(http.MethodGet, "/call?"+cmQuery443, nil)
	req.Pattern = "GET /call"
	h.ServeHTTP(httptest.NewRecorder(), req)
	if got := hits.Load(); got != 2 {
		t.Fatalf("PRECONDITION: %d attempts reached the origin, want 2 (one 503, one retry)", got)
	}
	var rec map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "slowcall.request" {
			rec = m
		}
	}
	if rec == nil {
		t.Fatalf("PRECONDITION: no record")
	}
	for k, v := range map[string]any{"dial_branch": "passthrough", "dial_verb": "GET", "dial_attempts": float64(2),
		"dial_http_5xx": float64(1), "dial_status": float64(200), "stage_dial_n": float64(1)} {
		if rec[k] != v {
			t.Errorf("PASSTHROUGH RED: %s = %v, want %v", k, rec[k], v)
		}
	}
	if s := buf.String(); strings.Contains(s, "s1054-token") || strings.Contains(s, "127.0.0.1") {
		t.Errorf("a token or host reached the record")
	}
}
