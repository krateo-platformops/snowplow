package main

// The #1059 reviewer's adversarial arm, adopted as written: an identity carried by a URL
// path, a query, request headers, a pprof label and a goroutine argument
// reaches no slowcall.request line, no slowcall.goroutines snapshot and no
// /debug/vars ring. Production composition (newServerHandler), the PROCESS
// monitor (3 s bound, real watchdog, real goroutine profile), a real listener.

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

type revBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *revBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *revBuf) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

//go:noinline
func reviewParkedAs(who string, release <-chan struct{}) int { <-release; return len(who) }

func TestReview1059_NoIdentityFromPathQueryHeaderLabelOrArgument(t *testing.T) {
	const who = "rv1059-zed-identity" // synthetic; never a real user
	buf := &revBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	release := make(chan struct{})
	var started sync.WaitGroup
	var sawPath, sawQuery, sawHeader bool
	var mu sync.Mutex
	h := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sawPath = sawPath || strings.Contains(r.URL.Path, who)
		sawQuery = sawQuery || strings.Contains(r.URL.RawQuery, who)
		sawHeader = sawHeader || strings.Contains(r.Header.Get("X-Remote-User"), who)
		mu.Unlock()
		pprof.Do(r.Context(), pprof.Labels("workclass", "customer", "user", who, "secret", who+"-clientconfig"), func(ctx context.Context) {
			w1 := slowcall.StartWait(ctx, slowcall.PhaseFlightWait, "content_flight", "users/"+who+"/key", "g/v1, Resource=r")
			started.Done()
			reviewParkedAs(who, release)
			w1.End()
		})
		w.WriteHeader(200)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /call", h)
	mux.HandleFunc("GET /rv/{who}", h)
	srv := httptest.NewServer(newServerHandler(mux))
	defer srv.Close()

	var done sync.WaitGroup
	for _, u := range []string{"/call?user=" + who + "&name=" + who + "-clientconfig", "/rv/" + who} {
		started.Add(1)
		done.Add(1)
		go func(u string) {
			defer done.Done()
			req, _ := http.NewRequest("GET", srv.URL+u, nil)
			req.Header.Set("X-Remote-User", who)
			req.Header.Set("Authorization", "Bearer "+who)
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}(u)
	}
	started.Wait()
	raw := func() string { var b bytes.Buffer; _ = pprof.Lookup("goroutine").WriteTo(&b, 1); return b.String() }()
	time.Sleep(slowcall.Bound + 1500*time.Millisecond) // the real watchdog fires (tick = Bound/4)
	close(release)
	done.Wait()

	// Positive controls: the identity was really in every carrier.
	if !sawPath || !sawQuery || !sawHeader {
		t.Fatalf("PRECONDITION: path=%v query=%v header=%v", sawPath, sawQuery, sawHeader)
	}
	if !strings.Contains(raw, who) {
		t.Fatalf("PRECONDITION: the raw profile does not carry the label")
	}
	out := buf.String()
	var nReq, nSnap int
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		switch m["msg"] {
		case "slowcall.request":
			nReq++
		case "slowcall.goroutines":
			nSnap++
			if s, _ := m["stacks"].(string); !strings.Contains(s, "reviewParkedAs") {
				t.Errorf("PRECONDITION: the snapshot does not show the parked frame")
			}
		}
	}
	if nReq != 2 || nSnap != 1 {
		t.Fatalf("PRECONDITION: %d records, %d snapshots; want 2 and 1\n%s", nReq, nSnap, out)
	}
	if strings.Contains(out, who) {
		t.Fatalf("REDACTION RED: identity in the log")
	}
	for _, k := range []string{"snowplow_slow_requests_recent", "snowplow_slow_request_snapshots_recent", "snowplow_slow_requests_total", "snowplow_slow_request_phase_ms"} {
		v := expvar.Get(k)
		if v == nil {
			t.Fatalf("PRECONDITION: expvar %s missing", k)
		}
		if strings.Contains(v.String(), who) {
			t.Fatalf("REDACTION RED: identity in expvar %s", k)
		}
	}
	if !strings.Contains(expvar.Get("snowplow_slow_requests_total").String(), "GET /rv/{who}") {
		t.Fatalf("PRECONDITION: the wildcard route was not counted by its pattern: %s", expvar.Get("snowplow_slow_requests_total").String())
	}
	t.Logf("records=%d snapshots=%d; log bytes=%d", nReq, nSnap, len(out))
}
