package api

// apistage_fill_flight_slowcall_1054_test.go — #1054: a request blocked on
// ANOTHER reader's content fill (the #710 single-flight) past the slow-call
// bound emits exactly one record, naming the flight_wait phase, the content
// key's digest, the GVR and why the wait was released. Through the production
// reader apistageContentServe on a real informer (the #710 harness), behind
// the production slow-call middleware (a test monitor with a short bound).
//
//   - F1054-SF1: the flight completes → one record, wait_outcome completed.
//     The leader is background work (no request), so the joiner's is the
//     only record; a later fast read of the same cell adds none.
//   - F1054-SF2: the flight's leader falls through (no servable result) →
//     wait_outcome leader_failed.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

const slow1054Bound = 150 * time.Millisecond

type slow1054Log struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *slow1054Log) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *slow1054Log) records(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(l.b.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if m["msg"] == "slowcall.request" {
			out = append(out, m)
		}
	}
	return out
}

// slow1054Serve runs one tracked GET /call whose handler is a customer read of
// the fixture LIST through apistageContentServe, as r1User.
func slow1054Serve(mon *slowcall.MonitorForTest, store *cache.ResolvedCacheStore) {
	h := mon.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := xcontext.BuildContext(r.Context(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: r1User}))
		apistageContentServe(ctx, store, fill710ListCall(), false)
	}))
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Pattern = "GET /call"
	h.ServeHTTP(httptest.NewRecorder(), req)
}

// slow1054JoinBehindHeldLeader holds a BACKGROUND leader's fill in its
// dispatch, sends one tracked request that joins its flight, keeps it waiting
// past the bound, then lets the leader finish with finish (the real dispatch,
// or a fall-through).
func slow1054JoinBehindHeldLeader(t *testing.T, finish func(context.Context, httpcall.RequestOptions) ([]byte, bool, context.Context)) (*slowcall.MonitorForTest, *slow1054Log, *cache.ResolvedCacheStore) {
	t.Helper()
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	fill710MakeKnownStale(t, dyn, store)

	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	var arrived atomic.Int64
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool, context.Context) {
		arrived.Add(1)
		<-gate
		if finish != nil {
			return finish(ctx, call)
		}
		return orig(ctx, call)
	}

	logs := &slow1054Log{}
	mon := slowcall.NewMonitorForTest(slow1054Bound, slog.New(slog.NewJSONHandler(logs, nil)))

	leaderDone := make(chan struct{})
	go func() { // background work: no request, no record
		defer close(leaderDone)
		apistageContentServe(r1Ctx(), store, fill710ListCall(), false)
	}()
	if !fill710WaitFor(10*time.Second, func() bool { return arrived.Load() == 1 }) {
		t.Fatalf("PRECONDITION: the leader never dispatched")
	}
	joinerDone := make(chan struct{})
	go func() {
		defer close(joinerDone)
		slow1054Serve(mon, store)
	}()
	if !fill710WaitFor(10*time.Second, func() bool { return ContentFillStatsSnapshot().JoinWaiting == 1 }) {
		t.Fatalf("PRECONDITION: the tracked request never joined the flight")
	}
	time.Sleep(2 * slow1054Bound)
	release()
	<-joinerDone
	<-leaderDone
	if got := arrived.Load(); got != 1 {
		t.Fatalf("PRECONDITION: %d dispatches, want 1 (the joiner must wait, not fill)", got)
	}
	return mon, logs, store
}

func slow1054AssertOneFlightRecord(t *testing.T, recs []map[string]any, outcome string) {
	t.Helper()
	if len(recs) != 1 {
		t.Fatalf("F1054 RED: %d slow-call records, want exactly 1: %v", len(recs), recs)
	}
	r := recs[0]
	want := map[string]any{
		"route":        "GET /call",
		"wait_phase":   "flight_wait",
		"kind":         "content_flight",
		"wait_key":     slowcall.WaitKeyLabelForTest(fill710ListKey()),
		"gvr":          r1FsaGVR.String(),
		"wait_outcome": outcome,
		"phase":        "flight_wait",
	}
	for k, v := range want {
		if r[k] != v {
			t.Errorf("F1054 RED: %s = %v, want %v", k, r[k], v)
		}
	}
	if n, _ := r["flight_wait_n"].(float64); n != 1 {
		t.Errorf("flight_wait_n = %v, want 1", r["flight_wait_n"])
	}
	if ms, _ := r["wait_ms"].(float64); int64(ms) < 2*slow1054Bound.Milliseconds() {
		t.Errorf("wait_ms = %v, want ≥ %d (the time the leader was held)", r["wait_ms"], 2*slow1054Bound.Milliseconds())
	}
	b, _ := json.Marshal(r)
	if strings.Contains(string(b), fill710ListKey()) || strings.Contains(string(b), r1User) {
		t.Errorf("F1054 RED: a raw key or the requester reached the record: %s", b)
	}
}

func TestS1054_JoinerBlockedOnFlightPastBoundEmitsOneRecord(t *testing.T) {
	mon, logs, store := slow1054JoinBehindHeldLeader(t, nil)
	slow1054AssertOneFlightRecord(t, logs.records(t), "completed")
	if got := mon.SlowRequestsForTest(); got["GET /call"] != 1 || len(got) != 1 {
		t.Fatalf("snowplow_slow_requests_total = %v, want {GET /call: 1}", got)
	}
	// A fast read of the now-fresh cell through the same monitor adds nothing.
	slow1054Serve(mon, store)
	if got := len(logs.records(t)); got != 1 {
		t.Fatalf("FAST RED: a read under the bound emitted a record (%d total)", got)
	}
}

func TestS1054_JoinerOfAFailedFlightReportsLeaderFailed(t *testing.T) {
	_, logs, _ := slow1054JoinBehindHeldLeader(t, func(ctx context.Context, _ httpcall.RequestOptions) ([]byte, bool, context.Context) {
		return nil, false, ctx // the leader's dispatch falls through: no servable result
	})
	slow1054AssertOneFlightRecord(t, logs.records(t), "leader_failed")
}
