package api

// apistage_fill_flight_slowcall_1054_test.go — #1054: the slow-call record at
// the resolver's own sites, behind the production slow-call middleware (a test
// monitor with a short bound).
//
//   - F1054-SF1: a request blocked on ANOTHER reader's content fill (the #710
//     single-flight) past the bound emits exactly one record: flight_wait, the
//     content key's digest, the GVR, released completed, and WHOSE fill it
//     waited on (a refresher's). A later fast read adds none.
//   - F1054-SF2: the flight's leader falls through → leader_failed.
//   - F1054-SF3: a CUSTOMER leader's own record carries the lead marker with
//     the same digest the joiner's wait names, so the two records join.
//   - F1054-SF4: a refresher reader queued behind an in-flight fill (#765's
//     pending flight) records its pending wait, named and led.
//   - F1054-D1: an apiserver LIST answered 429 + Retry-After is ONE dial with
//     two attempts, a 429 and the client-go retry sleep, and is the longest
//     wait (through dispatchViaInternalRESTConfig and credhttp's client).
//   - F1054-C1: a stage body's decode and its filter's JQ are phases.

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
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/client-go/rest"
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

func slow1054Monitor() (*slowcall.MonitorForTest, *slow1054Log) {
	logs := &slow1054Log{}
	return slowcall.NewMonitorForTest(slow1054Bound, slog.New(slog.NewJSONHandler(logs, nil))), logs
}

// slow1054Run runs one tracked GET /call whose handler is fn(the request ctx).
func slow1054Run(mon *slowcall.MonitorForTest, fn func(ctx context.Context)) {
	h := mon.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fn(r.Context()) }))
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Pattern = "GET /call"
	h.ServeHTTP(httptest.NewRecorder(), req)
}

// slow1054Customer is a customer read of the fixture LIST as r1User, on ctx.
func slow1054Customer(store *cache.ResolvedCacheStore) func(ctx context.Context) {
	return func(ctx context.Context) {
		ctx, end := workclass.Begin(xcontext.BuildContext(ctx, xcontext.WithUserInfo(jwtutil.UserInfo{Username: r1User})), workclass.Customer)
		defer end()
		apistageContentServe(ctx, store, fill710ListCall(), false)
	}
}

// slow1054JoinBehindHeldLeader holds a leader's fill in its dispatch, sends one
// tracked request that joins its flight, keeps it waiting past the bound, then
// lets the leader finish with finish (nil: the real dispatch). The leader is a
// refresher unit with no request (trackedLeader false), or a tracked customer
// request through the same monitor.
func slow1054JoinBehindHeldLeader(t *testing.T, trackedLeader bool, finish func(context.Context, httpcall.RequestOptions) ([]byte, bool, context.Context)) (*slowcall.MonitorForTest, *slow1054Log, *cache.ResolvedCacheStore) {
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
	mon, logs := slow1054Monitor()

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		if trackedLeader {
			slow1054Run(mon, slow1054Customer(store))
			return
		}
		// Background work: a refresher unit, no request, no record.
		ctx, end := workclass.Begin(r1Ctx(), workclass.Refresh)
		defer end()
		apistageContentServe(ctx, store, fill710ListCall(), false)
	}()
	if !fill710WaitFor(10*time.Second, func() bool { return arrived.Load() == 1 }) {
		t.Fatalf("PRECONDITION: the leader never dispatched")
	}
	joinerDone := make(chan struct{})
	go func() {
		defer close(joinerDone)
		slow1054Run(mon, slow1054Customer(store))
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

// slow1054Joiner returns the one record whose wait is the content flight.
func slow1054Joiner(t *testing.T, recs []map[string]any) map[string]any {
	t.Helper()
	var out []map[string]any
	for _, r := range recs {
		if r["wait_kind"] == "content_flight" {
			out = append(out, r)
		}
	}
	if len(out) != 1 {
		t.Fatalf("F1054 RED: %d joiner records, want exactly 1: %v", len(out), recs)
	}
	return out[0]
}

func slow1054AssertFlightWait(t *testing.T, r map[string]any, outcome, leaderWork string) {
	t.Helper()
	want := map[string]any{
		"route":            "GET /call",
		"class":            "customer",
		"wait_phase":       "flight_wait",
		"wait_kind":        "content_flight",
		"wait_key":         slowcall.WaitKeyLabelForTest(fill710ListKey()),
		"wait_gvr":         r1FsaGVR.String(),
		"wait_outcome":     outcome,
		"wait_leader_work": leaderWork,
		"phase":            "flight_wait",
	}
	for k, v := range want {
		if r[k] != v {
			t.Errorf("F1054 RED: %s = %v, want %v", k, r[k], v)
		}
	}
	if _, ok := r["wait_leader_class"].(string); !ok {
		t.Errorf("F1054 RED: the flight wait does not name its leader's admission class: %v", r)
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
	mon, logs, store := slow1054JoinBehindHeldLeader(t, false, nil)
	recs := logs.records(t)
	if len(recs) != 1 {
		t.Fatalf("F1054 RED: %d slow-call records, want exactly 1 (the leader is not a request): %v", len(recs), recs)
	}
	slow1054AssertFlightWait(t, recs[0], "completed", "refresh")
	if got := mon.SlowRequestsForTest(); got[slowcall.RouteClass{Route: "GET /call", Class: "customer"}] != 1 || len(got) != 1 {
		t.Fatalf("snowplow_slow_requests_total = %v, want {GET /call, customer: 1}", got)
	}
	// A fast read of the now-fresh cell through the same monitor adds nothing.
	slow1054Run(mon, slow1054Customer(store))
	if got := len(logs.records(t)); got != 1 {
		t.Fatalf("FAST RED: a read under the bound emitted a record (%d total)", got)
	}
}

func TestS1054_JoinerOfAFailedFlightReportsLeaderFailed(t *testing.T) {
	_, logs, _ := slow1054JoinBehindHeldLeader(t, false, func(ctx context.Context, _ httpcall.RequestOptions) ([]byte, bool, context.Context) {
		return nil, false, ctx // the leader's dispatch falls through: no servable result
	})
	recs := logs.records(t)
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	slow1054AssertFlightWait(t, recs[0], "leader_failed", "refresh")
}

func TestS1054_CustomerLeaderRecordCarriesTheSameKeyAsItsJoiner(t *testing.T) {
	_, logs, _ := slow1054JoinBehindHeldLeader(t, true, nil)
	recs := logs.records(t)
	if len(recs) != 2 {
		t.Fatalf("PRECONDITION: %d records, want the leader's and the joiner's", len(recs))
	}
	joiner := slow1054Joiner(t, recs)
	slow1054AssertFlightWait(t, joiner, "completed", "customer")
	var leader map[string]any
	for _, r := range recs {
		if r["wait_kind"] != "content_flight" {
			leader = r
		}
	}
	if leader["lead_kind"] != "content_flight" || leader["lead_key"] != joiner["wait_key"] || leader["lead_gvr"] != r1FsaGVR.String() {
		t.Fatalf("LEAD RED: the leader's record does not carry the joiner's key: lead=%v/%v/%v, joiner wait_key=%v",
			leader["lead_kind"], leader["lead_key"], leader["lead_gvr"], joiner["wait_key"])
	}
	if n, _ := leader["leads"].(float64); n != 1 {
		t.Fatalf("leads = %v, want 1", leader["leads"])
	}
}

// TestS1054_PendingFlightWaitIsRecorded is #765's B3 shape: a refresher reader
// refused by the in-flight fill (its coordinate moved) leads a PENDING flight
// and waits for the in-flight one before it fills. That wait is its record's
// flight wait, named content_flight_pending and led by the first refresher.
func TestS1054_PendingFlightWaitIsRecorded(t *testing.T) {
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)
	_, read, release := stamp765HoldDispatches(t, 1)
	stamp765Change(t, dyn, 2)
	first, endFirst := workclass.Begin(stamp765RefreshCtx(), workclass.Refresh)
	defer endFirst()
	lead := stamp765Serve(first, store)
	stamp765WaitClosed(t, "the first refresher re-fill never dispatched", read[0])
	stamp765Change(t, dyn, 3)

	mon, logs := slow1054Monitor()
	done := make(chan struct{})
	go func() {
		defer close(done)
		slow1054Run(mon, func(ctx context.Context) {
			ctx = xcontext.BuildContext(ctx, xcontext.WithUserInfo(jwtutil.UserInfo{Username: r1User}))
			ctx = cache.WithRefreshTriggerGVR(cache.WithBackgroundResolve(ctx), r1FsaGVR)
			apistageContentServe(ctx, store, fill710ListCall(), false)
		})
	}()
	if !fill710WaitFor(10*time.Second, func() bool { return stamp765Refused() == 1 }) {
		t.Fatalf("PRECONDITION: the second reader was not refused by the in-flight fill")
	}
	time.Sleep(2 * slow1054Bound)
	release[0]()
	stamp765Recv(t, "first refresh", lead)
	<-done
	recs := logs.records(t)
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	r := recs[0]
	for k, v := range map[string]any{"wait_kind": "content_flight_pending", "wait_phase": "flight_wait",
		"wait_key": slowcall.WaitKeyLabelForTest(fill710ListKey()), "wait_leader_work": "refresh", "wait_leader_class": "background"} {
		if r[k] != v {
			t.Errorf("PENDING RED: %s = %v, want %v", k, r[k], v)
		}
	}
	if ms, _ := r["wait_ms"].(float64); int64(ms) < 2*slow1054Bound.Milliseconds() {
		t.Errorf("wait_ms = %v, want ≥ %d", r["wait_ms"], 2*slow1054Bound.Milliseconds())
	}
}

// TestS1054_ApiserverListRetriedAfter429IsOneDialWithTwoAttempts sends an
// apiserver LIST through the production dispatch and credhttp's client to a
// server that answers the first attempt 429 with Retry-After: 1. client-go
// sleeps it and retries: one dial, two attempts, one 429, ~1 s of retry wait.
func TestS1054_ApiserverListRetriedAfter429IsOneDialWithTwoAttempts(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"composition.krateo.io/v1","kind":"FullStackAppList","metadata":{},"items":[]}`))
	}))
	defer srv.Close()
	rc := &rest.Config{Host: srv.URL}
	mon, logs := slow1054Monitor()
	var derr error
	slow1054Run(mon, func(ctx context.Context) {
		ctx = ownClusterCtx(cache.WithInternalRESTConfig(ctx, rc))
		_, _, derr = dispatchViaInternalRESTConfig(ctx, httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{
				Path: "/apis/composition.krateo.io/v1/namespaces/" + r1Ns + "/fullstackapps",
				Verb: ptr.To(http.MethodGet),
			},
		})
	})
	if derr != nil {
		t.Fatalf("PRECONDITION: dispatch: %v", derr)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("PRECONDITION: %d attempts reached the server, want 2 (one 429, one retry)", got)
	}
	recs := logs.records(t)
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	r := recs[0]
	for k, v := range map[string]any{"wait_kind": "dial", "wait_branch": "internal_rest_config", "wait_phase": "stage_dial", "wait_verb": "list",
		"wait_gvr": r1FsaGVR.String(), "wait_outcome": "completed", "dial_branch": "internal_rest_config", "phase": "stage_dial"} {
		if r[k] != v {
			t.Errorf("DIAL RED: %s = %v, want %v", k, r[k], v)
		}
	}
	for k, v := range map[string]float64{"wait_attempts": 2, "wait_http_429": 1, "wait_status": 200, "stage_dial_n": 1} {
		if r[k] != v {
			t.Errorf("DIAL RED: %s = %v, want %v", k, r[k], v)
		}
	}
	if ms, _ := r["wait_retry_wait_ms"].(float64); ms < 900 {
		t.Errorf("DIAL RED: wait_retry_wait_ms = %v, want ≈ 1000 (client-go's Retry-After sleep)", r["wait_retry_wait_ms"])
	}
	if b, _ := json.Marshal(r); strings.Contains(string(b), "127.0.0.1") || strings.Contains(string(b), "/namespaces/") {
		t.Errorf("a dial's host or path reached the record: %s", b)
	}
}

// TestS1054_ExternalStageRetriedAfter5xxIsOneDialWithTwoAttempts drives the
// full Resolve to an external endpoint (the AC6 harness shape) that answers
// the first attempt 503: plumbing's retry client backs off and retries. The
// resolve.go dial records one dial, two attempts, one 5xx and the backoff.
func TestS1054_ExternalStageRetriedAfter5xxIsOneDialWithTwoAttempts(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "1")
	t.Setenv("CLIENT_BASE_BACKOFF", "400ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "400ms")
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	mon, logs := slow1054Monitor()
	slow1054Run(mon, func(ctx context.Context) {
		ctx = xcontext.BuildContext(ctx, xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s1054-external-user"}))
		ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
		Resolve(ctx, ResolveOptions{
			RC:                  &rest.Config{},
			Items:               []*templates.API{{Name: "ext", Path: "/x", Verb: ptr.To(http.MethodGet)}},
			RESTActionNamespace: "default",
			RESTActionName:      "s1054-external",
		})
	})
	if got := hits.Load(); got != 2 {
		t.Fatalf("PRECONDITION: %d attempts reached the endpoint, want 2 (one 503, one retry)", got)
	}
	recs := logs.records(t)
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	r := recs[0]
	for k, v := range map[string]any{"wait_kind": "dial", "wait_branch": "external_fetch", "wait_phase": "stage_dial", "wait_verb": "GET", "wait_last_cause": "ok",
		"wait_attempts": float64(2), "wait_http_5xx": float64(1), "wait_status": float64(200), "stage_dial_n": float64(1)} {
		if r[k] != v {
			t.Errorf("DIAL RED: %s = %v, want %v", k, r[k], v)
		}
	}
	if ms, _ := r["wait_retry_wait_ms"].(float64); ms < 150 {
		t.Errorf("DIAL RED: wait_retry_wait_ms = %v, want the retry client's backoff", r["wait_retry_wait_ms"])
	}
	if b, _ := json.Marshal(r); strings.Contains(string(b), "127.0.0.1") {
		t.Errorf("the endpoint's host reached the record: %s", b)
	}
}

// TestS1054_StageDecodeAndFilterJQArePhases runs a stage body through the
// production handler with a filter: the record names parse and jq, once per
// stage body.
func TestS1054_StageDecodeAndFilterJQArePhases(t *testing.T) {
	mon, logs := slow1054Monitor()
	filter := `[range(0; 2000)] | map(. * 2) | length`
	var herr error
	slow1054Run(mon, func(ctx context.Context) {
		for i := 0; i < 5 && herr == nil; i++ {
			opts := jsonHandlerOptions{key: "s", out: map[string]any{}, dict: map[string]any{}, filter: &filter}
			herr = jsonHandlerBytesApply(ctx, opts, []byte(`{"items":[1,2,3]}`))
		}
		time.Sleep(slow1054Bound) // a slow request whatever the host's speed
	})
	if herr != nil {
		t.Fatalf("PRECONDITION: handler: %v", herr)
	}
	recs := logs.records(t)
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	r := recs[0]
	if r["parse_n"] != float64(5) || r["jq_n"] != float64(5) {
		t.Fatalf("COMPUTE RED: parse_n=%v jq_n=%v, want 5 each", r["parse_n"], r["jq_n"])
	}
}
