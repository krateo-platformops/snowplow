package slowcall

// slowcall_1054_test.go — #1054: the slow-call record and the goroutine
// snapshot, through the production middleware and phase API.
//
//   - FAST: a request under the bound emits nothing (no line, no count, no
//     ring entry), however many phases it entered.
//   - SLOW: a request over the bound emits exactly one record, naming every
//     phase it entered with its time and count, the longest wait (phase, kind,
//     digest, release) and its own release cause.
//   - UNTRACKED: the SSE stream, probes and /debug are never recorded.
//   - SNAPSHOT: taken once while a request is in flight past the bound,
//     rate-limited to one per window (a slow request first seen inside it is
//     counted suppressed, once) and size-capped.
//   - REDACTION: an identity carried by the request (a pprof label, a wait
//     key, a blocked goroutine's argument) reaches no record, snapshot or
//     ring; the raw profile DOES carry it (the filter is load-bearing).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// lineBuf is a goroutine-safe log sink.
type lineBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lineBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lineBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// records returns the JSON log records whose msg is msg.
func (l *lineBuf) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(l.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func testMonitor(t *testing.T, bound time.Duration) (*monitor, *lineBuf) {
	t.Helper()
	buf := &lineBuf{}
	m := newMonitor(bound, newStats())
	log := slog.New(slog.NewJSONHandler(buf, nil))
	m.logger = func() *slog.Logger { return log }
	return m, buf
}

// serve runs one request on pattern through m's middleware.
func serve(m *monitor, pattern string, h http.HandlerFunc) {
	path := routePath(pattern)
	if path == "" {
		path = "/unmatched" // a request no pattern serves
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Pattern = pattern
	m.middleware(h).ServeHTTP(httptest.NewRecorder(), req)
}

func num(t *testing.T, rec map[string]any, k string) int64 {
	t.Helper()
	v, ok := rec[k].(float64)
	if !ok {
		t.Fatalf("record has no number %q: %v", k, rec)
	}
	return int64(v)
}

func TestS1054_FastRequestEmitsNothing(t *testing.T) {
	m, buf := testMonitor(t, 200*time.Millisecond)
	for i := 0; i < 5; i++ {
		serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
			for p := Phase(0); p < numPhases; p++ {
				Start(r.Context(), p).End()
			}
			wait := StartWait(r.Context(), PhaseFlightWait, "content_flight", "k", "g/v/r")
			wait.End()
		})
	}
	if s := buf.String(); s != "" {
		t.Fatalf("FAST RED: a request under the bound logged: %s", s)
	}
	if got := m.stats.routes(); len(got) != 0 {
		t.Fatalf("FAST RED: a request under the bound was counted: %v", got)
	}
	if got := m.stats.ring(&m.stats.recent); len(got) != 0 {
		t.Fatalf("FAST RED: a request under the bound entered the ring: %v", got)
	}
	if m.n != 0 || m.head != nil {
		t.Fatalf("finished requests left the in-flight list: n=%d", m.n)
	}
}

func TestS1054_SlowRequestEmitsOneRecordNamingItsPhases(t *testing.T) {
	const bound = 60 * time.Millisecond
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		NoteClass(ctx, "customer")
		Start(ctx, PhaseAuth).End()
		for i := 0; i < 3; i++ {
			Start(ctx, PhaseStageDial).End()
		}
		short := StartWait(ctx, PhaseLockWait, "discovery_group", "short-key", "")
		short.End()
		long := StartWait(ctx, PhaseFlightWait, "content_flight", "the-long-key", "composition.krateo.io/v1, Resource=fsas")
		time.Sleep(2 * bound)
		long.EndRelease(ReleaseLeaderFailed)
	})
	recs := buf.records(t, "slowcall.request")
	if len(recs) != 1 {
		t.Fatalf("SLOW RED: %d records, want exactly 1:\n%s", len(recs), buf.String())
	}
	r := recs[0]
	want := map[string]string{
		"level": "WARN", "subsystem": "slowcall", "route": "GET /call", "method": "GET",
		"outcome": "completed", "class": "customer", "phase": "flight_wait",
		"wait_phase": "flight_wait", "kind": "content_flight", "wait_outcome": "leader_failed",
		"wait_key": waitKeyLabel("the-long-key"), "gvr": "composition.krateo.io/v1, Resource=fsas",
	}
	for k, v := range want {
		if r[k] != v {
			t.Errorf("SLOW RED: %s = %v, want %q", k, r[k], v)
		}
	}
	if got := num(t, r, "wall_ms"); got <= bound.Milliseconds() {
		t.Errorf("wall_ms = %d, want > bound %d", got, bound.Milliseconds())
	}
	if got := num(t, r, "flight_wait_ms"); got < 2*bound.Milliseconds() {
		t.Errorf("flight_wait_ms = %d, want ≥ %d", got, 2*bound.Milliseconds())
	}
	if got := num(t, r, "wait_ms"); got < 2*bound.Milliseconds() {
		t.Errorf("wait_ms = %d, want ≥ %d", got, 2*bound.Milliseconds())
	}
	for k, n := range map[string]int64{"stage_dial_n": 3, "auth_n": 1, "flight_wait_n": 1, "lock_wait_n": 1} {
		if got := num(t, r, k); got != n {
			t.Errorf("%s = %d, want %d", k, got, n)
		}
	}
	if _, ok := r["l1_get_n"]; ok {
		t.Errorf("a phase the request never entered is reported: %v", r)
	}
	if strings.Contains(buf.String(), "the-long-key") || strings.Contains(buf.String(), "short-key") {
		t.Errorf("a raw wait key reached the log: %s", buf.String())
	}
	if got := m.stats.routes()["GET /call"]; got != 1 {
		t.Errorf("snowplow_slow_requests_total{GET /call} = %d, want 1", got)
	}
	if got := m.stats.hist[PhaseFlightWait].count; got != 1 {
		t.Errorf("phase histogram flight_wait count = %d, want 1", got)
	}
	if ring := m.stats.ring(&m.stats.recent); len(ring) != 1 || ring[0]["wait_key"] != waitKeyLabel("the-long-key") {
		t.Errorf("recent ring = %v, want the one record", ring)
	}
}

func TestS1054_CancelledRequestReportsItsReleaseCause(t *testing.T) {
	const bound = 30 * time.Millisecond
	m, buf := testMonitor(t, bound)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx)
	req.Pattern = "GET /call"
	m.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wait := StartWait(r.Context(), PhaseFlightWait, "content_flight", "k", "")
		time.AfterFunc(2*bound, cancel)
		<-r.Context().Done()
		wait.EndRelease(CauseOf(r.Context().Err()))
	})).ServeHTTP(httptest.NewRecorder(), req)
	recs := buf.records(t, "slowcall.request")
	if len(recs) != 1 || recs[0]["outcome"] != "ctx_cancelled" || recs[0]["wait_outcome"] != "ctx_cancelled" {
		t.Fatalf("records = %v, want one with outcome and wait_outcome ctx_cancelled", recs)
	}
}

func TestS1054_UntrackedRoutesAreNeverRecorded(t *testing.T) {
	const bound = 10 * time.Millisecond
	m, buf := testMonitor(t, bound)
	for _, p := range []string{"GET /refreshes", "POST /refreshes", "GET /health", "GET /readyz",
		"GET /debug/vars", "GET /debug/pprof/", "POST /telemetry/v1/traces", "GET /swagger/", ""} {
		serve(m, p, func(w http.ResponseWriter, r *http.Request) {
			if fromContext(r.Context()) != nil {
				t.Errorf("untracked route %q got a record", p)
			}
			time.Sleep(2 * bound)
		})
	}
	if s := buf.String(); s != "" {
		t.Fatalf("an untracked route logged: %s", s)
	}
}

func TestS1054_DetachedContextCarriesNoRecord(t *testing.T) {
	m, _ := testMonitor(t, time.Second)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		if fromContext(r.Context()) == nil {
			t.Fatalf("PRECONDITION: the request has no record")
		}
		d := Detach(context.WithoutCancel(r.Context()))
		if fromContext(d) != nil {
			t.Fatalf("Detach left the request's record on the context")
		}
		if s := Start(d, PhaseStageDial); s.r != nil {
			t.Fatalf("a span on a detached context was recorded")
		}
	})
}

// blockedRequest starts one tracked request that opens a flight wait and
// blocks until release is closed. It returns once the request is in flight.
func blockedRequest(t *testing.T, m *monitor, key string, release <-chan struct{}) <-chan struct{} {
	t.Helper()
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
			wait := StartWait(r.Context(), PhaseFlightWait, "content_flight", key, "")
			close(started)
			<-release
			wait.End()
		})
	}()
	<-started
	return done
}

// syntheticProfile is a debug=1 goroutine profile of n distinct request
// stacks of frames frames each.
func syntheticProfile(n, frames int) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "goroutine profile: total %d\n", n*3)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "3 @ 0x1 0x2\n# labels: {%q:%q}\n", workclass.Key, "customer")
		for f := 0; f < frames; f++ {
			fmt.Fprintf(&b, "#\t0x%x\tgithub.com/krateo-platformops/snowplow/internal/cache.(*someVeryLongTypeName).someVeryLongMethodName%d_%d+0x1c\t/build/src/internal/cache/file_%d.go:%d\n", f, i, f, f, f+10)
		}
		fmt.Fprintf(&b, "#\t0x99\tnet/http.(*conn).serve+0x1\t/usr/local/go/src/net/http/server.go:2122\n\n")
	}
	return []byte(b.String())
}

func TestS1054_SnapshotIsRateLimitedToOnePerWindow(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, buf := testMonitor(t, bound)
	m.capture = func() []byte { return syntheticProfile(3, 4) }
	release := make(chan struct{})
	defer close(release)

	a := time.Now()
	blockedRequest(t, m, "key-a", release)
	t0 := a.Add(2 * bound)
	if !m.check(t0) {
		t.Fatalf("SNAPSHOT RED: no snapshot for a request in flight past the bound")
	}
	if m.check(t0.Add(time.Second)) {
		t.Fatalf("RATE RED: a second snapshot for the same request")
	}
	blockedRequest(t, m, "key-b", release)
	if m.check(t0.Add(10 * time.Second)) {
		t.Fatalf("RATE RED: a second snapshot inside the %v window", m.snapEvery)
	}
	m.check(t0.Add(20 * time.Second)) // still inside: counted once, not twice
	if got := m.stats.suppressed.Load(); got != 1 {
		t.Fatalf("suppressed = %d, want 1 (the second request, once)", got)
	}
	if !m.check(t0.Add(m.snapEvery + time.Second)) {
		t.Fatalf("no snapshot after the window for a slow request no snapshot covered")
	}
	if got := m.stats.snapshots.Load(); got != 2 {
		t.Fatalf("snapshots = %d, want 2", got)
	}
	if got := len(buf.records(t, "slowcall.goroutines")); got != 2 {
		t.Fatalf("snapshot log lines = %d, want 2", got)
	}
	if got := len(m.stats.ring(&m.stats.snaps)); got != recentSnapshots {
		t.Fatalf("snapshot ring holds %d, want %d", got, recentSnapshots)
	}
}

func TestS1054_SnapshotIsSizeCapped(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, buf := testMonitor(t, bound)
	m.capture = func() []byte { return syntheticProfile(400, 80) }
	release := make(chan struct{})
	defer close(release)
	a := time.Now()
	for i := 0; i < maxSnapshotRequests+3; i++ {
		blockedRequest(t, m, fmt.Sprintf("key-%d", i), release)
	}
	if !m.check(a.Add(time.Hour)) {
		t.Fatalf("PRECONDITION: no snapshot")
	}
	recs := buf.records(t, "slowcall.goroutines")
	if len(recs) != 1 {
		t.Fatalf("snapshot lines = %d, want 1", len(recs))
	}
	r := recs[0]
	stacks, _ := r["stacks"].(string)
	reqs, _ := r["requests"].(string)
	if len(stacks)+len(reqs) > maxSnapshotBytes {
		t.Fatalf("SIZE RED: snapshot text %d bytes, cap %d", len(stacks)+len(reqs), maxSnapshotBytes)
	}
	if r["truncated"] != true && num(t, r, "stacks_omitted") == 0 {
		t.Fatalf("SIZE RED: 400 distinct stacks all kept: %v", r)
	}
	if got := num(t, r, "stacks_kept"); got == 0 || got > maxStacksPerClass*2 {
		t.Fatalf("stacks_kept = %d, want 1..%d", got, maxStacksPerClass*2)
	}
	if got := num(t, r, "slow_inflight"); got != maxSnapshotRequests {
		t.Fatalf("slow_inflight = %d, want the cap %d", got, maxSnapshotRequests)
	}
	// Frames per stack: a stack is a count line then its frames, indented.
	frames := 0
	for _, line := range strings.Split(stacks, "\n") {
		switch {
		case strings.HasPrefix(line, "  "):
			frames++
			if frames > maxFramesPerStack {
				t.Fatalf("SIZE RED: a stack kept more than %d frames", maxFramesPerStack)
			}
		default:
			frames = 0
		}
	}
	if strings.Contains(stacks, "0x1c") || strings.Contains(stacks, "/build/src/") {
		t.Fatalf("a pc offset or a full path survived: %s", stacks[:200])
	}
	if strings.Count(reqs, "\n") != maxSnapshotRequests {
		t.Fatalf("requests section lists %d requests, want %d", strings.Count(reqs, "\n"), maxSnapshotRequests)
	}
}

// s1054ParkedFor blocks its goroutine with an identity as its argument.
//
//go:noinline
func s1054ParkedFor(username string, release <-chan struct{}) int {
	<-release
	return len(username)
}

func TestS1054_SnapshotFromTheRealProfileNamesTheBlockedRequest(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, buf := testMonitor(t, bound)
	release := make(chan struct{})
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Pattern = "GET /call"
		m.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			wait := StartWait(r.Context(), PhaseFlightWait, "content_flight", "real-key", "g/v1, Resource=r")
			close(started)
			s1054ParkedFor("nobody", release)
			wait.End()
		})).ServeHTTP(w, r)
	}))
	defer srv.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := http.Get(srv.URL + "/call"); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	took := m.check(time.Now().Add(2 * bound))
	close(release)
	<-done
	if !took {
		t.Fatalf("no snapshot")
	}
	r := buf.records(t, "slowcall.goroutines")[0]
	stacks, _ := r["stacks"].(string)
	if !strings.Contains(stacks, "== request stacks ==") || !strings.Contains(stacks, "slowcall.s1054ParkedFor") {
		t.Fatalf("the real snapshot does not show the blocked request's frame:\n%s", stacks)
	}
	reqs, _ := r["requests"].(string)
	if !strings.Contains(reqs, "wait{flight_wait content_flight "+waitKeyLabel("real-key")) {
		t.Fatalf("the snapshot does not name the open wait: %q", reqs)
	}
	if r["kind"] != "content_flight" || r["wait_outcome"] != "open" || r["wait_key"] != waitKeyLabel("real-key") {
		t.Fatalf("the snapshot's oldest wait = %v", r)
	}
	if got := num(t, r, "request_goroutines"); got < 1 {
		t.Fatalf("request_goroutines = %d", got)
	}
}

func TestS1054_NoIdentityReachesAnyRecordSnapshotOrRing(t *testing.T) {
	const bound = 20 * time.Millisecond
	const user = "s1054-alice-identity"
	m, buf := testMonitor(t, bound)
	release := make(chan struct{})
	started := make(chan struct{})
	var raw []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Pattern = "GET /call"
		m.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The request's goroutine carries the identity as a pprof label
			// next to an allowlisted one, waits on a key that names it (the
			// impersonation review's key shape), and blocks with it as an
			// argument.
			pprof.Do(r.Context(), pprof.Labels(workclass.Key, "customer", "user", user), func(ctx context.Context) {
				NoteClass(ctx, "customer")
				wait := StartWait(ctx, PhaseFlightWait, "impersonation_review", "7\x00users\x00\x00"+user, "")
				close(started)
				s1054ParkedFor(user, release)
				wait.End()
			})
		})).ServeHTTP(w, r)
	}))
	defer srv.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := http.Get(srv.URL + "/call"); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	raw = goroutineProfile()
	took := m.check(time.Now().Add(2 * bound))
	time.Sleep(2 * bound) // the request itself crosses the bound
	close(release)
	<-done

	// Positive control: the raw profile carries the identity, so the summary's
	// label filter is what keeps it out.
	if !strings.Contains(string(raw), user) {
		t.Fatalf("PRECONDITION: the raw goroutine profile does not carry the label; the arm could not fail")
	}
	if !took {
		t.Fatalf("PRECONDITION: no snapshot")
	}
	snaps := buf.records(t, "slowcall.goroutines")
	recs := buf.records(t, "slowcall.request")
	if len(snaps) != 1 || len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d snapshots, %d records, want 1 each", len(snaps), len(recs))
	}
	if stacks, _ := snaps[0]["stacks"].(string); !strings.Contains(stacks, workclass.Key+"=customer") {
		t.Fatalf("PRECONDITION: the allowlisted label is not kept, so the arm cannot tell filtering from dropping:\n%s", stacks)
	}
	if s := buf.String(); strings.Contains(s, user) {
		t.Fatalf("REDACTION RED: the identity reached the log:\n%s", s)
	}
	for _, ring := range [][]map[string]any{m.stats.ring(&m.stats.recent), m.stats.ring(&m.stats.snaps)} {
		b, _ := json.Marshal(ring)
		if strings.Contains(string(b), user) {
			t.Fatalf("REDACTION RED: the identity reached a /debug/vars ring: %s", b)
		}
	}
	if recs[0]["wait_key"] != waitKeyLabel("7\x00users\x00\x00"+user) {
		t.Fatalf("wait_key = %v, want the key's digest", recs[0]["wait_key"])
	}
}

func TestS1054_SummaryKeepsOnlyAllowlistedLabelsAndNoArguments(t *testing.T) {
	prof := "goroutine profile: total 4\n" +
		"3 @ 0x1 0x2\n" +
		"# labels: {\"user\":\"bob\", \"" + workclass.Key + "\":\"customer\", \"weird, \\\"key\\\"\":\"x\"}\n" +
		"#\t0x1\truntime.gopark+0x1\t/go/src/runtime/proc.go:435\n" +
		"#\t0x2\tnet/http.(*conn).serve+0x2\t/go/src/net/http/server.go:2122\n" +
		"\n" +
		"1 @ 0x3\n" +
		"#\t0x3\tgithub.com/krateo-platformops/snowplow/internal/cache.(*refresher).processNext+0x3\t/src/cache/refresher.go:9\n\n"
	s := summarizeGoroutines([]byte(prof), maxSnapshotBytes)
	if strings.Contains(s.text, "bob") || strings.Contains(s.text, "user") || strings.Contains(s.text, "weird") {
		t.Fatalf("a label outside the allowlist survived:\n%s", s.text)
	}
	if !strings.Contains(s.text, "3 {"+workclass.Key+"=customer}") {
		t.Fatalf("the allowlisted label was not kept:\n%s", s.text)
	}
	if s.total != 4 || s.requestGoroutines != 3 || s.backgroundGoroutines != 1 {
		t.Fatalf("counts = %+v", s)
	}
	if !strings.Contains(s.text, "runtime.gopark proc.go:435") || strings.Contains(s.text, "0x") {
		t.Fatalf("frames not reduced to func + file:line:\n%s", s.text)
	}
}

// BenchmarkS1054_FastPath measures what the record costs a request under the
// bound: the same handler (five phases, the shape of a cached /call: auth,
// object fetch, RBAC check, key mint, L1 get) with and without the middleware.
func BenchmarkS1054_FastPath(b *testing.B) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		for _, p := range []Phase{PhaseAuth, PhaseObjectFetch, PhaseRBACCheck, PhaseKeyMint, PhaseL1Get} {
			Start(ctx, p).End()
		}
	})
	empty := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	m := newMonitor(Bound, newStats())
	for _, c := range []struct {
		name string
		h    http.Handler
	}{
		{"without", h},                       // five phase sites, no record
		{"with", m.middleware(h)},            // the same, recorded
		{"record_only", m.middleware(empty)}, // the record alone, no phase site
		{"record_only_baseline", empty},      // ...and its baseline
	} {
		b.Run(c.name, func(b *testing.B) {
			req := httptest.NewRequest(http.MethodGet, "/call", nil)
			req.Pattern = "GET /call"
			w := httptest.NewRecorder()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.h.ServeHTTP(w, req)
			}
		})
	}
}

// BenchmarkS1054_SpanOnUntrackedContext is the cost of a phase site on
// background work (no record): one ctx.Value walk of a 24-deep context.
func BenchmarkS1054_SpanOnUntrackedContext(b *testing.B) {
	ctx := context.Background()
	for i := 0; i < 24; i++ {
		ctx = context.WithValue(ctx, struct{ i int }{i}, i)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Start(ctx, PhaseStageDial).End()
	}
}
