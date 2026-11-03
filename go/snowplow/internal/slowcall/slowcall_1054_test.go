package slowcall

// slowcall_1054_test.go — #1054: the slow-call record and the goroutine
// snapshot, through the production middleware and phase API.
//
//   - FAST: a request under the bound emits nothing, however many phases it
//     entered; a request AT the bound is not slow (the comparison is strict).
//   - SLOW: one record per slow request, naming its phases, the LONGEST wait
//     (not the last), its class and release cause.
//   - DIALS: every HTTP attempt is recorded on its dial (attempts, status,
//     429s, retry sleep, server time); a dial is a wait candidate.
//   - RESPONSE PATH: the network write and gzip are separate phases.
//   - CPU: the cgroup throttling and process CPU over the request's lifetime.
//   - CLASS: a background request is counted under its class and never takes
//     a snapshot.
//   - SNAPSHOT: taken once while a customer request is in flight past the
//     bound, for EVERY such request however many there are; rate-limited per
//     process (a later one is suppressed once) and size-capped.
//   - REDACTION: an identity carried by the request reaches no record,
//     snapshot or ring; the raw profile DOES carry it.
//
// The arms named TestReview_* are the #1059 reviewer's, adopted as written.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzhttp"
	"github.com/krateo-platformops/plumbing/http/util"

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

func (m *monitor) inflightForTest() int {
	n := 0
	for i := range m.shards {
		m.shards[i].mu.Lock()
		n += m.shards[i].n
		m.shards[i].mu.Unlock()
	}
	return n
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
			_, _ = w.Write([]byte("{}"))
		})
	}
	if s := buf.String(); s != "" {
		t.Fatalf("FAST RED: a request under the bound logged: %s", s)
	}
	if got := m.stats.counts(); len(got) != 0 {
		t.Fatalf("FAST RED: a request under the bound was counted: %v", got)
	}
	if got := m.stats.ring(&m.stats.recent); len(got) != 0 {
		t.Fatalf("FAST RED: a request under the bound entered the ring: %v", got)
	}
	if n := m.inflightForTest(); n != 0 {
		t.Fatalf("finished requests left the in-flight list: n=%d", n)
	}
}

// TestS1054_RequestAtTheBoundIsNotSlow pins the comparison: wall == bound is
// not slow, bound + 1 ns is (the reviewer's MA2).
func TestS1054_RequestAtTheBoundIsNotSlow(t *testing.T) {
	const bound = 100 * time.Millisecond
	for _, c := range []struct {
		wall time.Duration
		slow bool
	}{{bound, false}, {bound + time.Nanosecond, true}} {
		m, buf := testMonitor(t, bound)
		t0 := time.Now()
		calls := 0
		m.now = func() time.Time {
			calls++
			if calls == 1 {
				return t0
			}
			return t0.Add(c.wall)
		}
		serve(m, "GET /call", func(http.ResponseWriter, *http.Request) {})
		if got := len(buf.records(t, "slowcall.request")) == 1; got != c.slow {
			t.Errorf("BOUNDARY RED: wall=%v slow=%v, want %v", c.wall, got, c.slow)
		}
	}
}

func TestS1054_SlowRequestEmitsOneRecordNamingItsPhases(t *testing.T) {
	const bound = 60 * time.Millisecond
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		NoteClass(ctx, workclass.Customer)
		Start(ctx, PhaseAuth).End()
		for i := 0; i < 3; i++ {
			Start(ctx, PhaseJQ).End()
		}
		short := StartWait(ctx, PhaseLockWait, "discovery_group", "short-key", "")
		short.End()
		long := StartLedWait(ctx, PhaseFlightWait, "content_flight", "the-long-key", "composition.krateo.io/v1, Resource=fsas", "background", "refresh")
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
		"wait_phase": "flight_wait", "wait_kind": "content_flight", "wait_outcome": "leader_failed",
		"wait_key": waitKeyLabel("the-long-key"), "wait_gvr": "composition.krateo.io/v1, Resource=fsas",
		"wait_leader_class": "background", "wait_leader_work": "refresh",
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
	for k, n := range map[string]int64{"jq_n": 3, "auth_n": 1, "flight_wait_n": 1, "lock_wait_n": 1} {
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
	if got := m.stats.counts()[RouteClass{"GET /call", "customer"}]; got != 1 {
		t.Errorf("snowplow_slow_requests_total{GET /call, customer} = %d, want 1", got)
	}
	if got := m.stats.hist[PhaseFlightWait].count; got != 1 {
		t.Errorf("phase histogram flight_wait count = %d, want 1", got)
	}
	if ring := m.stats.ring(&m.stats.recent); len(ring) != 1 || ring[0]["wait_key"] != waitKeyLabel("the-long-key") {
		t.Errorf("recent ring = %v, want the one record", ring)
	}
}

// TestReview_RecordNamesLongestNotLastWait (reviewer, MB): with a long wait
// then a short one, the record names the LONGEST.
func TestReview_RecordNamesLongestNotLastWait(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		a := StartWait(r.Context(), PhaseFlightWait, "content_flight", "long", "g/v1, Resource=long")
		time.Sleep(3 * bound)
		a.End()
		b := StartWait(r.Context(), PhaseLockWait, "discovery_group", "short", "")
		time.Sleep(bound / 4)
		b.End()
	})
	recs := buf.records(t, "slowcall.request")
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	if recs[0]["wait_kind"] != "content_flight" || recs[0]["wait_key"] != waitKeyLabel("long") {
		t.Fatalf("LONGEST RED: record names %v / %v, want the long content_flight", recs[0]["wait_kind"], recs[0]["wait_key"])
	}
}

// TestReview_BoundIsTheGoal (reviewer, MA1/MA3/MM): the bound is the goal,
// exactly 3 s, and the watchdog checks every quarter bound.
func TestReview_BoundIsTheGoal(t *testing.T) {
	if Bound != 3*time.Second {
		t.Fatalf("BOUND RED: Bound=%v, want 3s", Bound)
	}
	m := newMonitor(Bound, newStats())
	if m.tick != 750*time.Millisecond {
		t.Fatalf("tick=%v, want 750ms", m.tick)
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
		if rc, _ := d.Value(recordCtxKey{}).(*recordCtx); rc != nil {
			t.Fatalf("Detach left the request's writers on the context")
		}
		if s := Start(d, PhaseStageDial); s.r != nil {
			t.Fatalf("a span on a detached context was recorded")
		}
	})
}

func TestS1054_HistogramBucketBoundaryIsInclusive(t *testing.T) {
	var h phaseHist
	h.buckets = make([]uint64, len(PhaseBoundsMS)+1)
	h.record(3000) // exactly the bound: in le_3000
	h.record(3001) // over it: in le_5000
	idx := func(b int64) int {
		for i, v := range PhaseBoundsMS {
			if v == b {
				return i
			}
		}
		t.Fatalf("no bound %d", b)
		return -1
	}
	if h.buckets[idx(3000)] != 1 || h.buckets[idx(5000)] != 1 {
		t.Fatalf("BUCKET RED: buckets=%v, want 3000 → le_3000 and 3001 → le_5000", h.buckets)
	}
}

// --- dials -----------------------------------------------------------------

// TestS1054_DialRecordsEveryAttemptAndIsTheLongestWait drives plumbing's
// retry client (the per-user and external endpoint dial) through Transport
// against a server that answers one 429 without Retry-After: the retry client
// sleeps its 2 s default and retries. The record's dial says so (attempts 2,
// one 429, ~2 s of retry wait, status 200) and, being the longest wait, is
// the reported wait.
func TestS1054_DialRecordsEveryAttemptAndIsTheLongestWait(t *testing.T) {
	const bound = 200 * time.Millisecond
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		time.Sleep(50 * time.Millisecond) // server time
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		short := StartWait(r.Context(), PhaseFlightWait, "content_flight", "k", "")
		time.Sleep(bound / 2)
		short.End()
		ctx, dial := StartDial(r.Context(), BranchExternalFetch, "GET", "")
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/x?token=secret", nil)
		cli := util.NewRetryClient(&http.Client{Transport: Transport(http.DefaultTransport)})
		resp, err := cli.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		dial.EndErr(r.Context(), err)
	})
	recs := buf.records(t, "slowcall.request")
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	r := recs[0]
	for k, v := range map[string]string{"wait_kind": "dial", "wait_branch": "external_fetch", "wait_phase": "stage_dial", "wait_verb": "GET",
		"wait_outcome": "completed", "wait_last_cause": "ok", "dial_branch": "external_fetch", "wait_host": hostDigest(strings.TrimPrefix(srv.URL, "http://"))} {
		if r[k] != v {
			t.Errorf("DIAL RED: %s = %v, want %q", k, r[k], v)
		}
	}
	for k, v := range map[string]int64{"wait_attempts": 2, "wait_http_429": 1, "wait_status": 200, "http_attempts": 2, "http_429": 1, "dial_attempts": 2} {
		if got := num(t, r, k); got != v {
			t.Errorf("DIAL RED: %s = %d, want %d", k, got, v)
		}
	}
	if got := num(t, r, "wait_retry_wait_ms"); got < 1900 {
		t.Errorf("DIAL RED: wait_retry_wait_ms = %d, want ≈ 2000 (the retry client's 429 default)", got)
	}
	if got := num(t, r, "wait_ttfb_ms"); got < 40 {
		t.Errorf("DIAL RED: wait_ttfb_ms = %d, want ≥ 40 (the server's own time to first byte)", got)
	}
	if d, _ := r["wait_attempt_detail"].(string); !strings.HasPrefix(d, "1 429 reused:false") || !strings.Contains(d, "; 2 200 ") {
		t.Errorf("DIAL RED: wait_attempt_detail = %q, want a fresh-connection 429 then a 200", d)
	}
	if s := buf.String(); strings.Contains(s, "token=secret") || strings.Contains(s, "127.0.0.1") {
		t.Errorf("a dial's path, query or host reached the log: %s", s)
	}
}

func TestS1054_AttemptOutsideAnyDialIsCountedUnscoped(t *testing.T) {
	const bound = 20 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, srv.URL, nil)
		if resp, err := (&http.Client{Transport: Transport(http.DefaultTransport)}).Do(req); err == nil {
			resp.Body.Close()
		}
		time.Sleep(2 * bound)
	})
	r := buf.records(t, "slowcall.request")[0]
	if num(t, r, "unscoped_http_n") != 1 || num(t, r, "http_attempts") != 0 {
		t.Fatalf("an attempt outside any dial: unscoped_http_n=%v http_attempts=%v, want 1 and 0", r["unscoped_http_n"], r["http_attempts"])
	}
}

// TestS1054_NetworkErrorAttemptIsClassifiedAndRetried: the first connection
// is dropped before any response (the shape of a connect timeout or a reset):
// plumbing's retry client retries it. The dial's first attempt is class
// network, the second a 200, and the dial counts one network error.
func TestS1054_NetworkErrorAttemptIsClassifiedAndRetried(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "1")
	t.Setenv("CLIENT_BASE_BACKOFF", "50ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "50ms")
	const bound = 20 * time.Millisecond
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close() // no response at all
			}
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		ctx, dial := StartDial(r.Context(), BranchExternalFetch, "GET", "")
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		resp, err := util.NewRetryClient(&http.Client{Transport: Transport(&http.Transport{})}).Do(req)
		if err == nil {
			resp.Body.Close()
		}
		dial.EndErr(r.Context(), err)
		time.Sleep(2 * bound)
	})
	r := buf.records(t, "slowcall.request")[0]
	if num(t, r, "dial_http_network_errors") != 1 || num(t, r, "dial_attempts") != 2 {
		t.Fatalf("NETWORK RED: dial_http_network_errors=%v dial_attempts=%v, want 1 and 2", r["dial_http_network_errors"], r["dial_attempts"])
	}
	if d, _ := r["dial_attempt_detail"].(string); !strings.HasPrefix(d, "1 connection ") || !strings.Contains(d, "; 2 200 ") {
		t.Fatalf("NETWORK RED: dial_attempt_detail = %q, want a connection-cause attempt then a 200", d)
	}
}

// TestS1054_AttemptCauseVocabulary pins the cause of an attempt (shared with
// the endpoint retry counter, #1088): which httptrace step failed when there
// is no response, else the status class.
func TestS1054_AttemptCauseVocabulary(t *testing.T) {
	boom := fmt.Errorf("boom")
	for _, c := range []struct {
		name                  string
		dnsErr, conn, connErr bool
		tlsErr                bool
		status                int64
		err                   error
		want                  string
	}{
		{"dns", true, false, false, false, 0, boom, "dns"},
		{"dial", false, true, true, false, 0, boom, "dial"},
		{"tls", false, true, false, true, 0, boom, "tls_handshake"},
		{"connection", false, true, false, false, 0, boom, "connection"},
		{"429", false, true, false, false, 429, nil, "status_429"},
		{"503", false, true, false, false, 503, nil, "status_5xx"},
		{"404", false, true, false, false, 404, nil, "status_4xx"},
		{"200", false, true, false, false, 200, nil, "ok"},
	} {
		var tr attemptTrace
		tr.dnsErr.Store(c.dnsErr)
		tr.connErr.Store(c.connErr)
		tr.tlsErr.Store(c.tlsErr)
		if c.conn {
			tr.connStart.Store(1)
		}
		if got := tr.errorClass(c.status, c.err); got != c.want {
			t.Errorf("CAUSE RED: %s = %q, want %q", c.name, got, c.want)
		}
	}
}

// --- the response path -----------------------------------------------------

// TestS1054_ResponseWriteAndCompressionAreSeparatePhases serves a slow,
// compressible response through the production order (Middleware → gzip →
// Body) to a gzip client: the record has a write phase AND a compress phase.
func TestS1054_ResponseWriteAndCompressionAreSeparatePhases(t *testing.T) {
	const bound = 30 * time.Millisecond
	m, buf := testMonitor(t, bound)
	body := bytes.Repeat([]byte(`{"name":"item","status":"ready","spec":{"replicas":3}},`), 20000)
	h := m.middleware(gzhttp.GzipHandler(Body(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * bound)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Pattern = "GET /call"
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/call", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := (&http.Transport{DisableCompression: true}).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("PRECONDITION: the response was not compressed")
	}
	recs := buf.records(t, "slowcall.request")
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	r := recs[0]
	if num(t, r, "write_n") < 1 || num(t, r, "compress_n") < 1 {
		t.Fatalf("RESPONSE RED: write_n=%v compress_n=%v, want both ≥ 1", r["write_n"], r["compress_n"])
	}
}

// --- CPU starvation ----------------------------------------------------------

func TestS1054_RecordAndSnapshotCarryCPUStarvation(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, buf := testMonitor(t, bound)
	m.capture = func() []byte { return syntheticProfile(1, 2) }
	var reads atomic.Int64
	base := time.Now().Add(-time.Second)
	m.cpu.read = func() cpuSample {
		if reads.Add(1) == 1 {
			return cpuSample{at: base, procOK: true, procNS: 10e9, cgOK: true, usageNS: 11e9, periods: 100, thrPer: 10, thrNS: 1e9}
		}
		return cpuSample{at: time.Now(), procOK: true, procNS: 16e9, cgOK: true, usageNS: 17e9, periods: 112, thrPer: 22, thrNS: 3500e6}
	}
	m.cpu.sample() // the sampler's reading before the request starts
	release := make(chan struct{})
	done := blockedRequest(t, m, "k", release)
	if !m.check(time.Now().Add(2 * bound)) {
		t.Fatalf("PRECONDITION: no snapshot")
	}
	time.Sleep(2 * bound)
	close(release)
	<-done
	for _, msg := range []string{"slowcall.request", "slowcall.goroutines"} {
		recs := buf.records(t, msg)
		if len(recs) != 1 {
			t.Fatalf("PRECONDITION: %d %s lines", len(recs), msg)
		}
		r := recs[0]
		for k, v := range map[string]int64{"cfs_throttled_periods": 12, "cfs_periods": 12, "cfs_throttled_ms": 2500, "process_cpu_ms": 6000, "cgroup_cpu_ms": 6000} {
			if got := num(t, r, k); got != v {
				t.Errorf("CPU RED: %s %s = %d, want %d", msg, k, got, v)
			}
		}
	}
}

// TestS1054_WatchdogStartsTheCPUSampler (re-review C, MC4): the process
// monitor's start is what starts the sampler; without it every record would
// silently lose its CPU fields.
func TestS1054_WatchdogStartsTheCPUSampler(t *testing.T) {
	m, _ := testMonitor(t, time.Hour) // the watchdog's tick is a quarter hour: it never checks here
	var reads atomic.Int64
	m.cpu.read = func() cpuSample { reads.Add(1); return cpuSample{at: time.Now()} }
	m.startWatchdog()
	deadline := time.Now().Add(5 * time.Second)
	for reads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if reads.Load() == 0 {
		t.Fatalf("SAMPLER RED: startWatchdog did not start the CPU sampler")
	}
	if _, _, ok := m.cpu.before(time.Now()); !ok {
		t.Fatalf("SAMPLER RED: the sampler holds no sample")
	}
}

func TestS1054_ParseCPUStatV1AndV2(t *testing.T) {
	u, p, tp, tn, ok := parseCPUStat([]byte("usage_usec 5000\nuser_usec 1\nsystem_usec 2\nnr_periods 40\nnr_throttled 7\nthrottled_usec 9000\n"))
	if !ok || u != 5e6 || p != 40 || tp != 7 || tn != 9e6 {
		t.Fatalf("v2: %d %d %d %d %v", u, p, tp, tn, ok)
	}
	_, p, tp, tn, ok = parseCPUStat([]byte("nr_periods 3\nnr_throttled 2\nthrottled_time 123456\n"))
	if !ok || p != 3 || tp != 2 || tn != 123456 {
		t.Fatalf("v1: %d %d %d %v", p, tp, tn, ok)
	}
}

// --- class -------------------------------------------------------------------

func TestS1054_BackgroundRequestIsCountedUnderItsClassAndTakesNoSnapshot(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, buf := testMonitor(t, bound)
	m.capture = func() []byte { return syntheticProfile(1, 2) }
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
			NoteClass(r.Context(), workclass.Refresh)
			close(started)
			<-release
		})
	}()
	<-started
	if m.check(time.Now().Add(2 * bound)) {
		t.Fatalf("CLASS RED: a background request took the snapshot")
	}
	if got := m.stats.suppressed.Load(); got != 0 {
		t.Fatalf("CLASS RED: a background request was counted suppressed")
	}
	// A customer request then does, and the background one is listed.
	cust := blockedRequest(t, m, "k", release)
	if !m.check(time.Now().Add(2 * bound)) {
		t.Fatalf("no snapshot for the customer request")
	}
	time.Sleep(2 * bound)
	close(release)
	<-done
	<-cust
	snap := buf.records(t, "slowcall.goroutines")[0]
	if reqs, _ := snap["requests"].(string); !strings.Contains(reqs, "class:refresh") || !strings.Contains(reqs, "class:customer") {
		t.Fatalf("the snapshot does not list both requests with their class: %q", reqs)
	}
	got := m.stats.counts()
	if got[RouteClass{"GET /call", "refresh"}] != 1 || got[RouteClass{"GET /call", "customer"}] != 1 {
		t.Fatalf("CLASS RED: snowplow_slow_requests_total = %v, want one refresh and one customer", got)
	}
}

// --- the snapshot ------------------------------------------------------------

// blockedRequest starts one tracked request that opens a flight wait and
// blocks until release is closed. It returns once the request is in flight.
func blockedRequest(t *testing.T, m *monitor, key string, release <-chan struct{}) <-chan struct{} {
	t.Helper()
	return blockedRequestOn(t, m, "GET /call", key, release)
}

func blockedRequestOn(t *testing.T, m *monitor, route, key string, release <-chan struct{}) <-chan struct{} {
	t.Helper()
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		serve(m, route, func(w http.ResponseWriter, r *http.Request) {
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

func TestS1054_SnapshotIsRateLimitedToOnePerProcessWindow(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, buf := testMonitor(t, bound)
	m.capture = func() []byte { return syntheticProfile(3, 4) }
	release := make(chan struct{})
	defer close(release)

	blockedRequest(t, m, "key-a", release)
	// After the request has started (its start is recorded), so the check
	// time is past the bound whatever the scheduler did.
	t0 := time.Now().Add(2 * bound)
	if !m.check(t0) {
		t.Fatalf("SNAPSHOT RED: no snapshot for a request in flight past the bound")
	}
	if m.check(t0.Add(time.Second)) {
		t.Fatalf("RATE RED: a second snapshot for the same request")
	}
	// A slow request on ANOTHER route inside the window: the window is the
	// process's, not the route's.
	blockedRequestOn(t, m, "POST /call/read", "key-b", release)
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

// TestReview_SuppressedCountsEveryUncoveredSlowRequest (reviewer, F3): with
// more slow requests in flight than a snapshot renders, a later one is still
// counted suppressed inside the window and snapshotted after it.
func TestReview_SuppressedCountsEveryUncoveredSlowRequest(t *testing.T) {
	const bound = 20 * time.Millisecond
	m, _ := testMonitor(t, bound)
	m.capture = func() []byte { return syntheticProfile(1, 2) }
	release := make(chan struct{})
	defer close(release)
	for i := 0; i < maxSnapshotRequests; i++ {
		blockedRequest(t, m, fmt.Sprintf("old-%d", i), release)
	}
	t0 := time.Now().Add(2 * bound)
	if !m.check(t0) {
		t.Fatalf("PRECONDITION: no first snapshot")
	}
	// A ninth request starts after the snapshot and crosses the bound inside
	// the window: no snapshot covers it.
	blockedRequest(t, m, "late", release)
	m.check(time.Now().Add(2 * bound))
	if got := m.stats.suppressed.Load(); got != 1 {
		t.Errorf("SUPPRESSED RED: suppressed=%d, want 1 (the ninth slow request, uncovered)", got)
	}
	// After the window, the ninth request is still uncovered and still slow.
	if !m.check(t0.Add(m.snapEvery + time.Second)) {
		t.Errorf("STARVATION RED: no snapshot after the window for an uncovered slow request (8 older covered requests hide it)")
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
	if got := num(t, r, "slow_inflight"); got != maxSnapshotRequests+3 {
		t.Fatalf("slow_inflight = %d, want every slow request (%d)", got, maxSnapshotRequests+3)
	}
	if got := num(t, r, "requests_omitted"); got != 3 {
		t.Fatalf("requests_omitted = %d, want 3", got)
	}
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
	if _, ok := r["capture_ms"].(float64); !ok {
		t.Fatalf("the snapshot does not report its own cost (capture_ms)")
	}
}

// TestS1054_SummaryCapsStacksPerClassAndClassifiesDeepStacks: small stacks
// (so the byte cap cannot hide it) are capped per class (the reviewer's MN),
// and a request stack deeper than the kept frames is still a request stack
// (the reviewer's ME: conn.serve is outermost).
func TestS1054_SummaryCapsStacksPerClassAndClassifiesDeepStacks(t *testing.T) {
	s := summarizeGoroutines(syntheticProfile(maxStacksPerClass+5, maxFramesPerStack+10), 1<<30)
	if s.kept != maxStacksPerClass || s.omitted != 5 {
		t.Fatalf("CAP RED: kept=%d omitted=%d, want %d and 5", s.kept, s.omitted, maxStacksPerClass)
	}
	if s.requestGoroutines != (maxStacksPerClass+5)*3 || s.backgroundGoroutines != 0 {
		t.Fatalf("CLASSIFY RED: request=%d background=%d: a deep request stack was not classified by its outermost frame", s.requestGoroutines, s.backgroundGoroutines)
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
	if r["wait_kind"] != "content_flight" || r["wait_outcome"] != "open" || r["wait_key"] != waitKeyLabel("real-key") {
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Pattern = "GET /call"
		m.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The request's goroutine carries the identity as a pprof label
			// next to an allowlisted one, waits on a key that names it (the
			// impersonation review's key shape), and blocks with it as an
			// argument.
			pprof.Do(r.Context(), pprof.Labels(workclass.Key, "customer", "user", user), func(ctx context.Context) {
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
	raw := goroutineProfile()
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

// BenchmarkS1054_ParallelRecord is the record under parallel requests (the
// reviewer's: the registry lock was the one contention point).
func BenchmarkS1054_ParallelRecord(b *testing.B) {
	empty := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	m := newMonitor(Bound, newStats())
	for _, c := range []struct {
		name string
		h    http.Handler
	}{{"baseline", empty}, {"record", m.middleware(empty)}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				req := httptest.NewRequest(http.MethodGet, "/call", nil)
				req.Pattern = "GET /call"
				w := httptest.NewRecorder()
				for pb.Next() {
					c.h.ServeHTTP(w, req)
				}
			})
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
