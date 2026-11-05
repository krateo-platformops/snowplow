package slowcall

// The #1059 round-2 reviewer's arms (issuecomment-6598240482), adopted as written.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/gzhttp"
	"github.com/krateo-platformops/plumbing/http/util"
)

// hangFirstConnect is a real net.Dialer (so net/http/httptrace's
// ConnectStart/ConnectDone fire exactly as in production) whose FIRST connect
// never completes: its socket is created and it then waits until the dialer's
// own timeout, like a SYN nobody answers. Every later connect goes through.
// Every dial goes to addr whatever the URL's host says.
func hangFirstConnect(timeout time.Duration, addr string) (*http.Transport, *atomic.Int64) {
	var n atomic.Int64
	d := &net.Dialer{Timeout: timeout, ControlContext: func(ctx context.Context, _, _ string, _ syscall.RawConn) error {
		if n.Add(1) == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}}
	return &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return d.DialContext(ctx, network, addr)
	}}, &n
}

var connectRE = regexp.MustCompile(`^1 (\S+) reused:(\S+) dns:(\d+) connect:(\d+) tls:(\d+) ttfb:(\d+) ms:(\d+); 2 (\S+) reused:(\S+) dns:\d+ connect:(\d+) tls:\d+ ttfb:\d+ ms:\d+$`)

const r2Secret = "zq7xtenant"

// TestReview2_Run20Shape_ConnectTimeoutThenRetry reproduces run 20's 30.6 s
// page, scaled: one stage dial to https://kubernetes.default.svc through
// plumbing's RetryClient (default backoff), whose first TCP connect hangs
// until the dialer's connect timeout and whose retry succeeds. The record
// must say: one dial, 2 attempts, the first cause `dial` with connect ≈ the
// timeout, the retry sleep, the second a 200; the dial is the longest wait;
// no URL, path, host or query reaches the log or the ring.
func TestReview2_Run20Shape_ConnectTimeoutThenRetry(t *testing.T) {
	const bound = 100 * time.Millisecond
	const connectTimeout = time.Second
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"Status"}`))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()
	tr, dials := hangFirstConnect(connectTimeout, addr)
	m, buf := testMonitor(t, bound)
	var fetchErr error
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		short := StartWait(r.Context(), PhaseFlightWait, "content_flight", "k", "")
		time.Sleep(bound / 2)
		short.End()
		ctx, dial := StartDial(r.Context(), BranchExternalFetch, "GET", "")
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			"http://kubernetes.default.svc/api/v1/namespaces/"+r2Secret+"/secrets/"+r2Secret+"-clientconfig?fieldSelector=metadata.name%3D"+r2Secret, nil)
		resp, err := util.NewRetryClient(&http.Client{Transport: Transport(tr)}).Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		fetchErr = err
		dial.EndErr(r.Context(), err)
	})
	if fetchErr != nil || dials.Load() != 2 {
		t.Fatalf("PRECONDITION: fetch err=%v dials=%d, want nil and 2", fetchErr, dials.Load())
	}
	recs := buf.records(t, "slowcall.request")
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: %d records", len(recs))
	}
	r := recs[0]
	t.Logf("record: %v", r)
	for k, v := range map[string]string{"wait_kind": "dial", "wait_phase": "stage_dial", "wait_branch": "external_fetch",
		"wait_outcome": "completed", "wait_last_cause": "ok", "phase": "stage_dial",
		"wait_host": hostDigest("kubernetes.default.svc"), "dial_host": hostDigest("kubernetes.default.svc")} {
		if r[k] != v {
			t.Errorf("RUN20 RED: %s = %v, want %q", k, r[k], v)
		}
	}
	for k, v := range map[string]int64{"wait_attempts": 2, "dial_attempts": 2, "wait_http_network_errors": 1, "wait_status": 200,
		"http_attempts": 2, "http_network_errors": 1, "wait_reused": 0} {
		if got := num(t, r, k); got != v {
			t.Errorf("RUN20 RED: %s = %d, want %d", k, got, v)
		}
	}
	lo, hi := connectTimeout.Milliseconds()*9/10, connectTimeout.Milliseconds()+700
	if got := num(t, r, "wait_connect_ms"); got < lo || got > hi {
		t.Errorf("RUN20 RED: wait_connect_ms = %d, want ≈ the %v connect timeout", got, connectTimeout)
	}
	// plumbing's first network-error backoff: 50–100 % of 500 ms.
	if got := num(t, r, "wait_retry_wait_ms"); got < 240 || got > 900 {
		t.Errorf("RUN20 RED: wait_retry_wait_ms = %d, want the retry client's 250–500 ms backoff", got)
	}
	if got := num(t, r, "wait_ms"); got < connectTimeout.Milliseconds()+240 {
		t.Errorf("RUN20 RED: wait_ms = %d, want ≥ connect timeout + backoff", got)
	}
	d, _ := r["wait_attempt_detail"].(string)
	mm := connectRE.FindStringSubmatch(d)
	if mm == nil {
		t.Fatalf("RUN20 RED: wait_attempt_detail = %q, want two attempts", d)
	}
	if mm[1] != "dial" || mm[2] != "false" {
		t.Errorf("RUN20 RED: attempt 1 = cause %q reused %q, want dial / false (detail %q)", mm[1], mm[2], d)
	}
	if c, _ := strconv.ParseInt(mm[4], 10, 64); c < lo || c > hi {
		t.Errorf("RUN20 RED: attempt 1 connect = %d ms, want ≈ %v (detail %q)", c, connectTimeout, d)
	}
	if ttfb, _ := strconv.ParseInt(mm[6], 10, 64); ttfb != 0 {
		t.Errorf("RUN20 RED: attempt 1 ttfb = %d, want 0 (no response)", ttfb)
	}
	if mm[8] != "200" || mm[9] != "false" {
		t.Errorf("RUN20 RED: attempt 2 = %q reused %q, want 200 on a fresh connection (detail %q)", mm[8], mm[9], d)
	}
	ring, _ := json.Marshal(m.stats.ring(&m.stats.recent))
	_, port, _ := net.SplitHostPort(addr)
	for _, s := range []string{buf.String(), string(ring)} {
		for _, leak := range []string{r2Secret, "kubernetes", "default.svc", "127.0.0.1", ":" + port, "fieldSelector", "/api/v1", "secrets"} {
			if strings.Contains(s, leak) {
				t.Errorf("LEAK RED: %q reached the log or the ring", leak)
			}
		}
	}
}

// TestReview2_AttemptAbandonedMidConnect: the caller's context ends while the
// attempt is still connecting (a cut scope, a client that left, a stage
// deadline shorter than the 30 s connect timeout). The attempt was in its
// connect: its cause should be `dial` and its connect ≈ the time it spent.
func TestReview2_AttemptAbandonedMidConnect(t *testing.T) {
	const bound = 50 * time.Millisecond
	const cut = 400 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()
	tr, _ := hangFirstConnect(5*time.Second, srv.Listener.Addr().String())
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		cctx, cancel := context.WithTimeout(r.Context(), cut)
		defer cancel()
		ctx, dial := StartDial(cctx, BranchExternalFetch, "GET", "")
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://kubernetes.default.svc/x", nil)
		resp, err := util.NewRetryClient(&http.Client{Transport: Transport(tr)}).Do(req)
		if err == nil {
			resp.Body.Close()
		}
		dial.EndErr(cctx, err)
	})
	r := buf.records(t, "slowcall.request")[0]
	t.Logf("record: %v", r)
	if r["wait_outcome"] != "timeout" {
		t.Errorf("wait_outcome = %v, want timeout", r["wait_outcome"])
	}
	d, _ := r["wait_attempt_detail"].(string)
	if !strings.HasPrefix(d, "1 dial ") {
		t.Errorf("MIDCONNECT RED: attempt detail %q, want cause dial (the attempt was connecting when it was abandoned)", d)
	}
	if got := num(t, r, "wait_connect_ms"); got < cut.Milliseconds()*8/10 {
		t.Errorf("MIDCONNECT RED: wait_connect_ms = %d, want ≈ %v (the time spent connecting)", got, cut)
	}
}

// TestReview2_SnapshotDuringHungConnectShowsTheOpenDial: the watchdog fires
// while the attempt is still connecting. The snapshot names the open dial.
func TestReview2_SnapshotDuringHungConnectShowsTheOpenDial(t *testing.T) {
	const bound = 100 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()
	tr, _ := hangFirstConnect(800*time.Millisecond, srv.Listener.Addr().String())
	m, buf := testMonitor(t, bound)
	m.capture = func() []byte { return syntheticProfile(1, 2) }
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
			ctx, dial := StartDial(r.Context(), BranchExternalFetch, "GET", "")
			close(started)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://kubernetes.default.svc/x", nil)
			resp, err := util.NewRetryClient(&http.Client{Transport: Transport(tr)}).Do(req)
			if err == nil {
				resp.Body.Close()
			}
			dial.EndErr(r.Context(), err)
		})
	}()
	<-started
	time.Sleep(3 * bound)
	if !m.check(time.Now()) {
		t.Fatalf("PRECONDITION: no snapshot")
	}
	<-done
	s := buf.records(t, "slowcall.goroutines")[0]
	t.Logf("snapshot: wait_kind=%v wait_attempts=%v wait_ms=%v requests=%q", s["wait_kind"], s["wait_attempts"], s["wait_ms"], s["requests"])
	if s["wait_kind"] != "dial" || s["wait_outcome"] != "open" {
		t.Errorf("SNAPSHOT RED: wait_kind=%v outcome=%v, want an open dial", s["wait_kind"], s["wait_outcome"])
	}
}

// TestReview2_CgroupV1ReportsNoCgroupCPUInsteadOfZero: a v1 cpu.stat has no
// usage (it lives in cpuacct). cgroup_cpu_ms must then be absent
// (UNMEASURED), never 0.
func TestReview2_CgroupV1ReportsNoCgroupCPUInsteadOfZero(t *testing.T) {
	v1 := [][]byte{[]byte("nr_periods 100\nnr_throttled 3\nthrottled_time 5000000\n"), []byte("nr_periods 160\nnr_throttled 9\nthrottled_time 95000000\n")}
	c := &cpuSampler{}
	i := 0
	c.read = func() cpuSample {
		s := cpuSample{at: time.Now()}
		s.usageNS, s.periods, s.thrPer, s.thrNS, s.cgOK = parseCPUStat(v1[min(i, 1)])
		i++
		return s
	}
	c.sample()
	got := attrsMap(c.windowAttrs(time.Now().Add(time.Second)))
	t.Logf("v1 window: %v", got)
	if got["cfs_throttled_periods"] != int64(6) || got["cfs_throttled_ms"] != int64(90) {
		t.Errorf("v1 throttling wrong: %v", got)
	}
	if v, ok := got["cgroup_cpu_ms"]; ok {
		t.Errorf("V1 RED: cgroup_cpu_ms = %v reported on cgroup v1, where cpu.stat carries no usage: want absent (UNMEASURED)", v)
	}
}

// TestReview2_CgroupV2RealFile: a real cgroup v2 cpu.stat (kernel 6.x, as
// GKE COS writes it, core_sched and burst lines included) parses into the
// right deltas; a v2 file with no cpu controller (no nr_periods) and no
// cgroup at all leave every cgroup field absent, never 0.
func TestReview2_CgroupV2RealFile(t *testing.T) {
	a := []byte("usage_usec 81234567\nuser_usec 70000000\nsystem_usec 11234567\ncore_sched.force_idle_usec 0\nnr_periods 41000\nnr_throttled 1200\nthrottled_usec 9876543\nnr_bursts 0\nburst_usec 0\n")
	b := []byte("usage_usec 85234567\nuser_usec 73000000\nsystem_usec 12234567\ncore_sched.force_idle_usec 0\nnr_periods 41010\nnr_throttled 1207\nthrottled_usec 10276543\nnr_bursts 0\nburst_usec 0\n")
	for _, c := range []struct {
		name string
		from []byte
		to   []byte
		want map[string]any
	}{
		{"v2", a, b, map[string]any{"cgroup_cpu_ms": int64(4000), "cfs_periods": int64(10), "cfs_throttled_periods": int64(7), "cfs_throttled_ms": int64(400)}},
		{"v2-no-cpu-controller", []byte("usage_usec 1\nuser_usec 1\nsystem_usec 0\n"), []byte("usage_usec 9000\nuser_usec 1\nsystem_usec 0\n"), nil},
		{"none", nil, nil, nil},
	} {
		files := [][]byte{c.from, c.to}
		i := 0
		s := &cpuSampler{}
		s.read = func() cpuSample {
			x := cpuSample{at: time.Now()}
			if f := files[min(i, 1)]; f != nil {
				x.usageNS, x.periods, x.thrPer, x.thrNS, x.cgOK = parseCPUStat(f)
			}
			i++
			return x
		}
		s.sample()
		got := attrsMap(s.windowAttrs(time.Now().Add(time.Second)))
		for _, k := range []string{"cgroup_cpu_ms", "cfs_periods", "cfs_throttled_periods", "cfs_throttled_ms"} {
			if c.want == nil {
				if v, ok := got[k]; ok {
					t.Errorf("%s: %s = %v, want absent (UNMEASURED)", c.name, k, v)
				}
				continue
			}
			if got[k] != c.want[k] {
				t.Errorf("%s: %s = %v, want %v", c.name, k, got[k], c.want[k])
			}
		}
	}
}

func attrsMap(as []slog.Attr) map[string]any {
	out := map[string]any{}
	for _, a := range as {
		out[a.Key] = a.Value.Any()
	}
	return out
}

// TestReview2_WritersAreByteAndHeaderTransparent (CodeQL go/reflected-xss at
// writer.go:37/:77): the same handler, served bare through gzip and through
// the production order (Middleware → gzip → Body), gives byte-identical
// bodies, the same status and the same headers, for a handler that pins
// application/json + nosniff with a reflected `<script>` and for one that
// sets no Content-Type (net/http's own sniffing decides, as before).
func TestReview2_WritersAreByteAndHeaderTransparent(t *testing.T) {
	m, _ := testMonitor(t, time.Hour)
	for _, c := range []struct {
		name string
		h    http.HandlerFunc
	}{
		{"pinned-json", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"` + r.URL.Query().Get("fields") + `"}`))
		}},
		{"unset", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(r.URL.Query().Get("fields")))
		}},
	} {
		for _, gz := range []bool{false, true} {
			serveVia := func(h http.Handler) (int, http.Header, []byte) {
				req := httptest.NewRequest(http.MethodGet, "/export?fields="+"%3Cscript%3Ealert(1)%3C%2Fscript%3E"+strings.Repeat("x", 2000), nil)
				req.Pattern = "GET /export"
				if gz {
					req.Header.Set("Accept-Encoding", "gzip")
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				body := rec.Body.Bytes()
				if rec.Header().Get("Content-Encoding") == "gzip" {
					zr, err := gzip.NewReader(bytes.NewReader(body))
					if err != nil {
						t.Fatal(err)
					}
					body, _ = io.ReadAll(zr)
				}
				return rec.Code, rec.Header(), body
			}
			bc, bh, bb := serveVia(gzhttp.GzipHandler(c.h))
			wc, wh, wb := serveVia(m.middleware(gzhttp.GzipHandler(Body(c.h))))
			if bc != wc || !bytes.Equal(bb, wb) {
				t.Errorf("%s gz=%v: wrapped status/body differ: %d %q vs %d %q", c.name, gz, bc, bb[:40], wc, wb[:40])
			}
			for _, k := range []string{"Content-Type", "X-Content-Type-Options", "Content-Encoding", "Vary", "Content-Length"} {
				if bh.Get(k) != wh.Get(k) {
					t.Errorf("%s gz=%v: header %s differs: %q vs %q", c.name, gz, k, bh.Get(k), wh.Get(k))
				}
			}
			if len(bh) != len(wh) {
				t.Errorf("%s gz=%v: header sets differ: %v vs %v", c.name, gz, bh, wh)
			}
			t.Logf("%s gz=%v: status %d Content-Type %q nosniff %q identical", c.name, gz, wc, wh.Get("Content-Type"), wh.Get("X-Content-Type-Options"))
		}
	}
}

// BenchmarkReview2_SamplerRead is one CPU-sampler tick on this host
// (getrusage + the cgroup file probes) and the parse of a real v2 file.
func BenchmarkReview2_SamplerRead(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = readCPU()
	}
}

func BenchmarkReview2_ParseV2(b *testing.B) {
	f := []byte("usage_usec 81234567\nuser_usec 70000000\nsystem_usec 11234567\ncore_sched.force_idle_usec 0\nnr_periods 41000\nnr_throttled 1200\nthrottled_usec 9876543\nnr_bursts 0\nburst_usec 0\n")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _ = parseCPUStat(f)
	}
}

// TestReview2_LongestDialNotLastDial: two dials, the long one first; the
// record's dial_* names the long one (verb distinguishes them).
func TestReview2_LongestDialNotLastDial(t *testing.T) {
	const bound = 50 * time.Millisecond
	m, buf := testMonitor(t, bound)
	serve(m, "GET /call", func(w http.ResponseWriter, r *http.Request) {
		_, a := StartDial(r.Context(), BranchInternalRESTConfig, "list", "g/v1, Resource=long")
		time.Sleep(4 * bound)
		a.End()
		_, b := StartDial(r.Context(), BranchDiscovery, "get", "")
		time.Sleep(bound / 5)
		b.End()
	})
	r := buf.records(t, "slowcall.request")[0]
	if r["dial_verb"] != "list" || r["dial_branch"] != "internal_rest_config" || r["wait_verb"] != "list" {
		t.Errorf("LONGEST-DIAL RED: dial_verb=%v dial_branch=%v wait_verb=%v, want the long list dial", r["dial_verb"], r["dial_branch"], r["wait_verb"])
	}
}
