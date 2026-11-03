package slowcall

// dial.go — one network call of a request, attempt by attempt (#1054 F1).
//
// A dial (StartDial) is opened around one logical fetch of one dispatch
// branch: an apiserver GET or LIST page through the internal rest config, a
// discovery read, an external endpoint fetch, an access review, a /call
// passthrough. The retry loops that can stretch it run ABOVE the transport:
// client-go's rest.Request retries a 429 (and a 5xx carrying Retry-After)
// after sleeping Retry-After, and plumbing's util.NewRetryClient retries a
// network error or a 5xx after a jittered 0.5–8 s backoff and a 429 after
// Retry-After or 2 s. Every attempt therefore crosses the RoundTripper, so
// Transport sees each one on the request's context and records, per ATTEMPT
// (net/http/httptrace):
//   - reused:  whether the connection came from the pool (a new transport per
//     fetch never reuses: every attempt pays a fresh connect);
//   - dns, connect, tls: the time to resolve, to connect (a 30 s connect
//     timeout shows here, not as server time) and to handshake;
//   - ttfb:    request written → first response byte (an apiserver's
//     Priority-and-Fairness queueing shows here);
//   - the cause: dns (resolution failed, no connect), dial (the connect
//     failed or timed out), tls_handshake,
//     connection (no response on an established connection), status_429,
//     status_5xx, status_4xx, ok (the endpoint retry counter's vocabulary);
// and per FETCH: the attempt count, the retry wait (from one attempt's end to
// the next attempt's start: the retry loop's sleep), the dispatch branch and
// the target host as a sha256 digest. So a 30 s connect timeout and a retry
// reads as attempts=2, a first attempt of cause dial with connect≈30 s,
// and the retry wait; never as one opaque slow call. A dial is a wait
// candidate: a 29 s dial can be the record's longest wait, and the longest
// dial is reported on its own.
//
// A dial names a verb, a GVR when it has one and its host's digest, never a
// URL, a path, a host or a query, which can carry names.

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type dialKey struct{}

// maxAttemptDetail is how many attempts of one dial are kept one by one; the
// counters cover every attempt.
const maxAttemptDetail = 8

// attemptInfo is one attempt of a dial, as reported.
type attemptInfo struct {
	reused                       bool
	dns, connect, tls, ttfb, dur int64 // ns
	status                       int64
	class                        string
}

// dialState is one dial's attempts, written by Transport.
type dialState struct {
	t0                        time.Time
	attempts, lastStatus      atomic.Int64
	retryWait, connWait, ttfb atomic.Int64
	dns, connect, tls, reused atomic.Int64
	n429, n5xx, nNet          atomic.Int64
	lastEnd                   atomic.Int64 // ns after t0 the last attempt answered; 0 = none yet
	mu                        sync.Mutex
	host                      string // raw: rendered only as its sha256 digest
	lastClass                 string
	detail                    []attemptInfo
}

// dialTotals are a request's attempts across all its dials, and the HTTP
// attempts it made outside any dial (unscoped: an object fetch, a
// clientconfig re-read).
type dialTotals struct {
	attempts, retryWait, connWait, ttfb, connect atomic.Int64
	n429, n5xx, nNet, reused                     atomic.Int64
	unscopedN, unscopedNS                        atomic.Int64
}

// The dispatch branches a dial is opened for.
const (
	BranchInternalRESTConfig = "internal_rest_config" // an apiserver read through the internal rest config
	BranchDiscovery          = "discovery"
	BranchExternalFetch      = "external_fetch" // a stage's endpoint (plumbing's retry client)
	BranchAccessReview       = "access_review"
	BranchPassthrough        = "passthrough" // a /call with no resolver (credhttp.Do)
)

// StartDial opens one network fetch on ctx's request: branch names the
// dispatch branch (Branch*), verb the method or API verb, gvr the resource
// type when there is one. The returned context carries the dial to
// Transport: send the fetch with it. End the span with EndErr.
func StartDial(ctx context.Context, branch, verb, gvr string) (context.Context, Span) {
	s := Start(ctx, PhaseStageDial)
	if s.r == nil {
		return ctx, s
	}
	d := &dialState{t0: s.t0}
	s.wait, s.kind, s.branch, s.verb, s.gvr, s.dial = true, "dial", branch, verb, gvr, d
	s.slot = s.r.openWait(&s)
	return context.WithValue(ctx, dialKey{}, d), s
}

// EndErr closes a dial (or any wait) by the outcome of the work: completed,
// the caller's context's end, or failed.
func (s Span) EndErr(ctx context.Context, err error) {
	switch {
	case err == nil:
		s.End()
	case ctx != nil && ctx.Err() != nil:
		s.EndRelease(CauseOf(ctx.Err()))
	default:
		s.EndRelease(ReleaseFailed)
	}
}

// Transport records every HTTP attempt made on a tracked request's context
// (see the file comment). On any other context it is one ctx.Value lookup.
// It wraps a transport: nil returns nil (the caller's own default applies;
// credhttp resolves it before wrapping, since only credhttp may name the
// default transport).
func Transport(next http.RoundTripper) http.RoundTripper {
	if next == nil {
		return nil
	}
	if _, ok := next.(attemptRT); ok {
		return next
	}
	return attemptRT{next: next}
}

type attemptRT struct{ next http.RoundTripper }

type attemptMark struct{}

// WrappedRoundTripper lets client-go/utilnet and credhttp walk the chain.
func (a attemptRT) WrappedRoundTripper() http.RoundTripper { return a.next }

// attemptTrace is one attempt's httptrace marks (UnixNano) and spans (ns).
// The hooks may run on the transport's goroutines: atomics.
type attemptTrace struct {
	getConn, dnsStart, connStart, tlsStart, wrote atomic.Int64
	connWait, dns, connect, tls, first            atomic.Int64
	reused                                        atomic.Bool
	dnsErr, connErr, tlsErr                       atomic.Bool
}

func since(mark *atomic.Int64) int64 {
	if m := mark.Load(); m != 0 {
		return time.Now().UnixNano() - m
	}
	return 0
}

func stamp(mark *atomic.Int64) { mark.Store(time.Now().UnixNano()) }

func (tr *attemptTrace) hooks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(string) { stamp(&tr.getConn) },
		GotConn: func(i httptrace.GotConnInfo) {
			tr.reused.Store(i.Reused)
			tr.connWait.Store(since(&tr.getConn))
		},
		DNSStart: func(httptrace.DNSStartInfo) { stamp(&tr.dnsStart) },
		DNSDone: func(i httptrace.DNSDoneInfo) {
			tr.dns.Add(since(&tr.dnsStart))
			if i.Err != nil {
				tr.dnsErr.Store(true)
			}
		},
		ConnectStart: func(string, string) { stamp(&tr.connStart) },
		ConnectDone: func(_, _ string, err error) {
			tr.connect.Add(since(&tr.connStart))
			if err != nil {
				tr.connErr.Store(true)
			}
		},
		TLSHandshakeStart: func() { stamp(&tr.tlsStart) },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			tr.tls.Add(since(&tr.tlsStart))
			if err != nil {
				tr.tlsErr.Store(true)
			}
		},
		WroteRequest:         func(httptrace.WroteRequestInfo) { stamp(&tr.wrote) },
		GotFirstResponseByte: func() { tr.first.Store(since(&tr.wrote)) },
	}
}

// errorClass is an attempt's cause, in the vocabulary the endpoint retry
// counter uses: an attempt with no response failed to dial (the connect
// itself, a connect timeout), in the TLS handshake, or on the connection
// after it was up; one with a response is its status class.
func (tr *attemptTrace) errorClass(status int64, err error) string {
	switch {
	case err != nil || status == 0:
		switch {
		case tr.dnsErr.Load() && tr.connStart.Load() == 0:
			return "dns"
		case tr.connErr.Load():
			return "dial"
		case tr.tlsErr.Load():
			return "tls_handshake"
		}
		return "connection"
	case status == http.StatusTooManyRequests:
		return "status_429"
	case status >= 500:
		return "status_5xx"
	case status >= 400:
		return "status_4xx"
	}
	return "ok"
}

// networkClass reports a cause with no response.
func networkClass(c string) bool {
	return c == "dns" || c == "dial" || c == "tls_handshake" || c == "connection"
}

func (a attemptRT) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	r := fromContext(ctx)
	// A chain may carry this transport twice (a credhttp-hardened client a
	// caller wraps again): the outermost records the attempt and marks it, the
	// others pass it through.
	if r == nil || r.closed.Load() || ctx.Value(attemptMark{}) != nil {
		return a.next.RoundTrip(req)
	}
	d, _ := ctx.Value(dialKey{}).(*dialState)
	var tr attemptTrace
	start := time.Now()
	resp, err := a.next.RoundTrip(req.WithContext(context.WithValue(httptrace.WithClientTrace(ctx, tr.hooks()), attemptMark{}, true)))
	end := time.Now()

	status := int64(0)
	if resp != nil {
		status = int64(resp.StatusCode)
	}
	class := tr.errorClass(status, err)
	t := &r.dials
	if d == nil {
		t.unscopedN.Add(1)
		t.unscopedNS.Add(int64(end.Sub(start)))
		return resp, err
	}
	if last := d.lastEnd.Load(); last != 0 {
		gap := int64(start.Sub(d.t0)) - last
		d.retryWait.Add(gap)
		t.retryWait.Add(gap)
	}
	d.lastEnd.Store(int64(end.Sub(d.t0)))
	a1 := attemptInfo{reused: tr.reused.Load(), dns: tr.dns.Load(), connect: tr.connect.Load(), tls: tr.tls.Load(),
		ttfb: tr.first.Load(), dur: int64(end.Sub(start)), status: status, class: class}
	d.attempts.Add(1)
	d.lastStatus.Store(status)
	d.connWait.Add(tr.connWait.Load())
	d.ttfb.Add(a1.ttfb)
	d.dns.Add(a1.dns)
	d.connect.Add(a1.connect)
	d.tls.Add(a1.tls)
	t.attempts.Add(1)
	t.connWait.Add(tr.connWait.Load())
	t.ttfb.Add(a1.ttfb)
	t.connect.Add(a1.connect)
	if a1.reused {
		d.reused.Add(1)
		t.reused.Add(1)
	}
	switch {
	case networkClass(class):
		d.nNet.Add(1)
		t.nNet.Add(1)
	case class == "status_429":
		d.n429.Add(1)
		t.n429.Add(1)
	case class == "status_5xx":
		d.n5xx.Add(1)
		t.n5xx.Add(1)
	}
	d.mu.Lock()
	if d.host == "" {
		d.host = req.URL.Host
	}
	d.lastClass = class
	if len(d.detail) < maxAttemptDetail {
		d.detail = append(d.detail, a1)
	}
	d.mu.Unlock()
	return resp, err
}

// hostDigest is a host's reported form: sha256 (16 hex), stable across pods
// so the same endpoint correlates everywhere, and never the host itself.
func hostDigest(host string) string {
	if host == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.ToLower(host)))
	return "sha256:" + hex.EncodeToString(sum[:8])
}

// attemptDetail renders a dial's attempts, one per "; ": the attempt number,
// its error class or status, then reused / dns / connect / tls / ttfb / total
// in ms.
func (d *dialState) attemptDetail() (string, string, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var b strings.Builder
	for i, a := range d.detail {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(strconv.Itoa(i + 1))
		b.WriteString(" ")
		if a.status != 0 {
			b.WriteString(strconv.FormatInt(a.status, 10))
		} else {
			b.WriteString(a.class)
		}
		b.WriteString(" reused:")
		b.WriteString(strconv.FormatBool(a.reused))
		for _, f := range []struct {
			k string
			v int64
		}{{"dns", a.dns}, {"connect", a.connect}, {"tls", a.tls}, {"ttfb", a.ttfb}, {"ms", a.dur}} {
			b.WriteString(" ")
			b.WriteString(f.k)
			b.WriteString(":")
			b.WriteString(strconv.FormatInt(ms(f.v), 10))
		}
	}
	return b.String(), hostDigest(d.host), d.lastClass
}
