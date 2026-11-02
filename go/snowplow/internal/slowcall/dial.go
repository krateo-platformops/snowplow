package slowcall

// dial.go — one network call of a request, attempt by attempt (#1054 F1).
//
// A dial (StartDial) is opened around one logical call: an apiserver GET or
// LIST page, a discovery read, an external endpoint fetch, an access review.
// The retry loops that can stretch it run ABOVE the transport: client-go's
// rest.Request retries a 429 (and a 5xx carrying Retry-After) after sleeping
// Retry-After, and plumbing's util.NewRetryClient retries a 429 after its
// Retry-After or 2 s and a 5xx after a jittered 0.5–8 s backoff. Every
// attempt therefore crosses the RoundTripper, so Transport sees each one on
// the request's context and records, for the dial it belongs to: the attempt
// count, the last status, the 429 / 5xx / transport-error counts, and the
// time split into
//   - conn_wait: GetConn → GotConn, waiting for a pooled or new connection;
//   - server:    request written → first response byte (an apiserver's
//     Priority-and-Fairness queueing shows here);
//   - retry_wait: from one attempt's response to the next attempt's start,
//     the retry loop's sleep.
// So five 429s with 2 s sleeps read as attempts=6, http_429=5, retry_wait≈10 s,
// never as one slow call. A dial is a wait candidate: a 29 s dial can be the
// record's longest wait, and the longest dial is reported on its own.
//
// A dial names a verb and a GVR (or a kind for a non-resource call), never a
// path, a host or a query, which can carry names.

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

type dialKey struct{}

// dialState is one dial's attempts, written by Transport.
type dialState struct {
	t0                          time.Time
	attempts, lastStatus        atomic.Int64
	retryWait, connWait, server atomic.Int64
	n429, n5xx, errs            atomic.Int64
	lastEnd                     atomic.Int64 // ns after t0 the last attempt answered; 0 = none yet
}

// dialTotals are a request's attempts across all its dials, and the HTTP
// attempts it made outside any dial (unscoped: a clientconfig re-read, an
// object fetch).
type dialTotals struct {
	attempts, retryWait, connWait, server atomic.Int64
	n429, n5xx, errs                      atomic.Int64
	unscopedN, unscopedNS                 atomic.Int64
}

// StartDial opens one network call on ctx's request: kind names the target
// ("apiserver", "discovery", "external", "access_review"), verb the method or
// API verb, gvr the resource type when there is one. The returned context
// carries the dial to Transport: send the call with it. End the span with
// EndErr.
func StartDial(ctx context.Context, kind, verb, gvr string) (context.Context, Span) {
	s := Start(ctx, PhaseStageDial)
	if s.r == nil {
		return ctx, s
	}
	d := &dialState{t0: s.t0}
	s.wait, s.kind, s.verb, s.gvr, s.dial = true, kind, verb, gvr, d
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

type attemptTrace struct {
	getConn, wrote  atomic.Int64 // UnixNano
	connWait, first atomic.Int64 // ns
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
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { tr.getConn.Store(time.Now().UnixNano()) },
		GotConn: func(httptrace.GotConnInfo) {
			if g := tr.getConn.Load(); g != 0 {
				tr.connWait.Store(time.Now().UnixNano() - g)
			}
		},
		WroteRequest: func(httptrace.WroteRequestInfo) { tr.wrote.Store(time.Now().UnixNano()) },
		GotFirstResponseByte: func() {
			if w := tr.wrote.Load(); w != 0 {
				tr.first.Store(time.Now().UnixNano() - w)
			}
		},
	}
	start := time.Now()
	resp, err := a.next.RoundTrip(req.WithContext(context.WithValue(httptrace.WithClientTrace(ctx, trace), attemptMark{}, true)))
	end := time.Now()

	status := int64(0)
	if resp != nil {
		status = int64(resp.StatusCode)
	}
	t := &r.dials
	if d == nil {
		t.unscopedN.Add(1)
		t.unscopedNS.Add(int64(end.Sub(start)))
	} else {
		if last := d.lastEnd.Load(); last != 0 {
			gap := int64(start.Sub(d.t0)) - last
			d.retryWait.Add(gap)
			t.retryWait.Add(gap)
		}
		d.lastEnd.Store(int64(end.Sub(d.t0)))
		d.attempts.Add(1)
		d.lastStatus.Store(status)
		d.connWait.Add(tr.connWait.Load())
		d.server.Add(tr.first.Load())
		t.attempts.Add(1)
		t.connWait.Add(tr.connWait.Load())
		t.server.Add(tr.first.Load())
	}
	switch {
	case err != nil || resp == nil:
		t.errs.Add(1)
		if d != nil {
			d.errs.Add(1)
		}
	case status == http.StatusTooManyRequests:
		t.n429.Add(1)
		if d != nil {
			d.n429.Add(1)
		}
	case status >= 500:
		t.n5xx.Add(1)
		if d != nil {
			d.n5xx.Add(1)
		}
	}
	return resp, err
}
