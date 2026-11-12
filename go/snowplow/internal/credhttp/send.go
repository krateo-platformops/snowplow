package credhttp

// send.go — #1088: the one send policy for an endpoint dial (the RESTAction
// branch-E stage fetch and the /call passthrough, credhttp.Do).
//
// A WRITE IS SENT ONCE. plumbing's util.RetryClient (v1.28.1 http/util/retry.go)
// re-sends GET, HEAD, OPTIONS, PUT and DELETE on a network error, a 429 or a
// 5xx. For PUT and DELETE that is a re-send after the server may already have
// received the request: measured against a server that reads the whole request
// and then drops the connection, a DELETE with no body reached it 6 times (the
// delete that succeeded is re-sent and answered 404), and a PUT with a body was
// re-sent with its body already drained (req.Clone does not rewind Body), so the
// re-send failed locally after the full backoff and the server's real answer was
// replaced by "ContentLength=N with Body length 0". So a request whose method is
// not safe (SafeMethod) gets exactly one attempt, whatever went wrong. POST and
// PATCH already did; PUT and DELETE now do too. This is #905's "a write is sent
// exactly once", kept at the transport.
//
// A SAFE REQUEST keeps the RetryClient, built per send as before: a RetryClient
// kept across sends would switch on its 30 QPS token bucket, and #920 rules out
// a client-side limit.
//
// EVERY RETRY IS COUNTED WITH ITS CAUSE. An attempt observer sits under the
// retry loop (each RetryClient attempt is one Client.Do, so one observer
// RoundTrip that is not a redirect hop). It classifies every attempt's outcome
// from net/http/httptrace, and each attempt after the first counts
// snowplow_endpoint_retries_total{site,cause} and logs one WARN naming the cause
// of the attempt it replaces. The cause strings and the host digest are the ones
// #1059's per-attempt slow-call fields use, so the two records join on them.
// #1059 keeps per-attempt timings in the slow-request record; this is the
// process-level count, so nothing is recorded twice.
//
// A REFUSED DESTINATION IS NEVER RETRIED (#1131). The answer cannot change by
// asking again, and each retry would hold a worker for a backoff that ends in the
// same refusal.

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/util"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// Endpoint dial sites (closed set).
const (
	SendSiteStage = "stage" // the RESTAction branch-E stage fetch
	SendSiteCall  = "call"  // the /call raw passthrough (credhttp.Do)
)

// Retry causes (closed set): what ended the attempt a retry replaces.
const (
	RetryCauseDNS          = "dns"           // name resolution failed, no connect
	RetryCauseDial         = "dial"          // the TCP connect failed
	RetryCauseTLSHandshake = "tls_handshake" // the TLS handshake failed
	RetryCauseConnection   = "connection"    // any other error with no response
	RetryCause429          = "status_429"
	RetryCause5xx          = "status_5xx"
	// RetryCauseOther: a retry after an attempt that answered with neither an
	// error, a 429 nor a 5xx. The RetryClient never does that; the cell exists
	// so a retry is counted even if it ever did.
	RetryCauseOther = "other"
)

// SendSites and RetryCauses are the closed label sets, in publication order.
var (
	SendSites   = []string{SendSiteStage, SendSiteCall}
	RetryCauses = []string{RetryCauseDNS, RetryCauseDial, RetryCauseTLSHandshake, RetryCauseConnection, RetryCause429, RetryCause5xx, RetryCauseOther}
)

// SafeMethod reports whether an HTTP method (empty = GET) cannot change state
// on the server: GET, HEAD, OPTIONS, TRACE, in any case. It is the resolver's
// write classifier too (isSafeMethod), so "is this a write" has one answer.
func SafeMethod(method string) bool {
	switch strings.ToUpper(method) {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	return false
}

// Send sends req on cli under the endpoint send policy (see the file comment):
// no redirect hop leaves req's origin (#1106); a write is sent once; a safe
// request goes through plumbing's RetryClient with every retry counted and
// logged with its cause. cli is not modified.
//
// #1115 — a request that carries a customer page budget (pagebudget) is sent
// inside it instead: sendBudgeted (safe methods) or sendOnceBudgeted (writes).
// A request without one (background work) keeps the path below, unchanged.
//
// #847 — every connection this request takes counts under site, on every path
// below, a write's included (connections.go).
//
// #1298 — what it returned is recorded on the request's Answer (answer.go): the
// final status line, or why there is none. Both dials record it here, so
// neither can report a code it made up as the authority's.
func Send(site string, cli *http.Client, req *http.Request) (*http.Response, error) {
	resp, err := sendPolicy(site, cli, req)
	AnswerFrom(req.Context()).observe(resp, err)
	return resp, err
}

// sendPolicy is Send without the observation.
func sendPolicy(site string, cli *http.Client, req *http.Request) (*http.Response, error) {
	req = req.WithContext(countConnections(req, site))
	// #1106 — no redirect hop leaves the origin this request is sent to,
	// whatever the client (endpoint_redirect.go). Outermost, so nothing inside
	// cli (a wire dump, a plumbing-built chain) can displace it; the budgeted
	// paths below send on the same wrapped client.
	cli = SameOriginOnly(cli, Origin(req.URL))
	if b, ok := pagebudget.From(req.Context()); ok {
		if !SafeMethod(req.Method) {
			return sendOnceBudgeted(cli, req, b)
		}
		return sendBudgeted(site, cli, req, b)
	}
	if !SafeMethod(req.Method) {
		return cli.Do(req)
	}
	observed := *cli
	policy := util.NewRetryClient(&observed)
	observed.Transport = &attemptObserver{next: cli.Transport, site: site, policy: policy}
	return policy.Do(req)
}

// sendOnceBudgeted sends a write once inside its budget (#1115 §5). A write
// whose budget is already spent is never dialled (not_sent). Otherwise its one
// attempt ends by the deadline; a write whose request was written is reported
// as possibly applied, and is never re-sent (#1088, #905).
func sendOnceBudgeted(cli *http.Client, req *http.Request, b *pagebudget.Budget) (*http.Response, error) {
	if !budgetNow().Before(b.Deadline()) {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		b.Note(pagebudget.NotSent)
		return nil, &pagebudget.ExhaustedError{Result: pagebudget.NotSent}
	}
	x, actx := beginBudgetedAttempt(req.Context(), b)
	resp, err := cli.Do(req.WithContext(actx))
	if err != nil {
		e := x.exhausted(err, 1, "", req.Method)
		x.end()
		if e != nil {
			b.Note(e.Result)
			return nil, e
		}
		return nil, err
	}
	b.Note(pagebudget.Within)
	resp.Body = x.bodyEnding(resp.Body)
	return resp, nil
}

// sendBudgeted is a customer exchange of a safe method inside its page budget
// (#1115 §3). Send decides; the attempt observer only records. At most two
// attempts, in place of RetryClient's five-retry backoff, which cannot fit a
// 10 s budget:
//   - attempt 1 stops waiting on its connection at deadline − reserve if it has
//     none by then (the dial goes on for whoever else waits on it);
//   - attempt 2, once, only if the remaining budget covers the reserve plus the
//     wait it owes: at once after a connection-phase failure (a fresh
//     connection), after RetryClient's first backoff after a 5xx or an error on
//     an established connection, after Retry-After after a 429;
//   - otherwise the last answer, or a *pagebudget.ExhaustedError when the
//     budget ended it.
func sendBudgeted(site string, cli *http.Client, req *http.Request, b *pagebudget.Budget) (*http.Response, error) {
	if !budgetNow().Before(b.Deadline()) {
		b.Note(pagebudget.NotSent)
		return nil, &pagebudget.ExhaustedError{Result: pagebudget.NotSent}
	}
	obs := &attemptObserver{next: cli.Transport, site: site}
	observed := *cli
	observed.Transport = obs
	reserve, _ := freshAnswers.reserve()
	policy := util.NewRetryClient(&observed)

	resp, err, x := budgetedTry(&observed, req, b, b.Deadline().Add(-reserve))
	wait, retry := budgetedRetryWait(req, resp, err, x, policy)
	// A connect cut lands exactly at deadline − reserve: what it left IS the
	// reserve, so a cut attempt always has room for its retry.
	fits := x.wasCut() || b.Deadline().Sub(budgetNow()) >= reserve+wait
	switch {
	case retry && fits:
		if resp != nil {
			_ = resp.Body.Close()
		}
		x.end()
		if !sleepBudget(req.Context(), b, wait) {
			b.Note(pagebudget.ExhaustedConnect)
			return nil, &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedConnect, Cause: obs.prevCause, Attempts: obs.attempts}
		}
		resp, err, x = budgetedTry(&observed, req, b, time.Time{})
		if err == nil {
			b.Note(pagebudget.Retried)
			resp.Body = x.bodyEnding(resp.Body)
			return resp, nil
		}
	case err == nil:
		b.Note(pagebudget.Within)
		resp.Body = x.bodyEnding(resp.Body)
		return resp, nil
	}
	// The send failed. A connect cut whose retry did not fit is the budget's
	// doing too: its connection phase is what ran out.
	e := x.exhausted(err, obs.attempts, obs.prevCause, req.Method)
	if e == nil && x.wasCut() {
		e = &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedConnect, Cause: obs.prevCause, Attempts: obs.attempts}
	}
	x.end()
	if e != nil {
		b.Note(e.Result)
		return nil, e
	}
	return nil, err
}

// budgetedTry is one attempt of sendBudgeted, under the budget's deadline and,
// when cutAt is set, the connect cut. On success the attempt stays open until
// the caller ends it (or the body it hands on is closed).
func budgetedTry(cli *http.Client, req *http.Request, b *pagebudget.Budget, cutAt time.Time) (*http.Response, error, *budgetedAttempt) {
	x, actx := beginBudgetedAttempt(req.Context(), b)
	if !cutAt.IsZero() {
		x.cutConnectAt(cutAt)
	}
	r := req.Clone(actx)
	if req.Body != nil && req.GetBody != nil {
		if body, err := req.GetBody(); err == nil {
			r.Body = body
		}
	}
	resp, err := cli.Do(r)
	x.stopCut()
	return resp, err, x
}

// budgetedRetryWait reports whether attempt 1's outcome earns the one retry,
// and the wait it owes first: none after a connection-phase failure (no
// connection was obtained: the retry dials fresh), RetryClient's first backoff
// after a 5xx or an error on an established connection, Retry-After after a
// 429. A request with a body that cannot be replayed is not retried, and the
// deadline itself ends the send.
func budgetedRetryWait(req *http.Request, resp *http.Response, err error, x *budgetedAttempt, policy *util.RetryClient) (time.Duration, bool) {
	if req.Body != nil && req.GetBody == nil {
		return 0, false
	}
	if err != nil {
		if context.Cause(x.ctx) == pagebudget.ErrExhausted || DestinationRefused(err) {
			return 0, false
		}
		if !x.connected() {
			return 0, true
		}
		return firstBackoff(policy.BaseBackoff, policy.MaxBackoff), true
	}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return retryAfter(resp), true
	case resp.StatusCode >= 500:
		return firstBackoff(policy.BaseBackoff, policy.MaxBackoff), true
	}
	return 0, false
}

// retryPolicyTranscribedFrom is the plumbing version firstBackoff and
// retryAfter transcribe (http/util/retry.go: backoff, parseRetryAfter, both
// unexported). TestBudget1115_RetryPolicyPinnedToPlumbing fails when the module
// builds against another one, so a plumbing bump re-audits them.
//
// Transcribed from v1.28.1. v1.28.1 → v1.34.0 re-audited: http/util/ (retry.go
// included) is byte-identical between the two module trees. v1.34.0 → v1.36.0
// re-audited the same way: http/util/ is byte-identical. v1.36.0 → v1.52.0
// re-audited the same way (snowplow#1359): http/util/ is byte-identical.
// v1.52.0 → v1.54.0 re-audited the same way (plumbing#141): http/util/ is
// byte-identical. v1.54.0 → v1.61.0 re-audited the same way (snowplow#1382):
// http/util/ is byte-identical.
const retryPolicyTranscribedFrom = "v1.61.0"

// firstBackoff is RetryClient's backoff before its first retry (plumbing
// v1.28.1 http/util/retry.go backoff(0, base, max)): base capped at max,
// jittered into [d/2, d).
func firstBackoff(base, maxB time.Duration) time.Duration {
	d := min(base, maxB)
	if d < 2 {
		return d
	}
	return d/2 + time.Duration(rand.Int64N(int64(d)/2))
}

// retryAfter is RetryClient's 429 wait (plumbing's parseRetryAfter): the
// Retry-After seconds or date, else 2 s.
func retryAfter(resp *http.Response) time.Duration {
	ra := resp.Header.Get("Retry-After")
	if ra == "" {
		return 2 * time.Second
	}
	if secs, err := strconv.Atoi(ra); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(ra); err == nil {
		return time.Until(t)
	}
	return 2 * time.Second
}

// sleepBudget waits d; false if the request's own context ended first or the
// budget ran out meanwhile.
func sleepBudget(ctx context.Context, b *pagebudget.Budget, d time.Duration) bool {
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return false
		}
	}
	return ctx.Err() == nil && budgetNow().Before(b.Deadline())
}

// attemptObserver sees each attempt of one Send. The RetryClient's attempts are
// sequential, so its fields need no lock.
type attemptObserver struct {
	next      http.RoundTripper
	site      string
	attempts  int
	prevCause string
	prevMS    int64
	// policy is the RetryClient of an unbudgeted send, nil on a budgeted one
	// (budgetedRetryWait decides there). A refused destination ends it.
	policy *util.RetryClient
}

func (o *attemptObserver) RoundTrip(req *http.Request) (*http.Response, error) {
	next := o.next
	if next == nil {
		next = guardedTransport
	}
	// net/http sets Request.Response only on a redirect hop: part of the same
	// attempt, not a new one.
	if req.Response != nil {
		return next.RoundTrip(req)
	}
	if o.attempts > 0 {
		cause := o.prevCause
		if cause == "" {
			cause = RetryCauseOther
		}
		recordRetry(req, o.site, cause, o.attempts, o.prevMS)
	}
	o.attempts++

	tr := &attemptTrace{}
	start := time.Now()
	resp, err := next.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), tr.hooks())))
	o.prevMS = time.Since(start).Milliseconds()
	o.prevCause = tr.cause(resp, err)
	// #1131 — RetryClient has no way to be told an error is final, and it reads
	// MaxRetries after each attempt, sequentially, so this ends its loop with
	// this error. TestIssue1131_RefusalIsSentOnce pins that.
	if o.policy != nil && DestinationRefused(err) {
		o.policy.MaxRetries = 0
	}
	// #1115 — the retry reserve's data: how long an attempt that opened a NEW
	// connection took from asking for it to its first response byte.
	if err == nil {
		if d, ok := tr.freshAnswer(); ok {
			freshAnswers.record(d)
		}
	}
	return resp, err
}

// attemptTrace records what one attempt's connection did. Dial hooks can run
// on the dialing goroutine, so it is locked.
type attemptTrace struct {
	mu         sync.Mutex
	dnsErr     bool
	connected  bool
	connectErr bool
	tlsErr     bool
	// #1115 — phases begun and not finished when the attempt ended: an attempt
	// its waiter abandoned (a connect cut, the deadline) is classified by the
	// phase it was in, since its detached dial reports its end later or never.
	dnsOpen, connectOpen, tlsOpen bool
	// #1115 — the reserve sample: GetConn to first response byte, on a new
	// connection only.
	getConn, firstByte time.Time
	reused             bool
}

// freshAnswer is the attempt's reserve sample, if it opened a new connection
// and was answered.
func (t *attemptTrace) freshAnswer() (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.connected || t.reused || t.getConn.IsZero() || t.firstByte.IsZero() {
		return 0, false
	}
	return t.firstByte.Sub(t.getConn), true
}

func (t *attemptTrace) hooks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(string) {
			t.mu.Lock()
			t.getConn = time.Now()
			t.mu.Unlock()
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			t.mu.Lock()
			t.dnsOpen = true
			t.mu.Unlock()
		},
		DNSDone: func(i httptrace.DNSDoneInfo) {
			t.mu.Lock()
			t.dnsOpen = false
			if i.Err != nil {
				t.dnsErr = true
			}
			t.mu.Unlock()
		},
		ConnectStart: func(_, _ string) {
			t.mu.Lock()
			t.connectOpen = true
			t.mu.Unlock()
		},
		ConnectDone: func(_, _ string, err error) {
			t.mu.Lock()
			t.connectOpen = false
			if err != nil {
				t.connectErr = true
			} else {
				t.connected = true
			}
			t.mu.Unlock()
		},
		TLSHandshakeStart: func() {
			t.mu.Lock()
			t.tlsOpen = true
			t.mu.Unlock()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			t.mu.Lock()
			t.tlsOpen = false
			if err != nil {
				t.tlsErr = true
			}
			t.mu.Unlock()
		},
		GotConn: func(i httptrace.GotConnInfo) {
			t.mu.Lock()
			t.connected = true
			t.reused = i.Reused
			t.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			t.firstByte = time.Now()
			t.mu.Unlock()
		},
	}
}

// cause classifies the attempt's outcome; "" means nothing a retry follows.
func (t *attemptTrace) cause(resp *http.Response, err error) string {
	if err == nil {
		switch {
		case resp == nil:
			return ""
		case resp.StatusCode == http.StatusTooManyRequests:
			return RetryCause429
		case resp.StatusCode >= 500:
			return RetryCause5xx
		}
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.tlsErr:
		return RetryCauseTLSHandshake
	case t.connectErr && !t.connected:
		return RetryCauseDial
	case t.dnsErr && !t.connected:
		return RetryCauseDNS
	// #1115 — abandoned mid-phase (no Done yet): the phase it was in.
	case t.tlsOpen:
		return RetryCauseTLSHandshake
	case t.connectOpen && !t.connected:
		return RetryCauseDial
	case t.dnsOpen && !t.connected:
		return RetryCauseDNS
	}
	return RetryCauseConnection
}

// HostDigest is the host label every endpoint record carries instead of the
// host: "sha256:" + the first 16 hex of sha256(lowercase hostname, no port).
// The same digest as #1059's slow-call record.
func HostDigest(host string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(host)))
	return "sha256:" + hex.EncodeToString(sum[:])[:16]
}

func recordRetry(req *http.Request, site, cause string, attempt int, prevMS int64) {
	endpointRetries.add(site, cause)
	host := ""
	if req.URL != nil {
		host = req.URL.Hostname()
	}
	xcontext.Logger(req.Context()).WarnContext(req.Context(), "endpoint attempt failed; retrying",
		slog.String("site", site),
		slog.String("cause", cause),
		slog.Int("attempt", attempt+1),
		slog.Int64("failed_attempt_ms", prevMS),
		slog.String("method", req.Method),
		slog.String("host", HostDigest(host)))
}

// retryCounter is one counter per closed {site, cause} cell, all present from
// construction; a value outside the sets folds into its "other" cell, so the
// label set cannot grow from data.
type retryCounter map[string]map[string]*atomic.Uint64

func newRetryCounter() retryCounter {
	m := make(retryCounter, len(SendSites))
	for _, s := range SendSites {
		m[s] = make(map[string]*atomic.Uint64, len(RetryCauses))
		for _, c := range RetryCauses {
			m[s][c] = new(atomic.Uint64)
		}
	}
	return m
}

func (m retryCounter) add(site, cause string) {
	bySite, ok := m[site]
	if !ok {
		bySite = m[SendSiteStage]
	}
	c, ok := bySite[cause]
	if !ok {
		c = bySite[RetryCauseOther]
	}
	c.Add(1)
}

var endpointRetries = newRetryCounter()

// EndpointRetryCount is one snowplow_endpoint_retries_total cell.
type EndpointRetryCount struct {
	Site, Cause string
	Count       uint64
}

// EndpointRetriesSnapshot returns every snowplow_endpoint_retries_total cell,
// zeros included, in SendSites × RetryCauses order.
func EndpointRetriesSnapshot() []EndpointRetryCount {
	out := make([]EndpointRetryCount, 0, len(SendSites)*len(RetryCauses))
	for _, s := range SendSites {
		for _, c := range RetryCauses {
			out = append(out, EndpointRetryCount{Site: s, Cause: c, Count: endpointRetries[s][c].Load()})
		}
	}
	return out
}
