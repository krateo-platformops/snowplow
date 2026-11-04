package credhttp

// send.go — #1088: the one send policy for an endpoint dial (the RESTAction
// branch-E stage fetch and the /call passthrough, credhttp.Do).
//
// A WRITE IS SENT ONCE. plumbing's util.RetryClient (v1.15.1 http/util/retry.go)
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

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/util"
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
// a write is sent once; a safe request goes through plumbing's RetryClient with
// every retry counted and logged with its cause. cli is not modified.
func Send(site string, cli *http.Client, req *http.Request) (*http.Response, error) {
	if !SafeMethod(req.Method) {
		return cli.Do(req)
	}
	observed := *cli
	observed.Transport = &attemptObserver{next: cli.Transport, site: site}
	return util.NewRetryClient(&observed).Do(req)
}

// attemptObserver sees each attempt of one Send. The RetryClient's attempts are
// sequential, so its fields need no lock.
type attemptObserver struct {
	next      http.RoundTripper
	site      string
	attempts  int
	prevCause string
	prevMS    int64
}

func (o *attemptObserver) RoundTrip(req *http.Request) (*http.Response, error) {
	next := o.next
	if next == nil {
		next = http.DefaultTransport
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
}

func (t *attemptTrace) hooks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSDone: func(i httptrace.DNSDoneInfo) {
			if i.Err != nil {
				t.mu.Lock()
				t.dnsErr = true
				t.mu.Unlock()
			}
		},
		ConnectDone: func(_, _ string, err error) {
			t.mu.Lock()
			if err != nil {
				t.connectErr = true
			} else {
				t.connected = true
			}
			t.mu.Unlock()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err != nil {
				t.mu.Lock()
				t.tlsErr = true
				t.mu.Unlock()
			}
		},
		GotConn: func(httptrace.GotConnInfo) {
			t.mu.Lock()
			t.connected = true
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
	xcontext.Logger(req.Context()).Warn("endpoint attempt failed; retrying",
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
