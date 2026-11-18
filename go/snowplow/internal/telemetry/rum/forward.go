package rum

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// ForwardQueue is how many accepted batches may wait for the collector. A
// batch that finds the queue full is dropped and counted (forward_overflow):
// the request that carried it has already been answered, so a slow or absent
// collector never holds a portal request.
const ForwardQueue = 64

// ForwardTimeout bounds one POST to the collector (one attempt).
const ForwardTimeout = 5 * time.Second

// The wait between attempts at one batch (#1533): the OTel Go OTLP/HTTP exporter's backoff INTERVALS
// (go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.47.0 internal/retry: InitialInterval 5 s,
// MaxInterval 30 s; github.com/cenkalti/backoff/v5 v5.0.3 DefaultMultiplier 1.5, DefaultRandomizationFactor 0.5),
// so the numbers are the exporter's, not ours, WITHOUT its MaxElapsedTime: the exporter's default shape also gives
// up after 1 min and drops the batch; here a batch is held at the head of the queue until it is delivered, refused,
// or snowplow shuts down (shutdown cuts any wait). The queue (ForwardQueue) and the per-attempt timeout
// (ForwardTimeout) stay the only bounds.
const (
	retryInitial    = 5 * time.Second
	retryMax        = 30 * time.Second
	retryMultiplier = 1.5
	retryJitter     = 0.5
)

type batch struct {
	body  []byte // OTLP protobuf ExportTraceServiceRequest
	spans int
}

// Forwarder posts accepted batches to the collector's OTLP/HTTP traces
// endpoint from ONE background worker, in protobuf (the encoding every
// collector OTLP/HTTP receiver accepts). It is deliberately NOT
// tracing.WrapTransport'd: forwarding the browser's spans must not mint
// snowplow Client spans of its own.
//
// #1533: a batch the collector cannot take NOW (a 429/502/503/504, or no answer for a reason that heals: a refused
// connection, a timeout; transientTransport) stays at the head of the queue and is tried again (deliver); one refused
// for good (any other answer, or a transport failure that recurs on every attempt) is counted forward_failed after
// that one attempt.
type Forwarder struct {
	url    string
	client *http.Client
	queue  chan batch
	// sleep waits d, false when ctx ended first (the worker's own wait; a test replaces it)
	sleep func(ctx context.Context, d time.Duration) bool
}

// NewForwarder returns a forwarder for the collector at url (the full
// .../v1/traces URL). Start runs its worker. Its client comes from credhttp
// like every client the module builds (#723/#724): it refuses redirects, so a
// collector address can never bounce the browser's spans elsewhere.
func NewForwarder(url string) *Forwarder {
	return &Forwarder{
		url:    url,
		client: credhttp.CountConnectionsOn(credhttp.EgressRUMForward, credhttp.Client(&http.Client{Timeout: ForwardTimeout})),
		queue:  make(chan batch, ForwardQueue),
		sleep:  sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// enqueue hands b to the worker without ever blocking; false = queue full.
func (f *Forwarder) enqueue(b batch) bool {
	select {
	case f.queue <- b:
		return true
	default:
		return false
	}
}

// Start runs the worker until ctx is done. The batch being retried then, and
// every batch still queued, are counted as forward_failed: they were accepted
// and never delivered.
func (f *Forwarder) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				for {
					select {
					case b := <-f.queue:
						spansForwardFailed.Add(uint64(b.spans))
					default:
						return
					}
				}
			case b := <-f.queue:
				f.deliver(ctx, b)
			}
		}
	}()
}

// deliver tries b until the collector takes it (forwarded), refuses it for good (forward_failed, after one
// attempt), or ctx ends (forward_failed). Between attempts it waits the exporter's backoff interval, or the
// collector's Retry-After when that is longer; ctx's end cuts the wait.
func (f *Forwarder) deliver(ctx context.Context, b batch) {
	wait := time.Duration(0)
	for {
		o, retryAfter := f.post(ctx, b)
		switch o {
		case forwardDelivered:
			spansForwarded.Add(uint64(b.spans))
			return
		case forwardRefused:
			spansForwardFailed.Add(uint64(b.spans))
			return
		}
		if wait == 0 {
			wait = retryInitial
		} else if wait = time.Duration(float64(wait) * retryMultiplier); wait > retryMax {
			wait = retryMax
		}
		d := time.Duration(float64(wait) * (1 + retryJitter*(2*rand.Float64()-1)))
		if retryAfter > d {
			d = retryAfter
		}
		if ctx.Err() != nil || !f.sleep(ctx, d) {
			spansForwardFailed.Add(uint64(b.spans)) // shut down while it was held: accepted, never delivered
			return
		}
	}
}

type forwardOutcome int

const (
	forwardDelivered forwardOutcome = iota // a 2xx
	forwardRefused                         // a permanent refusal: never tried again
	forwardTransient                       // try again later
)

// forwardClass is the OTLP/HTTP rule for one answer (OpenTelemetry Protocol specification, OTLP/HTTP Response,
// "Retryable Response Codes": "The requests that receive a response status code listed in following table SHOULD
// be retried." (429, 502, 503, 504) "All other 4xx or 5xx response status codes MUST NOT be retried."). So a 500,
// the collector judging this payload itself, is a permanent refusal.
//
// A request that never got an answer is retried when the failure is one the same specification retries
// (transientTransport: the server could not be reached, or went away without answering), not by upstream parity
// (the OTel Go exporter does NOT retry a refused connection: its url.Error is not Temporary). A refused connection is
// exactly a restarting node-local collector, the daemonset roll this was reported on (#1533).
func forwardClass(status int) forwardOutcome {
	switch {
	case status/100 == 2:
		return forwardDelivered
	case status == http.StatusTooManyRequests, status == http.StatusBadGateway,
		status == http.StatusServiceUnavailable, status == http.StatusGatewayTimeout:
		return forwardTransient
	default:
		return forwardRefused
	}
}

// post is one attempt at b, bounded by ForwardTimeout: its outcome, and the collector's Retry-After on a transient
// answer that carries one (credhttp.RetryAfter, the module's one parser: an absent or unparsable header is no
// Retry-After, a past date or negative seconds is 0, a value past the largest time.Duration is the largest; such a
// wait is cut by shutdown like any other). OTLP/HTTP Throttling: "The client SHOULD honour the value specified in
// the "Retry-After" header if it is present."
func (f *Forwarder) post(ctx context.Context, b batch) (forwardOutcome, time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, ForwardTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url, bytes.NewReader(b.body))
	if err != nil {
		return forwardRefused, 0 // the URL cannot make a request: no attempt can succeed
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := f.client.Do(req)
	if err != nil {
		if transientTransport(err) {
			return forwardTransient, 0
		}
		return forwardRefused, 0
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	o := forwardClass(resp.StatusCode)
	if o != forwardTransient {
		return o, 0
	}
	ra, _ := credhttp.RetryAfter(resp)
	return o, ra
}

// transientTransport reports whether a request that got no answer may be taken if sent again later (#1535
// review): the transport failures the OTLP/HTTP specification retries, and only those. OTLP/HTTP Connection: "If the
// client cannot connect to the server, the client SHOULD retry the connection using an exponential backoff strategy
// between retries." OTLP/HTTP Response, All Other Responses: "If the server disconnects without returning a
// response, the client SHOULD retry and send the same request." So: a connection refused, reset, aborted, a broken
// pipe, a host or network unreachable (a collector restarting, a node network blip); the server gone without an
// answer (EOF); the attempt timing out (ForwardTimeout, a dial or read timeout); a DNS failure the resolver calls
// temporary. The specification has no word "transient" of its own: these two sentences are its retryable transport
// cases. Every other failure recurs the same on every attempt however long the wait (a destination the dial policy
// denies, a certificate the client does not trust, an unsupported scheme): held at the head of the queue it would
// stall every later batch into forward_overflow, so it is a permanent refusal, one attempt and forward_failed, like
// an answer outside the retryable set. (A refused redirect is an answer, a 3xx: forwardClass refuses it.)
//
// One disconnect shape is not retried, against the "disconnects without returning a response" sentence: a status
// line cut before its code. Go's client reports a status line truncated by a disconnect ("HTTP/1.1 20", then EOF)
// as `malformed HTTP status code`, an error with no type of its own and no EOF in its chain, so it cannot be told
// from a server that answered garbage, which recurs. It is classified like that: permanent. A cut anywhere after the
// status line reads as an unexpected EOF and is retried.
func transientTransport(err error) bool {
	switch {
	case errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNABORTED), errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTemporary || dnsErr.IsTimeout
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
