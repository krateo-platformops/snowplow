package rum

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
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
// #1533: a batch the collector cannot take NOW (a 429/502/503/504, or no answer at all: a refused connection, a
// disconnect, a timeout) stays at the head of the queue and is tried again (deliver); one refused for good (any other
// answer, or a transport failure positively identified as recurring on every attempt: permanentTransport) is counted
// forward_failed after that one attempt.
type Forwarder struct {
	url    string
	client *http.Client
	queue  chan batch
	// sleep waits d, false when ctx ended first (the worker's own wait; a test replaces it)
	sleep func(ctx context.Context, d time.Duration) bool
}

// NewForwarder returns a forwarder for the collector at collectorURL (the full
// .../v1/traces URL). Start runs its worker. Its client comes from credhttp
// like every client the module builds (#723/#724): it refuses redirects, so a
// collector address can never bounce the browser's spans elsewhere.
//
// A URL that is not http or https, or that names no host, is refused here, at
// configuration (#1535 review, #1539): no attempt to it could ever succeed, so it
// never reaches the worker, where every failure without an answer is retried
// (permanentTransport). The host is the name, not the authority: ":4318" or
// "user:pw@:4318" names a port and no host (u.Host is not empty, u.Hostname()
// is), and an empty host fails every batch at runtime (net/http's untyped "no
// Host in request URL", held at the head of the queue for good; with only a
// port, the dial policy's refusal per batch).
func NewForwarder(collectorURL string) (*Forwarder, error) {
	u, err := url.Parse(collectorURL)
	if err != nil {
		return nil, fmt.Errorf("rum: collector endpoint: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("rum: collector endpoint scheme %q: only http and https can carry OTLP/HTTP", u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, errors.New("rum: collector endpoint has no host")
	}
	return &Forwarder{
		url:    collectorURL,
		client: credhttp.CountConnectionsOn(credhttp.EgressRUMForward, credhttp.Client(&http.Client{Timeout: ForwardTimeout})),
		queue:  make(chan batch, ForwardQueue),
		sleep:  sleepCtx,
	}, nil
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
// A request that never got an answer is retried, by the same specification, unless the failure is positively
// identified as one that recurs (permanentTransport), not by upstream parity (the OTel Go exporter does NOT retry a
// refused connection: its url.Error is not Temporary). A refused connection is exactly a restarting node-local
// collector, the daemonset roll this was reported on (#1533).
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
		if permanentTransport(err) {
			return forwardRefused, 0
		}
		return forwardTransient, 0
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

// permanentTransport reports whether a request that got no answer failed in a way that recurs on every attempt
// however long the wait (#1535 review), and only when that is positively identified: the client does not trust the
// collector's certificate (a TLS verification failure: the certificate does not change between attempts), the dial
// policy denies the destination (credhttp.DestinationRefused: the policy does not change), the collector's name does
// not exist (a DNS answer that the name is not found, not a failed lookup). Such a batch is one attempt and
// forward_failed, like an answer outside the retryable set: held at the head of the queue it would stall every later
// batch into forward_overflow. (A scheme that can never be sent is refused at configuration, NewForwarder; a refused
// redirect is an answer, a 3xx, which forwardClass refuses.)
//
// Every other failure without an answer is retried, as the OTLP/HTTP specification asks of a client that got none:
// OTLP/HTTP Connection, "If the client cannot connect to the server, the client SHOULD retry the connection using an
// exponential backoff strategy between retries."; All Other Responses, "If the server disconnects without returning a
// response, the client SHOULD retry and send the same request." The specification has no "transient" category of its
// own: these two sentences are its retryable transport cases, and the failures they cover are not a closed set. A
// collector restarting under a kept-alive connection gives net/http's `http: server closed idle connection`, an
// error with no type, no errno and no EOF, which net/http does not replay for a POST; a status line cut before its
// code reads as `malformed HTTP status code`. Both are disconnects without a response, and both are retried because
// retrying is the default, not because they were listed.
func permanentTransport(err error) bool {
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, new(*tls.CertificateVerificationError)),
		errors.As(err, new(x509.UnknownAuthorityError)),
		errors.As(err, new(x509.HostnameError)),
		errors.As(err, new(x509.CertificateInvalidError)):
		return true
	case credhttp.DestinationRefused(err):
		return true
	case errors.As(err, &dnsErr):
		return dnsErr.IsNotFound
	}
	return false
}
