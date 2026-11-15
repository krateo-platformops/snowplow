// external_fetch_flight.go — #1320: identical concurrent external READS share
// one request on the wire.
//
// WHY. Measured on 057 (2026-11-10, 1.12.67, synthetic load): every page load
// opened its own identical external call (the dashboard events RESTAction's
// stage → sse-proxy /events), up to 48 at once at 50 virtual users over 3
// identities. Bench run 26i: a page of 20 widgets reading one external
// endpoint multiplied outbound calls by about 8.
//
// THE RULE EVERYTHING BELOW FOLLOWS (owner, 2026-11-15): no reader gets a
// worse outcome than its own call would have given it — the same answer, no
// later — and snowplow never sends upstream work it would not have sent
// without sharing.
//
// WHAT IS SHARED: ONE REQUEST ON THE WIRE, and nothing above it. The sharing is
// a transport (externalWire) installed under credhttp.Send, outermost on the
// stage fetch's client. Each reader runs the whole fetch itself, exactly as
// without sharing: its own send policy (credhttp/send.go: the customer's two
// attempts inside its page budget, background work's RetryClient), its own
// backoff, connect cut, budget notes, audit observation (credhttp.Answer),
// retry counters, body handling and conversion. Only when one of ITS attempts
// reaches the wire does it either start a wire request or wait on an identical
// one already there. So a wire request has no policy of its own: background
// work that shares an attempt a customer started gets that attempt's answer
// and applies its own retry rule to it; backoff is always the reader's own.
//
// THE KEY is the whole outbound request (externalReadKeyOf): the endpoint with
// every field (server URL, CA, client certificate and key, token, username,
// password, proxy, flags), the method, the resolved URL, EVERY header in order
// (an exported JWT rides there as Authorization), the payload, and the
// provenance that changes what is sent (a held credential, a ServiceAccount
// dial and the token file it reads, a #615 impersonation with its user, UID,
// groups and extras, the #443 inert flag). Two identities therefore never
// share. A key that cannot fold the endpoint exactly (exactEndpointKeyOf)
// shares nothing. Only the fetch's own first request is shared: a redirect hop
// is sent by its reader.
//
// READS ONLY. A write (any method but GET, HEAD, OPTIONS, TRACE —
// credhttp.SafeMethod, the module's one write classifier) is sent by its own
// caller: never joined, never joining.
//
// THE JOIN RULE (owner rulings 2026-11-15; joinableLocked is the one place it
// is decided): a request on the wire is joinable if and only if the reader
// that started it is still waiting on it AND it is younger than the page
// budget; a customer never joins a request background work started; a write
// is never shared; the key is every input of the request the stage asks for
// (credentials, URL, headers, payload, provenance). The session id is NOT a
// key component (owner ruling 2026-11-15): it is correlation, like the trace
// id, so readers of different sessions share, and the request on the wire
// carries its starter's.
//   - "Younger than the page budget": until the page-budget deadline of the
//     customer that started it, or, for a request background work started (no
//     page budget), until pagebudget.PageMaxGoal after it started. So one
//     request that never answers can cost at most the readers that arrive
//     within one page budget of it; a slow upstream is NOT converged on by
//     late readers, who start their own, as without sharing.
//   - "Still waiting": once its starter has left (its budget ran out, its
//     client disconnected, its connect was cut), nobody new joins. Readers
//     already waiting on it stay, unaffected.
//   - Besides the rule: a reader whose context is already done neither starts
//     nor joins (the transport below sends nothing for a done context, as
//     without sharing); nobody joins after the first response byte (the
//     freshness contract, external_fetch_cache.go) and nothing is reused after
//     completion; a reader's own RETRY never re-joins the attempt it gave up
//     on: it is sent on its own, as its fresh connection would be.
//
// ITS LIFETIME IS ITS WAITERS'. A reader waits on the wire request under its
// own attempt's context (its page budget, its client's connection). When the
// last waiter leaves before the answer is complete, the wire request is
// cancelled and the key freed (owner ruling 2026-11-15, reversing 2026-11-10's
// "runs to its deadline"): nothing outlives the readers it was sent for.
//
// WHAT EACH WAITER GETS. The response's status line, headers and trailers (its
// own copy), and a body that reads the bytes as the wire request receives
// them, under the waiter's own context. Of a non-2xx answer only the first
// maxUnstructuredResponseTextBytes are held: all the fetch reads of one.
//
// ONE REQUEST FEEDS EACH INSTRUMENT ONCE. The connection counters and the
// retry reserve (credhttp connections.go, send.go) listen on the httptrace
// hooks of the reader that started the request: they count its one connection
// and time its one answer. A joiner's attempt is only CLASSIFIED by the wire
// request's connection events (connected or not, the phase it failed in),
// which reach it through credhttp.ObserveSharedAttempt and never through
// httptrace: a joiner counts no connection and takes no reserve sample.
//
// WHOSE REQUEST IS ON THE WIRE. The reader's that started it, byte for byte
// what it would have sent alone: its X-Krateo-TraceId, and the W3C traceparent
// and baggage the tracing transport (below the sharing) injects from its
// context. A joiner sends nothing, so nothing of a joiner — no trace id, no
// session id — reaches the upstream, and a joiner's own trace has no client
// span for the exchange: its wait is on its slow-call record (a flight wait on
// the key's digest). The correlation headers are not part of what the stage
// asks for and are not in the key.
//
// WHAT A SHARED READ CHANGES FOR AN UPSTREAM'S RECORDS, AND THE LINK BACK. The
// upstream sees one request, under its starter's trace context and session
// id; a joiner's read leaves no record of its own there. So that a joiner's
// trace still leads to that one request, the span active on the joiner's
// fetch gets a SPAN LINK to the wire request's client span (owner ruling
// 2026-11-15): the span context of the W3C traceparent written on the wire,
// which is unique to that one request (a session id is not: a caller can send
// the same one on many requests). The link carries the two ids and nothing
// else: no attribute, no user, no session id, no URL. It is added when the
// fetch ends, and only when the answer the fetch returned came from another
// reader's request: a reader that sent its own request, or that re-sent after
// a shared attempt failed, links to nothing. With tracing off no traceparent
// is written, so there is no link and nothing is done. Snowplow's audit
// trail is unchanged: it records writes, and a shared request is a read.
//
// The other end: the wire request's client span carries
// snowplow.upstream.joiners, the number of OTHER readers its answer went to,
// so a trace store that samples each trace on its own can keep the trace a
// joiner's link points at. Only when it is above zero: an unshared request's
// span has no such attribute. A count, no identity. It is final when it is
// set: the readers an answer goes to are fixed, under the lock, at the moment
// the status line is published (nobody joins after it, and a reader that has
// not left by then is answered), and the client span is still open then (it
// ends when the body is read to its end or closed, which the wire request
// does afterwards).
//
// Nothing here logs a key, URL or header: the key is a sha256 digest.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/rest"
)

// externalFlightKey identifies one outbound read (externalReadKeyOf). Opaque;
// never logged.
type externalFlightKey [sha256.Size]byte

// externalFlight is one request on the wire that identical reads wait on.
type externalFlight struct {
	c   *externalFetchCache
	key externalFlightKey
	// ctx is the wire request's: the values of the reader that started it,
	// none of its cancellation or page budget. cancel ends the wire request.
	ctx    context.Context
	cancel context.CancelFunc
	// joinUntil is when it stops being "younger than the page budget": the
	// page-budget deadline of the customer that started it, or
	// pagebudget.PageMaxGoal after its start when background work did.
	joinUntil time.Time
	// customerStarted: a reader with a page budget started it.
	customerStarted bool
	// ref is what identifies the wire request to its upstream: the W3C trace
	// context written on the wire (set under traceMu as the headers are
	// written; empty when tracing is off).
	ref externalWireRef

	// Under c.flightsMu.
	waiters     int
	starterGone bool // the reader that started it stopped waiting
	closed      bool // admits no more joiners
	// published: the wire request's outcome (status line, error or panic) is
	// decided; every reader still waiting at that moment gets it, even one
	// whose own context ends in the same instant.
	published bool

	// headers is closed once the wire request has a status line, an error or a
	// panic; resp, err, panicked and panicVal are written before that and read
	// only after.
	headers  chan struct{}
	resp     *http.Response // no Body: each waiter reads it through externalFlightBody
	err      error
	panicked bool
	panicVal any

	// The body as received so far, under mu. bodyErr is set when it ended
	// (io.EOF, or the read error); more is closed and replaced at each change.
	mu        sync.Mutex
	buf       []byte
	bodyErr   error
	bodyPanic bool
	more      chan struct{}

	// The wire request's connection events so far and the joiners' attempt
	// contexts they are told to, under traceMu (held across the calls, so a
	// joiner hears every event once, in order).
	traceMu sync.Mutex
	events  []flightEvent
	joiners []context.Context
}

// externalWire is one fetch's sharing transport: its attempts start or wait on
// the key's wire request. Built per fetch (externalFetchCache.share).
type externalWire struct {
	c    *externalFetchCache
	next http.RoundTripper
	key  externalFlightKey
	// shareable: the fetch is a read whose key folds exactly.
	shareable bool
	// method and url are the fetch's own first request: only that is shared.
	method, url string

	// waited is the wire requests this fetch already waited on: a retry never
	// re-joins one. A fetch's attempts are sequential; the lock is for safety.
	mu     sync.Mutex
	waited map[*externalFlight]struct{}
}

// share returns the transport wrapper httpFetchConverting installs outermost
// on the fetch's client, under credhttp.Send (external_fetch.go).
func (c *externalFetchCache) share(ctx context.Context, opts httpcall.RequestOptions) func(http.RoundTripper, *http.Request) http.RoundTripper {
	key, ok := externalReadKeyOf(ctx, opts)
	return func(next http.RoundTripper, first *http.Request) http.RoundTripper {
		return &externalWire{c: c, next: next, key: key, shareable: ok,
			method: first.Method, url: first.URL.String(), waited: map[*externalFlight]struct{}{}}
	}
}

func (w *externalWire) RoundTrip(req *http.Request) (*http.Response, error) {
	// A request this fetch's key does not describe is its reader's own: a
	// write, an inexact key, a redirect hop (net/http sets Response on one).
	if !w.shareable || req.Response != nil || req.Method != w.method || req.URL.String() != w.url {
		w.c.flightNotShared.Add(1)
		answeredBy(req.Context()).clear()
		return w.next.RoundTrip(req)
	}
	// A reader that is already gone starts nothing and joins nothing: the
	// transport below sends nothing for a done context.
	if req.Context().Err() != nil {
		return w.next.RoundTrip(req)
	}
	f, role := w.joinOrLead(req.Context())
	// Whatever an earlier attempt of this fetch was answered by, this one's
	// answer is its own unless it joins and is answered (await).
	answeredBy(req.Context()).clear()
	switch role {
	case flightOwn:
		w.c.flightNotShared.Add(1)
		return w.next.RoundTrip(req)
	case flightLead:
		w.c.flightLed.Add(1)
		w.start(f, req)
	default:
		w.c.flightJoined.Add(1)
		if req.Body != nil {
			_ = req.Body.Close() // a joiner sends nothing
		}
	}
	return f.await(req, role == flightLead)
}

// WrappedRoundTripper lets utilnet.CloseIdleConnectionsFor and friends walk
// through to the transport.
func (w *externalWire) WrappedRoundTripper() http.RoundTripper { return w.next }

type flightRole int

const (
	flightLead flightRole = iota // started the key's wire request
	flightJoin                   // waits on one already there
	flightOwn                    // sent by its reader, outside the key's slot
)

// joinOrLead returns the key's wire request for this attempt to wait on:
// the joinable one already there, or a new one this attempt starts. flightOwn
// (nil): the one there is the attempt this fetch already gave up on.
func (w *externalWire) joinOrLead(ctx context.Context) (*externalFlight, flightRole) {
	c := w.c
	b, customer := pagebudget.From(ctx)
	w.mu.Lock()
	defer w.mu.Unlock()
	c.flightsMu.Lock()
	defer c.flightsMu.Unlock()
	now := time.Now()
	if f := c.flights[w.key]; f != nil && f.joinableLocked(now, customer) {
		if _, again := w.waited[f]; again {
			return nil, flightOwn
		}
		f.waiters++
		w.waited[f] = struct{}{}
		return f, flightJoin
	}
	f := &externalFlight{c: c, key: w.key, waiters: 1, headers: make(chan struct{}), more: make(chan struct{}),
		customerStarted: customer, joinUntil: now.Add(pagebudget.PageMaxGoal)}
	if customer {
		f.joinUntil = b.Deadline()
	}
	f.ctx, f.cancel = context.WithCancel(pagebudget.Detach(ctx))
	// A wire request that admits no more joiners is replaced in the slot; it
	// goes on for the readers it has.
	c.flights[w.key] = f
	w.waited[f] = struct{}{}
	return f, flightLead
}

// joinableLocked is THE JOIN RULE (file comment): a reader arriving now may
// wait on f if and only if f's starter is still waiting on it and f is younger
// than the page budget, and a customer never joins a request background work
// started. closed covers what ends joining for good: the first response byte,
// completion, the last waiter leaving. customer: the arriving reader has a
// page budget.
func (f *externalFlight) joinableLocked(now time.Time, customer bool) bool {
	return !f.closed &&
		!f.starterGone &&
		now.Before(f.joinUntil) &&
		(f.customerStarted || !customer)
}

// closeJoins takes f out of its key's slot: no read joins it from here on.
// Idempotent; a newer wire request in the slot is left alone.
func (c *externalFetchCache) closeJoins(f *externalFlight) {
	c.flightsMu.Lock()
	f.closed = true
	if c.flights[f.key] == f {
		delete(c.flights, f.key)
	}
	c.flightsMu.Unlock()
}

// leave records that one waiter stopped waiting on f. The last one out ends
// the wire request and frees the key.
func (f *externalFlight) leave(starter bool) { f.leaveUnless(starter, false) }

// leaveIfUnpublished is leave for a reader whose context ended while it waited
// for the outcome, in one critical section with publish. false: the outcome
// was published first, so the reader is one of those it goes to and has not
// left.
func (f *externalFlight) leaveIfUnpublished(starter bool) bool { return f.leaveUnless(starter, true) }

func (f *externalFlight) leaveUnless(starter, unlessPublished bool) bool {
	c := f.c
	c.flightsMu.Lock()
	if unlessPublished && f.published {
		c.flightsMu.Unlock()
		return false
	}
	f.waiters--
	if starter {
		f.starterGone = true
	}
	last := f.waiters == 0
	if last {
		f.closed = true
		if c.flights[f.key] == f {
			delete(c.flights, f.key)
		}
	}
	c.flightsMu.Unlock()
	if last {
		f.cancel()
	}
	return true
}

// publish fixes who the wire request's outcome goes to: every reader still
// waiting. It returns how many of them are joiners (readers other than the
// one that started it). It retires the request: nobody joins from here on,
// and its key is free.
func (f *externalFlight) publish() (joiners int) {
	c := f.c
	c.flightsMu.Lock()
	f.published, f.closed = true, true
	if c.flights[f.key] == f {
		delete(c.flights, f.key)
	}
	joiners = f.waiters
	if !f.starterGone {
		joiners--
	}
	c.flightsMu.Unlock()
	return joiners
}

// spanAttrUpstreamJoiners is on the client span of a request on the wire whose
// answer went to other readers too: how many (file comment).
const spanAttrUpstreamJoiners = "snowplow.upstream.joiners"

// markShared sets spanAttrUpstreamJoiners on the wire request's client span:
// the span whose id went on the wire in the traceparent, found on the context
// of the request the transport sent. Nothing is done without joiners or
// without a trace context (tracing off).
func (f *externalFlight) markShared(resp *http.Response, joiners int) {
	if joiners <= 0 || resp == nil || resp.Request == nil {
		return
	}
	f.traceMu.Lock()
	ref := f.ref
	f.traceMu.Unlock()
	if ref.traceparent == "" {
		return
	}
	wire := ref.SpanContext()
	if span := trace.SpanFromContext(resp.Request.Context()); wire.IsValid() && span.SpanContext().SpanID() == wire.SpanID() {
		span.SetAttributes(attribute.Int(spanAttrUpstreamJoiners, joiners))
	}
}

// start sends f's wire request: req, on f's own context, in a goroutine of its
// own, so no single reader's cancellation ends it while another still waits.
func (w *externalWire) start(f *externalFlight, req *http.Request) {
	wreq := req.Clone(httptrace.WithClientTrace(f.ctx, f.trace()))
	// The request is cancelled by f.ctx alone: a deprecated Cancel channel on
	// the starter's request (http.Client.Timeout sets one) would end it under
	// its joiners.
	wreq.Cancel = nil
	go f.fly(w.next, wreq)
}

// fly performs the wire request and publishes its status line, then its body.
func (f *externalFlight) fly(next http.RoundTripper, req *http.Request) {
	defer f.cancel()
	published := false
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		f.c.closeJoins(f)
		f.c.flightsMu.Lock()
		unwaited := f.waiters == 0
		f.c.flightsMu.Unlock()
		if unwaited {
			// Nobody is left to raise it in: counted and logged, never lost.
			f.c.flightPanicsUnwaited.Add(1)
			// The value is not ours to predict (a transport's panic can carry a
			// URL or a header): its type, and its text only when it is an
			// error, with any URL in it redacted.
			attrs := []any{slog.String("flight", hex.EncodeToString(f.key[:8])), slog.String("panic_type", fmt.Sprintf("%T", p))}
			if perr, ok := p.(error); ok {
				attrs = append(attrs, slog.String("panic", redact.ErrorURL(perr).Error()))
			}
			slog.Default().ErrorContext(f.ctx, "external read: the shared request panicked after its last waiter left", attrs...)
		}
		// Raised again in every waiter, where a panic in the transport
		// surfaces without sharing: in the reader's own goroutine.
		f.panicVal = p
		if !published {
			f.panicked = true
			f.publish()
			close(f.headers)
			return
		}
		f.mu.Lock()
		f.bodyPanic = true
		close(f.more)
		f.more = make(chan struct{})
		f.mu.Unlock()
	}()
	resp, err := next.RoundTrip(req)
	// Whatever it returned, publishing it retires the request: it is over as
	// something to join, and nothing is reused after completion.
	if err != nil {
		f.err = err
		published = true
		f.publish()
		close(f.headers)
		return
	}
	body := resp.Body
	view := *resp
	view.Body = http.NoBody
	f.resp = &view
	published = true
	// Before the body is read: the client span is still open.
	f.markShared(resp, f.publish())
	close(f.headers)

	defer body.Close()
	var r io.Reader = body
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// All the fetch reads of a non-2xx answer (external_fetch.go).
		r = io.LimitReader(body, maxUnstructuredResponseTextBytes)
	}
	chunk := make([]byte, 32<<10)
	for {
		n, rerr := r.Read(chunk)
		f.mu.Lock()
		if n > 0 {
			f.buf = append(f.buf, chunk[:n]...)
		}
		if rerr != nil {
			f.bodyErr = rerr
		}
		close(f.more)
		f.more = make(chan struct{})
		f.mu.Unlock()
		if rerr != nil {
			return
		}
	}
}

// await waits for f's status line under the reader's own attempt context.
// led: this reader started f (its own dial span already covers the wait); a
// joiner records a flight wait and is told f's connection events (tell).
func (f *externalFlight) await(req *http.Request, led bool) (*http.Response, error) {
	ctx := req.Context()
	var span slowcall.Span
	if !led {
		span = slowcall.StartWait(ctx, slowcall.PhaseFlightWait, "external_flight", hex.EncodeToString(f.key[:]), "")
		f.tell(ctx)
	}
	select {
	case <-f.headers:
	case <-ctx.Done():
		if f.leaveIfUnpublished(led) {
			span.EndRelease(slowcall.CauseOf(ctx.Err()))
			return nil, context.Cause(ctx)
		}
		// The outcome was published before this reader left: it is one of the
		// readers it goes to (and was counted as one). It is here in a moment.
		<-f.headers
	}
	if f.panicked {
		span.EndRelease(slowcall.ReleaseLeaderFailed)
		f.leave(led)
		panic(f.panicVal)
	}
	if f.err != nil {
		span.EndRelease(slowcall.ReleaseLeaderFailed)
		f.leave(led)
		return nil, f.err
	}
	span.End()
	if !led {
		f.traceMu.Lock()
		ref := f.ref
		f.traceMu.Unlock()
		answeredBy(ctx).set(ref)
	}
	// The reader's own copy of the status line, headers and trailers.
	resp := *f.resp
	resp.Header = f.resp.Header.Clone()
	resp.Trailer = f.resp.Trailer.Clone()
	resp.Request = req
	resp.Body = &externalFlightBody{f: f, ctx: ctx, starter: led}
	return &resp, nil
}

// externalFlightBody is one waiter's read of f's body, under that waiter's own
// context. Closing it is how the waiter leaves f.
type externalFlightBody struct {
	f       *externalFlight
	ctx     context.Context
	starter bool
	off     int
	once    sync.Once
}

func (b *externalFlightBody) Read(p []byte) (int, error) {
	f := b.f
	for {
		f.mu.Lock()
		if b.off < len(f.buf) {
			n := copy(p, f.buf[b.off:])
			b.off += n
			f.mu.Unlock()
			return n, nil
		}
		if f.bodyPanic {
			f.mu.Unlock()
			panic(f.panicVal)
		}
		if f.bodyErr != nil {
			err := f.bodyErr
			f.mu.Unlock()
			return 0, err
		}
		more := f.more
		f.mu.Unlock()
		select {
		case <-more:
		case <-b.ctx.Done():
			return 0, context.Cause(b.ctx)
		}
	}
}

func (b *externalFlightBody) Close() error {
	b.once.Do(func() { b.f.leave(b.starter) })
	return nil
}

// externalWireRef identifies one request on the wire to whoever received it:
// the W3C trace context it carried, as written (the traceparent and tracestate
// header values). Empty when none was sent (tracing off).
type externalWireRef struct {
	traceparent, tracestate string
}

// wireHeaders reads a ref's trace context the way a receiver does.
type wireHeaders externalWireRef

func (h wireHeaders) Get(key string) string {
	switch key {
	case "traceparent":
		return h.traceparent
	case "tracestate":
		return h.tracestate
	}
	return ""
}
func (h wireHeaders) Set(string, string) {}
func (h wireHeaders) Keys() []string     { return []string{"traceparent", "tracestate"} }

// SpanContext is the span context of the wire request's client span: what its
// upstream received as the parent of its own span. Invalid (the zero value)
// when no well-formed traceparent was written.
func (r externalWireRef) SpanContext() trace.SpanContext {
	if r.traceparent == "" {
		return trace.SpanContext{}
	}
	return trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), wireHeaders(r)))
}

// externalAnsweredBy says which request on the wire answered a fetch whose
// answer came from a request it did not send. Put on the fetch's context by
// externalFetchCache.do; set when an attempt joins and is answered, cleared by
// every attempt that sends its own, so at the fetch's end it describes the
// attempt whose answer the fetch returned.
type externalAnsweredBy struct {
	mu     sync.Mutex
	joined bool
	ref    externalWireRef
}

type externalAnsweredByKey struct{}

// withExternalAnsweredBy returns ctx carrying a fresh record for one fetch.
func withExternalAnsweredBy(ctx context.Context) (context.Context, *externalAnsweredBy) {
	a := &externalAnsweredBy{}
	return context.WithValue(ctx, externalAnsweredByKey{}, a), a
}

func answeredBy(ctx context.Context) *externalAnsweredBy {
	a, _ := ctx.Value(externalAnsweredByKey{}).(*externalAnsweredBy)
	return a
}

func (a *externalAnsweredBy) set(ref externalWireRef) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.joined, a.ref = true, ref
	a.mu.Unlock()
}

func (a *externalAnsweredBy) clear() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.joined, a.ref = false, externalWireRef{}
	a.mu.Unlock()
}

// Wire is the request that answered this fetch, and whether another reader's
// request did (false: the answer came from a request the fetch sent itself).
// Nil-safe.
func (a *externalAnsweredBy) Wire() (ref externalWireRef, joined bool) {
	if a == nil {
		return externalWireRef{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ref, a.joined
}

// linkAnsweringRequest gives the span active on ctx a link to the wire request
// that answered the fetch, when another reader's request did and it carried a
// trace context (see the file comment). Two ids on the link, no attribute.
// Nothing is done, and nothing allocated, otherwise: tracing off, or a fetch
// answered by its own request.
func linkAnsweringRequest(ctx context.Context, by *externalAnsweredBy) {
	ref, joined := by.Wire()
	if !joined || ref.traceparent == "" {
		return
	}
	if sc := ref.SpanContext(); sc.IsValid() {
		trace.SpanFromContext(ctx).AddLink(trace.Link{SpanContext: sc})
	}
}

// flightEvent is one connection-phase event of the wire request, as a joined
// attempt is classified by it.
type flightEvent struct {
	ev     credhttp.SharedAttemptEvent
	failed bool
}

// trace is the wire request's own hooks: the freshness contract's first-byte
// close, and the connection-phase events its joiners' attempts are classified
// by. The hooks of the reader that started it are already on f.ctx and run
// natively: that is where the connection is counted and the answer timed,
// once.
func (f *externalFlight) trace() *httptrace.ClientTrace {
	emit := func(ev credhttp.SharedAttemptEvent, failed bool) {
		f.traceMu.Lock()
		f.events = append(f.events, flightEvent{ev, failed})
		for _, ctx := range f.joiners {
			credhttp.ObserveSharedAttempt(ctx, ev, failed)
		}
		f.traceMu.Unlock()
	}
	return &httptrace.ClientTrace{
		// The trace context as it goes on the wire is what identifies this
		// request to its upstream (externalWireRef). No other header is read,
		// and none is handed to a joiner.
		WroteHeaderField: func(key string, value []string) {
			if len(value) == 0 {
				return
			}
			switch {
			case strings.EqualFold(key, "traceparent"):
				f.traceMu.Lock()
				f.ref.traceparent = value[len(value)-1]
				f.traceMu.Unlock()
			case strings.EqualFold(key, "tracestate"):
				f.traceMu.Lock()
				f.ref.tracestate = strings.Join(value, ",")
				f.traceMu.Unlock()
			}
		},
		GotFirstResponseByte: func() {
			// The freshness contract: once response bytes arrive, a read
			// arriving later would be handed bytes received before it arrived.
			f.c.closeJoins(f)
		},
		DNSStart:          func(httptrace.DNSStartInfo) { emit(credhttp.SharedDNSStart, false) },
		DNSDone:           func(i httptrace.DNSDoneInfo) { emit(credhttp.SharedDNSDone, i.Err != nil) },
		ConnectStart:      func(_, _ string) { emit(credhttp.SharedConnectStart, false) },
		ConnectDone:       func(_, _ string, err error) { emit(credhttp.SharedConnectDone, err != nil) },
		TLSHandshakeStart: func() { emit(credhttp.SharedTLSStart, false) },
		TLSHandshakeDone:  func(_ tls.ConnectionState, err error) { emit(credhttp.SharedTLSDone, err != nil) },
		GotConn:           func(httptrace.GotConnInfo) { emit(credhttp.SharedGotConn, false) },
		WroteHeaders:      func() { emit(credhttp.SharedWroteHeaders, false) },
	}
}

// tell gives a joiner's attempt (on ctx) every connection event f's wire
// request has had, then keeps it for the events to come.
func (f *externalFlight) tell(ctx context.Context) {
	f.traceMu.Lock()
	for _, e := range f.events {
		credhttp.ObserveSharedAttempt(ctx, e.ev, e.failed)
	}
	f.joiners = append(f.joiners, ctx)
	f.traceMu.Unlock()
}

// exactEndpointKeyOf is credhttp.ExactEndpointKeyOf; a var so the fail-closed
// arm can drive the inexact branch from this package.
var exactEndpointKeyOf = credhttp.ExactEndpointKeyOf

// externalReadKeyOf is the key of an outbound READ: a digest of everything
// httpFetchConverting sends besides the per-request correlation headers (see
// the file comment). ok=false for a write, and for a request whose endpoint the
// key cannot fold exactly: neither is shared.
//
// NO TWO REQUESTS CAN FOLD TO THE SAME BYTES, BY CONSTRUCTION. The components
// are written in one fixed order. A string is written as its length then its
// bytes; a list as its count then its elements; something that may be absent
// as a presence byte then, when present, its value. So no value can be read
// as another component, as a separator, or as "absent": there is no sentinel
// text anywhere.
func externalReadKeyOf(ctx context.Context, opts httpcall.RequestOptions) (externalFlightKey, bool) {
	var k externalFlightKey
	verb := ptr.Deref(opts.Verb, http.MethodGet)
	if !credhttp.SafeMethod(verb) {
		return k, false
	}
	// The provenance httpClientForEndpoint and httpFetchConverting read from
	// ctx: each changes the chain built or the headers sent. Always the same
	// four inputs, each its own length-prefixed element of the endpoint key
	// (credhttp.EndpointKeyOf): the held mark, the #443 inert flag (an inert
	// read already differs by its cache.InertHeader; the flag is folded too,
	// so the wire request's ctx, its starter's, is never inert for a joiner
	// that is not), the ServiceAccount-dial mark, and the token file such a
	// dial reads (empty otherwise).
	held, saDial := credhttp.HeldCredential(ctx), cache.ServiceAccountDialFromContext(ctx)
	saTokenFile := ""
	if saDial {
		saTokenFile = dynamic.ServiceAccountTokenFile()
	}
	epKey, exact := exactEndpointKeyOf(opts.Endpoint,
		"held="+boolString(held), "inert="+boolString(cache.Inert(ctx)), "sa-dial="+boolString(saDial), "sa-token-file="+saTokenFile)
	if !exact {
		return k, false
	}
	serverURL := ""
	if opts.Endpoint != nil {
		serverURL = opts.Endpoint.ServerURL
	}

	h := sha256.New()
	var n [8]byte
	putCount := func(c int) {
		binary.BigEndian.PutUint64(n[:], uint64(c))
		h.Write(n[:])
	}
	putString := func(s string) {
		putCount(len(s))
		h.Write([]byte(s))
	}
	putPresent := func(present bool) {
		if present {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	}
	putStrings := func(ss []string) {
		putCount(len(ss))
		for _, s := range ss {
			putString(s)
		}
	}
	putString("external-read/v4") // the layout's version
	h.Write(epKey[:])             // fixed length
	putString(verb)
	putString(credhttp.RequestURL(serverURL, opts.Path))
	// Every header, in order: a later duplicate overrides an earlier one on
	// the wire, so the order is part of the request.
	putStrings(opts.Headers)
	putPresent(opts.Payload != nil)
	if opts.Payload != nil {
		putString(*opts.Payload)
	}
	// #615 — the impersonation a branch-E dial is sent as.
	imp, impersonated := ctx.Value(endpointImpersonationKey{}).(rest.ImpersonationConfig)
	putPresent(impersonated)
	if impersonated {
		putString(imp.UserName)
		putString(imp.UID)
		putStrings(imp.Groups)
		keys := make([]string, 0, len(imp.Extra))
		for ek := range imp.Extra {
			keys = append(keys, ek)
		}
		slices.Sort(keys)
		putCount(len(keys))
		for _, ek := range keys {
			putString(ek)
			putStrings(imp.Extra[ek])
		}
	}
	h.Sum(k[:0])
	return k, true
}

func boolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// externalFlightsInFlight is how many keys have a wire request in their slot
// (tests).
func (c *externalFetchCache) externalFlightsInFlight() int {
	c.flightsMu.Lock()
	defer c.flightsMu.Unlock()
	return len(c.flights)
}
