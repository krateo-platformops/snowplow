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
// one already there. So a wire request has no policy of its own: a customer
// that shares an attempt background work started gets that attempt's answer
// and applies the customer rule to it; backoff is always the reader's own.
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
// WHO MAY JOIN, AND UNTIL WHEN.
//   - A reader whose context is already done neither starts nor joins: its
//     attempt goes to the transport below, which sends nothing for a done
//     context, as without sharing.
//   - A wire request a CUSTOMER started admits joiners only until that
//     customer's page-budget deadline (owner ruling 2026-11-15, "join only
//     young calls"): one request that never answers can cost at most the
//     readers that arrive within one page budget of it. After that a reader
//     starts its own, which is what it would have done without sharing. A
//     slow upstream is therefore NOT converged on by late readers.
//   - A wire request BACKGROUND work started (no page budget) is never joined
//     by a customer: it has no page budget to bound the join by, and the
//     customer's own call is the outcome the rule promises it. Other
//     background readers join it while the reader that started it still waits.
//   - Nobody joins after the first response byte (the freshness contract,
//     external_fetch_cache.go), and nothing is reused after completion.
//   - A reader's own RETRY never re-joins the attempt it gave up on (a
//     connect cut): it is sent on its own, as its fresh connection would be.
//
// ITS LIFETIME IS ITS WAITERS'. A reader waits on the wire request under its
// own attempt's context (its page budget, its client's connection). When the
// last waiter leaves before the answer is complete, the wire request is
// cancelled and the key freed (owner ruling 2026-11-15, reversing 2026-11-10's
// "runs to its deadline"): nothing outlives the readers it was sent for.
//
// WHAT EACH WAITER GETS. The response's status line and headers (its own
// copy), and a body that reads the bytes as the wire request receives them,
// under the waiter's own context. Of a non-2xx answer only the first
// maxUnstructuredResponseTextBytes are held: all the fetch reads of one. The
// connection-phase trace events of the wire request (net/http/httptrace: DNS,
// connect, TLS, got-connection, first byte) are replayed to each joiner's own
// trace hooks, so the send policy classifies a joiner's attempt (connected or
// not, the retry cause) from what the shared attempt did.
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
// Nothing here logs a key, URL or header: the key is a sha256 digest.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sync"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
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
	// joinUntil is the page-budget deadline of the customer that started it;
	// zero when background work did.
	joinUntil time.Time

	// Under c.flightsMu.
	waiters     int
	starterGone bool // the reader that started it stopped waiting
	closed      bool // admits no more joiners

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

	// The wire request's trace events so far and the joiners' hooks they are
	// replayed to, under traceMu (held across the hook calls, so a joiner sees
	// every event once, in order).
	traceMu sync.Mutex
	events  []func(*httptrace.ClientTrace)
	traces  []*httptrace.ClientTrace
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
		return w.next.RoundTrip(req)
	}
	// A reader that is already gone starts nothing and joins nothing: the
	// transport below sends nothing for a done context.
	if req.Context().Err() != nil {
		return w.next.RoundTrip(req)
	}
	f, role := w.joinOrLead(req.Context())
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
	if f := c.flights[w.key]; f != nil && f.joinableLocked(time.Now(), customer) {
		if _, again := w.waited[f]; again {
			return nil, flightOwn
		}
		f.waiters++
		w.waited[f] = struct{}{}
		return f, flightJoin
	}
	f := &externalFlight{c: c, key: w.key, waiters: 1, headers: make(chan struct{}), more: make(chan struct{})}
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

// joinableLocked reports whether a reader arriving now may wait on f (see the
// file comment, "WHO MAY JOIN"). customer: the reader has a page budget.
func (f *externalFlight) joinableLocked(now time.Time, customer bool) bool {
	if f.closed {
		return false
	}
	if !f.joinUntil.IsZero() {
		return now.Before(f.joinUntil)
	}
	return !customer && !f.starterGone
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
func (f *externalFlight) leave(starter bool) {
	c := f.c
	c.flightsMu.Lock()
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
}

// start sends f's wire request: req, on f's own context, in a goroutine of its
// own, so no single reader's cancellation ends it while another still waits.
func (w *externalWire) start(f *externalFlight, req *http.Request) {
	wreq := req.Clone(httptrace.WithClientTrace(f.ctx, f.trace()))
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
			slog.Default().Error("external read: the shared request panicked after its last waiter left",
				slog.String("flight", hex.EncodeToString(f.key[:8])), slog.Any("panic", p))
		}
		// Raised again in every waiter, where a panic in the transport
		// surfaces without sharing: in the reader's own goroutine.
		f.panicVal = p
		if !published {
			f.panicked = true
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
	// Whatever it returned, it is over as something to join: nothing is
	// reused after completion.
	f.c.closeJoins(f)
	if err != nil {
		f.err = err
		published = true
		close(f.headers)
		return
	}
	body := resp.Body
	view := *resp
	view.Body = http.NoBody
	f.resp = &view
	published = true
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
// joiner records a flight wait and has f's trace events replayed to it.
func (f *externalFlight) await(req *http.Request, led bool) (*http.Response, error) {
	ctx := req.Context()
	var span slowcall.Span
	if !led {
		span = slowcall.StartWait(ctx, slowcall.PhaseFlightWait, "external_flight", hex.EncodeToString(f.key[:]), "")
		f.replayTo(httptrace.ContextClientTrace(ctx))
	}
	select {
	case <-f.headers:
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
		// The reader's own copy of the status line and headers.
		resp := *f.resp
		resp.Header = f.resp.Header.Clone()
		resp.Request = req
		resp.Body = &externalFlightBody{f: f, ctx: ctx, starter: led}
		return &resp, nil
	case <-ctx.Done():
		span.EndRelease(slowcall.CauseOf(ctx.Err()))
		f.leave(led)
		return nil, context.Cause(ctx)
	}
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

// trace is the wire request's own hooks: the freshness contract's first-byte
// close, and every connection-phase event recorded for the joiners. The hooks
// of the reader that started it are already on f.ctx and run natively.
// WroteHeaderField is not forwarded: a joiner is never handed another
// request's header values.
func (f *externalFlight) trace() *httptrace.ClientTrace {
	emit := func(ev func(*httptrace.ClientTrace)) {
		f.traceMu.Lock()
		f.events = append(f.events, ev)
		for _, t := range f.traces {
			ev(t)
		}
		f.traceMu.Unlock()
	}
	return &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			emit(func(t *httptrace.ClientTrace) {
				if t.GetConn != nil {
					t.GetConn(hostPort)
				}
			})
		},
		GotConn: func(info httptrace.GotConnInfo) {
			emit(func(t *httptrace.ClientTrace) {
				if t.GotConn != nil {
					t.GotConn(info)
				}
			})
		},
		GotFirstResponseByte: func() {
			// The freshness contract: once response bytes arrive, a read
			// arriving later would be handed bytes received before it arrived.
			f.c.closeJoins(f)
			emit(func(t *httptrace.ClientTrace) {
				if t.GotFirstResponseByte != nil {
					t.GotFirstResponseByte()
				}
			})
		},
		DNSStart: func(info httptrace.DNSStartInfo) {
			emit(func(t *httptrace.ClientTrace) {
				if t.DNSStart != nil {
					t.DNSStart(info)
				}
			})
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			emit(func(t *httptrace.ClientTrace) {
				if t.DNSDone != nil {
					t.DNSDone(info)
				}
			})
		},
		ConnectStart: func(network, addr string) {
			emit(func(t *httptrace.ClientTrace) {
				if t.ConnectStart != nil {
					t.ConnectStart(network, addr)
				}
			})
		},
		ConnectDone: func(network, addr string, err error) {
			emit(func(t *httptrace.ClientTrace) {
				if t.ConnectDone != nil {
					t.ConnectDone(network, addr, err)
				}
			})
		},
		TLSHandshakeStart: func() {
			emit(func(t *httptrace.ClientTrace) {
				if t.TLSHandshakeStart != nil {
					t.TLSHandshakeStart()
				}
			})
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			emit(func(t *httptrace.ClientTrace) {
				if t.TLSHandshakeDone != nil {
					t.TLSHandshakeDone(state, err)
				}
			})
		},
		WroteHeaders: func() {
			emit(func(t *httptrace.ClientTrace) {
				if t.WroteHeaders != nil {
					t.WroteHeaders()
				}
			})
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			emit(func(t *httptrace.ClientTrace) {
				if t.WroteRequest != nil {
					t.WroteRequest(info)
				}
			})
		},
	}
}

// replayTo hands a joiner's hooks every event f's wire request has had, then
// keeps them for the events to come.
func (f *externalFlight) replayTo(t *httptrace.ClientTrace) {
	if t == nil {
		return
	}
	f.traceMu.Lock()
	for _, ev := range f.events {
		ev(t)
	}
	f.traces = append(f.traces, t)
	f.traceMu.Unlock()
}

// exactEndpointKeyOf is credhttp.ExactEndpointKeyOf; a var so the fail-closed
// arm can drive the inexact branch from this package.
var exactEndpointKeyOf = credhttp.ExactEndpointKeyOf

// externalReadKeyOf is the key of an outbound READ: a digest of everything
// httpFetchConverting sends besides the per-request correlation headers (see
// the file comment). ok=false for a write, and for a request whose endpoint the
// key cannot fold exactly: neither is shared.
func externalReadKeyOf(ctx context.Context, opts httpcall.RequestOptions) (externalFlightKey, bool) {
	var k externalFlightKey
	verb := ptr.Deref(opts.Verb, http.MethodGet)
	if !credhttp.SafeMethod(verb) {
		return k, false
	}
	// The provenance httpClientForEndpoint and httpFetchConverting read from
	// ctx: each changes the chain built or the headers sent.
	held, saDial := credhttp.HeldCredential(ctx), cache.ServiceAccountDialFromContext(ctx)
	// #443 — an inert (dry-run) read already differs by its cache.InertHeader;
	// the flag is folded too, so the wire request's ctx (its starter's) is
	// never inert for a joiner that is not, or the reverse.
	prov := []string{"held=" + boolString(held), "sa-dial=" + boolString(saDial), "inert=" + boolString(cache.Inert(ctx))}
	if saDial {
		prov = append(prov, "sa-token-file:"+dynamic.ServiceAccountTokenFile())
	}
	epKey, exact := exactEndpointKeyOf(opts.Endpoint, prov...)
	if !exact {
		return k, false
	}
	serverURL := ""
	if opts.Endpoint != nil {
		serverURL = opts.Endpoint.ServerURL
	}

	h := sha256.New()
	var n [8]byte
	put := func(s string) {
		binary.BigEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	putCount := func(c int) {
		binary.BigEndian.PutUint64(n[:], uint64(c))
		h.Write(n[:])
	}
	put("external-read/v2")
	h.Write(epKey[:])
	put(verb)
	put(credhttp.RequestURL(serverURL, opts.Path))
	// Every header, in order: a later duplicate overrides an earlier one on
	// the wire, so the order is part of the request.
	putCount(len(opts.Headers))
	for _, hd := range opts.Headers {
		put(hd)
	}
	if opts.Payload == nil {
		put("payload:none")
	} else {
		put("payload:" + *opts.Payload)
	}
	// #615 — the impersonation a branch-E dial is sent as.
	if imp, ok := ctx.Value(endpointImpersonationKey{}).(rest.ImpersonationConfig); ok {
		put("impersonate")
		put(imp.UserName)
		put(imp.UID)
		putCount(len(imp.Groups))
		for _, g := range imp.Groups {
			put(g)
		}
		keys := make([]string, 0, len(imp.Extra))
		for ek := range imp.Extra {
			keys = append(keys, ek)
		}
		slices.Sort(keys)
		putCount(len(keys))
		for _, ek := range keys {
			put(ek)
			putCount(len(imp.Extra[ek]))
			for _, v := range imp.Extra[ek] {
				put(v)
			}
		}
	} else {
		put("no-impersonation")
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
