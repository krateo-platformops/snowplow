// external_fetch_flight.go — #1320 (owner ruling 2026-11-10): identical
// concurrent external READS share one in-flight call, detached from any single
// page.
//
// WHY. Measured on 057 (2026-11-10, 1.12.67, synthetic load): every page load
// opened its own identical external call (the dashboard events RESTAction's
// stage → sse-proxy /events), up to 48 at once at 50 virtual users over 3
// identities. Each was cut at its 10 s page budget, which cancelled its
// in-flight request, so no page load succeeded — while the upstream ran every
// abandoned query to completion anyway.
//
// THE KEY is the whole outbound request (externalReadKeyOf): the endpoint with
// every field (server URL, CA, client certificate and key, token, username,
// password, proxy, flags), the method, the resolved URL, EVERY header in order
// (an exported JWT rides there as Authorization), the payload, and the
// provenance that changes what is sent (a held credential, a ServiceAccount
// dial and the token file it reads, a #615 impersonation). Two identities
// therefore never share a call. The two correlation headers snowplow adds per
// request (X-Krateo-TraceId, and the W3C traceparent/baggage the tracing
// transport injects) are not part of what the stage asks for, so they are not
// in the key: a shared call carries the leader's. A key that cannot fold the
// endpoint exactly (credhttp.ExactEndpointKeyOf) shares nothing.
//
// READS ONLY. A write (any method but GET, HEAD, OPTIONS, TRACE —
// credhttp.SafeMethod, the module's one write classifier) is sent by its own
// caller, on its own context, exactly as before: never joined, never joining.
//
// THE FLIGHT. The first read of a key starts the call in a goroutine of its
// own, DETACHED from every caller (pagebudget.Detach: no request's
// cancellation, no request's page budget) and bounded by the server write
// deadline (cache.DemandRevalidationDeadline), as the #710 shared fill leader
// is (detachedFillContext). Every caller — the one that started it included —
// then WAITS, and its wait alone is bounded by its own context and its own
// page budget: a page cut at 10 s gets its own budget error (504 Timeout,
// exhausted_connect or exhausted_answer by how far the shared call had got)
// and stops waiting; the call keeps running for the others. A caller whose
// request is cancelled cannot fail anyone else.
//
// THE FRESHNESS CONTRACT (external_fetch_cache.go). A read joins only while
// the call is in flight AND before its first response byte arrives: the call
// leaves the key's slot at its first response byte (httptrace) and in any
// case when it completes. So a joiner may get an answer from a call that
// started up to one call-duration before it arrived, never bytes received
// before it arrived, and nothing is ever reused after completion: a read
// arriving after that dials afresh. No TTL, no cap, no per-endpoint rule.
//
// PER CALLER. Each caller gets its own copy of the status envelope (the body
// bytes are immutable and shared, as the conversion memo already shares them)
// and records its own outcome: the stage's item error and audit record are
// built by each caller from its own result (resolve.go), the dial's audit
// observation is copied onto each caller's own credhttp.Answer, and each
// customer's page budget notes its own exchange. The shared call's attempts
// are recorded on its starter's slow-call dial while that request is open
// (slowcall drops them once it closes); a joiner's record shows a flight wait
// on the key's digest. Nothing here logs, and no key, URL or header leaves
// this file: the key is a sha256 digest.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sync/atomic"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
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

// externalFlight is one in-flight external read that identical reads join.
type externalFlight struct {
	key  externalFlightKey
	done chan struct{}
	// connected: the call has had a connection, so a waiter its budget cuts
	// reports exhausted_answer rather than exhausted_connect.
	connected atomic.Bool

	// The call's outcome: written by the flight before done is closed, read by
	// its waiters only after.
	st       *response.Status
	body     []byte
	ct       string
	err      error
	answer   *credhttp.Answer
	panicked bool
	panicVal any
}

// fetchShared is the external read path with the cache on: a read joins or
// starts the key's flight and waits for it under its own context and budget; a
// write, or a request the key cannot identify exactly, is fetched by its own
// caller as before.
func (c *externalFetchCache) fetchShared(ctx context.Context, opts httpcall.RequestOptions) (*response.Status, []byte, string, error) {
	key, ok := externalReadKeyOf(ctx, opts)
	if !ok {
		c.flightNotShared.Add(1)
		return c.fetch(ctx, opts, c.convert)
	}
	b, _ := pagebudget.From(ctx)
	if b != nil && !time.Now().Before(b.Deadline()) {
		// #1115 — an exchange started after its budget ran out is never sent,
		// and starts nothing for anyone else (Send's own rule).
		e := &pagebudget.ExhaustedError{Result: pagebudget.NotSent}
		b.Note(e.Result)
		credhttp.AnswerFrom(ctx).Unanswered(e)
		st, _ := credhttp.BudgetStatus(e)
		return st, nil, "", e
	}
	f, lead := c.joinOrLeadFlight(key)
	if lead {
		c.flightLed.Add(1)
		go c.fly(ctx, f, cloneRequestOptions(opts))
	} else {
		c.flightJoined.Add(1)
	}
	return c.awaitFlight(ctx, f, lead, b)
}

// joinOrLeadFlight returns key's in-flight call, or installs a new one this
// caller leads.
func (c *externalFetchCache) joinOrLeadFlight(key externalFlightKey) (*externalFlight, bool) {
	c.flightsMu.Lock()
	defer c.flightsMu.Unlock()
	if f := c.flights[key]; f != nil {
		return f, false
	}
	f := &externalFlight{key: key, done: make(chan struct{})}
	c.flights[key] = f
	return f, true
}

// closeJoins takes f out of its key's slot: no read joins it from here on.
// Idempotent; a newer flight in the slot is left alone.
func (c *externalFetchCache) closeJoins(f *externalFlight) {
	c.flightsMu.Lock()
	if c.flights[f.key] == f {
		delete(c.flights, f.key)
	}
	c.flightsMu.Unlock()
}

// fly performs the call, detached from ctx's cancellation and page budget and
// bounded by the server write deadline, and publishes its outcome. ctx lends
// only its values (the dial's provenance, already folded into the key).
func (c *externalFetchCache) fly(ctx context.Context, f *externalFlight, opts httpcall.RequestOptions) {
	fctx, cancel := detachedFillContext(ctx)
	defer cancel()
	// The shared call's own audit observation: each waiter copies it.
	fctx, answer := credhttp.WithAnswer(fctx)
	fctx = httptrace.WithClientTrace(fctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { f.connected.Store(true) },
		// The freshness contract: once response bytes arrive, a read arriving
		// later would be handed bytes received before it arrived.
		GotFirstResponseByte: func() { c.closeJoins(f) },
	})
	defer func() {
		if p := recover(); p != nil {
			// Raised again in every waiter (awaitFlight), where a panic in the
			// fetch surfaced before #1320: in the caller's own goroutine.
			f.panicked, f.panicVal = true, p
		}
		f.answer = answer
		c.closeJoins(f)
		close(f.done)
	}()
	f.st, f.body, f.ct, f.err = c.fetch(fctx, opts, c.convert)
}

// awaitFlight waits for f under the caller's own context and page budget b
// (nil: none). led: this caller started f (its own dial span already covers
// the wait); a joiner records a flight wait.
func (c *externalFetchCache) awaitFlight(ctx context.Context, f *externalFlight, led bool, b *pagebudget.Budget) (*response.Status, []byte, string, error) {
	var cut <-chan time.Time
	if b != nil {
		t := time.NewTimer(time.Until(b.Deadline()))
		defer t.Stop()
		cut = t.C
	}
	var w slowcall.Span
	if !led {
		w = slowcall.StartWait(ctx, slowcall.PhaseFlightWait, "external_flight", hex.EncodeToString(f.key[:]), "")
	}
	select {
	case <-f.done:
		if f.panicked {
			w.EndRelease(slowcall.ReleaseLeaderFailed)
			panic(f.panicVal)
		}
		if f.err != nil {
			w.EndRelease(slowcall.ReleaseLeaderFailed)
		} else {
			w.End()
		}
		if b != nil && f.err == nil {
			b.Note(pagebudget.Within) // the answer reached this caller inside its budget
		}
		credhttp.AnswerFrom(ctx).Adopt(f.answer)
		var st *response.Status
		if f.st != nil {
			cp := *f.st
			st = &cp
		}
		return st, f.body, f.ct, f.err
	case <-cut:
		e := &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedConnect}
		if f.connected.Load() {
			e.Result = pagebudget.ExhaustedAnswer
		}
		b.Note(e.Result)
		credhttp.AnswerFrom(ctx).Unanswered(e)
		w.EndRelease(slowcall.CauseOf(e))
		st, _ := credhttp.BudgetStatus(e)
		return st, nil, "", e
	case <-ctx.Done():
		err := ctx.Err()
		credhttp.AnswerFrom(ctx).Unanswered(err)
		w.EndRelease(slowcall.CauseOf(err))
		return response.New(http.StatusInternalServerError, err), nil, "", err
	}
}

// cloneRequestOptions copies what the fetch may write (it appends to and
// rewrites Headers on the AWS branch), so the detached call never shares
// mutable state with the caller that started it.
func cloneRequestOptions(opts httpcall.RequestOptions) httpcall.RequestOptions {
	opts.Headers = slices.Clone(opts.Headers)
	if opts.Endpoint != nil {
		ep := *opts.Endpoint
		opts.Endpoint = &ep
	}
	return opts
}

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
	prov := []string{"held=" + boolString(held), "sa-dial=" + boolString(saDial)}
	if saDial {
		prov = append(prov, "sa-token-file:"+dynamic.ServiceAccountTokenFile())
	}
	epKey, exact := credhttp.ExactEndpointKeyOf(opts.Endpoint, prov...)
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
	put("external-read/v1")
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

// externalFlightsInFlight is how many keys have a joinable flight (tests).
func (c *externalFetchCache) externalFlightsInFlight() int {
	c.flightsMu.Lock()
	defer c.flightsMu.Unlock()
	return len(c.flights)
}
