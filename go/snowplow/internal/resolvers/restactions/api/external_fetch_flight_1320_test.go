//go:build unit
// +build unit

package api

// external_fetch_flight_1320_test.go — #1320: identical concurrent external
// reads share one request on the wire (external_fetch_flight.go).
//
// THE RULE the arms test (owner, 2026-11-15): no reader gets a worse outcome
// than its own call would have given it, and nothing is sent that would not
// have been sent without sharing. "Without sharing" is in this binary: the
// same fetch with no sharing transport (wireOwn), so the parity arms compare
// the two paths directly.
//
// Most arms drive the real fetch (externalFetchCache.do → httpFetchConverting
// → credhttp.Send) over the wire against an httptest origin, and count
// requests where they land: at the origin. The white-box arms at the end drive
// the sharing transport over a scripted transport.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/rest"
)

// ---------------------------------------------------------------- helpers

// wireOrigin is an upstream that counts what reaches it: requests, requests
// whose client went away, open connections.
type wireOrigin struct {
	srv       *httptest.Server
	requests  atomic.Int64
	cancelled atomic.Int64
	open      atomic.Int64
	stop      chan struct{}
	release   chan struct{}
	once      sync.Once
}

// newWireOrigin serves h; n is the request's sequence number, from 1.
func newWireOrigin(t *testing.T, h func(o *wireOrigin, n int64, w http.ResponseWriter, r *http.Request)) *wireOrigin {
	t.Helper()
	o := &wireOrigin{stop: make(chan struct{}), release: make(chan struct{})}
	o.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h(o, o.requests.Add(1), w, r)
	}))
	o.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			o.open.Add(1)
		case http.StateClosed, http.StateHijacked:
			o.open.Add(-1)
		}
	}
	o.srv.Start()
	t.Cleanup(func() { close(o.stop); o.srv.CloseClientConnections(); o.srv.Close() })
	return o
}

// hold blocks until the origin is opened; false when the client went away
// first (counted) or the test ended.
func (o *wireOrigin) hold(r *http.Request) bool {
	select {
	case <-o.release:
		return true
	case <-r.Context().Done():
		o.cancelled.Add(1)
		return false
	case <-o.stop:
		return false
	}
}

func (o *wireOrigin) open1() { o.once.Do(func() { close(o.release) }) }

// answer writes the JSON every arm reads: the sequence number, the credential
// and the path the request carried.
func answer(w http.ResponseWriter, r *http.Request, n int64) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"n":%d,"cred":%q,"path":%q}`, n, r.Header.Get("Authorization"), r.URL.Path)
}

// heldOriginHandler answers every request once the origin is opened.
func heldOriginHandler(o *wireOrigin, n int64, w http.ResponseWriter, r *http.Request) {
	if o.hold(r) {
		answer(w, r, n)
	}
}

// firstHangsHandler never answers request 1 and answers every other at once.
func firstHangsHandler(o *wireOrigin, n int64, w http.ResponseWriter, r *http.Request) {
	if n == 1 {
		o.hold(r)
		return
	}
	answer(w, r, n)
}

func (o *wireOrigin) read(token, path string, headers ...string) httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet), Headers: headers},
		Endpoint:    &endpoints.Endpoint{ServerURL: o.srv.URL, Token: token},
	}
}

// wireCache is a cache with sharing on and the module's default retry policy.
func wireCache(t *testing.T) *externalFetchCache {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	c := newExternalFetchCache()
	c.budget = func(int64) int64 { return 1 << 20 }
	return c
}

// customerCtx is a customer request whose page budget has `left` to run.
func customerCtx(left time.Duration) context.Context {
	return pagebudget.WithCustomer(context.Background(), time.Now().Add(left-pagebudget.PageMaxGoal))
}

type wireResult struct {
	st   *response.Status
	body []byte
	err  error
	took time.Duration
	at   time.Time
}

// outcome names a read's result the way the parity arms compare it.
func (r wireResult) outcome() string {
	var be *pagebudget.ExhaustedError
	switch {
	case errors.As(r.err, &be):
		return "504-budget(" + be.Result.String() + ")"
	case r.err != nil:
		code := 0
		if r.st != nil {
			code = r.st.Code
		}
		return fmt.Sprintf("error(code=%d cancelled=%v)", code, errors.Is(r.err, context.Canceled))
	case r.st != nil && r.st.Status == response.StatusFailure:
		return fmt.Sprintf("upstream-%d", r.st.Code)
	}
	return "ok"
}

// wireShared is the read with sharing; wireOwn is the same fetch with no
// sharing transport: what the reader's own call does.
func wireShared(c *externalFetchCache, ctx context.Context, opts httpcall.RequestOptions) wireResult {
	start := time.Now()
	st, body, _, err := c.do(ctx, opts)
	return wireResult{st: st, body: body, err: err, took: time.Since(start), at: time.Now()}
}

func wireOwn(c *externalFetchCache, ctx context.Context, opts httpcall.RequestOptions) wireResult {
	start := time.Now()
	st, body, _, err := c.fetch(ctx, opts, toJSONBytes, nil)
	return wireResult{st: st, body: body, err: err, took: time.Since(start), at: time.Now()}
}

func goShared(c *externalFetchCache, ctx context.Context, opts httpcall.RequestOptions) <-chan wireResult {
	ch := make(chan wireResult, 1)
	go func() { ch <- wireShared(c, ctx, opts) }()
	return ch
}

// eventually polls cond for up to d. It only paces an arm; the verdicts are on
// what the origin saw.
func eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func recvWire(t *testing.T, ch <-chan wireResult, what string) wireResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(20 * time.Second):
		t.Fatalf("%s: no result after 20s", what)
	}
	return wireResult{}
}

func mustWireOK(t *testing.T, r wireResult, what string) string {
	t.Helper()
	if r.outcome() != "ok" {
		t.Fatalf("%s: %s (err=%v st=%+v)", what, r.outcome(), r.err, r.st)
	}
	return string(r.body)
}

// --------------------------------------------------- sharing and identity

// N identical concurrent reads → 1 request on the wire. Every reader gets the
// one answer, and each reader's own audit observation records it.
func TestExternalFlight1320_IdenticalConcurrentReadsSendOnce(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	const n = 16
	answers := make([]*credhttp.Answer, n)
	chs := make([]<-chan wireResult, n)
	for i := 0; i < n; i++ {
		ctx, a := credhttp.WithAnswer(customerCtx(pagebudget.PageMaxGoal))
		answers[i] = a
		chs[i] = goShared(c, ctx, o.read("alice", "/events"))
	}
	eventually(5*time.Second, func() bool { return o.requests.Load() >= n || c.flightJoined.Load() == n-1 })
	o.open1()
	var first string
	for i, ch := range chs {
		body := mustWireOK(t, recvWire(t, ch, fmt.Sprintf("reader %d", i)), fmt.Sprintf("reader %d", i))
		if i == 0 {
			first = body
		} else if body != first {
			t.Errorf("reader %d got %s; reader 0 got %s", i, body, first)
		}
	}
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d requests for %d identical concurrent reads; want 1", got, n)
	}
	if led, joined := c.flightLed.Load(), c.flightJoined.Load(); led != 1 || joined != n-1 {
		t.Errorf("led=%d joined=%d; want 1 and %d (the counter must let a bench read the collapse)", led, joined, n-1)
	}
	for i, a := range answers {
		if oc, code, _ := a.Outcome(); oc != "success" || code != http.StatusOK {
			t.Errorf("reader %d's own audit observation = %s/%d; want success/200", i, oc, code)
		}
	}
}

// The /audit shape (bench run 26i): a page of widgets reading an external
// endpoint through one static credential, three distinct requests, loaded by
// many customers at once. Three requests reach the upstream.
func TestExternalFlight1320_OnePageOfWidgetsSendsOneRequestPerShape(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	shapes := []string{"/?query=rows", "/?query=summary", "/?query=facets"}
	const widgets = 20
	type pending struct {
		shape string
		ch    <-chan wireResult
	}
	var all []pending
	for i := 0; i < widgets; i++ {
		for _, s := range shapes {
			all = append(all, pending{s, goShared(c, customerCtx(pagebudget.PageMaxGoal), o.read("clickhouse", s))})
		}
	}
	total := int64(len(all))
	eventually(5*time.Second, func() bool {
		return o.requests.Load() >= total || int64(c.flightJoined.Load()) == total-int64(len(shapes))
	})
	o.open1()
	for _, p := range all {
		mustWireOK(t, recvWire(t, p.ch, p.shape), p.shape)
	}
	if got := o.requests.Load(); got != int64(len(shapes)) {
		t.Fatalf("origin saw %d requests for %d widgets x %d shapes; want %d", got, widgets, len(shapes), len(shapes))
	}
	if led, joined := c.flightLed.Load(), c.flightJoined.Load(); led != uint64(len(shapes)) || int64(joined) != total-int64(len(shapes)) {
		t.Errorf("led=%d joined=%d; want %d and %d", led, joined, len(shapes), total-int64(len(shapes)))
	}
}

// Different identities → separate requests: a different endpoint credential,
// and the same endpoint with a different exported JWT, never share; each
// reader gets the answer to its own credential.
func TestExternalFlight1320_DifferentIdentitiesSendSeparately(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	type ident struct {
		name string
		opts httpcall.RequestOptions
		cred string
	}
	idents := []ident{
		{"token-alice", o.read("alice", "/events"), "Bearer alice"},
		{"token-bob", o.read("bob", "/events"), "Bearer bob"},
		{"jwt-carol", o.read("", "/events", "Authorization: Bearer jwt-carol"), "Bearer jwt-carol"},
		{"jwt-dave", o.read("", "/events", "Authorization: Bearer jwt-dave"), "Bearer jwt-dave"},
	}
	const per = 4
	type pending struct {
		id ident
		ch <-chan wireResult
	}
	var all []pending
	for _, id := range idents {
		for j := 0; j < per; j++ {
			all = append(all, pending{id, goShared(c, customerCtx(pagebudget.PageMaxGoal), id.opts)})
		}
	}
	total := int64(len(idents) * per)
	eventually(5*time.Second, func() bool {
		return o.requests.Load() >= total || (o.requests.Load() == int64(len(idents)) && int64(c.flightJoined.Load()) == total-int64(len(idents)))
	})
	o.open1()
	for _, p := range all {
		body := mustWireOK(t, recvWire(t, p.ch, p.id.name), p.id.name)
		if !strings.Contains(body, fmt.Sprintf(`"cred":%q`, p.id.cred)) {
			t.Errorf("%s got an answer to another credential: %s", p.id.name, body)
		}
	}
	if got := o.requests.Load(); got != int64(len(idents)) {
		t.Fatalf("origin saw %d requests for %d identities; want one per identity", got, len(idents))
	}
}

// A write never shares: N identical concurrent writes are N requests, and a
// read identical in every other respect neither joins a write nor is joined.
func TestExternalFlight1320_WriteNeverShares(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	const n = 4
	var chs []<-chan wireResult
	for i := 0; i < n; i++ {
		post := o.read("alice", "/events")
		post.Verb = ptr.To(http.MethodPost)
		post.Payload = ptr.To(`{"a":1}`)
		chs = append(chs, goShared(c, context.Background(), post))
	}
	read := o.read("alice", "/events")
	read.Payload = ptr.To(`{"a":1}`)
	chs = append(chs, goShared(c, context.Background(), read))
	eventually(5*time.Second, func() bool { return o.requests.Load() >= n+1 })
	o.open1()
	for i, ch := range chs {
		mustWireOK(t, recvWire(t, ch, fmt.Sprintf("call %d", i)), fmt.Sprintf("call %d", i))
	}
	if got := o.requests.Load(); got != n+1 {
		t.Fatalf("origin saw %d requests for %d writes + 1 read; want %d (a write is never shared)", got, n, n+1)
	}
	if got := c.flightNotShared.Load(); got != n {
		t.Errorf("not_shared = %d; want %d", got, n)
	}
}

// A key that cannot fold the endpoint exactly shares nothing: identical
// concurrent reads each send their own request.
func TestExternalFlight1320_InexactKeySharesNothing(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	old := exactEndpointKeyOf
	exactEndpointKeyOf = func(ep *endpoints.Endpoint, prov ...string) (credhttp.EndpointKey, bool) {
		k, _ := old(ep, prov...)
		return k, false
	}
	t.Cleanup(func() { exactEndpointKeyOf = old })
	if _, ok := externalReadKeyOf(context.Background(), o.read("alice", "/events")); ok {
		t.Fatal("a read whose endpoint key is inexact was keyed for sharing")
	}
	const n = 3
	var chs []<-chan wireResult
	for i := 0; i < n; i++ {
		chs = append(chs, goShared(c, context.Background(), o.read("alice", "/events")))
	}
	eventually(5*time.Second, func() bool { return o.requests.Load() >= n })
	o.open1()
	for _, ch := range chs {
		mustWireOK(t, recvWire(t, ch, "reader"), "reader")
	}
	if got, ns := o.requests.Load(), c.flightNotShared.Load(); got != n || ns != n || c.flightJoined.Load() != 0 {
		t.Fatalf("requests=%d not_shared=%d joined=%d; want %d, %d, 0 (an inexact key shares nothing)", got, ns, c.flightJoined.Load(), n, n)
	}
}

// ------------------------------------------------- lifetime and join bounds

// A joiner whose page budget runs out gets its own budget error at its own
// deadline (504 Timeout, exhausted_answer: the shared attempt had a
// connection, which the joiner knows from the replayed trace events), its own
// budget notes it, its own audit observation records no answer; the request
// goes on for the reader that started it.
func TestExternalFlight1320_JoinerBudgetEndsOnlyItsOwnWait(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	opts := o.read("alice", "/events")
	leader := goShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the first reader's request never reached the origin")
	}
	const left = 300 * time.Millisecond
	deadline := time.Now().Add(left)
	bctx, ans := credhttp.WithAnswer(customerCtx(left))
	jr := recvWire(t, goShared(c, bctx, opts), "budgeted joiner")
	if c.flightJoined.Load() != 1 {
		t.Fatalf("joined = %d; want 1", c.flightJoined.Load())
	}
	if jr.outcome() != "504-budget(exhausted_answer)" {
		t.Fatalf("joiner outcome = %s (%v); want its own 504, exhausted_answer", jr.outcome(), jr.err)
	}
	if jr.st == nil || jr.st.Code != http.StatusGatewayTimeout || string(jr.st.Reason) != credhttp.BudgetStatusReason {
		t.Fatalf("joiner status = %+v; want 504 Timeout", jr.st)
	}
	if early := deadline.Sub(jr.at); early > 50*time.Millisecond {
		t.Fatalf("joiner was cut %v before its own deadline", early)
	}
	if late := jr.at.Sub(deadline); late > time.Second {
		t.Fatalf("joiner was cut %v after its own deadline", late)
	}
	if got := pagebudget.Outcome(bctx); got != pagebudget.ExhaustedAnswer {
		t.Errorf("joiner's budget outcome = %v; want exhausted_answer", got)
	}
	if oc, code, _ := ans.Outcome(); oc != "failure" || code != 0 {
		t.Errorf("joiner's own audit observation = %s/%d; want failure with no code", oc, code)
	}
	if o.cancelled.Load() != 0 {
		t.Fatal("the joiner leaving cancelled the request another reader still waits on")
	}
	o.open1()
	mustWireOK(t, recvWire(t, leader, "first reader"), "first reader")
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d requests; want 1", got)
	}
}

// A within-budget read notes `within` on its own budget, joined or not.
func TestExternalFlight1320_EveryReaderNotesItsOwnBudget(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	opts := o.read("alice", "/events")
	ctxs := []context.Context{customerCtx(pagebudget.PageMaxGoal), customerCtx(pagebudget.PageMaxGoal)}
	chs := []<-chan wireResult{goShared(c, ctxs[0], opts)}
	eventually(5*time.Second, func() bool { return o.requests.Load() == 1 })
	chs = append(chs, goShared(c, ctxs[1], opts))
	eventually(5*time.Second, func() bool { return c.flightJoined.Load() == 1 })
	o.open1()
	for i, ch := range chs {
		mustWireOK(t, recvWire(t, ch, "reader"), "reader")
		if got := pagebudget.Outcome(ctxs[i]); got != pagebudget.Within {
			t.Errorf("reader %d's budget outcome = %v; want within", i, got)
		}
	}
	if o.requests.Load() != 1 {
		t.Fatalf("origin saw %d requests; want 1", o.requests.Load())
	}
}

// The last waiter leaving cancels the request on the wire and frees the key:
// nothing outlives its readers, and the next reader sends afresh.
func TestExternalFlight1320_LastWaiterLeavingCancelsTheRequest(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, firstHangsHandler)
	opts := o.read("alice", "/events")
	r := wireShared(c, customerCtx(300*time.Millisecond), opts)
	if r.outcome() != "504-budget(exhausted_answer)" {
		t.Fatalf("outcome = %s (%v); want the reader's own 504", r.outcome(), r.err)
	}
	if !eventually(3*time.Second, func() bool {
		return o.cancelled.Load() == 1 && o.open.Load() == 0 && c.externalFlightsInFlight() == 0
	}) {
		t.Fatalf("after the only reader left: cancelled upstream=%d open connections=%d keys held=%d; want 1, 0, 0",
			o.cancelled.Load(), o.open.Load(), c.externalFlightsInFlight())
	}
	mustWireOK(t, wireShared(c, customerCtx(pagebudget.PageMaxGoal), opts), "the next reader")
	if got := o.requests.Load(); got != 2 {
		t.Fatalf("origin saw %d requests; want 2 (the next reader sends afresh)", got)
	}
}

// The reader that started a request leaves (its client disconnected) while
// another still waits: the request is not cancelled and answers the other.
func TestExternalFlight1320_AStarterLeavingLeavesItsJoinersTheRequest(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	opts := o.read("alice", "/events")
	// What a reader whose client leaves mid-request gets from its own call.
	octx, ocancel := context.WithCancel(customerCtx(pagebudget.PageMaxGoal))
	ownCh := make(chan wireResult, 1)
	go func() { ownCh <- wireOwn(c, octx, o.read("own", "/events")) }()
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("SETUP: the reader's own request never reached the origin")
	}
	ocancel()
	own := recvWire(t, ownCh, "own call")
	if !eventually(3*time.Second, func() bool { return o.cancelled.Load() == 1 }) {
		t.Fatal("SETUP: the reader's own request was not cancelled by its client leaving")
	}

	lctx, cancel := context.WithCancel(customerCtx(pagebudget.PageMaxGoal))
	defer cancel()
	leader := goShared(c, lctx, opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 2 }) {
		t.Fatal("the first reader's request never reached the origin")
	}
	joiner := goShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
	eventually(5*time.Second, func() bool { return c.flightJoined.Load() == 1 })
	cancel()
	if lr := recvWire(t, leader, "cancelled starter"); lr.outcome() != own.outcome() || lr.outcome() == "ok" {
		t.Fatalf("cancelled starter: %s (%v); its own call: %s (%v)", lr.outcome(), lr.err, own.outcome(), own.err)
	}
	time.Sleep(100 * time.Millisecond) // a cancellation that reached the wire would land here
	if o.cancelled.Load() != 1 {
		t.Fatal("the starter leaving cancelled the request its joiner still waits on")
	}
	o.open1()
	mustWireOK(t, recvWire(t, joiner, "joiner"), "joiner")
	if got := o.requests.Load(); got != 2 {
		t.Fatalf("origin saw %d requests; want 2 (the own call's and the shared one)", got)
	}
}

// JOIN ONLY YOUNG REQUESTS (owner ruling 2026-11-15). A customer's request
// admits joiners only until that customer's page-budget deadline. A reader
// arriving later sends its own request — and here gets its answer at once —
// even though the old request is still on the wire for a joiner that arrived
// in time. (So a late reader does NOT converge on a slow request: it gets its
// own call, as without sharing.)
func TestExternalFlight1320_AReaderJoinsOnlyAYoungRequest(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, firstHangsHandler)
	opts := o.read("alice", "/events")
	const starterBudget = 400 * time.Millisecond
	starter := goShared(c, customerCtx(starterBudget), opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the first reader's request never reached the origin")
	}
	jctx, jcancel := context.WithCancel(customerCtx(pagebudget.PageMaxGoal))
	defer jcancel()
	inTime := goShared(c, jctx, opts)
	if !eventually(time.Second, func() bool { return c.flightJoined.Load() == 1 }) {
		t.Fatal("SETUP: a reader inside the starter's page budget did not join")
	}
	if sr := recvWire(t, starter, "starter"); sr.outcome() != "504-budget(exhausted_answer)" {
		t.Fatalf("starter outcome = %s; want its own 504", sr.outcome())
	}
	// Past the starter's deadline; its request is still on the wire for inTime.
	late := wireShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
	if late.outcome() != "ok" || late.took > 2*time.Second {
		t.Fatalf("a reader arriving after the starter's page budget: %s after %v; want its own answer at once", late.outcome(), late.took)
	}
	if got, joined := o.requests.Load(), c.flightJoined.Load(); got != 2 || joined != 1 {
		t.Fatalf("requests=%d joined=%d; want 2 and 1 (the late reader sent its own)", got, joined)
	}
	select {
	case r := <-inTime:
		t.Fatalf("the in-time joiner was answered (%s) by a request that never answers", r.outcome())
	default:
	}
	if o.cancelled.Load() != 0 {
		t.Fatal("the old request was cancelled while a joiner still waits on it")
	}
}

// ONE STUCK REQUEST, otherwise healthy upstream, identical readers arriving
// at a fixed interval, each with a page budget: only readers that arrive
// within one page budget of the stuck request can fail. (Reviewer measurement
// on the previous design: 61 of 65 at 5 s spacing for 300 s.)
func TestExternalFlight1320_OneStuckRequestCostsAtMostOneBudgetOfReaders(t *testing.T) {
	const budget = 600 * time.Millisecond
	for _, tc := range []struct {
		name    string
		every   time.Duration
		readers int
	}{
		{"readers overlap (every budget/2.4)", 250 * time.Millisecond, 10},
		{"readers do not overlap (every 1.5 budgets)", 900 * time.Millisecond, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := wireCache(t)
			o := newWireOrigin(t, firstHangsHandler)
			type read struct {
				arrived time.Duration
				res     wireResult
			}
			reads := make([]read, tc.readers)
			var wg sync.WaitGroup
			t0 := time.Now()
			for i := 0; i < tc.readers; i++ {
				time.Sleep(time.Until(t0.Add(time.Duration(i) * tc.every)))
				wg.Add(1)
				go func() {
					defer wg.Done()
					reads[i].arrived = time.Since(t0)
					reads[i].res = wireShared(c, customerCtx(budget), o.read("alice", "/events"))
				}()
			}
			wg.Wait()
			failed := 0
			for i, r := range reads {
				if r.res.outcome() == "ok" {
					continue
				}
				failed++
				// The stuck request went out at ~t0 and its reader's deadline is
				// t0+budget: nobody arriving after that may be tied to it.
				if r.arrived > budget+100*time.Millisecond {
					t.Errorf("reader %d arrived %v after the stuck request (more than one page budget, %v) and still failed: %s",
						i, r.arrived.Round(time.Millisecond), budget, r.res.outcome())
				}
			}
			if failed == 0 {
				t.Fatal("SETUP: no reader failed; the stuck request was not exercised")
			}
			if most := int(budget/tc.every) + 1; failed > most {
				t.Errorf("%d of %d readers failed; at most %d arrive within one page budget", failed, tc.readers, most)
			}
			t.Logf("failed %d of %d; origin requests %d", failed, tc.readers, o.requests.Load())
		})
	}
}

// Many distinct reads against an upstream that never answers: once their
// readers are gone, nothing is left open. (Reviewer measurement on the
// previous design: 200 connections held for 300 s.)
func TestExternalFlight1320_HungReadsLeaveNothingOpenOnceTheirReadersAreGone(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, func(o *wireOrigin, _ int64, _ http.ResponseWriter, r *http.Request) { o.hold(r) })
	const n = 200
	g0 := runtime.NumGoroutine()
	var wg sync.WaitGroup
	var cut atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if wireShared(c, customerCtx(500*time.Millisecond), o.read(fmt.Sprintf("user-%d", i), "/events")).outcome() == "504-budget(exhausted_answer)" {
				cut.Add(1)
			}
		}()
	}
	wg.Wait()
	if cut.Load() != n || o.requests.Load() != n {
		t.Fatalf("SETUP: %d of %d readers cut by their budget, %d requests reached the origin", cut.Load(), n, o.requests.Load())
	}
	if !eventually(5*time.Second, func() bool {
		return o.open.Load() == 0 && c.externalFlightsInFlight() == 0 && runtime.NumGoroutine() <= g0+20
	}) {
		t.Fatalf("after every reader left: open upstream connections=%d keys held=%d goroutines above start=%d; want 0, 0, ~0",
			o.open.Load(), c.externalFlightsInFlight(), runtime.NumGoroutine()-g0)
	}
}

// A customer never joins a request background work started (it has no page
// budget to bound the join by): the customer sends its own.
func TestExternalFlight1320_ACustomerNeverJoinsABackgroundRequest(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, firstHangsHandler)
	opts := o.read("alice", "/events")
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	background := goShared(c, bctx, opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the background read's request never reached the origin")
	}
	cr := wireShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
	if cr.outcome() != "ok" || cr.took > 2*time.Second {
		t.Fatalf("customer: %s after %v; want its own answer at once (it must not wait on the background request)", cr.outcome(), cr.took)
	}
	if got, joined := o.requests.Load(), c.flightJoined.Load(); got != 2 || joined != 0 {
		t.Fatalf("requests=%d joined=%d; want 2 and 0", got, joined)
	}
	select {
	case r := <-background:
		t.Fatalf("the background read ended: %s", r.outcome())
	default:
	}
}

// Background reads share a background request while the reader that started
// it still waits; once it has left, a new background reader sends its own.
func TestExternalFlight1320_BackgroundJoinsBackgroundWhileItsStarterWaits(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, firstHangsHandler)
	opts := o.read("alice", "/events")
	sctx, scancel := context.WithCancel(context.Background())
	defer scancel()
	starter := goShared(c, sctx, opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the first request never reached the origin")
	}
	jctx, jcancel := context.WithCancel(context.Background())
	defer jcancel()
	joiner := goShared(c, jctx, opts)
	if !eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 }) {
		t.Fatalf("a background read did not join the background request (requests=%d)", o.requests.Load())
	}
	scancel()
	recvWire(t, starter, "starter")
	late := wireShared(c, context.Background(), opts)
	if late.outcome() != "ok" {
		t.Fatalf("a background read arriving after the starter left: %s; want its own answer", late.outcome())
	}
	if got := o.requests.Load(); got != 2 {
		t.Fatalf("origin saw %d requests; want 2", got)
	}
	select {
	case r := <-joiner:
		t.Fatalf("the joiner ended: %s", r.outcome())
	default:
	}
}

// Background work may join a young customer request.
func TestExternalFlight1320_BackgroundJoinsAYoungCustomerRequest(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	opts := o.read("alice", "/events")
	customer := goShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the customer's request never reached the origin")
	}
	background := goShared(c, context.Background(), opts)
	eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 })
	o.open1()
	mustWireOK(t, recvWire(t, customer, "customer"), "customer")
	mustWireOK(t, recvWire(t, background, "background"), "background")
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d requests; want 1", got)
	}
}

// A read arriving after a request completed → a fresh request, which carries
// the upstream's change: nothing is reused after completion.
func TestExternalFlight1320_ReadAfterCompletionSendsFresh(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, func(_ *wireOrigin, n int64, w http.ResponseWriter, r *http.Request) { answer(w, r, n) })
	opts := o.read("alice", "/events")
	b1 := mustWireOK(t, wireShared(c, context.Background(), opts), "first read")
	b2 := mustWireOK(t, wireShared(c, context.Background(), opts), "second read")
	if got := o.requests.Load(); got != 2 {
		t.Fatalf("origin saw %d requests for 2 sequential reads; want 2", got)
	}
	if !strings.Contains(b1, `"n":1`) || !strings.Contains(b2, `"n":2`) {
		t.Fatalf("reads got %s then %s; want answers 1 then 2", b1, b2)
	}
	if c.externalFlightsInFlight() != 0 {
		t.Fatal("a completed request still holds its key")
	}
}

// The freshness contract's strict half: a read arriving after the request's
// first response byte does not join it (it would be handed bytes received
// before it arrived); it sends afresh.
func TestExternalFlight1320_NoJoinAfterFirstResponseByte(t *testing.T) {
	c := wireCache(t)
	var headersSent atomic.Int64
	o := newWireOrigin(t, func(o *wireOrigin, n int64, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		headersSent.Add(1)
		if o.hold(r) {
			fmt.Fprintf(w, `{"n":%d}`, n)
		}
	})
	opts := o.read("alice", "/events")
	first := goShared(c, context.Background(), opts)
	if !eventually(5*time.Second, func() bool { return headersSent.Load() == 1 }) {
		t.Fatal("the origin never sent the first request's headers")
	}
	eventually(2*time.Second, func() bool { return c.externalFlightsInFlight() == 0 })
	second := goShared(c, context.Background(), opts)
	eventually(2*time.Second, func() bool { return o.requests.Load() >= 2 })
	o.open1()
	mustWireOK(t, recvWire(t, first, "first"), "first")
	mustWireOK(t, recvWire(t, second, "second"), "second")
	if got := o.requests.Load(); got != 2 {
		t.Fatalf("origin saw %d requests; want 2 (a read after the first response byte must not join)", got)
	}
}

// ------------------------------------- parity with the reader's own call

// A reader whose context is already done sends nothing, starts nothing and
// joins nothing, exactly as its own call. (Review B1: the previous design
// started a request and left it open.)
func TestExternalFlight1320_ADoneContextSendsNothing(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	opts := o.read("alice", "/events")
	done := func() context.Context {
		ctx, cancel := context.WithCancel(customerCtx(pagebudget.PageMaxGoal))
		cancel()
		return ctx
	}
	own := wireOwn(c, done(), opts)
	shared := wireShared(c, done(), opts)
	time.Sleep(200 * time.Millisecond)
	if own.outcome() != shared.outcome() {
		t.Errorf("outcome with sharing = %s; the reader's own call = %s", shared.outcome(), own.outcome())
	}
	if o.requests.Load() != 0 || o.open.Load() != 0 {
		t.Fatalf("a done context reached the origin: requests=%d open connections=%d; want 0, 0", o.requests.Load(), o.open.Load())
	}
	if led, joined, held := c.flightLed.Load(), c.flightJoined.Load(), c.externalFlightsInFlight(); led != 0 || joined != 0 || held != 0 {
		t.Fatalf("a done context led=%d joined=%d, keys held=%d; want 0, 0, 0", led, joined, held)
	}
	// And it does not join a request already on the wire either.
	live := goShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
	eventually(5*time.Second, func() bool { return o.requests.Load() == 1 })
	wireShared(c, done(), opts)
	if c.flightJoined.Load() != 0 {
		t.Fatal("a done context joined a request on the wire")
	}
	o.open1()
	mustWireOK(t, recvWire(t, live, "live reader"), "live reader")
}

// A request whose page budget is already spent sends nothing and starts
// nothing for anyone else (#1115: not_sent).
func TestExternalFlight1320_SpentBudgetSendsNothing(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	r := wireShared(c, customerCtx(-time.Second), o.read("alice", "/events"))
	if r.outcome() != "504-budget(not_sent)" {
		t.Fatalf("outcome = %s (%v); want not_sent", r.outcome(), r.err)
	}
	if got := o.requests.Load(); got != 0 || c.externalFlightsInFlight() != 0 || c.flightLed.Load() != 0 {
		t.Fatalf("a spent budget sent %d requests / started a shared request", got)
	}
}

// AN UPSTREAM THAT FAILS FAST, at the module's default retry policy: the
// customer gets the upstream's real failure, after the same number of
// attempts and at the same time as its own call. (Review B2: the previous
// design answered 504 after the whole 10 s page budget.) Then several
// customers at once: each still gets that outcome, no later, and the upstream
// sees no more requests than their own calls would have sent.
func TestExternalFlight1320_AFastFailingUpstreamAnswersAsTheReadersOwnCall(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	for _, tc := range []struct {
		name    string
		handler func(w http.ResponseWriter)
		refused bool
	}{
		{"503", func(w http.ResponseWriter) { w.WriteHeader(http.StatusServiceUnavailable) }, false},
		{"429", func(w http.ResponseWriter) { w.WriteHeader(http.StatusTooManyRequests) }, false},
		{"connection refused", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newExternalFetchCache()
			c.budget = func(int64) int64 { return 1 << 20 }
			o := newWireOrigin(t, func(_ *wireOrigin, _ int64, w http.ResponseWriter, _ *http.Request) { tc.handler(w) })
			opts := o.read("alice", "/events")
			if tc.refused {
				l, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				opts.Endpoint.ServerURL = "http://" + l.Addr().String()
				l.Close()
			}
			own := wireOwn(c, customerCtx(pagebudget.PageMaxGoal), opts)
			ownRequests := o.requests.Load()
			if strings.HasPrefix(own.outcome(), "504-budget") || own.outcome() == "ok" {
				t.Fatalf("SETUP: the reader's own call ended %s; the arm needs the upstream's own failure", own.outcome())
			}

			shared := wireShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
			sharedRequests := o.requests.Load() - ownRequests
			if shared.outcome() != own.outcome() {
				t.Errorf("outcome with sharing = %s; the reader's own call = %s", shared.outcome(), own.outcome())
			}
			if sharedRequests != ownRequests {
				t.Errorf("attempts with sharing = %d; the reader's own call = %d", sharedRequests, ownRequests)
			}
			if d := shared.took - own.took; d > 700*time.Millisecond {
				t.Errorf("with sharing the answer took %v; the reader's own call %v", shared.took.Round(time.Millisecond), own.took.Round(time.Millisecond))
			}

			const n = 6
			before := o.requests.Load()
			chs := make([]<-chan wireResult, n)
			for i := range chs {
				chs[i] = goShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
			}
			for i, ch := range chs {
				r := recvWire(t, ch, "concurrent reader")
				if r.outcome() != own.outcome() {
					t.Errorf("concurrent reader %d: %s; the reader's own call = %s", i, r.outcome(), own.outcome())
				}
				if d := r.took - own.took; d > time.Second {
					t.Errorf("concurrent reader %d took %v; its own call %v", i, r.took.Round(time.Millisecond), own.took.Round(time.Millisecond))
				}
			}
			if sent := o.requests.Load() - before; sent > n*ownRequests {
				t.Errorf("%d concurrent readers sent %d requests; their own calls would send %d", n, sent, n*ownRequests)
			}
			t.Logf("%s: own call %s after %v in %d attempts; with sharing %s after %v in %d",
				tc.name, own.outcome(), own.took.Round(time.Millisecond), ownRequests, shared.outcome(), shared.took.Round(time.Millisecond), sharedRequests)
		})
	}
}

// A page budget that runs out while the body is still arriving, and a non-2xx
// answer with a long body: the same outcome and the same message as the
// reader's own call.
func TestExternalFlight1320_BodyHandlingMatchesTheReadersOwnCall(t *testing.T) {
	c := wireCache(t)
	long := strings.Repeat("x", 10_000)
	o := newWireOrigin(t, func(o *wireOrigin, n int64, w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/slow-body":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"a":`)
			w.(http.Flusher).Flush()
			o.hold(r)
		case "/long-error":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, long)
		}
	})
	for _, tc := range []struct {
		path string
		left time.Duration
	}{{"/slow-body", 400 * time.Millisecond}, {"/long-error", pagebudget.PageMaxGoal}} {
		own := wireOwn(c, customerCtx(tc.left), o.read("alice", tc.path))
		shared := wireShared(c, customerCtx(tc.left), o.read("alice", tc.path))
		if own.outcome() != shared.outcome() {
			t.Errorf("%s: outcome with sharing = %s; the reader's own call = %s", tc.path, shared.outcome(), own.outcome())
		}
		if own.st != nil && shared.st != nil && (own.st.Message != shared.st.Message || own.st.Code != shared.st.Code) {
			t.Errorf("%s: status with sharing = %d %q; the reader's own call = %d %q", tc.path, shared.st.Code, shared.st.Message, own.st.Code, own.st.Message)
		}
		if (own.st == nil) != (shared.st == nil) {
			t.Errorf("%s: status with sharing = %+v; the reader's own call = %+v", tc.path, shared.st, own.st)
		}
	}
}

// THROUGH THE REAL RESOLVE (review B1): an iterator read of 40 items, one at a
// time, against an upstream that holds every answer. The client leaves while
// item 1 is on the wire. Nothing more reaches the upstream, nothing stays
// open, and the stage stops dispatching items.
func TestExternalFlight1320_AnIteratorWhoseClientLeftSendsNothingMore(t *testing.T) {
	const n = 40
	var open atomic.Int64
	srv := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
		open.Add(1)
		<-r.Context().Done()
		open.Add(-1)
	})
	defer srv.srv.CloseClientConnections()
	x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(srv.srv.URL)})
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	prefix := fmt.Sprintf("/gone%d/", time.Now().UnixNano())
	st := x1106Stage("each", http.MethodGet, `${ "`+prefix+`" + .p }`)
	st.ContinueOnError = ptr.To(true)
	st.DependsOn = &templates.Dependency{Name: "", Iterator: ptr.To(`[ .ps[] | {p: .} ]`)}
	items := make([]any, n)
	for i := range items {
		items[i] = fmt.Sprintf("item-%d", i)
	}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: x1106User, Groups: []string{"tenants"}}))
	ctx = pagebudget.WithCustomer(ctx, time.Now())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sent := func() int {
		k := 0
		for _, h := range srv.all() {
			if strings.HasPrefix(h.Path, prefix) {
				k++
			}
		}
		return k
	}
	dispatched := func() uint64 {
		var k uint64
		for _, c := range ExternalFetchSnapshot() {
			k += c.Count
		}
		return k
	}
	d0 := dispatched()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ResolveWithFailure(ctx, ResolveOptions{
			RC: &rest.Config{Host: "http://127.0.0.1:1"}, AuthnNS: x1106NS, Items: []*templates.API{st},
			Extras: map[string]any{"ps": items}, RESTActionNamespace: x1106NS, RESTActionName: "ra-1320-gone",
		})
	}()
	if !eventually(5*time.Second, func() bool { return sent() >= 1 }) {
		t.Fatal("SETUP: item 1 never reached the upstream")
	}
	atCancel := sent()
	cancel() // the client leaves
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("the resolve did not return after the client left")
	}
	time.Sleep(700 * time.Millisecond)
	if got := sent(); got != atCancel {
		t.Errorf("upstream requests: %d when the client left, %d afterwards; want no more", atCancel, got)
	}
	if got := open.Load(); got != 0 {
		t.Errorf("%d upstream requests still open after the client left; want 0", got)
	}
	// The loop itself stops: at most the item in flight and the one already
	// queued behind it are dispatched, not all 40.
	if got := dispatched() - d0; got > 3 {
		t.Errorf("the stage dispatched %d of %d items for a client that left during item 1; want it to stop", got, n)
	}
}

// END TO END through api.Resolve: concurrent resolves of the same external
// stage by the same identity reach the origin once, and each resolve records
// the answer in its own result.
func TestExternalFlight1320_E2E_ConcurrentResolvesSendOnce(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ac6FailFast(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	o := newWireOrigin(t, heldOriginHandler)
	joinedBefore := externalFetches.flightJoined.Load()
	// A path unique to this run: the process-wide map holds a key only while a
	// request is on the wire, never after.
	path := fmt.Sprintf("/events-%d", time.Now().UnixNano())
	const n = 6
	out := make(chan map[string]any, n)
	for i := 0; i < n; i++ {
		// Each resolve gets its own spec, as each /call does: runStage fills a
		// nil Headers in place on the spec it is handed.
		stage := &templates.API{Name: "ev", Path: path, Verb: ptr.To(http.MethodGet), Filter: ptr.To(".ev")}
		go func() { out <- runAC6Resolve(&ac6Fixture{server: o.srv}, []*templates.API{stage}) }()
	}
	eventually(5*time.Second, func() bool {
		return o.requests.Load() >= n || externalFetches.flightJoined.Load()-joinedBefore == n-1
	})
	o.open1()
	for i := 0; i < n; i++ {
		select {
		case dict := <-out:
			got, ok := dict["ev"].(map[string]any)
			if !ok || got["path"] != path {
				t.Fatalf("resolve: dict[ev] = %#v", dict["ev"])
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a resolve did not finish")
		}
	}
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d requests for %d concurrent identical resolves; want 1 (resolve.go must reach the sharing)", got, n)
	}
}

// ------------------------------------------------------------------ the key

func keyBase() httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/events", Verb: ptr.To(http.MethodGet), Headers: []string{"X-A: 1", "X-B: 2"}},
		Endpoint:    &endpoints.Endpoint{ServerURL: "https://ext.example", Token: "t"},
	}
}

func mustKey(t *testing.T, ctx context.Context, o httpcall.RequestOptions) externalFlightKey {
	t.Helper()
	k, ok := externalReadKeyOf(ctx, o)
	if !ok {
		t.Fatal("a read was not keyed")
	}
	return k
}

// The key folds every input that changes what is sent: each variation below
// differs from the base in exactly one, and none may share its key.
func TestExternalFlight1320_KeyFoldsEveryRequestInput(t *testing.T) {
	ctx := context.Background()
	k0 := mustKey(t, ctx, keyBase())
	if mustKey(t, ctx, keyBase()) != k0 {
		t.Fatal("identical requests keyed differently")
	}
	vary := map[string]func(o *httpcall.RequestOptions, c *context.Context){
		"path":         func(o *httpcall.RequestOptions, _ *context.Context) { o.Path = "/events?ns=a" },
		"method":       func(o *httpcall.RequestOptions, _ *context.Context) { o.Verb = ptr.To(http.MethodHead) },
		"header value": func(o *httpcall.RequestOptions, _ *context.Context) { o.Headers = []string{"X-A: 1", "X-B: 3"} },
		"header order": func(o *httpcall.RequestOptions, _ *context.Context) { o.Headers = []string{"X-B: 2", "X-A: 1"} },
		"header count": func(o *httpcall.RequestOptions, _ *context.Context) { o.Headers = []string{"X-A: 1"} },
		"exported jwt": func(o *httpcall.RequestOptions, _ *context.Context) {
			o.Headers = append(o.Headers, "Authorization: Bearer j")
		},
		"payload":       func(o *httpcall.RequestOptions, _ *context.Context) { o.Payload = ptr.To("{}") },
		"empty payload": func(o *httpcall.RequestOptions, _ *context.Context) { o.Payload = ptr.To("") },
		"held":          func(_ *httpcall.RequestOptions, c *context.Context) { *c = credhttp.WithHeldCredential(*c) },
		"sa dial":       func(_ *httpcall.RequestOptions, c *context.Context) { *c = cache.WithServiceAccountDial(*c) },
		"inert":         func(_ *httpcall.RequestOptions, c *context.Context) { *c = cache.WithInert(*c) },
		"impersonation": func(_ *httpcall.RequestOptions, c *context.Context) {
			*c = withEndpointImpersonation(*c, rest.ImpersonationConfig{UserName: "cohort"})
		},
	}
	for name, mut := range vary {
		o := keyBase()
		vctx := ctx
		mut(&o, &vctx)
		if mustKey(t, vctx, o) == k0 {
			t.Errorf("%s: the key does not fold it — two different requests would share one request on the wire", name)
		}
	}
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		o := keyBase()
		o.Verb = ptr.To(m)
		if _, ok := externalReadKeyOf(ctx, o); ok {
			t.Errorf("%s was keyed for sharing; a write must never be shared", m)
		}
	}
}

// Every field of the endpoint is in the key, whatever plumbing adds: each
// field is set in turn, by reflection.
func TestExternalFlight1320_KeyFoldsEveryEndpointField(t *testing.T) {
	ctx := context.Background()
	k0 := mustKey(t, ctx, keyBase())
	typ := reflect.TypeOf(endpoints.Endpoint{})
	for i := 0; i < typ.NumField(); i++ {
		o := keyBase()
		f := reflect.ValueOf(o.Endpoint).Elem().Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(f.String() + "-other")
		case reflect.Bool:
			f.SetBool(!f.Bool())
		default:
			t.Fatalf("endpoints.Endpoint.%s is a %s: the key folds only strings and bools exactly, so nothing may be shared (exactEndpointKeyOf must refuse)", typ.Field(i).Name, f.Kind())
		}
		if mustKey(t, ctx, o) == k0 {
			t.Errorf("endpoints.Endpoint.%s is not in the key", typ.Field(i).Name)
		}
	}
}

// Two impersonations that differ only in a VALUE never share a key: groups are
// the cohort identity on the refresher's dials.
func TestExternalFlight1320_KeyFoldsEveryImpersonationValue(t *testing.T) {
	key := func(c rest.ImpersonationConfig) externalFlightKey {
		return mustKey(t, withEndpointImpersonation(context.Background(), c), keyBase())
	}
	for name, pair := range map[string][2]rest.ImpersonationConfig{
		"user":        {{UserName: "a"}, {UserName: "b"}},
		"group value": {{UserName: "u", Groups: []string{"g1"}}, {UserName: "u", Groups: []string{"g2"}}},
		"group count": {{UserName: "u", Groups: []string{"g1"}}, {UserName: "u", Groups: []string{"g1", "g2"}}},
		"group order": {{UserName: "u", Groups: []string{"g1", "g2"}}, {UserName: "u", Groups: []string{"g2", "g1"}}},
		"uid":         {{UserName: "u", UID: "1"}, {UserName: "u", UID: "2"}},
		"extra value": {{UserName: "u", Extra: map[string][]string{"k": {"v1"}}}, {UserName: "u", Extra: map[string][]string{"k": {"v2"}}}},
		"extra key":   {{UserName: "u", Extra: map[string][]string{"k1": {"v"}}}, {UserName: "u", Extra: map[string][]string{"k2": {"v"}}}},
		"extra count": {{UserName: "u", Extra: map[string][]string{"k": {"v"}}}, {UserName: "u", Extra: map[string][]string{"k": {"v", "w"}}}},
		"extra keys":  {{UserName: "u", Extra: map[string][]string{"k": {"v"}}}, {UserName: "u", Extra: map[string][]string{"k": {"v"}, "l": {"v"}}}},
	} {
		if key(pair[0]) == key(pair[1]) {
			t.Errorf("%s: two different impersonations share one key", name)
		}
	}
	a := key(rest.ImpersonationConfig{UserName: "u", Extra: map[string][]string{"a": {"1"}, "b": {"2"}}})
	b := key(rest.ImpersonationConfig{UserName: "u", Extra: map[string][]string{"b": {"2"}, "a": {"1"}}})
	if a != b {
		t.Error("the same extras key differently (map iteration order leaked into the key)")
	}
}

// A ServiceAccount dial presents the token it reads from a file: the file is
// in the key.
func TestExternalFlight1320_KeyFoldsTheServiceAccountTokenFile(t *testing.T) {
	ctx := cache.WithServiceAccountDial(context.Background())
	restore := dynamic.SetServiceAccountTokenPathForTest(t.TempDir() + "/token-a")
	ka := mustKey(t, ctx, keyBase())
	restore()
	restore = dynamic.SetServiceAccountTokenPathForTest(t.TempDir() + "/token-b")
	kb := mustKey(t, ctx, keyBase())
	restore()
	if ka == kb {
		t.Fatal("two ServiceAccount dials reading different token files share one key")
	}
}

// The key enumerates the request's fields by hand (path, verb, headers,
// payload, endpoint). A field plumbing adds to the request must fail here,
// not be silently left out of the key.
func TestExternalFlight1320_KeyCoversEveryRequestField(t *testing.T) {
	fields := func(v any) []string {
		typ := reflect.TypeOf(v)
		var out []string
		for i := 0; i < typ.NumField(); i++ {
			out = append(out, typ.Field(i).Name)
		}
		return out
	}
	if got, want := fields(httpcall.RequestInfo{}), []string{"Path", "Verb", "Headers", "Payload"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("httpcall.RequestInfo has fields %v; externalReadKeyOf folds %v. Fold the new field (or state why it changes nothing on the wire), then update this list", got, want)
	}
	// ResponseHandler, ErrorKey and ContinueOnError do not change the request.
	if got, want := fields(httpcall.RequestOptions{}), []string{"RequestInfo", "Endpoint", "ResponseHandler", "ErrorKey", "ContinueOnError"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("httpcall.RequestOptions has fields %v; externalReadKeyOf knows %v. Fold the new field (or state why it changes nothing on the wire), then update this list", got, want)
	}
}

// --------------------------------------------------- white box: the transport

// scriptedWire is the transport below the sharing, scripted.
type scriptedWire struct {
	calls atomic.Int64
	fn    func(n int64, req *http.Request) (*http.Response, error)
}

func (s *scriptedWire) RoundTrip(req *http.Request) (*http.Response, error) {
	return s.fn(s.calls.Add(1), req)
}

func scriptedOK(req *http.Request, body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Proto: "HTTP/1.1",
		Header: http.Header{"Content-Type": {"application/json"}, "X-Seq": {"1"}},
		Body:   io.NopCloser(strings.NewReader(body)), Request: req}
}

// newReaderWire is one fetch's sharing transport over next, and its request.
func newReaderWire(t *testing.T, c *externalFetchCache, next http.RoundTripper, ctx context.Context) (*externalWire, *http.Request) {
	t.Helper()
	opts := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/events", Verb: ptr.To(http.MethodGet)},
		Endpoint:    &endpoints.Endpoint{ServerURL: "http://upstream.invalid", Token: "t"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://upstream.invalid/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	return c.share(ctx, opts)(next, req).(*externalWire), req
}

type tripResult struct {
	resp     *http.Response
	err      error
	panicked any
}

func goTrip(w *externalWire, req *http.Request) <-chan tripResult {
	ch := make(chan tripResult, 1)
	go func() {
		var r tripResult
		defer func() {
			r.panicked = recover()
			ch <- r
		}()
		r.resp, r.err = w.RoundTrip(req)
	}()
	return ch
}

// blockingWire holds every request until released or its context ends.
func blockingWire(release <-chan struct{}, then func(n int64, req *http.Request) (*http.Response, error)) *scriptedWire {
	return &scriptedWire{fn: func(n int64, req *http.Request) (*http.Response, error) {
		select {
		case <-release:
		case <-req.Context().Done():
			if then == nil {
				return nil, req.Context().Err()
			}
		}
		if then == nil {
			return scriptedOK(req, `{"n":1}`), nil
		}
		return then(n, req)
	}}
}

// Each waiter gets its own copy of the status line and headers, and reads the
// whole body.
func TestExternalFlight1320_EachWaiterGetsItsOwnResponse(t *testing.T) {
	c := wireCache(t)
	release := make(chan struct{})
	next := blockingWire(release, nil)
	w1, r1 := newReaderWire(t, c, next, context.Background())
	w2, r2 := newReaderWire(t, c, next, context.Background())
	t1 := goTrip(w1, r1)
	eventually(2*time.Second, func() bool { return next.calls.Load() == 1 })
	t2 := goTrip(w2, r2)
	eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 })
	close(release)
	a, b := <-t1, <-t2
	if a.err != nil || b.err != nil || next.calls.Load() != 1 {
		t.Fatalf("errs %v / %v, %d requests below; want none and 1", a.err, b.err, next.calls.Load())
	}
	if a.resp == b.resp {
		t.Fatal("two waiters were handed the same *http.Response")
	}
	a.resp.Header.Set("X-Seq", "changed-by-a")
	if got := b.resp.Header.Get("X-Seq"); got != "1" {
		t.Fatalf("one waiter's header change reached another: X-Seq = %q", got)
	}
	if a.resp.Request != r1 || b.resp.Request != r2 {
		t.Error("a waiter's response does not name its own request")
	}
	for i, r := range []tripResult{a, b} {
		body, err := io.ReadAll(r.resp.Body)
		if err != nil || string(body) != `{"n":1}` {
			t.Errorf("waiter %d read %q, %v", i, body, err)
		}
		r.resp.Body.Close()
	}
}

// A request that completed is out of its key's slot at once, before its
// waiters have left: a read arriving then is not handed a finished outcome.
func TestExternalFlight1320_ACompletedRequestIsNeverJoined(t *testing.T) {
	c := wireCache(t)
	failed := errors.New("upstream connection failed")
	next := &scriptedWire{fn: func(int64, *http.Request) (*http.Response, error) { return nil, failed }}
	w, req := newReaderWire(t, c, next, context.Background())
	f, role := w.joinOrLead(req.Context())
	if role != flightLead {
		t.Fatalf("role = %v; want lead", role)
	}
	w.start(f, req)
	<-f.headers // completed; its one waiter has not left yet
	if got := c.externalFlightsInFlight(); got != 0 {
		t.Fatalf("a completed request still holds its key (%d held): a read arriving now would be handed its outcome", got)
	}
	if _, err := f.await(req, true); !errors.Is(err, failed) {
		t.Fatalf("the waiter got %v; want the request's own error", err)
	}
}

// The freshness contract on its own: from the request's first response byte
// on — before its status line is even back — no read joins it.
func TestExternalFlight1320_NoJoinFromTheFirstResponseByteOn(t *testing.T) {
	c := wireCache(t)
	firstByte := make(chan struct{})
	release := make(chan struct{})
	next := &scriptedWire{}
	next.fn = func(n int64, req *http.Request) (*http.Response, error) {
		if n == 1 {
			httptrace.ContextClientTrace(req.Context()).GotFirstResponseByte()
			close(firstByte)
			<-release // the first byte is in; the status line is not back yet
		}
		return scriptedOK(req, fmt.Sprintf(`{"n":%d}`, n)), nil
	}
	w1, r1 := newReaderWire(t, c, next, context.Background())
	t1 := goTrip(w1, r1)
	<-firstByte
	w2, r2 := newReaderWire(t, c, next, context.Background())
	second := <-goTrip(w2, r2)
	if second.err != nil {
		t.Fatal(second.err)
	}
	body, _ := io.ReadAll(second.resp.Body)
	second.resp.Body.Close()
	if string(body) != `{"n":2}` || c.flightJoined.Load() != 0 || next.calls.Load() != 2 {
		t.Fatalf("a read arriving after the first response byte read %q, joined=%d, %d requests below; want its own request's answer, 0, 2",
			body, c.flightJoined.Load(), next.calls.Load())
	}
	close(release)
	if r := <-t1; r.err != nil {
		t.Fatal(r.err)
	} else {
		r.resp.Body.Close()
	}
}

// Of a non-2xx answer only what the fetch reads is held (the first
// maxUnstructuredResponseTextBytes), however long the upstream's body: a
// shared request must not hold in memory what the reader's own call never
// reads. A 2xx body is held whole.
func TestExternalFlight1320_ANon2xxBodyIsHeldOnlyAsFarAsTheFetchReadsIt(t *testing.T) {
	c := wireCache(t)
	long := strings.Repeat("x", 20*maxUnstructuredResponseTextBytes)
	for _, tc := range []struct {
		code int
		want int
	}{{http.StatusNotFound, maxUnstructuredResponseTextBytes}, {http.StatusInternalServerError, maxUnstructuredResponseTextBytes}, {http.StatusOK, len(long)}} {
		next := &scriptedWire{fn: func(_ int64, req *http.Request) (*http.Response, error) {
			resp := scriptedOK(req, long)
			resp.StatusCode = tc.code
			return resp, nil
		}}
		w, req := newReaderWire(t, c, next, context.Background())
		f, role := w.joinOrLead(req.Context())
		if role != flightLead {
			t.Fatalf("role = %v; want lead", role)
		}
		w.start(f, req)
		resp, err := f.await(req, true)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		f.mu.Lock()
		held := len(f.buf)
		f.mu.Unlock()
		if held != tc.want || len(got) != tc.want {
			t.Errorf("status %d: %d bytes held, %d read; want %d", tc.code, held, len(got), tc.want)
		}
	}
}

// A panic in the transport is raised again in every waiter, in its own
// goroutine, as without sharing.
func TestExternalFlight1320_APanicIsRaisedInEveryWaiter(t *testing.T) {
	c := wireCache(t)
	release := make(chan struct{})
	next := blockingWire(release, func(int64, *http.Request) (*http.Response, error) { panic("boom-1320") })
	w1, r1 := newReaderWire(t, c, next, context.Background())
	w2, r2 := newReaderWire(t, c, next, context.Background())
	t1 := goTrip(w1, r1)
	eventually(2*time.Second, func() bool { return next.calls.Load() == 1 })
	t2 := goTrip(w2, r2)
	eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 })
	close(release)
	for i, ch := range []<-chan tripResult{t1, t2} {
		select {
		case r := <-ch:
			if r.panicked != "boom-1320" {
				t.Errorf("waiter %d: panic = %v, err = %v; want the transport's panic raised in it", i, r.panicked, r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d never returned", i)
		}
	}
	if got := c.flightPanicsUnwaited.Load(); got != 0 {
		t.Errorf("unwaited panics = %d; want 0 (both waiters were there)", got)
	}
	if c.externalFlightsInFlight() != 0 {
		t.Error("a panicked request still holds its key")
	}
}

// A panic after the last waiter left has nobody to be raised in: it is counted
// (and logged), and does not take the process down.
func TestExternalFlight1320_APanicWithNoWaiterLeftIsCounted(t *testing.T) {
	c := wireCache(t)
	next := &scriptedWire{fn: func(_ int64, req *http.Request) (*http.Response, error) {
		<-req.Context().Done() // the last waiter left: the request is cancelled
		panic("boom-unwaited")
	}}
	ctx, cancel := context.WithCancel(context.Background())
	w, req := newReaderWire(t, c, next, ctx)
	tr := goTrip(w, req)
	eventually(2*time.Second, func() bool { return next.calls.Load() == 1 })
	cancel()
	if r := <-tr; !errors.Is(r.err, context.Canceled) || r.panicked != nil {
		t.Fatalf("the reader that left: err = %v, panic = %v; want context.Canceled", r.err, r.panicked)
	}
	if !eventually(2*time.Second, func() bool { return c.flightPanicsUnwaited.Load() == 1 }) {
		t.Fatalf("unwaited panics = %d; want 1", c.flightPanicsUnwaited.Load())
	}
}

// A fetch's retry never re-joins the attempt it gave up on (a connect cut):
// it is sent on its own, as its fresh connection would be.
func TestExternalFlight1320_ARetryNeverRejoinsTheAttemptItGaveUpOn(t *testing.T) {
	c := wireCache(t)
	release := make(chan struct{})
	defer close(release)
	next := &scriptedWire{}
	next.fn = func(n int64, req *http.Request) (*http.Response, error) {
		if n == 1 { // the attempt that never gets anywhere
			select {
			case <-release:
			case <-req.Context().Done():
			}
			return nil, errors.New("attempt 1 ended")
		}
		return scriptedOK(req, `{"n":2}`), nil
	}
	page := customerCtx(pagebudget.PageMaxGoal)
	attempt1, cut := context.WithCancel(page)
	w, req := newReaderWire(t, c, next, attempt1)
	t1 := goTrip(w, req)
	eventually(2*time.Second, func() bool { return next.calls.Load() == 1 })
	// Another reader keeps attempt 1 on the wire, still young and joinable.
	other, oreq := newReaderWire(t, c, next, customerCtx(pagebudget.PageMaxGoal))
	otherTrip := goTrip(other, oreq)
	eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 })
	cut() // this fetch gives up on attempt 1
	if r := <-t1; r.err == nil {
		t.Fatal("SETUP: attempt 1 was answered")
	}
	retry, err := w.RoundTrip(req.Clone(page))
	if err != nil {
		t.Fatalf("the retry: %v", err)
	}
	body, _ := io.ReadAll(retry.Body)
	retry.Body.Close()
	if string(body) != `{"n":2}` || next.calls.Load() != 2 {
		t.Fatalf("the retry read %q with %d requests below; want its own request's answer and 2", body, next.calls.Load())
	}
	if joined, own := c.flightJoined.Load(), c.flightNotShared.Load(); joined != 1 || own != 1 {
		t.Fatalf("joined=%d not_shared=%d; want 1 and 1 (the retry must not re-join attempt 1)", joined, own)
	}
	select {
	case r := <-otherTrip:
		t.Fatalf("the other reader ended: %v", r.err)
	default:
	}
}

// A joiner's own trace hooks see the shared attempt's connection events,
// those before it joined (replayed) and those after, each once and in order;
// never the request's header values.
func TestExternalFlight1320_AJoinerSeesTheAttemptsConnectionEvents(t *testing.T) {
	c := wireCache(t)
	step := make(chan struct{})
	release := make(chan struct{})
	next := &scriptedWire{fn: func(_ int64, req *http.Request) (*http.Response, error) {
		tr := httptrace.ContextClientTrace(req.Context())
		tr.GetConn("upstream.invalid:80")
		tr.GotConn(httptrace.GotConnInfo{Reused: true})
		if tr.WroteHeaderField != nil {
			tr.WroteHeaderField("Authorization", []string{"Bearer t"})
		}
		<-step
		tr.WroteHeaders()
		<-release
		return scriptedOK(req, `{}`), nil
	}}
	record := func() (*httptrace.ClientTrace, func() []string) {
		var mu sync.Mutex
		var got []string
		add := func(s string) { mu.Lock(); got = append(got, s); mu.Unlock() }
		return &httptrace.ClientTrace{
				GetConn:          func(string) { add("get-conn") },
				GotConn:          func(i httptrace.GotConnInfo) { add(fmt.Sprintf("got-conn reused=%v", i.Reused)) },
				WroteHeaders:     func() { add("wrote-headers") },
				WroteHeaderField: func(k string, _ []string) { add("header-field " + k) },
			}, func() []string {
				mu.Lock()
				defer mu.Unlock()
				return append([]string(nil), got...)
			}
	}
	w1, r1 := newReaderWire(t, c, next, context.Background())
	t1 := goTrip(w1, r1)
	eventually(2*time.Second, func() bool { return next.calls.Load() == 1 })
	tr, events := record()
	w2, r2 := newReaderWire(t, c, next, httptrace.WithClientTrace(context.Background(), tr))
	t2 := goTrip(w2, r2)
	eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 && len(events()) == 2 })
	close(step)
	eventually(2*time.Second, func() bool { return len(events()) == 3 })
	close(release)
	for _, ch := range []<-chan tripResult{t1, t2} {
		if r := <-ch; r.err != nil {
			t.Fatal(r.err)
		} else {
			r.resp.Body.Close()
		}
	}
	want := []string{"get-conn", "got-conn reused=true", "wrote-headers"}
	if got := events(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the joiner's trace saw %v; want %v", got, want)
	}
}
