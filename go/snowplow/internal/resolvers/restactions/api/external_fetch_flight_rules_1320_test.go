//go:build unit
// +build unit

package api

// external_fetch_flight_rules_1320_test.go — #1320, second review round and
// the owner rulings of 2026-11-15: the join rule's boundaries, the key's
// construction, one request feeding each instrument once, and a randomized
// interleaving. Helpers are in external_fetch_flight_1320_test.go.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"k8s.io/client-go/rest"
)

// ------------------------------------------------------------ the join rule

// THE JOIN RULE at its boundaries: joinable iff the starter is still waiting
// and the request is younger than the page budget; a customer never joins a
// request background work started. The age bound is exact (1 ns before, at,
// after) and the same for both kinds of starter.
func TestExternalFlight1320_TheJoinRuleAtItsBoundaries(t *testing.T) {
	until := time.Now().Add(time.Hour)
	for _, tc := range []struct {
		name            string
		customerStarted bool
		starterGone     bool
		closed          bool
		at              time.Time
		customer        bool
		want            bool
	}{
		{"customer-started, 1 ns before the bound, customer", true, false, false, until.Add(-1), true, true},
		{"customer-started, at the bound, customer", true, false, false, until, true, false},
		{"customer-started, 1 ns after the bound, customer", true, false, false, until.Add(1), true, false},
		{"customer-started, young, background reader", true, false, false, until.Add(-1), false, true},
		{"background-started, 1 ns before the bound, background reader", false, false, false, until.Add(-1), false, true},
		{"background-started, at the bound, background reader", false, false, false, until, false, false},
		{"background-started, 1 ns after the bound, background reader", false, false, false, until.Add(1), false, false},
		{"background-started, young, customer", false, false, false, until.Add(-1), true, false},
		{"customer-started, young, starter gone, customer", true, true, false, until.Add(-1), true, false},
		{"background-started, young, starter gone, background reader", false, true, false, until.Add(-1), false, false},
		{"customer-started, young, closed", true, false, true, until.Add(-1), true, false},
	} {
		f := &externalFlight{customerStarted: tc.customerStarted, starterGone: tc.starterGone, closed: tc.closed, joinUntil: until}
		if got := f.joinableLocked(tc.at, tc.customer); got != tc.want {
			t.Errorf("%s: joinable = %v; want %v", tc.name, got, tc.want)
		}
	}
}

// The bound a request is given when it starts: a customer's page-budget
// deadline; one page budget (pagebudget.PageMaxGoal) after its start when
// background work started it. No other number.
func TestExternalFlight1320_ARequestIsYoungForOnePageBudget(t *testing.T) {
	c := wireCache(t)
	release := make(chan struct{})
	defer close(release)
	next := blockingWire(release, nil)

	before := time.Now()
	w, req := newReaderWire(t, c, next, context.Background())
	f, role := w.joinOrLead(req.Context())
	after := time.Now()
	if role != flightLead || f.customerStarted {
		t.Fatalf("background: role=%v customerStarted=%v; want lead, false", role, f.customerStarted)
	}
	if f.joinUntil.Before(before.Add(pagebudget.PageMaxGoal)) || f.joinUntil.After(after.Add(pagebudget.PageMaxGoal)) {
		t.Errorf("a background-started request is joinable until %v after its start; want the page budget, %v",
			f.joinUntil.Sub(before).Round(time.Millisecond), pagebudget.PageMaxGoal)
	}
	f.leave(true)

	const left = 3 * time.Second
	cctx := customerCtx(left)
	b, _ := pagebudget.From(cctx)
	w2, req2 := newReaderWire(t, c, next, cctx)
	f2, role2 := w2.joinOrLead(req2.Context())
	if role2 != flightLead || !f2.customerStarted || !f2.joinUntil.Equal(b.Deadline()) {
		t.Errorf("customer: role=%v customerStarted=%v joinUntil=%v; want lead, true, the customer's page-budget deadline %v",
			role2, f2.customerStarted, f2.joinUntil, b.Deadline())
	}
	f2.leave(true)
}

// A background-started request stops admitting background readers when it is
// one page budget old, even though its starter still waits: one stuck request
// cannot hold every background reader of its key for the starter's whole
// deadline. (The age is moved, not waited for.)
func TestExternalFlight1320_AnOldBackgroundRequestIsNotJoined(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, firstHangsHandler)
	opts := o.read("alice", "/events")
	sctx, scancel := context.WithCancel(context.Background())
	defer scancel()
	starter := goShared(c, sctx, opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the background read's request never reached the origin")
	}
	jctx, jcancel := context.WithCancel(context.Background())
	defer jcancel()
	young := goShared(c, jctx, opts)
	if !eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 }) {
		t.Fatal("SETUP: a background reader did not join the young background request")
	}
	// The request turns one page budget old.
	c.flightsMu.Lock()
	for _, f := range c.flights {
		f.joinUntil = time.Now()
	}
	c.flightsMu.Unlock()
	var late wireResult
	select {
	case late = <-goShared(c, context.Background(), opts):
	case <-time.After(5 * time.Second):
		t.Fatal("a background reader arriving after one page budget is waiting on the old request (it joined it)")
	}
	if late.outcome() != "ok" || o.requests.Load() != 2 || c.flightJoined.Load() != 1 {
		t.Fatalf("late background reader: %s, requests=%d joined=%d; want its own answer, 2, 1", late.outcome(), o.requests.Load(), c.flightJoined.Load())
	}
	for _, ch := range []<-chan wireResult{starter, young} {
		select {
		case r := <-ch:
			t.Fatalf("a reader attached to the old request ended: %s", r.outcome())
		default:
		}
	}
}

// A fetch's retry never re-joins the attempt it gave up on (a connect cut):
// it is sent on its own, as its fresh connection would be. The attempt is
// another reader's, still young and its starter still waiting, so only this
// rule keeps the retry off it.
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
	starter, sreq := newReaderWire(t, c, next, customerCtx(pagebudget.PageMaxGoal))
	starterTrip := goTrip(starter, sreq)
	eventually(2*time.Second, func() bool { return next.calls.Load() == 1 })

	page := customerCtx(pagebudget.PageMaxGoal)
	attempt1, cut := context.WithCancel(page)
	w, req := newReaderWire(t, c, next, attempt1)
	t1 := goTrip(w, req)
	if !eventually(2*time.Second, func() bool { return c.flightJoined.Load() == 1 }) {
		t.Fatal("SETUP: the reader did not join the attempt on the wire")
	}
	cut() // this fetch gives up on the attempt
	if r := <-t1; r.err == nil {
		t.Fatal("SETUP: the attempt was answered")
	}
	var rt tripResult
	select {
	case rt = <-goTrip(w, req.Clone(page)):
	case <-time.After(5 * time.Second):
		t.Fatal("the retry is waiting on the attempt its fetch gave up on (it re-joined it)")
	}
	if rt.err != nil {
		t.Fatalf("the retry: %v", rt.err)
	}
	body, _ := io.ReadAll(rt.resp.Body)
	rt.resp.Body.Close()
	if string(body) != `{"n":2}` || next.calls.Load() != 2 {
		t.Fatalf("the retry read %q with %d requests below; want its own request's answer and 2", body, next.calls.Load())
	}
	if joined, own := c.flightJoined.Load(), c.flightNotShared.Load(); joined != 1 || own != 1 {
		t.Fatalf("joined=%d not_shared=%d; want 1 and 1 (the retry must not re-join)", joined, own)
	}
	select {
	case r := <-starterTrip:
		t.Fatalf("the starter ended: %v", r.err)
	default:
	}
}

// Only the fetch's own first request is shared. A redirect hop (net/http sets
// Response on one) and a request to another URL are sent by their reader,
// even while an identical-keyed request is on the wire to join.
func TestExternalFlight1320_OnlyTheFirstRequestIsShared(t *testing.T) {
	c := wireCache(t)
	release := make(chan struct{})
	defer close(release)
	next := &scriptedWire{}
	next.fn = func(n int64, req *http.Request) (*http.Response, error) {
		if n == 1 {
			select {
			case <-release:
			case <-req.Context().Done():
			}
			return nil, errors.New("request 1 ended")
		}
		return scriptedOK(req, fmt.Sprintf(`{"n":%d,"url":%q}`, n, req.URL.String())), nil
	}
	first, freq := newReaderWire(t, c, next, context.Background())
	firstTrip := goTrip(first, freq)
	eventually(2*time.Second, func() bool { return next.calls.Load() == 1 })

	w, req := newReaderWire(t, c, next, context.Background())
	hop := req.Clone(req.Context())
	hop.Response = &http.Response{StatusCode: http.StatusFound} // what net/http sets on a redirect hop
	elsewhere, err := http.NewRequestWithContext(req.Context(), http.MethodGet, "http://upstream.invalid/other", nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]*http.Request{"a redirect hop": hop, "a request to another URL": elsewhere} {
		var tr tripResult
		select {
		case tr = <-goTrip(w, r):
		case <-time.After(5 * time.Second):
			t.Fatalf("%s is waiting on the first request's wire request (it was shared under its key)", name)
		}
		if tr.err != nil {
			t.Fatalf("%s: %v", name, tr.err)
		}
		body, _ := io.ReadAll(tr.resp.Body)
		tr.resp.Body.Close()
		if !strings.Contains(string(body), fmt.Sprintf("%q", r.URL.String())) {
			t.Errorf("%s was answered %s; want its own request's answer", name, body)
		}
	}
	if joined, own, sent := c.flightJoined.Load(), c.flightNotShared.Load(), next.calls.Load(); joined != 0 || own != 2 || sent != 3 {
		t.Fatalf("joined=%d not_shared=%d requests below=%d; want 0, 2, 3", joined, own, sent)
	}
	select {
	case r := <-firstTrip:
		t.Fatalf("the first request ended: %v", r.err)
	default:
	}
}

// The wire request is cancelled by its own context alone: a Cancel channel on
// the starter's request (http.Client.Timeout sets one) is not carried over,
// so a starter's client timeout cannot end the request under its joiners.
func TestExternalFlight1320_TheWireRequestDropsTheStartersCancelChannel(t *testing.T) {
	c := wireCache(t)
	var carried atomic.Bool
	next := &scriptedWire{fn: func(_ int64, req *http.Request) (*http.Response, error) {
		carried.Store(req.Cancel != nil) //nolint:staticcheck // the deprecated field is what this arm is about
		return scriptedOK(req, `{}`), nil
	}}
	w, req := newReaderWire(t, c, next, context.Background())
	req.Cancel = make(chan struct{}) //nolint:staticcheck // as above
	r := <-goTrip(w, req)
	if r.err != nil {
		t.Fatal(r.err)
	}
	r.resp.Body.Close()
	if carried.Load() {
		t.Fatal("the request on the wire carries its starter's Cancel channel")
	}
}

// ----------------------------------------------------------------- the key

// B1 (second review): no value can be read as "absent" or as another
// component. On the wire: a read with no payload and a read whose payload is
// the text `none` are two requests, each answered as its own; so are two
// different payloads.
func TestExternalFlight1320_DifferentPayloadsNeverShare(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, func(o *wireOrigin, _ int64, w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if o.hold(r) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"body":%q}`, body)
		}
	})
	payloads := []*string{nil, ptr.To("none"), ptr.To("payload:none"), ptr.To("a"), ptr.To("b")}
	chs := make([]<-chan wireResult, len(payloads))
	for i, p := range payloads {
		opts := o.read("alice", "/events")
		opts.Payload = p
		chs[i] = goShared(c, customerCtx(pagebudget.PageMaxGoal), opts)
	}
	eventually(3*time.Second, func() bool { return o.requests.Load() == int64(len(payloads)) })
	time.Sleep(100 * time.Millisecond) // a join that should not happen would show as a missing request
	o.open1()
	for i, ch := range chs {
		want := ""
		if payloads[i] != nil {
			want = *payloads[i]
		}
		got := mustWireOK(t, recvWire(t, ch, "reader"), "reader")
		if got != fmt.Sprintf(`{"body":%q}`, want) {
			t.Errorf("the reader that sent %q was answered %s", want, got)
		}
	}
	if got, joined := o.requests.Load(), c.flightJoined.Load(); got != int64(len(payloads)) || joined != 0 {
		t.Fatalf("origin saw %d requests, joined=%d; want %d and 0 (different payloads never share)", got, joined, len(payloads))
	}
}

// The key's construction: pairs that differ only in where a value sits, or in
// present-against-absent, never collide. Each pair would collide under a key
// that joined its fields with separators or wrote a sentinel for "absent".
func TestExternalFlight1320_KeyHasNoSeparatorOrSentinelAmbiguity(t *testing.T) {
	type req struct {
		path    string
		headers []string
		payload *string
		imp     *rest.ImpersonationConfig
	}
	key := func(r req) externalFlightKey {
		o := keyBase()
		o.Path, o.Headers, o.Payload = r.path, r.headers, r.payload
		ctx := context.Background()
		if r.imp != nil {
			ctx = withEndpointImpersonation(ctx, *r.imp)
		}
		return mustKey(t, ctx, o)
	}
	for name, pair := range map[string][2]req{
		"no payload / the text none":            {{path: "/e"}, {path: "/e", payload: ptr.To("none")}},
		"no payload / an empty payload":         {{path: "/e"}, {path: "/e", payload: ptr.To("")}},
		"payload a / payload b":                 {{path: "/e", payload: ptr.To("a")}, {path: "/e", payload: ptr.To("b")}},
		"one header / split in two":             {{path: "/e", headers: []string{"X-A: 1X-B: 2"}}, {path: "/e", headers: []string{"X-A: 1", "X-B: 2"}}},
		"header text moved into the path":       {{path: "/eX-A: 1"}, {path: "/e", headers: []string{"X-A: 1"}}},
		"payload text moved into a header":      {{path: "/e", headers: []string{"X"}, payload: ptr.To("Y")}, {path: "/e", headers: []string{"XY"}, payload: ptr.To("")}},
		"no impersonation / an empty one":       {{path: "/e"}, {path: "/e", imp: &rest.ImpersonationConfig{}}},
		"user a+uid b / user ab":                {{path: "/e", imp: &rest.ImpersonationConfig{UserName: "a", UID: "b"}}, {path: "/e", imp: &rest.ImpersonationConfig{UserName: "ab"}}},
		"groups [ab] / groups [a b]":            {{path: "/e", imp: &rest.ImpersonationConfig{Groups: []string{"ab"}}}, {path: "/e", imp: &rest.ImpersonationConfig{Groups: []string{"a", "b"}}}},
		"uid g / group g":                       {{path: "/e", imp: &rest.ImpersonationConfig{UID: "g"}}, {path: "/e", imp: &rest.ImpersonationConfig{Groups: []string{"g"}}}},
		"extra k=[v w] / extra k=[v], w=[]":     {{path: "/e", imp: &rest.ImpersonationConfig{Extra: map[string][]string{"k": {"v", "w"}}}}, {path: "/e", imp: &rest.ImpersonationConfig{Extra: map[string][]string{"k": {"v"}, "w": {}}}}},
		"extras {a:[b c] d:[]} / {a:[b] c:[d]}": {{path: "/e", imp: &rest.ImpersonationConfig{Extra: map[string][]string{"a": {"b", "c"}, "d": {}}}}, {path: "/e", imp: &rest.ImpersonationConfig{Extra: map[string][]string{"a": {"b"}, "c": {"d"}}}}},
		"group g / extra g":                     {{path: "/e", imp: &rest.ImpersonationConfig{Groups: []string{"g"}}}, {path: "/e", imp: &rest.ImpersonationConfig{Extra: map[string][]string{"g": {}}}}},
	} {
		if key(pair[0]) == key(pair[1]) {
			t.Errorf("%s: two different requests share one key", name)
		}
	}
}

// The ServiceAccount-dial mark is in the key on its own, whatever the token
// file's path (here: empty).
func TestExternalFlight1320_KeyFoldsTheServiceAccountDialMark(t *testing.T) {
	restore := dynamic.SetServiceAccountTokenPathForTest("")
	defer restore()
	plain := mustKey(t, context.Background(), keyBase())
	sa := mustKey(t, cache.WithServiceAccountDial(context.Background()), keyBase())
	if plain == sa {
		t.Fatal("a ServiceAccount dial and a plain dial share one key")
	}
}

// ------------------------------------------------------------- the session

// The session id is not a key component: readers of different sessions share
// one request, which carries its starter's trace context and session id.
// Each joiner is handed what identifies that one request to the upstream (the
// trace id and span id of the traceparent written on the wire, and the
// session id), so its own record can name it exactly; the starter's own record
// needs none (the request is its own).
func TestExternalFlight1320_AJoinerLearnsWhichRequestAnsweredIt(t *testing.T) {
	c := wireCache(t)
	o := newWireOrigin(t, heldOriginHandler)
	const traceID, spanID = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
	// The tracing transport injects the traceparent below the sharing; here the
	// stage sends one itself, which reaches the wire the same way.
	opts := o.read("clickhouse", "/events", "traceparent: 00-"+traceID+"-"+spanID+"-01")
	read := func(session string) (context.Context, *externalAnsweredBy) {
		ctx := customerCtx(pagebudget.PageMaxGoal)
		if session != "" {
			ctx = audit.WithSessionID(ctx, session)
		}
		return withExternalAnsweredBy(ctx)
	}
	sctx, starterBy := read("session-starter")
	starter := goShared(c, sctx, opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the starter's request never reached the origin")
	}
	var joinerBy []*externalAnsweredBy
	var joiners []<-chan wireResult
	for _, session := range []string{"session-other", "", "session-starter"} {
		jctx, by := read(session)
		joinerBy = append(joinerBy, by)
		joiners = append(joiners, goShared(c, jctx, opts))
	}
	if !eventually(3*time.Second, func() bool { return c.flightJoined.Load() == 3 }) {
		t.Fatalf("readers of other sessions did not share: joined=%d requests=%d; want 3 and 1 (the session id is not in the key)",
			c.flightJoined.Load(), o.requests.Load())
	}
	o.open1()
	mustWireOK(t, recvWire(t, starter, "starter"), "starter")
	for _, ch := range joiners {
		mustWireOK(t, recvWire(t, ch, "joiner"), "joiner")
	}
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d requests; want 1", got)
	}
	if ref, joined := starterBy.Wire(); joined || ref != (externalWireRef{}) {
		t.Errorf("the starter's record says joined=%v ref=%+v; want false and nothing (its answer came from its own request)", joined, ref)
	}
	want := externalWireRef{Session: "session-starter", TraceID: traceID, SpanID: spanID}
	for i, by := range joinerBy {
		if ref, joined := by.Wire(); !joined || ref != want {
			t.Errorf("joiner %d's record says joined=%v ref=%+v; want true and %+v", i, joined, ref, want)
		}
	}
	// A nil record (a fetch with none on its context) reads as not joined.
	if ref, joined := (*externalAnsweredBy)(nil).Wire(); joined || ref != (externalWireRef{}) {
		t.Errorf("a nil record reads joined=%v ref=%+v", joined, ref)
	}
}

// Only a well-formed traceparent yields ids; anything else yields none.
func TestExternalFlight1320_TraceparentIDs(t *testing.T) {
	const traceID, spanID = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
	if tr, sp, ok := traceparentIDs("00-" + traceID + "-" + spanID + "-01"); !ok || tr != traceID || sp != spanID {
		t.Errorf("a well-formed traceparent read as %q, %q, %v", tr, sp, ok)
	}
	for _, bad := range []string{"", "00", "00-" + traceID + "-" + spanID, "00-" + traceID[:31] + "-" + spanID + "-01",
		"00-" + traceID + "-" + spanID[:15] + "-01", "00-" + strings.Repeat("z", 32) + "-" + spanID + "-01", "0-" + traceID + "-" + spanID + "-01"} {
		if tr, sp, ok := traceparentIDs(bad); ok || tr != "" || sp != "" {
			t.Errorf("traceparentIDs(%q) = %q, %q, %v; want nothing", bad, tr, sp, ok)
		}
	}
}

// ---------------------------------------------------------- the instruments

// B2 (second review): one request on the wire feeds each instrument ONCE,
// with the wire's own timings. Eight identical readers, one request, one
// connection: the egress connection counters move by one, the retry reserve
// takes one sample and it is the wire request's time to its first byte, and a
// joiner's own trace hooks hear nothing (they are where instruments listen).
func TestExternalFlight1320_OneRequestFeedsEachInstrumentOnce(t *testing.T) {
	c := wireCache(t)
	const answerAfter = 300 * time.Millisecond
	gate := make(chan struct{})
	o := newWireOrigin(t, func(o *wireOrigin, n int64, w http.ResponseWriter, r *http.Request) {
		select {
		case <-gate:
		case <-o.stop:
			return
		}
		time.Sleep(answerAfter)
		answer(w, r, n)
	})
	connections := func() (opened, handovers uint64) {
		for _, cell := range credhttp.EgressConnectionsSnapshot() {
			opened += cell.Opened
			handovers += cell.Handovers
		}
		return
	}
	credhttp.ResetRetryReserveForTest()
	opened0, handovers0 := connections()
	samples0 := credhttp.RetryReserveSamples()

	const n = 8
	var joinerHooks atomic.Int64
	hook := func() { joinerHooks.Add(1) }
	listening := &httptrace.ClientTrace{
		GetConn: func(string) { hook() }, GotConn: func(httptrace.GotConnInfo) { hook() },
		ConnectStart: func(_, _ string) { hook() }, ConnectDone: func(_, _ string, _ error) { hook() },
		GotFirstResponseByte: hook, WroteHeaders: hook,
	}
	opts := o.read("alice", "/events")
	chs := []<-chan wireResult{goShared(c, customerCtx(pagebudget.PageMaxGoal), opts)}
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the first reader's request never reached the origin")
	}
	// The joiners arrive well after the connection was asked for: a sample
	// timed from a join would be far shorter than the answer.
	time.Sleep(200 * time.Millisecond)
	for i := 1; i < n; i++ {
		chs = append(chs, goShared(c, httptrace.WithClientTrace(customerCtx(pagebudget.PageMaxGoal), listening), opts))
	}
	if !eventually(3*time.Second, func() bool { return c.flightJoined.Load() == n-1 }) {
		t.Fatalf("SETUP: joined = %d; want %d", c.flightJoined.Load(), n-1)
	}
	close(gate)
	for _, ch := range chs {
		mustWireOK(t, recvWire(t, ch, "reader"), "reader")
	}
	if o.requests.Load() != 1 {
		t.Fatalf("SETUP: origin saw %d requests; want 1", o.requests.Load())
	}
	opened1, handovers1 := connections()
	if d := opened1 - opened0; d != 1 {
		t.Errorf("connections opened moved by %d for 1 connection the origin accepted (%d readers)", d, n)
	}
	if d := handovers1 - handovers0; d != 1 {
		t.Errorf("connection handovers moved by %d for 1 request on the wire (%d readers)", d, n)
	}
	if d := credhttp.RetryReserveSamples() - samples0; d != 1 {
		t.Errorf("the retry reserve took %d samples from 1 answer (%d readers)", d, n)
	}
	// The one sample is the wire's: the connection was asked for at least
	// 200 ms + answerAfter before the first byte.
	if reserve, measured := credhttp.RetryReserve(); !measured || reserve < 200*time.Millisecond+answerAfter-50*time.Millisecond {
		t.Errorf("the retry reserve reads %v (measured=%v) after an answer that took at least %v on the wire",
			reserve.Round(time.Millisecond), measured, 200*time.Millisecond+answerAfter)
	}
	if got := joinerHooks.Load(); got != 0 {
		t.Errorf("the joiners' own trace hooks heard %d events of a request they did not send; want 0", got)
	}
}

// ------------------------------------------------- randomized interleaving

type interleaveKey struct{}

// interleaveReader is who a reader is, on its context: the scripted transport
// reads the STARTER's from the wire request's context and stamps it on the
// answer, so every reader can check whose request answered it.
type interleaveReader struct {
	id       int
	customer bool
	shape    int
	arrived  time.Time
	deadline time.Time // a customer's page-budget deadline
}

// Many readers, customers and background, arriving at random over three
// request shapes, with random budgets and cancellations, against an upstream
// that answers after a random delay, fails, or never answers. Every reader
// that is answered must have been answered for its own shape, by a request
// the join rule let it share: never a customer on a background request, never
// a request older than its starter's page budget, never after its starter
// left... and when it is over, nothing is left: no key, no goroutine.
func TestExternalFlight1320_RandomizedInterleaving(t *testing.T) {
	c := wireCache(t)
	const readers = 1500
	rng := rand.New(rand.NewSource(1320))
	g0 := runtime.NumGoroutine()

	type started struct {
		starter interleaveReader
		at      time.Time
	}
	var mu sync.Mutex
	wireStarts := map[int64]started{}
	next := &scriptedWire{}
	next.fn = func(n int64, req *http.Request) (*http.Response, error) {
		who, _ := req.Context().Value(interleaveKey{}).(interleaveReader)
		mu.Lock()
		wireStarts[n] = started{starter: who, at: time.Now()}
		kind := rng.Intn(10)
		delay := time.Duration(rng.Intn(30)) * time.Millisecond
		mu.Unlock()
		if kind == 0 { // never answers
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		select {
		case <-time.After(delay):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		if kind == 1 {
			return nil, errors.New("upstream failed")
		}
		resp := scriptedOK(req, fmt.Sprintf(`{"shape":%q}`, req.URL.Path))
		resp.Header.Set("X-Wire", fmt.Sprint(n))
		return resp, nil
	}

	var violations, answered, cut atomic.Int64
	report := func(format string, args ...any) {
		if violations.Add(1) <= 10 {
			t.Errorf(format, args...)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		mu.Lock()
		who := interleaveReader{id: i, customer: rng.Intn(3) != 0, shape: rng.Intn(3)}
		left := time.Duration(20+rng.Intn(60)) * time.Millisecond
		cancelAfter := time.Duration(-1)
		if rng.Intn(5) == 0 {
			cancelAfter = time.Duration(rng.Intn(40)) * time.Millisecond
		}
		gap := time.Duration(rng.Intn(400)) * time.Microsecond
		mu.Unlock()
		time.Sleep(gap)
		wg.Add(1)
		go func() {
			defer wg.Done()
			who.arrived = time.Now()
			ctx := context.Background()
			if who.customer {
				ctx = customerCtx(left)
				b, _ := pagebudget.From(ctx)
				who.deadline = b.Deadline()
			}
			ctx = context.WithValue(ctx, interleaveKey{}, who)
			// Each reader bounds its own wait, as a fetch's attempt context does.
			ctx, stop := context.WithTimeout(ctx, left)
			defer stop()
			if cancelAfter >= 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				time.AfterFunc(cancelAfter, cancel)
			}
			url := fmt.Sprintf("http://upstream.invalid/shape-%d", who.shape)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				report("%v", err)
				return
			}
			opts := keyBase()
			opts.Endpoint.ServerURL, opts.Path, opts.Headers = "http://upstream.invalid", fmt.Sprintf("/shape-%d", who.shape), nil
			w := c.share(ctx, opts)(next, req)
			resp, err := w.RoundTrip(req)
			if err != nil {
				cut.Add(1)
				return
			}
			body, rerr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if rerr != nil {
				cut.Add(1)
				return
			}
			answered.Add(1)
			if string(body) != fmt.Sprintf(`{"shape":"/shape-%d"}`, who.shape) {
				report("reader %d (shape %d) was answered %s", who.id, who.shape, body)
			}
			var n int64
			fmt.Sscan(resp.Header.Get("X-Wire"), &n)
			mu.Lock()
			s, ok := wireStarts[n]
			mu.Unlock()
			if !ok {
				report("reader %d was answered by a request the transport never saw (%q)", who.id, resp.Header.Get("X-Wire"))
				return
			}
			if s.starter.id == who.id {
				return // its own request
			}
			if who.customer && !s.starter.customer {
				report("customer %d was answered by a request background reader %d started", who.id, s.starter.id)
			}
			bound := s.at.Add(pagebudget.PageMaxGoal)
			if s.starter.customer {
				bound = s.starter.deadline
			}
			if who.arrived.After(bound) {
				report("reader %d joined a request %v past its bound", who.id, who.arrived.Sub(bound))
			}
		}()
	}
	wg.Wait()
	if violations.Load() > 0 {
		t.Fatalf("%d violations", violations.Load())
	}
	sent := next.calls.Load()
	if joined := int64(c.flightJoined.Load()); answered.Load() == 0 || joined == 0 || sent >= readers {
		t.Fatalf("SETUP: answered=%d joined=%d requests=%d of %d readers; the arm did not exercise sharing", answered.Load(), joined, sent, readers)
	}
	if !eventually(5*time.Second, func() bool { return c.externalFlightsInFlight() == 0 && runtime.NumGoroutine() <= g0+10 }) {
		t.Fatalf("left behind: %d keys held, %d goroutines above start", c.externalFlightsInFlight(), runtime.NumGoroutine()-g0)
	}
	t.Logf("%d readers: %d requests on the wire, %d joins, %d answered, %d cut", readers, sent, c.flightJoined.Load(), answered.Load(), cut.Load())
}
