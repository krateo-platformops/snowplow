//go:build unit
// +build unit

package api

// external_fetch_flight_1320_test.go — #1320 (owner ruling 2026-11-10):
// identical concurrent external reads share one in-flight call, detached from
// any single page. Every arm drives the real fetch (httpFetchConverting) over
// the wire against an httptest origin that holds its answers until released,
// and counts DIALS where they land: at the origin.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/rest"
)

// heldOrigin answers every request with a JSON body naming the request's
// sequence number and the credential it carried, but only once released.
// headersFirst: it sends the status line and headers (flushed) on arrival, and
// holds only the body.
type heldOrigin struct {
	srv          *httptest.Server
	requests     atomic.Int64
	headersSent  atomic.Int64
	release      chan struct{}
	releaseOnce  sync.Once
	headersFirst bool
	mu           sync.Mutex
	version      string
}

func newHeldOrigin(t *testing.T, headersFirst bool) *heldOrigin {
	t.Helper()
	o := &heldOrigin{release: make(chan struct{}), headersFirst: headersFirst, version: "v1"}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := o.requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if o.headersFirst {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			o.headersSent.Add(1)
		}
		select {
		case <-o.release:
		case <-r.Context().Done():
			return
		}
		o.mu.Lock()
		v := o.version
		o.mu.Unlock()
		cred := r.Header.Get("Authorization")
		fmt.Fprintf(w, `{"n":%d,"cred":%q,"version":%q,"method":%q}`, n, cred, v, r.Method)
	}))
	t.Cleanup(func() { o.open(); o.srv.Close() })
	return o
}

func (o *heldOrigin) open() { o.releaseOnce.Do(func() { close(o.release) }) }

func (o *heldOrigin) setVersion(v string) {
	o.mu.Lock()
	o.version = v
	o.mu.Unlock()
}

func (o *heldOrigin) opts(ep endpoints.Endpoint, verb string, headers ...string) httpcall.RequestOptions {
	ep.ServerURL = o.srv.URL
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/events", Verb: ptr.To(verb), Headers: headers},
		Endpoint:    &ep,
	}
}

type fetchResult struct {
	st   *response.Status
	body []byte
	err  error
	at   time.Time
}

func goFetch(c *externalFetchCache, ctx context.Context, opts httpcall.RequestOptions) <-chan fetchResult {
	ch := make(chan fetchResult, 1)
	go func() {
		st, body, _, err := c.do(ctx, opts)
		ch <- fetchResult{st: st, body: body, err: err, at: time.Now()}
	}()
	return ch
}

// eventually polls cond for up to d. It only paces the arm (it never decides
// a verdict): every assertion below is on what the origin saw.
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

func recv(t *testing.T, ch <-chan fetchResult, what string) fetchResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: no result after 10s", what)
	}
	return fetchResult{}
}

func mustOK(t *testing.T, r fetchResult, what string) string {
	t.Helper()
	if r.err != nil || r.st == nil || r.st.Status == response.StatusFailure {
		t.Fatalf("%s: err=%v st=%+v", what, r.err, r.st)
	}
	return string(r.body)
}

func testFlightCache(t *testing.T) *externalFetchCache {
	t.Helper()
	return testExternalFetchCache(t, 1<<20)
}

// N identical concurrent reads → exactly 1 dial. Every reader gets the one
// answer, and each reader's own audit observation (credhttp.Answer) records it.
func TestExternalFlight1320_IdenticalConcurrentReadsDialOnce(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	const n = 16
	answers := make([]*credhttp.Answer, n)
	chs := make([]<-chan fetchResult, n)
	for i := 0; i < n; i++ {
		ctx, a := credhttp.WithAnswer(context.Background())
		answers[i] = a
		chs[i] = goFetch(c, ctx, o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet))
	}
	eventually(5*time.Second, func() bool { return o.requests.Load() >= n || c.flightJoined.Load() == n-1 })
	o.open()
	var first string
	for i, ch := range chs {
		body := mustOK(t, recv(t, ch, fmt.Sprintf("reader %d", i)), fmt.Sprintf("reader %d", i))
		if i == 0 {
			first = body
		} else if body != first {
			t.Errorf("reader %d got %s; reader 0 got %s", i, body, first)
		}
	}
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d dials for %d identical concurrent reads; want 1", got, n)
	}
	for i, a := range answers {
		if oc, code, _ := a.Outcome(); oc != "success" || code != http.StatusOK {
			t.Errorf("reader %d's own audit observation = %s/%d; want success/200", i, oc, code)
		}
	}
}

// Different identities → separate dials: a different endpoint credential, and
// the same endpoint with a different exported JWT, never share; each reader
// gets the answer to its own credential.
func TestExternalFlight1320_DifferentIdentitiesDialSeparately(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	type ident struct {
		name string
		opts httpcall.RequestOptions
		cred string
	}
	idents := []ident{
		{"token-alice", o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet), "Bearer alice"},
		{"token-bob", o.opts(endpoints.Endpoint{Token: "bob"}, http.MethodGet), "Bearer bob"},
		{"jwt-carol", o.opts(endpoints.Endpoint{}, http.MethodGet, "Authorization: Bearer jwt-carol"), "Bearer jwt-carol"},
		{"jwt-dave", o.opts(endpoints.Endpoint{}, http.MethodGet, "Authorization: Bearer jwt-dave"), "Bearer jwt-dave"},
	}
	const per = 4
	type pending struct {
		id ident
		ch <-chan fetchResult
	}
	var all []pending
	for _, id := range idents {
		for j := 0; j < per; j++ {
			all = append(all, pending{id, goFetch(c, context.Background(), id.opts)})
		}
	}
	total := int64(len(idents) * per)
	eventually(5*time.Second, func() bool {
		return o.requests.Load() >= total || (o.requests.Load() == int64(len(idents)) && int64(c.flightJoined.Load()) == total-int64(len(idents)))
	})
	o.open()
	for _, p := range all {
		body := mustOK(t, recv(t, p.ch, p.id.name), p.id.name)
		if !strings.Contains(body, fmt.Sprintf(`"cred":%q`, p.id.cred)) {
			t.Errorf("%s got an answer to another credential: %s", p.id.name, body)
		}
	}
	if got := o.requests.Load(); got != int64(len(idents)) {
		t.Fatalf("origin saw %d dials for %d identities; want one per identity", got, len(idents))
	}
}

// A joiner whose page budget runs out gets its own budget error (504 Timeout,
// its own budget noting the exhaustion), and stops waiting at its deadline,
// while the call goes on: the leader completes and a later joiner gets the
// answer, all on one dial.
func TestExternalFlight1320_JoinerBudgetEndsOnlyItsOwnWait(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	opts := o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet)
	leader := goFetch(c, context.Background(), opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the leader's call never reached the origin")
	}
	const left = 300 * time.Millisecond
	bctx := pagebudget.WithCustomer(context.Background(), time.Now().Add(left-pagebudget.PageMaxGoal))
	bctx, ans := credhttp.WithAnswer(bctx)
	start := time.Now()
	joiner := goFetch(c, bctx, opts)
	var jr fetchResult
	select {
	case jr = <-joiner:
	case <-time.After(3 * time.Second):
		o.open()
		t.Fatal("the budgeted joiner was still waiting 3s after its budget ran out (its wait is not bounded by its own budget)")
	}
	if waited := jr.at.Sub(start); waited > left+time.Second {
		t.Errorf("joiner waited %v for a %v budget", waited, left)
	}
	var be *pagebudget.ExhaustedError
	if !errors.As(jr.err, &be) || be.Result != pagebudget.ExhaustedAnswer {
		t.Fatalf("joiner err = %v; want its own ExhaustedError (exhausted_answer)", jr.err)
	}
	if jr.st == nil || jr.st.Code != http.StatusGatewayTimeout || string(jr.st.Reason) != credhttp.BudgetStatusReason {
		t.Fatalf("joiner status = %+v; want 504 Timeout", jr.st)
	}
	if got := pagebudget.Outcome(bctx); got != pagebudget.ExhaustedAnswer {
		t.Errorf("joiner's budget outcome = %v; want exhausted_answer", got)
	}
	if oc, code, _ := ans.Outcome(); oc != "failure" || code != 0 {
		t.Errorf("joiner's own audit observation = %s/%d; want failure with no code", oc, code)
	}
	later := goFetch(c, context.Background(), opts)
	eventually(5*time.Second, func() bool { return c.flightJoined.Load() == 2 })
	o.open()
	lb := mustOK(t, recv(t, leader, "leader"), "leader")
	ab := mustOK(t, recv(t, later, "later joiner"), "later joiner")
	if lb != ab {
		t.Errorf("later joiner got %s; leader got %s", ab, lb)
	}
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d dials; want 1 (the budget cut must not end or repeat the shared call)", got)
	}
}

// The caller that STARTED the call has a page budget that runs out: it gets
// its own budget error, and the call — detached from its budget — still
// answers its joiners.
func TestExternalFlight1320_LeaderBudgetDoesNotCutTheCall(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	opts := o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet)
	bctx := pagebudget.WithCustomer(context.Background(), time.Now().Add(300*time.Millisecond-pagebudget.PageMaxGoal))
	leader := goFetch(c, bctx, opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the leader's call never reached the origin")
	}
	joiner := goFetch(c, context.Background(), opts)
	lr := recv(t, leader, "budgeted leader")
	var be *pagebudget.ExhaustedError
	if !errors.As(lr.err, &be) {
		t.Fatalf("budgeted leader err = %v; want its own ExhaustedError", lr.err)
	}
	time.Sleep(100 * time.Millisecond) // the call outlives the leader's deadline
	o.open()
	mustOK(t, recv(t, joiner, "joiner"), "joiner")
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d dials; want 1", got)
	}
}

// A cancelled leader request → joiners unaffected: the call runs on, and every
// joiner gets the answer from the one dial.
func TestExternalFlight1320_CancelledLeaderLeavesJoinersUnaffected(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	opts := o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet)
	lctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leader := goFetch(c, lctx, opts)
	if !eventually(5*time.Second, func() bool { return o.requests.Load() == 1 }) {
		t.Fatal("the leader's call never reached the origin")
	}
	j1 := goFetch(c, context.Background(), opts)
	j2 := goFetch(c, context.Background(), opts)
	eventually(5*time.Second, func() bool { return c.flightJoined.Load() == 2 })
	cancel()
	lr := recv(t, leader, "cancelled leader")
	if !errors.Is(lr.err, context.Canceled) {
		t.Fatalf("cancelled leader err = %v; want context.Canceled", lr.err)
	}
	time.Sleep(100 * time.Millisecond) // a cancellation that reached the call would land here
	o.open()
	b1 := mustOK(t, recv(t, j1, "joiner 1"), "joiner 1")
	b2 := mustOK(t, recv(t, j2, "joiner 2"), "joiner 2")
	if b1 != b2 {
		t.Errorf("joiners got different answers: %s / %s", b1, b2)
	}
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d dials; want 1", got)
	}
}

// A write never joins: N identical concurrent writes are N dials, and a read
// identical in every other respect neither joins a write nor is joined by one.
func TestExternalFlight1320_WriteNeverJoins(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	ep := endpoints.Endpoint{Token: "alice"}
	const n = 4
	var chs []<-chan fetchResult
	for i := 0; i < n; i++ {
		post := o.opts(ep, http.MethodPost)
		post.Payload = ptr.To(`{"a":1}`)
		chs = append(chs, goFetch(c, context.Background(), post))
	}
	read := o.opts(ep, http.MethodGet)
	read.Payload = ptr.To(`{"a":1}`)
	chs = append(chs, goFetch(c, context.Background(), read))
	eventually(5*time.Second, func() bool { return o.requests.Load() >= n+1 })
	o.open()
	for i, ch := range chs {
		mustOK(t, recv(t, ch, fmt.Sprintf("call %d", i)), fmt.Sprintf("call %d", i))
	}
	if got := o.requests.Load(); got != n+1 {
		t.Fatalf("origin saw %d dials for %d writes + 1 read; want %d (a write is never shared)", got, n, n+1)
	}
	if got := c.flightNotShared.Load(); got != n {
		t.Errorf("not_shared = %d; want %d", got, n)
	}
}

// A read arriving after a call completed → a fresh dial, which carries the
// upstream's change: nothing is reused after completion.
func TestExternalFlight1320_ReadAfterCompletionDialsFresh(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	o.open()
	opts := o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet)
	b1 := mustOK(t, recv(t, goFetch(c, context.Background(), opts), "first read"), "first read")
	o.setVersion("v2")
	b2 := mustOK(t, recv(t, goFetch(c, context.Background(), opts), "second read"), "second read")
	if got := o.requests.Load(); got != 2 {
		t.Fatalf("origin saw %d dials for 2 sequential reads; want 2", got)
	}
	if !strings.Contains(b1, `"version":"v1"`) || !strings.Contains(b2, `"version":"v2"`) {
		t.Fatalf("reads got %s then %s; want v1 then v2", b1, b2)
	}
}

// The freshness contract's strict half: a read arriving after the call's first
// response byte does not join it (it would be handed bytes received before it
// arrived); it dials afresh.
func TestExternalFlight1320_NoJoinAfterFirstResponseByte(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, true)
	opts := o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet)
	first := goFetch(c, context.Background(), opts)
	if !eventually(5*time.Second, func() bool { return o.headersSent.Load() == 1 }) {
		t.Fatal("the origin never sent the first call's headers")
	}
	eventually(2*time.Second, func() bool { return c.externalFlightsInFlight() == 0 })
	second := goFetch(c, context.Background(), opts)
	eventually(2*time.Second, func() bool { return o.requests.Load() >= 2 })
	o.open()
	mustOK(t, recv(t, first, "first"), "first")
	mustOK(t, recv(t, second, "second"), "second")
	if got := o.requests.Load(); got != 2 {
		t.Fatalf("origin saw %d dials; want 2 (a read after the first response byte must not join)", got)
	}
}

// A request whose page budget is already spent sends nothing and starts
// nothing for anyone else (#1115: not_sent).
func TestExternalFlight1320_SpentBudgetSendsNothing(t *testing.T) {
	c := testFlightCache(t)
	o := newHeldOrigin(t, false)
	o.open()
	bctx := pagebudget.WithCustomer(context.Background(), time.Now().Add(-pagebudget.PageMaxGoal-time.Second))
	_, _, _, err := c.do(bctx, o.opts(endpoints.Endpoint{Token: "alice"}, http.MethodGet))
	var be *pagebudget.ExhaustedError
	if !errors.As(err, &be) || be.Result != pagebudget.NotSent {
		t.Fatalf("err = %v; want not_sent", err)
	}
	if got := o.requests.Load(); got != 0 || c.externalFlightsInFlight() != 0 {
		t.Fatalf("a spent budget dialled %d times / left a flight", got)
	}
}

// The key folds every input that changes what is sent: each variation below
// differs from the base in exactly one, and none may share its key.
func TestExternalFlight1320_KeyFoldsEveryRequestInput(t *testing.T) {
	ctx := context.Background()
	base := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/events", Verb: ptr.To(http.MethodGet), Headers: []string{"X-A: 1", "X-B: 2"}},
		Endpoint:    &endpoints.Endpoint{ServerURL: "https://ext.example", Token: "t"},
	}
	k0, ok := externalReadKeyOf(ctx, base)
	if !ok {
		t.Fatal("a GET was not keyed")
	}
	if k, _ := externalReadKeyOf(ctx, cloneRequestOptions(base)); k != k0 {
		t.Fatal("identical requests keyed differently")
	}
	vary := map[string]func(o *httpcall.RequestOptions, c *context.Context){
		"token":        func(o *httpcall.RequestOptions, _ *context.Context) { o.Endpoint.Token = "u" },
		"username":     func(o *httpcall.RequestOptions, _ *context.Context) { o.Endpoint.Username = "u" },
		"client cert":  func(o *httpcall.RequestOptions, _ *context.Context) { o.Endpoint.ClientCertificateData = "c" },
		"server url":   func(o *httpcall.RequestOptions, _ *context.Context) { o.Endpoint.ServerURL = "https://other.example" },
		"path":         func(o *httpcall.RequestOptions, _ *context.Context) { o.Path = "/events?ns=a" },
		"method":       func(o *httpcall.RequestOptions, _ *context.Context) { o.Verb = ptr.To(http.MethodHead) },
		"header value": func(o *httpcall.RequestOptions, _ *context.Context) { o.Headers = []string{"X-A: 1", "X-B: 3"} },
		"header order": func(o *httpcall.RequestOptions, _ *context.Context) { o.Headers = []string{"X-B: 2", "X-A: 1"} },
		"exported jwt": func(o *httpcall.RequestOptions, _ *context.Context) {
			o.Headers = append(o.Headers, "Authorization: Bearer j")
		},
		"payload": func(o *httpcall.RequestOptions, _ *context.Context) { o.Payload = ptr.To("{}") },
		"held":    func(_ *httpcall.RequestOptions, c *context.Context) { *c = credhttp.WithHeldCredential(*c) },
		"sa dial": func(_ *httpcall.RequestOptions, c *context.Context) { *c = cache.WithServiceAccountDial(*c) },
		"inert":   func(_ *httpcall.RequestOptions, c *context.Context) { *c = cache.WithInert(*c) },
		"impersonation": func(_ *httpcall.RequestOptions, c *context.Context) {
			*c = withEndpointImpersonation(*c, rest.ImpersonationConfig{UserName: "cohort"})
		},
		"impersonation group": func(_ *httpcall.RequestOptions, c *context.Context) {
			*c = withEndpointImpersonation(*c, rest.ImpersonationConfig{Groups: []string{"g"}})
		},
	}
	for name, mut := range vary {
		o := cloneRequestOptions(base)
		vctx := ctx
		mut(&o, &vctx)
		k, ok := externalReadKeyOf(vctx, o)
		if !ok {
			t.Errorf("%s: a read was not keyed", name)
			continue
		}
		if k == k0 {
			t.Errorf("%s: the key does not fold it — two different requests would share one call", name)
		}
	}
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		o := cloneRequestOptions(base)
		o.Verb = ptr.To(m)
		if _, ok := externalReadKeyOf(ctx, o); ok {
			t.Errorf("%s was keyed for sharing; a write must never be shared", m)
		}
	}
}

// END TO END through api.Resolve: concurrent resolves of the same external
// stage by the same identity reach the origin once, and each resolve records
// the answer in its own result.
func TestExternalFlight1320_E2E_ConcurrentResolvesDialOnce(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ac6FailFast(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	o := newHeldOrigin(t, false)
	joinedBefore := externalFetches.flightJoined.Load()
	// A path unique to this run: the process-wide flight map outlives a
	// -count=N run only while a call is in flight, never after.
	path := fmt.Sprintf("/events-%d", time.Now().UnixNano())
	stage := &templates.API{Name: "ev", Path: path, Verb: ptr.To(http.MethodGet), Filter: ptr.To(".ev")}
	const n = 6
	out := make(chan map[string]any, n)
	for i := 0; i < n; i++ {
		go func() { out <- runAC6Resolve(&ac6Fixture{server: o.srv}, []*templates.API{stage}) }()
	}
	eventually(5*time.Second, func() bool {
		return o.requests.Load() >= n || externalFetches.flightJoined.Load()-joinedBefore == n-1
	})
	o.open()
	for i := 0; i < n; i++ {
		select {
		case dict := <-out:
			got, ok := dict["ev"].(map[string]any)
			if !ok || got["version"] != "v1" {
				t.Fatalf("resolve: dict[ev] = %#v", dict["ev"])
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a resolve did not finish")
		}
	}
	if got := o.requests.Load(); got != 1 {
		t.Fatalf("origin saw %d dials for %d concurrent identical resolves; want 1 (resolve.go must reach the shared call)", got, n)
	}
}
