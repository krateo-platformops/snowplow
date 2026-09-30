// call_read_186_test.go — #186 read-route dispatch + read-only security
// falsifiers (arch/pm C1a/C1b/C1c). The body-carrying read route must never be
// able to create/mutate a resource, on EITHER dispatch arm:
//   - C1a matched   — a POST whose GVR has a resolve handler reaches THAT
//     handler (read-only by nature), not the write path;
//   - C1a unmatched — a POST whose GVR has none falls through to the read-only
//     CallRead, which dials the apiserver as a GET with no body;
//   - C1b control   — the SAME POST body to the WRITE handler (Call) DOES issue
//     a create (POST + body). This is the arm-that-cannot-fail
//     guard: it proves the fake apiserver CAN observe a create,
//     so the read-only arms discriminate rather than pass because
//     creation is globally impossible in the harness;
//   - C1c           — the GET-only Dispatcher still bypasses a POST (falls
//     through, never reaches a resolve handler) — the fix is
//     read-route-scoped, not a global relax of the method guard.
package handlers

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// spyHandler records whether it was invoked and answers 200.
type spyHandler struct{ called bool }

func (s *spyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.called = true
	w.WriteHeader(http.StatusOK)
}

func dispatchHandlers() (ra, widget *spyHandler, m map[string]http.Handler) {
	ra, widget = &spyHandler{}, &spyHandler{}
	// Keyed exactly as dispatchers.All() keys them (proxy.go: restactions gets
	// the "restactions." prefix; widgets is keyed by bare group).
	m = map[string]http.Handler{
		"restactions.templates.krateo.io": ra,
		"widgets.templates.krateo.io":     widget,
	}
	return
}

// TestReadDispatcher_MatchedGVR_ReachesResolveHandler — C1a matched arm.
func TestReadDispatcher_MatchedGVR_ReachesResolveHandler(t *testing.T) {
	ra, _, m := dispatchHandlers()
	next := &spyHandler{}
	h := ReadDispatcher(m)(next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call/read?apiVersion=templates.krateo.io/v1&resource=restactions&name=demo&namespace=krateo-system", nil)
	h.ServeHTTP(rec, req)

	if !ra.called {
		t.Fatal("C1a matched: a POST /call/read to a handled GVR must reach the resolve handler (read-only by nature)")
	}
	if next.called {
		t.Fatal("C1a matched: it must NOT fall through to the read-only Call — it has a resolve handler")
	}
}

// TestReadDispatcher_UnmatchedGVR_FallsThroughToReadOnly — C1a unmatched arm.
func TestReadDispatcher_UnmatchedGVR_FallsThroughToReadOnly(t *testing.T) {
	ra, widget, m := dispatchHandlers()
	next := &spyHandler{}
	h := ReadDispatcher(m)(next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call/read?apiVersion=example.com/v1&resource=foos&name=x&namespace=default", nil)
	h.ServeHTTP(rec, req)

	if !next.called {
		t.Fatal("C1a unmatched: a POST /call/read to an unhandled GVR must fall through to the read-only CallRead fallthrough")
	}
	if ra.called || widget.called {
		t.Fatal("C1a unmatched: a resolve handler must not be reached for an unhandled GVR")
	}
}

// TestGetDispatcher_GuardIntact_PostBypasses — C1c. The GET-only Dispatcher
// (mounted on GET /call and the write-verb routes) must STILL bypass a POST:
// the #186 fix adds ReadDispatcher, it does not relax the global guard. A POST
// through the plain Dispatcher must fall through, never reaching a resolve
// handler — otherwise every write-verb /call could reach the resolvers.
func TestGetDispatcher_GuardIntact_PostBypasses(t *testing.T) {
	ra, widget, m := dispatchHandlers()
	next := &spyHandler{}
	h := Dispatcher(m)(next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/call?apiVersion=templates.krateo.io/v1&resource=restactions&name=demo&namespace=krateo-system", nil)
	h.ServeHTTP(rec, req)

	if ra.called || widget.called {
		t.Fatal("C1c: the GET-only Dispatcher must NOT dispatch a POST to a resolve handler — that guard protects the write-verb passthrough")
	}
	if !next.called {
		t.Fatal("C1c: a POST through the GET-only Dispatcher must fall through to next")
	}

	// Sanity: the same Dispatcher DOES route a GET to the resolve handler.
	ra2, _, m2 := dispatchHandlers()
	next2 := &spyHandler{}
	h2 := Dispatcher(m2)(next2)
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", "/call?apiVersion=templates.krateo.io/v1&resource=restactions&name=demo&namespace=krateo-system", nil)
	h2.ServeHTTP(rec2, req2)
	if !ra2.called || next2.called {
		t.Fatalf("Dispatcher GET must route to the resolve handler; ra=%v next=%v", ra2.called, next2.called)
	}
}

// capturedDial records what the fake apiserver received.
type capturedDial struct {
	method  string
	bodyLen int
}

// fakeAPIServer stands up an httptest server that records the method + body of
// the single request it receives and answers 200 {}. Returns the endpoint ctx
// value and a pointer to the captured dial.
func fakeAPIServerCtx(t *testing.T) (endpoints.Endpoint, *capturedDial, func()) {
	t.Helper()
	// plumbing's xcontext.UserConfig rewrites ServerURL to
	// kubernetes.default.svc UNLESS test mode is on — enable it so the
	// injected httptest endpoint is actually dialed (the real /call path
	// runs in-cluster; here we only need to observe the outbound verb/body).
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	dial := &capturedDial{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dial.method = r.Method
		b, _ := io.ReadAll(r.Body)
		dial.bodyLen = len(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	return endpoints.Endpoint{ServerURL: srv.URL}, dial, func() {
		srv.Close()
		xenv.SetTestMode(prev)
	}
}

func readCtx(ep endpoints.Endpoint) (ctxBuilder func(*http.Request) *http.Request) {
	return func(req *http.Request) *http.Request {
		ctx := xcontext.BuildContext(req.Context(),
			xcontext.WithAccessToken("test-token"),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
			xcontext.WithUserConfig(ep),
		)
		return req.WithContext(ctx)
	}
}

// createBody is a resource payload — what a CREATE would ship.
const createBody = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default"},"data":{"k":"v"}}`

const nsPath = "/call?apiVersion=v1&resource=configmaps&namespace=default&name=cm-x"

// TestCall_WriteControl_IssuesCreate — C1b positive control (arm-that-cannot-
// fail). The write-capable Call() handler POSTs the body to the apiserver: the
// fake dial sees a POST with a non-empty body — a genuine create. This proves
// the harness CAN observe a create, so the read-only assertion below is real.
func TestCall_WriteControl_IssuesCreate(t *testing.T) {
	ep, dial, closeFn := fakeAPIServerCtx(t)
	defer closeFn()
	withCtx := readCtx(ep)

	rec := httptest.NewRecorder()
	req := withCtx(httptest.NewRequest("POST", nsPath, bytes.NewReader([]byte(createBody))))
	Call().ServeHTTP(rec, req)

	if dial.method != http.MethodPost {
		t.Fatalf("C1b control: the write handler must POST (create) to the apiserver; dialed %q", dial.method)
	}
	if dial.bodyLen == 0 {
		t.Fatal("C1b control: the write handler must forward the object body as the create payload; dialed an empty body")
	}
}

// TestCallRead_IsReadOnly_DialsGetNeverCreates — C1a read-only fallthrough.
// The SAME POST + body driven through CallRead() dials the apiserver as a GET
// with NO body — it can never create/mutate, even though the inbound method is
// POST and a body is present. Contrast with the write control above: same
// input, opposite apiserver verb, proving read-only-ness is the discriminator.
func TestCallRead_IsReadOnly_DialsGetNeverCreates(t *testing.T) {
	ep, dial, closeFn := fakeAPIServerCtx(t)
	defer closeFn()
	withCtx := readCtx(ep)

	rec := httptest.NewRecorder()
	req := withCtx(httptest.NewRequest("POST", nsPath, bytes.NewReader([]byte(createBody))))
	CallRead().ServeHTTP(rec, req)

	if dial.method != http.MethodGet {
		t.Fatalf("C1a read-only: CallRead must force a GET to the apiserver regardless of the inbound POST; dialed %q — a body-carrying read must never create", dial.method)
	}
	if dial.bodyLen != 0 {
		t.Fatalf("C1a read-only: CallRead must NOT forward a body as an object payload; dialed a %d-byte body", dial.bodyLen)
	}
}
