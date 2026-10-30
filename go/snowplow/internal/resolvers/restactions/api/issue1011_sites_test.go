package api

// issue1011_sites_test.go — #1011: every read optimisation serves a call only
// when its verb is a plain GET (isPlainGet). The collapse arms are in
// issue1011_collapse_verb_test.go; these pin the other serve sites that had no
// arm of their own (a logic mutation removing each gate survived the whole
// package before them):
//   C  the content cell: a write to a path whose content cell is warm is sent
//      to the upstream, never answered from the cell.
//   I  the internal-rest-config dispatch (background drivers): a non-GET call
//      is not served, and nothing is dialled for it.
//   N  the in-process resolve: a non-GET call to a RESTAction path is never
//      substituted by the nested resolve.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

// TestIssue1011_C_AWriteToAWarmContentCellPathIsSent: a GET of a namespaced
// collection warms its content cell; a POST to the same path must still reach
// the upstream on every call. RED when the content serve admits a non-GET call:
// the POST is answered from the cell and never sent.
func TestIssue1011_C_AWriteToAWarmContentCellPathIsSent(t *testing.T) {
	u := newUp1011(t)
	ctx := setup1011(t, u)
	const p = "/apis/widgets.krateo.io/v1/namespaces/team-a/widgets"
	read := &templates.API{Name: "read", Path: p, Verb: ptr.To(http.MethodGet),
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("readErr")}
	resolve1011(t, ctx, u, read)
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "team-a", ""))
	//serve-probe: test precondition — is the content cell warm; serves nothing
	if e, hit := cache.ResolvedCache().GetNoTouch(contentKey); !hit || e == nil {
		t.Fatal("setup: the GET did not warm the namespaced content cell")
	}
	write := &templates.API{Name: "write", Path: p, Verb: ptr.To(http.MethodPost),
		Payload:         ptr.To(`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget","metadata":{"name":"w"}}`),
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("writeErr")}
	for i := 1; i <= 2; i++ {
		raw := resolve1011(t, ctx, u, write)
		u.mu.Lock()
		n := u.count["POST "+p]
		u.mu.Unlock()
		if n != i {
			t.Fatalf("#1011 C RED: after call %d the upstream saw %d POSTs, want %d; outcomes %s", i, n, i, raw)
		}
	}
}

// TestIssue1011_I_TheInternalDispatchServesOnlyAPlainGet: on a background
// driver's context (an internal rest config), a non-GET call falls through
// (not served, no error) and dials nothing. RED when the internal dispatch
// admits a non-GET call.
func TestIssue1011_I_TheInternalDispatchServesOnlyAPlainGet(t *testing.T) {
	var dials atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dials.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","metadata":{},"items":[]}`))
	}))
	t.Cleanup(srv.Close)
	ctx := cache.WithInternalRESTConfig(context.Background(), &rest.Config{Host: srv.URL})
	for _, verb := range []string{http.MethodPost, http.MethodDelete, http.MethodHead} {
		call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
			Path: "/apis/widgets.krateo.io/v1/namespaces/team-a/widgets", Verb: ptr.To(verb)}}
		_, served, err := dispatchViaInternalRESTConfig(ctx, call)
		if served || err != nil {
			t.Errorf("#1011 I RED: a %s call through the internal dispatch was served=%v err=%v; want a plain fall-through", verb, served, err)
		}
	}
	if n := dials.Load(); n != 0 {
		t.Errorf("#1011 I RED: the internal dispatch dialled %d request(s) for non-GET calls", n)
	}
}

// TestIssue1011_N_TheInProcessResolveSubstitutesOnlyAPlainGet: a non-GET call
// to a RESTAction path is never substituted by the nested resolve. RED when
// the in-process resolve admits a non-GET call.
func TestIssue1011_N_TheInProcessResolveSubstitutesOnlyAPlainGet(t *testing.T) {
	// inProcessRun skips under cache-off; this arm sets cache on itself so it
	// always runs (the package's own in-process arms skip in the default gate).
	t.Setenv("CACHE_ENABLED", "true")
	stub := &seamStub{ret: []byte(`{"should":"not appear"}`)}
	stub.install(t)
	r := inProcessRun(t)
	for _, verb := range []string{http.MethodPut, http.MethodDelete, http.MethodHead} {
		call := directRACall()
		call.Verb = ptr.To(verb)
		if _, did, err := r.maybeResolveInProcess(r.ctx, call, true); did || err != nil {
			t.Errorf("#1011 N RED: a %s call was substituted (did=%v err=%v)", verb, did, err)
		}
	}
	if n := stub.calls.Load(); n != 0 {
		t.Errorf("#1011 N RED: the nested resolve ran %d time(s) for non-GET calls", n)
	}
}
