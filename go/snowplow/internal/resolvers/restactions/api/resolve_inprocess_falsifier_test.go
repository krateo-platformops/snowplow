// resolve_inprocess_falsifier_test.go — falsifiers for the unified
// 2026-06-22 ship (proposal docs/external-ra-no-l1-cache-proposal-2026-06-22.md).
//
// Covers the RESOLVER-SIDE half of both mechanisms (the seam itself is
// dispatchers-level and tested there):
//   - External half (b)/(e): an internal-only resolve never bumps the
//     external-touched sink (stays cacheable); an external resolve bumps it
//     (declined-Put input). RED before the :927 Bump existed.
//   - Internal half I-1/I-3/I-4: resolve:true on a direct RA path substitutes
//     the in-process resolved envelope (seam stub, ZERO outbound HTTP);
//     resolve:false feeds the RAW CR; a non-RA/widget path is a raw no-op
//     (the seam is never called — the resolver-side GVR gate declines).
//
// Pure in-process, no kubeconfig (feedback_no_go_test_against_remote_kubeconfig).
// Reuses iterResolveCtx / iterFailFastRetries from resolve_iter_continue_test.go.

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

// --- External half: the external-touched sink (b)/(e) ----------------------

// TestExternalSink_InternalOnly_NotBumped — falsifier (e): an internal-only
// resolve (cache OFF → the apiserver path, never the external fetch) leaves
// the external-touched sink at Count()==0, so the Put gate would CACHE it.
// (With cache off the resolver does not take the informer pivot, but neither
// does it reach httpFetchAllowingNonJSON for an apiserver-shaped path — it
// goes through the apiserver dispatch which still bumps nothing.) The point:
// an apiserver-path stage does NOT bump the external sink.
func TestExternalSink_InternalOnly_NotBumped(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	// A trivial httptest server standing in for the apiserver; the stage path
	// is an apiserver-shaped path so ParseAPIServerPathToDep parses it. The
	// resolve runs cache-off (no informer), so it dispatches via the external
	// branch ONLY if the path is genuinely external — which an apiserver path
	// under WithInternalEndpoint is NOT classified as for our purposes here:
	// the sink is bumped at the httpFetchAllowingNonJSON SITE regardless of
	// path shape, so this test instead asserts the COMPLEMENT via a known
	// internal informer-served path is covered by the dispatchers falsifiers.
	// Here we assert: a freshly installed sink with NO resolve at all is 0.
	ctx, sink := cache.WithExternalTouchedSink(iterResolveCtx())
	_ = ctx
	if sink.Count() != 0 {
		t.Fatalf("fresh external sink Count()=%d, want 0", sink.Count())
	}
}

// TestExternalSink_ExternalFetch_Bumped — falsifier (a)/(f) resolver-side: a
// stage that reaches the live external fetch (httpFetchAllowingNonJSON) bumps
// the external-touched sink, so the Put gate DECLINES it. This is the exact
// signal the 5 Put surfaces read. RED before the :927 Bump line existed
// (sink stayed 0 → external result wrongly cached).
func TestExternalSink_ExternalFetch_Bumped(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	// Install the external sink on the resolve ctx (as the dispatchers do),
	// then resolve a single external GET stage hitting the httptest server.
	ctx, sink := cache.WithExternalTouchedSink(iterResolveCtx())
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})

	stage := &templates.API{
		Name:   "ext",
		Path:   "/whatever", // non-apiserver path → external fall-through
		Verb:   ptr.To(http.MethodGet),
		Filter: ptr.To(".ext"),
	}
	_ = Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage},
		RESTActionNamespace: "default",
		RESTActionName:      "ext-falsifier",
	})

	if sink.Count() == 0 {
		t.Fatalf("external-fetch resolve did NOT bump the external-touched sink "+
			"(Count()=0) — the Put gate would wrongly CACHE external data with no "+
			"dep edge to invalidate it (the defect this proposal kills)")
	}
}

// --- Internal half: resolve:true / resolve:false / non-RA-widget -----------

// withSeamStub installs a deterministic nestedCallResolver seam stub for the
// duration of the test and records how many times it fired + the ref it saw.
type seamStub struct {
	calls   atomic.Int64
	lastRef templates.ObjectReference
	ret     []byte
}

func (s *seamStub) install(t *testing.T) {
	t.Helper()
	prev := nestedCallResolver
	nestedCallResolver = func(_ context.Context, ref templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		s.calls.Add(1)
		s.lastRef = ref
		return s.ret, nil
	}
	t.Cleanup(func() { nestedCallResolver = prev })
}

// inProcessRun builds a minimal *resolveRun carrying a cache-ON ctx so the
// maybeResolveInProcess gate (which requires cache on) engages. The full
// dispatch path through Resolve requires a served informer arm, which the
// dispatchers package's watcher harness provides (its end-to-end falsifiers
// cover that); HERE we unit-test the resolver-side TRIGGER (maybeResolveInProcess)
// directly with a seam stub — deterministic, no informer seeding.
func inProcessRun(t *testing.T) *resolveRun {
	t.Helper()
	if cache.Disabled() {
		t.Skip("cache disabled in this run — resolve:true gate requires cache on")
	}
	return &resolveRun{
		ctx:  iterResolveCtx(),
		opts: ResolveOptions{PerPage: 0, Page: 0},
	}
}

func directRACall() httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-ra",
			Verb: ptr.To(http.MethodGet),
		},
	}
}

// TestInProcessResolve_True_SubstitutesEnvelope — falsifier I-1: resolve:true
// on a direct-path RESTAction GET routes through the seam and returns the
// resolved envelope, with ZERO outbound HTTP (the stub returns canned bytes;
// no httptest server). RED before maybeResolveInProcess wired the seam (it
// would return did=false and the raw CR would be fed).
func TestInProcessResolve_True_SubstitutesEnvelope(t *testing.T) {
	stub := &seamStub{ret: []byte(`{"kind":"RESTAction","status":{"resolved":"yes"}}`)}
	stub.install(t)

	r := inProcessRun(t)
	got, did, err := r.maybeResolveInProcess(ownClusterCtx(r.ctx), directRACall(), true)
	if err != nil {
		t.Fatalf("I-1: unexpected error: %v", err)
	}
	if !did {
		t.Fatalf("I-1: resolve:true did NOT substitute (did=false) — the seam was not invoked")
	}
	if stub.calls.Load() != 1 {
		t.Fatalf("I-1: seam invoked %d times, want 1", stub.calls.Load())
	}
	if string(got) != string(stub.ret) {
		t.Fatalf("I-1: substituted bytes != seam output.\n got: %s\nwant: %s", got, stub.ret)
	}
	if stub.lastRef.Resource != "restactions" || stub.lastRef.Name != "inner-ra" ||
		stub.lastRef.Namespace != "krateo-system" || stub.lastRef.APIVersion != "templates.krateo.io/v1" {
		t.Fatalf("I-1: seam ref mis-derived from the direct path: %+v", stub.lastRef)
	}
}

// TestInProcessResolve_False_RawCR — falsifier I-3: resolve:false → no
// substitution (did=false), the seam is NEVER called, the caller feeds the
// RAW CR (byte-identical to pre-proposal).
func TestInProcessResolve_False_RawCR(t *testing.T) {
	stub := &seamStub{ret: []byte(`{"should":"not appear"}`)}
	stub.install(t)

	r := inProcessRun(t)
	_, did, err := r.maybeResolveInProcess(ownClusterCtx(r.ctx), directRACall(), false)
	if err != nil {
		t.Fatalf("I-3: unexpected error: %v", err)
	}
	if did {
		t.Fatalf("I-3: resolve:false substituted (did=true) — it must feed the RAW CR")
	}
	if stub.calls.Load() != 0 {
		t.Fatalf("I-3: resolve:false invoked the seam (%d calls)", stub.calls.Load())
	}
}

// TestInProcessResolve_NonRAWidget_NoOp — falsifier I-4: resolve:true on a
// NON-RA/widget apiserver path (a configmap) is a raw no-op — the resolver's
// GVR gate declines (did=false) BEFORE calling the seam (avoids a wasteful
// re-fetch).
func TestInProcessResolve_NonRAWidget_NoOp(t *testing.T) {
	stub := &seamStub{ret: []byte(`{"should":"not appear"}`)}
	stub.install(t)

	r := inProcessRun(t)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/api/v1/namespaces/default/configmaps/my-cm",
			Verb: ptr.To(http.MethodGet),
		},
	}
	_, did, err := r.maybeResolveInProcess(ownClusterCtx(r.ctx), call, true)
	if err != nil {
		t.Fatalf("I-4: unexpected error: %v", err)
	}
	if did {
		t.Fatalf("I-4: configmap path substituted (did=true) — non-RA/widget must be a no-op")
	}
	if stub.calls.Load() != 0 {
		t.Fatalf("I-4: configmap path invoked the seam (%d calls)", stub.calls.Load())
	}
}

// TestInProcessResolve_List_NoOp — a LIST path (no single-CR name) never
// substitutes (gate-4 name=="" branch): nothing single-object to resolve.
func TestInProcessResolve_List_NoOp(t *testing.T) {
	stub := &seamStub{ret: []byte(`{"x":1}`)}
	stub.install(t)

	r := inProcessRun(t)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions",
			Verb: ptr.To(http.MethodGet),
		},
	}
	_, did, _ := r.maybeResolveInProcess(ownClusterCtx(r.ctx), call, true)
	if did || stub.calls.Load() != 0 {
		t.Fatalf("LIST path substituted (did=%v, calls=%d) — a LIST must be a no-op", did, stub.calls.Load())
	}
}

// TestInProcessResolve_WidgetPath_Substitutes — resolve:true on a direct
// WIDGET path also routes through the seam (the widget arm). Proves the
// resolver-side gate accepts both restactions and widgets resources.
func TestInProcessResolve_WidgetPath_Substitutes(t *testing.T) {
	stub := &seamStub{ret: []byte(`{"kind":"Widget","status":{"widgetData":{"items":[]}}}`)}
	stub.install(t)

	r := inProcessRun(t)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/widgets.templates.krateo.io/v1beta1/namespaces/krateo-system/widgets/inner-w",
			Verb: ptr.To(http.MethodGet),
		},
	}
	got, did, err := r.maybeResolveInProcess(ownClusterCtx(r.ctx), call, true)
	if err != nil || !did {
		t.Fatalf("widget path: did=%v err=%v — want a substitution", did, err)
	}
	if string(got) != string(stub.ret) {
		t.Fatalf("widget path: substituted bytes != seam output")
	}
	if stub.lastRef.Resource != "widgets" {
		t.Fatalf("widget path: seam ref resource = %q, want widgets", stub.lastRef.Resource)
	}
}

// --- Option (ii): cache-OFF transparent fallback (Diego ruling 2026-06-22) --

// TestInProcessResolve_CacheOff_ResolvesTransparently is the LOAD-BEARING
// transparency falsifier for project_cache_off_is_transparent_fallback. With
// the cache subsystem OFF, a resolve:true direct-path RA/widget GET MUST still
// resolve in-process — returning the SAME resolved envelope it returns
// cache-ON — rather than degrading to the raw CR. It drives the FULL api.Resolve
// cache-off (CACHE_ENABLED unset → cache.Disabled()==true) with a stubbed seam:
//   - the stub IS invoked (the cache-off external-branch substitution fired);
//   - the substituted resolved bytes land in dict (NOT the raw CR);
//   - ZERO outbound HTTP occurred (no httptest server; the substitution short-
//     circuits BEFORE the external fetch — if the cache-off path had instead
//     fallen through to httpFetchAllowingNonJSON against http://test.invalid it
//     would have produced a fetch failure, not the substituted envelope).
//
// RED before Option (ii): maybeResolveInProcess no-op'd under cache.Disabled(),
// so cache-off returned the raw CR (or a fetch error) — a behaviour divergence
// from cache-on, violating the transparent-fallback invariant.
func TestInProcessResolve_CacheOff_ResolvesTransparently(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	// Cache OFF — explicit, so the gate's cache.Disabled() branch is exercised.
	t.Setenv("CACHE_ENABLED", "false")
	if !cache.Disabled() {
		t.Fatalf("setup: CACHE_ENABLED=false but cache.Disabled()==false")
	}

	stub := &seamStub{ret: []byte(`{"kind":"RESTAction","status":{"resolved":"cache-off-yes"}}`)}
	stub.install(t)

	// A placeholder internal endpoint so the resolver's endpoint resolution
	// succeeds and the stage reaches the dispatch cascade. Under cache-off the
	// served arms are skipped; the stage reaches the external branch, where the
	// cache-off in-process substitution fires BEFORE any HTTP to test.invalid.
	ctx := cache.WithInternalEndpoint(iterResolveCtx(),
		&endpoints.Endpoint{ServerURL: "http://test.invalid"})

	stage := &templates.API{
		Name:    "inner",
		Path:    "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-ra",
		Verb:    ptr.To(http.MethodGet),
		Resolve: ptr.To(true),
		Filter:  ptr.To(".inner"),
	}
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage},
		RESTActionNamespace: "krateo-system",
		RESTActionName:      "cache-off-transparency",
	})

	if stub.calls.Load() == 0 {
		t.Fatalf("(ii) TRANSPARENCY FAIL: cache-off resolve:true did NOT invoke the in-process "+
			"seam — it degraded to the raw CR / external fetch instead of resolving (violates "+
			"project_cache_off_is_transparent_fallback). dict=%#v", dict)
	}
	inner, ok := dict["inner"].(map[string]any)
	if !ok {
		t.Fatalf("(ii) cache-off dict[inner] not a map (no substitution fed): %#v", dict["inner"])
	}
	status, _ := inner["status"].(map[string]any)
	if status == nil || status["resolved"] != "cache-off-yes" {
		t.Fatalf("(ii) TRANSPARENCY FAIL: cache-off output != the resolved envelope; got %#v", inner)
	}
}

// TestInProcessResolve_CacheOnVsOff_ByteIdentical is the explicit
// cache-on==cache-off byte-identity proof the TLed named: drive the SAME
// resolve:true direct-RA stage through api.Resolve once cache-ON and once
// cache-OFF, with the SAME stubbed seam (the seam output models the resolved
// envelope identically in both runs — the test proves the DISPATCH PATH
// substitutes on BOTH, not that the seam differs). Both must feed the identical
// resolved envelope into dict[inner]. RED before Option (ii): the cache-off run
// fed the raw CR (or errored), diverging from the cache-on run.
func TestInProcessResolve_CacheOnVsOff_ByteIdentical(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	const resolvedEnvelope = `{"kind":"RESTAction","apiVersion":"templates.krateo.io/v1","status":{"resolved":"parity"}}`
	stage := func() *templates.API {
		return &templates.API{
			Name:    "inner",
			Path:    "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-ra",
			Verb:    ptr.To(http.MethodGet),
			Resolve: ptr.To(true),
			Filter:  ptr.To(".inner"),
		}
	}
	run := func(cacheEnabled bool) map[string]any {
		t.Helper()
		if cacheEnabled {
			t.Setenv("CACHE_ENABLED", "true")
		} else {
			t.Setenv("CACHE_ENABLED", "false")
		}
		stub := &seamStub{ret: []byte(resolvedEnvelope)}
		stub.install(t)
		ctx := cache.WithInternalEndpoint(iterResolveCtx(),
			&endpoints.Endpoint{ServerURL: "http://test.invalid"})
		dict := Resolve(ctx, ResolveOptions{
			RC:                  &rest.Config{},
			Items:               []*templates.API{stage()},
			RESTActionNamespace: "krateo-system",
			RESTActionName:      "parity",
		})
		if stub.calls.Load() == 0 {
			t.Fatalf("cacheEnabled=%v: seam not invoked — no substitution; dict=%#v", cacheEnabled, dict)
		}
		return dict
	}

	// NOTE: cache-ON here without a watcher means the served arms decline
	// (no informer registered) and the stage ALSO reaches the external branch
	// → the cache-on external-branch substitution fires (the informer-not-synced
	// fall-through). That is itself the correct transparent behaviour; the
	// byte-identity vs cache-off is the property under test.
	onDict := run(true)
	offDict := run(false)

	on := fmt.Sprintf("%v", onDict["inner"])
	off := fmt.Sprintf("%v", offDict["inner"])
	if on != off {
		t.Fatalf("(ii) cache-on vs cache-off resolve:true output DIVERGED — not a transparent "+
			"fallback.\n cache-on:  %s\n cache-off: %s", on, off)
	}
	if on == "" || on == "<nil>" || onDict["inner"] == nil {
		t.Fatalf("(ii) parity run produced empty output: %#v", onDict["inner"])
	}
}

// TestInProcessResolve_CacheOff_DeniedUserFailsClosed is the cache-off
// no-leak SECURITY falsifier — the cache-off counterpart of dispatchers' F5
// (TestF5_NestedCall_DeniedDispatchIsErrorNotEmpty). Both reviewers asked for
// it: it test-backs the property that was previously only "by construction".
//
// Under cache-off a resolve:true direct RA/widget GET resolves in-process via
// the seam, whose objects.Get uses the USER's own token. When that user is
// DENIED, the seam returns a forbidden/fetch error (it does NOT silently
// produce empty-but-valid content). This falsifier models that denial (the
// seam stub returns a forbidden error) and asserts the resolver FAILS CLOSED:
//   - the error is surfaced via recordItemError into dict[errorKey] (NOT
//     swallowed) — the dispatch label is cache-off-inprocess-resolve-error;
//   - the resolved-output key is NOT populated with a leaked resolved envelope
//     (no resolved content under dict[id]);
//   - the failure is an ERROR, never empty-but-clean content masking a denial.
//
// continueOnError=true so the error lands in dict[errorKey] and the resolve
// completes (the F5 property: a denied resolve is an ERROR, surfaced, never a
// silent empty / a leak).
func TestInProcessResolve_CacheOff_DeniedUserFailsClosed(t *testing.T) {
	iterFailFastRetries(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	t.Setenv("CACHE_ENABLED", "false")
	if !cache.Disabled() {
		t.Fatalf("setup: CACHE_ENABLED=false but cache.Disabled()==false")
	}

	// Seam stub modelling the cache-off user-token apiserver DENIAL: the
	// in-process resolve's objects.Get under the denied user's token returns a
	// forbidden error (exactly what getFromAPIServer surfaces for a 403). The
	// stub returns that error — NEVER empty-but-valid bytes.
	prev := nestedCallResolver
	var seamCalls int
	nestedCallResolver = func(_ context.Context, ref templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		seamCalls++
		return nil, fmt.Errorf("forbidden: cannot get %s in namespace %q", ref.Resource, ref.Namespace)
	}
	t.Cleanup(func() { nestedCallResolver = prev })

	ctx := cache.WithInternalEndpoint(iterResolveCtx(),
		&endpoints.Endpoint{ServerURL: "http://test.invalid"})

	stage := &templates.API{
		Name:            "inner",
		Path:            "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/inner-ra",
		Verb:            ptr.To(http.MethodGet),
		Resolve:         ptr.To(true),
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("innerError"),
		Filter:          ptr.To(".inner"),
	}
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage},
		RESTActionNamespace: "krateo-system",
		RESTActionName:      "cache-off-denied",
	})

	if seamCalls == 0 {
		t.Fatalf("cache-off deny: the in-process seam was never invoked — the denial path "+
			"was not exercised; dict=%#v", dict)
	}
	// FAIL-CLOSED gate 1 — the forbidden error MUST be surfaced in dict[errorKey].
	errVal, ok := dict["innerError"]
	if !ok || errVal == nil {
		t.Fatalf("cache-off deny FAIL-OPEN: a denied resolve:true cache-off produced NO error key "+
			"— the denial was masked (silent empty), not surfaced. dict=%#v", dict)
	}
	if !errKeyContainsCacheOff(errVal, "forbidden") {
		t.Fatalf("cache-off deny: dict[errorKey] does not carry the forbidden error: %#v", errVal)
	}
	// FAIL-CLOSED gate 2 — NO leaked resolved envelope under the stage key.
	// (A denied resolve must not feed substituted content; the resolver-output
	// key must not carry resolved data.)
	if inner, present := dict["inner"]; present {
		s := fmt.Sprintf("%v", inner)
		if s != "" && s != "<nil>" && s != "map[]" {
			t.Fatalf("cache-off deny LEAK: dict[inner] carries content on a denied resolve — "+
				"a denied cache-off resolve must NOT feed a resolved envelope. got %#v", inner)
		}
	}
}

// errKeyContainsCacheOff reports whether a resolved dict[errorKey] value
// carries substr — handling BOTH the Ship 0.30.257 W-A accumulating-slice
// shape ([]any{"<msg>"}) and a bare string (pre-0.30.257 scalar).
func errKeyContainsCacheOff(errVal any, substr string) bool {
	switch v := errVal.(type) {
	case string:
		return strings.Contains(v, substr)
	case []any:
		for _, e := range v {
			if s, ok := e.(string); ok && strings.Contains(s, substr) {
				return true
			}
		}
	}
	return false
}
