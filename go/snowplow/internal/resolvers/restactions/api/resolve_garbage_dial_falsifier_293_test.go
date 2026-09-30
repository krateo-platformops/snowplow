//go:build unit
// +build unit

// resolve_garbage_dial_falsifier_293_test.go — #293 RED-first falsifier
// (garbage-dial class, split from #288).
//
// THE BUG (TRACED against origin/main@443c54cf, i.e. WITH #288's guard):
// #288's validInterpolatedPath (setup.go) guards only parseOK=TRUE
// empty-interpolation collapses; a path that ParseAPIServerPathToDep rejects
// (parseOK=false) returns valid=true (setup.go:163) and is DIALED. Two garbage
// shapes take that fall-through:
//   1. UNRENDERED "${": jqutil.MaybeQuery returns (s,false) when the "${" has no
//      matching "}" (jqutil.go:113), so evalJQ returns the raw path with a
//      literal "${" → parseOK=false → dialed (garbage 404).
//   2. FAILED evalJQ: on a jq Eval error, evalJQ returns err.Error() as the
//      path (setup.go:221) → a non-path string is dialed as the request path.
//
// THE FIX (arch-1217 ruling pending sub-case-2 form): neither garbage shape may
// be dialed. GREEN below asserts the design-STABLE core (skip OR recordItemError
// both prevent the dial): the fake apiserver is NEVER hit with a "${"-bearing or
// jq-error path; a legitimate parseOK=false external-shape path (no "${", no jq
// error) is STILL dialed (no regression to external-URL dialing or #288).
//
// Drives the REAL api.Resolve → createRequestOptions (the guard site) →
// dispatch → dial, recording every path the server receives. Hermetic
// (//go:build unit, no kind cluster).

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/client-go/rest"
)

// garbageDialFixture records the path of every request the fake server receives.
type garbageDialFixture struct {
	server *httptest.Server
	mu     sync.Mutex
	paths  []string
}

func newGarbageDialFixture(t *testing.T) *garbageDialFixture {
	t.Helper()
	f := &garbageDialFixture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		// RequestURI carries the raw path exactly as dialed (URL.Path may
		// decode %-escapes; we want the raw wire path for the "${" check).
		f.paths = append(f.paths, r.RequestURI)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	f.server = srv
	return f
}

func (f *garbageDialFixture) received() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.paths))
	copy(out, f.paths)
	return out
}

// garbage293Ctx: UserInfo (Resolve bails without it) + the fake endpoint via
// WithInternalEndpoint (nil-ref stages resolve to it). isSA-ness is irrelevant
// here — the defect is path rendering/validation, upstream of auth.
func garbage293Ctx(f *garbageDialFixture) context.Context {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "garbage-293-user"}),
	)
	return cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: f.server.URL})
}

func garbage293FailFast(t *testing.T) {
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
}

// TestFalsifier293_GarbagePathsAreNotDialed — the load-bearing RED/GREEN.
// Three sibling stages (all continueOnError so none aborts the resolve):
//   - unbalanced "${" path  (sub-case 1)
//   - jq-error path         (sub-case 2: evalJQ returns err.Error())
//   - a legitimate external-shape path "/good" (parseOK=false, no "${", no error)
//
// GREEN (post-fix): the server is hit ONLY with "/good"; the two garbage paths
// are never dialed.
// RED (today): the server also receives the "${"-bearing path and the jq-error
// string → the asserts below fire.
func TestFalsifier293_GarbagePathsAreNotDialed(t *testing.T) {
	garbage293FailFast(t)

	f := newGarbageDialFixture(t)

	// Sub-case 1: unbalanced "${" (no closing "}") → MaybeQuery false → the raw
	// path with a literal "${" survives to the dial.
	unbalanced := &templates.API{
		Name:            "unrendered_template",
		Path:            "/api/v1/namespaces/default/configmaps/${.missing",
		Verb:            ptr.To(http.MethodGet),
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("errUnrendered"),
	}
	// Sub-case 2: a balanced jq expr that ERRORS (divide by zero) → evalJQ
	// returns err.Error() as the path.
	jqError := &templates.API{
		Name:            "failed_evaljq",
		Path:            "${1/0}",
		Verb:            ptr.To(http.MethodGet),
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("errJQ"),
	}
	// Control: a legitimate parseOK=false path (external-shape, no "${", no jq
	// error) MUST still be dialed — guards against a fix that over-skips
	// external URLs (#293 "must NOT regress external-URL dialing").
	valid := &templates.API{
		Name:            "valid_control",
		Path:            "/good",
		Verb:            ptr.To(http.MethodGet),
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("errGood"),
	}

	beforeSkips := MalformedDialSkippedTotal()
	beforeUnrendered := MalformedDialSkippedByReason(reasonUnrenderedTemplate)
	beforeJQErr := MalformedDialSkippedByReason(reasonJQPathError)
	_ = Resolve(garbage293Ctx(f), ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{unbalanced, jqError, valid},
		RESTActionNamespace: "default",
		RESTActionName:      "garbage-293-falsifier",
	})

	got := f.received()

	// Counter-delta (arch-1217): both garbage stages must bump the skip counter
	// (the valid /good control does not) — observability, mirrors #288's C1.
	if d := MalformedDialSkippedTotal() - beforeSkips; d != 2 {
		t.Fatalf("#293: expected 2 malformed-dial skips (unrendered ${ + jq-error path); got delta %d (received=%v)", d, got)
	}
	// pm-1217 AXIS: the reason must reach the METRIC, not just the DEBUG — each
	// garbage shape bumps its OWN per-reason counter (attributable from expvar
	// alone, no log tailing; a single MIXED counter would be the fallthrough_total mistake).
	if d := MalformedDialSkippedByReason(reasonUnrenderedTemplate) - beforeUnrendered; d != 1 {
		t.Fatalf("#293: the unrendered-${ skip must bump the %q per-reason metric by 1; got delta %d", reasonUnrenderedTemplate, d)
	}
	if d := MalformedDialSkippedByReason(reasonJQPathError) - beforeJQErr; d != 1 {
		t.Fatalf("#293: the jq-error-path skip must bump the %q per-reason metric by 1; got delta %d", reasonJQPathError, d)
	}

	// Liveness + no-regression: the valid external-shape path WAS dialed.
	sawGood := false
	for _, p := range got {
		if p == "/good" {
			sawGood = true
		}
	}
	if !sawGood {
		t.Fatalf("premise/no-regression: the valid control path \"/good\" was NOT dialed (received=%v) — "+
			"either the harness is not dialing at all (vacuous) or the fix over-skips legitimate external-shape paths.",
			got)
	}

	// Sub-case 1 GREEN: no dialed path may carry a literal "${".
	for _, p := range got {
		if strings.Contains(p, "${") {
			t.Fatalf("#293 sub-case 1 (RED): an UNRENDERED \"${\"-bearing path was dialed as an apiserver path "+
				"(%q). An unrendered template must be skipped, never dialed. received=%v", p, got)
		}
	}

	// Sub-case 2 GREEN + the combined invariant: the ONLY path dialed is the
	// valid control. Any extra dial is a garbage dial (the jq-error string, or
	// the "${" path already caught above).
	if len(got) != 1 {
		t.Fatalf("#293 (RED): expected exactly ONE dial (the valid /good control); got %d dials: %v — "+
			"the garbage stages (unrendered \"${\" and jq-error path) were dialed instead of skipped.", len(got), got)
	}
}

// TestFalsifier293_ValidInterpolatedPath_RejectsUnrenderedTemplate — the unit
// complement for sub-case 1: validInterpolatedPath must reject a path carrying a
// literal "${" (today it returns true because ParseAPIServerPathToDep rejects it
// as parseOK=false and the guard early-returns true). No-regression rows pin that
// well-formed paths and legitimate external-shape paths stay valid.
func TestFalsifier293_ValidInterpolatedPath_RejectsUnrenderedTemplate(t *testing.T) {
	cases := []struct {
		name string
		path string
		verb string
		want bool
	}{
		{"unrendered ${ in name segment", "/api/v1/namespaces/default/configmaps/${.missing", http.MethodGet, false},
		{"unrendered ${ mid path", "/apis/g/v1/namespaces/${.ns}/things", http.MethodGet, false},
		{"well-formed single-object GET stays valid", "/api/v1/namespaces/default/configmaps/kube-root-ca.crt", http.MethodGet, true},
		{"external-shape path (no ${) stays valid", "/good", http.MethodGet, true},
		{"nameless LIST stays valid", "/api/v1/namespaces/default/configmaps", http.MethodGet, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := validInterpolatedPath(tc.path, tc.verb); got != tc.want {
				t.Fatalf("validInterpolatedPath(%q,%q) = %v, want %v", tc.path, tc.verb, got, tc.want)
			}
		})
	}
}

// TestFalsifier302_PayloadJQError_SkipsDial — #302 REVERSES the #293-era
// placeholder (formerly TestFalsifier293_PayloadJQError_PathStillDials), which
// PINNED the DEFERRED behaviour "a payload jq-error still dials, swallowed into
// the body." That deferred follow-up has now landed: a payload whose jq
// expression errors must NOT be dialed (the swallowed error would be the request
// BODY) — it must SKIP + bump the jq_payload_error per-reason counter. A
// valid-payload control still dials (discriminated skip, arm-that-cannot-fail).
//
// RED on the pre-#302 tree (evalJQ swallowed the error into the body → the
// bad-payload stage dialed → 2 dials); GREEN after (skip → 1 dial).
func TestFalsifier302_PayloadJQError_SkipsDial(t *testing.T) {
	garbage293FailFast(t)
	f := newGarbageDialFixture(t)

	// Erroring payload (jq divide-by-zero) on an otherwise-valid path/verb.
	badPayload := &templates.API{
		Name:            "payload_jq_error",
		Path:            "/bad-pl",
		Verb:            ptr.To(http.MethodPost),
		Payload:         ptr.To("${1/0}"),
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("errPl"),
	}
	// Control: a VALID (non-template) payload on the same shape MUST still dial.
	okPayload := &templates.API{
		Name:            "payload_ok_control",
		Path:            "/good-ok",
		Verb:            ptr.To(http.MethodPost),
		Payload:         ptr.To(`{"literal":"body"}`),
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("errOk"),
	}

	beforeTotal := MalformedDialSkippedTotal()
	beforePayload := MalformedDialSkippedByReason(reasonJQPayloadError)
	_ = Resolve(garbage293Ctx(f), ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{badPayload, okPayload},
		RESTActionNamespace: "default",
		RESTActionName:      "garbage-302-payload",
	})

	got := f.received()

	// The erroring-payload stage must be SKIPPED (its path never dialed); the
	// valid-payload control MUST dial. Exactly ONE dial, and it is /good-ok.
	if len(got) != 1 || got[0] != "/good-ok" {
		t.Fatalf("#302 payload (RED pre-fix): expected exactly ONE dial (the valid-payload control /good-ok); got %v — a payload jq-error must SKIP the dial (never dial a request whose body is a swallowed jq error), while the valid control still dials", got)
	}
	if d := MalformedDialSkippedByReason(reasonJQPayloadError) - beforePayload; d != 1 {
		t.Fatalf("#302 payload: the payload-jq-error skip must bump the %q per-reason metric by 1; got delta %d", reasonJQPayloadError, d)
	}
	if d := MalformedDialSkippedTotal() - beforeTotal; d != 1 {
		t.Fatalf("#302 payload: exactly one total skip expected; got delta %d", d)
	}
}

// TestFalsifier302_HeaderJQError_SkipsDial — the SECURITY crux (arch-1217 C2). A
// header whose jq expression errors must NOT be dialed: the swallowed error would
// be a garbage HEADER value (e.g. Authorization). Assert SKIP + jq_header_error
// counter + that the ONLY dialed request carries the intended Authorization, not
// an error string; a valid-header control still dials.
func TestFalsifier302_HeaderJQError_SkipsDial(t *testing.T) {
	garbage293FailFast(t)

	type dialRec struct{ path, auth string }
	var mu sync.Mutex
	var dials []dialRec
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		dials = append(dials, dialRec{r.RequestURI, r.Header.Get("Authorization")})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "garbage-302-user"}),
	)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})

	// Erroring header (jq divide-by-zero as the header value) on a valid path.
	badHeader := &templates.API{
		Name:            "header_jq_error",
		Path:            "/bad-hdr",
		Verb:            ptr.To(http.MethodGet),
		Headers:         []string{"${1/0}"},
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("errHdr"),
	}
	// Control: a VALID literal header MUST still dial, carrying its real value.
	okHeader := &templates.API{
		Name:            "header_ok_control",
		Path:            "/good-hdr",
		Verb:            ptr.To(http.MethodGet),
		Headers:         []string{"Authorization: Bearer literal-token"},
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("errOkHdr"),
	}

	beforeHeader := MalformedDialSkippedByReason(reasonJQHeaderError)
	_ = Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{badHeader, okHeader},
		RESTActionNamespace: "default",
		RESTActionName:      "garbage-302-header",
	})

	mu.Lock()
	got := append([]dialRec(nil), dials...)
	mu.Unlock()

	// The erroring-header stage must SKIP (never dial /bad-hdr). Only /good-hdr dials.
	if len(got) != 1 || got[0].path != "/good-hdr" {
		t.Fatalf("#302 header (RED pre-fix): expected exactly ONE dial (the valid-header control /good-hdr); got %+v — a header jq-error must SKIP the dial, never dial a request carrying a swallowed jq error as a header value", got)
	}
	// SECURITY: the one dialed request carries the intended Authorization, never
	// an error string — a garbage-header dial can never leak onto the wire.
	if got[0].auth != "Bearer literal-token" {
		t.Fatalf("#302 header SECURITY: the dialed request's Authorization must be the intended value, never a jq-error string; got %q", got[0].auth)
	}
	if d := MalformedDialSkippedByReason(reasonJQHeaderError) - beforeHeader; d != 1 {
		t.Fatalf("#302 header: the header-jq-error skip must bump the %q per-reason metric by 1; got delta %d", reasonJQHeaderError, d)
	}
}

// TestFalsifier293_EvalJQErrorContract — the contract-change unit arm
// (arch-1217): evalJQE SIGNALS a path jq-eval error (the #293 signal), while
// evalJQ stays byte-identical (err.Error()) for payload/header parity.
func TestFalsifier293_EvalJQErrorContract(t *testing.T) {
	_, err := evalJQE("${1/0}", nil)
	if err == nil {
		t.Fatalf("evalJQE must return a non-nil error on a jq eval failure (the #293 path-eval signal)")
	}
	if got := evalJQ("${1/0}", nil); got != err.Error() {
		t.Fatalf("evalJQ must stay byte-identical (err.Error()) for payload/header parity; got %q want %q", got, err.Error())
	}
	if v, e := evalJQE("/api/v1/namespaces/default/configmaps", nil); e != nil || v != "/api/v1/namespaces/default/configmaps" {
		t.Fatalf("evalJQE must pass a non-template path through with nil error; got (%q,%v)", v, e)
	}
}
