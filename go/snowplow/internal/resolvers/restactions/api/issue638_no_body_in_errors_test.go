//go:build unit
// +build unit

package api

// issue638_no_body_in_errors_test.go — #638 rework (reviewer-628 finding 2).
//
// A stage-filter or iterator jq error must reach the log and dict[errorKey]
// with its CLASS (and, for a parse error, its position) — never with bytes of
// the response it failed on. gojq previews ~24 bytes of the failing value into
// most runtime errors; on a Secret/ConfigMap read that is credential material.
//
// The arm drives the REAL Resolve over a Secret-shaped response, through the
// #503 capture sink (every record, every attribute, WithAttrs honoured), and
// asserts the Secret's distinctive prefix appears in NO log record and NOT in
// dict[errorKey]. It first asserts each failure path FIRED with its class, so a
// refactor that stopped logging cannot turn it green.
//
// Also covered: the iterator "unable to execute iterator" line that predates
// #638, and a non-JSON body (encoding/json quotes the offending body byte).

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/client-go/rest"
)

// s638Sentinel is the Secret value's distinctive prefix — the bytes gojq's preview
// would copy (it previews the first ~24 bytes of a string).
const s638Sentinel = "withheld-sentinel-638-zz"

func newS638SecretServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/secret":
			fmt.Fprintf(w, `{"kind":"Secret","apiVersion":"v1","metadata":{"name":"db"},"data":{"password":%q}}`, s638Sentinel)
		case "/notjson":
			fmt.Fprint(w, s638Sentinel+" this body is not JSON")
		default:
			fmt.Fprint(w, `{"ok":true}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func s638Resolve(t *testing.T, srv *httptest.Server, stages []*templates.API) (map[string]any, *s503Capture) {
	t.Helper()
	rec := &s503Capture{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s638-user", Groups: []string{"devs"}}),
		xcontext.WithLogger(slog.New(rec.handler())),
	)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
	ctx, _ = cache.WithStageErrorSink(ctx)
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               stages,
		RESTActionNamespace: "ns-638",
		RESTActionName:      "ra-638-secret",
	})
	return dict, rec
}

func s638AssertNoToken(t *testing.T, dict map[string]any, rec *s503Capture, errKey string) {
	t.Helper()
	for _, r := range rec.snapshot() {
		if strings.Contains(r.all, s638Sentinel) {
			t.Errorf("a log record carries response bytes: level=%s %s", r.level, r.all)
		}
	}
	b, _ := json.Marshal(dict[errKey])
	if strings.Contains(string(b), s638Sentinel) {
		t.Errorf("dict[%q] carries response bytes: %s", errKey, b)
	}
}

func s638AssertFired(t *testing.T, dict map[string]any, rec *s503Capture, errKey, want string) {
	t.Helper()
	b, _ := json.Marshal(dict[errKey])
	if !strings.Contains(string(b), want) {
		t.Fatalf("dict[%q] = %s; want the failure surfaced with %q — the arm did not exercise the path", errKey, b, want)
	}
	for _, r := range rec.snapshot() {
		if r.level >= slog.LevelError && strings.Contains(r.all, want) {
			return
		}
	}
	t.Fatalf("no ERROR record names %q — the failure was not logged", want)
}

// TestIssue638_StageFilterError_NoResponseBytes — RED on e8b15c5a: gojq's
// preview of the Secret value rides the "stage filter failed" and "response
// handler failed" ERROR lines and dict[errorKey].
func TestIssue638_StageFilterError_NoResponseBytes(t *testing.T) {
	iterFailFastRetries(t)
	srv := newS638SecretServer(t)
	for _, c := range []struct{ filter, class string }{
		{`.sec.data.password | keys`, "func0TypeError in keys"},
		{`.sec.data.password[]`, "iteratorError"},
		{`.sec.data.password | error`, "exitCodeError"},
		{`.sec.data.password | fromjson`, "func0WrapError"},
		{`.sec | .data.password + 1`, "binopTypeError"},
	} {
		t.Run(c.class, func(t *testing.T) {
			dict, rec := s638Resolve(t, srv, []*templates.API{{
				Name: "sec", Path: "/secret", Verb: ptr.To(http.MethodGet),
				Filter: ptr.To(c.filter), ErrorKey: ptr.To("secErr"), ContinueOnError: ptr.To(true),
			}})
			s638AssertFired(t, dict, rec, "secErr", c.class)
			s638AssertNoToken(t, dict, rec, "secErr")
		})
	}
}

// TestIssue638_IteratorError_NoResponseBytes — the iterator line ("unable to
// execute iterator") predates #638 and carried the same preview.
func TestIssue638_IteratorError_NoResponseBytes(t *testing.T) {
	iterFailFastRetries(t)
	srv := newS638SecretServer(t)
	dict, rec := s638Resolve(t, srv, []*templates.API{
		{Name: "sec", Path: "/secret", Verb: ptr.To(http.MethodGet)},
		{
			Name: "fan", Path: `${ "/x/" + . }`, Verb: ptr.To(http.MethodGet),
			DependsOn: &templates.Dependency{Name: "sec", Iterator: ptr.To(`.sec.data.password | keys`)},
			ErrorKey:  ptr.To("fanErr"),
		},
	})
	s638AssertFired(t, dict, rec, "fanErr", "func0TypeError in keys")
	s638AssertNoToken(t, dict, rec, "fanErr")
}

// TestIssue638_NonJSONBody_NoResponseBytes — encoding/json quotes the offending
// body byte ("invalid character 'S' ..."). Driven at the shared []byte handler
// (jsonHandlerBytesApply): the external fetch converts non-JSON bodies before
// they reach it, but the informer / internal-rest-config / nested-call bytes
// paths feed it directly. The decode error keeps class + offset only.
func TestIssue638_NonJSONBody_NoResponseBytes(t *testing.T) {
	err := jsonHandlerBytesApply(context.Background(), jsonHandlerOptions{key: "bad", out: map[string]any{}},
		[]byte(s638Sentinel+" this body is not JSON"))
	if err == nil {
		t.Fatal("a non-JSON body decoded — the arm cannot see the decode error")
	}
	if strings.Contains(err.Error(), "invalid character") || strings.Contains(err.Error(), "'S'") {
		t.Errorf("decode error quotes a body byte: %v", err)
	}
	if !strings.Contains(err.Error(), "byte offset") {
		t.Errorf("decode error lost its position: %v", err)
	}
}

// TestIssue638_StreamIterator_IsAnHonestStageError — the portal's
// cluster-compositions shape. The jq STREAM form (`.spokeCrds.compositionCrds[]?`)
// was never supported by the iterator contract: on origin/main it failed, was
// logged, and the stage silently dispatched nothing (an empty table, cacheable).
// #638 keeps main's element semantics exactly and makes that failure a STAGE
// ERROR (sink bumped → no Put; dict[errorKey]); supporting the stream form is
// a separate feature. The array form over the same data is unchanged.
func TestIssue638_StreamIterator_IsAnHonestStageError(t *testing.T) {
	iterFailFastRetries(t)
	var hits []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/crds":
			fmt.Fprint(w, `{"items":[{"spec":{"group":"composition.krateo.io","plural":"a"}},{"spec":{"group":"composition.krateo.io","plural":"b"}},{"spec":{"group":"other.io","plural":"c"}}]}`)
		default:
			hits = append(hits, r.URL.Path)
			fmt.Fprintf(w, `{"path":%q}`, r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	run := func(iter string) (map[string]any, *cache.StageErrorSink) {
		hits = nil
		rec := &s503Capture{}
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s638-user"}),
			xcontext.WithLogger(slog.New(rec.handler())),
		)
		ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
		ctx, sink := cache.WithStageErrorSink(ctx)
		dict := Resolve(ctx, ResolveOptions{
			RC: &rest.Config{},
			Items: []*templates.API{
				{
					Name: "spokeCrds", Path: "/crds", Verb: ptr.To(http.MethodGet),
					Filter: ptr.To(`{ compositionCrds: [ .spokeCrds.items[]? | select(.spec.group == "composition.krateo.io") | { plural: .spec.plural } ] }`),
				},
				{
					Name: "spokeCompositions", Path: `${ "/c/" + .plural }`, Verb: ptr.To(http.MethodGet),
					DependsOn: &templates.Dependency{Name: "spokeCrds", Iterator: ptr.To(iter)},
					ErrorKey:  ptr.To("spokeCompositionsError"), ContinueOnError: ptr.To(true),
				},
			},
			RESTActionNamespace: "ns-638", RESTActionName: "cluster-compositions-shape",
		})
		return dict, sink
	}

	// Array form: unchanged — every element dispatched, no stage error.
	dict, sink := run(`.spokeCrds.compositionCrds`)
	if sink.Count() != 0 || strings.Join(hits, ",") != "/c/a,/c/b" {
		t.Fatalf("array form: sink=%d dispatched=%v, want 0 and [/c/a /c/b]", sink.Count(), hits)
	}
	if _, ok := dict["spokeCompositions"]; !ok {
		t.Fatalf("array form: dict[spokeCompositions] absent (keys=%v)", keysOf(dict))
	}

	// Stream form: an honest stage error, nothing dispatched, Put declined.
	dict, sink = run(`.spokeCrds.compositionCrds[]?`)
	if len(hits) != 0 {
		t.Errorf("stream form dispatched %v; main dispatched nothing", hits)
	}
	if sink.Count() == 0 {
		t.Errorf("stream form: no stage error — the empty result would be cached as if the spoke had no compositions")
	}
	if _, ok := dict["spokeCompositionsError"]; !ok {
		t.Errorf("stream form: dict[spokeCompositionsError] absent — the failure is not surfaced (keys=%v)", keysOf(dict))
	}
}
