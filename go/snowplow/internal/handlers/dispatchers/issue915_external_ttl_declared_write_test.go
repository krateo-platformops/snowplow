package dispatchers

// issue915_external_ttl_declared_write_test.go — #915 (#903 F1 on every Put
// path): a widget that opted into the external-TTL cache
// (krateo.io/external-cache-ttl-seconds) stores an external-touched resolve
// ON PURPOSE. A resolve whose apiRef RESTAction DECLARES a write is never
// stored, that Put included: otherwise a later /call of the widget within the
// TTL is an L1 hit and its write never runs.
//
//   X1  the annotated widget whose apiRef RESTAction declares a POST: two
//       customer /calls send the POST twice and leave no cell resident.
//   X2  control: the same annotated widget with a read-only external touch IS
//       stored with the external TTL (the opt-in path is live in this harness,
//       so X1's absence of a cell is the decline, not a dead path).
//
// Mutation (logic-only): drop `!extTouchedSink.DeclaredWrite()` from the
// external-TTL branch in widgets.go → X1 RED (1 POST: the second /call is a
// hit; the cell is resident).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/client-go/rest"
)

// x915Upstream counts the POSTs it receives.
func x915Upstream(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var posts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &posts
}

func TestIssue915_X1_ExternalTTLWidgetNeverStoresADeclaredWriteResolve(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	h := newK547Harness(t, k547DenyBackground)
	h.widgetU.SetAnnotations(map[string]string{externalCacheTTLAnnotation: "300"})
	up, posts := x915Upstream(t)
	inner := h.resolve
	// The widget's apiRef RESTAction, resolved by the real api resolver under
	// the widget dispatcher's ctx (and so its Put-gate sinks): it declares and,
	// on this customer request, sends one POST.
	t.Cleanup(setWidgetsResolveForTest(func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		rctx := cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: up.URL})
		create := &templates.API{Name: "create", Verb: ptr.To(http.MethodPost), Path: "/create", Payload: ptr.To(`{"n":1}`)}
		_, _ = restactionsapi.ResolveWithFailure(rctx, restactionsapi.ResolveOptions{
			RC: &rest.Config{Host: up.URL}, Items: []*templates.API{create},
			RESTActionNamespace: k547WNS, RESTActionName: "ra-915",
		})
		return inner(ctx, opts)
	}))
	key := h.key(0, k547Changed)
	for i := 1; i <= 2; i++ {
		if _, code := h.get(0, k547Changed); code != http.StatusOK {
			t.Fatalf("/call #%d: status %d", i, code)
		}
	}
	_, resident := cache.ResolvedCache().GetNoTouch(key)
	if n := posts.Load(); n != 2 || resident {
		t.Fatalf("#915 RED: two customer /calls of an external-TTL widget whose RESTAction declares a write sent %d POST(s) "+
			"(want 2) and left the cell resident=%v (want false) — the external-TTL Put stored it, so the later /call was a hit "+
			"that never ran the write", n, resident)
	}
}

func TestIssue915_X2_Control_ExternalTTLWidgetStoresAReadOnlyExternalResolve(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.widgetU.SetAnnotations(map[string]string{externalCacheTTLAnnotation: "300"})
	h.external.Store(true) // a read-only genuine external touch
	if _, code := h.get(0, k547Changed); code != http.StatusOK {
		t.Fatalf("/call: status %d", code)
	}
	if e, ok := cache.ResolvedCache().GetNoTouch(h.key(0, k547Changed)); !ok || !e.ExternalTTL {
		t.Fatalf("PRE (control): the external-TTL Put did not store a read-only external resolve (resident=%v) — "+
			"X1 would be vacuous", ok)
	}
}
