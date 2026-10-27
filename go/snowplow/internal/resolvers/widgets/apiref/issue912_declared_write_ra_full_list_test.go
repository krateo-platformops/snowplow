package apiref

// issue912_declared_write_ra_full_list_test.go — #912 re-review F-B: a
// paginated widget whose apiRef RESTAction DECLARES a write, end to end
// through the REAL apiref.Resolve → restactions.Resolve.
//
//   P1  two page /calls send the POST exactly twice (once each) and leave no
//       raFullList cell: the raFullList layer never serves a RESTAction that
//       declares a write (it is never stored, and the layer's first-sight path
//       would resolve it twice per /call).
//
// Mutations (logic-only):
//   - drop the declared-write fallback in raFullListServe → P1 RED (the
//     first-sight path resolves twice: 4 POSTs for 2 /calls);
//   - additionally drop PutRAFullListIfGen's verdict guard AND make
//     raFullListPermanentDecline / raFullListDeclines ignore declared_write →
//     P1 RED on the raKey cell too (the full list of a write RESTAction stored).

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

func TestIssue912_P1_PaginatedWidgetOverAWriteRA_OnePOSTPerCall_NoRAFullListCell(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()

	var posts atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(up.Close)

	const raName, ns = "i912-write-ra", edge3BackingNS
	writeRA := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": raName, "namespace": ns},
		"spec": map[string]any{
			"api": []any{
				// A list read (informer-served) the widget pages over …
				map[string]any{"name": "items", "verb": "GET",
					"path": "/apis/templates.krateo.io/v1/namespaces/" + ns + "/restactions"},
				// … and a write the RESTAction declares.
				map[string]any{"name": "create", "verb": "POST", "path": "/create",
					"payload": `{"n":1}`, "continueOnError": true, "errorKey": "createErr",
					"dependsOn": map[string]any{"name": "items"}},
			},
			"filter": `{ items: (.items.items // []) }`,
		},
	}}
	edge3NewWatcher(t, append(f6BuildFixture(), writeRA)...)
	added, syncCh := cache.Global().EnsureResourceType(gvr())
	if added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatal("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gvr(), 3*time.Second) {
		t.Fatal("restactions GVR never became servable")
	}

	userCtx := f6CtxWithUser(t, "admin", []string{"system:masters"})
	_, raKey, ok := seedFullListRAKey(userCtx, gvr(), ns, raName, nil)
	if !ok || raKey == "" {
		t.Fatalf("PRE: the raFullList key must derive (ok=%v key=%q)", ok, raKey)
	}
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: raName, Namespace: ns}, Resource: "restactions", APIVersion: "templates.krateo.io/v1",
	}
	for page := 1; page <= 2; page++ {
		// A customer /call of the widget: its own L1 key, its own sinks.
		ctx := cache.WithL1KeyContext(userCtx, "L1_i912_widget_page")
		ctx, _ = cache.WithExternalTouchedSink(ctx)
		ctx, _ = cache.WithStageErrorSink(ctx)
		ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: up.URL})
		if _, err := Resolve(ctx, ResolveOptions{ApiRef: ref, PerPage: 5, Page: page,
			RC: &rest.Config{Host: up.URL}}); err != nil {
			t.Fatalf("apiref.Resolve(page %d): %v", page, err)
		}
	}
	if n := posts.Load(); n != 2 {
		t.Errorf("#912 F-B RED: two page /calls of a widget over a write RESTAction sent %d POST(s), want exactly 2", n)
	}
	if _, stored := cache.ResolvedCache().GetNoTouch(raKey); stored {
		t.Error("#912 F-B RED: the raFullList cell of a RESTAction that declares a write was stored")
	}
}
