package main

// main_telemetry_widget_wiring_test.go: the ingest main builds (newRUMIngest)
// is wired to snowplow's own informers: a widget in a real, synced informer
// keeps its three attributes, one that is not in it loses namespace and name,
// and an unknown plural loses all three.
//
// MUTATION (RED): .WithWidgets(widgetSetFromCache) removed from newRUMIngest
// (every widget attribute is then dropped, and the first arm fails).

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestRUMWidgetAttrs_MainWiresTheIngestToTheInformers(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "tables"}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(), "kind": "Table",
		"metadata": map[string]any{"namespace": "demo", "name": "t1"},
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			gvr: "TableList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		}, obj)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v, %v", rw, err)
	}
	t.Cleanup(rw.Stop)
	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(30 * time.Second):
		t.Fatal("informer did not sync")
	}
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(prev) })

	c := newCollector(t, nil)
	t.Cleanup(c.srv.Close)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", c.srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mux := http.NewServeMux()
	mux.Handle("GET /call", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	mountTelemetryRoute(newRouteTable(mux), use.NewChain(), debugAuthTestKeys, newRUMIngest(ctx, mux))
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)

	mk := func(r, ns, n string) otlpSpan {
		return widgetFetchWith(strAttr(wResKey, r), strAttr(wNsKey, ns), strAttr(wNameKey, n))
	}
	before := rum.Read()
	rec := postRUM(t, mux, mintRUMToken(t, "widget-wiring"), otlpBody(t, rum.ServiceName, nil,
		mk("tables", "demo", "t1"), mk("tables", "demo", "gone"), mk("tablez", "demo", "t1")))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	out := awaitExport(t, c).GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()
	if len(out) != 3 {
		t.Fatalf("forwarded %d spans, want 3", len(out))
	}
	for i, want := range []string{
		wBase + "," + wResKey + "," + wNsKey + "," + wNameKey,
		wBase + "," + wResKey,
		wBase,
	} {
		if got := attrKeys(out[i]); got != want {
			t.Errorf("span %d forwards [%s], want [%s]", i, got, want)
		}
	}
	if d := rum.Read().Attributes - before.Attributes; d != 5 {
		t.Errorf("attributes_dropped delta %d, want 5 (2 for the absent widget, 3 for the unknown plural)", d)
	}
}
