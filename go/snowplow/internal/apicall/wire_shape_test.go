package apicall

import (
	"net/url"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestParseResourcePath_DoesNotAllocate is #237 RW-2, moved with the parser.
// The classifier runs inside every outbound request (the reflector-path
// wrapper on the watcher's config, the #988 resolve-list counter on every
// config and client credhttp hardens), so every fallthrough GET-by-name on the
// serve path pays this scan. The earlier strings.Split allocated a []string
// before discovering the path was not a collection — on the single most common
// shape.
//
// Asserted as zero allocations rather than as a benchmark number, because the
// claim in the code is a claim about allocation, not about speed.
func TestParseResourcePath_DoesNotAllocate(t *testing.T) {
	paths := []string{
		"/apis/widgets.templates.krateo.io/v1beta1/namespaces/krateo-system/pageheaders/portal-builder-page-header",
		"/apis/widgets.templates.krateo.io/v1beta1/pageheaders",
		"/api/v1/namespaces/krateo-system/configmaps",
		"/api/v1/namespaces/krateo-system",
		"/version",
	}
	if n := testing.AllocsPerRun(200, func() {
		for _, p := range paths {
			_, _, _ = ParseResourcePath(p)
		}
	}); n != 0 {
		t.Fatalf("ParseResourcePath allocates %.1f time(s) per run over %d paths, want 0 — the "+
			"hot path here is a named GET falling through to the apiserver, and it must pay "+
			"no garbage for an instrument it is not the subject of", n, len(paths))
	}
}

func TestParseResourcePath_Shapes(t *testing.T) {
	cm := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	ns := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	ph := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders"}
	for _, c := range []struct {
		path       string
		gvr        schema.GroupVersionResource
		collection bool
		ok         bool
	}{
		{"/api/v1/configmaps", cm, true, true},
		{"/api/v1/namespaces/krateo-system/configmaps", cm, true, true},
		{"/api/v1/namespaces/krateo-system/configmaps/cm-1", cm, false, true},
		{"/api/v1/namespaces", ns, true, true},
		{"/api/v1/namespaces/krateo-system", ns, false, true},
		{"/apis/widgets.templates.krateo.io/v1beta1/pageheaders", ph, true, true},
		{"/apis/widgets.templates.krateo.io/v1beta1/namespaces/a/pageheaders/x/status", ph, false, true},
		{"/version", schema.GroupVersionResource{}, false, false},
		{"/apis", schema.GroupVersionResource{}, false, false},
		{"/apis/widgets.templates.krateo.io/v1beta1", schema.GroupVersionResource{}, false, false},
	} {
		gvr, collection, ok := ParseResourcePath(c.path)
		if gvr != c.gvr || collection != c.collection || ok != c.ok {
			t.Errorf("ParseResourcePath(%q) = (%v, %v, %v); want (%v, %v, %v)",
				c.path, gvr, collection, ok, c.gvr, c.collection, c.ok)
		}
	}
}

func TestClassifyCollectionQuery_Shapes(t *testing.T) {
	for _, c := range []struct{ q, want string }{
		{"watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan", WireWatchList},
		{"watch=true&resourceVersion=42", WireWatchPlain},
		{"limit=500&continue=abc", WireListPage},
		{"limit=500&resourceVersion=42", WireListEtcdDelegated},
		{"limit=500&resourceVersion=0", WireListCacheEligible},
		{"limit=500", WireListCacheEligible},
		{"", WireListCacheEligible},
	} {
		q, err := url.ParseQuery(c.q)
		if err != nil {
			t.Fatal(err)
		}
		if got := ClassifyCollectionQuery(q); got != c.want {
			t.Errorf("ClassifyCollectionQuery(%q) = %q; want %q", c.q, got, c.want)
		}
	}
}
