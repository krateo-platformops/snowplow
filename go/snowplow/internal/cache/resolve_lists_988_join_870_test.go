package cache

// resolve_lists_988_join_870_test.go — the bench joins two published families
// on their GVR key: snowplow_resolve_collection_lists ("gvr|reason" -> count,
// #988) and snowplow_store_swaps_by_gvr ({"gvr": {"outcome": n}}, #870). It
// asks "how many resolve LISTs reached the apiserver for a GVR while that GVR's
// store was being swapped", which is only answerable if both families spell
// one GVR the same way: schema.GroupVersionResource.String().

import (
	"context"
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// TestIssue988_JOIN_ResolveListsAndStoreSwapsKeyTheSameGVR: one LIST each for
// a core and a grouped GVR, and one swap outcome for each of the same GVRs,
// are read back from the two PUBLISHED expvar families (the bench's vantage);
// the GVR keys must be the same strings, gvr.String(). RED if either family
// changes how it spells a GVR (its resource name alone, a "group/version/
// resource" path, ...).
func TestIssue988_JOIN_ResolveListsAndStoreSwapsKeyTheSameGVR(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	reset988(t)
	resetStoreSwapStatsForTest()
	t.Cleanup(resetStoreSwapStatsForTest)
	RegisterExpvarForTest()

	gvrs := []struct {
		gvr  schema.GroupVersionResource
		path string
	}{
		{schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "/api/v1/namespaces/n/configmaps"},
		{schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1alpha1", Resource: "panels"},
			"/apis/composition.krateo.io/v1alpha1/namespaces/n/panels"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"List","apiVersion":"v1","items":[]}`))
	}))
	defer srv.Close()
	ctx := WithFallthroughScope(context.Background(), ScopeCallRestactions)
	want := make([]string, 0, len(gvrs))
	for _, g := range gvrs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+g.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := credhttp.Client(&http.Client{}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		NoteStoreSwapOutcomeForTest(g.gvr, gvrOutcomeInstalled)
		want = append(want, g.gvr.String())
	}
	sort.Strings(want)

	read := func(name string, into any) {
		t.Helper()
		v := expvar.Get(name)
		if v == nil {
			t.Fatalf("%s is not published", name)
		}
		if err := json.Unmarshal([]byte(v.String()), into); err != nil {
			t.Fatalf("%s: %v (%s)", name, err, v.String())
		}
	}
	var lists map[string]uint64
	read("snowplow_resolve_collection_lists", &lists)
	var swaps map[string]map[string]uint64
	read("snowplow_store_swaps_by_gvr", &swaps)

	var listGVRs, swapGVRs []string
	for k := range lists {
		i := strings.LastIndex(k, "|")
		if i < 0 {
			t.Fatalf("snowplow_resolve_collection_lists key %q is not gvr|reason", k)
		}
		listGVRs = append(listGVRs, k[:i])
	}
	for k := range swaps {
		swapGVRs = append(swapGVRs, k)
	}
	sort.Strings(listGVRs)
	sort.Strings(swapGVRs)
	if strings.Join(listGVRs, "\n") != strings.Join(want, "\n") {
		t.Errorf("JOIN RED: snowplow_resolve_collection_lists spells the GVRs %q; want gvr.String() %q", listGVRs, want)
	}
	if strings.Join(swapGVRs, "\n") != strings.Join(want, "\n") {
		t.Errorf("JOIN RED: snowplow_store_swaps_by_gvr spells the GVRs %q; want gvr.String() %q", swapGVRs, want)
	}
}
