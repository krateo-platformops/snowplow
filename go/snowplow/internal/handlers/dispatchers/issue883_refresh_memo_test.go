// issue883_refresh_memo_test.go — #883 F4 at the refresher's install site: the
// memo refreshResolveMemo hands the refresher is the one the dep tracker drops
// entries from, and its size is on /debug/vars as snowplow_resolve_memo.

package dispatchers

import (
	"encoding/json"
	"expvar"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestIssue883_RefreshMemoIsTrackedAndGauged — the installed refresher memo
// drops an entry when the informer moves its dependency (no Load needed), and
// snowplow_resolve_memo reports its entries and bytes before and after.
func TestIssue883_RefreshMemoIsTrackedAndGauged(t *testing.T) {
	cacheOnForMemo(t)
	resetRefreshMemoForTest()
	t.Cleanup(resetRefreshMemoForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	publishResolveMemoExpvar()

	gvr := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "benchapps"}
	const ns = "ns-883-refresh"
	d := cache.Deps()
	d.RecordList(t.Context(), "L1_883_refresh_producer", gvr, ns)
	dep := cache.DepKey{GVR: gvr, Namespace: ns, Name: "*"}

	m := refreshResolveMemo()
	body := []byte(`{"items":[{"metadata":{"name":"a"}}]}`)
	m.Store("k-883", body, []cache.DepKey{dep}, cache.DepGenEpochNow())

	gauge := func() map[string]any {
		t.Helper()
		v := expvar.Get("snowplow_resolve_memo")
		if v == nil {
			t.Fatal("snowplow_resolve_memo is not published")
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(v.String()), &out); err != nil {
			t.Fatalf("snowplow_resolve_memo is not a JSON object: %v", err)
		}
		return out
	}
	if g := gauge(); g["entries"] != float64(1) || g["body_bytes"] != float64(len(body)) {
		t.Fatalf("gauge after one Store: %v, want entries=1 body_bytes=%d", g, len(body))
	}
	_, movedBefore, _ := cache.ResolveMemoDropTotals()

	d.OnUpdate(gvr, ns, "obj-0") // the informer moves the dependency; nobody Loads

	if n := m.Entries(); n != 0 {
		t.Fatalf("#883 F4 RED: the refresher's memo kept %d entries after the dep tracker moved their dependency "+
			"— refreshResolveMemo must hand out a memo the dep tracker tracks", n)
	}
	if g := gauge(); g["entries"] != float64(0) || g["body_bytes"] != float64(0) {
		t.Fatalf("gauge after the move: %v, want entries=0 body_bytes=0", g)
	}
	if _, movedAfter, _ := cache.ResolveMemoDropTotals(); movedAfter != movedBefore+1 {
		t.Fatalf("dropped_moved_total moved by %d, want 1", movedAfter-movedBefore)
	}
}
