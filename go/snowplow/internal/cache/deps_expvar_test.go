// deps_expvar_test.go — 1.12.5 / #187 acceptance for the snowplow_deps
// surface.
//
// The point of the surface is that #187 could not be answered from the live
// 057 pod: every number that would have said "DELETE-driven invalidation
// stopped" was computed and then thrown into an INFO line the production
// LOG_LEVEL=warn discards. So the arms here are not "the map is non-empty" —
// they are:
//
//	1. the key is published and readable through expvar.Handler() (the actual
//	   /debug/vars route), not merely computable;
//	2. every documented stat is PRESENT. A missing key is a dashboard panel
//	   that renders "no data" instead of alerting, which is worse than a wrong
//	   number;
//	3. the three #187 stats TRACK REAL ACTIVITY — a real eviction moves
//	   evict_delete_total, a real panicking OnDelete moves
//	   delete_worker_panics_total. A surface that reports a constant zero is
//	   indistinguishable from a healthy cluster, which is precisely how the
//	   defect stayed invisible for 11 days.

package cache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"expvar"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// depsExpvarStats reads snowplow_deps back through the real expvar HTTP
// handler — the same route /debug/vars serves — and decodes it. Going
// through the handler rather than calling DepsStatsByStat() directly is
// deliberate: it proves the key is actually PUBLISHED, which is the half a
// direct call cannot observe.
func depsExpvarStats(t *testing.T) map[string]int64 {
	t.Helper()
	rec := httptest.NewRecorder()
	expvar.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/debug/vars returned %d", rec.Code)
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("decoding /debug/vars: %v", err)
	}
	raw, ok := all["snowplow_deps"]
	if !ok {
		t.Fatalf("snowplow_deps is NOT published at /debug/vars. The dep tracker's counters " +
			"stay invisible at LOG_LEVEL=warn, which is the #187 diagnosis gap.")
	}
	var stats map[string]int64
	if err := json.Unmarshal(raw, &stats); err != nil {
		t.Fatalf("decoding snowplow_deps: %v", err)
	}
	return stats
}

func TestDepsExpvar_PublishesEveryDocumentedStat(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	RegisterDepsExpvarForTest()

	stats := depsExpvarStats(t)

	want := []string{
		// dep records
		"records", "max_records", "record_total",
		"dropped_cap", "dropped_no_key", "remove_l1_total",
		// what the tracker does with events
		"evict_delete_total", "dirty_mark_total", "enqueue_update_total",
		"evict_self_gone_total", "self_notfound_evict_total",
		// 1.12.6 C4: the non-404 drop-point route on its own counter
		"evict_drop_point_total",
		// informer bridge: ADD gate
		"add_propagated", "add_dropped_pre_sync", "add_nil_syncch",
		// informer bridge: the unified dep-event worker (1.12.6 C1) — the
		// #187 H1 surface plus the probe outcomes
		"events_submitted_total", "dep_event_queue_depth", "delete_worker_panics_total",
		"probe_exists_total", "probe_absent_total",
		"probe_unknown_total", "probe_unknown_degraded_total",
		// the sampled reconcile audit (1.12.6 C3) — reconcile_divergence_total
		// is the pipeline-health number
		"reconcile_ticks_total", "reconcile_sampled_total", "reconcile_probed_total",
		"reconcile_divergence_total", "reconcile_unknown_total",
		"reconcile_skipped_no_edge_total", "reconcile_panics_total",
	}
	for _, k := range want {
		if _, ok := stats[k]; !ok {
			t.Errorf("snowplow_deps is missing stat %q — a missing key renders as \"no data\" "+
				"on a dashboard rather than as an alert (got %v)", k, stats)
		}
	}
	// 1.12.6 C1 retired the bounded-channel surface: the typed workqueue has no
	// capacity and no overflow case, so these keys must be GONE, not zero — a
	// dashboard still reading them would show a healthy "0" forever.
	for _, retired := range []string{"delete_queue_depth", "delete_queue_cap", "delete_queue_full_total"} {
		if _, ok := stats[retired]; ok {
			t.Errorf("snowplow_deps still publishes retired stat %q (1.12.6 C1 §3.5)", retired)
		}
	}
}

// TestDepsExpvar_TracksRealEvictionAndPanic is the arm that matters: the
// three #187 numbers must MOVE for real activity. A surface pinned at zero
// looks exactly like a healthy cluster.
func TestDepsExpvar_TracksRealEvictionAndPanic(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	RegisterDepsExpvarForTest()
	defer withCleanDepWatch(t)()

	gvr := schema.GroupVersionResource{
		Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes",
	}
	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)

	before := depsExpvarStats(t)

	// (1) A real informer DELETE of a self-representation -> one eviction.
	store.Put("L1_evictme", &ResolvedEntry{
		RawJSON: []byte(`{}`),
		Inputs: &ResolvedKeyInputs{
			CacheEntryClass: "widgets",
			Group:           gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
			Namespace: "ns", Name: "evictme",
		},
	})
	d.Record("L1_evictme", gvr, "ns", "evictme")

	// (2) A panicking OnDelete -> one lost eviction, counted.
	d.SetRefreshHook(func(string, schema.GroupVersionResource) {
		panic("simulated fault inside OnDelete")
	})
	store.Put("L1_other", &ResolvedEntry{
		RawJSON: []byte(`{}`),
		Inputs: &ResolvedKeyInputs{
			CacheEntryClass: "widgets",
			Group:           gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
			Namespace: "ns", Name: "somebody-else",
		},
	})
	d.Record("L1_other", gvr, "ns", "panicky")

	h := syncedWatcher(t, gvr).depEventHandlers(gvr)
	h.DeleteFunc(unstructuredObj(gvr, "ns", "panicky"))
	h.DeleteFunc(unstructuredObj(gvr, "ns", "evictme"))
	waitQuiet()

	after := depsExpvarStats(t)

	if after["evict_delete_total"] <= before["evict_delete_total"] {
		t.Errorf("evict_delete_total did not move across a real DELETE eviction (%d -> %d). "+
			"A frozen counter here is the #187 signature and MUST be visible.",
			before["evict_delete_total"], after["evict_delete_total"])
	}
	if after["delete_worker_panics_total"] <= before["delete_worker_panics_total"] {
		t.Errorf("delete_worker_panics_total did not move across a panicking OnDelete (%d -> %d). "+
			"A lost eviction that nobody counts is how DELETE handling died silently for 11 days.",
			before["delete_worker_panics_total"], after["delete_worker_panics_total"])
	}
	if after["records"] < 0 {
		t.Errorf("records went negative: %d", after["records"])
	}
}
