// issue1126_c1_state_derived_test.go — 1.12.6 C1 arms (design §3.7, §10
// rows D4 / E2, PM gate C2): the dep-event worker derives its action from the
// informer's CURRENT state, not from the event type.
//
// Every arm drives the REAL handler closures (rw.depEventHandlers) over a
// REAL synced informer on a fake dynamic client, the real DepTracker and the
// real store. Assertions are on the SERVED BODY (store.Get) — never only on a
// counter.
//
//	D4  — a late duplicate DELETE arriving after a same-name recreate must NOT
//	      evict the correct fresh entry. RED on main: isSelfRepresentation
//	      matches on coordinates only, so the queued DELETE evicts it.
//	E2  — in the schema-relist teardown window the informer is torn down;
//	      a boolean probe would read every object of the GVR as "absent" and
//	      evict the whole GVR. objUnknown must evict NOTHING during the window
//	      and resolve correctly after the informer is back.
//	C2  — a coordinate that stays objUnknown for the whole requeue budget
//	      must degrade to a dirty-mark (the refresher's re-fetch then decides
//	      against the apiserver), never sit in the queue or keep the entry
//	      silently. Until item 4 row 3 lands, the 403/500 leg of that refresh
//	      is bounded by the 3600 s TTL, not by seconds — stated, not implied.
//	OFF — CACHE_ENABLED=false / passthrough: the probe is objUnknown without
//	      touching the dynamic client, and the bridge never starts a worker.

package cache

import (
	"context"
	"testing"
	"time"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// c1FastBackoff shrinks the shared requeue backoff so the 5-requeue budget
// elapses in milliseconds. The knobs are the REFRESHER's (design §3.3: one
// backoff shape in the subsystem); the dep-event queue reads them at
// singleton construction, which withCleanDepWatch resets.
func c1FastBackoff(t *testing.T) {
	t.Helper()
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
}

// --- D4 — a late duplicate DELETE must not evict the recreated entry --------

func TestIssue1126_D4_LateDeleteDoesNotEvictTheRecreatedEntry(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name, key = "krateo-system", "page-agents", "L1_page_agents"
	const oldBody = `{"v":"old"}`
	const newBody = `{"v":"new"}`

	rw, dyn := realWatcher(t, gvr)
	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)
	marked := make(chan string, 64)
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) { marked <- k })

	// The object EXISTS on the cluster (it was deleted and recreated under the
	// same name a moment ago — the recreate has already landed in the indexer)
	// and a cold dispatch has already Put the CORRECT fresh entry.
	createObj(t, rw, dyn, gvr, ns, name, newBody)
	store.Put(key, &ResolvedEntry{RawJSON: []byte(oldBody), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)
	store.Put(key, &ResolvedEntry{RawJSON: []byte(newBody), Inputs: widgetInputs(gvr, ns, name)})

	// The DELETE from the old incarnation arrives LATE — and duplicated,
	// interleaved with a same-coordinate UPDATE and ADD, shuffled.
	h := rw.depEventHandlers(gvr)
	obj := unstructuredObj(gvr, ns, name)
	h.DeleteFunc(obj)
	h.UpdateFunc(obj, obj)
	h.DeleteFunc(obj)
	h.AddFunc(obj)
	h.DeleteFunc(obj)
	waitQueueIdle(t, 5*time.Second)

	if got := served(t, store, key); got != newBody {
		t.Fatalf("1.12.6 D4 RED: a late/duplicate DELETE evicted the CORRECT recreated entry "+
			"(served %q, want %q). The action must derive from the object's current state "+
			"(it EXISTS), not from the event type.", got, newBody)
	}
	// The existing object's events are dirty-marks (stale-while-revalidate),
	// deduplicated by the typed workqueue — at least one, never an eviction.
	select {
	case k := <-marked:
		if k != key {
			t.Fatalf("D4: dirty-marked %q, want %q", k, key)
		}
	default:
		t.Fatalf("D4: no dirty-mark for the existing object's events — the entry was neither refreshed nor evicted")
	}
	if got := d.Stats().EvictDeleteTotal; got != 0 {
		t.Fatalf("D4: evict_delete_total=%d, want 0 (the object exists; nothing may be evicted)", got)
	}
}

// --- D4b — the same coordinate, but the object is GONE: it must still evict -

func TestIssue1126_D4b_StateAbsentStillEvictsAcrossDuplicates(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name, key = "krateo-system", "page-agents", "L1_page_agents"

	rw, dyn := realWatcher(t, gvr)
	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)

	createObj(t, rw, dyn, gvr, ns, name, "x")
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)
	deleteObj(t, rw, dyn, gvr, ns, name)

	h := rw.depEventHandlers(gvr)
	obj := unstructuredObj(gvr, ns, name)
	h.DeleteFunc(obj)
	h.DeleteFunc(obj) // duplicate — dedup'd or harmless, never wrong
	waitQueueIdle(t, 5*time.Second)

	if _, ok := store.Get(key); ok {
		t.Fatalf("D4b RED: the object is ABSENT from the informer but its self entry survived")
	}
	if got := d.Stats().EvictDeleteTotal; got != 1 {
		t.Fatalf("D4b: evict_delete_total=%d, want exactly 1 (duplicates must not double-count)", got)
	}
}

// --- E2 — the relist teardown window must not evict the GVR -----------------

func TestIssue1126_E2_TeardownWindowEvictsNothing(t *testing.T) {
	defer withCleanDepWatch(t)()
	c1FastBackoff(t)

	gvr := gvrFlexes()
	const ns = "krateo-system"
	rw, dyn := realWatcher(t, gvr)
	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)
	marked := make(chan string, 64)
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) { marked <- k })

	// Two live objects with resident self entries.
	for _, n := range []string{"alive-a", "alive-b"} {
		createObj(t, rw, dyn, gvr, ns, n, "x")
		store.Put("L1_"+n, &ResolvedEntry{RawJSON: []byte(`{"v":"` + n + `"}`), Inputs: widgetInputs(gvr, ns, n)})
		d.Record("L1_"+n, gvr, ns, n)
	}
	h := rw.depEventHandlers(gvr)

	// TEARDOWN WINDOW: the informer is removed (what a schema relist does
	// first). IsServable is now false for the GVR; the indexer is gone.
	rw.RemoveResourceType(gvr)
	if rw.IsServable(gvr) {
		t.Fatalf("precondition: GVR still servable after RemoveResourceType")
	}
	if got := rw.probeObjectState(gvr, ns, "alive-a"); got != objUnknown {
		t.Fatalf("E2: probe in the teardown window = %v, want UNKNOWN — a boolean probe here reads "+
			"every object of the GVR as absent and evicts the whole GVR", got)
	}

	// Events for LIVE objects land inside the window.
	h.UpdateFunc(unstructuredObj(gvr, ns, "alive-a"), unstructuredObj(gvr, ns, "alive-a"))
	h.DeleteFunc(unstructuredObj(gvr, ns, "alive-b")) // a stray/late DELETE for a live object
	time.Sleep(30 * time.Millisecond)                 // several requeue rounds at 1–2 ms backoff

	for _, n := range []string{"alive-a", "alive-b"} {
		if _, ok := store.Get("L1_" + n); !ok {
			t.Fatalf("E2 RED: live object %s was EVICTED during the relist teardown window — the "+
				"probe treated 'informer torn down' as 'object absent'", n)
		}
	}
	if got := DepWatchStatsSnapshot().ProbeUnknown; got == 0 {
		t.Fatalf("E2: probe_unknown_total=0 — the window was never observed; the arm is not driving it")
	}

	// The informer comes back (the relist's re-registration) and syncs with
	// the live objects. Whatever is still pending resolves against REAL state:
	// alive objects → dirty-mark, nothing evicted.
	_, syncCh := rw.EnsureResourceType(gvr)
	deadline := time.Now().Add(10 * time.Second)
	select {
	case <-syncCh:
	case <-time.After(time.Until(deadline)):
		t.Fatalf("re-registered informer did not sync")
	}
	waitIndexer(t, rw, gvr, ns, "alive-a", true)
	waitIndexer(t, rw, gvr, ns, "alive-b", true)
	waitQueueIdle(t, 10*time.Second)
	// Whether the coordinates were re-probed after sync (EXISTS → dirty-mark)
	// or degraded on budget exhaustion (→ dirty-mark), the outcome is the same
	// and it is NOT an eviction — and it is not NOTHING either: both live
	// objects' entries must have been handed to the refresher. An
	// implementation that simply dropped unknown coordinates would keep the
	// entries (passing the survival check) while never re-resolving them.
	for _, n := range []string{"alive-a", "alive-b"} {
		if _, ok := store.Get("L1_" + n); !ok {
			t.Fatalf("E2 RED: live object %s evicted after the informer came back", n)
		}
	}
	got := map[string]bool{}
	deadline = time.Now().Add(10 * time.Second)
	for len(got) < 2 && time.Now().Before(deadline) {
		select {
		case k := <-marked:
			got[k] = true
		case <-time.After(50 * time.Millisecond):
		}
	}
	for _, n := range []string{"alive-a", "alive-b"} {
		if !got["L1_"+n] {
			t.Fatalf("E2 RED: live object %s was never dirty-marked after the window — the unknown "+
				"coordinate was dropped instead of requeued/degraded (marked: %v)", n, got)
		}
	}
	if got := d.Stats().EvictDeleteTotal; got != 0 {
		t.Fatalf("E2: evict_delete_total=%d, want 0 across a teardown window with only live objects", got)
	}
}

// --- C2 — the objUnknown degradation chain, end to end ----------------------

func TestIssue1126_C2_UnknownDegradesToDirtyMarkAfterTheBudget(t *testing.T) {
	defer withCleanDepWatch(t)()
	c1FastBackoff(t)

	gvr := gvrFlexes()
	other := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "buttons"}
	const ns, name, key = "krateo-system", "orphan", "L1_orphan"

	// The watcher serves gvr but has NEVER registered `other`: every probe for
	// an `other` coordinate is objUnknown, for ever.
	rw, _ := realWatcher(t, gvr)
	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)
	marked := make(chan string, 8)
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) { marked <- k })

	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"resident"}`), Inputs: widgetInputs(other, ns, name)})
	d.Record(key, other, ns, name)

	h := rw.depEventHandlers(other)
	h.DeleteFunc(unstructuredObj(other, ns, name))

	// The chain: UNKNOWN ×(budget) → degrade → dirty-mark. The dirty-mark IS the
	// hand-off to the refresher, whose re-fetch reaches the apiserver (a not-
	// servable GVR falls through to a live GET in objects.Get): a definite 404
	// there evicts at the drop point (1.12.5, arm B3b); a 403/500 is bounded by
	// the TTL until item 4 row 3 lands. This arm pins the bridge's half.
	select {
	case k := <-marked:
		if k != key {
			t.Fatalf("C2: degraded dirty-mark hit %q, want %q", k, key)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("C2 RED: a permanently-UNKNOWN coordinate never degraded to a dirty-mark — it sits in " +
			"the queue (or was dropped) and the entry is kept with no apiserver decision ever taken")
	}
	waitQueueIdle(t, 5*time.Second)

	s := DepWatchStatsSnapshot()
	if s.ProbeUnknownDegraded != 1 {
		t.Fatalf("C2: probe_unknown_degraded_total=%d, want 1", s.ProbeUnknownDegraded)
	}
	if s.ProbeUnknown != uint64(maxRefreshRequeues)+1 {
		t.Fatalf("C2: probe_unknown_total=%d, want %d (one initial probe + %d requeues — the SAME budget the "+
			"refresher uses, so the subsystem has one number)", s.ProbeUnknown, maxRefreshRequeues+1, maxRefreshRequeues)
	}
	// Not an eviction: the bridge cannot know the object is gone, so it must not
	// pretend to. The refresher decides.
	if _, ok := store.Get(key); !ok {
		t.Fatalf("C2: the entry was EVICTED on an UNKNOWN probe — that is the boolean-probe failure E2 guards against")
	}
	if got := d.Stats().EvictDeleteTotal; got != 0 {
		t.Fatalf("C2: evict_delete_total=%d, want 0", got)
	}
}

// --- OFF — passthrough / nil watcher: UNKNOWN, no client call, no worker ----

func TestIssue1126_OffPath_PassthroughProbeIsUnknownWithoutAClientCall(t *testing.T) {
	defer withCleanDepWatch(t)()

	var nilRW *ResourceWatcher
	if got := nilRW.probeObjectState(gvrFlexes(), "ns", "x"); got != objUnknown {
		t.Fatalf("nil watcher probe = %v, want UNKNOWN", got)
	}

	// A passthrough watcher has a dynamic client but no informers. GetObject
	// in this mode does a LIVE apiserver GET; the probe must NOT — it returns
	// UNKNOWN before touching the client. A client that counts calls proves it.
	counting := &countingDynamic{Interface: dynamicfake.NewSimpleDynamicClient(k8sruntime.NewScheme())}
	rw := &ResourceWatcher{mode: modePassthrough, dyn: counting}
	if got := rw.probeObjectState(gvrFlexes(), "ns", "x"); got != objUnknown {
		t.Fatalf("passthrough probe = %v, want UNKNOWN", got)
	}
	if counting.calls != 0 {
		t.Fatalf("passthrough probe touched the dynamic client %d time(s); the probe must add no apiserver reads", counting.calls)
	}

	// Reading the bridge stats never starts the worker.
	_ = DepWatchStatsSnapshot()
	if depWatchSingleton().watcher.Load() != nil {
		t.Fatalf("a stats read bound a watcher / started the bridge")
	}
	// The expvar publisher is gated at init on Disabled() — the C0 structural
	// guard (#192) enforces that for every publisher; this arm only pins that
	// nothing in this package registers snowplow_deps eagerly under cache-off.
	t.Setenv("CACHE_ENABLED", "false")
	if !Disabled() {
		t.Fatalf("Disabled() false with CACHE_ENABLED=false")
	}
}

// countingDynamic counts Resource() calls — the only way anything in the
// package reaches the apiserver through a dynamic client.
type countingDynamic struct {
	dynamic.Interface
	calls int
}

func (c *countingDynamic) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	c.calls++
	return c.Interface.Resource(gvr)
}

var _ = context.Background // keep the import stable across edits
