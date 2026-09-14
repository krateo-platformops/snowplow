// issue187_recreate_falsifier_test.go — #187 hermetic falsifier.
//
// Real DepTracker + real ResolvedCacheStore + the REAL informer bridge
// handler closures (rw.depEventHandlers). A CR is deleted and recreated
// under the SAME (gvr, ns, name) with DIFFERENT content; the assertion is
// on the SERVED BODY (what a subsequent dispatch would get out of L1),
// never on a counter.
//
// Arms:
//   A1  plain DELETE then ADD (recreate) — served body must not be the
//       pre-delete body.
//   A2  DELETE and ADD delivered back-to-back with the eviction worker
//       STALLED, then released (the real production interleaving: ADD is
//       inline on the processor goroutine, DELETE is async on the worker).
//   A3  the June class — a deleted LIST member must dirty-mark the list
//       widget's key.
//   A4  worker-liveness: a panic inside ONE OnDelete must not silence
//       every subsequent DELETE in the process.

package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func gvrFlexes() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "widgets.templates.krateo.io",
		Version:  "v1beta1",
		Resource: "flexes",
	}
}

func widgetInputs(gvr schema.GroupVersionResource, ns, name string) *ResolvedKeyInputs {
	return &ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           gvr.Group,
		Version:         gvr.Version,
		Resource:        gvr.Resource,
		Namespace:       ns,
		Name:            name,
	}
}

// syncedWatcher returns a bare watcher whose syncCh for gvr is CLOSED
// (informer post-sync) so ADDs propagate exactly as in production.
func syncedWatcher(gvr schema.GroupVersionResource) *ResourceWatcher {
	rw := &ResourceWatcher{syncCh: map[schema.GroupVersionResource]chan struct{}{}}
	ch := make(chan struct{})
	close(ch)
	rw.syncCh[gvr] = ch
	return rw
}

// refresherStub stands in for the real refresher: on a dirty-mark it
// re-resolves the key from the live "cluster" map and re-Puts, exactly
// like resolveAndPopulateL1 (including the post-resolve liveness re-check
// that refuses to resurrect an evicted entry).
type refresherStub struct {
	mu      sync.Mutex
	store   *ResolvedCacheStore
	cluster map[string]string // ns/name -> body ("" == absent)
	gvr     schema.GroupVersionResource
	ns      string
	name    string
	key     string
	fails   int
}

func (rs *refresherStub) onDirty(k string, _ schema.GroupVersionResource) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if k != rs.key {
		return
	}
	body := rs.cluster[rs.ns+"/"+rs.name]
	if body == "" {
		// Object absent at re-resolve time: the real path returns an error
		// from objects.Get and the entry is LEFT AS IS.
		rs.fails++
		return
	}
	if _, alive := rs.store.Get(rs.key); !alive {
		return // liveness re-check: never resurrect (resolve_populate.go:238)
	}
	rs.store.Put(rs.key, &ResolvedEntry{
		RawJSON: []byte(body),
		Inputs:  widgetInputs(rs.gvr, rs.ns, rs.name),
	})
	Deps().Record(rs.key, rs.gvr, rs.ns, rs.name)
}

func served(t *testing.T, store *ResolvedCacheStore, key string) string {
	t.Helper()
	e, ok := store.Get(key)
	if !ok || e == nil {
		return "" // MISS — a cold dispatch would resolve fresh; acceptable
	}
	return string(e.RawJSON)
}

// waitQuiet gives the async eviction worker time to drain.
func waitQuiet() { time.Sleep(300 * time.Millisecond) }

// --- A1 — plain DELETE then ADD (same name, different content) --------------

func TestIssue187_A1_DeleteThenRecreateDoesNotServePreDeleteBody(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name, key = "krateo-system", "page-agents", "L1_page_agents"
	const oldBody = `{"children":["agents-header","agents-list","agents-servers"]}`
	const newBody = `{"children":["agents-page-header","agents-stats","agents-main"]}`

	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)

	rs := &refresherStub{store: store, gvr: gvr, ns: ns, name: name, key: key,
		cluster: map[string]string{ns + "/" + name: oldBody}}
	d.SetRefreshHook(rs.onDirty)

	// Cold dispatch: resolve + Put + self dep edge (deps_extract.go:113).
	store.Put(key, &ResolvedEntry{RawJSON: []byte(oldBody), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)

	h := syncedWatcher(gvr).depEventHandlers(gvr)

	// DELETE, then recreate with DIFFERENT content.
	h.DeleteFunc(unstructuredObj(gvr, ns, name))
	rs.mu.Lock()
	rs.cluster[ns+"/"+name] = newBody
	rs.mu.Unlock()
	h.AddFunc(unstructuredObj(gvr, ns, name))
	waitQuiet()

	if got := served(t, store, key); got == oldBody {
		t.Fatalf("#187 A1 RED: L1 still serves the PRE-DELETE body after delete+recreate: %s", got)
	}
}

// --- A2 — the production interleaving: worker stalled across the ADD ---------

func TestIssue187_A2_LateDeleteDoesNotRestoreOrPinPreDeleteBody(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name, key = "krateo-system", "page-agents", "L1_page_agents"
	const oldBody = `{"v":"old"}`
	const newBody = `{"v":"new"}`

	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)
	rs := &refresherStub{store: store, gvr: gvr, ns: ns, name: name, key: key,
		cluster: map[string]string{ns + "/" + name: oldBody}}
	d.SetRefreshHook(rs.onDirty)

	store.Put(key, &ResolvedEntry{RawJSON: []byte(oldBody), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)

	h := syncedWatcher(gvr).depEventHandlers(gvr)

	// Deliver DELETE (async, may land late) and ADD (inline) back to back.
	h.DeleteFunc(unstructuredObj(gvr, ns, name))
	rs.mu.Lock()
	rs.cluster[ns+"/"+name] = newBody
	rs.mu.Unlock()
	h.AddFunc(unstructuredObj(gvr, ns, name))
	waitQuiet()

	if got := served(t, store, key); got == oldBody {
		t.Fatalf("#187 A2 RED: pre-delete body survived the delete/recreate interleaving: %s", got)
	}
}

// --- A3 — June class: a deleted LIST member must dirty-mark the list widget --

func TestIssue187_A3_DeletedListMemberDirtyMarksListWidget(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	member := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}
	const ns, name, key = "krateo-system", "list-widget", "L1_list_widget"

	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)

	marked := make(chan string, 8)
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) { marked <- k })

	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"items":["a","b"]}`), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)      // self edge
	d.RecordList(key, member, ns)     // list-scope edge on the member kind

	h := syncedWatcher(member).depEventHandlers(member)
	h.DeleteFunc(unstructuredObj(member, ns, "b"))
	waitQuiet()

	select {
	case k := <-marked:
		if k != key {
			t.Fatalf("#187 A3: dirty-marked %q, want %q", k, key)
		}
	default:
		t.Fatalf("#187 A3 RED: deleting list member %s/%s did not dirty-mark the list widget key", ns, "b")
	}
}

// --- B1 — PLAIN DELETE, no recreate: the self entry must be GONE ------------
//
// The team lead's primary arm. Drives the real DeleteFunc closure with real
// widgets-class Inputs (the same shape helpers.go:259-288 builds) and asserts
// the entry is removed from the store, not merely dirty-marked.

func TestIssue187_B1_PlainDeleteEvictsSelfEntry(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name, key = "krateo-system", "alerts-new-cta", "L1_alerts_new_cta"

	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)

	var marked []string
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) { marked = append(marked, k) })

	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"resident"}`), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name) // deps_extract.go:113 self edge

	h := syncedWatcher(gvr).depEventHandlers(gvr)
	h.DeleteFunc(unstructuredObj(gvr, ns, name))
	waitQuiet()

	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B1 RED: a plain DELETE (no recreate) left the SELF entry resident; "+
			"dirty-marked=%v — isSelfRepresentation classified it non-self", marked)
	}
	if len(marked) != 0 {
		t.Fatalf("#187 B1: self entry was DIRTY-MARKED (%v) instead of evicted", marked)
	}
}

// --- B2 — the refresher's own-object NotFound must not leave a stale entry ---
//
// Drives the REAL refresher loop (real workqueue, real requeue budget, real
// poison-pill drop at maxRefreshRequeues=5) with a resolve that fails NotFound
// on the entry's OWN object — the exact shape of the 264 WARN
// refresher.refresh_failed lines on krateo-057.

func TestIssue187_B2_RefresherNotFoundOnOwnObjectEvicts(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	resetRefresherForTest()
	defer resetRefresherForTest()
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name = "krateo-system", "alerts-new-cta"

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	d := Deps()
	d.SetStore(store)

	inputs := widgetInputs(gvr, ns, name)
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"stale"}`), Inputs: inputs})

	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, in ResolvedKeyInputs) error {
		attempts.Add(1)
		// The production error shape: resolveOnceProd's objects.Get NotFound
		// (resolve_populate.go:445-448).
		return fmt.Errorf("resolveAndPopulateL1 %s/%s: re-fetch %s/%s: %s.%s %q not found",
			in.CacheEntryClass, in.Name, in.Resource, in.Name, in.Resource, in.Group, in.Name)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	EnqueueRefresh(key)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if attempts.Load() > maxRefreshRequeues {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B2 RED: after %d NotFound re-resolves of the entry's OWN object the "+
			"refresher dropped the key (refresh_dropped) and LEFT the stale body resident. "+
			"A NotFound on the self object means the object is gone — it must EVICT.", attempts.Load())
	}
}

// --- A5 — resurrection: an in-flight cold dispatch re-Puts the PRE-DELETE
// body AFTER the DELETE eviction. The request path (widgets.go:511) has no
// post-resolve liveness re-check, unlike the refresher (resolve_populate.go:238).

func TestIssue187_A5_InFlightColdDispatchCannotResurrectPreDeleteBody(t *testing.T) {
	// OUT OF SCOPE for 1.12.5, and deliberately NOT "fixed" by a bare
	// Get-before-Put: the request path would still lose the race (the
	// eviction can land between the Get and the Put). Closing it wants a
	// generation counter folded into the entry, which is its own design
	// pass — the team lead files it as a separate issue. The arm stays in
	// the tree, RED-by-construction, so whoever takes that issue starts
	// from a driver rather than a description. Remove this Skip to run it.
	t.Skip("#187 A5 — request-path resurrection: out of scope for 1.12.5, needs a generation-counter design (tracked separately)")

	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name, key = "krateo-system", "page-agents", "L1_page_agents"
	const oldBody = `{"v":"old"}`
	const newBody = `{"v":"new"}`

	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)
	rs := &refresherStub{store: store, gvr: gvr, ns: ns, name: name, key: key,
		cluster: map[string]string{ns + "/" + name: oldBody}}
	d.SetRefreshHook(rs.onDirty)

	store.Put(key, &ResolvedEntry{RawJSON: []byte(oldBody), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)

	h := syncedWatcher(gvr).depEventHandlers(gvr)

	// A /call is IN FLIGHT: it already fetched the OLD CR and is resolving.
	// Meanwhile the CR is deleted and recreated with different content.
	h.DeleteFunc(unstructuredObj(gvr, ns, name))
	rs.mu.Lock()
	rs.cluster[ns+"/"+name] = newBody
	rs.mu.Unlock()
	h.AddFunc(unstructuredObj(gvr, ns, name))
	waitQuiet()

	// The in-flight dispatch now completes and Puts what IT resolved (old),
	// exactly as widgets.go:511 does — unconditionally.
	store.Put(key, &ResolvedEntry{RawJSON: []byte(oldBody), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)

	if got := served(t, store, key); got == oldBody {
		t.Fatalf("#187 A5 RED: an in-flight cold dispatch resurrected the PRE-DELETE body after " +
			"the DELETE eviction; nothing will invalidate it again (no further informer event) — " +
			"stale until the 1h TTL. The request-path Put has no liveness re-check.")
	}
}

// --- A4 — worker liveness: one panicking OnDelete must not kill every DELETE -

func TestIssue187_A4_DeleteWorkerSurvivesAPanic(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)

	// Wire a refresh hook that panics for the FIRST delete only. In
	// production the equivalent panic source is anything reachable from
	// OnDelete (store lookup, refresher enqueue, eviction batch).
	first := true
	d.SetRefreshHook(func(string, schema.GroupVersionResource) {
		if first {
			first = false
			panic("simulated fault inside OnDelete")
		}
	})

	// Victim 1 — non-self dep, so the panicking enqueue hook is reached.
	store.Put("L1_v1", &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: widgetInputs(gvr, "ns", "other")})
	d.Record("L1_v1", gvr, "ns", "victim1")
	// Victim 2 — a SELF representation that MUST be evicted afterwards.
	store.Put("L1_v2", &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: widgetInputs(gvr, "ns", "victim2")})
	d.Record("L1_v2", gvr, "ns", "victim2")

	h := syncedWatcher(gvr).depEventHandlers(gvr)
	h.DeleteFunc(unstructuredObj(gvr, "ns", "victim1")) // panics inside the worker
	waitQuiet()
	h.DeleteFunc(unstructuredObj(gvr, "ns", "victim2")) // must still be processed
	waitQuiet()

	if _, ok := store.Get("L1_v2"); ok {
		t.Fatalf("#187 A4 RED: after one panic inside OnDelete the eviction worker is dead — " +
			"every subsequent DELETE in the process is silently lost (deps_watch.go:96-127)")
	}
	// 1.12.5 — the lost eviction must be COUNTED and greppable, not silent.
	// Without this the arm would also pass on an implementation that
	// swallowed the panic without telling anybody.
	if got := DepWatchStatsSnapshot().DeleteWorkerPanics; got != 1 {
		t.Fatalf("#187 A4: deleteWorkerPanics = %d, want 1 — a lost DELETE eviction must be "+
			"counted (expvar snowplow_deps.delete_worker_panics_total) and WARN-logged", got)
	}
}

// --- A4b — CONCURRENT DELETE storm across the panicking worker ---------------
//
// A4 drives two events in strict sequence, which cannot observe a
// worker/submitter race. The 1.12.5 change moves a recover across a
// goroutine boundary and adds a re-spawn path, so it is a concurrency
// change and needs a concurrent arm under -race.
//
// K = 8 concurrent submitters × M = 12 DELETEs each. Every third event
// panics inside OnDelete. All non-panicking self entries must be evicted
// and the panic count must equal the number of panicking events: a worker
// that died mid-storm leaves resident entries behind.
func TestIssue187_A4b_DeleteWorkerSurvivesAConcurrentPanicStorm(t *testing.T) {
	defer withCleanDepWatch(t)()

	const (
		submitters   = 8
		perSubmitter = 12
	)

	gvr := gvrFlexes()
	store := newResolvedCache(4096, 1<<24, time.Hour)
	d := Deps()
	d.SetStore(store)

	// panics on every event whose name ends in a multiple of 3.
	shouldPanic := func(i int) bool { return i%3 == 0 }

	var panicked atomic.Int64

	type victim struct {
		key  string
		name string
		bad  bool
	}
	var victims []victim
	for s := 0; s < submitters; s++ {
		for i := 0; i < perSubmitter; i++ {
			name := fmt.Sprintf("victim-%d-%d", s, i)
			key := "L1_" + name
			v := victim{key: key, name: name, bad: shouldPanic(i)}
			victims = append(victims, v)
			store.Put(key, &ResolvedEntry{
				RawJSON: []byte(`{"v":"resident"}`),
				Inputs:  widgetInputs(gvr, "ns", name),
			})
			d.Record(key, gvr, "ns", name)
		}
	}

	// The panic source is the dirty-mark enqueue hook, the one seam
	// OnDelete calls out through. A "bad" object also carries a NON-self
	// dep edge so the hook is reached for it.
	for _, v := range victims {
		if v.bad {
			d.Record("L1_sibling_"+v.name, gvr, "ns", v.name)
		}
	}
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		if len(k) > 12 && k[:12] == "L1_sibling_v" {
			panicked.Add(1)
			panic("simulated fault inside OnDelete: " + k)
		}
	})

	h := syncedWatcher(gvr).depEventHandlers(gvr)

	var wg sync.WaitGroup
	for s := 0; s < submitters; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < perSubmitter; i++ {
				h.DeleteFunc(unstructuredObj(gvr, "ns", fmt.Sprintf("victim-%d-%d", s, i)))
			}
		}(s)
	}
	wg.Wait()

	// Drain: stopDeleteWorker (via withCleanDepWatch) drains on exit, but
	// assert before teardown, so wait for the queue to empty.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if DepWatchStatsSnapshot().DeleteQueueDepth == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitQuiet()

	var resident []string
	for _, v := range victims {
		if v.bad {
			continue // its OnDelete panicked before the eviction batch — expected loss
		}
		if _, ok := store.Get(v.key); ok {
			resident = append(resident, v.key)
		}
	}
	if len(resident) != 0 {
		t.Fatalf("#187 A4b RED: %d/%d clean DELETEs left their self entry resident after a "+
			"concurrent panic storm — the eviction worker died mid-drain (first few: %v)",
			len(resident), len(victims), resident[:min(5, len(resident))])
	}
	if panicked.Load() == 0 {
		t.Fatalf("#187 A4b: the arm never panicked — it is not exercising the recover path")
	}
	if got := DepWatchStatsSnapshot().DeleteWorkerPanics; int64(got) != panicked.Load() {
		t.Fatalf("#187 A4b: deleteWorkerPanics = %d, want %d (one per panicking OnDelete)",
			got, panicked.Load())
	}
}
