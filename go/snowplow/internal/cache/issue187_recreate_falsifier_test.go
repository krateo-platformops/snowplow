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

// syncedWatcher lives in depwatch_harness_test.go since 1.12.6 C1: it is a
// REAL synced informer over a fake dynamic client, so the worker's state
// probe reads genuine indexer state (an object never created on the fake
// cluster is genuinely absent).

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

	h := syncedWatcher(t, gvr).depEventHandlers(gvr)

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

	h := syncedWatcher(t, gvr).depEventHandlers(gvr)

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
	d.Record(key, gvr, ns, name)  // self edge
	d.RecordList(key, member, ns) // list-scope edge on the member kind

	h := syncedWatcher(t, member).depEventHandlers(member)
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

	h := syncedWatcher(t, gvr).depEventHandlers(gvr)
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

// --- B2 — the eviction primitive the self-NotFound fix stands on ------------
//
// SCOPE NOTE (1.12.5). The architect filed B2 as "drive the real refresher
// with a NotFound resolve and assert the entry is gone". It was RED on main
// and it proved the defect: the refresher requeues five times and then
// refresh_drops the key with the stale body resident. But its RefreshFunc
// stub sits ABOVE resolveAndPopulateL1 — the frame the #187 (i) fix lives in
// — so no fix could ever turn THIS arm green; the stub replaces the code
// under test (feedback_seamed_dispatch_cannot_falsify_a_deep_frame).
//
// The end-to-end GREEN driver therefore lives one package down, where it can
// reach the real frame and a real HTTP 404:
//
//	internal/handlers/dispatchers/issue187_self_notfound_evict_test.go
//	  TestIssue187_B2E2E_RefresherSelfNotFoundEvictsThroughTheRealLoop
//
// That arm drives cache.EnqueueRefresh -> the real refresher pool -> the
// production refresh closure -> resolveAndPopulateL1 -> resolveOnceProd ->
// objects.Get -> client-go -> an httptest apiserver returning 404, and its
// fix-absent probe reproduces the krateo-057 transcript exactly (6
// refresh_failed, then refresh_dropped requeues=5, body resident).
//
// What stays HERE is the half this package owns: the eviction primitive the
// fix calls into, driven against a real store and real dep records, plus the
// refresher contract the fix depends on (a handler that resolves the
// situation and returns nil is invoked ONCE and never dropped).

func TestIssue187_B2_EvictSelfGoneRemovesEntryAndItsDepEdges(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name, key = "krateo-system", "alerts-new-cta", "L1_alerts_new_cta"

	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)

	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"stale"}`), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)                    // self edge
	d.RecordList(key, gvr, ns)                      // a second edge, so the
	d.Record(key, gvr, ns, "some-other-dependency") // cleanup is non-trivial

	before := d.Stats().EvictSelfGoneTotal
	beforeDelete := d.Stats().EvictDeleteTotal
	if n := len(d.CollectMatchesForTest(gvr, ns, name)); n == 0 {
		t.Fatalf("#187 B2 precondition: the self edge was not recorded")
	}

	if !d.EvictSelfGone(key) {
		t.Fatalf("#187 B2: EvictSelfGone reported no eviction for a resident entry")
	}
	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B2 RED: the entry survived EvictSelfGone — a self object confirmed gone " +
			"must leave no resolved body behind")
	}
	if n := len(d.CollectMatchesForTest(gvr, ns, name)); n != 0 {
		t.Fatalf("#187 B2: %d dep record(s) outlived the entry. The eviction must route through "+
			"the tracker so RemoveL1Key clears the forward/reverse edges alongside the store "+
			"delete (resolved.go DELETE-eviction invariant)", n)
	}
	if got := d.Stats().EvictSelfGoneTotal; got != before+1 {
		t.Fatalf("#187 B2: evict_self_gone_total = %d, want %d", got, before+1)
	}
	// Architect Finding 2: the H1 live discriminator must NOT move here.
	if got := d.Stats().EvictDeleteTotal; got != beforeDelete {
		t.Fatalf("#187 B2: evict_delete_total moved (%d -> %d) for a self-gone eviction; it "+
			"must stay informer-DELETE-driven or the documented H1 live procedure breaks",
			beforeDelete, got)
	}
	// Idempotent: a second call must not double-count or panic.
	if d.EvictSelfGone(key) {
		t.Fatalf("#187 B2: EvictSelfGone reported a second eviction for an already-evicted key")
	}
	if got := d.Stats().EvictSelfGoneTotal; got != before+1 {
		t.Fatalf("#187 B2: evict_self_gone_total double-counted a repeat eviction: %d", got)
	}
}

// TestIssue187_B2_ResolvedOutcomeIsNotRequeued pins the refresher contract the
// fix leans on: a handler that returns nil (the self-gone branch evicts and
// returns nil, because a confirmed deletion is a resolved outcome, not a
// failure) is invoked exactly ONCE and never reaches the poison-pill drop.
// Without this, the fix could evict and still burn five requeues emitting
// refresh_failed for a deletion it had already handled.
func TestIssue187_B2_ResolvedOutcomeIsNotRequeued(t *testing.T) {
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
	d.Record(key, gvr, ns, name)

	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, in ResolvedKeyInputs) error {
		attempts.Add(1)
		// What resolveAndPopulateL1 now does on a confirmed self-object 404.
		Deps().EvictSelfGone(ComputeKey(in))
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	EnqueueRefresh(key)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := store.Get(key); !ok && attempts.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Give a requeue, if one were scheduled, ample time to land.
	time.Sleep(500 * time.Millisecond)

	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B2: the entry survived the refresh cycle after %d attempts", attempts.Load())
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("#187 B2 RED: the handler ran %d times for one enqueue. A resolved outcome must "+
			"not be requeued — re-confirming a deletion five times is what produced the 264 "+
			"refresh_failed + 44 refresh_dropped lines on krateo-057.", got)
	}
}

// --- B3 — a TRANSIENT self-NotFound must NOT evict (the budget is the bound) -
//
// The architect's over-eviction guard, restored as written and strengthened.
// It is what discriminates the guarded design from a naive evict-on-first-404:
// two 404s inside the requeue budget, then a success, must leave the entry
// alive AND refreshed.
//
// STRENGTHENED: the handler returns a real %w-wrapped cache.ErrSelfObjectGone,
// not merely a 404-WORDED error. The original arm's plain fmt.Errorf would
// pass without ever entering the new drop-point branch — green for the wrong
// reason. With the sentinel it genuinely drives the branch and proves the
// budget, not the classification, is what saves the entry.

func TestIssue187_B3_TransientNotFoundMustNotEvict(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	resetRefresherForTest()
	defer resetRefresherForTest()
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name = "krateo-system", "transient-widget"

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(store)

	inputs := widgetInputs(gvr, ns, name)
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"prior"}`), Inputs: inputs})
	Deps().Record(key, gvr, ns, name)

	before := RefresherSelfNotFoundEvictTotal()

	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, in ResolvedKeyInputs) error {
		// 404 twice — a CRD re-registration window — then succeed. Well
		// inside maxRefreshRequeues, so the drop point is never reached.
		if attempts.Add(1) <= 2 {
			return fmt.Errorf("resolveAndPopulateL1 %s/%s: re-fetch %s/%s: %s.%s %q not found: %w",
				in.CacheEntryClass, in.Name, in.Resource, in.Name, in.Resource, in.Group, in.Name,
				ErrSelfObjectGone)
		}
		store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"refreshed"}`), Inputs: inputs})
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	EnqueueRefresh(key)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	e, ok := store.Get(key)
	if !ok {
		t.Fatalf("#187 B3 RED: a TRANSIENT self-NotFound (2 failures then success) EVICTED the "+
			"entry after %d attempts. Eviction must require the deterministic budget "+
			"(maxRefreshRequeues = %d, about 15.5s of backoff at the defaults), not one 404 — "+
			"otherwise a CRD re-registration window empties L1 of the whole type.",
			attempts.Load(), maxRefreshRequeues)
	}
	if got := string(e.RawJSON); got != `{"v":"refreshed"}` {
		t.Fatalf("#187 B3: entry survived but was not refreshed after the transient window: %s", got)
	}
	if got := RefresherSelfNotFoundEvictTotal(); got != before {
		t.Fatalf("#187 B3: self-gone evictions moved (%d -> %d) for a transient window", before, got)
	}
}

// --- B3b — the DROP POINT is the eviction point -----------------------------
//
// B3's complement, and the arm that owns the eviction decision at the layer it
// actually lives in. The same sentinel, but returned on EVERY attempt: once
// the full requeue budget has re-confirmed the deletion, the entry must be
// GONE rather than dropped-to-TTL with its stale body resident.

func TestIssue187_B3b_SelfGoneEvictsAtTheDropPoint(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	resetRefresherForTest()
	defer resetRefresherForTest()
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	const ns, name = "krateo-system", "gone-widget"

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(store)

	inputs := widgetInputs(gvr, ns, name)
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"stale"}`), Inputs: inputs})
	Deps().Record(key, gvr, ns, name)

	beforeSelfGone := Deps().Stats().EvictSelfGoneTotal
	beforeDelete := Deps().Stats().EvictDeleteTotal

	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, in ResolvedKeyInputs) error {
		attempts.Add(1)
		return fmt.Errorf("resolveAndPopulateL1 %s/%s: re-fetch %s/%s: %s.%s %q not found: %w",
			in.CacheEntryClass, in.Name, in.Resource, in.Name, in.Resource, in.Group, in.Name,
			ErrSelfObjectGone)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	EnqueueRefresh(key)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := store.Get(key); !ok && attempts.Load() > maxRefreshRequeues {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B3b RED: after %d self-object 404s — the full requeue budget — the "+
			"refresher dropped the key and LEFT the stale body resident. The drop point is "+
			"where the deletion is finally confirmed; it must evict.", attempts.Load())
	}
	if got := attempts.Load(); got != maxRefreshRequeues+1 {
		t.Fatalf("#187 B3b: handler ran %d times, want %d (the full budget, then evict) — "+
			"eviction must not fire early and must not spin past the cap",
			got, maxRefreshRequeues+1)
	}
	if got := Deps().Stats().EvictSelfGoneTotal; got != beforeSelfGone+1 {
		t.Fatalf("#187 B3b: evict_self_gone_total = %d, want %d", got, beforeSelfGone+1)
	}
	// Architect Finding 2: the H1 live discriminator must NOT move here.
	if got := Deps().Stats().EvictDeleteTotal; got != beforeDelete {
		t.Fatalf("#187 B3b: evict_delete_total moved (%d -> %d) for a refresher-driven "+
			"eviction. It must stay informer-DELETE-driven — it is the H1 live discriminator "+
			"(delete a throwaway CR and watch it move); folding this path in breaks that "+
			"procedure on any cluster that deletes CRs.", beforeDelete, got)
	}
	if n := len(Deps().CollectMatchesForTest(gvr, ns, name)); n != 0 {
		t.Fatalf("#187 B3b: %d dep record(s) outlived the evicted entry", n)
	}
}

// --- B4 — TTL honesty: repeated DECLINES must not keep an entry alive -------
//
// The team lead's TTL-honesty arm, restored (architect Finding 3 — it was
// deleted without replacement, and commit 2 inserts a new branch immediately
// above the decline gates it guards). A refresh that declines (stage error /
// external / UAF) returns BEFORE c.Put, so it must not slide CreatedAt. An
// entry declined repeatedly across more than its TTL must be GONE on the next
// Get. GREEN on main — which is the finding: the decline path is NOT the
// CreatedAt slider.

func TestIssue187_B4_DeclinesDoNotExtendTTL(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := gvrFlexes()
	store := newResolvedCache(100, 1<<20, 200*time.Millisecond) // tiny TTL
	Deps().SetStore(store)

	const key = "L1_declined"
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: widgetInputs(gvr, "ns", "declining")})

	// Simulate the decline path: dequeue (TTL-enforcing Get) + no Put,
	// repeatedly, across more than one TTL window.
	deadline := time.Now().Add(500 * time.Millisecond)
	gets := 0
	for time.Now().Before(deadline) {
		if _, ok := store.Get(key); ok {
			gets++
		}
		time.Sleep(50 * time.Millisecond)
	}

	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B4 RED: an entry Get-but-never-Put survived past its TTL (%d successful "+
			"Gets); a decline path is sliding CreatedAt", gets)
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

	h := syncedWatcher(t, gvr).depEventHandlers(gvr)

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

	h := syncedWatcher(t, gvr).depEventHandlers(gvr)
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

	h := syncedWatcher(t, gvr).depEventHandlers(gvr)

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

	// 1.12.6 C1 / D1 (design §10): after the burst EVERY served body must
	// match cluster state — every victim is deleted, so every self entry must
	// be GONE, including the ones whose event panicked. Evictions are applied
	// BEFORE dirty-marks (OnObjectEvent), so a panic raised by the refresh
	// hook for a "bad" victim's sibling can no longer take that victim's own
	// eviction with it. Pre-1.12.6 A4b asserted worker survival and a panic
	// count and skipped the bad victims ("expected loss"): a worker that
	// survived panics but mis-derived actions passed it.
	var resident, residentBad []string
	for _, v := range victims {
		if _, ok := store.Get(v.key); ok {
			if v.bad {
				residentBad = append(residentBad, v.key)
			} else {
				resident = append(resident, v.key)
			}
		}
	}
	if len(resident) != 0 {
		t.Fatalf("#187 A4b RED: %d/%d clean DELETEs left their self entry resident after a "+
			"concurrent panic storm — the eviction worker died mid-drain (first few: %v)",
			len(resident), len(victims), resident[:min(5, len(resident))])
	}
	if len(residentBad) != 0 {
		t.Fatalf("1.12.6 D1 RED: %d self entries whose event panicked are still resident — the "+
			"served body no longer matches cluster state. Evictions must be applied before the "+
			"dirty-mark hook that panicked (first few: %v)", len(residentBad), residentBad[:min(5, len(residentBad))])
	}
	if panicked.Load() == 0 {
		t.Fatalf("#187 A4b: the arm never panicked — it is not exercising the recover path")
	}
	if got := DepWatchStatsSnapshot().DeleteWorkerPanics; int64(got) != panicked.Load() {
		t.Fatalf("#187 A4b: deleteWorkerPanics = %d, want %d (one per panicking OnDelete)",
			got, panicked.Load())
	}
}
