// apistage_dep_record_test.go — Ship 0.30.212 F-4 freshness falsifier
// for the content-keyed api-stage L1 dep-edge wiring.
//
// THE DEFECT (#72, traced 2026-05-29): apistage.go's MISS branch did
// `store.Put(contentKey, newEntry)` with NO matching `cache.Deps()
// .RecordList`/`.Record` call. Consequence: an informer ADD/UPDATE/
// DELETE on the underlying GVR/namespace never dirty-marks the
// content cell — the cache freezes at boot-time content until the
// 3600s TTL. Empirical proof: `compositions-get-ns-and-crd` returned
// only 13 pre-boot system namespaces after a post-boot bench-ns-*
// creation storm; `dep_record_total=1535` vs `dep_add_propagated=
// 69178` (only 5.6% find a dep target).
//
// THE FIX (Ship 0.30.212, Sites 1+2): the MISS-branch Put is followed
// by the symmetric dep-record (RecordList for name=="" / Record for
// name!=""); the HIT branch also re-records (idempotent — required to
// converge after rollout for entries Put under earlier binary or via
// cluster_list collapse). Mirrors the dispatcher L1 dep-record at
// resolve.go:546-562 and the widgetContent recordWidgetDeps at
// widget_content.go:267 (AC-G.5 pattern).
//
// FALSIFIER PROTOCOL: each test asserts BOTH the dep-tracker primary
// invariant (CollectMatchesForTest finds the contentKey under the
// expected DepKey bucket) AND the mechanism end-to-end (firing an
// informer event via Deps().OnAdd enqueues the contentKey through the
// refresh hook). Pre-fix the assertions FAIL on the MISS path because
// no dep edge exists; post-fix they PASS.

package api

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// depRecordCapturedHook returns a SetRefreshHook closure that captures
// every enqueued L1 key, plus a snapshot function. Concurrency-safe so
// the test can run under -race.
func depRecordCapturedHook() (hook func(string, schema.GroupVersionResource), snapshot func() []string) {
	var (
		mu      sync.Mutex
		entries []string
	)
	hook = func(k string, _ schema.GroupVersionResource) {
		mu.Lock()
		entries = append(entries, k)
		mu.Unlock()
	}
	snapshot = func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(entries))
		copy(out, entries)
		sort.Strings(out)
		return out
	}
	return hook, snapshot
}

// TestApistageContentServe_MissRecordsDepEdges drives apistageContentServe
// down its MISS branch (cold cache → dispatchViaInformer → store.Put) and
// asserts the new Ship 0.30.212 dep-record block at apistage.go:529's
// neighbourhood fired with the matching (gvr, ns, name) bucket. Both the
// PRIMARY invariant (CollectMatchesForTest) AND the MECHANISM
// (Deps().OnAdd → refresh-hook enqueue) are asserted.
//
// Pre-fix this test FAILS because the MISS branch Put has no dep-record
// companion → CollectMatchesForTest finds NO entry under the LIST bucket
// for the dispatched GVR/ns → the refresh hook is never called on
// subsequent OnAdd. Post-fix both invariants PASS.
func TestApistageContentServe_MissRecordsDepEdges(t *testing.T) {
	rw := newF1Watcher(t) // installs CACHE_ENABLED + RESOLVED_CACHE_ENABLED + apistage flag, seeds widgets, sets cache.SetGlobal.
	_ = rw

	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil under RESOLVED_CACHE_ENABLED=true")
	}

	// Wire the capture hook BEFORE the MISS resolve so the very-first
	// OnAdd this test fires is observed. ResetDepsForTest re-runs in
	// newF1Watcher's t.Cleanup so the hook does not leak across tests.
	hook, snapshot := depRecordCapturedHook()
	cache.Deps().SetRefreshHook(hook)

	// Drive a MISS through the F1 broad user (authorised for every
	// f1AllNamespaces). The cluster-wide widgets LIST is dispatched
	// un-gated, parsed, Put under contentKey, and (Ship 0.30.212) the
	// dep edge is recorded.
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser}),
	)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/" + f1WidgetsGVR.Group + "/" + f1WidgetsGVR.Version + "/" + f1WidgetsGVR.Resource,
			Verb: ptr.To(http.MethodGet),
		},
	}
	_, served, ok, _ := apistageContentServe(ctx, store, call, false)
	if !ok {
		t.Fatalf("apistageContentServe ok=false on the F1 watcher's broad-user widget LIST; "+
			"setup is broken (pivot-servable cluster-wide LIST should succeed): served=%v", served)
	}
	if !served {
		t.Fatalf("apistageContentServe served=false on the broad-user widget LIST; "+
			"broad user is authorised in every f1AllNamespaces — should have served")
	}

	// Compute the contentKey the Ship F1 apistage layer keys on. Path
	// is a cluster-wide LIST → ns="", name="". This is the EXACT input
	// shape contentKeyInputs (apistage.go:62) builds from.
	expectedKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))

	// PRIMARY ASSERTION — CollectMatchesForTest finds expectedKey under
	// the LIST bucket (gvr, ns="", any-name). Hitting it with a
	// previously-unseen synthetic name proves the LIST-scope edge is
	// present (the LIST bucket has wildcard name semantics — RecordList
	// stores under DepKey{gvr, ns, "*"}).
	matched := cache.Deps().CollectMatchesForTest(f1WidgetsGVR, "", "newobj-post-put")
	if _, present := matched[expectedKey]; !present {
		t.Fatalf("Ship 0.30.212 F-4 FAIL: apistageContentServe MISS did NOT record a "+
			"LIST dep edge for the content cell.\n"+
			"  contentKey  = %q\n"+
			"  GVR         = %s\n"+
			"  matched set = %v\n"+
			"Without this edge an informer ADD/UPDATE/DELETE on %s/* "+
			"never dirty-marks the apistage content cell — the cache freezes "+
			"at boot-time content until TTL=3600s (F-4 defect, #72). The fix "+
			"is the cache.Deps().RecordList(contentKey, gvr, ns) call mirroring "+
			"dispatcher resolve.go:546-562.",
			expectedKey, f1WidgetsGVR.String(), matched, f1WidgetsGVR.String())
	}

	// MECHANISM ASSERTION — an informer ADD event in any seeded namespace
	// (Deps().OnAdd) MUST propagate via the refresh hook with expectedKey.
	// This proves the end-to-end wire (record → OnAdd lookup → hook
	// dispatch), not just the dep-tracker insert.
	beforeAdd := snapshot()
	matchCount := cache.Deps().OnAdd(f1WidgetsGVR, "team-a", "new-widget-post-fix")
	if matchCount < 1 {
		t.Fatalf("Ship 0.30.212 F-4 MECHANISM FAIL: Deps().OnAdd returned matchCount=%d, "+
			"want >=1 — the LIST dep edge exists in the tracker but did not match "+
			"a new ADD event on the same (gvr, ns). Check listWildcard / collectMatches "+
			"semantics.", matchCount)
	}
	afterAdd := snapshot()
	if len(afterAdd) <= len(beforeAdd) {
		t.Fatalf("Ship 0.30.212 F-4 MECHANISM FAIL: refresh hook was NOT called after "+
			"OnAdd matched %d edge(s); enqueued-keys before=%v after=%v. "+
			"The dep tracker has the edge but the hook is not firing — wiring broken.",
			matchCount, beforeAdd, afterAdd)
	}
	// And the captured key must contain expectedKey somewhere in the
	// new entries.
	found := false
	prev := map[string]int{}
	for _, k := range beforeAdd {
		prev[k]++
	}
	for _, k := range afterAdd {
		if k == expectedKey {
			if prev[k] > 0 {
				prev[k]--
				continue
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Ship 0.30.212 F-4 MECHANISM FAIL: refresh hook fired but the captured "+
			"L1 keys do NOT include the apistage contentKey %q. captured=%v",
			expectedKey, afterAdd)
	}

	// NEGATIVE CONTROL — an unrelated GVR must NOT have a matching edge
	// to expectedKey. If this fires, the record is too-broad and would
	// dirty-mark the content cell on irrelevant events.
	unrelatedGVR := f1WidgetsGVR
	unrelatedGVR.Resource = "totally-unrelated-resource-xyz"
	if neg := cache.Deps().CollectMatchesForTest(unrelatedGVR, "", "anything"); neg != nil {
		if _, ok := neg[expectedKey]; ok {
			t.Fatalf("Ship 0.30.212 NEGATIVE CONTROL FAIL: the apistage contentKey %q "+
				"was recorded under an unrelated GVR %s — dep edge is over-broad",
				expectedKey, unrelatedGVR.String())
		}
	}
}

// TestApistageContentServe_HitReRecordsDepEdges proves the HIT-branch
// re-record (Ship 0.30.212 Site 2). Pre-populate the apistage store
// with a content entry directly (simulating either pre-fix-binary
// state OR the cluster_list collapse path's Put) and confirm that the
// first HIT through apistageContentServe re-records the dep edge —
// without this the cache stays frozen on entries that were stored
// under an older binary.
//
// Also asserts idempotency: 100 repeated HITs MUST NOT inflate
// RecordTotal (sync.Map.LoadOrStore semantics make repeated record
// calls no-ops after the first).
func TestApistageContentServe_HitReRecordsDepEdges(t *testing.T) {
	rw := newF1Watcher(t)

	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil under RESOLVED_CACHE_ENABLED=true")
	}

	// Drive a MISS first to populate the store with a real-shape entry.
	// This is the simplest way to obtain a byte-correct envelope on
	// disk — the Ship 0.30.212 dep-record block ALSO fires on the MISS,
	// which we then surgically reset so the test isolates the HIT-path
	// re-record.
	ctxBroad := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser}),
	)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/" + f1WidgetsGVR.Group + "/" + f1WidgetsGVR.Version + "/" + f1WidgetsGVR.Resource,
			Verb: ptr.To(http.MethodGet),
		},
	}
	_, served, ok, _ := apistageContentServe(ctxBroad, store, call, false)
	if !ok || !served {
		t.Fatalf("seed: apistageContentServe ok=%v served=%v — setup MISS failed", ok, served)
	}

	expectedKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))

	// Task #331 half (b) — ORDERED TEARDOWN before the mid-test reset
	// (#85 methodology). The seed MISS above drove a real LIST through the
	// live newF1Watcher informer; its processor goroutine
	// (depEventHandlers.func1 → Deps().OnAdd, deps_watch.go:208) may still
	// be draining queued ADD events. ResetDepsForTest() nils the package
	// `depsInstance` singleton (deps.go:933) with an UNGUARDED write —
	// racing that drain produces a DATA RACE (reproduced 1-of-3 attempts
	// under 32 CPU hogs + -race, /tmp/snowplow-331/race-repro.txt;
	// goroutine 87 reset vs goroutine 124 informer-bridge read). BOTH ends
	// are test lifecycle: the reset is a *_test.go-only helper production
	// never calls, and the reader is a watcher goroutine this test started.
	//
	// rw.Stop() (watcher.go:2118 Phase 2) BLOCKS on goroutineWG.Wait()
	// until every watcher-spawned goroutine has exited — its own doc
	// states "a subsequent ResetDepsForTest() ... cannot race an in-flight
	// depEventHandlers.func1 -> Deps() access." Stop() is idempotent (the
	// stopCh close is select/default-guarded), so newF1Watcher's
	// t.Cleanup(rw.Stop) remains safe. Stopping the watcher does NOT touch
	// the resolved store or the dep tracker, so the populated entry below
	// survives and the HIT path (a pure store.Get + re-record, no
	// dispatch) is unaffected.
	rw.Stop()

	// Surgically reset the dep tracker — but keep the populated store
	// entry. After this, the store has the content but the dep tracker
	// has NO edge for it (simulating pre-fix-binary state). The HIT
	// branch must re-record on the next call.
	cache.ResetDepsForTest()
	if m := cache.Deps().CollectMatchesForTest(f1WidgetsGVR, "", "x"); len(m) > 0 {
		t.Fatalf("setup invariant: after ResetDepsForTest, dep tracker should be empty; got %v", m)
	}
	statsBefore := cache.Deps().Stats()

	// Hot-path HIT — apistageContentServe finds the entry, runs the
	// per-user RBAC gate, and (Ship 0.30.212 Site 2) re-records the
	// dep edge so any LATER informer event dirty-marks the entry.
	_, served2, ok2, _ := apistageContentServe(ctxBroad, store, call, false)
	if !ok2 || !served2 {
		t.Fatalf("HIT: apistageContentServe ok=%v served=%v on populated entry", ok2, served2)
	}

	// PRIMARY — the re-record fired.
	matched := cache.Deps().CollectMatchesForTest(f1WidgetsGVR, "", "post-hit-newobj")
	if _, present := matched[expectedKey]; !present {
		t.Fatalf("Ship 0.30.212 Site 2 FAIL: HIT branch did NOT re-record the dep edge "+
			"for the content cell. Entries stored under an earlier binary OR via the "+
			"cluster_list collapse path will stay frozen on informer events.\n"+
			"  contentKey = %q\n  matched    = %v",
			expectedKey, matched)
	}

	statsAfter1 := cache.Deps().Stats()
	if statsAfter1.RecordTotal <= statsBefore.RecordTotal {
		t.Fatalf("Site 2: expected RecordTotal to grow on HIT re-record; before=%d after=%d",
			statsBefore.RecordTotal, statsAfter1.RecordTotal)
	}

	// IDEMPOTENCY — 100 repeated HITs MUST NOT inflate RecordTotal,
	// otherwise sync.Map.LoadOrStore semantics are broken and the
	// Deps.recordTotal counter would blow up under refresher load.
	statsBeforeLoop := cache.Deps().Stats()
	for i := 0; i < 100; i++ {
		_, _, _, _ = apistageContentServe(ctxBroad, store, call, false)
	}
	statsAfterLoop := cache.Deps().Stats()
	delta := statsAfterLoop.RecordTotal - statsBeforeLoop.RecordTotal
	if delta != 0 {
		t.Fatalf("Site 2 IDEMPOTENCY FAIL: 100 repeated HITs grew RecordTotal by %d "+
			"(want 0). sync.Map.LoadOrStore is supposed to make repeated record calls "+
			"no-ops after the first; if this grows under load the refresher will blow up "+
			"the dep-record cap.", delta)
	}
}
