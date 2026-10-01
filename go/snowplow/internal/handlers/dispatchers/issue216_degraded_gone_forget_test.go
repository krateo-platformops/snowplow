// issue216_degraded_gone_forget_test.go — #216: a confirmed-404 EvictSelfGone on
// the DEGRADED probe path must carry the authoritative GONE verdict to the
// harvesters, exactly as the informer-DELETE (objAbsent) path does — or the
// harvested in-memory copy survives the L1 eviction and the next seed pass
// re-Puts the deleted object's content for the pod's life (stale-POSITIVE serve).
//
// WHY THIS IS SEPARATE FROM #279. #279 is the stale-NEGATIVE edge-recording gap;
// this is stale-POSITIVE (serves deleted content), a different site/mechanism:
// notifyObjectGone fires at OnObjectEvent's objAbsent branch (deps.go) but NOT
// from EvictSelfGone — so a DEGRADED probe (informer not-servable → objUnknown →
// objUnknownDegraded → dirty → refresher confirmed-404 → EvictSelfGone) evicts L1
// yet leaves the harvested copy behind.
//
// HOW REAL IS REAL. The whole degraded confirmed-404 boundary is production code:
// cache.EnqueueRefresh → the real refresher worker pool → the production
// refreshFunc → resolveAndPopulateL1 → resolveOnceProd → objects.Get → an
// httptest apiserver that discovery-answers then 404s the entry's OWN object →
// apierrors.IsNotFound → cache.ErrSelfObjectGone → the full requeue budget →
// deps.EvictSelfGone. Nothing about the eviction decision is hand-installed
// (feedback_falsifier_must_drive_real_boundary_not_install_crossed_state /
// feedback_seamed_dispatch_cannot_falsify_a_deep_frame). The harvester forget is
// asserted through the PRODUCTION registrar (registerHarvesterGoneForgetHook),
// never a test-wired probe, and the outcome is proven on the REAL seedOneWidget
// entry discriminator (the issue216_f6b idiom): after the eviction the seed must
// NOT re-enter the deleted coordinate, while it still reaches a LIVE one.
//
// RED on the base: delete the notifyObjectGone fire in deps.EvictSelfGone — the
// L1 entry is still evicted (that path predates this fix) but the harvested copy
// survives, so the seed re-enters the deleted coordinate and the replay loop is
// intact. (Reuses the #187 real-404 harness — newI187APIServer / i187RunRefreshCycle
// / i187RefresherEnv — and the F6b harvester+seed harness; fixCWidgetGVR == i187GVR,
// both widgets.templates.krateo.io/v1beta1/flexes, so one watcher serves both the
// RBAC/seed-enumeration and the type-exists conjunct.)

package dispatchers

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// i216HarvestFlex harvests one flexes widget copy into the nav harvester, exactly
// as the production harvest path does (not by writing into the map).
func i216HarvestFlex(nav *navWidgetHarvester, ns, name string) {
	w := &unstructured.Unstructured{}
	w.SetNamespace(ns)
	w.SetName(name)
	w.SetGroupVersionKind(schema.GroupVersionKind{
		Group: fixCWidgetGVR.Group, Version: fixCWidgetGVR.Version, Kind: i187Kind,
	})
	nav.harvestNavWidget(w, fixCWidgetGVR, -1, -1, -1, -1)
}

func TestIssue216_DegradedConfirmed404_ForgetsHarvestAndStopsSeedReplay(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	i187RefresherEnv(t)
	// The httptest apiserver 404s the DELETED coordinate's own object GET (the
	// i187 coordinate: flexes/krateo-system/alerts-new-cta).
	srv := newI187APIServer(t, http.StatusNotFound)

	// One watcher: RBAC (userGranted get flexes → the seed enumerates a cohort and
	// derives a non-empty BindingUID) + the flexes GVR registered (the #187
	// type-exists conjunct, so a 404 is a deletion not a CRD-absent transient) +
	// RESOLVED_CACHE_ENABLED + published global.
	buildFixCWatcher(t)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{fixCWidgetGVR})
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)

	store := cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	cache.Deps().SetStore(store)

	// The DELETED coordinate — a resident widgets-class L1 entry whose own object
	// the apiserver will 404. This is the entry the refresher evicts.
	del := cache.ResolvedKeyInputs{
		CacheEntryClass:        "widgets",
		Group:                  i187Group,
		Version:                i187Version,
		Resource:               i187Resource,
		Namespace:              i187NS,
		Name:                   i187Name,
		BindingUID:             "uid-216",
		RepresentativeUsername: "admin",
		RepresentativeGroups:   []string{"admins"},
	}
	delKey := cache.ComputeKey(del)
	store.Put(delKey, &cache.ResolvedEntry{
		RawJSON: []byte(`{"stale":"deleted-content"}`),
		Inputs:  &del,
	})
	cache.Deps().Record(context.Background(), delKey, i187GVR(), i187NS, i187Name) // the self dep edge

	// Harvest BOTH the deleted coordinate and a LIVE one (dashboard-flex, the CR
	// buildFixCWatcher seeds). The live one is the over-forget guard AND the proof
	// the seed pass is functional (so the deleted-coordinate 0 below is the forget,
	// not a broken seed).
	const liveName = "dashboard-flex"
	nav := newNavWidgetHarvester()
	i216HarvestFlex(nav, i187NS, i187Name)
	i216HarvestFlex(nav, i187NS, liveName)
	content := newContentPrewarmHarvester()
	// THE PRODUCTION REGISTRAR — not a closure this test wrote.
	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: nav, harvester: content})

	// Drive the REAL degraded confirmed-404 loop → EvictSelfGone → notifyObjectGone.
	saEP := &endpoints.Endpoint{ServerURL: srv.URL}
	saRC := &rest.Config{Host: srv.URL}
	invocations := i187RunRefreshCycle(t, del, delKey, saEP, saRC, store)

	// PRECONDITION — the real loop actually evicted via a confirmed apiserver 404.
	if _, ok := store.Get(delKey); ok {
		t.Fatalf("precondition: the confirmed-404 refresh loop did not evict the entry "+
			"(%d invocations, %d object GETs) — the arm is not at the EvictSelfGone boundary",
			invocations, srv.objectGets.Load())
	}
	if srv.objectGets.Load() == 0 {
		t.Fatalf("precondition: the apiserver never served the object GET — not driving the real re-fetch boundary")
	}

	// THE FIX (#216) — the harvested copy of the DELETED coordinate must be dropped.
	if f6bHarvestedStillHolds(nav, i187GVR(), i187NS, i187Name) {
		t.Fatalf("RED (#216): after a confirmed-404 EvictSelfGone for %s/%s the nav harvester STILL "+
			"holds the harvested copy. EvictSelfGone evicted L1 but never fired notifyObjectGone, so the "+
			"per-binding seed re-resolves that copy on every pass and re-Puts the deleted object into L1 "+
			"for the pod's life (stale-positive serve).", i187NS, i187Name)
	}
	// OVER-FORGET GUARD — the LIVE coordinate must survive the deleted one's verdict.
	if !f6bHarvestedStillHolds(nav, i187GVR(), i187NS, liveName) {
		t.Fatalf("#216 over-forget: the confirmed-404 gone verdict for %s dropped the LIVE widget %s too — "+
			"removal must be exact to the evicted coordinate", i187Name, liveName)
	}

	// AND a REAL seed pass must not re-enter seedOneWidget for the deleted coordinate,
	// while it STILL reaches the live one (the entry discriminator, issue216_f6b idiom).
	var buf f6bSyncBuf
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_ = seedScopeYielding(ctx, nil, nav.snapshot(), endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
	cancel()
	slog.SetDefault(prev)
	captured := buf.snapshot()

	if got := f6bSeedEntriesFor(captured, i187NS, i187Name); got != 0 {
		t.Fatalf("RED (#216): the seed pass re-entered seedOneWidget %d time(s) for the DELETED %s/%s "+
			"after a confirmed-404 eviction — the harvested copy was never forgotten, so the replay loop "+
			"is intact and the deleted object is written back into L1", got, i187NS, i187Name)
	}
	if got := f6bSeedEntriesFor(captured, i187NS, liveName); got == 0 {
		t.Fatalf("premise: the seed pass never reached the LIVE %s (0 seed entries) — the seed is not "+
			"functional in this fixture, so the deleted-coordinate 0 above proves nothing", liveName)
	}

	// IDEMPOTENCY — notifyObjectGone now has TWO fire sites (EvictSelfGone above +
	// OnObjectEvent's objAbsent branch). A double-forget must be a no-op: firing the
	// OTHER site (Deps().OnDelete == OnObjectEvent objAbsent) for the already-forgotten
	// coordinate must not panic and must not disturb the LIVE one.
	cache.Deps().OnDelete(i187GVR(), i187NS, i187Name)
	if f6bHarvestedStillHolds(nav, i187GVR(), i187NS, i187Name) {
		t.Fatalf("#216 idempotency: the deleted coordinate %s/%s reappeared after a second (objAbsent) forget", i187NS, i187Name)
	}
	if !f6bHarvestedStillHolds(nav, i187GVR(), i187NS, liveName) {
		t.Fatalf("#216 idempotency: a second forget of the deleted coordinate dropped the LIVE %s", liveName)
	}
}

// TestIssue216_DropPointNon404_DoesNotForgetHarvest is the NEGATIVE CONTROL for
// the 404-only gate (feedback_arm_that_cannot_fail_is_not_coverage): a
// deterministic NON-404 failure (500) evicts L1 at the C4 drop point behind the
// breaker, but must NEVER fire notifyObjectGone — the harvester KEEPS the copy.
// Over-forget on a transient outage is NOT safe (unlike #279's over-mark): it
// would empty the warm set for every entry that comes up for refresh during an
// apiserver blip. Without this arm the positive test's "confirmed-404 forgets"
// claim is untested and cannot fail.
func TestIssue216_DropPointNon404_DoesNotForgetHarvest(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	i187RefresherEnv(t)
	// Breaker ON (default): a deterministic non-404 across the budget evicts at the
	// C4 drop point — so this arm tests "evicted-but-not-forgotten", not "not evicted".
	t.Setenv("REFRESH_DROP_EVICT_MAX_PER_MINUTE", "64")
	srv := newI187APIServer(t, http.StatusInternalServerError) // 500 — NOT a confirmed deletion

	buildFixCWatcher(t)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{fixCWidgetGVR})
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)

	store := cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	cache.Deps().SetStore(store)

	in := cache.ResolvedKeyInputs{
		CacheEntryClass:        "widgets",
		Group:                  i187Group,
		Version:                i187Version,
		Resource:               i187Resource,
		Namespace:              i187NS,
		Name:                   i187Name,
		BindingUID:             "uid-216-neg",
		RepresentativeUsername: "admin",
		RepresentativeGroups:   []string{"admins"},
	}
	key := cache.ComputeKey(in)
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"stale":"outage-not-deletion"}`), Inputs: &in})
	cache.Deps().Record(context.Background(), key, i187GVR(), i187NS, i187Name)

	nav := newNavWidgetHarvester()
	i216HarvestFlex(nav, i187NS, i187Name)
	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: nav, harvester: newContentPrewarmHarvester()})

	beforeSelfGone := cache.Deps().Stats().EvictSelfGoneTotal
	saEP := &endpoints.Endpoint{ServerURL: srv.URL}
	saRC := &rest.Config{Host: srv.URL}
	invocations := i187RunRefreshCycle(t, in, key, saEP, saRC, store)

	// PRECONDITION — the 500 DID evict L1 at the drop point (arm is not vacuous).
	if _, ok := store.Get(key); ok {
		t.Fatalf("negative-control precondition: a 500 across the budget did NOT evict at the C4 drop "+
			"point (%d invocations, %d GETs) — cannot test whether a non-deletion eviction forgets",
			invocations, srv.objectGets.Load())
	}
	// It took the DROP-POINT route, not the confirmed-404 self-gone route.
	if got := cache.Deps().Stats().EvictSelfGoneTotal; got != beforeSelfGone {
		t.Fatalf("negative-control: a 500 moved evict_self_gone_total (%d -> %d) — a non-404 must never "+
			"take the confirmed-deletion route", beforeSelfGone, got)
	}
	// THE NEGATIVE CONTROL: a non-404 eviction must NOT forget the harvested copy.
	if !f6bHarvestedStillHolds(nav, i187GVR(), i187NS, i187Name) {
		t.Fatalf("RED (#216 over-forget): a 500 drop-point eviction FORGOT the harvested copy of %s/%s. "+
			"Only a CONFIRMED 404 may forget — a transient apiserver error would empty the warm set "+
			"during an outage.", i187NS, i187Name)
	}
}
