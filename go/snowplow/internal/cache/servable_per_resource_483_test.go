// servable_per_resource_483_test.go — #483: servability is decided per
// RESOURCE, not per group/version.
//
// DEFECT (TRACED). RefreshDiscovery (servable.go) and its duplicate in
// ConfirmResourceTypes deduped the discovery CALL per group/version — correct,
// one round-trip per gv — but then cached the ANSWER per group/version and
// handed it to every sibling GVR:
//
//	s, known, reason := resourceTypeServed(disco, gvr)  // the FIRST gvr only
//	served[gv] = gvServedResult{...}
//	...
//	r := served[groupVersionString(gvr)]                // every SIBLING
//
// gvrs is iterated from a Go map, so WHICH sibling becomes the representative
// is random. A group/version where one resource is served and another is not
// therefore gets a single uniform verdict, wrong for at least one of them in
// whichever direction the iteration fell.
//
// MEASURED (057, 1.12.38, two /debug/vars scrapes 61s apart):
// informer_confirm_retracted_by_reason.discovery_refresh = 5,182 on a cluster
// with NO CRD churn, a counter that must be flat at 0 there. Customer-visible
// as apiserver_fallthrough_cells.call-generic for composition.krateo.io/v0-1-0
// Resource=eventsites = 19 (informer-fallthrough-not-servable): eventsites DOES
// serve v0-1-0 (9,605 dirty marks against that exact GVR) and was poisoned by
// meetupsites, which shares the group/version and serves only v0-3-0.
//
// FIX: the successful list a single discovery call returns already enumerates
// every resource in the group/version, so index it by name and answer each
// sibling from it (gvServedLookup.resultFor). Zero extra round-trips.
//
// WHY THE DETERMINISM ARGUMENT MATTERS — read before changing these arms. The
// defect's representative is RANDOM, so an arm asserting only one direction
// would be FLAKY on origin/main (red only on the iterations that elected the
// unlucky sibling). TestFalsifier483_* instead asserts EVERY sibling's own
// correct verdict in a single pass. Pre-fix all siblings share one verdict, so
// whichever one the map elected:
//
//	representative PRESENT ⇒ the absent sibling is wrongly CONFIRMED  → red
//	representative ABSENT  ⇒ the present siblings are wrongly RETRACTED → red
//
// Both branches are covered by the same assertions, so the arm is
// DETERMINISTICALLY red on origin/main regardless of map order. That is why the
// two directions belong in ONE test and must not be split apart.
//
// MEASURED on origin/main (79f68b42), both cohort shapes, 22 runs total:
//
//	3-present/1-absent  12/12 red, all 12 "absent sibling wrongly CONFIRMED"
//	1-present/3-absent  10/10 red,  8 that way, 2 "present resource wrongly
//	                               RETRACTED" — the 057 eventsites harm itself
//
// Go's map iteration order is unspecified but NOT guaranteed uniform, and the
// 12/12 above shows it can favour one representative run after run. So the
// harmful direction is only reliably reachable from a cohort shaped to make it
// the likely draw, which is why both shapes are kept.
//
// The error-path arms guard the constraint that is easiest to break while
// fixing the above: sharing is CORRECT for every NON-successful outcome.
// NotFound/Gone is the apiserver stating the whole group/version is gone and
// must retract ALL siblings (the missed-CRD-DELETE backstop — fail-open there
// is unbounded stale-serve); a transient error or a nil list is UNKNOWN and
// must retain ALL siblings (fail-closed there is mass retraction).

package cache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// The four RBAC GVRs are bootstrap-registered by NewResourceWatcher and ALL
// FOUR share one group/version — the exact sibling shape #483 mis-answers, and
// the same shape production hits with several CompositionDefinition versions
// under one group/version. Using them means the arms need no custom CRD fixture
// and no extra informer registration.
var (
	s483Roles               = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	s483RoleBindings        = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	s483ClusterRoles        = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	s483ClusterRoleBindings = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}

	// s483Siblings is the whole cohort, in the order the assertions report it.
	s483Siblings = []schema.GroupVersionResource{
		s483Roles, s483RoleBindings, s483ClusterRoles, s483ClusterRoleBindings,
	}
)

// perResourceDiscovery is a ResourceTypeDiscovery double whose successful list
// carries an EXPLICIT SET of resource names for the group/version, so one call
// can report some siblings present and others absent. The #217 double
// (retainDiscovery) cannot express that: its list is all-or-nothing.
type perResourceDiscovery struct {
	mu sync.RWMutex
	// present is the set of resource names the successful list contains.
	present map[string]bool
	// err, when non-nil, is returned INSTEAD of a list.
	err error
	// nilList returns (nil, nil) — the #217 UNKNOWN nil-list path.
	nilList bool
	// calls counts round-trips per group/version, so an arm can prove the
	// per-resource fix did not start issuing one call per sibling.
	calls map[string]int
}

func newPerResourceDiscovery(present ...schema.GroupVersionResource) *perResourceDiscovery {
	d := &perResourceDiscovery{present: map[string]bool{}, calls: map[string]int{}}
	for _, gvr := range present {
		d.present[gvr.Resource] = true
	}
	return d
}

func (d *perResourceDiscovery) setErr(e error) {
	d.mu.Lock()
	d.err, d.nilList = e, false
	d.mu.Unlock()
}

func (d *perResourceDiscovery) setNilList() {
	d.mu.Lock()
	d.err, d.nilList = nil, true
	d.mu.Unlock()
}

// setPresent replaces the successful list's name set and clears any error mode.
func (d *perResourceDiscovery) setPresent(present ...schema.GroupVersionResource) {
	d.mu.Lock()
	d.present = map[string]bool{}
	for _, gvr := range present {
		d.present[gvr.Resource] = true
	}
	d.err, d.nilList = nil, false
	d.mu.Unlock()
}

func (d *perResourceDiscovery) callsFor(gv string) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.calls[gv]
}

func (d *perResourceDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	d.mu.Lock()
	d.calls[groupVersion]++
	err, nilList := d.err, d.nilList
	present := make(map[string]bool, len(d.present))
	for k, v := range d.present {
		present[k] = v
	}
	d.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if nilList {
		return nil, nil
	}
	list := &metav1.APIResourceList{GroupVersion: groupVersion}
	for name := range present {
		list.APIResources = append(list.APIResources, metav1.APIResource{
			Name: name, Namespaced: true, Kind: "X", Verbs: metav1.Verbs{"list", "watch", "get"},
		})
	}
	return list, nil
}

// newS483Watcher builds a cache=on watcher over the fake dynamic client with
// the perResourceDiscovery double wired. The four RBAC informers are
// bootstrap-registered by the constructor, so nothing else needs registering.
func newS483Watcher(t *testing.T, disco cache.ResourceTypeDiscovery) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetInformerWatchStatsForTest()
	t.Cleanup(cache.ResetInformerWatchStatsForTest)

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(newTestScheme(), servableListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	rw.SetDiscoveryClient(disco)
	return rw
}

// s483Confirmed reads conjunct 4 per GVR out of the public servability
// snapshot. Conjunct 4 is what #483 corrupts; reading it directly (rather than
// IsServable) keeps the arms independent of HasSynced/watch health, so a slow
// fake informer cannot turn a #483 regression into a green.
func s483Confirmed(t *testing.T, rw *cache.ResourceWatcher, gvr schema.GroupVersionResource) bool {
	t.Helper()
	for _, row := range rw.ServableSnapshot() {
		if row.GVR == gvr.String() {
			return row.Confirmed
		}
	}
	t.Fatalf("GVR %v absent from ServableSnapshot — the arm cannot read conjunct 4 "+
		"for a GVR that is not registered; check the bootstrap registration", gvr)
	return false
}

// assertConfirmed checks every sibling's own expected conjunct-4 state in one
// pass and reports the FULL cohort on failure, so the output names which
// direction the defect fell in on this iteration.
func assertConfirmed(t *testing.T, rw *cache.ResourceWatcher, want map[schema.GroupVersionResource]bool, phase string) {
	t.Helper()
	bad := false
	for _, gvr := range s483Siblings {
		if got := s483Confirmed(t, rw, gvr); got != want[gvr] {
			bad = true
			t.Errorf("%s: %s confirmed=%v, want %v", phase, gvr.Resource, got, want[gvr])
		}
	}
	if bad {
		for _, gvr := range s483Siblings {
			t.Logf("%s: cohort state %s confirmed=%v (want %v)",
				phase, gvr.Resource, s483Confirmed(t, rw, gvr), want[gvr])
		}
		t.Fatalf("%s: #483 — every sibling above shares ONE group/version, so a uniform "+
			"verdict across them IS the defect: the successful discovery list names each "+
			"resource individually and must be read per resource", phase)
	}
}

// TestFalsifier483_SiblingResourceKeepsItsOwnVerdict is the headline arm. One
// group/version, four registered resources, a SUCCESSFUL discovery list
// containing three of them. The three listed must stay CONFIRMED and only the
// unlisted one may retract.
//
// RED on origin/main in both map-order branches — see the determinism argument
// in this file's header.
func TestFalsifier483_SiblingResourceKeepsItsOwnVerdict(t *testing.T) {
	// rolebindings is the absent one; the other three are served. This is the
	// eventsites/meetupsites shape: same group/version, divergent servability.
	disco := newPerResourceDiscovery(s483Roles, s483ClusterRoles, s483ClusterRoleBindings)
	rw := newS483Watcher(t, disco)

	// Setup: a list containing ALL FOUR confirms the whole cohort, so the arm
	// starts from "every sibling confirmed" and what it measures afterwards is
	// the RETRACTION decision, not a failure to confirm in the first place.
	disco.setPresent(s483Siblings...)
	rw.RefreshDiscovery(context.Background())
	assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
		s483Roles: true, s483RoleBindings: true,
		s483ClusterRoles: true, s483ClusterRoleBindings: true,
	}, "setup (all four listed)")

	// The defect: rolebindings leaves the list, its three siblings do not.
	disco.setPresent(s483Roles, s483ClusterRoles, s483ClusterRoleBindings)
	retractedBefore := cache.InformerWatchStatsSnapshot().ConfirmRetractedTotal
	rw.RefreshDiscovery(context.Background())

	assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
		s483Roles: true, s483RoleBindings: false,
		s483ClusterRoles: true, s483ClusterRoleBindings: true,
	}, "one sibling absent from a SUCCESSFUL list")

	// Cross-discrimination: EXACTLY ONE retraction. A count of 4 would mean the
	// cohort retracted together (the defect, representative-absent branch); a
	// count of 0 would mean nothing retracted (the representative-present
	// branch) and would also catch a fix that made the absent case fail-open.
	snap := cache.InformerWatchStatsSnapshot()
	if got := snap.ConfirmRetractedTotal - retractedBefore; got != 1 {
		t.Fatalf("#483: exactly ONE retraction expected (only rolebindings left the list); "+
			"retracted_total moved by %d. >1 means siblings retracted together (the shared-verdict "+
			"defect); 0 means a genuine per-resource absence stopped retracting at all", got)
	}

	// The fix must not have bought per-resource accuracy with per-resource
	// round-trips: the whole point is that ONE call already carries every name.
	// Two RefreshDiscovery passes over a 4-GVR cohort sharing one gv ⇒ 2 calls.
	if got := disco.callsFor("rbac.authorization.k8s.io/v1"); got != 2 {
		t.Fatalf("#483 cost regression: 2 discovery round-trips expected (one per "+
			"RefreshDiscovery pass over a cohort sharing ONE group/version); got %d. The "+
			"per-resource answer must come from the SAME call's list, not a call per sibling", got)
	}
}

// TestFalsifier483_PresentResourceIsNotPoisonedByAbsentSiblings is the
// MEASURED customer shape, and it is the inverse direction of the arm above.
//
// On 057 the harm ran this way round: `eventsites` IS served and was RETRACTED
// because `meetupsites` — same group/version, serving only v0-3-0 — was the
// representative. So the cohort here is ONE present resource among THREE absent
// ones, and the assertion that matters is that the present one survives.
//
// This arm exists because the sibling-verdict arm above, run 12 times on
// origin/main, elected a PRESENT representative every time and therefore only
// ever demonstrated the milder direction (an absent resource wrongly
// confirmed). Go's map iteration order is unspecified, not guaranteed-uniform,
// so the harmful direction needs a cohort shape that makes it the LIKELY draw
// rather than a hoped-for one.
func TestFalsifier483_PresentResourceIsNotPoisonedByAbsentSiblings(t *testing.T) {
	disco := newPerResourceDiscovery(s483Siblings...)
	rw := newS483Watcher(t, disco)

	rw.RefreshDiscovery(context.Background())
	assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
		s483Roles: true, s483RoleBindings: true,
		s483ClusterRoles: true, s483ClusterRoleBindings: true,
	}, "setup (all four listed)")

	// Only `roles` survives discovery — the eventsites-among-meetupsites shape.
	disco.setPresent(s483Roles)
	pre := cache.InformerWatchStatsSnapshot()
	rw.RefreshDiscovery(context.Background())

	assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
		s483Roles: true, s483RoleBindings: false,
		s483ClusterRoles: false, s483ClusterRoleBindings: false,
	}, "one sibling present among three absent (the 057 eventsites shape)")

	// Exactly THREE retractions — the three genuinely absent resources. Four
	// would mean the served resource was poisoned by its siblings, which is
	// precisely the 19 informer-fallthrough-not-servable cells measured for
	// call-generic / eventsites.
	post := cache.InformerWatchStatsSnapshot()
	if got := post.ConfirmRetractedTotal - pre.ConfirmRetractedTotal; got != 3 {
		t.Fatalf("#483: exactly THREE retractions expected (the three absent resources); "+
			"retracted_total moved by %d. 4 means the SERVED resource was retracted too — "+
			"the measured customer defect", got)
	}
}

// TestFalsifier483_NotFoundRetractsEverySibling guards the constraint that is
// easiest to break while making the successful path per-resource: an
// AUTHORITATIVE-absent discovery error is the apiserver stating the WHOLE
// group/version is gone, so it must retract EVERY sibling.
//
// RefreshDiscovery is the reconciling backstop for a CRD-DELETE whose watch
// event was missed. Retracting only one resource here (or none) would leave the
// rest serving a removed group/version forever — unbounded stale-serve, the
// exact #217/#218/#190 degradation.
func TestFalsifier483_NotFoundRetractsEverySibling(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"NotFound404", apierrors.NewNotFound(schema.GroupResource{
			Group: s483Roles.Group, Resource: s483Roles.Resource}, "")},
		{"Gone410", apierrors.NewGone("the requested resource is no longer available")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disco := newPerResourceDiscovery(s483Siblings...)
			rw := newS483Watcher(t, disco)

			rw.RefreshDiscovery(context.Background())
			assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
				s483Roles: true, s483RoleBindings: true,
				s483ClusterRoles: true, s483ClusterRoleBindings: true,
			}, "setup (all four listed)")

			pre := cache.InformerWatchStatsSnapshot()
			disco.setErr(tc.err)
			rw.RefreshDiscovery(context.Background())

			// EVERY sibling retracts — sharing is CORRECT here.
			assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
				s483Roles: false, s483RoleBindings: false,
				s483ClusterRoles: false, s483ClusterRoleBindings: false,
			}, tc.name+" (authoritative-absent discovery error)")

			post := cache.InformerWatchStatsSnapshot()
			if got := post.ConfirmRetractedTotal - pre.ConfirmRetractedTotal; got != uint64(len(s483Siblings)) {
				t.Fatalf("#483/#217: an authoritative %s must retract all %d siblings of the "+
					"group/version; retracted_total moved by %d", tc.name, len(s483Siblings), got)
			}
			// Cross-discrimination: definite-absent is NOT retain-on-unknown.
			if post.ConfirmRetainedUnknownTotal != pre.ConfirmRetainedUnknownTotal {
				t.Fatalf("#217 cross-discrimination: %s moved retained_unknown (%d→%d) — a 404/410 "+
					"is definite-absent and must not be counted as retain-on-unknown",
					tc.name, pre.ConfirmRetainedUnknownTotal, post.ConfirmRetainedUnknownTotal)
			}
		})
	}
}

// TestFalsifier483_TransientErrorRetainsEverySibling is the opposite-direction
// guard: a TRANSIENT discovery error (and a nil list) says nothing about any
// resource, so EVERY sibling must be RETAINED. Making the error paths
// per-resource would turn a discovery blip into mass retraction — the #217
// regression this pairs with.
func TestFalsifier483_TransientErrorRetainsEverySibling(t *testing.T) {
	allConfirmed := map[schema.GroupVersionResource]bool{
		s483Roles: true, s483RoleBindings: true,
		s483ClusterRoles: true, s483ClusterRoleBindings: true,
	}

	for _, tc := range []struct {
		name  string
		drive func(d *perResourceDiscovery)
	}{
		{"TransientServerError", func(d *perResourceDiscovery) {
			d.setErr(errors.New("the server is currently unable to handle the request"))
		}},
		{"ServiceUnavailable", func(d *perResourceDiscovery) {
			d.setErr(apierrors.NewServiceUnavailable("try again"))
		}},
		{"NilListNoError", func(d *perResourceDiscovery) { d.setNilList() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disco := newPerResourceDiscovery(s483Siblings...)
			rw := newS483Watcher(t, disco)

			rw.RefreshDiscovery(context.Background())
			assertConfirmed(t, rw, allConfirmed, "setup (all four listed)")

			pre := cache.InformerWatchStatsSnapshot()
			tc.drive(disco)
			for i := 0; i < 3; i++ {
				rw.RefreshDiscovery(context.Background())
			}

			// EVERY sibling retained — sharing is CORRECT here too.
			assertConfirmed(t, rw, allConfirmed, tc.name+" (UNKNOWN discovery outcome)")

			post := cache.InformerWatchStatsSnapshot()
			if post.ConfirmRetractedTotal != pre.ConfirmRetractedTotal {
				t.Fatalf("#483/#217: %s must retract NOTHING — an UNKNOWN answer covers the whole "+
					"group/version and no sibling may be retracted on it; retracted_total moved %d→%d",
					tc.name, pre.ConfirmRetractedTotal, post.ConfirmRetractedTotal)
			}
			// The detector must actually read non-zero DURING the defect window,
			// or a green here would only prove the instrument is dead.
			if post.ConfirmRetainedUnknownTotal <= pre.ConfirmRetainedUnknownTotal {
				t.Fatalf("#217: retain-on-unknown detector stayed flat (%d) across 3 %s refreshes — "+
					"a counter that reads zero during the condition it names is not a detector",
					pre.ConfirmRetainedUnknownTotal, tc.name)
			}
		})
	}
}

// TestFalsifier483_WalkConfirmPathIsAlsoPerResource covers the SECOND copy of
// the defect. ConfirmResourceTypes carries its own per-gv dedup loop, is reached
// from the prewarm walk's registration path (PrewarmRegisterFromNavigation), and
// is handed exactly the multi-version cohorts (several CompositionDefinition
// versions under one group/version) that the shared verdict mis-answers. Fixing
// only RefreshDiscovery would leave every walk-registered GVR exposed.
func TestFalsifier483_WalkConfirmPathIsAlsoPerResource(t *testing.T) {
	disco := newPerResourceDiscovery(s483Siblings...)
	rw := newS483Watcher(t, disco)

	// Setup through the SAME path under test, so a failure cannot be blamed on
	// RefreshDiscovery having confirmed the cohort instead.
	rw.ConfirmResourceTypes(context.Background(), s483Siblings)
	assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
		s483Roles: true, s483RoleBindings: true,
		s483ClusterRoles: true, s483ClusterRoleBindings: true,
	}, "setup via ConfirmResourceTypes (all four listed)")

	disco.setPresent(s483Roles, s483ClusterRoles, s483ClusterRoleBindings)
	retractedBefore := cache.InformerWatchStatsSnapshot().ConfirmRetractedTotal
	rw.ConfirmResourceTypes(context.Background(), s483Siblings)

	assertConfirmed(t, rw, map[schema.GroupVersionResource]bool{
		s483Roles: true, s483RoleBindings: false,
		s483ClusterRoles: true, s483ClusterRoleBindings: true,
	}, "ConfirmResourceTypes, one sibling absent from a SUCCESSFUL list")

	snap := cache.InformerWatchStatsSnapshot()
	if got := snap.ConfirmRetractedTotal - retractedBefore; got != 1 {
		t.Fatalf("#483 (walk-confirm path): exactly ONE retraction expected; retracted_total "+
			"moved by %d", got)
	}
	// Same cost bound as RefreshDiscovery: one call per gv per pass, two passes.
	if got := disco.callsFor("rbac.authorization.k8s.io/v1"); got != 2 {
		t.Fatalf("#483 cost regression (walk-confirm path): 2 discovery round-trips expected "+
			"over a cohort sharing ONE group/version across two passes; got %d", got)
	}
}
