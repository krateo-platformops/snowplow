// skip_unserved_group_119_test.go — #119 unserved-GROUP pre-check falsifier.
//
// #119 reclassified residual: an informer registered for an API GROUP the
// apiserver does NOT serve (a dead/typo'd apiGroup, or a group from an
// uninstalled operator) has a HasSynced that never flips → conjunct 2 never
// true → never servable → every dispatch falls through to a live apiserver
// 404, and the reflector's ListAndWatch churns 404 forever (boot latency +
// perpetual refresher-404 churn). The fix is an unserved-GROUP pre-check
// inside EnsureResourceType (the single spawn-site funnel): before spawning an
// informer, consult ServerGroups() and SKIP registration when the GROUP is
// entirely absent. Gated by SKIP_UNSERVED_GROUP_INFORMERS (default ON).
//
// GROUP granularity, NOT resource granularity (architect adjudication). A
// resource-level skip would regress the S4/stale-delete post-startup-CRD design
// (#50/#116), which DELIBERATELY registers a GVR whose RESOURCE type is not yet
// served and relies on the ~30s confirm-ticker to heal it. A post-startup CRD
// lands under a group that is ALREADY served, so the group-level check does NOT
// skip it → it registers + heals. Only a group entirely absent from
// ServerGroups() (a dead group) is skipped. The skip is NON-TERMINAL: a
// brand-new group's first CRD (group briefly absent at first-touch) is
// recovered by the next dispatch's EnsureResourceType re-touch.
//
// THE CRUX — fail-safe open on uncertainty. groupAuthoritativelyAbsent is
// THREE-STATE: it skips ONLY on a SUCCESSFUL ServerGroups() response that
// definitively lacks the group. A ServerGroups ERROR, no-ServerGroups-surface,
// a nil list, or the core group "" ALL register (never skip). A false-skip of a
// genuinely-served group would silently drop real data forever.
//
// Falsifier shape (feedback_falsifier_shape_must_discriminate): K>1 groups
// under ONE discovery double, so the arms discriminate PER-GROUP decisions, not
// a global flag; plus a brand-new-group RECOVERY arm that drives the real
// boundary ≥2× (feedback_falsifier_must_drive_real_boundary).

package cache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// skip119ServedGVR is a GVR under a SERVED group (secrets/core — always in the
// fake dynamic scheme). NOTE: the core group "" is never skipped, so we use a
// custom served GROUP for the discrimination arms below (skip119ServedCustom).
var skip119ServedGVR = servableTestGVR

// skip119ServedCustom is a GVR under a served CUSTOM group. Its group is
// reported present by the double's ServerGroups(); its List kind is registered
// so its informer LIST does not panic when it registers.
var skip119ServedCustom = schema.GroupVersionResource{
	Group: "served.krateo.io", Version: "v1", Resource: "servedthings",
}

// skip119UnservedGVR is a GVR under an UNSERVED (dead/typo'd) group. Its group
// is absent from the double's ServerGroups(). In the GREEN case it is skipped
// (never registers); its List kind is registered for the RED (flag-off) path.
var skip119UnservedGVR = schema.GroupVersionResource{
	Group: "dead.krateo.io", Version: "v1", Resource: "widgets",
}

// skip119PendingResourceGVR is a GVR whose GROUP is served but whose RESOURCE
// is not yet in the served set — the S4 post-startup-CRD shape. It MUST
// register (group-level check does not skip it) so the confirm-ticker can heal.
var skip119PendingResourceGVR = schema.GroupVersionResource{
	Group: "served.krateo.io", Version: "v1", Resource: "pendingcrd",
}

func skip119ListKinds() map[schema.GroupVersionResource]string {
	m := servableListKinds()
	m[skip119ServedCustom] = "ServedThingList"
	m[skip119UnservedGVR] = "WidgetList"
	m[skip119PendingResourceGVR] = "PendingCRDList"
	return m
}

// skip119Disco is the discovery double. It satisfies cache.ResourceTypeDiscovery
// (ServerResourcesForGroupVersion) AND the pre-check's serverGroupsLister
// (ServerGroups). Two INDEPENDENT axes, which is the whole point of the
// group-level boundary:
//   - servedGroups (group name -> served) drives the #119 GROUP-level pre-check
//     (ServerGroups). groupsErr makes ServerGroups return an error (fail-safe-
//     open guard).
//   - servedResources (gv -> set of resource names) drives the RESOURCE-level
//     confirm path (ServerResourcesForGroupVersion) that flips conjunct-4
//     typeConfirmed and thus IsServable. A resource can be absent here while its
//     GROUP is present in servedGroups — the post-startup-CRD shape.
type skip119Disco struct {
	mu              sync.Mutex
	servedGroups    map[string]bool            // group name -> served (drives ServerGroups)
	servedResources map[string]map[string]bool // gv -> resourceName -> served (drives confirm)
	groupsErr       error                      // when non-nil, ServerGroups returns it
	groupsCalls     int
	resourcesCalls  int
}

func (d *skip119Disco) ServerGroups() (*metav1.APIGroupList, error) {
	d.mu.Lock()
	d.groupsCalls++
	err := d.groupsErr
	names := make([]string, 0, len(d.servedGroups))
	for name, served := range d.servedGroups {
		if served {
			names = append(names, name)
		}
	}
	d.mu.Unlock()

	if err != nil {
		return nil, err
	}
	list := &metav1.APIGroupList{}
	for _, name := range names {
		list.Groups = append(list.Groups, metav1.APIGroup{Name: name})
	}
	return list, nil
}

func (d *skip119Disco) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	// COPY the inner map under the lock. Releasing d.mu while still holding a
	// reference to d.servedResources[gv] let the range below run concurrently
	// with setServed's write INTO that same inner map: the lock guarded the
	// outer-map lookup, but the inner map escaped it. The confirm path calls
	// this from primeConfirmAsyncLocked's goroutine, so a test that flips a
	// resource served while a confirm is in flight raced — which is exactly
	// what this test does. -race caught it once the package's scheduling
	// shifted; the bug was always here.
	d.mu.Lock()
	d.resourcesCalls++
	res := make(map[string]bool, len(d.servedResources[gv]))
	for name, served := range d.servedResources[gv] {
		res[name] = served
	}
	d.mu.Unlock()

	// The confirm path (resourceTypeServed) reads this: a resource name present
	// here flips conjunct-4 typeConfirmed → servable. Absent (or empty) → the
	// GVR stays unconfirmed → not servable (the pre-heal post-startup-CRD state).
	list := &metav1.APIResourceList{GroupVersion: gv}
	for name, served := range res {
		if served {
			list.APIResources = append(list.APIResources, metav1.APIResource{
				Name: name, Namespaced: true, Kind: "X",
			})
		}
	}
	return list, nil
}

func (d *skip119Disco) setGroupServed(name string, served bool) {
	d.mu.Lock()
	if d.servedGroups == nil {
		d.servedGroups = map[string]bool{}
	}
	d.servedGroups[name] = served
	d.mu.Unlock()
}

func (d *skip119Disco) setResourceServed(gv, resource string, served bool) {
	d.mu.Lock()
	if d.servedResources == nil {
		d.servedResources = map[string]map[string]bool{}
	}
	if d.servedResources[gv] == nil {
		d.servedResources[gv] = map[string]bool{}
	}
	d.servedResources[gv][resource] = served
	d.mu.Unlock()
}

func newSkip119Watcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	// The served-group memo is process-global; reset so each test reads its own
	// freshly-installed double (not a prior test's snapshot).
	cache.ResetServedGroupsMemoForTest()
	t.Cleanup(cache.ResetServedGroupsMemoForTest)

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), skip119ListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	return rw
}

// gvKey renders a GVR's group/version the way discovery keys it.
func gvKey(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return gvr.Version
	}
	return gvr.Group + "/" + gvr.Version
}

// TestSkip119_GREEN_UnservedGroupSkipped is the GREEN arm. With the pre-check
// active (default ON), an EnsureResourceType for a GVR under a GROUP absent from
// ServerGroups() returns added=false and does NOT register — no informer, no
// watch-404 churn. A GVR under a SERVED custom group in the same test registers
// (per-GROUP discrimination, K>1).
func TestSkip119_GREEN_UnservedGroupSkipped(t *testing.T) {
	rw := newSkip119Watcher(t) // default ON

	disco := &skip119Disco{servedGroups: map[string]bool{
		"served.krateo.io": true,
		"dead.krateo.io":   false,
	}}
	rw.SetDiscoveryClient(disco)

	// Served custom group → registers.
	addedServed, _ := rw.EnsureResourceType(skip119ServedCustom)
	if !addedServed {
		t.Fatalf("served-group GVR: want added=true")
	}
	if !rw.IsRegistered(skip119ServedCustom) {
		t.Fatalf("served-group GVR: want IsRegistered=true")
	}

	// Dead group → SKIPPED.
	addedDead, syncCh := rw.EnsureResourceType(skip119UnservedGVR)
	if addedDead {
		t.Fatalf("dead-group GVR: want added=false (pre-check must skip a group " +
			"absent from ServerGroups()), got added=true")
	}
	if rw.IsRegistered(skip119UnservedGVR) {
		t.Fatalf("dead-group GVR: want IsRegistered=false — the informer must NOT " +
			"spawn for an unserved group (no perpetual watch-404 churn)")
	}
	select {
	case <-syncCh:
	default:
		t.Fatalf("dead-group GVR: skip must return a pre-closed sync channel")
	}
	t.Logf("GREEN: served-group %s registered; dead-group %s skipped (no churn)",
		skip119ServedCustom.Group, skip119UnservedGVR.Group)
}

// TestSkip119_S4NonRegression_ServedGroupResourceHeals is the S4/stale-delete
// non-regression arm — THE collision the group-level boundary resolves, and it
// asserts the FULL register-then-CONFIRM-then-HEAL cycle, NOT merely "not
// skipped" (per the TL hard guard: a weakened S4 test laundered through #119 is
// the anti-pattern). A GVR whose GROUP is served but whose RESOURCE is not yet
// in the served set (a post-startup CRD mid-install) must, WITH the #119
// pre-check ACTIVE:
//
//  1. REGISTER (added=true) — the group-level pre-check does not skip it (its
//     group is present in ServerGroups()). added=false here = the pre-check
//     wrongly regressed to RESOURCE granularity = S4/stale-delete regression.
//  2. stay NOT-SERVABLE while the resource type is unconfirmed (conjunct 4) —
//     exactly the S4 gate that stops the pivot serving [] for a not-yet-served
//     post-startup CRD.
//  3. HEAL to servable once the apiserver serves the RESOURCE type and the
//     confirm-ticker (RefreshDiscovery) observes it.
//
// This arm RED-fails if #119 skips the post-startup CRD (step 1: not registered
// → never heals) OR if the S4 confirm gate is broken (step 2 servable-too-early
// or step 3 never-heals). It is the exact discriminator the TL required: it
// cannot pass on a "merely not skipped" implementation because it drives the
// full heal.
func TestSkip119_S4NonRegression_ServedGroupResourceHeals(t *testing.T) {
	rw := newSkip119Watcher(t) // #119 pre-check ACTIVE (default ON)

	gv := gvKey(skip119PendingResourceGVR)
	// GROUP served (so #119 does not skip) but the RESOURCE type not yet served
	// (so the S4 confirm gate keeps it unconfirmed) — the post-startup-CRD shape.
	disco := &skip119Disco{
		servedGroups:    map[string]bool{"served.krateo.io": true},
		servedResources: map[string]map[string]bool{gv: {skip119PendingResourceGVR.Resource: false}},
	}
	rw.SetDiscoveryClient(disco)

	// Step 1 — REGISTERS despite the resource being unserved (group-level skip
	// does not fire). The informer syncs over the empty fake apiserver.
	added, syncCh := rw.EnsureResourceType(skip119PendingResourceGVR)
	if !added {
		t.Fatalf("S4 step 1: want added=true — a post-startup CRD (group served, " +
			"resource not-yet-served) MUST register so the confirm-ticker can heal it; " +
			"added=false means #119 wrongly skipped at RESOURCE granularity = " +
			"S4/stale-delete regression (this is the TL hard-guard failure)")
	}
	if !rw.IsRegistered(skip119PendingResourceGVR) {
		t.Fatalf("S4 step 1: want IsRegistered=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("S4 step 1: informer did not sync within 5s")
	}

	// Step 2 — confirm pass against the resource-UNSERVED state: conjunct 4
	// typeConfirmed is false → MUST NOT be servable, even though registered +
	// synced both hold. This is the S4 gate the fix must not weaken.
	rw.RefreshDiscovery(context.Background())
	if rw.IsServable(skip119PendingResourceGVR) {
		t.Fatalf("S4 step 2: a registered+synced post-startup CRD whose RESOURCE " +
			"type is not yet served MUST stay NOT servable (conjunct 4 unconfirmed); " +
			"servable-too-early = the S4 bug the confirm gate exists to stop")
	}

	// Step 3 — the CRD installs: the apiserver now serves the RESOURCE type. The
	// confirm-ticker observes it → HEALS to servable.
	disco.setResourceServed(gv, skip119PendingResourceGVR.Resource, true)
	rw.RefreshDiscovery(context.Background())
	if !rw.IsServable(skip119PendingResourceGVR) {
		t.Fatalf("S4 step 3: once the apiserver serves the resource type, the " +
			"confirm-ticker MUST heal the GVR to servable; it did not — the " +
			"register-then-heal path is broken (S4/stale-delete regression)")
	}
	if _, servable := rw.ListObjectsServable(skip119PendingResourceGVR, ""); !servable {
		t.Fatalf("S4 step 3: healed GVR must be servable via ListObjectsServable too")
	}
	t.Logf("S4 NON-REGRESSION: post-startup CRD registered (step1) → not-servable-while-unconfirmed " +
		"(step2) → healed to servable after resource served (step3) — full heal proven under active #119")
}

// TestSkip119_RED_FlagOffRegistersUnserved is the RED arm. With the pre-check
// DISABLED (SKIP_UNSERVED_GROUP_INFORMERS=false), the SAME dead-group call
// registers + spawns the informer — reproducing the pre-#119 churn state.
// Proves the pre-check (not another guard) is what skips the registration.
func TestSkip119_RED_FlagOffRegistersUnserved(t *testing.T) {
	t.Setenv("SKIP_UNSERVED_GROUP_INFORMERS", "false")
	rw := newSkip119Watcher(t)

	disco := &skip119Disco{servedGroups: map[string]bool{"dead.krateo.io": false}}
	rw.SetDiscoveryClient(disco)

	added, _ := rw.EnsureResourceType(skip119UnservedGVR)
	if !added {
		t.Fatalf("RED (flag off): want added=true — with the pre-check disabled the " +
			"dead group must register (pre-#119 behaviour), got added=false")
	}
	if !rw.IsRegistered(skip119UnservedGVR) {
		t.Fatalf("RED (flag off): want IsRegistered=true — proves the pre-check does " +
			"the skipping, not some other guard")
	}
	t.Logf("RED: flag off → dead-group %s REGISTERED (churn reproduced)", skip119UnservedGVR.Group)
}

// TestSkip119_BrandNewGroupRecovery is the C-specific NON-TERMINAL arm. It
// drives the real boundary ≥2×: a GVR whose group is initially absent is
// skipped; the double's ServerGroups() then flips to include the group (the
// operator's first CRD lands); a re-invocation of EnsureResourceType now
// REGISTERS it. Proves the skip is non-terminal and the dispatch re-touch
// recovers a brand-new group — no permanent false-skip.
func TestSkip119_BrandNewGroupRecovery(t *testing.T) {
	rw := newSkip119Watcher(t) // default ON

	disco := &skip119Disco{servedGroups: map[string]bool{"served.krateo.io": false}}
	rw.SetDiscoveryClient(disco)

	// Pass 1: group absent → skipped.
	added1, _ := rw.EnsureResourceType(skip119ServedCustom)
	if added1 || rw.IsRegistered(skip119ServedCustom) {
		t.Fatalf("recovery pass 1: group absent must skip (added=%v registered=%v)",
			added1, rw.IsRegistered(skip119ServedCustom))
	}

	// The operator installs — the group now appears in ServerGroups(). Expire
	// the memo so the next check re-reads.
	disco.setGroupServed("served.krateo.io", true)
	cache.ResetServedGroupsMemoForTest()

	// Pass 2: re-touch → now registers (recovery via re-invocation).
	added2, _ := rw.EnsureResourceType(skip119ServedCustom)
	if !added2 {
		t.Fatalf("recovery pass 2: want added=true — once the group appears in " +
			"ServerGroups(), the re-touch must register it (skip is NON-terminal); " +
			"added=false means a permanent false-skip")
	}
	if !rw.IsRegistered(skip119ServedCustom) {
		t.Fatalf("recovery pass 2: want IsRegistered=true after the group is served")
	}
	t.Logf("RECOVERY: group absent→skip (pass 1), group served→register (pass 2) — non-terminal")
}

// TestSkip119_FalseSkipGuard_ServerGroupsError is THE CRUX arm. A group that IS
// genuinely served but whose ServerGroups() is MOMENTARILY ERRORING (and no
// prior good snapshot exists) must NOT be skipped — it must register. Fails if
// the impl skips on uncertainty.
func TestSkip119_FalseSkipGuard_ServerGroupsError(t *testing.T) {
	rw := newSkip119Watcher(t) // pre-check ON

	disco := &skip119Disco{
		servedGroups: map[string]bool{"served.krateo.io": true},
		groupsErr:    context.DeadlineExceeded, // ServerGroups errors right now
	}
	rw.SetDiscoveryClient(disco)

	added, _ := rw.EnsureResourceType(skip119ServedCustom)
	if !added {
		t.Fatalf("FALSE-SKIP GUARD (ServerGroups error): want added=true — a " +
			"ServerGroups() error with no prior snapshot MUST NOT skip (fail-safe open). " +
			"added=false means the impl skipped on uncertainty = the false-skip defect.")
	}
	if !rw.IsRegistered(skip119ServedCustom) {
		t.Fatalf("FALSE-SKIP GUARD (ServerGroups error): want IsRegistered=true")
	}
	t.Logf("FALSE-SKIP GUARD (ServerGroups error): registered (fail-safe open)")
}

// TestSkip119_FalseSkipGuard_NilDiscovery: with NO discovery client wired
// (rw.disco==nil — degraded S4 mode), the pre-check has no ServerGroups surface
// and MUST register (never skip).
func TestSkip119_FalseSkipGuard_NilDiscovery(t *testing.T) {
	rw := newSkip119Watcher(t) // pre-check ON, no SetDiscoveryClient

	added, _ := rw.EnsureResourceType(skip119UnservedGVR)
	if !added {
		t.Fatalf("FALSE-SKIP GUARD (nil disco): want added=true — with no discovery " +
			"client the pre-check has no authoritative signal and must register")
	}
	if !rw.IsRegistered(skip119UnservedGVR) {
		t.Fatalf("FALSE-SKIP GUARD (nil disco): want IsRegistered=true")
	}
	t.Logf("FALSE-SKIP GUARD (nil disco): registered (fail-safe open)")
}

// TestSkip119_CoreGroupNeverSkipped: the core group ("") is always served and
// must never be skipped even if the double's ServerGroups() omits it (a double
// that does not list core).
func TestSkip119_CoreGroupNeverSkipped(t *testing.T) {
	rw := newSkip119Watcher(t) // pre-check ON

	// ServerGroups() lists only a custom group — core "" is NOT listed. The
	// core-group short-circuit must still register the core GVR.
	disco := &skip119Disco{servedGroups: map[string]bool{"served.krateo.io": true}}
	rw.SetDiscoveryClient(disco)

	added, _ := rw.EnsureResourceType(skip119ServedGVR) // secrets, core group ""
	if !added || !rw.IsRegistered(skip119ServedGVR) {
		t.Fatalf("core group must never be skipped (added=%v registered=%v)",
			added, rw.IsRegistered(skip119ServedGVR))
	}
	t.Logf("core group never skipped: %s registered", skip119ServedGVR)
}

// TestSkip119_MixedK_Concurrent_Race is the mixed-K + -race arm. Concurrent
// EnsureResourceType of a served-group GVR + a dead-group GVR under one double,
// asserting per-group discrimination holds under concurrency with no data race
// on the shared disco read + served-group memo + informers write.
func TestSkip119_MixedK_Concurrent_Race(t *testing.T) {
	rw := newSkip119Watcher(t) // pre-check ON

	disco := &skip119Disco{servedGroups: map[string]bool{
		"served.krateo.io": true,
		"dead.krateo.io":   false,
	}}
	rw.SetDiscoveryClient(disco)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); rw.EnsureResourceType(skip119ServedCustom) }()
		go func() { defer wg.Done(); rw.EnsureResourceType(skip119UnservedGVR) }()
	}
	wg.Wait()

	if !rw.IsRegistered(skip119ServedCustom) {
		t.Fatalf("mixed-K race: served-group GVR must be registered")
	}
	if rw.IsRegistered(skip119UnservedGVR) {
		t.Fatalf("mixed-K race: dead-group GVR must NOT be registered (skipped under concurrency)")
	}
	t.Logf("mixed-K race: served-group registered + dead-group skipped under 16 concurrent EnsureResourceType")
}
