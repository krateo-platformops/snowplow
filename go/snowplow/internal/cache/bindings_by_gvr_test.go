// bindings_by_gvr_test.go — Ship 1 unit tests for the incremental
// BindingsByGVR reverse index. Package cache (not cache_test) so it can
// reach the unexported build/enumerate/delta helpers + PublishRBACSnapshotForTest.
//
// NON-DESTRUCTIVE — builds synthetic snapshots in-process; never touches
// the rbac package's destructive TestMain. Safe under `go test
// ./internal/cache/...`.

package cache

import (
	"context"
	"expvar"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// gr is a terse GVR (group, resource) for the tests (version irrelevant
// to the index).
func gr(group, resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Resource: resource}
}

// crbRuleUID builds a CRB whose roleRef points at a ClusterRole carrying
// the given rules. Returns the CRB + the CR. uid makes the binding
// identity stable across the index build + delta hooks.
func crbRuleUID(name, uid string, sub rbacv1.Subject, rules []rbacv1.PolicyRule) (*rbacv1.ClusterRoleBinding, *rbacv1.ClusterRole) {
	cr := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-role"},
		Rules:      rules,
	}
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
		Subjects:   []rbacv1.Subject{sub},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name + "-role"},
	}
	return crb, cr
}

func wildcardRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{{Verbs: []string{"*"}, APIGroups: []string{"*"}, Resources: []string{"*"}}}
}

func getListRules(group, resource string) []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{{
		Verbs:     []string{"get", "list"},
		APIGroups: []string{group},
		Resources: []string{resource},
	}}
}

// buildSnap assembles a snapshot from CRBs + CRs (+ their subject indexes
// so the cohort hash helpers stay consistent).
func buildSnap(crbs []*rbacv1.ClusterRoleBinding, crs []*rbacv1.ClusterRole) *RBACSnapshot {
	s := &RBACSnapshot{
		ClusterRoleBindings: crbs,
		RoleBindingsByNS:    map[string][]*rbacv1.RoleBinding{},
		ClusterRolesByName:  map[string]*rbacv1.ClusterRole{},
		RolesByNSName:       map[string]*rbacv1.Role{},
	}
	for _, cr := range crs {
		s.ClusterRolesByName[cr.Name] = cr
	}
	rebuildSubjectIndexes(s)
	return s
}

// Ship 0.30.242 H.c-layered Phase 2c — assertions migrated from
// EnumerateResourceCohorts (deleted) to EnumeratePrewarmTargetsForGVR.
// Same INDEX BEHAVIOUR under test (the wildcard bucket matches every
// navigated GVR); assertion shape adapted to per-binding targets
// instead of per-cohort tuples.
func TestBindingsByGVRIndex_WildcardMatchesEveryGVR(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	adminCRB, adminCR := crbRuleUID("admin-binding", "uid-admin",
		rbacv1.Subject{Kind: "User", Name: "admin"}, wildcardRules())
	snap := buildSnap([]*rbacv1.ClusterRoleBinding{adminCRB}, []*rbacv1.ClusterRole{adminCR})
	PublishRBACSnapshotForTest(snap)

	navigated := []schema.GroupVersionResource{
		gr("composition.krateo.io", "compositions"),
		gr("widgets.templates.krateo.io", "panels"),
	}
	if n := BuildBindingsByGVRIndex(navigated); n != 1 {
		t.Fatalf("expected 1 binding enrolled, got %d", n)
	}

	// The wildcard binding must match EVERY navigated GVR; one
	// per-binding target per GVR with the admin user as representative.
	for _, g := range navigated {
		targets := EnumeratePrewarmTargetsForGVR(g, "list")
		if len(targets) != 1 {
			t.Fatalf("gvr %s: expected 1 target (admin via wildcard), got %d: %+v", g, len(targets), targets)
		}
		if targets[0].Subject.Username != "admin" {
			t.Fatalf("gvr %s: expected admin representative, got %q", g, targets[0].Subject.Username)
		}
	}
}

// Ship 0.30.242 H.c-layered Phase 2c — migrated assertion shape.
// Group-only cohort sentinel semantics (groupOnlyCohortSentinel +
// [group]) collapse to Subject.Username="" + Subject.Groups=[group]
// per pickRepresentativeFromSubjectKeys (prewarm_enumeration.go).
func TestBindingsByGVRIndex_ExactMatchScopesToGVR(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	// devsCRB grants get/list ONLY on compositions.
	devsCRB, devsCR := crbRuleUID("devs-binding", "uid-devs",
		rbacv1.Subject{Kind: "Group", Name: "devs"},
		getListRules("composition.krateo.io", "compositions"))
	snap := buildSnap([]*rbacv1.ClusterRoleBinding{devsCRB}, []*rbacv1.ClusterRole{devsCR})
	PublishRBACSnapshotForTest(snap)

	compGVR := gr("composition.krateo.io", "compositions")
	panelGVR := gr("widgets.templates.krateo.io", "panels")
	BuildBindingsByGVRIndex([]schema.GroupVersionResource{compGVR, panelGVR})

	// devs matches compositions (group-only representative: empty Username + [devs] Groups).
	comp := EnumeratePrewarmTargetsForGVR(compGVR, "list")
	if len(comp) != 1 {
		t.Fatalf("compositions: expected 1 target, got %d: %+v", len(comp), comp)
	}
	if comp[0].Subject.Username != "" || len(comp[0].Subject.Groups) != 1 || comp[0].Subject.Groups[0] != "devs" {
		t.Fatalf("compositions representative: want group-only [devs], got %+v", comp[0].Subject)
	}

	// devs does NOT match panels — the scoping must exclude it.
	if pan := EnumeratePrewarmTargetsForGVR(panelGVR, "list"); len(pan) != 0 {
		t.Fatalf("panels: expected 0 targets (devs doesn't grant panels), got %d: %+v", len(pan), pan)
	}
}

func TestBindingsByGVRIndex_DeltaAddRemove(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	compGVR := gr("composition.krateo.io", "compositions")

	// Start with admin (wildcard) only.
	adminCRB, adminCR := crbRuleUID("admin-binding", "uid-admin",
		rbacv1.Subject{Kind: "User", Name: "admin"}, wildcardRules())
	devsCRB, devsCR := crbRuleUID("devs-binding", "uid-devs",
		rbacv1.Subject{Kind: "Group", Name: "devs"},
		getListRules("composition.krateo.io", "compositions"))
	snap := buildSnap([]*rbacv1.ClusterRoleBinding{adminCRB, devsCRB},
		[]*rbacv1.ClusterRole{adminCR, devsCR})
	PublishRBACSnapshotForTest(snap)

	BuildBindingsByGVRIndex([]schema.GroupVersionResource{compGVR})
	if got := targetRepresentativeLabels(EnumeratePrewarmTargetsForGVR(compGVR, "list")); !equalSorted(got, []string{"admin", "group:devs"}) {
		t.Fatalf("after build: want [admin group:devs], got %v", got)
	}

	// DELETE devs binding → only admin (wildcard) remains.
	onBindingDelete(devsCRB)
	if got := targetRepresentativeLabels(EnumeratePrewarmTargetsForGVR(compGVR, "list")); !equalSorted(got, []string{"admin"}) {
		t.Fatalf("after devs delete: want [admin], got %v", got)
	}

	// ADD a new binding granting compositions to a fresh group → it appears.
	opsCRB, opsCR := crbRuleUID("ops-binding", "uid-ops",
		rbacv1.Subject{Kind: "Group", Name: "ops"},
		getListRules("composition.krateo.io", "compositions"))
	// The new role must be in the published snapshot for the delta hook to
	// resolve its rules — republish with it.
	snap2 := buildSnap([]*rbacv1.ClusterRoleBinding{adminCRB, opsCRB},
		[]*rbacv1.ClusterRole{adminCR, opsCR})
	PublishRBACSnapshotForTest(snap2)
	onBindingAdd(opsCRB)
	if got := targetRepresentativeLabels(EnumeratePrewarmTargetsForGVR(compGVR, "list")); !equalSorted(got, []string{"admin", "group:ops"}) {
		t.Fatalf("after ops add: want [admin group:ops], got %v", got)
	}
}

func TestBindingsByGVRIndex_NotBuiltReturnsNil(t *testing.T) {
	ResetBindingsByGVRIndexForTest()
	if c := EnumeratePrewarmTargetsForGVR(gr("composition.krateo.io", "compositions"), "list"); c != nil {
		t.Fatalf("expected nil before build, got %+v", c)
	}
	if BindingsByGVRIndexBuilt() {
		t.Fatal("expected built=false before build")
	}
}

// TestBindingsByGVRIndex_DeltaNonTypedBumpsCanary (S1) asserts a delta
// event whose object is neither a typed *rbacv1.* pointer nor convertible
// bumps the drift canary instead of silently dropping.
func TestBindingsByGVRIndex_DeltaNonTypedBumpsCanary(t *testing.T) {
	ResetBindingsByGVRIndexForTest()
	// Build (empty navigated) so deltaActive() is true.
	PublishRBACSnapshotForTest(buildSnap(nil, nil))
	BuildBindingsByGVRIndex([]schema.GroupVersionResource{gr("composition.krateo.io", "compositions")})

	before := BindingsIndexDeltaSkippedNonTyped()
	// A garbage object — not typed, not a bytesObject/Unstructured.
	onBindingAdd("not-a-binding")
	if got := BindingsIndexDeltaSkippedNonTyped(); got != before+1 {
		t.Fatalf("expected drift canary +1 on non-typed delta, got before=%d after=%d", before, got)
	}
}

// TestRAFullListMemoExpvarPublished (0.30.210) asserts the
// snowplow_ra_full_list_memo expvar:
//   - is published under CACHE_ENABLED=true,
//   - returns a snapshot whose shape includes raKey + sliceShape + verdict
//     + recordedAtUnixSeconds + caller-CR labels per entry,
//   - reflects EVERY recorded (RA × sliceShape) entry (true + false verdicts)
//     with labels populated from RecordSliceabilityWithLabels,
//   - is safe under -race against concurrent record() writers (the race
//     coverage lives in TestRAFullList_ResidentAndMemoRace; this test adds
//     a concurrent walk-while-write check specifically against the snapshot
//     reader).
func TestRAFullListMemoExpvarPublished(t *testing.T) {
	resetSliceabilityMemoForTest()
	RegisterBindingsByGVRMetricsForTest()

	v := expvar.Get("snowplow_ra_full_list_memo")
	if v == nil {
		t.Fatal("snowplow_ra_full_list_memo not published")
	}

	// Seed two entries: one TRUE verdict + labels, one FALSE verdict + labels.
	raKeyA := ComputeKey(RAFullListKeyInputs("composition.krateo.io", "v1", "panels",
		"krateo-system", "compositions-panels", "uid-1234", nil))
	shapeA := SliceShapeHash("apiref", "widgets.templates.krateo.io", "v1beta1",
		"tables", "krateo-system", "compositions-page-datagrid", "{}")
	labelsA := SliceabilityLabels{
		CallerClass:     "apiref",
		CallerGroup:     "widgets.templates.krateo.io",
		CallerVersion:   "v1beta1",
		CallerResource:  "tables",
		CallerNamespace: "krateo-system",
		CallerName:      "compositions-page-datagrid",
	}
	RecordSliceabilityWithLabels(raKeyA, shapeA, true, labelsA)

	raKeyB := ComputeKey(RAFullListKeyInputs("composition.krateo.io", "v1", "panels",
		"OTHER-NS", "compositions-panels", "uid-c0ffee", nil))
	shapeB := SliceShapeHash("apiref", "widgets.templates.krateo.io", "v1beta1",
		"charts", "krateo-system", "compositions-chart", "{ sum: 0 }")
	labelsB := SliceabilityLabels{
		CallerClass:     "apiref",
		CallerGroup:     "widgets.templates.krateo.io",
		CallerVersion:   "v1beta1",
		CallerResource:  "charts",
		CallerNamespace: "krateo-system",
		CallerName:      "compositions-chart",
	}
	RecordSliceabilityWithLabels(raKeyB, shapeB, false, labelsB)

	// Scrape via the expvar.Func interface — must return []SliceabilityMemoEntry.
	raw := v.(expvar.Func).Value()
	snap, ok := raw.([]SliceabilityMemoEntry)
	if !ok {
		t.Fatalf("expvar value wrong type: %T (want []SliceabilityMemoEntry)", raw)
	}
	if len(snap) == 0 {
		t.Fatal("snapshot empty after two RecordSliceabilityWithLabels calls")
	}

	findByName := func(callerName string) *SliceabilityMemoEntry {
		for i := range snap {
			if snap[i].CallerName == callerName {
				return &snap[i]
			}
		}
		return nil
	}

	gridEntry := findByName("compositions-page-datagrid")
	if gridEntry == nil {
		t.Fatalf("compositions-page-datagrid entry missing from snapshot: %+v", snap)
	}
	if !gridEntry.Verdict {
		t.Fatalf("compositions-page-datagrid verdict = %v, want true", gridEntry.Verdict)
	}
	if gridEntry.RAKey != raKeyA {
		t.Fatalf("compositions-page-datagrid raKey = %q, want %q", gridEntry.RAKey, raKeyA)
	}
	if gridEntry.SliceShape != shapeA {
		t.Fatalf("compositions-page-datagrid sliceShape = %q, want %q", gridEntry.SliceShape, shapeA)
	}
	if gridEntry.RecordedAtUnixSeconds == 0 {
		t.Fatalf("compositions-page-datagrid recordedAtUnixSeconds = 0, want non-zero")
	}
	// REQ-2 (0.30.210): on INSERT, recordedAt == lastUpdatedAt (same clock
	// read for both). The consumer cares about monotonicity, not byte-equality.
	if gridEntry.LastUpdatedAtUnixSeconds != gridEntry.RecordedAtUnixSeconds {
		t.Fatalf("on insert lastUpdatedAt (%d) should equal recordedAt (%d)",
			gridEntry.LastUpdatedAtUnixSeconds, gridEntry.RecordedAtUnixSeconds)
	}
	if gridEntry.CallerClass != labelsA.CallerClass ||
		gridEntry.CallerGroup != labelsA.CallerGroup ||
		gridEntry.CallerVersion != labelsA.CallerVersion ||
		gridEntry.CallerResource != labelsA.CallerResource ||
		gridEntry.CallerNamespace != labelsA.CallerNamespace {
		t.Fatalf("compositions-page-datagrid labels mismatch: got %+v want %+v", gridEntry, labelsA)
	}

	chartEntry := findByName("compositions-chart")
	if chartEntry == nil {
		t.Fatalf("compositions-chart entry missing from snapshot: %+v", snap)
	}
	if chartEntry.Verdict {
		t.Fatalf("compositions-chart verdict = %v, want false", chartEntry.Verdict)
	}
	if chartEntry.CallerName != "compositions-chart" || chartEntry.CallerResource != "charts" {
		t.Fatalf("compositions-chart labels mismatch: %+v", chartEntry)
	}

	// Refresh-in-place preserves recordedAt (first-freeze semantics) BUT
	// updates lastUpdatedAt to "now" (REQ-2 — Mode-3 stuck-vs-refreshing
	// discriminator).
	firstRecordedAt := gridEntry.RecordedAtUnixSeconds
	// Bump the simulated clock so a NEW lastUpdatedAt is observable.
	bumped := firstRecordedAt + 100
	f := func() int64 { return bumped }
	prev := nowUnix.Load()
	nowUnix.Store(&f)
	t.Cleanup(func() { nowUnix.Store(prev) })

	// Re-record FLIPPING the verdict for the SAME (raKey, shape) — labels arg
	// here is empty to assert that refresh-in-place preserves the ORIGINAL
	// labels (does not overwrite them with empty).
	RecordSliceabilityWithLabels(raKeyA, shapeA, false, SliceabilityLabels{})
	snap2 := v.(expvar.Func).Value().([]SliceabilityMemoEntry)
	grid2 := func() *SliceabilityMemoEntry {
		for i := range snap2 {
			if snap2[i].CallerName == "compositions-page-datagrid" {
				return &snap2[i]
			}
		}
		return nil
	}()
	if grid2 == nil {
		t.Fatal("compositions-page-datagrid entry lost after refresh")
	}
	if grid2.Verdict {
		t.Fatalf("verdict refresh did not flip: %+v", grid2)
	}
	if grid2.RecordedAtUnixSeconds != firstRecordedAt {
		t.Fatalf("recordedAt mutated on refresh: was %d now %d",
			firstRecordedAt, grid2.RecordedAtUnixSeconds)
	}
	// REQ-2: lastUpdatedAt tracks last-write, not first-freeze.
	if grid2.LastUpdatedAtUnixSeconds != bumped {
		t.Fatalf("lastUpdatedAt did not advance on refresh: got %d, want %d",
			grid2.LastUpdatedAtUnixSeconds, bumped)
	}
	if grid2.LastUpdatedAtUnixSeconds <= grid2.RecordedAtUnixSeconds {
		t.Fatalf("after refresh lastUpdatedAt (%d) must be > recordedAt (%d)",
			grid2.LastUpdatedAtUnixSeconds, grid2.RecordedAtUnixSeconds)
	}
	if grid2.CallerName != "compositions-page-datagrid" {
		t.Fatalf("refresh-in-place LOST labels: %+v", grid2)
	}
}

// TestSliceabilityLookupGate_TTLAsyncScheduling (Ship #91 / 0.30.211, PM
// redesign 2026-05-30 — supersedes the architect-brief "return known=false"
// wording) covers the Lever A async-worker TTL gate:
//
//   - A FRESH verdict=false entry returns (false, true) — the customer
//     /call falls back, no re-verify. EnqueuedTotal stays at 0.
//   - The SAME entry after `now > lastUpdated + T_unverify` AND retryCount <
//     cap STILL returns (false, true) — the customer /call never blocks —
//     but the lookup fire-and-forgets BOTH:
//        a) SubmitSliceabilityInvalidate (worker.EnqueuedTotal +1).
//        b) EnqueueRefresh (refresher.enqueueTotal +1).
//   - After retryCount reaches the cap (3 for non-permanent), further
//     stuck-false lookups stop scheduling: counters stay flat.
//
// Time is faked via nowUnix.Store so T_unverify (60s) elapses
// instantaneously; the refresher singleton is reset so its enqueueTotal is
// readable in isolation.
func TestSliceabilityLookupGate_TTLAsyncScheduling(t *testing.T) {
	resetSliceabilityMemoForTest()
	resetSliceabilityReverifyWorkerForTest()
	resetRefresherForTest()

	var clock atomic.Int64
	clock.Store(1_000_000) // arbitrary starting Unix-seconds
	f := func() int64 { return clock.Load() }
	prev := nowUnix.Load()
	nowUnix.Store(&f)
	t.Cleanup(func() { nowUnix.Store(prev) })

	raKey := "rakey-A"
	shape := SliceShapeHash("apiref", "g", "v", "r", "ns", "n", "{}")

	// Insert verdict=false (Class A — not permanent).
	RecordSliceabilityWithLabels(raKey, shape, false, SliceabilityLabels{CallerClass: "apiref"})

	// FRESH: lookup must return (false, true), and MUST NOT schedule any
	// async work (the rate-floor isn't expired yet).
	if sliceable, known := SliceabilityLookup(raKey, shape); known != true || sliceable {
		t.Fatalf("fresh verdict=false: lookup=(%v,%v), want (false,true)", sliceable, known)
	}
	if got := SliceabilityReverifyStatsSnapshot().EnqueuedTotal; got != 0 {
		t.Fatalf("fresh lookup must NOT submit invalidate: EnqueuedTotal=%d", got)
	}
	if got := refresherEnqueueTotalForTest(); got != 0 {
		t.Fatalf("fresh lookup must NOT enqueue refresh: refresherEnqueueTotal=%d", got)
	}

	// Advance the clock past T_unverify (60s default).
	clock.Add(61)

	// EXPIRED with retryCount=0 < cap(3): lookup STILL returns (false, true)
	// — customer /call falls back fast — but BOTH async paths fire.
	if sliceable, known := SliceabilityLookup(raKey, shape); known != true || sliceable {
		t.Fatalf("expired r=0 cap=3: lookup=(%v,%v), want (false,true) — customer must NEVER block", sliceable, known)
	}
	if got := SliceabilityReverifyStatsSnapshot().EnqueuedTotal; got != 1 {
		t.Fatalf("expired lookup must submit ONCE: EnqueuedTotal=%d, want 1", got)
	}
	if got := refresherEnqueueTotalForTest(); got != 1 {
		t.Fatalf("expired lookup must enqueue refresh ONCE: refresherEnqueueTotal=%d, want 1", got)
	}

	// Simulate the eventual first-sight cycle (worker invalidates → next
	// /call enters first-sight → record() with verdict=false again →
	// retryCount climbs). Three records take retryCount 0→1→2→3.
	for i := 0; i < 3; i++ {
		RecordSliceabilityWithLabels(raKey, shape, false, SliceabilityLabels{})
	}

	// Move past T_unverify again. retryCount==3 == cap → lookup must
	// return (false, true) AND must NOT schedule further async work.
	clock.Add(61)
	priorEnq := SliceabilityReverifyStatsSnapshot().EnqueuedTotal
	priorRefresh := refresherEnqueueTotalForTest()
	if sliceable, known := SliceabilityLookup(raKey, shape); known != true || sliceable {
		t.Fatalf("cap reached: lookup=(%v,%v), want (false,true)", sliceable, known)
	}
	if got := SliceabilityReverifyStatsSnapshot().EnqueuedTotal; got != priorEnq {
		t.Fatalf("cap reached: EnqueuedTotal advanced (%d -> %d); must stay flat", priorEnq, got)
	}
	if got := refresherEnqueueTotalForTest(); got != priorRefresh {
		t.Fatalf("cap reached: refresherEnqueueTotal advanced (%d -> %d); must stay flat", priorRefresh, got)
	}

	// Snapshot must reflect retryCount=3, permanent=false.
	snap := SliceabilityMemoSnapshot()
	var got *SliceabilityMemoEntry
	for i := range snap {
		if snap[i].RAKey == raKey {
			got = &snap[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("entry missing from snapshot")
	}
	if got.RetryCount != 3 || got.Permanent {
		t.Fatalf("snapshot: retryCount=%d permanent=%v, want 3 + false", got.RetryCount, got.Permanent)
	}
}

// TestSliceabilityLookupGate_PermanentShortCircuits (Ship #91 / 0.30.211,
// PM redesign 2026-05-30) covers the Class C heuristic short-circuit: a
// permanent=true entry's lookup ALWAYS returns the cached false verdict
// and NEVER schedules an async re-verify (no submit, no refresh enqueue).
// A structurally non-sliceable shape can never flip, so burning worker
// cycles on it is pure waste — the architectural invariant `permanent`
// captures.
func TestSliceabilityLookupGate_PermanentShortCircuits(t *testing.T) {
	resetSliceabilityMemoForTest()
	resetSliceabilityReverifyWorkerForTest()
	resetRefresherForTest()

	var clock atomic.Int64
	clock.Store(2_000_000)
	f := func() int64 { return clock.Load() }
	prev := nowUnix.Load()
	nowUnix.Store(&f)
	t.Cleanup(func() { nowUnix.Store(prev) })

	raKey := "rakey-C"
	shape := SliceShapeHash("apiref", "g", "v", "r", "ns", "c", "{ sum: 0 }")

	// Insert verdict=false with permanent=true (Class C).
	RecordSliceabilityClassified(raKey, shape, false, true, SliceabilityLabels{})

	// Fresh: gate returns cached false; no schedule.
	if sliceable, known := SliceabilityLookup(raKey, shape); known != true || sliceable {
		t.Fatalf("fresh permanent: lookup=(%v,%v), want (false,true)", sliceable, known)
	}

	// Advance well past T_unverify. Permanent entries MUST NOT trigger
	// submit or enqueue — even an unlimited number of expired lookups.
	clock.Add(3600)
	for i := 0; i < 5; i++ {
		if sliceable, known := SliceabilityLookup(raKey, shape); known != true || sliceable {
			t.Fatalf("permanent iter %d: lookup=(%v,%v), want (false,true)", i, sliceable, known)
		}
	}
	if got := SliceabilityReverifyStatsSnapshot().EnqueuedTotal; got != 0 {
		t.Fatalf("permanent MUST NOT submit invalidate: EnqueuedTotal=%d, want 0", got)
	}
	if got := refresherEnqueueTotalForTest(); got != 0 {
		t.Fatalf("permanent MUST NOT enqueue refresh: refresherEnqueueTotal=%d, want 0", got)
	}

	// Verify the permanent flag is sticky on subsequent refresh-in-place
	// even if the caller passes permanent=false (defensive against a /call
	// site that lost the heuristic).
	RecordSliceabilityClassified(raKey, shape, false, false, SliceabilityLabels{})
	snap := SliceabilityMemoSnapshot()
	for i := range snap {
		if snap[i].RAKey == raKey {
			if !snap[i].Permanent {
				t.Fatalf("permanent must be sticky-once-true; snapshot Permanent=%v", snap[i].Permanent)
			}
			break
		}
	}
}

// TestSliceabilityLookupGate_TrueVerdictUngated (Ship #91 / 0.30.211)
// asserts a verdict=TRUE entry is NEVER subject to the TTL gate — the
// fast-path stays fast forever (until an L1 dirty-mark or process restart).
func TestSliceabilityLookupGate_TrueVerdictUngated(t *testing.T) {
	resetSliceabilityMemoForTest()

	var clock atomic.Int64
	clock.Store(3_000_000)
	f := func() int64 { return clock.Load() }
	prev := nowUnix.Load()
	nowUnix.Store(&f)
	t.Cleanup(func() { nowUnix.Store(prev) })

	raKey := "rakey-true"
	shape := SliceShapeHash("apiref", "g", "v", "r", "ns", "ok", "{}")

	RecordSliceabilityWithLabels(raKey, shape, true, SliceabilityLabels{})
	// Jump WAY past T_unverify.
	clock.Add(3600)

	if sliceable, known := SliceabilityLookup(raKey, shape); known != true || !sliceable {
		t.Fatalf("verdict=true after 1h: lookup=(%v,%v), want (true,true) — true verdicts are NEVER expired by Lever A", sliceable, known)
	}
}

// TestSliceabilityInvalidate_RateFloorRespected (Ship #91 / 0.30.211)
// covers Lever C: InvalidateSliceabilityForKey removes memo entries with
// matching raKey, BUT only those whose lastUpdated is at least T_unverify
// in the past (rate-floor). Before the floor it is a no-op.
func TestSliceabilityInvalidate_RateFloorRespected(t *testing.T) {
	resetSliceabilityMemoForTest()

	var clock atomic.Int64
	clock.Store(4_000_000)
	f := func() int64 { return clock.Load() }
	prev := nowUnix.Load()
	nowUnix.Store(&f)
	t.Cleanup(func() { nowUnix.Store(prev) })

	raKey := "rakey-inv"
	shape := SliceShapeHash("apiref", "g", "v", "r", "ns", "inv", "{}")
	RecordSliceabilityWithLabels(raKey, shape, false, SliceabilityLabels{})

	// BEFORE rate-floor expires: invalidate must be a no-op (returns 0).
	if n := InvalidateSliceabilityForKey(raKey); n != 0 {
		t.Fatalf("invalidate before rate-floor: removed=%d, want 0", n)
	}
	if _, known := SliceabilityLookup(raKey, shape); !known {
		t.Fatalf("entry must remain known after rate-floored invalidate")
	}

	// AFTER rate-floor expires: invalidate removes the entry.
	clock.Add(61)
	if n := InvalidateSliceabilityForKey(raKey); n != 1 {
		t.Fatalf("invalidate after rate-floor: removed=%d, want 1", n)
	}
	if _, known := SliceabilityLookup(raKey, shape); known {
		t.Fatalf("entry must be UNKNOWN after invalidate")
	}

	// Invalidating an unknown key is a no-op (zero matches).
	if n := InvalidateSliceabilityForKey("nonexistent"); n != 0 {
		t.Fatalf("invalidate nonexistent: removed=%d, want 0", n)
	}
	// Invalidating with empty key is rejected up-front.
	if n := InvalidateSliceabilityForKey(""); n != 0 {
		t.Fatalf("invalidate empty: removed=%d, want 0", n)
	}
}

// TestSliceabilityInvalidate_MultipleSliceShapesSameRAKey (Ship #91 / 0.30.211)
// covers the multi-shape case: ONE raKey backs MANY (raKey, sliceShape)
// memo entries (one per caller widget). InvalidateSliceabilityForKey must
// clear EVERY entry whose raKey matches.
func TestSliceabilityInvalidate_MultipleSliceShapesSameRAKey(t *testing.T) {
	resetSliceabilityMemoForTest()

	var clock atomic.Int64
	clock.Store(5_000_000)
	f := func() int64 { return clock.Load() }
	prev := nowUnix.Load()
	nowUnix.Store(&f)
	t.Cleanup(func() { nowUnix.Store(prev) })

	raKey := "rakey-multi"
	shapes := []string{
		SliceShapeHash("apiref", "g", "v", "r1", "ns", "w1", "{}"),
		SliceShapeHash("apiref", "g", "v", "r2", "ns", "w2", "{}"),
		SliceShapeHash("apiref", "g", "v", "r3", "ns", "w3", "{}"),
	}
	for _, s := range shapes {
		RecordSliceabilityWithLabels(raKey, s, false, SliceabilityLabels{})
	}

	clock.Add(61)
	if n := InvalidateSliceabilityForKey(raKey); n != 3 {
		t.Fatalf("invalidate multi-shape: removed=%d, want 3", n)
	}
	for _, s := range shapes {
		if _, known := SliceabilityLookup(raKey, s); known {
			t.Fatalf("entry shape=%q must be UNKNOWN after invalidate", s)
		}
	}
}

// TestIsStructurallyNonSliceable_ClassC (Ship #91 / 0.30.211) covers the
// Class C heuristic: full and S_ra are byte-identical AND arrLen(full) >
// perPage → permanent=true. All other cases → false.
func TestIsStructurallyNonSliceable_ClassC(t *testing.T) {
	mk := func(items []any) map[string]any {
		return map[string]any{"items": items}
	}
	mkItems := func(n int) []any {
		out := make([]any, n)
		for i := 0; i < n; i++ {
			out[i] = map[string]any{"i": float64(i)}
		}
		return out
	}

	// Class C TRUE case: full == sRA (RA did not paginate) AND len > perPage.
	full := mk(mkItems(50))
	sRA := mk(mkItems(50))
	if !IsStructurallyNonSliceable(full, sRA, 10) {
		t.Fatalf("Class C: identical 50-element full/sRA at perPage=10 — must be permanent")
	}

	// arrLen <= perPage: NOT Class C.
	smallFull := mk(mkItems(5))
	smallSRA := mk(mkItems(5))
	if IsStructurallyNonSliceable(smallFull, smallSRA, 10) {
		t.Fatalf("arrLen<=perPage: must NOT be Class C")
	}

	// full != sRA: the RA actually paginated → NOT permanent (this is a
	// transient byte-mismatch, not a structural non-sliceability).
	differing := mk(mkItems(50))
	differing2 := mk(mkItems(10))
	if IsStructurallyNonSliceable(differing, differing2, 10) {
		t.Fatalf("full != sRA: must NOT be Class C")
	}

	// perPage<=0: never Class C.
	if IsStructurallyNonSliceable(full, sRA, 0) {
		t.Fatalf("perPage=0: must NOT be Class C")
	}

	// Multi-array shape (not the single-array contract): NOT Class C.
	weird := map[string]any{"a": mkItems(50), "b": mkItems(50)}
	weird2 := map[string]any{"a": mkItems(50), "b": mkItems(50)}
	if IsStructurallyNonSliceable(weird, weird2, 10) {
		t.Fatalf("multi-array shape: must NOT be Class C")
	}

	// Nil inputs: NOT Class C.
	if IsStructurallyNonSliceable(nil, sRA, 10) {
		t.Fatalf("nil full: must NOT be Class C")
	}
	if IsStructurallyNonSliceable(full, nil, 10) {
		t.Fatalf("nil sRA: must NOT be Class C")
	}
}

// TestSliceabilityReverifyWorker_AsyncDrain (Ship #91 / 0.30.211) covers
// the async invalidator worker end-to-end: Submit puts a raKey onto the
// bounded queue, the worker drains it and invalidates the matching memo
// entries. The drop-on-full path is exercised separately below.
//
// IMPORTANT: this test starts a worker goroutine bound to a ctx that is
// cancelled on t.Cleanup, AND blocks on a barrier to ensure the worker has
// EXITED before returning so subsequent tests' resetSliceabilityMemoForTest
// does not race a still-running worker (the original failure mode at
// commit time — flushed out by -race).
func TestSliceabilityReverifyWorker_AsyncDrain(t *testing.T) {
	resetSliceabilityMemoForTest()
	resetSliceabilityReverifyWorkerForTest()

	var clock atomic.Int64
	clock.Store(6_000_000)
	f := func() int64 { return clock.Load() }
	prev := nowUnix.Load()
	nowUnix.Store(&f)
	t.Cleanup(func() { nowUnix.Store(prev) })

	raKey := "rakey-async"
	shape := SliceShapeHash("apiref", "g", "v", "r", "ns", "w", "{}")
	RecordSliceabilityWithLabels(raKey, shape, false, SliceabilityLabels{})

	// Past rate-floor so invalidate() can act.
	clock.Add(61)

	ctx, cancel := context.WithCancel(t.Context())
	w := reverifyWorker
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		w.runWorker(ctx, 0)
	}()
	t.Cleanup(func() {
		cancel()
		// Block until the worker goroutine has actually exited so the
		// next test's reset does not race the still-running worker.
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			t.Errorf("worker did not exit within 2s of ctx cancel")
		}
	})

	if !SubmitSliceabilityInvalidate(raKey) {
		t.Fatalf("submit must succeed for first call (queue empty)")
	}

	// Wait up to 2s for the worker to drain the queue. We poll on
	// ProcessedTotal (a write the runWorker performs AFTER calling
	// InvalidateSliceabilityForKey) rather than on the lookup, because the
	// Lever A TTL gate ALSO returns known=false for an expired stuck-false
	// entry — using ProcessedTotal disambiguates "worker drained" from
	// "lookup gate opened by Lever A".
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if SliceabilityReverifyStatsSnapshot().ProcessedTotal > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := SliceabilityReverifyStatsSnapshot()
	if s.EnqueuedTotal == 0 || s.ProcessedTotal == 0 || s.InvalidatedTotal == 0 {
		t.Fatalf("worker stats not updated: %+v", s)
	}

	// And confirm the underlying memo entry is gone (whichever mechanism —
	// Lever A or Lever C — wins, the entry should now be unknown to the
	// memo store itself).
	if e := raSliceabilityMemo.snapshot(); len(e) != 0 {
		// Whether Lever A or Lever C removed the entry, the snapshot must
		// be empty because the worker explicitly Delete()d the memo key.
		// (Lever A alone never Deletes — it just reports known=false on
		// lookup.) Distinguishes the two paths.
		// Allow either path: the strict assertion is that the snapshot is
		// empty since invalidate Delete()s.
		t.Fatalf("memo entry should be deleted after worker invalidate, snapshot=%+v", e)
	}
}

// TestSliceabilityReverifyWorker_DropOnFull (Ship #91 / 0.30.211) asserts
// the worker's bounded-queue + drop-on-full policy: once the queue is
// full, additional Submit calls return false and tick the drop counter.
// The worker never blocks the producer.
func TestSliceabilityReverifyWorker_DropOnFull(t *testing.T) {
	resetSliceabilityReverifyWorkerForTest()
	w := reverifyWorker

	// NB: deliberately do NOT start the worker goroutine — we want the
	// queue to fill up and stay full so the drop path is exercised
	// deterministically. The Submit calls fill the queue until it's full,
	// then subsequent submits drop.

	qcap := cap(w.queue)
	accepted := 0
	for i := 0; i < qcap+10; i++ {
		if SubmitSliceabilityInvalidate("rk-" + itoa(i)) {
			accepted++
		}
	}
	if accepted != qcap {
		t.Fatalf("accepted=%d, want exactly qcap=%d", accepted, qcap)
	}
	s := SliceabilityReverifyStatsSnapshot()
	if s.EnqueuedTotal != uint64(qcap) {
		t.Fatalf("EnqueuedTotal=%d, want %d", s.EnqueuedTotal, qcap)
	}
	if s.DroppedTotal != 10 {
		t.Fatalf("DroppedTotal=%d, want 10", s.DroppedTotal)
	}
	if s.QueueLen != qcap || s.QueueCap != qcap {
		t.Fatalf("queue len/cap mismatch: %+v", s)
	}
}

// TestRAFullListMemoSnapshotConcurrentWriters (0.30.210) — race coverage for
// the snapshot reader running concurrently with record() writers. The
// existing TestRAFullList_ResidentAndMemoRace exercises lookup/record but not
// the snapshot walk. Run under -race; passes when no race report fires and
// the snapshot is never observed with a nil entry.
func TestRAFullListMemoSnapshotConcurrentWriters(t *testing.T) {
	resetSliceabilityMemoForTest()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 4 writers churning distinct (raKey, shape) pairs.
	for w := 0; w < 4; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				rk := "rakey-" + itoa(w) + "-" + itoa(i%17)
				sh := SliceShapeHash("apiref", "g", "v", "r", "ns",
					"name-"+itoa(w)+"-"+itoa(i%13), "{}")
				RecordSliceabilityWithLabels(rk, sh, (i%2) == 0, SliceabilityLabels{
					CallerClass:    "apiref",
					CallerResource: "r",
					CallerName:     "name-" + itoa(w),
				})
			}
		}()
	}

	// 2 snapshot readers.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := SliceabilityMemoSnapshot()
				for _, e := range snap {
					// Defensive: an entry's raKey/sliceShape must not be empty
					// (we never record with empty strings, and the snapshot
					// must not invent any). Catches an interleaving where the
					// reader sees a half-initialised entry.
					if e.RAKey == "" || e.SliceShape == "" {
						panic("snapshot returned entry with empty raKey/sliceShape: " + e.RAKey + "/" + e.SliceShape)
					}
				}
			}
		}()
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestRAFullListServeExpvarPublished (clause-5) asserts the
// snowplow_ra_full_list_serve expvar is published and reflects a recorded
// Hit — so the tester can assert the cheap raFullList serve path over the
// LB.
func TestRAFullListServeExpvarPublished(t *testing.T) {
	RegisterBindingsByGVRMetricsForTest()
	v := expvar.Get("snowplow_ra_full_list_serve")
	if v == nil {
		t.Fatal("snowplow_ra_full_list_serve not published")
	}
	before := RAFullListServeSnapshot().Hit
	RecordRAFullListServe(RAFullListServeHit)
	if got := RAFullListServeSnapshot().Hit; got != before+1 {
		t.Fatalf("Hit counter did not advance: before=%d after=%d", before, got)
	}
	// The expvar Func must reflect the live counter at scrape time.
	m, ok := v.(expvar.Func).Value().(map[string]uint64)
	if !ok {
		t.Fatalf("expvar value wrong type: %T", v.(expvar.Func).Value())
	}
	if m["hit"] != RAFullListServeSnapshot().Hit {
		t.Fatalf("expvar hit=%d != snapshot hit=%d", m["hit"], RAFullListServeSnapshot().Hit)
	}
}

// ── helpers ──

// targetRepresentativeLabels renders the per-binding targets' representative
// SubjectIdentity into stable string labels for set-equality assertions.
// User-kind representative → bare username (e.g. "admin"). Group-only
// representative (Username=="", single Group) → "group:<name>". SA-kind →
// the canonical "system:serviceaccount:<ns>:<name>" Username form. Used by
// the BindingsByGVRIndex tests to assert WHICH bindings landed without
// pinning to the per-binding UIDs (which the test fixtures synthesise).
//
// Ship 0.30.242 H.c-layered Phase 2c — replaces the pre-ship cohortUsernames
// helper that returned []Cohort.Username; cohort type is deleted.
func targetRepresentativeLabels(ts []PrewarmTarget) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		switch {
		case t.Subject.Username != "":
			out = append(out, t.Subject.Username)
		case len(t.Subject.Groups) > 0:
			out = append(out, "group:"+t.Subject.Groups[0])
		default:
			out = append(out, "<anonymous>")
		}
	}
	return out
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	ac := append([]string(nil), a...)
	bc := append([]string(nil), b...)
	sort.Strings(ac)
	sort.Strings(bc)
	for i := range ac {
		if ac[i] != bc[i] {
			return false
		}
	}
	return true
}

// TestBindingsByGVRIndex_ConcurrentRace (M1) — the concurrency falsifier.
// N goroutines mutate the index via the delta hooks (onBindingAdd /
// onBindingUpdate / onBindingDelete / onRoleObjectChanged), M goroutines
// enumerate (EnumeratePrewarmTargetsForGVR), and one rebuilds
// (BuildBindingsByGVRIndex) — all concurrent against the singleton. The
// real hazard is the RBAC informer processor goroutine mutating while the
// engine goroutine enumerates; the single-goroutine -race run never
// exercised it. Run with `go test -race`: asserts no data race, no panic.
func TestBindingsByGVRIndex_ConcurrentRace(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	compGVR := gr("composition.krateo.io", "compositions")
	panelGVR := gr("widgets.templates.krateo.io", "panels")
	navigated := []schema.GroupVersionResource{compGVR, panelGVR}

	// Seed an initial snapshot + build so deltaActive() is true and the
	// hooks do real work. Pre-build the churn roles into the snapshot so
	// the add hooks enrol rather than parking in byRole.
	adminCRB, adminCR := crbRuleUID("admin-binding", "uid-admin",
		rbacv1.Subject{Kind: "User", Name: "admin"}, wildcardRules())
	const poolSize = 16
	pool := make([]*rbacv1.ClusterRoleBinding, poolSize)
	crs := []*rbacv1.ClusterRole{adminCR}
	for i := 0; i < poolSize; i++ {
		crb, cr := crbRuleUID(
			"churn-"+itoa(i), "uid-churn-"+itoa(i),
			rbacv1.Subject{Kind: "Group", Name: "g" + itoa(i)},
			getListRules("composition.krateo.io", "compositions"))
		pool[i] = crb
		crs = append(crs, cr)
	}
	snap := buildSnap(append([]*rbacv1.ClusterRoleBinding{adminCRB}, pool...), crs)
	PublishRBACSnapshotForTest(snap)
	BuildBindingsByGVRIndex(navigated)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Mutators.
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				b := pool[i%poolSize]
				switch i % 4 {
				case 0:
					onBindingAdd(b)
				case 1:
					onBindingUpdate(b, b)
				case 2:
					onBindingDelete(b)
				case 3:
					onRoleObjectChanged(bumpSrcRoleUpdate, adminCR)
				}
				i++
			}
		}(w)
	}

	// Enumerators.
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = EnumeratePrewarmTargetsForGVR(compGVR, "list")
				_ = EnumeratePrewarmTargetsForGVR(panelGVR, "list")
			}
		}()
	}

	// One rebuilder.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			BuildBindingsByGVRIndex(navigated)
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	// Reaching here without a -race report or panic is the pass.
}
