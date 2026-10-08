// phase1_test.go — 0.30.102 Tag B unit + falsifier tests for the
// cache-package side of Phase 1 (the seed budget, the readiness gate,
// the sync barrier) and the CRD-watch (group extraction + membership).
//
// The end-to-end no-hardcode falsifier (orphan RESTAction never resolved)
// and the premature-Ready falsifier live in
// internal/handlers/dispatchers/phase1_walk_test.go where the resolution
// walk is reachable. These tests pin the primitives that walk relies on.

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// --- Seed-budget falsifier -------------------------------------------------

// forbiddenSeedResources are resource names that must NEVER appear in a
// Tag B meta-query seed. feedback_no_special_cases.md is a HARD
// requirement: every business GVR (compositions, panels, the concrete
// widget resources) is discovered by resolution, never named in code.
//
// #483 — this name list is a BACKSTOP, not the guard. It only catches
// resources someone thought to enumerate here, and a re-added seed under
// a local alias defeats it entirely. The real guard is
// TestMetaQuerySeeds_IsDerivedFromItsTwoSanctionedSources below, which
// derives the permitted set instead of matching literals.
var forbiddenSeedResources = map[string]bool{
	"compositions":  true,
	"panels":        true,
	"routesloaders": true,
	"navmenus":      true,
}

// TestMetaQuerySeeds_IsDerivedFromItsTwoSanctionedSources is the #483
// structural guard on the seed budget.
//
// It does NOT compare against a hand-written list of GVR literals. It
// RECOMPUTES the permitted set from the only two declarations that carry
// a chicken-and-egg claim on the walk — restActionGVR (inventory.go) and
// RBACResourceTypes (watcher.go) — and requires exact set equality with
// MetaQuerySeeds(). Consequences, which are the point:
//
//   - re-adding `navmenus` / `routesloaders` fails here WHATEVER the new
//     variable is called, because the test never looks at names; a
//     literal-matching guard is defeated by a local alias, and most of
//     this repo's guards are name-matching, so this one is deliberately
//     not.
//   - adding any OTHER seed fails too, so the guard does not have to
//     anticipate what gets added next.
//   - widening the set legitimately means widening its SOURCES, which is
//     a visible change to a sanctioned declaration rather than one more
//     line in a slice.
//
// It also pins the count, so a source that silently loses an entry is
// caught rather than quietly shrinking the budget to match.
func TestMetaQuerySeeds_IsDerivedFromItsTwoSanctionedSources(t *testing.T) {
	seeds := cache.MetaQuerySeeds()

	// Derive, not enumerate. restactions is reconstructed from the SAME
	// group/version/resource inventory.go declares; RBACResourceTypes is read
	// straight from the exported declaration.
	want := map[schema.GroupVersionResource]bool{
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}: true,
	}
	for _, g := range cache.RBACResourceTypes {
		want[g] = true
	}

	if len(seeds) != len(want) {
		t.Fatalf("#483: meta-query seed budget must be EXACTLY its two sanctioned sources "+
			"(restactions + the %d RBACResourceTypes = %d); got %d: %v",
			len(cache.RBACResourceTypes), len(want), len(seeds), seeds)
	}

	seen := map[schema.GroupVersionResource]bool{}
	for _, s := range seeds {
		if !want[s] {
			t.Errorf("#483: %v is a hardcoded meta-query seed but is NOT derivable from "+
				"restActionGVR or RBACResourceTypes. Every seed must come from a source with a "+
				"chicken-and-egg claim on the walk. `routesloaders`/`navmenus` were removed "+
				"because they had none — the walk gets its roots from the frontend ConfigMap "+
				"(dispatchers/phase1_roots.go), so a navigation kind is NOT a precondition for it. "+
				"Renaming the variable does not make this pass.", s)
		}
		if seen[s] {
			t.Errorf("#483: duplicate meta-query seed %v — each seed costs an informer", s)
		}
		seen[s] = true
		// Backstop (see forbiddenSeedResources): a named business/legacy GVR.
		if forbiddenSeedResources[s.Resource] {
			t.Errorf("business or removed-legacy GVR %v must NOT be a hardcoded seed", s)
		}
	}
	for g := range want {
		if !seen[g] {
			t.Errorf("#483: sanctioned anchor %v is MISSING from the seed set — the seeds must be "+
				"the full union of their sources, not a subset", g)
		}
	}

	// The legacy navigation kinds specifically, by group: the routing refactor
	// emptied widgets.templates.krateo.io of nav roots, and no seed may anchor
	// an informer there again. This complements the derivation check by naming
	// the blast radius in the failure message.
	for _, s := range seeds {
		if s.Group == "widgets.templates.krateo.io" {
			t.Errorf("#483: %v seeds an informer in the legacy navigation group "+
				"widgets.templates.krateo.io. Both nav kinds were removed in the routing refactor "+
				"(no CRD, zero objects); seeding one registers an informer for a type the "+
				"apiserver does not serve — a permanent reconnect loop and a readiness tax.", s)
		}
	}

	// Ship 0 / 0.30.222 + Ship 0.5 (v6) — the CRD GVR is explicitly
	// walker-driven (under v6 the CRD informer no longer exists at all;
	// composition GVRs come from one-shot apiserver discovery via
	// cache.DiscoverGroupResources). Asserting its ABSENCE from the
	// seed set catches a regression that re-adds it.
	crdGVR := schema.GroupVersionResource{
		Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
	}
	for _, s := range seeds {
		if s == crdGVR {
			t.Fatalf("Ship 0 invariant: customresourcedefinitions MUST NOT be a meta-query seed " +
				"(walker-driven via AddNavigationDiscoveredGroup); got %v in seed set", s)
		}
	}
}

// #483: TestRoutesLoadersGVR_IsV1Beta1 and TestNavMenusGVR_IsV1Beta1 were
// DELETED with the accessors they pinned. Both asserted that a seed GVR held a
// specific group/version/resource — a contract that ends when the seed does.
// The invariant that replaced them is not "these two GVRs have these values"
// but "no seed anchors an informer for a removed navigation kind", which
// TestMetaQuerySeeds_IsDerivedFromItsTwoSanctionedSources enforces by
// derivation and which no local alias can slip past.

// --- PrewarmEnabled gate (#57 fold: implicit-on-cache) ---------------------

// TestPrewarmEnabled_TracksCacheEnabled is the #57 fold truth-table.
// PrewarmEnabled() was folded to implicit-on-cache: it returns
// !Disabled() (the standalone PREWARM_ENABLED env flag was retired).
// Prewarm is therefore on iff the cache subsystem is on.
//
// The last two rows assert the FOLD: a stale PREWARM_ENABLED in the
// environment is IGNORED — prewarm tracks CACHE_ENABLED regardless of the
// retired flag's value (a stale "false" no longer suppresses prewarm when
// cache is on; main.go's retired-flag audit warns once on it).
func TestPrewarmEnabled_TracksCacheEnabled(t *testing.T) {
	cases := []struct {
		name      string
		cacheEnv  string
		staleFlag string
		setStale  bool
		want      bool
	}{
		{name: "cache_on", cacheEnv: "true", want: true},
		{name: "cache_off_false", cacheEnv: "false", want: false},
		{name: "cache_off_unset", cacheEnv: "", want: false},
		{name: "cache_on_stale_prewarm_false_ignored", cacheEnv: "true", staleFlag: "false", setStale: true, want: true},
		{name: "cache_off_stale_prewarm_true_ignored", cacheEnv: "false", staleFlag: "true", setStale: true, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", tc.cacheEnv)
			if tc.setStale {
				t.Setenv("PREWARM_ENABLED", tc.staleFlag)
			}
			if got := cache.PrewarmEnabled(); got != tc.want {
				t.Fatalf("PrewarmEnabled() with CACHE_ENABLED=%q stale PREWARM_ENABLED=%q: want %v; got %v",
					tc.cacheEnv, tc.staleFlag, tc.want, got)
			}
		})
	}
}

// --- Phase1Done readiness gate (premature-Ready falsifier primitive) -------

// TestPhase1Done_GateTransition asserts the gate is set-once and that
// IsPhase1Done reflects MarkPhase1Done. The /readyz handler test (in the
// handlers package) drives the HTTP status off this same signal.
func TestPhase1Done_GateTransition(t *testing.T) {
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)

	if cache.IsPhase1Done() {
		t.Fatalf("premature-Ready: Phase1Done must be false before MarkPhase1Done")
	}
	cache.MarkPhase1Done()
	if !cache.IsPhase1Done() {
		t.Fatalf("Phase1Done must be true after MarkPhase1Done")
	}
	// Idempotent.
	cache.MarkPhase1Done()
	if !cache.IsPhase1Done() {
		t.Fatalf("Phase1Done must stay true after a second MarkPhase1Done")
	}
}

// --- CRD-watch: group extraction ------------------------------------------

// TestExtractAPIServerGroupFromTemplatedPath covers the templated-path
// group extraction the Phase 1 walk feeds into the auto-discover set.
func TestExtractAPIServerGroupFromTemplatedPath(t *testing.T) {
	cases := []struct {
		name      string
		path      string
		wantGroup string
		wantOK    bool
	}{
		{
			name:      "fully-static composition path",
			path:      "/apis/composition.krateo.io/v1/namespaces/bench/githubscaffoldingwithcompositionpages",
			wantGroup: "composition.krateo.io",
			wantOK:    true,
		},
		{
			name:      "templated version segment — group still static",
			path:      "/apis/composition.krateo.io/${.spec.version}/namespaces/${.ns}/${.kind}",
			wantGroup: "composition.krateo.io",
			wantOK:    true,
		},
		{
			name:      "templated namespace + resource — group static",
			path:      "/apis/widgets.templates.krateo.io/v1beta1/namespaces/${.ns}/panels",
			wantGroup: "widgets.templates.krateo.io",
			wantOK:    true,
		},
		{
			name:   "core group path — no named group",
			path:   "/api/v1/namespaces/bench/configmaps",
			wantOK: false,
		},
		{
			name:   "templated GROUP segment — cannot key the set",
			path:   "/apis/${.group}/v1/namespaces/x/things",
			wantOK: false,
		},
		{
			name:   "external endpoint — not an apiserver path",
			path:   "https://api.github.com/repos/foo/bar",
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			grp, ok := cache.ExtractAPIServerGroupFromTemplatedPath(tc.path)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (group=%q)", ok, tc.wantOK, grp)
			}
			if ok && grp != tc.wantGroup {
				t.Fatalf("group = %q, want %q", grp, tc.wantGroup)
			}
		})
	}
}

// TestAutoDiscoverGroups_NavigationDerived asserts the auto-discover set
// starts EMPTY and only AddNavigationDiscoveredGroup populates it — no group is
// hardcoded.
func TestAutoDiscoverGroups_NavigationDerived(t *testing.T) {
	cache.ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(cache.ResetNavigationDiscoveredGroupsForTest)

	if got := cache.NavigationDiscoveredGroupsSnapshot(); len(got) != 0 {
		t.Fatalf("auto-discover set must start EMPTY — no hardcoded group; got %v", got)
	}

	// The empty string (core group) is rejected — admitting it would
	// auto-register informers for every core resource.
	cache.AddNavigationDiscoveredGroup("")
	if got := cache.NavigationDiscoveredGroupsSnapshot(); len(got) != 0 {
		t.Fatalf("AddNavigationDiscoveredGroup(\"\") must be rejected; got %v", got)
	}

	cache.AddNavigationDiscoveredGroup("composition.krateo.io")
	if got := cache.NavigationDiscoveredGroupsSnapshot(); len(got) != 1 || got[0] != "composition.krateo.io" {
		t.Fatalf("auto-discover set = %v, want [composition.krateo.io]", got)
	}
	// Idempotent.
	cache.AddNavigationDiscoveredGroup("composition.krateo.io")
	if got := cache.NavigationDiscoveredGroupsSnapshot(); len(got) != 1 {
		t.Fatalf("AddNavigationDiscoveredGroup must be idempotent; got %v", got)
	}
}

// --- WaitAllInformersSynced ------------------------------------------------

// TestWaitAllInformersSynced_NoInformers returns nil immediately when
// nothing is registered (the flag-OFF / no-roots path).
func TestWaitAllInformersSynced_NilWatcher(t *testing.T) {
	var rw *cache.ResourceWatcher
	if err := rw.WaitAllInformersSynced(context.Background()); err != nil {
		t.Fatalf("nil watcher WaitAllInformersSynced must be a no-op nil; got %v", err)
	}
}

// TestRegisterMetaQuerySeeds_RegistersAndIdempotent registers the seeds
// on a real (fake-backed) watcher and asserts the 3 non-RBAC seeds plus
// the RBAC overlap are all registered, and that a second call is a no-op.
func TestRegisterMetaQuerySeeds_RegistersAndIdempotent(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), phase1ListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	first := rw.RegisterMetaQuerySeeds()
	// The 4 RBAC GVRs are already registered by NewResourceWatcher; only the
	// non-RBAC seed (restactions) is new. Ship 0 / 0.30.222 removed
	// customresourcedefinitions (walker-spawned via
	// AddNavigationDiscoveredGroup), dropping the count 4->3; #483 removed
	// routesloaders + navmenus, dropping it 3->1.
	//
	// Derived rather than written as `1`, so this does not have to be revisited
	// when a source legitimately changes: the newly-registered count is
	// whatever the seed set holds beyond the bootstrap-registered RBAC types.
	wantNew := len(cache.MetaQuerySeeds()) - len(cache.RBACResourceTypes)
	if first != wantNew {
		t.Fatalf("first RegisterMetaQuerySeeds must register the %d non-RBAC seed(s) "+
			"(the %d RBAC types are bootstrap-registered by NewResourceWatcher); got %d",
			wantNew, len(cache.RBACResourceTypes), first)
	}
	for _, gvr := range cache.MetaQuerySeeds() {
		if !rw.IsRegistered(gvr) {
			t.Errorf("meta-query seed %v must be registered after RegisterMetaQuerySeeds", gvr)
		}
	}
	// Idempotent — second call registers nothing new.
	if second := rw.RegisterMetaQuerySeeds(); second != 0 {
		t.Fatalf("second RegisterMetaQuerySeeds must be a no-op; registered %d", second)
	}
}

// phase1ListKinds extends rbacListKinds with List-kind registrations for
// the non-RBAC meta-query seed plus the lateGVRs used by the
// re-snapshot-loop test, so the fake dynamic client can serve their
// informers' initial LISTs without panicking. The List-kind names are
// arbitrary — the fake client only needs SOME registered kind.
//
// #483: the routesloaders / navmenus entries went with the seeds. The CRD
// entry is kept deliberately — it is not a seed (Ship 0 removed it) but
// other tests in this package register that GVR directly.
func phase1ListKinds() map[schema.GroupVersionResource]string {
	m := rbacListKinds()
	m[schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}] = "CustomResourceDefinitionList"
	m[schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}] = "RESTActionList"
	for _, g := range lateGVRs() {
		m[g] = g.Resource + "List"
	}
	return m
}

// lateGVRs are customer-style composition GVRs the re-snapshot-loop test
// registers DURING WaitAllInformersSynced — modeling the CRD-watch's
// mid-wait per-GVR EnsureResourceType.
func lateGVRs() []schema.GroupVersionResource {
	out := make([]schema.GroupVersionResource, 0, 12)
	for i := 0; i < 12; i++ {
		out = append(out, schema.GroupVersionResource{
			Group:    "composition.krateo.io",
			Version:  "v1",
			Resource: "latething" + string(rune('a'+i)),
		})
	}
	return out
}

// TestWaitAllInformersSynced_ReSnapshotRace is the architect-flagged
// concurrency falsifier. A CRD-add (here: a late EnsureResourceType) that
// lands AFTER the barrier's snapshot but DURING its wait must NOT be
// missed — a single snapshot+wait would let Phase1Done flip while that
// composition informer is still cold (premature-Ready).
//
// The test registers 12 composition GVRs concurrently while
// WaitAllInformersSynced is running, then asserts the CONTRACT: when the
// barrier returns nil, EVERY registered informer — including every late
// one — reports HasSynced()==true. A single-snapshot implementation
// fails this: the barrier would return with late informers still
// unsynced.
func TestWaitAllInformersSynced_ReSnapshotRace(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), phase1ListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	// Seed the barrier with the meta-query seeds so it has a non-empty
	// starting set.
	rw.RegisterMetaQuerySeeds()

	// Concurrently register the 12 late GVRs while the barrier runs.
	lateDone := make(chan struct{})
	go func() {
		defer close(lateDone)
		for _, g := range lateGVRs() {
			rw.EnsureResourceType(g)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := rw.WaitAllInformersSynced(ctx); err != nil {
		t.Fatalf("WaitAllInformersSynced returned error: %v", err)
	}
	<-lateDone

	// CONTRACT: every registered informer is synced at barrier return.
	for _, g := range lateGVRs() {
		if !rw.IsRegistered(g) {
			t.Fatalf("late GVR %v was not registered — test setup error", g)
		}
		if !rw.IsSynced(g) {
			t.Fatalf("re-snapshot race FAIL: WaitAllInformersSynced returned "+
				"while late GVR %v was still UNSYNCED — Phase1Done would flip "+
				"on a cold composition informer (premature-Ready)", g)
		}
	}
}

// TestWaitAllInformersSynced_CtxBound asserts a cancelled ctx bounds the
// re-snapshot loop — a pathological never-stabilizing cluster (CRDs added
// forever) must not hang Phase 1; the PHASE1_TIMEOUT_SECONDS budget
// (ctx) cuts it off.
func TestWaitAllInformersSynced_CtxBound(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), phase1ListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.RegisterMetaQuerySeeds()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled — the loop must return promptly

	if err := rw.WaitAllInformersSynced(ctx); err == nil {
		t.Fatalf("WaitAllInformersSynced must return a non-nil error on a cancelled ctx")
	}
}

// --- Readiness-gate startup-flip helper (Ship 0.30.153) -------------------

// TestShouldFlipPhase1DoneOnStartup is the AC-3 truth table for the
// startup safety-net. The bug fixed at 0.30.153: the inline conditional
// at main.go missed (cache=off, prewarm=on, watcher-non-nil) — the pod
// was stuck `{"status":"warming","phase1Done":false}` forever because
// neither disjunct of the old 2-term condition fired. The helper here
// is the four-disjunct invariant the conditional was supposed to encode.
//
// The healthy serving path (cache=on, prewarm=on, watcher-non-nil) is
// the ONLY case where the helper returns false — Phase1Warmup owns the
// flip there. Every other tuple has nothing to warm and must flip.
func TestShouldFlipPhase1DoneOnStartup(t *testing.T) {
	tests := []struct {
		name           string
		cacheEnabled   bool
		prewarmEnabled bool
		watcherIsNil   bool
		want           bool
		why            string
	}{
		{
			name:           "incident_repro_cache_off_prewarm_on_watcher_non_nil",
			cacheEnabled:   false,
			prewarmEnabled: true,
			watcherIsNil:   false,
			want:           true,
			why:            "the 0.30.153 incident: passthrough watcher built, nothing to warm, must flip",
		},
		{
			name:           "healthy_serving_path_cache_on_prewarm_on_watcher_non_nil",
			cacheEnabled:   true,
			prewarmEnabled: true,
			watcherIsNil:   false,
			want:           false,
			why:            "Phase1Warmup goroutine owns the flip — do NOT pre-flip or the premature-Ready invariant breaks",
		},
		{
			name:           "cache_off_prewarm_off_watcher_non_nil",
			cacheEnabled:   false,
			prewarmEnabled: false,
			watcherIsNil:   false,
			want:           true,
			why:            "cache off AND prewarm off: clearly nothing to warm",
		},
		{
			name:           "cache_off_prewarm_off_watcher_nil",
			cacheEnabled:   false,
			prewarmEnabled: false,
			watcherIsNil:   true,
			want:           true,
			why:            "all three reasons fire — must flip",
		},
		{
			name:           "cache_off_prewarm_on_watcher_nil",
			cacheEnabled:   false,
			prewarmEnabled: true,
			watcherIsNil:   true,
			want:           true,
			why:            "cache off + watcher missing — nothing to warm",
		},
		{
			name:           "cache_on_prewarm_off_watcher_non_nil",
			cacheEnabled:   true,
			prewarmEnabled: false,
			watcherIsNil:   false,
			want:           true,
			why:            "prewarm disabled (the original main.go condition) — must flip",
		},
		{
			name:           "cache_on_prewarm_off_watcher_nil",
			cacheEnabled:   true,
			prewarmEnabled: false,
			watcherIsNil:   true,
			want:           true,
			why:            "both fallback disjuncts of the original condition true — must flip",
		},
		{
			name:           "cache_on_prewarm_on_watcher_nil",
			cacheEnabled:   true,
			prewarmEnabled: true,
			watcherIsNil:   true,
			want:           true,
			why:            "watcher construction failed (the original main.go condition) — nothing to warm",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := cache.ShouldFlipPhase1DoneOnStartup(
				tc.cacheEnabled, tc.prewarmEnabled, tc.watcherIsNil,
			)
			if got != tc.want {
				t.Fatalf("ShouldFlipPhase1DoneOnStartup(cache=%v, prewarm=%v, watcherNil=%v) = %v, want %v\n  why: %s",
					tc.cacheEnabled, tc.prewarmEnabled, tc.watcherIsNil, got, tc.want, tc.why)
			}
		})
	}
}

// TestShouldFlipPhase1DoneOnStartup_HealthyPathIsTheOnlyFalse is a
// targeted falsifier: the ONLY tuple of the 8-case truth table that
// must return false is (cache=on, prewarm=on, watcher-non-nil). Any
// future regression that adds a new false case (e.g. accidentally
// removing the !cacheEnabled disjunct, re-introducing the 0.30.153
// incident) trips here in addition to the broader truth table above.
func TestShouldFlipPhase1DoneOnStartup_HealthyPathIsTheOnlyFalse(t *testing.T) {
	falseCount := 0
	for _, cacheEnabled := range []bool{false, true} {
		for _, prewarmEnabled := range []bool{false, true} {
			for _, watcherIsNil := range []bool{false, true} {
				if !cache.ShouldFlipPhase1DoneOnStartup(cacheEnabled, prewarmEnabled, watcherIsNil) {
					falseCount++
					if !(cacheEnabled && prewarmEnabled && !watcherIsNil) {
						t.Fatalf("unexpected false case: cache=%v prewarm=%v watcherNil=%v — only the healthy serving path may return false",
							cacheEnabled, prewarmEnabled, watcherIsNil)
					}
				}
			}
		}
	}
	if falseCount != 1 {
		t.Fatalf("exactly one of 8 tuples must return false (the healthy serving path); got %d", falseCount)
	}
}
