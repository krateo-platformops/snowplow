package dispatchers

// issue643_prb2_walk_seams_test.go — #643 PR-B2 arms that read the NEW driver
// label / walk counters or swap a walk-skip rule out (so they are RED on main by
// construction — the label and the rules do not exist there). The arms that
// only need APIs main already has are in issue643_prb2_walk_test.go.
//
//   W1d    W1's fixture read through external_fetch_total{driver}: after the
//          first walk {prewarm-engine-boot} and {cohort-seed} move by 0; the
//          walk positive control moves.
//   W2     the differential: harvester snapshots, recordWalkChildren coverage
//          and the warm L1 (keys + sha256) are IDENTICAL with the walk skip on
//          and forced off; descendants (static ref, template ref, grandchild)
//          are walked every pass and seeded.
//   W2-m   the replay removed → the differential detects the missing subtree.
//   W3-m   the widget uid+RV fold removed → a resourcesRefs edit replays the
//          stale children and the new child is never walked.
//   Cc-iii ruling (c) leaf check removed → an errored widget WITH refs is
//          marked and its child is never discovered.
//   W4     ruling (a): the content prewarm (and, since PR-B2, the phase1 walk)
//          count under driver=prewarm-path during the one pre-Ready pass; the
//          engine passes after it move prewarm-path by 0.
//   W5     attribution: walk, seed, content prewarm, a /call-shaped resolve and
//          a refresher re-resolve each land in exactly their own driver cell;
//          every closed cell is present before any fetch.
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func b2DriverSnap() map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range restactionsapi.ExternalFetchByDriverSnapshot() {
		out[c.Driver] = c.Count
	}
	return out
}

func b2DriverDelta(before map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for d, v := range b2DriverSnap() {
		out[d] = v - before[d]
	}
	return out
}

// ── W1d ────────────────────────────────────────────────────────────────────

func TestIssue643_PRB2_W1d_DriverCellsAfterTheFirstWalk(t *testing.T) {
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-a", "ext-b", "int-a"}, "").
		panel("ext-a", "ext-a-ra", true, nil, "").
		panel("ext-b", "ext-b-ra", true, nil, "").
		panel("int-a", "int-a-ra", false, nil, "")
	env := b2Setup(t, h643K, "b2-root", tree)

	d0 := b2DriverSnap()
	env.pass(prewarmScope{kind: scopeKindBoot})
	first := b2DriverDelta(d0)
	if first[cache.BackgroundOriginPrewarmEngineBoot] != 4 {
		t.Fatalf("precondition: the first engine walk dialled %d under driver=prewarm-engine-boot, want 4 (2 widgets × 2 stages)",
			first[cache.BackgroundOriginPrewarmEngineBoot])
	}
	if first[cache.BackgroundOriginCohortSeed] == 0 {
		t.Fatal("precondition: the boot seed dialled nothing under driver=cohort-seed")
	}

	skip0 := WalkSkipDeclinedExternalTotal()
	d1 := b2DriverSnap()
	for _, s := range []prewarmScope{{kind: scopeKindGVRDiscovered}, {kind: scopeKindGVRDiscovered}, {kind: scopeKindBoot}} {
		env.pass(s)
	}
	after := b2DriverDelta(d1)
	for d, v := range after {
		if v != 0 {
			t.Errorf("W1d: external_fetch_total{driver=%s} moved by %d across the passes after the first walk, want 0", d, v)
		}
	}
	if got := WalkSkipDeclinedExternalTotal() - skip0; got != 2*3 {
		t.Fatalf("W1d: walk_skip_declined_external_total moved by %d over 3 walks, want 6 (2 external widgets per walk) — the positive control", got)
	}
}

// ── W2 / W2-m ──────────────────────────────────────────────────────────────

// b2W2Tree: an external widget whose children come from a static resourcesRefs
// item (desc-s, which has its own child desc-s2) and from its
// resourcesRefsTemplate (desc-t).
func b2W2Tree() *b2Tree {
	return (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-p", "int-a"}, "").
		panel("ext-p", "ext-p-ra", true, []string{"desc-s"}, "desc-t").
		panel("desc-s", "desc-s-ra", false, []string{"desc-s2"}, "").
		panel("desc-s2", "desc-s2-ra", false, nil, "").
		panel("desc-t", "desc-t-ra", false, nil, "").
		panel("int-a", "int-a-ra", false, nil, "")
}

var b2W2Names = []string{"b2-root", "ext-p", "desc-s", "desc-s2", "desc-t", "int-a"}

type b2W2Run struct {
	nav      []string
	apiRefs  []string
	coverage map[string]phase1WalkChildrenEntry
	l1       map[string]string
	skips    uint64
	descSeed int
}

func b2W2Sequence(t *testing.T, name string, skipOn bool, mutate func()) b2W2Run {
	t.Helper()
	var out b2W2Run
	if !t.Run(name, func(t *testing.T) {
		env := b2Setup(t, 2, "b2-root", b2W2Tree())
		prevOn := walkDeclinedExternalSkipEnabled
		walkDeclinedExternalSkipEnabled = skipOn
		t.Cleanup(func() { walkDeclinedExternalSkipEnabled = prevOn })
		if mutate != nil {
			mutate()
		}
		skip0 := WalkSkipDeclinedExternalTotal()
		for _, s := range []prewarmScope{{kind: scopeKindBoot}, {kind: scopeKindGVRDiscovered}, {kind: scopeKindGVRDiscovered}} {
			env.pass(s)
		}
		out.skips = WalkSkipDeclinedExternalTotal() - skip0
		for _, e := range env.deps.navHarv.snapshot() {
			out.nav = append(out.nav, e.W.GetName()+"|"+e.GVR.String())
		}
		sort.Strings(out.nav)
		for _, r := range env.deps.harvester.snapshot() {
			out.apiRefs = append(out.apiRefs, r.Namespace+"/"+r.Name)
		}
		sort.Strings(out.apiRefs)
		out.coverage = map[string]phase1WalkChildrenEntry{}
		for _, n := range b2W2Names {
			if v, ok := phase1WalkChildren.Load(h643WidgetGVR.String() + "|" + h643NS + "|" + n); ok {
				out.coverage[n] = *v.(*atomic.Pointer[phase1WalkChildrenEntry]).Load()
			}
		}
		out.l1 = h643L1Snapshot()
		for _, n := range []string{"desc-s", "desc-s2", "desc-t"} {
			for _, k := range h643WidgetKeysFor(n) {
				if e, ok := cache.ResolvedCache().GetNoTouch(k); ok && e.Inputs != nil && e.Inputs.CacheEntryClass == "widgets" {
					out.descSeed++
				}
			}
		}
	}) {
		t.FailNow()
	}
	return out
}

func TestIssue643_PRB2_W2_DiscoveryReplayDifferential(t *testing.T) {
	on := b2W2Sequence(t, "skip-on", true, nil)
	off := b2W2Sequence(t, "skip-forced-off", false, nil)

	if on.skips == 0 {
		t.Fatal("W2: the walk skip never fired in the skip-on run — the differential compares two identical code paths")
	}
	if off.skips != 0 {
		t.Fatalf("W2: the walk skip fired %d times with the consult forced off", off.skips)
	}
	if !reflect.DeepEqual(on.nav, off.nav) {
		t.Errorf("W2: nav-widget harvest differs:\n on=%v\noff=%v", on.nav, off.nav)
	}
	if !reflect.DeepEqual(on.apiRefs, off.apiRefs) {
		t.Errorf("W2: apiRef harvest differs:\n on=%v\noff=%v", on.apiRefs, off.apiRefs)
	}
	if !reflect.DeepEqual(on.coverage, off.coverage) {
		t.Errorf("W2: recordWalkChildren coverage differs:\n on=%+v\noff=%+v", on.coverage, off.coverage)
	}
	if d := h643L1Diff(on.l1, off.l1); len(d) != 0 {
		t.Errorf("W2: the warm L1 (keys + sha256) differs with the walk skip on vs off: %v", d)
	}
	for _, n := range b2W2Names {
		if got := on.coverage[n].Observations; got != 3 {
			t.Errorf("W2: %s walked %d times over 3 passes with the skip on, want 3", n, got)
		}
	}
	if on.descSeed != 3*2 {
		t.Errorf("W2: %d descendant widget cells seeded, want 6 (3 descendants × 2 cohorts)", on.descSeed)
	}
}

func TestIssue643_PRB2_W2m_ReplayRemoved_DescendantsMissing(t *testing.T) {
	off := b2W2Sequence(t, "skip-forced-off", false, nil)
	mut := b2W2Sequence(t, "mutant-no-replay", true, func() {
		prev := walkReplayChildrenFn
		walkReplayChildrenFn = func([]navChildRef) []navChildRef { return nil }
		t.Cleanup(func() { walkReplayChildrenFn = prev })
	})
	if reflect.DeepEqual(mut.coverage, off.coverage) {
		t.Fatal("W2-m: with the replay removed the walk coverage still matches the no-skip run — the W2 differential cannot see a lost subtree")
	}
	for _, n := range []string{"desc-s", "desc-s2", "desc-t"} {
		if mut.coverage[n].Observations >= off.coverage[n].Observations {
			t.Errorf("W2-m: %s walked %d times without the replay (no-skip run: %d) — expected the subtree to go missing",
				n, mut.coverage[n].Observations, off.coverage[n].Observations)
		}
	}
}

// ── W3-m ───────────────────────────────────────────────────────────────────

func TestIssue643_PRB2_W3m_WidgetVersionFoldRemoved_StaleChildrenReplayed(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-p"}, "").
		panel("ext-p", "ext-p-ra", true, []string{"kid-1"}, "").
		panel("kid-1", "kid-1-ra", false, nil, "").
		panel("kid-2", "kid-2-ra", false, nil, "")
	env := b2Setup(t, 1, "b2-root", tree)
	prev := walkWidgetVersionHoldsFn
	walkWidgetVersionHoldsFn = func(*cache.DeclinedExternalMark, *unstructured.Unstructured) bool { return true }
	t.Cleanup(func() { walkWidgetVersionHoldsFn = prev })

	env.pass(prewarmScope{kind: scopeKindBoot})
	b2Update(t, env, h643WidgetGVR, "ext-p", func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, map[string]any{"items": []any{b2Ref("kid-1"), b2Ref("kid-2")}}, "spec", "resourcesRefs")
	})
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	if b2WalkObs("kid-2") != 0 {
		t.Fatal("W3-m: with the widget uid+RV fold removed kid-2 was still walked — W3a is not guarding the fold")
	}
}

// ── ruling (c)-iii ─────────────────────────────────────────────────────────

func TestIssue643_PRB2_Cciii_LeafCheckRemoved_ChildNeverDiscovered(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-par"}, "").
		panel("ext-par", "ext-par-ra", true, []string{"par-kid"}, "").
		panel("par-kid", "par-kid-ra", false, nil, "")
	b2Erroring(tree, "ext-par")
	env := b2Setup(t, 1, "b2-root", tree)
	prev := walkErroredMarkableFn
	walkErroredMarkableFn = func(*unstructured.Unstructured) bool { return true }
	t.Cleanup(func() { walkErroredMarkableFn = prev })

	env.pass(prewarmScope{kind: scopeKindBoot})
	b2StringRows(env, true)
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	if b2WalkObs("par-kid") != 0 {
		t.Fatal("Cc-iii: with the leaf check removed par-kid was still discovered — Cc-ii is not guarding the leaf rule")
	}
}

// The unmarkable counter moves on the real (unmutated) path.
func TestIssue643_PRB2_Ccii_UnmarkableCounterMoves(t *testing.T) {
	b2StubSeeds(t)
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-par"}, "").
		panel("ext-par", "ext-par-ra", true, []string{"par-kid"}, "").
		panel("par-kid", "par-kid-ra", false, nil, "")
	b2Erroring(tree, "ext-par")
	env := b2Setup(t, 1, "b2-root", tree)
	u0 := WalkErroredUnmarkableTotal()
	env.pass(prewarmScope{kind: scopeKindBoot})
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	if got := WalkErroredUnmarkableTotal() - u0; got != 2 {
		t.Fatalf("walk_errored_unmarkable_total moved by %d over 2 walks of an errored widget with refs, want 2", got)
	}
	if cache.EngineDeclinedExternalSet().Marked(b2WCKey("ext-par")) {
		t.Fatal("an errored widget that declares refs was marked")
	}
}

// ── W4 ─────────────────────────────────────────────────────────────────────

func TestIssue643_PRB2_W4_PrewarmPathMovesOnlyInThePreReadyPass(t *testing.T) {
	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-a"}, "").
		panel("ext-a", "ext-a-ra", true, nil, "")
	env := b2Setup(t, 2, "b2-root", tree)
	roots, err := env.deps.lister(context.Background())
	if err != nil {
		t.Fatalf("lister: %v", err)
	}

	// The pre-Ready half, as phase1WarmupWith runs it: the phase1 walk, then
	// the one content-prewarm pass over what it harvested.
	d0 := b2DriverSnap()
	if err := resolveNavigationRoot(context.Background(), roots[0].Root, roots[0].GVR, env.deps.saEP, env.deps.saRC,
		env.deps.authnNS, env.deps.harvester, env.deps.navHarv, nil); err != nil {
		t.Fatalf("phase1 walk: %v", err)
	}
	walk := b2DriverDelta(d0)
	if walk[cache.RefDenialOriginPrewarmPath] != 2 || walk[cache.RefDenialOriginServe] != 0 {
		t.Fatalf("W4: the phase1 walk's dials attributed %v, want prewarm-path=2 and serve=0 (it is a prewarm root, not a customer)", walk)
	}
	d1 := b2DriverSnap()
	runContentPrewarmPass(context.Background(), env.deps.harvester, env.deps.saEP, env.deps.saRC, env.deps.authnNS)
	content := b2DriverDelta(d1)
	if content[cache.RefDenialOriginPrewarmPath] != 2 {
		t.Fatalf("W4: the content-prewarm pass dialled %v, want prewarm-path=2 (ext-a-ra's two stages)", content)
	}

	// The engine passes after Ready: prewarm-path does not move.
	d2 := b2DriverSnap()
	for _, s := range []prewarmScope{{kind: scopeKindBoot}, {kind: scopeKindGVRDiscovered}, {kind: scopeKindKeepwarm}} {
		env.pass(s)
	}
	if got := b2DriverDelta(d2)[cache.RefDenialOriginPrewarmPath]; got != 0 {
		t.Fatalf("W4: driver=prewarm-path moved by %d across engine passes — the content prewarm runs once, pre-Ready", got)
	}
}

// ── W5 ─────────────────────────────────────────────────────────────────────

func TestIssue643_PRB2_W5_EachDriverLandsInItsOwnCell(t *testing.T) {
	for _, d := range cache.RefDenialOrigins {
		found := false
		for _, c := range restactionsapi.ExternalFetchByDriverSnapshot() {
			if c.Driver == d {
				found = true
			}
		}
		if !found {
			t.Fatalf("W5: driver cell %q absent before any fetch — every closed cell must be pre-populated", d)
		}
	}

	tree := (&b2Tree{}).
		panel("b2-root", "b2-root-ra", false, []string{"ext-a"}, "").
		panel("ext-a", "ext-a-ra", true, nil, "")
	env := b2Setup(t, 1, "b2-root", tree)
	only := func(what, driver string, n uint64, before map[string]uint64) {
		t.Helper()
		for d, v := range b2DriverDelta(before) {
			want := uint64(0)
			if d == driver {
				want = n
			}
			if v != want {
				t.Errorf("W5 %s: external_fetch_total{driver=%s} moved by %d, want %d", what, d, v, want)
			}
		}
	}

	// walk only (seeds stubbed): prewarm-engine-boot.
	pw, pr := seedOneWidgetFn, seedOneRestactionFn
	b2StubSeeds(t)
	d := b2DriverSnap()
	env.pass(prewarmScope{kind: scopeKindBoot})
	only("walk", cache.BackgroundOriginPrewarmEngineBoot, 2, d)

	// seed only (the walk now replays): cohort-seed.
	seedOneWidgetFn, seedOneRestactionFn = pw, pr
	d = b2DriverSnap()
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	if got := b2DriverDelta(d)[cache.BackgroundOriginCohortSeed]; got == 0 {
		t.Error("W5 seed: driver=cohort-seed did not move on the first seed pass")
	}
	only("seed", cache.BackgroundOriginCohortSeed, b2DriverDelta(d)[cache.BackgroundOriginCohortSeed], d)

	// content prewarm: prewarm-path.
	d = b2DriverSnap()
	runContentPrewarmPass(context.Background(), env.deps.harvester, env.deps.saEP, env.deps.saRC, env.deps.authnNS)
	only("content prewarm", cache.RefDenialOriginPrewarmPath, 2, d)

	// a /call-shaped resolve: no background marker → serve.
	callCtx := withSAIdentityContext(context.Background(), env.deps.saEP, env.deps.saRC)
	d = b2DriverSnap()
	if _, err := apiref.Resolve(callCtx, apiref.ResolveOptions{RC: env.deps.saRC, ApiRef: b2ApiRef("ext-a-ra"),
		AuthnNS: env.deps.authnNS, PerPage: -1, Page: -1}); err != nil {
		t.Fatalf("W5 /call resolve: %v", err)
	}
	only("/call", cache.RefDenialOriginServe, 2, d)

	// a refresher re-resolve, through the production resolveOnceProd.
	d = b2DriverSnap()
	if _, err := resolveOnceProd(callCtx, cache.ResolvedKeyInputs{CacheEntryClass: "restactions",
		Group: h643RAGVR.Group, Version: h643RAGVR.Version, Resource: h643RAGVR.Resource,
		Namespace: h643NS, Name: "ext-a-ra", PerPage: -1, Page: -1}); err != nil {
		t.Fatalf("W5 refresher resolve: %v", err)
	}
	only("refresher", cache.BackgroundOriginRefresher, 2, d)
}

// b2ApiRef is the apiRef a fixture panel declares for RESTAction ra.
func b2ApiRef(ra string) templatesv1.ObjectReference {
	r, _ := widgets.GetApiRef(h643Widget("x", ra).Object)
	return r
}
