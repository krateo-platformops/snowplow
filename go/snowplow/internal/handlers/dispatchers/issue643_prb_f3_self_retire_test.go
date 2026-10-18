package dispatchers

// issue643_prb_f3_self_retire_test.go — #643 PR-B F3: a declined-external mark
// retires itself the moment the fact it recorded stops holding (I-1).
//
//   F3a definition — the RESTAction is edited so it dispatches internally: the
//                    next background pass resolves AND Puts it.
//   F3b Put        — an L1 Put of the key retires the mark; an ExternalTTL Put
//                    (the /call opt-in cache of an external result) does NOT,
//                    and the next background pass still dials 0.
//   F3c GONE       — deleting the RESTAction drops its marks.
//   F3d data       — an iterator stage over an internal list makes one external
//                    call per item: marked while the list has items; once the
//                    list is emptied the next background pass Puts the cell with
//                    0 external dials. Mutation arm: without the deps-moved
//                    check the mark survives and the cell stays cold.
//
// All on the shared #643 harness. Not t.Parallel(): reassigns package vars.

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// h643MarksFor returns the held mark keys whose widget is wName.
func h643MarksFor(wName string) []string {
	var out []string
	for k, m := range cache.EngineDeclinedExternalSet().MarksForTest() {
		if m.Widget.Name == wName {
			out = append(out, k)
		}
	}
	return out
}

func h643WaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// h643CohortCtx is a seed cohort ctx for informer reads under a cohort identity.
func h643CohortCtx(env *h643Env) context.Context {
	return withCohortSeedContext(context.Background(), seedTarget{Groups: []string{"h643-g0"}}, env.deps.saEP, env.deps.saRC)
}

func h643RAVersion(env *h643Env, ra string) string {
	u, ok := readRAForMark(h643CohortCtx(env), raObjectRef(h643NS, ra))
	if !ok {
		return ""
	}
	return u.GetResourceVersion()
}

func h643BootAndMark(t *testing.T, env *h643Env, wName string) []string {
	t.Helper()
	h643Budget(t, nil)
	env.pass(prewarmScope{kind: scopeKindBoot})
	keys := h643MarksFor(wName)
	if len(keys) != h643K {
		t.Fatalf("precondition: %d marks for %s after boot, want %d (one per cohort)", len(keys), wName, h643K)
	}
	return keys
}

func TestIssue643_F3a_RAEditedToInternal_NextBackgroundPassPuts(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, internal: h643N, things: 2})
	h643BootAndMark(t, env, "ext-w0")

	// Edit ext-ra0 so its stages hit an apiserver path (internal dispatch).
	u, err := env.dyn.Resource(h643RAGVR).Namespace(h643NS).Get(context.Background(), "ext-ra0", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ext-ra0: %v", err)
	}
	internalPath := "/apis/" + h643ThingsGVR.Group + "/" + h643ThingsGVR.Version + "/namespaces/" + h643NS + "/" + h643ThingsGVR.Resource
	_ = unstructured.SetNestedSlice(u.Object, []any{h643ExtStage("s0", internalPath)}, "spec", "api")
	u.SetResourceVersion("2")
	if _, err := env.dyn.Resource(h643RAGVR).Namespace(h643NS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update ext-ra0: %v", err)
	}
	h643WaitFor(t, "the informer to serve ext-ra0 at RV 2", func() bool { return h643RAVersion(env, "ext-ra0") == "2" })

	d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR})
	if d != 0 {
		t.Fatalf("gvr-discovered dialled %d: ext-ra0 is internal now and ext-ra1 is still marked — want 0 (dials=%v)", d, env.ep.byPath())
	}
	if got := len(h643WidgetKeysFor("ext-w0")); got != h643K {
		t.Fatalf("F3a: after its RESTAction became internal, ext-w0 must be Put for every cohort by the next background pass; got %d of %d (a stale mark left it cold — I-1)", got, h643K)
	}
	if got := len(h643MarksFor("ext-w0")); got != 0 {
		t.Fatalf("F3a: %d ext-w0 marks survived the RA edit", got)
	}
	if got := len(h643MarksFor("ext-w1")); got != h643K {
		t.Fatalf("F3a: the untouched ext-w1 must stay marked (%d of %d)", got, h643K)
	}
}

func TestIssue643_F3b_PutRetiresTheMark_ExternalTTLPutDoesNot(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, internal: h643N})
	keys := h643BootAndMark(t, env, "ext-w0")
	set := cache.EngineDeclinedExternalSet()
	store := cache.ResolvedCache()
	inputs := &cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Group: h643WidgetGVR.Group, Version: h643WidgetGVR.Version,
		Resource: h643WidgetGVR.Resource, Namespace: h643NS, Name: "ext-w0"}

	// An ExternalTTL Put — the /call path's opt-in cache of an EXTERNAL result
	// (widgets.go, krateo.io/external-cache-ttl-seconds) — proves nothing about
	// cacheability: the mark stays and the next background pass dials 0.
	store.Put(keys[0], &cache.ResolvedEntry{RawJSON: []byte(`{"ext":true}`), Inputs: inputs, ExternalTTL: true})
	if !set.Marked(keys[0]) {
		t.Fatal("F3b: an ExternalTTL Put retired the mark — that cell still touches external")
	}
	store.DeleteForTest(keys[0]) // the bounded-TTL cell expires; a background pass must still skip
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR}); d != 0 {
		t.Fatalf("F3b: after an ExternalTTL Put the next background pass dialled %d, want 0", d)
	}

	// A plain Put proves the key cacheable: the mark retires (store-side hook).
	store.Put(keys[1], &cache.ResolvedEntry{RawJSON: []byte(`{"cacheable":true}`), Inputs: inputs})
	if set.Marked(keys[1]) {
		t.Fatal("F3b: a plain L1 Put of the key did not retire its declined-external mark (retire rule a)")
	}
	if !set.Marked(keys[2]) {
		t.Fatal("F3b: a Put of one key retired another key's mark")
	}
}

func TestIssue643_F3c_DeletedRESTActionDropsItsMarks(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, external: h643M, internal: h643N})
	h643BootAndMark(t, env, "ext-w0")
	if err := env.dyn.Resource(h643RAGVR).Namespace(h643NS).Delete(context.Background(), "ext-ra0", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete ext-ra0: %v", err)
	}
	h643WaitFor(t, "the GONE verdict to drop ext-w0's marks", func() bool { return len(h643MarksFor("ext-w0")) == 0 })
	if got := len(h643MarksFor("ext-w1")); got != h643K {
		t.Fatalf("F3c: deleting ext-ra0 dropped marks of ext-w1 (%d of %d left)", got, h643K)
	}
}

// h643IterExtra is the F3d unit: s0 LISTs the internal things, s1 makes one
// external call per item.
var h643IterExtra = h643Extra{
	widget: "iter-w", ra: "iter-ra", external: true,
	api: []any{
		map[string]any{"name": "s0", "verb": "GET",
			"path": "/apis/" + h643ThingsGVR.Group + "/" + h643ThingsGVR.Version + "/namespaces/" + h643NS + "/" + h643ThingsGVR.Resource},
		func() map[string]any {
			st := h643ExtStage("s1", `${ "/ext/fast/iter/" + .metadata.name }`)
			st["dependsOn"] = map[string]any{"name": "s0", "iterator": ".s0.items"}
			return st
		}(),
	},
}

// h643F3dCell finds the apistage content cell holding the h643things LIST,
// straight from the store (independent of whether any mark noted it, so the
// note-removal mutation runs to its end instead of stopping here).
func h643F3dCell(t *testing.T) (key string, inputs cache.ResolvedKeyInputs) {
	t.Helper()
	store := cache.ResolvedCache()
	for _, k := range store.KeysForTest() {
		e, ok := store.GetNoTouch(k)
		if ok && e != nil && e.Inputs != nil && e.Inputs.CacheEntryClass == cache.CacheEntryClassApistage &&
			e.Inputs.Resource == h643ThingsGVR.Resource && e.Inputs.Name == "" {
			return k, *e.Inputs
		}
	}
	t.Fatal("precondition: no apistage content cell holds the h643things list")
	return "", cache.ResolvedKeyInputs{}
}

type h643F3dHow int

const (
	h643F3dRefresh h643F3dHow = iota // the real refresher re-Put of the list cell
	h643F3dEvict                     // the cell evicted (a refresh drop / TTL)
)

// h643F3d: boot marks iter-w (2 external dials per cohort); the list is
// emptied; pass A and pass B run while the list cell is still the stale one;
// then the cell is refreshed (or evicted); pass C must Put iter-w with 0 dials.
// Returns pass B's and pass C's dials and how many iter-w cells pass C Put.
func h643F3d(t *testing.T, how h643F3dHow, mutate func()) (dialsB, dialsC, putC int) {
	t.Helper()
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: 1, things: 2, extra: []h643Extra{h643IterExtra}})
	h643Budget(t, nil)
	RegisterRefreshHandlers(nil)
	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != h643K*2 {
		t.Fatalf("precondition: boot dialled %d, want %d (2 items × K cohorts) (dials=%v)", d, h643K*2, env.ep.byPath())
	}
	keys := h643MarksFor("iter-w")
	if len(keys) != h643K {
		t.Fatalf("precondition: %d iter-w marks, want %d", len(keys), h643K)
	}
	var allDeps []cache.DepKey
	recorded := false
	for _, k := range keys {
		m, _ := cache.EngineDeclinedExternalSet().Lookup(k)
		allDeps = append(allDeps, m.Deps...)
		for _, dk := range m.Deps {
			if dk.GVR == h643ThingsGVR {
				recorded = true
			}
		}
	}
	if !recorded {
		t.Fatalf("STOP (d4 precondition): no iter-w mark captured a dep on %s — the informer-served LIST did not land in the dep-gen sink", h643ThingsGVR)
	}
	cellKey, cellInputs := h643F3dCell(t)
	if mutate != nil {
		mutate()
	}

	for i := 0; i < 2; i++ {
		if err := env.dyn.Resource(h643ThingsGVR).Namespace(h643NS).Delete(context.Background(), "thing-"+itoaLine(i), metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete thing-%d: %v", i, err)
		}
	}
	stamp := cache.DepGenEpochNow()
	h643WaitFor(t, "the informer to serve the emptied list and its dep events", func() bool {
		return len(env.rw.ListObjects(h643ThingsGVR, h643NS)) == 0 && cache.DepGenEpochNow() >= stamp+2
	})

	// Pass A: a mark whose list read MISSED the cell at boot is uncovered and
	// retires on d4; it re-resolves, reads the still-stale cell and re-marks
	// (covered now). Pass B: every mark is covered by the unchanged stale cell —
	// it must SKIP: no re-dial loop while the cell is stale.
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643ThingsGVR})
	dialsB = env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR})

	switch how {
	case h643F3dRefresh:
		fn := cache.RefreshFuncForTest(cache.CacheEntryClassApistage)
		if fn == nil {
			t.Fatal("no apistage RefreshFunc registered")
		}
		seq0, _ := cache.ResolvedCache().CellPutSeq(cellKey)
		if err := fn(context.Background(), cellKey, cellInputs); err != nil {
			t.Fatalf("the real apistage refresh of the list cell failed: %v", err)
		}
		if seq1, ok := cache.ResolvedCache().CellPutSeq(cellKey); !ok || seq1 == seq0 {
			t.Fatalf("precondition: the refresh did not re-Put the list cell (putSeq %d → %d, live=%v)", seq0, seq1, ok)
		}
	case h643F3dEvict:
		cache.ResolvedCache().DeleteForTest(cellKey)
	}

	dialsC = env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643RAGVR})
	return dialsB, dialsC, len(h643WidgetKeysFor("iter-w"))
}

func TestIssue643_F3d_StaleListCell_HeldWhileStale_PutAfterRealRefresh(t *testing.T) {
	dialsB, dialsC, put := h643F3d(t, h643F3dRefresh, nil)
	if dialsB != 0 {
		t.Fatalf("F3d: while the list cell is still the stale one a background pass must SKIP (re-dialling would read the same stale body); pass B dialled %d", dialsB)
	}
	if dialsC != 0 {
		t.Fatalf("F3d: after the refresh the emptied iterator must make 0 external dials; got %d", dialsC)
	}
	if put != h643K {
		t.Fatalf("F3d: after the list cell's refresh iter-w is cacheable and must be Put for every cohort; got %d of %d (a stale mark left it cold — I-1)", put, h643K)
	}
}

func TestIssue643_F3d_StaleListCell_PutAfterEviction(t *testing.T) {
	_, dialsC, put := h643F3d(t, h643F3dEvict, nil)
	if dialsC != 0 || put != h643K {
		t.Fatalf("F3d (evict): after the list cell was evicted the next pass must Put iter-w for every cohort with 0 dials; got put=%d dials=%d", put, dialsC)
	}
}

func TestIssue643_F3d_Mutation_NoServedCellCheck_StaysCold(t *testing.T) {
	_, _, put := h643F3d(t, h643F3dRefresh, func() {
		prev := servedCellsChangedFn
		t.Cleanup(func() { servedCellsChangedFn = prev })
		servedCellsChangedFn = func([]cache.ServedCell) bool { return false }
	})
	if put == h643K {
		t.Fatalf("F3d mutation: without the served-cell check every iter-w cell was still Put — the arm cannot see the stale-cell defect")
	}
	t.Logf("F3d mutation (no served-cell check) RED as required: iter-w Put %d of %d", put, h643K)
}

func TestIssue643_F3d_Mutation_NoDepsCheck_StaysCold(t *testing.T) {
	_, _, put := h643F3d(t, h643F3dRefresh, func() {
		prev := depsMovedFn
		t.Cleanup(func() { depsMovedFn = prev })
		depsMovedFn = func([]cache.DepKey, uint64, []cache.ServedCell) bool { return false }
	})
	if put == h643K {
		t.Fatalf("F3d mutation: without d4 every iter-w cell was still Put — the arm cannot see the d4 defect (no boot read missed the list cell?)")
	}
	t.Logf("F3d mutation (no d4) RED as required: iter-w Put %d of %d", put, h643K)
}

// #749 — the F3d deletes land after a FORCED list/watch gap (every watch
// registers with the fake tracker 2 s late, outside the reactor lock, so the
// writes are not serialized behind it). Without h643Setup's watch barrier the
// two deletes are lost (the tracker never replays them) and the arm times out
// waiting for the emptied list — the flake, made deterministic. With it the
// setup returns only once the watch is established, and the arm is green.
// Mutation → RED: drop the barrier wait in h643Setup.
func TestIssue749_F3dSurvivesAForcedLateWatch(t *testing.T) {
	prev := h643WatchDelayForTest
	h643WatchDelayForTest = 2 * time.Second
	t.Cleanup(func() { h643WatchDelayForTest = prev })
	dialsB, dialsC, put := h643F3d(t, h643F3dRefresh, nil)
	if dialsB != 0 || dialsC != 0 || put != h643K {
		t.Fatalf("#749: under a forced late watch F3d read dialsB=%d dialsC=%d put=%d/%d, want 0/0/%d", dialsB, dialsC, put, h643K, h643K)
	}
}
