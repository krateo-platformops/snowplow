package dispatchers

// issue643_prc_f5z_zero_cold_test.go — #643 PR-C F5-z: a resumed gvr-discovered
// attempt never skips a unit whose cell is not provably the Put an earlier
// attempt made with unchanged inputs (zero-cold). Attempt 0 is cut after j
// units; then, for units it DID re-Put:
//
//   (a) the cell is evicted            → attempt 1 re-resolves and re-Puts it
//   (b) an input moves (dirty window)  → attempt 1 re-resolves it
//   (c) someone else re-Puts the cell  → attempt 1 re-resolves it
//   (d) an input moves and the refresh is DECLINED, leaving the cell
//       known-stale with the SAME pointer → attempt 1 re-resolves it
//
// and a done unit with none of these is still skipped (the resume works).
//
// Mutation arms drop one resume rule each through its seam and must leave a
// unit un-redone: rule 1 (epoch) → F5-e goes RED; rule 2 (same Put) → (a)/(c);
// rule 3 (inputs moved) → (b)/(d). The known-stale term inside
// cache.InputsMovedSincePut is mutated by source (recorded in the PR).
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// h643F5zCut runs boot then a gvr-discovered attempt 0 cut after j units, and
// returns the resume records attempt 0 left, by key.
func h643F5zCut(t *testing.T, env *h643Env, j int) map[string]passDoneRecord {
	t.Helper()
	budgets := map[prewarmScopeKind]time.Duration{}
	h643Budget(t, budgets)
	env.pass(prewarmScope{kind: scopeKindBoot})
	var units int
	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if mode == seedModeGVRDiscovered {
			units++
			if units == j+1 {
				<-ctx.Done()
				return ctx.Err()
			}
		}
		return prevW(ctx, e, authnNS, mode)
	}
	budgets[scopeKindGVRDiscovered] = 3 * time.Second
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	seedOneWidgetFn = prevW
	budgets[scopeKindGVRDiscovered] = time.Hour
	set := env.engine.passDoneSetFor(prewarmScope{kind: scopeKindGVRDiscovered}.key())
	set.mu.Lock()
	defer set.mu.Unlock()
	out := make(map[string]passDoneRecord, len(set.m))
	for k, r := range set.m {
		out[k] = r
	}
	if len(out) != j {
		t.Fatalf("precondition: attempt 0 left %d resume records, want %d", len(out), j)
	}
	return out
}

// h643DoneKeysOf returns the resume-record keys whose cell belongs to widget w.
func h643DoneKeysOf(done map[string]passDoneRecord, w string) []string {
	var out []string
	for k, r := range done {
		if r.entry != nil && r.entry.Inputs != nil && r.entry.Inputs.Name == w {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func h643Entry(key string) *cache.ResolvedEntry {
	e, _ := cache.ResolvedCache().GetNoTouch(key)
	return e
}

// h643Dirty edits widget w (an input of every cell of w) and waits until the
// cells' dirty windows are open.
func h643Dirty(t *testing.T, env *h643Env, w string, keys []string, until func(string) bool) {
	t.Helper()
	u, err := env.dyn.Resource(h643WidgetGVR).Namespace(h643NS).Get(context.Background(), w, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", w, err)
	}
	u.SetLabels(map[string]string{"edited": "yes"})
	u.SetResourceVersion("2")
	if _, err := env.dyn.Resource(h643WidgetGVR).Namespace(h643NS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update %s: %v", w, err)
	}
	h643WaitFor(t, "the edit to reach "+w+"'s cells", func() bool {
		for _, k := range keys {
			if !until(k) {
				return false
			}
		}
		return true
	})
}

type h643F5zResult struct {
	redone map[string]bool // key → attempt 1 put a new entry under it
	kept   []string        // done keys untouched by any case
}

// h643F5zABC runs cases (a), (b), (c) plus an untouched control.
func h643F5zABC(t *testing.T) (h643F5zResult, map[string]string) {
	t.Helper()
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	done := h643F5zCut(t, env, 2*h643K+1) // int-w0, int-w1 fully + one int-w2 unit
	w0, w1 := h643DoneKeysOf(done, "int-w0"), h643DoneKeysOf(done, "int-w1")
	if len(w0) != h643K || len(w1) != h643K {
		t.Fatalf("precondition: attempt 0 must have re-Put every cohort of int-w0 and int-w1 (got %d, %d)", len(w0), len(w1))
	}
	cases := map[string]string{w0[0]: "a-evicted", w0[1]: "c-reput"}
	for _, k := range w1 {
		cases[k] = "b-dirty"
	}
	control := w0[2]

	// The REAL refresher, whose widget refresh holds until the arm ends: a dep
	// event dirty-marks int-w1's cells and their windows stay OPEN (in flight)
	// while attempt 1 runs — the moved-inputs state rule 3 must see.
	hold := make(chan struct{})
	cache.ResetRefresherForTest()
	cache.RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, _ cache.ResolvedKeyInputs) error {
		select {
		case <-hold:
		case <-ctx.Done():
		}
		return nil
	})
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	t.Cleanup(func() { close(hold); rcancel(); cache.ResetRefresherForTest() })

	cache.ResolvedCache().DeleteForTest(w0[0]) // (a)
	old := h643Entry(w0[1])                    // (c): a different writer's Put
	cache.ResolvedCache().Put(w0[1], &cache.ResolvedEntry{RawJSON: append([]byte(nil), old.RawJSON...), Inputs: old.Inputs})
	h643Dirty(t, env, "int-w1", w1, cache.InputsMovedSincePut) // (b)

	before := map[string]*cache.ResolvedEntry{}
	for k := range cases {
		before[k] = h643Entry(k)
	}
	before[control] = h643Entry(control)
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})

	res := h643F5zResult{redone: map[string]bool{}}
	for k := range cases {
		after := h643Entry(k)
		res.redone[k] = after != nil && after != before[k]
	}
	if h643Entry(control) == before[control] {
		res.kept = append(res.kept, control)
	}
	return res, cases
}

func TestIssue643_F5z_ResumeRedoesEveryUnitWhoseCellIsNotProvablyItsPut(t *testing.T) {
	res, cases := h643F5zABC(t)
	for k, c := range cases {
		if !res.redone[k] {
			t.Errorf("F5-z (%s): attempt 1 did NOT re-resolve and re-Put the unit — a resumed pass skipped a cell that is not provably the Put it trusts (zero-cold)", c)
		}
	}
	if len(res.kept) != 1 {
		t.Fatalf("F5-z control: a done unit with an unchanged, clean cell must still be skipped (the resume works); kept=%d", len(res.kept))
	}
}

func TestIssue643_F5z_Mutation_DropSameEntryRule(t *testing.T) {
	prev := passDoneSameEntryRuleFn
	passDoneSameEntryRuleFn = func(cacheHandle, string, passDoneRecord) bool { return true }
	t.Cleanup(func() { passDoneSameEntryRuleFn = prev })
	res, cases := h643F5zABC(t)
	stale := 0
	for k, c := range cases {
		if (c == "a-evicted" || c == "c-reput") && !res.redone[k] {
			stale++
		}
	}
	if stale == 0 {
		t.Fatal("F5-z mutation (rule 2 dropped): (a)/(c) were still redone — the arm cannot see the evicted / re-Put defect")
	}
	t.Logf("F5-z mutation (rule 2 dropped) RED as required: %d of (a)/(c) left un-redone", stale)
}

func TestIssue643_F5z_Mutation_DropInputsMovedRule(t *testing.T) {
	prev := passDoneDirtyRuleFn
	passDoneDirtyRuleFn = func(string) bool { return false }
	t.Cleanup(func() { passDoneDirtyRuleFn = prev })
	res, cases := h643F5zABC(t)
	stale := 0
	for k, c := range cases {
		if c == "b-dirty" && !res.redone[k] {
			stale++
		}
	}
	if stale == 0 {
		t.Fatal("F5-z mutation (rule 3 dropped): (b) was still redone — the arm cannot see the moved-inputs defect")
	}
	t.Logf("F5-z mutation (rule 3 dropped) RED as required: %d (b) units left un-redone", stale)
}

func TestIssue643_F5e_Mutation_DropEpochRule(t *testing.T) {
	prev := passDoneEpochRuleFn
	passDoneEpochRuleFn = func(uint64, uint64) bool { return true }
	t.Cleanup(func() { passDoneEpochRuleFn = prev })
	_, _, a1, _ := h643F5(t, 5, func(env *h643Env) {
		cache.ResetGVRDiscoveredHooksForTest()
		t.Cleanup(cache.ResetGVRDiscoveredHooksForTest)
		registerEngineGVRDiscoveredHook(env.engine)
		cache.NotifyGVRDiscoveredForReprewarmTest(h643ThingsGVR)
	})
	if a1 == h643K*h643N {
		t.Fatal("F5-e mutation (rule 1 dropped): attempt 1 still re-resolved every key — the arm cannot see the epoch defect")
	}
	t.Logf("F5-e mutation (rule 1 dropped) RED as required: attempt 1 re-resolved %d of %d", a1, h643K*h643N)
}

// TestIssue643_F5z_KnownStaleAfterDeclinedRefresh — case (d), architect C3: the
// input moves, the REAL refresher dequeues the dirty cells and the refresh is
// DECLINED (it returns without a Put), leaving the cells KNOWN-STALE with the
// SAME pointer. Attempt 1 must re-resolve them.
func TestIssue643_F5z_KnownStaleAfterDeclinedRefresh(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	done := h643F5zCut(t, env, 2*h643K+1)
	w1 := h643DoneKeysOf(done, "int-w1")
	if len(w1) != h643K {
		t.Fatalf("precondition: int-w1 fully re-Put by attempt 0 (got %d)", len(w1))
	}
	// A refresher whose widget refresh ends WITHOUT a Put (declined).
	cache.ResetRefresherForTest()
	cache.RegisterRefreshFunc("widgets", func(context.Context, string, cache.ResolvedKeyInputs) error { return nil })
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest() })

	known := func(k string) bool { _, _, ok := cache.KnownStale(k); return ok }
	h643Dirty(t, env, "int-w1", w1, known)
	before := map[string]*cache.ResolvedEntry{}
	for _, k := range w1 {
		before[k] = h643Entry(k)
		if before[k] != done[k].entry {
			t.Fatalf("precondition (d): the declined refresh must leave the SAME pointer under %s", k)
		}
	}
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered})
	for _, k := range w1 {
		if after := h643Entry(k); after == nil || after == before[k] {
			t.Errorf("F5-z (d): a known-stale cell (declined refresh, same pointer) was skipped by the resume — its inputs moved since the Put it trusts")
		}
	}
}
