// seed_resolve_memo_test.go — #130 F4 falsifiers for the per-seed-pass
// RA-resolve memo (SeedResolveMemo). Covers PM conditions C-F4-3 (JSON-native
// value + -race + DeepCopyJSON round-trip), C-F4-4 (RBAC key divergence — the
// decisive leak RED arm), C-F4-5 (teardown), C-F4-6 (correctness: hit is
// byte-identical to the stored body), C-F4-8 (miss-when-absent), C-F4-9
// (toggle-off inert).
//
// The memo mechanism itself is falsified here (pure, no apiserver). The seam
// wiring — memo consulted ONLY under the seed context, never on the /call path
// — is falsified in internal/resolvers/widgets/apiref/seed_memo_seam_test.go.

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"

	pmaps "github.com/krateo-platformops/plumbing/maps"
)

func memoCacheOn(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
}

// filteredBody simulates a RESTAction's RBAC-filtered resolved Status: a list
// of the compositions THIS identity may see. Two cohorts with divergent RBAC
// return PROVABLY DIVERGENT bodies (C-F4-4: same-output pairs are inadmissible).
func filteredBody(visible []string) map[string]any {
	items := make([]any, 0, len(visible))
	for _, n := range visible {
		items = append(items, map[string]any{"metadata": map[string]any{"name": n}})
	}
	return map[string]any{"kind": "List", "items": items}
}

func visibleNames(body map[string]any) []string {
	items, _ := body["items"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		md, _ := m["metadata"].(map[string]any)
		name, _ := md["name"].(string)
		out = append(out, name)
	}
	return out
}

// TestSeedResolveMemo_RBACKeyDivergence_DECISIVE — C-F4-4 RED arm. Cohort A
// (groups [team-a]) and cohort B (groups [team-b]) resolve the SAME RESTAction
// but see PROVABLY DIVERGENT filtered bodies (A sees comp-a1/a2, B sees comp-b1).
// The production memo folds identity (username+groups) into the key, so A's
// Store under A's key and B's Load under B's key MISS → B resolves its own body.
// The RED half computes a key that DROPS identity (RA/page only): B's Load then
// HITS A's cell and B is served A's compositions — a cross-user RBAC LEAK. The
// arm FAILS (as it must) if the identity-less key ever serves B a body != B's.
func TestSeedResolveMemo_RBACKeyDivergence_DECISIVE(t *testing.T) {
	memoCacheOn(t)

	bodyA := filteredBody([]string{"comp-a1", "comp-a2"})
	bodyB := filteredBody([]string{"comp-b1"})
	// Premise guard: the two cohorts' outputs MUST diverge (same-output pair is
	// inadmissible per C-F4-4).
	if reflect.DeepEqual(visibleNames(bodyA), visibleNames(bodyB)) {
		t.Fatal("premise broken: cohort A and B must have PROVABLY DIVERGENT filtered output")
	}

	const raNS, raName = "demo-system", "compositions-list"
	extrasHash := HashExtras(nil)

	// ── PRODUCTION key (folds identity). A stores; B must MISS and resolve its own.
	prod := NewSeedResolveMemo(pmaps.DeepCopyJSON)
	keyA := prod.Key(raNS, raName, "alice", []string{"team-a"}, extrasHash, 0, 0)
	keyB := prod.Key(raNS, raName, "bob", []string{"team-b"}, extrasHash, 0, 0)
	if keyA == keyB {
		t.Fatal("RED: production memo key COLLIDES across divergent-RBAC cohorts — identity is not folded into the key. This is the A→B leak. C-F4-4.")
	}
	prod.Store(keyA, pmaps.DeepCopyJSON(bodyA), nil)
	if _, _, ok := prod.Load(keyB); ok {
		t.Fatal("RED: cohort B HIT cohort A's memo cell under the production (identity-folded) key — cross-user RBAC leak. C-F4-4.")
	}

	// ── LEAK key (identity DROPPED — RA/page only). This is the defect the
	// production key prevents; here we PROVE it would leak, so the arm has teeth.
	leakKey := func() string {
		return raNS + "|" + raName + "|pp=0|p=0" // NO username/groups
	}
	leak := NewSeedResolveMemo(pmaps.DeepCopyJSON)
	leak.Store(leakKey(), pmaps.DeepCopyJSON(bodyA), nil) // A resolves first
	got, _, ok := leak.Load(leakKey())                  // B "resolves" — same key
	if !ok {
		t.Fatal("harness broken: identity-less key should self-hit")
	}
	if reflect.DeepEqual(visibleNames(got), visibleNames(bodyB)) {
		t.Fatal("harness broken: the leak arm must serve B something != B's own body")
	}
	// The leak arm serves B cohort A's compositions — this is exactly the leak
	// the identity-folded production key eliminates. Assert the leak IS present
	// so a future refactor that silently drops identity is caught by the
	// production half above going RED while this half stays a demonstrated leak.
	if !reflect.DeepEqual(visibleNames(got), visibleNames(bodyA)) {
		t.Fatalf("harness broken: identity-less key must serve B cohort A's body; got %v", visibleNames(got))
	}
}

// TestSeedResolveMemo_CorrectnessByteIdentical — C-F4-6. A memo hit returns a
// body byte-identical (JSON-canonical) to the stored resolve for the same
// (RA, identity, page). Also proves the returned map is a DISTINCT deep copy
// (mutating the hit does not corrupt the stored snapshot — no aliasing).
func TestSeedResolveMemo_CorrectnessByteIdentical(t *testing.T) {
	memoCacheOn(t)
	memo := NewSeedResolveMemo(pmaps.DeepCopyJSON)
	body := filteredBody([]string{"comp-x", "comp-y"})
	key := memo.Key("ns", "ra", "carol", []string{"g1", "g2"}, HashExtras(nil), 5, 1)

	memo.Store(key, pmaps.DeepCopyJSON(body), nil)
	hit, _, ok := memo.Load(key)
	if !ok {
		t.Fatal("RED: expected memo HIT for the stored (RA, identity, page). C-F4-6.")
	}
	wantJSON, _ := json.Marshal(body)
	gotJSON, _ := json.Marshal(hit)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("RED: memo hit body != cold-resolve body.\n want %s\n got  %s", wantJSON, gotJSON)
	}
	// Aliasing guard: mutate the hit; a second Load must still equal the original.
	hit["items"] = []any{}
	hit2, _, _ := memo.Load(key)
	got2JSON, _ := json.Marshal(hit2)
	if string(got2JSON) != string(wantJSON) {
		t.Fatalf("RED: mutating a memo hit corrupted the stored snapshot (aliasing). Load must return a fresh deep copy.\n want %s\n got  %s", wantJSON, got2JSON)
	}
}

// TestSeedResolveMemo_KeyDivergesOnPageAndExtras — every input that changes the
// resolved body must change the key (C-F4-6 correctness precondition): page,
// perPage, and extras. If any of these collide, a hit would serve a wrong-page
// or wrong-extras body.
func TestSeedResolveMemo_KeyDivergesOnPageAndExtras(t *testing.T) {
	memoCacheOn(t)
	m := NewSeedResolveMemo(pmaps.DeepCopyJSON)
	base := m.Key("ns", "ra", "u", []string{"g"}, HashExtras(nil), 5, 1)
	cases := map[string]string{
		"page":    m.Key("ns", "ra", "u", []string{"g"}, HashExtras(nil), 5, 2),
		"perPage": m.Key("ns", "ra", "u", []string{"g"}, HashExtras(nil), 10, 1),
		"extras":  m.Key("ns", "ra", "u", []string{"g"}, HashExtras(map[string]any{"k": "v"}), 5, 1),
		"raName":  m.Key("ns", "ra2", "u", []string{"g"}, HashExtras(nil), 5, 1),
	}
	for dim, k := range cases {
		if k == base {
			t.Fatalf("RED: memo key does NOT diverge on %s — a hit could serve a body computed for a different %s.", dim, dim)
		}
	}
	// Group ORDER must NOT change the key (RBAC identity is a set).
	if m.Key("ns", "ra", "u", []string{"a", "b"}, HashExtras(nil), 5, 1) !=
		m.Key("ns", "ra", "u", []string{"b", "a"}, HashExtras(nil), 5, 1) {
		t.Fatal("RED: memo key changes with group ORDER — must be order-independent (groups are a set).")
	}
}

// TestSeedResolveMemo_Teardown_NoSurviveAcrossPass — C-F4-5. The memo is
// context-carried; a fresh context (a NEW seed pass) carries NO memo, so it
// cannot serve the prior pass's body. Also: a context that never installed a
// memo returns nil (the /call posture — C-F4-8).
func TestSeedResolveMemo_Teardown_NoSurviveAcrossPass(t *testing.T) {
	memoCacheOn(t)

	// Pass 1: install a memo, store a body.
	pass1 := WithSeedResolveMemo(context.Background(), NewSeedResolveMemo(pmaps.DeepCopyJSON))
	m1 := SeedResolveMemoFromContext(pass1)
	if m1 == nil {
		t.Fatal("premise broken: pass-1 ctx should carry a memo")
	}
	k := m1.Key("ns", "ra", "u", nil, HashExtras(nil), 0, 0)
	m1.Store(k, pmaps.DeepCopyJSON(filteredBody([]string{"leftover"})), nil)

	// Pass 2: a fresh base context (the previous pass returned; nothing
	// references its memo). No memo installed ⇒ nil ⇒ the pass MUST resolve, not
	// serve the pass-1 leftover.
	pass2 := context.Background()
	if got := SeedResolveMemoFromContext(pass2); got != nil {
		t.Fatal("RED: a fresh context carried a memo — the memo survived the seed pass (C-F4-5). It must be strictly context-carried, never process-global.")
	}
}

// TestSeedResolveMemo_MissWhenAbsent — C-F4-8. A nil memo (no memo on ctx = the
// /call path) is ALWAYS a miss; nil-receiver methods are safe. Store on a nil
// memo is a no-op (never panics).
func TestSeedResolveMemo_MissWhenAbsent(t *testing.T) {
	memoCacheOn(t)
	var nilMemo *SeedResolveMemo // as returned by SeedResolveMemoFromContext off the /call path
	if _, _, ok := nilMemo.Load("anything"); ok {
		t.Fatal("RED: nil memo (no memo installed — the /call path) reported a HIT. C-F4-8.")
	}
	nilMemo.Store("anything", map[string]any{"x": 1}, nil) // must not panic
	h, mi := nilMemo.Stats()
	if h != 0 || mi != 0 {
		t.Fatalf("RED: nil memo Stats non-zero (%d/%d).", h, mi)
	}
}

// TestSeedResolveMemo_ToggleOffInert — C-F4-9. Under Disabled() (CACHE_ENABLED
// =false) WithSeedResolveMemo returns the ctx UNCHANGED, so no memo is ever
// installed and the seam is byte-identical to pre-F4.
func TestSeedResolveMemo_ToggleOffInert(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	ctx := WithSeedResolveMemo(context.Background(), NewSeedResolveMemo(pmaps.DeepCopyJSON))
	if got := SeedResolveMemoFromContext(ctx); got != nil {
		t.Fatal("RED: WithSeedResolveMemo installed a memo while Disabled() — the memo must be inert in cache-off mode. C-F4-9.")
	}
}

// TestSeedResolveMemo_JSONNativeConcurrent_Race — C-F4-3. Store/Load a
// JSON-native body concurrently across many goroutines (the seed fans widgets
// across cohort goroutines). Run with -race. Uses pmaps.DeepCopyJSON (the
// production copyFn) so a non-JSON-native value would PANIC here — the round
// trip is exercised on every Store and every Load. Also asserts every hit is a
// valid, uncorrupted deep copy under concurrency.
func TestSeedResolveMemo_JSONNativeConcurrent_Race(t *testing.T) {
	memoCacheOn(t)
	memo := NewSeedResolveMemo(pmaps.DeepCopyJSON)

	// JSON-native body: nested map + []any + string/float64/bool/nil — every
	// type DeepCopyJSON accepts. A []string here would panic (the C-F4-3 guard).
	body := map[string]any{
		"kind":  "List",
		"count": float64(3),
		"ok":    true,
		"nested": map[string]any{
			"tags":  []any{"x", "y"},
			"inner": map[string]any{"z": nil},
		},
	}
	wantJSON, _ := json.Marshal(body)

	const cohorts, widgetsPer = 8, 32
	var wg sync.WaitGroup
	for c := 0; c < cohorts; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			// Each cohort has its own identity → own key; within a cohort the
			// widgetsPer goroutines race Store/Load on the SAME key (the
			// shared-RA fan-out the memo exists to collapse).
			key := memo.Key("ns", "ra", fmt.Sprintf("user-%d", c), []string{fmt.Sprintf("g-%d", c)}, HashExtras(nil), 0, 0)
			var inner sync.WaitGroup
			for w := 0; w < widgetsPer; w++ {
				inner.Add(1)
				go func() {
					defer inner.Done()
					if hit, _, ok := memo.Load(key); ok {
						if got, _ := json.Marshal(hit); string(got) != string(wantJSON) {
							t.Errorf("RED: concurrent memo hit corrupted: got %s", got)
						}
					} else {
						memo.Store(key, pmaps.DeepCopyJSON(body), nil)
					}
				}()
			}
			inner.Wait()
			// After the storm every cohort key must resolve to the body.
			hit, _, ok := memo.Load(key)
			if !ok {
				t.Errorf("RED: cohort %d key never populated", c)
				return
			}
			if got, _ := json.Marshal(hit); string(got) != string(wantJSON) {
				t.Errorf("RED: cohort %d final body corrupted: %s", c, got)
			}
		}(c)
	}
	wg.Wait()
}
