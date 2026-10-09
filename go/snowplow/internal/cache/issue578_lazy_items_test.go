//go:build unit || integration

package cache

import (
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func mkItems(names ...string) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, 0, len(names))
	for _, n := range names {
		out = append(out, &unstructured.Unstructured{Object: map[string]any{
			"kind":     "Thing",
			"metadata": map[string]any{"name": n},
		}})
	}
	return out
}

// TestIssue578_EnsureItems_ParsesOnceThenServesFromMemo is the core claim: the
// parse happens on the FIRST read and never again for that entry generation.
//
// This is what makes it safe to delete the refresher's eager parse. Ship #97's
// defect was the parse running on EVERY content-Get-hit; if this arm regressed,
// #578 would reintroduce exactly that defect.
func TestIssue578_EnsureItems_ParsesOnceThenServesFromMemo(t *testing.T) {
	ResetLazyItemsStatsForTest()
	e := &ResolvedEntry{RawJSON: []byte(`{"apiVersion":"v1","items":[],"kind":"ThingsList"}`)}

	var calls atomic.Int64
	parse := func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
		calls.Add(1)
		return mkItems("a", "b"), "v1", "ThingsList", true
	}

	items1, av1, k1, ok1 := e.EnsureItems(parse)
	if !ok1 || len(items1) != 2 || av1 != "v1" || k1 != "ThingsList" {
		t.Fatalf("first call: ok=%v n=%d av=%q kind=%q", ok1, len(items1), av1, k1)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("first call must parse exactly once, got %d", got)
	}

	for i := 0; i < 50; i++ {
		items2, av2, k2, ok2 := e.EnsureItems(parse)
		if !ok2 || av2 != av1 || k2 != k1 {
			t.Fatalf("repeat call %d disagreed: ok=%v av=%q kind=%q", i, ok2, av2, k2)
		}
		// Identity, not just equality: every serve must alias the SAME maps,
		// because the strip-while-private contract depends on exactly one
		// published tree.
		if &items2[0] != &items1[0] {
			t.Fatalf("repeat call %d returned a DIFFERENT items backing — the memo "+
				"is not being published/read", i)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("51 reads must parse exactly ONCE (Ship #97's guarantee), got %d", got)
	}

	st := LazyItemsStatsSnapshot()
	if st.Parsed != 1 || st.Served != 50 {
		t.Fatalf("counters: want Parsed=1 Served=50, got Parsed=%d Served=%d Eager=%d",
			st.Parsed, st.Served, st.Eager)
	}
}

// TestIssue578_EnsureItems_EagerItemsWinAndSkipParse pins back-compatibility:
// an entry written by a pre-#578 binary (or by the customer-facing miss path,
// which still fills Items) is served from Items and never parsed. This is what
// makes the change safe to roll WITHOUT draining the cache.
func TestIssue578_EnsureItems_EagerItemsWinAndSkipParse(t *testing.T) {
	ResetLazyItemsStatsForTest()
	eager := mkItems("eager")
	e := &ResolvedEntry{
		RawJSON:         []byte(`{"apiVersion":"v1","items":[],"kind":"ThingsList"}`),
		Items:           eager,
		ItemsAPIVersion: "v1",
		ItemsKind:       "ThingsList",
	}

	parse := func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
		t.Fatalf("parse MUST NOT run when eager Items are present")
		return nil, "", "", false
	}

	items, av, k, ok := e.EnsureItems(parse)
	if !ok || av != "v1" || k != "ThingsList" {
		t.Fatalf("eager path: ok=%v av=%q kind=%q", ok, av, k)
	}
	if &items[0] != &eager[0] {
		t.Fatalf("eager path must return the STORED Items slice, not a copy")
	}
	if st := LazyItemsStatsSnapshot(); st.Eager != 1 || st.Parsed != 0 {
		t.Fatalf("counters: want Eager=1 Parsed=0, got %+v", st)
	}
}

// TestIssue578_EnsureItems_MemoisesNegativeResult — a malformed envelope must
// be remembered as malformed.
//
// Without this, a cell whose RawJSON cannot be parsed would re-attempt the
// parse on EVERY serve — strictly worse than the pre-#578 behaviour it
// replaces, and invisible because the serve still succeeds via the caller's
// fallback.
func TestIssue578_EnsureItems_MemoisesNegativeResult(t *testing.T) {
	ResetLazyItemsStatsForTest()
	e := &ResolvedEntry{RawJSON: []byte(`{not json`)}

	var calls atomic.Int64
	parse := func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
		calls.Add(1)
		return nil, "", "", false
	}

	for i := 0; i < 20; i++ {
		if _, _, _, ok := e.EnsureItems(parse); ok {
			t.Fatalf("call %d: malformed envelope must report ok=false", i)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("a negative result must be memoised — want 1 parse, got %d", got)
	}
	if e.HasMaterialisedItems() {
		t.Fatalf("HasMaterialisedItems must stay false for a malformed envelope")
	}
}

// TestIssue578_EnsureItems_ConcurrentReadersSharePublishedTree is the race arm.
//
// Concurrent first-readers may each parse (that is deliberately tolerated), but
// exactly ONE tree may be PUBLISHED, and every caller must receive that one.
// If callers could receive their own private trees, two serves would alias
// different maps and the "strip once while private, then share" invariant that
// the gate and gojq depend on would be broken.
//
// Run under -race for this to mean anything.
func TestIssue578_EnsureItems_ConcurrentReadersSharePublishedTree(t *testing.T) {
	ResetLazyItemsStatsForTest()
	e := &ResolvedEntry{RawJSON: []byte(`{"apiVersion":"v1","items":[],"kind":"ThingsList"}`)}

	parse := func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
		// A fresh tree per call, so a caller returning its OWN result instead
		// of the published one is detectable by pointer identity.
		return mkItems("x"), "v1", "ThingsList", true
	}

	const n = 64
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = map[*unstructured.Unstructured]int{}
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			items, _, _, ok := e.EnsureItems(parse)
			if !ok || len(items) != 1 {
				t.Errorf("concurrent read: ok=%v n=%d", ok, len(items))
				return
			}
			mu.Lock()
			seen[items[0]]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(seen) != 1 {
		t.Fatalf("all %d readers must observe the SAME published tree; saw %d distinct", n, len(seen))
	}
	for _, c := range seen {
		if c != n {
			t.Fatalf("published tree observed %d times, want %d", c, n)
		}
	}
}

// TestIssue578_EnsureItems_TotalOnDegenerateInputs — the method must never
// panic and must always leave the caller a usable fallback signal.
func TestIssue578_EnsureItems_TotalOnDegenerateInputs(t *testing.T) {
	parse := func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
		return mkItems("a"), "v1", "ThingsList", true
	}

	var nilEntry *ResolvedEntry
	if _, _, _, ok := nilEntry.EnsureItems(parse); ok {
		t.Fatalf("nil entry must report ok=false")
	}
	if nilEntry.HasMaterialisedItems() {
		t.Fatalf("nil entry must report no materialised items")
	}

	// Nil parse func: no panic, no publish.
	e := &ResolvedEntry{RawJSON: []byte(`{}`)}
	if _, _, _, ok := e.EnsureItems(nil); ok {
		t.Fatalf("nil parse must report ok=false")
	}

	// Empty RawJSON: nothing to parse, and parse must not be invoked.
	empty := &ResolvedEntry{}
	if _, _, _, ok := empty.EnsureItems(func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
		t.Fatalf("parse MUST NOT run with empty RawJSON")
		return nil, "", "", false
	}); ok {
		t.Fatalf("empty RawJSON must report ok=false")
	}
}
