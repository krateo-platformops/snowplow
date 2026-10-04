// ra_full_list_slice_test.go — Ship 4a (0.30.198) falsifiers for the
// page-independent RAFullList cache core.
//
// Falsifier-first: these exercise the pure logic (key page-independence,
// Go-slice == RA jq slice, sliceability memo per shape, pinned-eviction
// protection, resident-region race) BEFORE any dispatcher/resolver wiring.

package cache

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jqutil"
)

// HG-4a.empty — FullListIsEmpty: the mechanism-uniform emptiness probe that
// the serve/refresher empty-full guard (0.30.208) keys off. A clean single-
// array map with a zero-length list is "empty" (→ true). A non-empty list,
// nil, a zero-array map, and a multi-array map are NOT (→ false). NO
// resource/name/GVR literal — keyed purely on the single-array shape contract.
func TestFullListIsEmpty(t *testing.T) {
	cases := []struct {
		name string
		full map[string]any
		want bool
	}{
		{"single empty array", map[string]any{"compositionspanels": []any{}}, true},
		{"single empty array generic", map[string]any{"items": []any{}}, true},
		{"single non-empty array", map[string]any{"compositionspanels": []any{map[string]any{"x": 1}}}, false},
		{"nil map", nil, false},
		{"no array key (aggregation shape)", map[string]any{"count": float64(0)}, false},
		{"multi array map", map[string]any{"a": []any{}, "b": []any{}}, false},
		{"empty array plus scalar sibling", map[string]any{"items": []any{}, "total": float64(0)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FullListIsEmpty(tc.full); got != tc.want {
				t.Fatalf("FullListIsEmpty(%v) = %v, want %v", tc.full, got, tc.want)
			}
		})
	}
}

// HG-4a.1 — page-independent key. Two ResolvedKeyInputs differing ONLY in
// Page/PerPage produce the IDENTICAL RAFullList key; differing in a non-slice
// input (resource/ns/name/cohort/non-slice extra) produces a DIFFERENT key.
// extrasMinusSlice strips slice/page/perPage/offset keys.
func TestRAFullList_PageIndependentKey(t *testing.T) {
	base := func(extras map[string]any) ResolvedKeyInputs {
		return RAFullListKeyInputsForTest("composition.krateo.io", "v1", "panels",
			"krateo-system", "compositions-panels", "uid-c0ffee", extras)
	}

	// Two callers differing ONLY in slice (page/perPage/offset/slice in
	// extras) MUST collapse to one key — the RAFullListKeyInputs forces
	// PerPage/Page to 0 and strips slice extras.
	k1 := ComputeKey(base(map[string]any{"page": float64(1), "perPage": float64(20), "tenant": "acme"}))
	k2 := ComputeKey(base(map[string]any{"page": float64(7), "perPage": float64(50), "offset": float64(300), "tenant": "acme"}))
	if k1 != k2 {
		t.Fatalf("page-independence broken: slice-only difference produced distinct keys\n  %s\n  %s", k1, k2)
	}

	// A no-extras caller equals a slice-only-extras caller (strip → nil).
	k3 := ComputeKey(base(nil))
	k4 := ComputeKey(base(map[string]any{"slice": map[string]any{"page": float64(2)}, "page": float64(2), "perPage": float64(10), "offset": float64(10)}))
	if k3 != k4 {
		t.Fatalf("extrasMinusSlice did not strip every slice key: %s vs %s", k3, k4)
	}

	// A NON-slice difference MUST change the key.
	type mut struct {
		name string
		in   ResolvedKeyInputs
	}
	muts := []mut{
		{"resource", RAFullListKeyInputsForTest("composition.krateo.io", "v1", "OTHER", "krateo-system", "compositions-panels", "uid-c0ffee", nil)},
		{"namespace", RAFullListKeyInputsForTest("composition.krateo.io", "v1", "panels", "OTHER-NS", "compositions-panels", "uid-c0ffee", nil)},
		{"name", RAFullListKeyInputsForTest("composition.krateo.io", "v1", "panels", "krateo-system", "OTHER-NAME", "uid-c0ffee", nil)},
		{"cohort", RAFullListKeyInputsForTest("composition.krateo.io", "v1", "panels", "krateo-system", "compositions-panels", "uid-dead", nil)},
		{"nonsliceExtra", RAFullListKeyInputsForTest("composition.krateo.io", "v1", "panels", "krateo-system", "compositions-panels", "uid-c0ffee", map[string]any{"tenant": "different"})},
	}
	zero := ComputeKey(base(nil))
	for _, m := range muts {
		if ComputeKey(m.in) == zero {
			t.Fatalf("non-slice input %q did not change the RAFullList key — cells would over-collapse", m.name)
		}
	}
}

// HG-4a.1b — extrasMinusSlice never mutates its input and strips exactly the
// slice keys.
func TestExtrasMinusSlice(t *testing.T) {
	in := map[string]any{
		"slice":   map[string]any{"page": 1.0},
		"page":    1.0,
		"perPage": 20.0,
		"offset":  0.0,
		"tenant":  "acme",
		"role":    "viewer",
	}
	out := extrasMinusSlice(in)
	if len(in) != 6 {
		t.Fatalf("input map was mutated: now has %d keys", len(in))
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 non-slice keys, got %d: %v", len(out), out)
	}
	for _, k := range []string{"slice", "page", "perPage", "offset"} {
		if _, present := out[k]; present {
			t.Fatalf("slice key %q survived extrasMinusSlice", k)
		}
	}
	if out["tenant"] != "acme" || out["role"] != "viewer" {
		t.Fatalf("non-slice keys lost: %v", out)
	}
	// All-slice extras → nil.
	if extrasMinusSlice(map[string]any{"page": 1.0, "perPage": 10.0}) != nil {
		t.Fatalf("all-slice extras should strip to nil")
	}
	if extrasMinusSlice(nil) != nil {
		t.Fatalf("nil extras should return nil")
	}
}

// raSliceJQ is the compositions-panels RESTAction's top-level output filter
// (verbatim shape): sort_by(creationTimestamp)|reverse, then
// $sorted[offset:offset+perPage] driven by the injected .slice. The Go-slice
// MUST be byte-identical to this jq's output for every (page,perPage).
const raSliceJQ = `
{
  compositionspanels: (
    (.compositionspanels // []) as $items
    | ($items | sort_by(.metadata.creationTimestamp // "") | reverse) as $sorted
    | (.slice.offset  // 0)                 as $offset
    | (.slice.perPage // ($sorted | length)) as $perPage
    | [
        $sorted
        | length as $len
        | range($offset; $offset + $perPage)
        | select(. < $len)
        | $sorted[.]
      ]
  )
}
`

// evalRA runs the RA output jq over dict (with optional injected slice),
// returning the parsed result map. Mirrors restactions.Resolve's jqutil.Eval.
func evalRA(t *testing.T, dict map[string]any) map[string]any {
	t.Helper()
	s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: raSliceJQ, Data: dict})
	if err != nil {
		t.Fatalf("RA jq eval failed: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		t.Fatalf("RA jq output not a JSON object: %v\n%s", err, s)
	}
	return out
}

// makePanels builds N panel objects with descending creationTimestamps so the
// RA's sort_by|reverse produces a deterministic order.
func makePanels(n int) []any {
	items := make([]any, n)
	for i := 0; i < n; i++ {
		// Earlier i → earlier timestamp; reverse puts newest (highest i) first.
		ts := fmt.Sprintf("2026-01-%02dT00:00:00Z", i+1)
		items[i] = map[string]any{
			"metadata": map[string]any{
				"name":              fmt.Sprintf("panel-%03d", i),
				"creationTimestamp": ts,
			},
		}
	}
	return items
}

// HG-4a.2 — Go-slice == RA `.slice` jq. For a sort-then-slice fixture the
// Go-slice over the UNPAGINATED full result is byte-identical to the RA's own
// `.slice` jq output for several (page, perPage). A NON-sliceable fixture
// (per-page aggregation) byte-differs → the byte-verify FAILS → fall-back.
func TestRAFullList_GoSliceEqualsJQSlice(t *testing.T) {
	panels := makePanels(49) // odd count to exercise the final partial page

	// Unpaginated full result (no .slice injected) — the cached RAFullList.
	fullDict := map[string]any{"compositionspanels": panels}
	full := evalRA(t, fullDict)

	cases := []struct{ page, perPage int }{
		{1, 20}, {2, 20}, {3, 20}, // page 3 is partial (40..49)
		{1, 50}, // perPage > len
		{4, 20}, // page 4 entirely past end → empty
		{1, 7}, {2, 7}, {7, 7},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("p%d_pp%d", c.page, c.perPage), func(t *testing.T) {
			offset := (c.page - 1) * c.perPage

			// Go-slice over the cached full result.
			goSliced, ok := GoSliceFullList(full, offset, c.perPage)
			if !ok {
				t.Fatalf("GoSliceFullList unexpectedly returned ok=false for sliceable shape")
			}
			goBytes, _ := json.Marshal(goSliced)

			// RA jq the OLD page-keyed way: inject .slice, resolve fresh.
			raDict := map[string]any{
				"compositionspanels": panels,
				"slice": map[string]any{
					"page":    float64(c.page),
					"perPage": float64(c.perPage),
					"offset":  float64(offset),
				},
			}
			raSliced := evalRA(t, raDict)
			raBytes, _ := json.Marshal(raSliced)

			if string(goBytes) != string(raBytes) {
				t.Fatalf("Go-slice != RA jq slice for page=%d perPage=%d\n  go: %s\n  ra: %s",
					c.page, c.perPage, goBytes, raBytes)
			}
		})
	}

	// Byte-verify of the FULL F itself: Go identity-slice (perPage<=0) equals
	// a fresh unpaginated RA resolve.
	goFull, ok := GoSliceFullList(full, 0, 0)
	if !ok {
		t.Fatalf("identity Go-slice returned ok=false")
	}
	gb, _ := json.Marshal(goFull)
	fb, _ := json.Marshal(full)
	if string(gb) != string(fb) {
		t.Fatalf("identity Go-slice != full F:\n  %s\n  %s", gb, fb)
	}
}

// HG-4a.2b — NON-sliceable shape → GoSliceFullList ok=false OR byte-mismatch
// vs the RA jq → the caller takes the fall-back (never a wrong result).
func TestRAFullList_NonSliceableFallsBack(t *testing.T) {
	// A per-page-AGGREGATION RA: the output depends on the slice (it returns a
	// COUNT of the page, not the page itself) so a Go-slice over the
	// unpaginated full result can NEVER reproduce a paginated resolve.
	aggJQ := `{ count: ((.items // []) | length), page_total: (.slice.perPage // 0) }`
	full := func(injectSlice bool, page, perPage int) map[string]any {
		dict := map[string]any{"items": makePanels(30)}
		if injectSlice {
			dict["slice"] = map[string]any{"perPage": float64(perPage), "offset": float64((page - 1) * perPage)}
		}
		s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: aggJQ, Data: dict})
		if err != nil {
			t.Fatalf("agg jq failed: %v", err)
		}
		var out map[string]any
		_ = json.Unmarshal([]byte(s), &out)
		return out
	}

	unpaged := full(false, 0, 0) // {count:30, page_total:0}
	// The unpaginated full result has NO array-valued key → GoSliceFullList
	// fails the shape contract → ok=false → fall-back.
	if _, ok := GoSliceFullList(unpaged, 0, 10); ok {
		t.Fatalf("aggregation shape (no single array) should fail the GoSliceFullList shape contract")
	}

	// Even if a shape DID slice, the byte-verify catches it: the RA's
	// paginated output differs from any Go-slice over the unpaginated full.
	paged := full(true, 1, 10) // {count:30, page_total:10}  (page_total != 0)
	ub, _ := json.Marshal(unpaged)
	pb, _ := json.Marshal(paged)
	if string(ub) == string(pb) {
		t.Fatalf("aggregation byte-verify should DIFFER (paginated output depends on slice)")
	}
}

// HG-4a.3 — sliceability memo per (key × sliceShape). Two distinct
// sliceShapes over the SAME RAFullList key get INDEPENDENT verdicts and never
// cross-apply.
func TestRAFullList_SliceabilityMemoPerShape(t *testing.T) {
	resetSliceabilityMemoForTest()

	raKey := ComputeKey(RAFullListKeyInputsForTest("composition.krateo.io", "v1", "panels",
		"krateo-system", "compositions-panels", "uid-1234", nil))

	// Widget A: a table widget that slices cleanly.
	shapeA := SliceShapeHash("widgets", "widgets.templates.krateo.io", "v1beta1",
		"tables", "krateo-system", "compositions-table", raSliceJQ)
	// Widget B: a different caller (a chart widget) with a different slice jq.
	shapeB := SliceShapeHash("widgets", "widgets.templates.krateo.io", "v1beta1",
		"charts", "krateo-system", "compositions-chart", "{ sum: ((.x)|add) }")

	if shapeA == shapeB {
		t.Fatalf("distinct callers/jq produced identical sliceShape — verdicts would cross-apply")
	}

	// No verdict yet for either.
	if _, known := SliceabilityLookup(raKey, shapeA); known {
		t.Fatalf("shapeA should be unknown before record")
	}

	// A is sliceable; B is NOT sliceable.
	RecordSliceability(raKey, shapeA, true)
	RecordSliceability(raKey, shapeB, false)

	gotA, knownA := SliceabilityLookup(raKey, shapeA)
	gotB, knownB := SliceabilityLookup(raKey, shapeB)
	if !knownA || !gotA {
		t.Fatalf("shapeA verdict lost/wrong: known=%v sliceable=%v", knownA, gotA)
	}
	if !knownB || gotB {
		t.Fatalf("shapeB verdict lost/wrong (must be NOT sliceable): known=%v sliceable=%v", knownB, gotB)
	}
	// Cross-apply guard: A's TRUE verdict must NOT make B look sliceable.
	if gotA == gotB {
		t.Fatalf("shapeA and shapeB share a verdict — the per-shape memo cross-applied")
	}

	// A DIFFERENT RAFullList key with shapeA gets its OWN (unknown) verdict.
	otherKey := ComputeKey(RAFullListKeyInputsForTest("composition.krateo.io", "v1", "panels",
		"OTHER-NS", "compositions-panels", "uid-1234", nil))
	if _, known := SliceabilityLookup(otherKey, shapeA); known {
		t.Fatalf("verdict leaked across RAFullList keys")
	}
}

// HG-4a.4a — Pinned entries are SKIPPED by evictUntilUnderCapsLocked under
// transient cap pressure while transient entries evict.
func TestRAFullList_PinnedSkipsLRUEviction(t *testing.T) {
	// Transient entry cap = 2; generous transient bytes; generous resident.
	c := newResolvedCache(2, 1<<30, time.Hour)
	c.maxResidentBytes = 1 << 30

	pinnedEntry := func(payload string) *ResolvedEntry {
		return &ResolvedEntry{
			RawJSON: []byte(payload),
			Pinned:  true,
			Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Name: "pinned"},
		}
	}
	transientEntry := func(payload string) *ResolvedEntry {
		return &ResolvedEntry{
			RawJSON: []byte(payload),
			Inputs:  &ResolvedKeyInputs{CacheEntryClass: "restactions"},
		}
	}

	// One PINNED cell.
	c.Put("pin1", pinnedEntry("PINNED-FULL-LIST"))
	// Fill transient region past its entry cap to force LRU eviction.
	c.Put("t1", transientEntry("a"))
	c.Put("t2", transientEntry("b"))
	c.Put("t3", transientEntry("c")) // evicts a transient (t1), NOT the pin

	if _, ok := c.Get("pin1"); !ok {
		t.Fatalf("PINNED entry was evicted under transient LRU pressure — pin protection broken")
	}
	if _, ok := c.Get("t1"); ok {
		t.Fatalf("t1 (LRU transient) should have evicted")
	}
	s := c.Stats()
	if s.ResidentEntries != 1 {
		t.Fatalf("resident_entries = %d, want 1", s.ResidentEntries)
	}
	if s.ResidentPinTotal != 1 {
		t.Fatalf("resident_pin_total = %d, want 1", s.ResidentPinTotal)
	}
	// The pinned bytes are NOT in the transient budget.
	if s.Bytes > 3 {
		t.Fatalf("transient bytes %d include the pinned cell — budgets not separated", s.Bytes)
	}
	if s.ResidentBytes != int64(len("PINNED-FULL-LIST")) {
		t.Fatalf("resident_bytes = %d, want %d", s.ResidentBytes, len("PINNED-FULL-LIST"))
	}
}

// HG-4a.4b — maxResidentBytes==0 DISABLES pinning (kill-switch): a pin
// request is demoted to transient.
func TestRAFullList_ResidentKillSwitch(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.maxResidentBytes = 0 // kill-switch

	c.Put("k", &ResolvedEntry{
		RawJSON: []byte("x"),
		Pinned:  true,
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList},
	})
	got, ok := c.Get("k")
	if !ok {
		t.Fatalf("entry missing")
	}
	if got.Pinned {
		t.Fatalf("pin should be DEMOTED when maxResidentBytes==0 (kill-switch)")
	}
	s := c.Stats()
	if s.ResidentEntries != 0 || s.ResidentBytes != 0 {
		t.Fatalf("resident region should be empty under kill-switch: entries=%d bytes=%d", s.ResidentEntries, s.ResidentBytes)
	}
	if s.ResidentDemoteTotal != 1 {
		t.Fatalf("resident_demote_total = %d, want 1", s.ResidentDemoteTotal)
	}
}

// HG-4a.4c — resident budget OVERFLOW demotes to transient rather than
// evicting another pinned cell.
func TestRAFullList_ResidentBudgetOverflowDemotes(t *testing.T) {
	c := newResolvedCache(100, 1<<20, time.Hour)
	c.maxResidentBytes = 10 // only ~10 bytes resident

	pin := func(name, payload string) {
		c.Put(name, &ResolvedEntry{
			RawJSON: []byte(payload),
			Pinned:  true,
			Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Name: name},
		})
	}
	pin("a", "12345")  // 5 bytes resident — fits
	pin("b", "678901") // +6 = 11 > 10 → DEMOTE b to transient
	ea, _ := c.Get("a")
	eb, _ := c.Get("b")
	if ea == nil || !ea.Pinned {
		t.Fatalf("a should remain pinned")
	}
	if eb == nil || eb.Pinned {
		t.Fatalf("b should be DEMOTED on resident overflow (a must NOT be evicted)")
	}
	s := c.Stats()
	if s.ResidentEntries != 1 {
		t.Fatalf("resident_entries = %d, want 1 (only a)", s.ResidentEntries)
	}
	if s.ResidentDemoteTotal != 1 {
		t.Fatalf("resident_demote_total = %d, want 1", s.ResidentDemoteTotal)
	}
}

// HG-4a.4d — re-pin in place (refresher re-resolve) keeps the cell resident
// and updates resident bytes; demote-then-repin moves bytes between budgets.
func TestRAFullList_RePinInPlace(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.maxResidentBytes = 1 << 20

	mk := func(payload string, pinned bool) *ResolvedEntry {
		return &ResolvedEntry{
			RawJSON: []byte(payload),
			Pinned:  pinned,
			Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Name: "x"},
		}
	}
	c.Put("k", mk("aaa", true)) // resident, 3 bytes
	if s := c.Stats(); s.ResidentEntries != 1 || s.ResidentBytes != 3 {
		t.Fatalf("after first pin: entries=%d bytes=%d", s.ResidentEntries, s.ResidentBytes)
	}
	// Refresher re-resolve: same key, still pinned, larger payload.
	c.Put("k", mk("bbbbb", true))
	if s := c.Stats(); s.ResidentEntries != 1 || s.ResidentBytes != 5 {
		t.Fatalf("after re-pin: entries=%d bytes=%d (want 1/5)", s.ResidentEntries, s.ResidentBytes)
	}
	if got, _ := c.Get("k"); string(got.RawJSON) != "bbbbb" || !got.Pinned {
		t.Fatalf("re-pin lost payload/pin")
	}
	// Now write the SAME key transient (e.g. a per-user fallback path) —
	// bytes move from resident to transient.
	c.Put("k", mk("cc", false))
	if s := c.Stats(); s.ResidentEntries != 0 || s.ResidentBytes != 0 {
		t.Fatalf("after demote-in-place: resident entries=%d bytes=%d (want 0/0)", s.ResidentEntries, s.ResidentBytes)
	}
	if s := c.Stats(); s.Bytes != 2 {
		t.Fatalf("transient bytes after demote-in-place = %d, want 2", s.Bytes)
	}
}

// HG-4a.4e — race: concurrent dual-resolve (Put pinned + Put transient + Get)
// against the resident region + the memo sync.Map. Run under -race.
func TestRAFullList_ResidentAndMemoRace(t *testing.T) {
	resetSliceabilityMemoForTest()
	c := newResolvedCache(200, 1<<24, time.Hour)
	c.maxResidentBytes = 1 << 24

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				key := fmt.Sprintf("k%d-%d", w, i%11)
				pinned := (i % 3) == 0
				c.Put(key, &ResolvedEntry{
					RawJSON: []byte(fmt.Sprintf("payload-%d-%d", w, i)),
					Pinned:  pinned,
					Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Name: key},
				})
				c.Get(key)
				// Memo race.
				raKey := fmt.Sprintf("rakey-%d", i%5)
				shape := SliceShapeHash("widgets", "g", "v", "r", "ns", fmt.Sprintf("w%d", w), raSliceJQ)
				if _, known := SliceabilityLookup(raKey, shape); !known {
					RecordSliceability(raKey, shape, (i%2) == 0)
				}
			}
		}()
	}
	wg.Wait()
	// Sanity: byte counters never went negative (defensive clamps hold).
	s := c.Stats()
	if s.Bytes < 0 || s.ResidentBytes < 0 {
		t.Fatalf("negative byte counters: transient=%d resident=%d", s.Bytes, s.ResidentBytes)
	}
}
