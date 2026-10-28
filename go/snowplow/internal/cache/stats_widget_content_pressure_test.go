// stats_widget_content_pressure_test.go — L8 hermetic coverage for the
// per-class evict-pressure ratio accessors on ResolvedCacheStats, mirroring
// TestApistageEvictPressure (resolved_test.go):
//
//	WidgetContentEvictPressure = WidgetContentEvictTotal / WidgetContentStoreTotal
//	RAFullListEvictPressure    = RAFullListEvictTotal    / RAFullListStoreTotal
//
// Both are 0 when their OWN store counter is 0 (never divide-by-zero, and never
// borrow another class's denominator). The discriminating power ("divide by the
// wrong field") is guaranteed by giving every class DISTINCT store/evict
// counters, so any cross-field numerator/denominator swap yields a ratio the
// assertions reject. A dedicated RED helper (assertNotWrongField) makes that
// discrimination explicit: it enumerates every wrong-field ratio the method
// could have computed and proves each differs from the correct one.
//
// Also asserts the counters are attributed by the entry's CacheEntryClass via
// real Put()/eviction (mirrors TestApistageCounters_ClassifiedByCacheEntryClass)
// so the ratio is fed by the real store, not just hand-set struct fields.

package cache

import (
	"testing"
	"time"
)

// TestWidgetContentEvictPressure_RatioArithmetic mirrors TestApistageEvictPressure
// for the Ship G widget-content class.
func TestWidgetContentEvictPressure_RatioArithmetic(t *testing.T) {
	var s ResolvedCacheStats
	if got := s.WidgetContentEvictPressure(); got != 0 {
		t.Fatalf("WidgetContentEvictPressure with zero stores = %v, want 0 (no divide-by-zero)", got)
	}

	// Distinct per-class counters: a wrong-field divide is detectable.
	s.WidgetContentStoreTotal = 8
	s.WidgetContentEvictTotal = 2
	s.RAFullListStoreTotal = 5
	s.RAFullListEvictTotal = 4
	s.ApistageStoreTotal = 100 // distractor denominator
	s.ApistageEvictTotal = 7   // distractor numerator

	if got := s.WidgetContentEvictPressure(); got != 0.25 {
		t.Fatalf("WidgetContentEvictPressure = %v, want 0.25 (2/8)", got)
	}
	// RED-arm discrimination: prove the correct ratio is not accidentally equal
	// to any wrong-field ratio the method could compute (2/100, 4/8, 7/8, ...).
	assertNotWrongField(t, "WidgetContentEvictPressure", s.WidgetContentEvictPressure(),
		[]float64{
			ratio(s.WidgetContentEvictTotal, s.ApistageStoreTotal),   // wrong denom: apistage
			ratio(s.WidgetContentEvictTotal, s.RAFullListStoreTotal), // wrong denom: raFullList
			ratio(s.ApistageEvictTotal, s.WidgetContentStoreTotal),   // wrong num: apistage
			ratio(s.RAFullListEvictTotal, s.WidgetContentStoreTotal), // wrong num: raFullList
		})
}

// TestRAFullListEvictPressure_RatioArithmetic mirrors TestApistageEvictPressure
// for the Ship 4a raFullList class.
func TestRAFullListEvictPressure_RatioArithmetic(t *testing.T) {
	var s ResolvedCacheStats
	if got := s.RAFullListEvictPressure(); got != 0 {
		t.Fatalf("RAFullListEvictPressure with zero stores = %v, want 0 (no divide-by-zero)", got)
	}

	s.RAFullListStoreTotal = 5
	s.RAFullListEvictTotal = 4
	s.WidgetContentStoreTotal = 8
	s.WidgetContentEvictTotal = 2
	s.ApistageStoreTotal = 100
	s.ApistageEvictTotal = 7

	if got := s.RAFullListEvictPressure(); got != 0.8 {
		t.Fatalf("RAFullListEvictPressure = %v, want 0.8 (4/5)", got)
	}
	assertNotWrongField(t, "RAFullListEvictPressure", s.RAFullListEvictPressure(),
		[]float64{
			ratio(s.RAFullListEvictTotal, s.ApistageStoreTotal),      // wrong denom: apistage
			ratio(s.RAFullListEvictTotal, s.WidgetContentStoreTotal), // wrong denom: widgetContent
			ratio(s.ApistageEvictTotal, s.RAFullListStoreTotal),      // wrong num: apistage
			ratio(s.WidgetContentEvictTotal, s.RAFullListStoreTotal), // wrong num: widgetContent
		})
}

// ratio is float64(a)/float64(b) with the same zero-guard the real accessors use.
func ratio(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// assertNotWrongField is the RED discriminator: it proves the correct ratio is
// strictly different from every wrong-field ratio the accessor could have
// computed. If a wrong-field impl HAPPENED to produce the same value, the test
// would be blind to that divide-by-the-wrong-field defect — so we fail loudly
// if any collision exists, and otherwise confirm the discrimination margin.
func assertNotWrongField(t *testing.T, name string, correct float64, wrong []float64) {
	t.Helper()
	for i, w := range wrong {
		if w == correct {
			t.Fatalf("%s: wrong-field ratio #%d == correct ratio %v — the distinct-counter "+
				"discrimination collapsed; the test cannot catch a divide-by-the-wrong-field defect",
				name, i, correct)
		}
	}
}

// TestWidgetContentCounters_ClassifiedByCacheEntryClass mirrors
// TestApistageCounters_ClassifiedByCacheEntryClass: the widget-content store +
// evict counters move ONLY for widget-content entries, driving the pressure
// ratio off the REAL store (not hand-set fields). A non-widgetContent Put must
// not touch them; a second widgetContent Put at cap-1 evicts the first →
// evict/store = 1/2 = 0.5.
func TestWidgetContentCounters_ClassifiedByCacheEntryClass(t *testing.T) {
	c := newResolvedCache(1, 1<<20, time.Hour) // entry cap 1 → next Put evicts

	// A non-widgetContent entry: widget-content counters stay 0.
	c.put("plain", &ResolvedEntry{
		RawJSON: []byte(`{}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: "restactions"},
	})
	if s := c.Stats(); s.WidgetContentStoreTotal != 0 || s.WidgetContentEvictTotal != 0 {
		t.Fatalf("non-widgetContent Put moved widget counters: store=%d evict=%d",
			s.WidgetContentStoreTotal, s.WidgetContentEvictTotal)
	}

	// First widgetContent entry: store ticks; evicting the plain entry does NOT
	// bump the widget-content evict counter (class-attributed).
	c.put("wcA", &ResolvedEntry{
		RawJSON: []byte(`{"v":1}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgetContent, Namespace: "ns", Name: "a"},
	})
	if s := c.Stats(); s.WidgetContentStoreTotal != 1 || s.WidgetContentEvictTotal != 0 {
		t.Fatalf("first widgetContent Put: store=%d want 1, evict=%d want 0",
			s.WidgetContentStoreTotal, s.WidgetContentEvictTotal)
	}

	// Second widgetContent entry evicts the first widgetContent entry → evict
	// counter ticks; ratio = 1/2.
	c.put("wcB", &ResolvedEntry{
		RawJSON: []byte(`{"v":2}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgetContent, Namespace: "ns", Name: "b"},
	})
	s := c.Stats()
	if s.WidgetContentStoreTotal != 2 || s.WidgetContentEvictTotal != 1 {
		t.Fatalf("second widgetContent Put: store=%d want 2, evict=%d want 1",
			s.WidgetContentStoreTotal, s.WidgetContentEvictTotal)
	}
	if got := s.WidgetContentEvictPressure(); got != 0.5 {
		t.Fatalf("WidgetContentEvictPressure off the real store = %v, want 0.5 (1/2)", got)
	}
}

// TestRAFullListCounters_ClassifiedByCacheEntryClass mirrors the above for the
// raFullList class, driving RAFullListEvictPressure off the real store. A
// raFullList entry is un-pinned here (Pinned defaults false) so the cap-1 LRU
// sweep can evict it.
func TestRAFullListCounters_ClassifiedByCacheEntryClass(t *testing.T) {
	c := newResolvedCache(1, 1<<20, time.Hour)

	c.put("plain", &ResolvedEntry{
		RawJSON: []byte(`{}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: "restactions"},
	})
	if s := c.Stats(); s.RAFullListStoreTotal != 0 || s.RAFullListEvictTotal != 0 {
		t.Fatalf("non-raFullList Put moved raFullList counters: store=%d evict=%d",
			s.RAFullListStoreTotal, s.RAFullListEvictTotal)
	}

	c.put("rfA", &ResolvedEntry{
		RawJSON: []byte(`{"items":[1]}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Name: "a"},
	})
	if s := c.Stats(); s.RAFullListStoreTotal != 1 || s.RAFullListEvictTotal != 0 {
		t.Fatalf("first raFullList Put: store=%d want 1, evict=%d want 0",
			s.RAFullListStoreTotal, s.RAFullListEvictTotal)
	}

	c.put("rfB", &ResolvedEntry{
		RawJSON: []byte(`{"items":[2]}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Name: "b"},
	})
	s := c.Stats()
	if s.RAFullListStoreTotal != 2 || s.RAFullListEvictTotal != 1 {
		t.Fatalf("second raFullList Put: store=%d want 2, evict=%d want 1",
			s.RAFullListStoreTotal, s.RAFullListEvictTotal)
	}
	if got := s.RAFullListEvictPressure(); got != 0.5 {
		t.Fatalf("RAFullListEvictPressure off the real store = %v, want 0.5 (1/2)", got)
	}
}
