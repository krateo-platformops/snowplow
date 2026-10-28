// resolved_test.go — Tag 0.30.7 binding: unit coverage for the
// resolved-output cache scaffold. Plan §"What's implemented" calls for
// hit/miss accounting, bounded-cache LRU eviction at cap, byte-budget
// eviction at cap, and basic Get/Put.
//
// We additionally cover:
//   - ResolvedCacheEnabled() obeys CACHE_ENABLED + RESOLVED_CACHE_ENABLED
//   - ComputeKey is stable across calls and sensitive to every input
//   - TTL expiry behaves like a miss + drops the entry
//   - Concurrent Get/Put is race-detector clean
//
// Each test resets the package singleton + env vars to avoid order
// dependencies.

package cache

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestResolvedCacheEnabled_CacheDisabledMeansL1Off(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	if ResolvedCacheEnabled() {
		t.Fatalf("ResolvedCacheEnabled() should be false when CACHE_ENABLED=false (cache subsystem off)")
	}
	if ResolvedCache() != nil {
		t.Fatalf("ResolvedCache() should return nil when CACHE_ENABLED=false")
	}
}

func TestResolvedCacheEnabled_PerFeatureToggleOff(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "false")
	if ResolvedCacheEnabled() {
		t.Fatalf("RESOLVED_CACHE_ENABLED=false must disable L1 even when CACHE_ENABLED=true")
	}
}

func TestResolvedCacheEnabled_DefaultsOnWhenCacheEnabledOn(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "")
	if !ResolvedCacheEnabled() {
		t.Fatalf("default for RESOLVED_CACHE_ENABLED should be ON when CACHE_ENABLED=true")
	}
}

func TestComputeKey_StableAcrossCalls(t *testing.T) {
	in := ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "compositionsgrids",
		Namespace:       "demo",
		Name:            "main",
		BindingUID:      "uid-0123456789abcdef", // Ship 0.30.242 H.c-layered (replaces BindingSetHash uint64)
		PerPage:         20,
		Page:            1,
		Extras:          map[string]any{"foo": "bar", "n": float64(7)},
	}
	a := ComputeKey(in)
	b := ComputeKey(in)
	if a != b {
		t.Fatalf("ComputeKey not stable: %q vs %q", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("ComputeKey should be 64 hex chars (sha256), got %d", len(a))
	}
}

// Ship 0.30.242 H.c-layered Phase 2c — BindingUID (per-layer first-match
// binding identity) replaces BindingSetHash uint64 as the identity fold.
// At the ComputeKey layer two inputs with the same BindingUID hash
// identically by construction.
func TestComputeKey_BindingUIDInvariant(t *testing.T) {
	in1 := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-0042"}
	in2 := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-0042"}
	if ComputeKey(in1) != ComputeKey(in2) {
		t.Fatalf("ComputeKey should be deterministic on identical BindingUID; got divergent keys")
	}
}

func TestComputeKey_SensitiveToEveryField(t *testing.T) {
	base := ResolvedKeyInputs{
		CacheEntryClass: "widgets", Group: "g", Version: "v", Resource: "r",
		Namespace: "ns", Name: "n", BindingUID: "uid-01",
		PerPage: 1, Page: 1, Extras: map[string]any{"k": "v"},
	}
	mutators := []struct {
		name string
		fn   func(*ResolvedKeyInputs)
	}{
		{"CacheEntryClass", func(in *ResolvedKeyInputs) { in.CacheEntryClass = "restactions" }},
		{"Group", func(in *ResolvedKeyInputs) { in.Group = "g2" }},
		{"Version", func(in *ResolvedKeyInputs) { in.Version = "v2" }},
		{"Resource", func(in *ResolvedKeyInputs) { in.Resource = "r2" }},
		{"Namespace", func(in *ResolvedKeyInputs) { in.Namespace = "ns2" }},
		{"Name", func(in *ResolvedKeyInputs) { in.Name = "n2" }},
		{"BindingUID", func(in *ResolvedKeyInputs) { in.BindingUID = "uid-02" }},
		{"PerPage", func(in *ResolvedKeyInputs) { in.PerPage = 2 }},
		{"Page", func(in *ResolvedKeyInputs) { in.Page = 2 }},
		{"Extras", func(in *ResolvedKeyInputs) { in.Extras = map[string]any{"k": "w"} }},
		{"Stage", func(in *ResolvedKeyInputs) { in.Stage = "stage\x1fcompositions\x1f\x1f" }},
		{"Query", func(in *ResolvedKeyInputs) { in.Query = "labelSelector=env%3Dprod" }},
	}
	baseKey := ComputeKey(base)
	for _, m := range mutators {
		t.Run(m.name, func(t *testing.T) {
			mutated := base
			// deep-copy maps that mutators rebind
			mutated.Extras = map[string]any{}
			for k, v := range base.Extras {
				mutated.Extras[k] = v
			}
			m.fn(&mutated)
			if ComputeKey(mutated) == baseKey {
				t.Fatalf("changing %s did not change the key — coalesced inputs", m.name)
			}
		})
	}
}

func TestResolvedCache_BasicGetPut(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	if _, ok := c.Get("nope"); ok {
		t.Fatalf("Get on empty cache must miss")
	}
	c.put("k", &ResolvedEntry{RawJSON: []byte(`{"a":1}`)})
	got, ok := c.Get("k")
	if !ok {
		t.Fatalf("Get after Put should hit")
	}
	if string(got.RawJSON) != `{"a":1}` {
		t.Fatalf("RawJSON round-trip wrong: %q", got.RawJSON)
	}
	s := c.Stats()
	if s.HitTotal != 1 || s.MissTotal != 1 || s.StoreTotal != 1 {
		t.Fatalf("counters wrong: %+v", s)
	}
}

func TestResolvedCache_LRUEvictionAtEntryCap(t *testing.T) {
	c := newResolvedCache(3, 1<<30, time.Hour) // 3 entries, generous bytes
	// Insert 3 entries.
	for i := 0; i < 3; i++ {
		c.put(fmt.Sprintf("k%d", i), &ResolvedEntry{RawJSON: []byte("x")})
	}
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", c.Len())
	}
	// Touch k0 to make it MRU.
	c.Get("k0")
	// Add a fourth — k1 (now LRU) should evict.
	c.put("k3", &ResolvedEntry{RawJSON: []byte("x")})
	if _, ok := c.Get("k1"); ok {
		t.Fatalf("k1 should have been LRU-evicted")
	}
	if _, ok := c.Get("k0"); !ok {
		t.Fatalf("k0 was touched so should survive")
	}
	if c.Stats().EvictLRUTotal != 1 {
		t.Fatalf("expected 1 LRU eviction, got %d", c.Stats().EvictLRUTotal)
	}
}

func TestResolvedCache_ByteBudgetEviction(t *testing.T) {
	// Entries are ~100 bytes; budget = 250 bytes -> at most 2 fit.
	c := newResolvedCache(1000, 250, time.Hour)
	payload := strings.Repeat("a", 100)
	c.put("k1", &ResolvedEntry{RawJSON: []byte(payload)})
	c.put("k2", &ResolvedEntry{RawJSON: []byte(payload)})
	c.put("k3", &ResolvedEntry{RawJSON: []byte(payload)})
	if got := c.Bytes(); got > 250 {
		t.Fatalf("byte budget violated: %d > 250", got)
	}
	if _, ok := c.Get("k1"); ok {
		t.Fatalf("k1 should have been evicted by byte-budget pressure")
	}
}

func TestResolvedCache_ReplaceInPlace(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.put("k", &ResolvedEntry{RawJSON: []byte("aaa")})
	c.put("k", &ResolvedEntry{RawJSON: []byte("bbbbb")})
	if c.Len() != 1 {
		t.Fatalf("expected 1 entry after replace, got %d", c.Len())
	}
	if got := c.Bytes(); got != 5 {
		t.Fatalf("expected 5 bytes after replace, got %d", got)
	}
	got, ok := c.Get("k")
	if !ok || string(got.RawJSON) != "bbbbb" {
		t.Fatalf("replace-in-place semantics broken: got=%+v ok=%v", got, ok)
	}
}

func TestResolvedCache_TTLExpiry(t *testing.T) {
	c := newResolvedCache(10, 1<<20, 10*time.Millisecond)
	c.put("k", &ResolvedEntry{RawJSON: []byte("x")})
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("immediate Get should hit")
	}
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatalf("Get after TTL should miss")
	}
	if c.Stats().EvictTTLTotal != 1 {
		t.Fatalf("expected 1 TTL eviction, got %d", c.Stats().EvictTTLTotal)
	}
}

func TestResolvedCache_StatsHitRate(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.put("k", &ResolvedEntry{RawJSON: []byte("x")})
	c.Get("k") // hit
	c.Get("k") // hit
	c.Get("k") // hit
	c.Get("y") // miss
	s := c.Stats()
	if s.HitTotal != 3 || s.MissTotal != 1 {
		t.Fatalf("counters wrong: %+v", s)
	}
	if hr := s.HitRate(); hr <= 0.74 || hr >= 0.76 {
		t.Fatalf("hit_rate should be ~0.75, got %f", hr)
	}
}

func TestResolvedCache_ConcurrentSafe(t *testing.T) {
	// Race-detector-clean concurrent Get/Put against the same cache.
	c := newResolvedCache(100, 1<<20, time.Hour)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("k%d-%d", w, i%17)
				if i%2 == 0 {
					c.put(k, &ResolvedEntry{RawJSON: []byte("x")})
				} else {
					c.Get(k)
				}
			}
		}()
	}
	wg.Wait()
}

func TestResolvedCache_EmptyTreatedAsMiss(t *testing.T) {
	// A nil receiver must not panic — defensive coding for callers
	// that take the L1 disabled path.
	var c *ResolvedCacheStore
	if _, ok := c.Get("x"); ok {
		t.Fatalf("nil cache must miss, not hit")
	}
	c.put("x", &ResolvedEntry{RawJSON: []byte("y")}) // must not panic
	if c.Len() != 0 || c.Bytes() != 0 {
		t.Fatalf("nil cache should report zero metrics")
	}
}

// --- Ship E (0.30.116) — api-stage L1 ---------------------------------------

// TestComputeKey_EmptyStageByteIdenticalToPreShipE asserts AC-E1 at the
// key layer: a ResolvedKeyInputs with Stage=="" (every "restactions" /
// "widgets" entry) hashes byte-identically whether the Stage field
// exists or not — ComputeKey folds Stage in ONLY when non-empty, so no
// pre-0.30.116 entry's key shifts on the rolling restart.
func TestComputeKey_EmptyStageByteIdenticalToPreShipE(t *testing.T) {
	in := ResolvedKeyInputs{
		CacheEntryClass: "restactions", Group: "g", Version: "v", Resource: "r",
		Namespace: "ns", Name: "n", BindingUID: "uid-0abc",
		PerPage: 1, Page: 1,
	}
	withEmptyStage := in
	withEmptyStage.Stage = ""
	if ComputeKey(in) != ComputeKey(withEmptyStage) {
		t.Fatalf("AC-E1: an empty Stage shifted the key — a non-apistage entry " +
			"must hash identically to pre-0.30.116")
	}
	// A non-empty Stage MUST shift the key (it is the apistage discriminator).
	withStage := in
	withStage.Stage = "stage\x1fcompositions\x1fabc\x1fdef"
	if ComputeKey(in) == ComputeKey(withStage) {
		t.Fatalf("a non-empty Stage did not change the key — apistage entries would collide")
	}
}

// TestApistageEvictPressure asserts AC-E7: the O6 budget signal is the
// evict/store ratio, 0 when no api-stage entry was ever stored.
func TestApistageEvictPressure(t *testing.T) {
	var s ResolvedCacheStats
	if got := s.ApistageEvictPressure(); got != 0 {
		t.Fatalf("ApistageEvictPressure with zero stores = %v, want 0", got)
	}
	s.ApistageStoreTotal = 10
	s.ApistageEvictTotal = 3
	if got := s.ApistageEvictPressure(); got != 0.3 {
		t.Fatalf("ApistageEvictPressure = %v, want 0.3", got)
	}
}

// TestApistageCounters_ClassifiedByCacheEntryClass asserts the store counts
// api-stage Put()s + evictions via the entry's CacheEntryClass — a non-
// apistage entry never moves the api-stage counters (AC-E7).
func TestApistageCounters_ClassifiedByCacheEntryClass(t *testing.T) {
	c := newResolvedCache(1, 1<<20, time.Hour) // entry cap 1 → next Put evicts

	// A non-apistage entry: api-stage counters stay 0.
	c.put("plain", &ResolvedEntry{
		RawJSON: []byte(`{}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: "restactions"},
	})
	if s := c.Stats(); s.ApistageStoreTotal != 0 {
		t.Fatalf("non-apistage Put bumped apistage_store_total to %d", s.ApistageStoreTotal)
	}

	// An api-stage entry: store counter ticks; the cap-1 store evicts the
	// "plain" entry (non-apistage → apistage_evict stays 0).
	c.put("stageA", &ResolvedEntry{
		RawJSON: []byte(`{"value":1}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Stage: "s1"},
	})
	if s := c.Stats(); s.ApistageStoreTotal != 1 {
		t.Fatalf("apistage Put: apistage_store_total=%d want 1", s.ApistageStoreTotal)
	}
	if s := c.Stats(); s.ApistageEvictTotal != 0 {
		t.Fatalf("evicting a non-apistage entry bumped apistage_evict_total to %d", s.ApistageEvictTotal)
	}

	// A second api-stage Put evicts the first api-stage entry → apistage
	// evict counter ticks.
	c.put("stageB", &ResolvedEntry{
		RawJSON: []byte(`{"value":2}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Stage: "s2"},
	})
	if s := c.Stats(); s.ApistageEvictTotal != 1 {
		t.Fatalf("evicting an apistage entry: apistage_evict_total=%d want 1", s.ApistageEvictTotal)
	}
}

// TestApistageL1Enabled_FoldedUnderResolvedCache asserts the #57 fold
// (F-FOLD-1 truth table): the api-stage L1 is now on iff
// ResolvedCacheEnabled() — no per-feature env flag. It is off when either
// master gate is off and on when both are open, regardless of the
// (now-ignored) RESOLVED_CACHE_APISTAGE_ENABLED env.
func TestApistageL1Enabled_FoldedUnderResolvedCache(t *testing.T) {
	// CACHE off → apistage off.
	t.Setenv("CACHE_ENABLED", "false")
	if ApistageL1Enabled() {
		t.Fatalf("F-FOLD-1: apistage L1 active with CACHE_ENABLED=false")
	}
	// CACHE on, RESOLVED_CACHE off → apistage off.
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "false")
	if ApistageL1Enabled() {
		t.Fatalf("F-FOLD-1: apistage L1 active with RESOLVED_CACHE_ENABLED=false")
	}
	// Both master gates open → on (default), no per-feature opt-in needed.
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	if !ApistageL1Enabled() {
		t.Fatalf("F-FOLD-1: apistage L1 must be on when both master gates are open")
	}
}

// TestApistageL1Enabled_ApistageEnvNoLongerConsulted is F-FOLD-2: with
// both master gates on, the retired RESOLVED_CACHE_APISTAGE_ENABLED env
// has no effect — toggling it through every shape leaves the result
// invariantly true.
func TestApistageL1Enabled_ApistageEnvNoLongerConsulted(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	for _, v := range []string{"", "false", "true", "garbage"} {
		t.Setenv("RESOLVED_CACHE_APISTAGE_ENABLED", v)
		if !ApistageL1Enabled() {
			t.Fatalf("F-FOLD-2: apistage L1 must stay on regardless of retired env=%q", v)
		}
	}
}

// TestEntryBytes_DecodedFormWeighsMoreThanRaw is the Ship 0.30.128
// P-CORE-2 PM-binding LRU-accounting test. An apistage content entry
// carries the decoded structured form (the R3 Items tree) so a cache hit
// hands back the already-decoded value. The LRU maxBytes accounting MUST
// count that decoded resident size, not just the RawJSON length —
// otherwise every content entry silently under-counts and the cache size
// bound (curBytes vs maxBytes) is broken.
//
// This asserts entryBytes for an entry CARRYING the decoded form weighs
// STRICTLY MORE than the same entry's raw byte length — i.e. the decoded
// tree is genuinely accounted, not dropped.
func TestEntryBytes_DecodedFormWeighsMoreThanRaw(t *testing.T) {
	raw := []byte(`{"apiVersion":"composition.krateo.io/v1","kind":"GithubScaffoldingsList",` +
		`"items":[{"metadata":{"name":"c1","namespace":"ns-a"}},` +
		`{"metadata":{"name":"c2","namespace":"ns-b"}}]}`)

	// Entry with ONLY RawJSON — no decoded form.
	rawOnly := &ResolvedEntry{
		RawJSON: raw,
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage},
	}
	// Same entry, additionally carrying the decoded Items tree (what an
	// apistage LIST content entry holds post-R3 / P-CORE-2).
	withDecoded := &ResolvedEntry{
		RawJSON: raw,
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage},
		Items: []*unstructured.Unstructured{
			{Object: map[string]any{"metadata": map[string]any{"name": "c1", "namespace": "ns-a"}}},
			{Object: map[string]any{"metadata": map[string]any{"name": "c2", "namespace": "ns-b"}}},
		},
		ItemsAPIVersion: "composition.krateo.io/v1",
		ItemsKind:       "GithubScaffoldingsList",
	}

	rawWeight := entryBytes(rawOnly)
	decodedWeight := entryBytes(withDecoded)

	// The raw-only weight is exactly the byte length.
	if rawWeight != int64(len(raw)) {
		t.Fatalf("entryBytes(raw-only) = %d, want exactly len(RawJSON) = %d",
			rawWeight, len(raw))
	}
	// The decoded-form entry MUST weigh strictly more — the decoded tree
	// is counted, not dropped. A miss here means the LRU cap under-counts
	// every apistage content entry (the PM-binding failure mode).
	if decodedWeight <= rawWeight {
		t.Fatalf("LRU under-count: entryBytes for an entry carrying the decoded form "+
			"= %d, NOT strictly greater than the raw byte length %d — the decoded "+
			"resident tree is not being accounted; the maxBytes bound is broken",
			decodedWeight, rawWeight)
	}
}
