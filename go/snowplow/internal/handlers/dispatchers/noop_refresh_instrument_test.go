// noop_refresh_instrument_test.go — falsifier for the #604 instrument.
//
// ARM-1 is the one that matters and the one that is RED without the production
// change: it drives the REAL refresher entry point (resolveAndPopulateL1) and
// asserts the counters moved. A classifier that is correct but never called
// measures nothing, and that is exactly the failure mode #574 shipped with
// (live and inert for three days) and that #598's memo had for its whole
// existence before #599 wired it. The pure-function arms below cannot catch it.
//
// ARM-5 pins the property that makes the measurement trustworthy: the
// instrument OBSERVES and does not skip. If a future change gates the store on
// an identical verdict, the number this instrument produces stops describing
// the system it is measuring.

package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func instrumentCacheOn(t *testing.T) *cache.ResolvedCacheStore {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	return cache.ResolvedCache()
}

func instrumentInputs(name string) cache.ResolvedKeyInputs {
	return cache.ResolvedKeyInputs{
		CacheEntryClass: "restactions",
		Name:            name,
		Namespace:       "krateo-system",
	}
}

// drive runs one refresher re-resolve whose resolver returns `produces`.
func drive(t *testing.T, inputs cache.ResolvedKeyInputs, produces []byte) {
	t.Helper()
	prev := resolveOnceFn
	resolveOnceFn = func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return produces, nil
	}
	t.Cleanup(func() { resolveOnceFn = prev })
	if err := resolveAndPopulateL1(context.Background(), inputs, nil, nil); err != nil {
		t.Fatalf("resolveAndPopulateL1: %v", err)
	}
}

// ARM-1 — THE WIRING ARM. A refresher rebuild that reproduces the stored bytes
// is counted as identical. RED without the call site in resolve_populate.go.
func TestNoopInstrument_ARM1_IdenticalRebuildIsCountedAtTheRealCallSite(t *testing.T) {
	c := instrumentCacheOn(t)
	inputs := instrumentInputs("arm1")
	body := []byte(`{"items":[{"a":1}]}`)
	c.PutResolved(fixtureStoreVerdict, cache.ComputeKey(inputs), &cache.ResolvedEntry{RawJSON: body, Inputs: &inputs})

	i0, ch0, _ := RefreshRebuildStats()
	drive(t, inputs, body)
	i1, ch1, _ := RefreshRebuildStats()

	if i1 != i0+1 {
		t.Fatalf("#604 instrument NOT WIRED: a refresher rebuild reproducing the stored bytes did not "+
			"increment the identical counter (%d -> %d). The classifier may be correct and still never "+
			"be called -- that is the inert-feature failure, and it is what this arm exists to catch.",
			i0, i1)
	}
	if ch1 != ch0 {
		t.Fatalf("identical rebuild was miscounted as changed (changed %d -> %d)", ch0, ch1)
	}
}

// ARM-2 — a rebuild producing different bytes is counted as changed.
func TestNoopInstrument_ARM2_ChangedRebuildIsCounted(t *testing.T) {
	c := instrumentCacheOn(t)
	inputs := instrumentInputs("arm2")
	c.PutResolved(fixtureStoreVerdict, cache.ComputeKey(inputs), &cache.ResolvedEntry{
		RawJSON: []byte(`{"items":[{"a":1}]}`), Inputs: &inputs})

	i0, ch0, _ := RefreshRebuildStats()
	drive(t, inputs, []byte(`{"items":[{"a":2}]}`))
	i1, ch1, _ := RefreshRebuildStats()

	if ch1 != ch0+1 {
		t.Fatalf("a rebuild producing DIFFERENT bytes was not counted as changed (%d -> %d)", ch0, ch1)
	}
	if i1 != i0 {
		t.Fatalf("a changed rebuild was miscounted as identical (identical %d -> %d). That inflates the "+
			"no-op share, which is the number #604 turns on -- an over-count here argues for narrowing "+
			"invalidation on evidence that does not exist.", i0, i1)
	}
}

// ARM-3 — with no prior entry there is nothing to compare, and that must be
// counted separately rather than silently folded into either share.
func TestNoopInstrument_ARM3_NoPriorIsItsOwnBucket(t *testing.T) {
	instrumentCacheOn(t)
	inputs := instrumentInputs("arm3") // deliberately NOT pre-Put

	i0, ch0, n0 := RefreshRebuildStats()
	drive(t, inputs, []byte(`{"items":[]}`))
	i1, ch1, n1 := RefreshRebuildStats()

	if n1 != n0+1 {
		t.Fatalf("a rebuild with no prior entry was not counted in the no_prior bucket (%d -> %d)", n0, n1)
	}
	if i1 != i0 || ch1 != ch0 {
		t.Fatalf("a rebuild with nothing to compare polluted the comparable buckets "+
			"(identical %d->%d, changed %d->%d); the denominator of identical_share must be "+
			"comparisons actually made", i0, i1, ch0, ch1)
	}
}

// ARM-4 — the classifier itself, including the case that would quietly bias the
// share: empty bytes must not read as "identical to an empty prior".
func TestNoopInstrument_ARM4_ClassifierBoundaries(t *testing.T) {
	body := []byte(`{"a":1}`)
	cases := []struct {
		name  string
		prior *cache.ResolvedEntry
		had   bool
		enc   []byte
		want  rebuildVerdict
	}{
		{"equal", &cache.ResolvedEntry{RawJSON: body}, true, []byte(`{"a":1}`), rebuildVerdictIdentical},
		{"differ", &cache.ResolvedEntry{RawJSON: body}, true, []byte(`{"a":2}`), rebuildVerdictChanged},
		{"no prior flag", &cache.ResolvedEntry{RawJSON: body}, false, body, rebuildVerdictNoPrior},
		{"nil prior", nil, true, body, rebuildVerdictNoPrior},
		{"empty prior bytes", &cache.ResolvedEntry{RawJSON: nil}, true, body, rebuildVerdictNoPrior},
		{"empty rebuild", &cache.ResolvedEntry{RawJSON: body}, true, nil, rebuildVerdictNoPrior},
		{"both empty", &cache.ResolvedEntry{RawJSON: nil}, true, nil, rebuildVerdictNoPrior},
	}
	for _, tc := range cases {
		if got := classifyRebuild(tc.prior, tc.had, tc.enc); got != tc.want {
			t.Errorf("%s: classifyRebuild = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// ARM-5 — THE INSTRUMENT ONLY OBSERVES. An identical rebuild must still be
// stored; nothing may be gated on the verdict.
//
// Skipping the store on an identical rebuild is the obvious follow-on and is
// deliberately not done yet. If someone adds it, this arm fails and forces the
// decision to be explicit -- because an instrument that alters what it measures
// cannot be used to decide #604.
func TestNoopInstrument_ARM5_ObservationDoesNotSkipTheStore(t *testing.T) {
	c := instrumentCacheOn(t)
	inputs := instrumentInputs("arm5")
	key := cache.ComputeKey(inputs)
	body := []byte(`{"items":[{"a":1}]}`)

	// A prior entry that is byte-identical to what the rebuild will produce,
	// but distinguishable by a field the comparison does not look at.
	c.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: body, Inputs: &inputs, Pinned: true})

	drive(t, inputs, body)

	e, ok := c.Get(key)
	if !ok || e == nil {
		t.Fatal("ARM-5: the entry vanished across an identical rebuild; the instrument must not affect storage")
	}
	if string(e.RawJSON) != string(body) {
		t.Fatalf("ARM-5: stored bytes changed across an identical rebuild: %q", string(e.RawJSON))
	}
	// The re-Put really happened: a non-RAFullList refresh does not preserve
	// the pin, so a surviving Pinned=true would mean the store was SKIPPED.
	if e.Pinned {
		t.Fatal("ARM-5 VIOLATED: the prior entry survived untouched across an identical rebuild, " +
			"so the store was skipped. The instrument must OBSERVE only -- gating the Put on the " +
			"verdict makes the identical_share describe a system that no longer exists.")
	}
}
