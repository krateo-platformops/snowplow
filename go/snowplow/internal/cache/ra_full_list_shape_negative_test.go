// ra_full_list_shape_negative_test.go — #42 FIX-A: SHAPE-LEVEL negative-permanent
// sliceability sharing (design §A5 FIX-A).
//
// A structurally-non-sliceable (Class C, false+permanent) verdict recorded
// under ONE per-BindingUID raKey is now consulted by the lookup of ANY OTHER
// raKey of the SAME sliceShape — because sliceShape is identity-free
// (SliceShapeHash folds caller class/gvr/ns/name + the RA slice-jq, NO
// BindingUID). This collapses identities #2..N on an aggregation RA from the
// expensive first-sight triple resolve to the always-correct page-keyed
// fallback (1 resolve). POSITIVE verdicts stay strictly PER-KEY — never shared.
//
// Pure unit, -race clean, no cluster.

package cache

import "testing"

// twoIdentityRAKeys returns two DISTINCT raFullList keys (differing ONLY in
// BindingUID — the per-identity dimension) for the SAME RA coordinates, plus
// the ONE identity-free sliceShape both share.
func twoIdentityRAKeys(t *testing.T) (raKeyA, raKeyB, shape string) {
	t.Helper()
	const (
		g, v, r = "widgets.templates.krateo.io", "v1beta1", "estategraphs"
		ns, nm  = "krateo-system", "estate-graph"
		sliceJQ = ".items |= sort_by(.x)" // the RA's Spec.Filter
	)
	// Two identities → two BindingUIDs → two distinct raKeys.
	raKeyA = ComputeKey(RAFullListKeyInputsForTest(g, v, r, ns, nm, "C:uid-identity-A", nil))
	raKeyB = ComputeKey(RAFullListKeyInputsForTest(g, v, r, ns, nm, "C:uid-identity-B", nil))
	if raKeyA == raKeyB {
		t.Fatalf("test setup wrong: the two raKeys must differ (distinct BindingUID); both=%s", raKeyA)
	}
	// The sliceShape is identity-free → identical for both.
	shape = SliceShapeHash("apiref", g, v, r, ns, nm, sliceJQ)
	return raKeyA, raKeyB, shape
}

// FIX-A GREEN — a negative-permanent verdict under identity A is SHARED at
// shape level to identity B's (previously-unknown) lookup.
func TestFixA_ShapeNegativeSharedAcrossIdentities(t *testing.T) {
	resetSliceabilityMemoForTest()
	raKeyA, raKeyB, shape := twoIdentityRAKeys(t)

	// Identity B has NO per-key verdict yet → unknown (baseline, pre-record).
	if sliceable, known := SliceabilityLookup(raKeyB, shape); known {
		t.Fatalf("baseline: identity-B lookup should be unknown before any record; got sliceable=%v known=%v", sliceable, known)
	}

	// Identity A's first-sight proves the shape structurally NON-sliceable
	// (Class C: false + permanent). This is what raFullListServe records via
	// RecordSliceabilityClassified after IsStructurallyNonSliceable.
	RecordSliceabilityClassified(raKeyA, shape, false /*sliceable*/, true /*permanent*/, SliceabilityLabels{})

	// FIX-A: identity B's lookup — still no PER-KEY verdict for raKeyB — now
	// returns (false, true) via the shape-negative set. known=true routes the
	// caller to the always-correct page-keyed fallback (1 resolve), skipping
	// the expensive first-sight triple resolve.
	sliceable, known := SliceabilityLookup(raKeyB, shape)
	if !known {
		t.Fatalf("FIX-A: identity-B lookup must be KNOWN via the shape-negative set (raKeyA proved the shape non-sliceable); got known=false — the 3-resolve/identity waste is NOT collapsed")
	}
	if sliceable {
		t.Fatalf("FIX-A: identity-B shared verdict must be NON-sliceable (false); got sliceable=true — would wrongly attempt a Go-slice")
	}
	if !SliceabilityShapeKnownNegative(shape) {
		t.Fatalf("FIX-A: SliceabilityShapeKnownNegative(shape) must report true after a false+permanent record")
	}
}

// FIX-A SAFETY — POSITIVE verdicts are NEVER shared across identities. A
// sliceable verdict under identity A must NOT make identity B's unknown lookup
// return known (that would risk serving identity B a Go-slice over a cell it
// hasn't byte-verified). Pins the design's "positives stay per-key".
func TestFixA_PositiveVerdictNotShared(t *testing.T) {
	resetSliceabilityMemoForTest()
	raKeyA, raKeyB, shape := twoIdentityRAKeys(t)

	// Identity A records a POSITIVE (sliceable) verdict.
	RecordSliceabilityClassified(raKeyA, shape, true /*sliceable*/, false /*permanent*/, SliceabilityLabels{})

	// Identity B still has NO verdict → MUST be unknown (positives never
	// enter the shape set; only the per-key raKeyA carries the positive).
	if sliceable, known := SliceabilityLookup(raKeyB, shape); known {
		t.Fatalf("FIX-A safety: a POSITIVE verdict must NOT be shared to another identity; identity-B got sliceable=%v known=%v (want known=false)", sliceable, known)
	}
	if SliceabilityShapeKnownNegative(shape) {
		t.Fatalf("FIX-A safety: a positive verdict must NOT populate the shape-negative set")
	}
	// Identity A's own per-key positive is intact.
	if sliceable, known := SliceabilityLookup(raKeyA, shape); !known || !sliceable {
		t.Fatalf("identity-A per-key positive lost: sliceable=%v known=%v", sliceable, known)
	}
}

// FIX-A — a NON-permanent false (Class A/D transient) is NOT promoted to the
// shape set (only structural-permanent negatives share). Guards over-sharing.
func TestFixA_NonPermanentFalseNotShared(t *testing.T) {
	resetSliceabilityMemoForTest()
	raKeyA, raKeyB, shape := twoIdentityRAKeys(t)

	RecordSliceabilityClassified(raKeyA, shape, false /*sliceable*/, false /*permanent=NO*/, SliceabilityLabels{})

	if SliceabilityShapeKnownNegative(shape) {
		t.Fatalf("FIX-A: a NON-permanent false must NOT enter the shape-negative set (only Class C permanent shares)")
	}
	if _, known := SliceabilityLookup(raKeyB, shape); known {
		t.Fatalf("FIX-A: identity-B must stay unknown when A's false was non-permanent")
	}
}

// FIX-A arch C-1 — SELF-HEAL: a per-key Lever-C dep-event invalidation must
// ALSO clear the shape-level negative it fed, so lookup returns known=false
// again (the next /call re-verifies from first sight). Without C-1 the shape
// set would pin a permanent false forever, defeating dep-event self-heal for
// the shared-negative path.
func TestFixA_C1_InvalidateClearsShapeNegative_SelfHeal(t *testing.T) {
	// Rate-floor 0 so invalidate() always deletes (no artificial clock skew
	// needed): (now - lastUpdated) < 0 is never true.
	t.Setenv(envSliceabilityReverifyRateFloorSeconds, "0")
	resetSliceabilityMemoForTest()
	raKeyA, raKeyB, shape := twoIdentityRAKeys(t)

	// Record permanent-false → shape negative set → identity-B shares it.
	RecordSliceabilityClassified(raKeyA, shape, false, true, SliceabilityLabels{})
	if !SliceabilityShapeKnownNegative(shape) {
		t.Fatalf("setup: shape must be known-negative after a permanent-false record")
	}
	if _, known := SliceabilityLookup(raKeyB, shape); !known {
		t.Fatalf("setup: identity-B must share the negative before invalidation")
	}

	// Lever-C dep-event invalidation of raKeyA's cell.
	if n := InvalidateSliceabilityForKey(raKeyA); n < 1 {
		t.Fatalf("C-1: invalidate should have removed at least the raKeyA entry, got %d", n)
	}

	// SELF-HEAL: the shape negative is cleared → lookups return known=false
	// again (first-sight re-verify restored) for BOTH the invalidated key and
	// any other identity that had been riding the shared negative.
	if SliceabilityShapeKnownNegative(shape) {
		t.Fatalf("C-1: the shape-negative set must be CLEARED after a per-key invalidation (self-heal); it is still known-negative")
	}
	if _, known := SliceabilityLookup(raKeyA, shape); known {
		t.Fatalf("C-1: raKeyA lookup must be known=false (unknown) after invalidation — re-verify path restored")
	}
	if _, known := SliceabilityLookup(raKeyB, shape); known {
		t.Fatalf("C-1: raKeyB (shared-negative rider) lookup must be known=false after the shape clear — self-heal did not propagate")
	}
}
