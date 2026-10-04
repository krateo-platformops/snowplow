package cache

// inert443_test.go — #443 part 2, reviewer-424 C2/C3: the side-effecting
// cache entry points the resolve path reaches gate on the inert flag INSIDE
// the cache package (structural, not a caller convention).
//
//   - SliceabilityLookupCtx: an aged non-permanent false verdict schedules the
//     async re-verify (SubmitSliceabilityInvalidate + EnqueueRefresh) — except
//     under the flag, where it only reads.
//   - RecordSliceabilityClassifiedCtx records no verdict under the flag.
//   - DepTracker.ReplayEdges replays no edge under the flag.
//
// Each arm has a control (the same call without the flag) that does move the
// observation, so the arm can fail.

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestS443_InertCacheEntryPoints(t *testing.T) {
	t.Run("SliceabilityLookupCtx_AgedVerdict_NoReverify", func(t *testing.T) {
		resetSliceabilityMemoForTest()
		t.Cleanup(resetSliceabilityMemoForTest)
		t.Setenv(envSliceabilityReverifyRateFloorSeconds, "1")
		base := int64(1_000_000)
		now := base
		f := func() int64 { return now }
		prev := nowUnix.Load()
		nowUnix.Store(&f)
		t.Cleanup(func() { nowUnix.Store(prev) })

		RecordSliceabilityClassified("rk-443", "shape-443", false, false, SliceabilityLabels{})
		now = base + 100 // the false verdict is now aged past the re-verify floor

		before := SliceabilityReverifyStatsSnapshot()
		s, known := SliceabilityLookupCtx(WithInert(context.Background()), "rk-443", "shape-443")
		if !known || s {
			t.Fatalf("inert lookup must still READ the verdict: sliceable=%v known=%v", s, known)
		}
		mid := SliceabilityReverifyStatsSnapshot()
		if mid.EnqueuedTotal+mid.DroppedTotal != before.EnqueuedTotal+before.DroppedTotal {
			t.Fatalf("an inert lookup scheduled a re-verify (enqueued+dropped %d → %d)",
				before.EnqueuedTotal+before.DroppedTotal, mid.EnqueuedTotal+mid.DroppedTotal)
		}
		// Control: the same lookup without the flag does schedule it.
		SliceabilityLookupCtx(context.Background(), "rk-443", "shape-443")
		after := SliceabilityReverifyStatsSnapshot()
		if after.EnqueuedTotal+after.DroppedTotal == mid.EnqueuedTotal+mid.DroppedTotal {
			t.Fatalf("CONTROL: a non-inert lookup of the aged verdict scheduled no re-verify — the arm cannot fail")
		}
	})

	t.Run("RecordSliceabilityClassifiedCtx_NoVerdict", func(t *testing.T) {
		resetSliceabilityMemoForTest()
		t.Cleanup(resetSliceabilityMemoForTest)
		RecordSliceabilityClassifiedCtx(WithInert(context.Background()), "rk-443b", "shape-443b", true, false, SliceabilityLabels{})
		if _, known := SliceabilityLookup("rk-443b", "shape-443b"); known {
			t.Fatal("an inert record stored a sliceability verdict")
		}
		RecordSliceabilityClassifiedCtx(context.Background(), "rk-443b", "shape-443b", true, false, SliceabilityLabels{})
		if _, known := SliceabilityLookup("rk-443b", "shape-443b"); !known {
			t.Fatal("CONTROL: a non-inert record stored no verdict — the arm cannot fail")
		}
	})

	t.Run("ReplayEdges_NoEdge", func(t *testing.T) {
		ResetDepsForTest()
		t.Cleanup(ResetDepsForTest)
		edges := []DepKey{{GVR: schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, Namespace: "x", Name: "y"}}
		records := func() int64 { return DepsStatsByStat()["records"] }
		Deps().ReplayEdges(WithInert(context.Background()), "widget-key-443", edges)
		if n := records(); n != 0 {
			t.Fatalf("an inert ReplayEdges recorded %d edge(s)", n)
		}
		Deps().ReplayEdges(context.Background(), "widget-key-443", edges)
		if n := records(); n == 0 {
			t.Fatal("CONTROL: a non-inert ReplayEdges recorded nothing — the arm cannot fail")
		}
	})
}
