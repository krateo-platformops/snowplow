//go:build calib787 && integration

// apistage_fill_calib_787_experiment_test.go — #787 step 1(b), REPORT-ONLY.
// Never part of the gate: it only builds with -tags 'calib787 integration'.
//
// QUESTION. Does the content-fill admission gate's learned decode expansion
// fall under GC pressure far enough that it charges a fill LESS than the fill
// actually RETAINS (charged < retained = a real undercharge)?
//
// SHAPE. The F-MEM harness (24 lists × 5,000 items, ~1.1 MB envelopes): every
// list resident, then 3 refresh-forced bursts that let the gate calibrate
// under the variant's GC setting. Then, per architect-623's correction on
// #787, ONE COLD FILL (a replacement fill frees the tree it replaces and its
// retained growth reads ≈0, #780): the cell is removed, GC runs twice, the fill
// runs and is Put, GC runs twice with the entry still in the store.
//
//	charged  = the weight the gate held for that fill (raw × the learned
//	           expansion read just before it);
//	alloc    = /gc/heap/allocs over the fill (GC-independent);
//	retained = /memory/classes/heap/objects after the double GC, minus before.
//
// The production samplers are used (no seams), so SetMemoryLimit /
// SetGCPercent act on the gate exactly as GOMEMLIMIT / GOGC do in the pod; both
// are restored in Cleanup. Run each variant ×3, with and without -race.
package api

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestContentFill787_Experiment_LearnedExpansionUnderGCPressure(t *testing.T) {
	const (
		lists   = 24
		perNS   = 5000
		readers = 240
		bursts  = 3
	)
	variant := os.Getenv("CALIB787_VARIANT") // V0 default | V1 gogc25 | V2 gogc10 | V3 tightlimit
	if variant == "" {
		variant = "V0"
	}
	newFill710MultiNSWatcher(t, lists, perNS)
	store := cache.ResolvedCache()
	fill710Admission(t)
	cache.ResetAdmissionRuntimeSeamsForTest()
	envLen := fill710SpyDispatch(t, nil, nil, nil)
	for n := 0; n < lists; n++ {
		if _, _, ok, _, _ := apistageContentServe(r1Ctx(), store, fill710NSListCall(n), false); !ok {
			t.Fatalf("PRECONDITION: list %d not content-servable", n)
		}
	}
	env := envLen.Load()

	prevGC := debug.SetGCPercent(-1)
	debug.SetGCPercent(prevGC)
	prevLimit := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetGCPercent(prevGC)
		debug.SetMemoryLimit(prevLimit)
	})
	runtime.GC()
	runtime.GC()
	live0 := fill710HeapObjects()
	seedFill := int64(float64(env) * contentFillExpansionSeed)
	limit := int64(math.MaxInt64)
	gogc := prevGC
	switch variant {
	case "V1":
		gogc = 25
		debug.SetGCPercent(gogc)
	case "V2":
		gogc = 10
		debug.SetGCPercent(gogc)
	case "V3":
		// The F-MEM soft limit: live + 1.5 seed fills, plus the 1/8 reserve.
		limit = (live0 + seedFill + seedFill/2) * 8 / 7
		debug.SetMemoryLimit(limit)
	}

	// Calibrate under the variant: the gate starts from the seed.
	ResetContentFillStatsForTest()
	resetContentFillGateForTest()
	for b := 0; b < bursts; b++ {
		if _, _, served := fill710RefreshBurst(store, lists, readers); served != readers {
			t.Fatalf("burst %d served %d of %d", b, served, readers)
		}
	}
	exp, _ := contentFillCalibrationSnapshot()
	learned := exp[r1FsaGVR.String()]
	samples := contentFillCalibrationSamplesMap()

	// ONE COLD FILL of list 0.
	key := cache.ComputeKey(contentKeyInputs(r1FsaGVR, fill710NS(0), ""))
	if _, ok := store.GetNoTouch(key); !ok {
		t.Fatalf("PRECONDITION: list 0's content cell %q is not resident", key)
	}
	store.DeleteForTest(key)
	runtime.GC()
	runtime.GC()
	h0, a0 := fill710HeapObjects(), fill710HeapAllocs()
	contentFillAdmissionInflightWeightPeak.Store(0)
	if _, _, ok, _, _ := apistageContentServe(r1Ctx(), store, fill710NSListCall(0), false); !ok {
		t.Fatalf("the cold fill was not served")
	}
	charged := contentFillAdmissionInflightWeightPeak.Load()
	alloc := fill710HeapAllocs() - a0
	if _, ok := store.GetNoTouch(key); !ok {
		t.Fatalf("the cold fill was not stored")
	}
	runtime.GC()
	runtime.GC()
	retained := fill710HeapObjects() - h0
	runtime.KeepAlive(store)

	line := fmt.Sprintf("#787 EXPERIMENT variant=%s race=%v gogc=%d limitMiB=%d envelope=%d learned=%.2f samples=%v charged=%d alloc=%d retained=%d charged/alloc=%.3f charged/retained=%.3f undercharge=%v",
		variant, raceEnabledForTest, gogc, func() int64 {
			if limit == math.MaxInt64 {
				return -1
			}
			return limit >> 20
		}(), env, learned, samples, charged, alloc, retained,
		float64(charged)/float64(max(alloc, 1)), float64(charged)/float64(max(retained, 1)), charged < retained)
	t.Log(line)
	if out := os.Getenv("CALIB787_OUT"); out != "" {
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = f.WriteString(line + "\n")
			_ = f.Close()
		}
	}
}
