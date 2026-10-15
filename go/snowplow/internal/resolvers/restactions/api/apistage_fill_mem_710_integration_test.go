//go:build integration

// apistage_fill_mem_710_integration_test.go — #710 PR-2a F-MEM: the memory
// admission gate bounds the REAL live heap of a burst of fills across
// distinct lists.
//
// THE SHAPE. #710's burst re-filled lists that were already RESIDENT
// (known-stale or refresh-forced): the old tree stays referenced by its cell
// until the new one replaces it, so every concurrent fill adds a whole new
// decoded tree on top of the resident set. PR-1 coalesces each list to one
// fill; across D distinct lists D trees still decode at once. Here a real
// informer holds 6 namespaces × 5,000 objects, every list is resident, and 200
// refresh-forced re-resolves are released together over the 6 lists.
//
// The runtime's soft limit (debug.SetMemoryLimit) is set to the SAME GOMEMLIMIT
// the ceiling reads — the live heap at the start plus the 1/8 reserve plus room
// for 1.5 fills at the seed weight — so the GC works against the limit as it
// does in the pod. The same burst runs twice:
//
//   - UNGATED (the gate transparent: main's behaviour after PR-1);
//   - GATED.
//
// The gated burst must keep its live-heap growth (runtime/metrics
// /memory/classes/heap/objects:bytes over its start) within the starting
// ceiling plus one fill's measured weight, and serve every reader. The ungated
// burst exceeds that bound: that is the RED on main.
package api

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func fill710HeapObjects() int64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

// fill710RefreshBurst releases readers as refresh-forced re-resolves over the
// lists together; it returns the live-heap growth over the start and how many
// readers were served.
func fill710RefreshBurst(store *cache.ResolvedCacheStore, lists, readers int) (growth, served int64) {
	runtime.GC()
	runtime.GC()
	base := fill710HeapObjects()
	var hi atomic.Int64
	hi.Store(base)
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			storeMax(&hi, fill710HeapObjects())
			time.Sleep(200 * time.Microsecond)
		}
	}()
	start := make(chan struct{})
	var wg sync.WaitGroup
	var ok atomic.Int64
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx := cache.WithRefreshTriggerGVR(cache.WithBackgroundResolve(r1Ctx()), r1FsaGVR)
			if _, s, o := apistageContentServe(ctx, store, fill710NSListCall(i%lists), false); o && s {
				ok.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(stop)
	<-sampled
	return hi.Load() - base, ok.Load()
}

func TestContentFill710_FMEM_GateBoundsRealHeapOfAResidentListBurst(t *testing.T) {
	const (
		lists   = 6
		perNS   = 5000
		readers = 200
	)
	newFill710MultiNSWatcher(t, lists, perNS)
	store := cache.ResolvedCache()
	fill710Admission(t)
	envLen := fill710SpyDispatch(t, nil, nil, nil)

	// Every list resident (the burst replaces them).
	for n := 0; n < lists; n++ {
		if _, _, ok := apistageContentServe(r1Ctx(), store, fill710NSListCall(n), false); !ok {
			t.Fatalf("PRECONDITION: list %d not content-servable", n)
		}
	}
	env := envLen.Load()
	seedFill := int64(float64(env) * contentFillExpansionSeed)
	runtime.GC()
	runtime.GC()
	live0 := fill710HeapObjects()
	limit := (live0 + seedFill + seedFill/2) * 8 / 7
	prevLimit := debug.SetMemoryLimit(limit)
	t.Cleanup(func() { debug.SetMemoryLimit(prevLimit) })

	// UNGATED.
	ResetContentFillStatsForTest()
	restore := cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return 1<<63 - 1 }, fill710HeapObjects)
	ungated, servedU := fill710RefreshBurst(store, lists, readers)
	restore()
	if servedU != readers {
		t.Fatalf("PRECONDITION: the ungated burst served %d of %d", servedU, readers)
	}

	// GATED, calibration back to the seed.
	ResetContentFillStatsForTest()
	restore = cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return limit }, fill710HeapObjects)
	t.Cleanup(restore)
	runtime.GC()
	ceiling0, _ := cache.AdmissionCeiling()
	gated, servedG := fill710RefreshBurst(store, lists, readers)
	if servedG != readers {
		t.Fatalf("F-MEM: the gated burst served %d of %d readers — the gate must queue, never drop", servedG, readers)
	}
	adm := ContentFillAdmissionStatsSnapshot()
	exp := adm.Expansion[r1FsaGVR.String()]
	// One REPLACEMENT fill's footprint at its Put is its own weight plus the
	// tree it replaces, which stays in heap/objects until the next sweep. Its
	// weight is what the gate reserved for it (the admitted-weight peak: the
	// gated fills ran one at a time, each at the seed or the calibrated weight).
	oneFill := contentFillAdmissionInflightWeightPeak.Load()
	bound := ceiling0 + 2*oneFill
	t.Logf("#710 PR-2a F-MEM: %d resident lists × %d items (envelope %d B), %d refresh-forced readers, soft limit %d MiB. "+
		"UNGATED live-heap growth +%d MiB; GATED +%d MiB (starting ceiling %d MiB + one replacement fill, 2 × the reserved weight; measured expansion %.1f×; bound %d MiB); "+
		"admitted-weight peak %d MiB; admission wait max %d ms",
		lists, perNS, env, readers, limit>>20, ungated>>20, gated>>20, ceiling0>>20, exp, bound>>20,
		contentFillAdmissionInflightWeightPeak.Load()>>20, adm.Wait.MaxMS)
	if gated > bound {
		t.Fatalf("F-MEM RED: the gated burst's live heap grew %d MiB, past the starting ceiling + one replacement fill (%d MiB)", gated>>20, bound>>20)
	}
	if ungated <= bound {
		t.Fatalf("PRECONDITION: the ungated burst stayed within the bound too (+%d MiB ≤ %d MiB) — the arm does not discriminate", ungated>>20, bound>>20)
	}
}
