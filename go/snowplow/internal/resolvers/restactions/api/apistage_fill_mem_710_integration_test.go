//go:build integration

// apistage_fill_mem_710_integration_test.go — #710 PR-2a F-MEM: the memory
// admission gate bounds the REAL live heap of a burst of fills across
// distinct lists.
//
// THE SHAPE. #710's burst re-filled lists that were already RESIDENT
// (known-stale or refresh-forced): the old tree stays referenced by its cell
// until the new one replaces it, so every concurrent fill adds a whole new
// decoded tree on top of the resident set. PR-1 coalesces each list to one
// fill; across K distinct lists K trees still decode at once. Here a real
// informer holds K = 64 namespaces × 1,000 objects, every list is resident,
// and 256 refresh-forced re-resolves are released together over the 64 lists —
// K ≫ ceiling / one fill's weight, so the admission queue is non-empty for
// most of the run. A queued flight must hold a ticket, not a parked envelope:
// the mutation "admit after the dispatch" parks K envelopes outside the
// ceiling and turns this arm RED.
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
// ceiling plus one REPLACEMENT fill (its reserved weight plus the tree it
// replaces, which stays in heap/objects until the next sweep), and serve every
// reader. The ungated burst exceeds that bound: that is the RED on main. The
// background wait p95/max is reported (R7: strict priority, no aging).
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

// fill710P95 is the upper bound of the histogram bucket holding the 95th
// percentile (-1 when it lies past the last bound).
func fill710P95(h ContentFillDurationStats) int64 {
	if h.Count == 0 {
		return 0
	}
	target := (h.Count*95 + 99) / 100
	for i, b := range ContentFillDurationBoundsMS {
		if h.Buckets[i] >= target {
			return b
		}
	}
	return -1
}

// fill710HeapLive is the heap the last GC marked live (/gc/heap/live:bytes): the
// reachable set, without the unswept garbage heap/objects also counts. Near the
// soft limit the GC runs often, so its peak over a burst is the live peak.
func fill710HeapLive() int64 {
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

func fill710HeapObjects() int64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

// fill710RefreshBurst releases readers as refresh-forced re-resolves over the
// lists together; it returns the growth of the live heap (/gc/heap/live) and
// of heap/objects (live + unswept garbage) over the start, and how many
// readers were served.
func fill710RefreshBurst(store *cache.ResolvedCacheStore, lists, readers int) (liveGrowth, objGrowth, served int64) {
	runtime.GC()
	runtime.GC()
	base, baseLive := fill710HeapObjects(), fill710HeapLive()
	var hi, hiLive atomic.Int64
	hi.Store(base)
	hiLive.Store(baseLive)
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
			storeMax(&hiLive, fill710HeapLive())
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
			if _, s, o, _ := apistageContentServe(ctx, store, fill710NSListCall(i%lists), false); o && s {
				ok.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(stop)
	<-sampled
	runtime.GC()
	storeMax(&hiLive, fill710HeapLive())
	return hiLive.Load() - baseLive, hi.Load() - base, ok.Load()
}

func TestContentFill710_FMEM_GateBoundsRealHeapOfAResidentListBurst(t *testing.T) {
	const (
		lists   = 24
		perNS   = 5000
		readers = 240
	)
	newFill710MultiNSWatcher(t, lists, perNS)
	store := cache.ResolvedCache()
	fill710Admission(t)
	envLen := fill710SpyDispatch(t, nil, nil, nil)

	// Every list resident (the burst replaces them).
	for n := 0; n < lists; n++ {
		if _, _, ok, _ := apistageContentServe(r1Ctx(), store, fill710NSListCall(n), false); !ok {
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
	ungated, ungatedObj, servedU := fill710RefreshBurst(store, lists, readers)
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
	gated, gatedObj, servedG := fill710RefreshBurst(store, lists, readers)
	adm := ContentFillAdmissionStatsSnapshot()
	if servedG != readers {
		t.Fatalf("F-MEM: the gated burst served %d of %d readers — the gate must queue, never drop", servedG, readers)
	}
	exp := adm.Expansion[r1FsaGVR.String()]
	// One fill's weight is what the gate reserved for it (the admitted-weight
	// peak: the gated fills ran one at a time, on the seed or the calibrated
	// expansion). The design bound: the live heap grows by at most the starting
	// ceiling plus one fill.
	oneFill := contentFillAdmissionInflightWeightPeak.Load()
	bound := ceiling0 + oneFill
	// Queued flights hold tickets, not envelopes: the envelope bytes in flight
	// (built at the dispatch, held to the end of the serve) never exceed the
	// flights the ceiling admits at once, plus the leader still gating its own
	// reply while the next flight runs, plus the empty-gate escape.
	st := ContentFillStatsSnapshot()
	envBound := (ceiling0/max(oneFill, 1) + 2) * env
	t.Logf("#710 PR-2a F-MEM: %d resident lists × %d items (envelope %d B), %d refresh-forced readers, soft limit %d MiB. "+
		"UNGATED live-heap growth +%d MiB (heap/objects +%d); GATED +%d MiB (heap/objects +%d) (starting ceiling %d MiB + one fill; measured expansion %.1f×; bound %d MiB); "+
		"envelope bytes in flight peak %d (%.1f envelopes, bound %d); admitted-weight peak %d MiB; K / (ceiling / one fill) = %.1f; background admission wait p95 ≤ %d ms, max %d ms",
		lists, perNS, env, readers, limit>>20, ungated>>20, ungatedObj>>20, gated>>20, gatedObj>>20, ceiling0>>20, exp, bound>>20,
		st.InflightBytesPeak, float64(st.InflightBytesPeak)/float64(max(env, 1)), envBound,
		contentFillAdmissionInflightWeightPeak.Load()>>20, float64(lists)/(float64(ceiling0)/float64(max(oneFill, 1))),
		fill710P95(adm.Wait["background"]), adm.Wait["background"].MaxMS)
	if st.InflightBytesPeak > envBound {
		t.Fatalf("F-MEM RED: %d envelope bytes were in flight at once (%.1f envelopes, bound %d B) — queued flights are holding parked envelopes outside the ceiling",
			st.InflightBytesPeak, float64(st.InflightBytesPeak)/float64(max(env, 1)), envBound)
	}
	if gated > bound {
		t.Fatalf("F-MEM RED: the gated burst's live heap grew %d MiB, past the starting ceiling + one fill (%d MiB)", gated>>20, bound>>20)
	}
	if ungated <= bound {
		t.Fatalf("PRECONDITION: the ungated burst stayed within the bound too (+%d MiB ≤ %d MiB) — the arm does not discriminate", ungated>>20, bound>>20)
	}
}
