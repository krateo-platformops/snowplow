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
//
// WHAT THIS ARM CAN AND CANNOT SEE (#780). F-MEM is a COARSE real-heap
// backstop: it catches the defect class that multiplies the live heap —
// admit-all, queued flights parking their envelopes, no gate at all.
// Over-admission by about 2× or less is NOT resolvable on this heap: the GC
// counts everything allocated during a mark phase as live, and under the
// integration TestMain's pacing one mark phase absorbs up to ~3.7 fills, so a
// gate admitting 2× its ceiling measured 60–67 MiB against a correct gate's
// 50–72 MiB. That class is owned by the deterministic ADM1 arms
// (apistage_fill_admission_710_test.go), where a `<= 2*ceiling` mutation is RED.
//
// STOP RULE (architect-623, #780): if a CORRECT gate ever exceeds ~4.5 fills
// of allocate-black excess (the gated-worst log line), the heap check becomes
// log-only. The bound (fmemAllocateBlackFills) is never widened again.
//
// THE STOP RULE HAS TRIGGERED: the heap check is LOG-ONLY (#780
// issuecomment-6284262773). A correct gate reached +112 MiB = 5.81 fills over
// ceiling0 + oneFill (runs [112 69 55], load ~24), and a paired pre/post-#772
// comparison showed that is the GC-pacing tail, not a code change. The live
// heap, its bound and the vacuity margin are still LOGGED every run, so the
// distribution stays visible. What stays HARD: no dispatch for an unadmitted
// flight (admit-after-dispatch), and envelope bytes in flight within
// (ceiling0/oneFill + 2) envelopes — oneFill being one isolated fill's REAL
// live-heap growth, so admit-all and a gate that undercharges a fill's weight
// both read RED there. What is no longer hard-checked: live memory a flight
// retains beyond its envelope (a tree or decode buffer kept after the serve,
// retention growing across fills) — #786 tracks a GC-independent check for it.
package api

import (
	"context"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
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

// fill710HeapAllocs is the cumulative bytes ever allocated on the heap
// (/gc/heap/allocs:bytes): deterministic, independent of when GC runs.
func fill710HeapAllocs() int64 {
	s := []metrics.Sample{{Name: "/gc/heap/allocs:bytes"}}
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
	// #765 — a refresher re-fills a resident cell only when a change landed after
	// its read (the content stamp). The burst is the refresh of every list after
	// one change to each, so each list is re-filled, as before #765.
	for n := 0; n < lists; n++ {
		cache.BumpCoordinateForTest(r1FsaGVR, fill710NS(n), "fsa-00000")
	}
	return fill710Burst(store, lists, readers, fill710BgCtx)
}

// fill710Burst is fill710RefreshBurst with the readers' ctx given.
func fill710Burst(store *cache.ResolvedCacheStore, lists, readers int, ctxFn func() context.Context) (liveGrowth, objGrowth, served int64) {
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
			if _, s, o, _ := apistageContentServe(ctxFn(), store, fill710NSListCall(i%lists), false); o && s {
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

// fmemAllocateBlackFills is how many fills' worth of allocations (oneFillAlloc,
// measured in isolation before the bursts) the heap bound allows on top of the
// starting ceiling and one fill: the GC counts what is allocated during its mark
// phase as live (allocate-black), and how many fills one mark phase absorbs
// depends on the GC's pacing, not on the gate. A FIXED design constant read
// from data, never from the run (a bound built from the run's own GC data moves
// with the defect — #719). Measured for #780 (issuecomments 6278532568,
// 6279429171), 24 lists × 5,000, 240 readers, -tags unit,integration (the
// worst GC pacing: 7–9 cycles per burst):
//
//   - a CORRECT gate's excess over ceiling0 + oneFill: ≤ 3.7 fills at the worst
//     observed (+86 MiB live growth under load ~22; typically 0.65–2.5);
//   - the guarded defect class: admit-all ≥ 9.7 fills, ungated ≥ 10.
//
// 6 sits ≈ 2.3 fills from each side (architect-623). If a correct-gate run
// ever exceeds ~4.5 fills, the heap check becomes log-only — it is NOT widened
// again. If a Go or GC change closes the gap, the ungated precondition
// (ungated ≥ bound + 1 × oneFillAlloc) goes RED rather than letting the arm
// pass vacuously.
const fmemAllocateBlackFills = 6

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

	// ONE FILL'S WEIGHT, fixed BEFORE the bursts and independent of the gate's own
	// calibration (which a defect could inflate): the live-heap growth of one
	// isolated replacement fill, or the seed weight if that is larger.
	ResetContentFillStatsForTest()
	restore := cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return 1<<63 - 1 }, fill710HeapObjects)
	a0 := fill710HeapAllocs()
	isolated, _, _ := fill710RefreshBurst(store, 1, 1)
	oneFillAlloc := fill710HeapAllocs() - a0
	restore()
	oneFill := max(isolated, seedFill)

	// Queued flights hold tickets, not envelopes: no dispatch may run for a
	// flight the gate has not admitted. Counted at the DISPATCH itself, against
	// the gate's admitted count.
	var checkAdmitted atomic.Bool
	var inDispatch, unadmittedPeak atomic.Int64
	prevDispatch := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		n := inDispatch.Add(1)
		defer inDispatch.Add(-1)
		if checkAdmitted.Load() {
			if admitted := contentFillAdmittedCount.Load(); n > admitted {
				storeMax(&unadmittedPeak, n-admitted)
			}
		}
		return prevDispatch(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = prevDispatch })

	// UNGATED.
	ResetContentFillStatsForTest()
	restore = cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return 1<<63 - 1 }, fill710HeapObjects)
	ungated, ungatedObj, servedU := fill710RefreshBurst(store, lists, readers)
	restore()
	if servedU != readers {
		t.Fatalf("PRECONDITION: the ungated burst served %d of %d", servedU, readers)
	}

	// GATED, calibration back to the seed for every run. EVERY run must hold the
	// structural checks (no dispatch for an unadmitted flight, envelope bytes);
	// the worst run's live heap is logged against the bound (log-only since the
	// #780 stop rule).
	const gatedRuns = 3
	restore = cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return limit }, fill710HeapObjects)
	t.Cleanup(restore)
	runtime.GC()
	ceiling0, _ := cache.AdmissionCeiling()
	var runs []int64
	gated, gatedObj := int64(0), int64(0)
	var adm ContentFillAdmissionStats
	var st ContentFillStats
	var envPeak int64
	for r := 0; r < gatedRuns; r++ {
		ResetContentFillStatsForTest()
		checkAdmitted.Store(true)
		g, gObj, servedG := fill710RefreshBurst(store, lists, readers)
		checkAdmitted.Store(false)
		if servedG != readers {
			t.Fatalf("F-MEM: gated run %d served %d of %d readers — the gate must queue, never drop", r, servedG, readers)
		}
		adm, st = ContentFillAdmissionStatsSnapshot(), ContentFillStatsSnapshot()
		envPeak = max(envPeak, st.InflightBytesPeak)
		runs = append(runs, g>>20)
		if g > gated {
			gated, gatedObj = g, gObj
		}
	}
	exp := adm.Expansion[r1FsaGVR.String()]
	// The bound: the live heap grows by at most the starting ceiling plus one
	// fill (both fixed before the bursts) plus fmemAllocateBlackFills fills'
	// ALLOCATIONS: the GC counts everything allocated during its mark phase as
	// live for that cycle (allocate-black), so the fills that run while a cycle
	// marks this large heap read as live in full — envelope, decode garbage and
	// tree. oneFillAlloc is measured on /gc/heap/allocs, which does not depend on
	// when GC runs.
	bound := ceiling0 + oneFill + fmemAllocateBlackFills*oneFillAlloc
	// Queued flights hold tickets, not envelopes: the envelope bytes in flight
	// (built at the dispatch, held to the end of the serve) never exceed the
	// flights the ceiling admits at once, plus the leader still gating its own
	// reply while the next flight runs, plus the empty-gate escape.
	envBound := (ceiling0/max(oneFill, 1) + 2) * env
	t.Logf("#710 PR-2a F-MEM: %d resident lists × %d items (envelope %d B), %d refresh-forced readers, soft limit %d MiB. "+
		"UNGATED live-heap growth +%d MiB (heap/objects +%d); GATED worst of %d runs +%d MiB (heap/objects +%d; all runs %v MiB) (starting ceiling %d MiB + one fill %d MiB + %d × its allocations %d MiB, measured in isolation; gate expansion %.1f×; bound %d MiB); unadmitted dispatches peak %d; "+
		"envelope bytes in flight peak %d (%.1f envelopes, bound %d); admitted-weight peak %d MiB; K / (ceiling / one fill) = %.1f; background admission wait p95 ≤ %d ms, max %d ms",
		lists, perNS, env, readers, limit>>20, ungated>>20, ungatedObj>>20, gatedRuns, gated>>20, gatedObj>>20, runs, ceiling0>>20, oneFill>>20, fmemAllocateBlackFills, oneFillAlloc>>20, exp, bound>>20, unadmittedPeak.Load(),
		envPeak, float64(envPeak)/float64(max(env, 1)), envBound,
		contentFillAdmissionInflightWeightPeak.Load()>>20, float64(lists)/(float64(ceiling0)/float64(max(oneFill, 1))),
		fill710P95(adm.Wait["background"]), adm.Wait["background"].MaxMS)
	// The two HARD checks (Errorf, so a defect shows each one it breaks).
	if u := unadmittedPeak.Load(); u > 0 {
		t.Errorf("F-MEM RED: %d dispatches ran for flights the gate had not admitted — a queued flight built its envelope outside the ceiling", u)
	}
	if envPeak > envBound {
		t.Errorf("F-MEM RED: %d envelope bytes were in flight at once (%.1f envelopes, bound %d B) — queued flights are holding parked envelopes outside the ceiling",
			envPeak, float64(envPeak)/float64(max(env, 1)), envBound)
	}
	// The heap bound is LOG-ONLY (the stop rule, #780 issuecomment-6284262773;
	// see the header). The same figures are logged every run, so the
	// distribution stays observable in CI logs: an overshoot is reported, never
	// failed.
	if gated > bound {
		t.Logf("#780 F-MEM heap (log-only): a gated run's live heap grew %d MiB (runs %v MiB), past the starting ceiling + one fill + %d × its allocations (%d MiB)",
			gated>>20, runs, fmemAllocateBlackFills, bound>>20)
	}
	t.Logf("#780 F-MEM gated worst: %.2f fills over ceiling0 + oneFill (runs %v MiB, bound %d MiB at %d fills; log-only)",
		float64(gated-ceiling0-oneFill)/float64(max(oneFillAlloc, 1)), runs, bound>>20, fmemAllocateBlackFills)
	// The vacuity precondition only protected the heap bound, so it is a log too.
	t.Logf("#780 F-MEM precondition margin: ungated − bound = %.2f × oneFillAlloc (was required ≥ 1.00; ungated +%d MiB, bound %d MiB, oneFillAlloc %d MiB; log-only)",
		float64(ungated-bound)/float64(max(oneFillAlloc, 1)), ungated>>20, bound>>20, oneFillAlloc>>20)
}

// TestContentFill710_FMEM_CustomerFirstLoadBurst_ReportOnly: users never queue
// (owner ruling), so a burst of CUSTOMER first loads across K distinct cold
// lists bypasses the cap. That memory risk is ACCEPTED (ruling 2: decided on
// bench run 8); this arm REPORTS it — live-heap growth and
// admission_over_ceiling_bytes_peak — and asserts only the ruling itself:
// every customer flight is admitted with no wait, and a background flight
// queued before the burst stays queued through it (active users squeeze
// background).
func TestContentFill710_FMEM_CustomerFirstLoadBurst_ReportOnly(t *testing.T) {
	const (
		lists   = 24
		perNS   = 5000
		readers = 240
	)
	newFill710MultiNSWatcher(t, lists, perNS)
	store := cache.ResolvedCache()
	fill710Admission(t)
	wg := fill710Goroutines(t)
	envLen := fill710SpyDispatch(t, nil, nil, nil)
	// One cold fill to learn the envelope size, then every list cold again.
	if _, _, ok, _ := apistageContentServe(r1Ctx(), store, fill710NSListCall(0), false); !ok {
		t.Fatalf("PRECONDITION: list 0 not content-servable")
	}
	store.DeleteForTest(cache.ComputeKey(contentKeyInputs(r1FsaGVR, fill710NS(0), "")))
	env := envLen.Load()
	seedFill := int64(float64(env) * contentFillExpansionSeed)
	runtime.GC()
	runtime.GC()
	limit := (fill710HeapObjects() + seedFill + seedFill/2) * 8 / 7
	prevLimit := debug.SetMemoryLimit(limit)
	t.Cleanup(func() { debug.SetMemoryLimit(prevLimit) })
	ResetContentFillStatsForTest()
	restore := cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return limit }, fill710HeapObjects)
	t.Cleanup(restore)
	ceiling0, _ := cache.AdmissionCeiling()

	// A background flight holds the WHOLE ceiling and another waits behind it
	// (its weight is the ceiling, so it reports no excursion by itself).
	held := ceiling0/int64(contentFillExpansionSeed) + 1
	holder, _, err := admitContentFillForTest(context.Background(), "burst/v1, Resource=held", held, fillClassBackground)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder)
	bgCtx, bgCancel := context.WithCancel(context.Background())
	t.Cleanup(bgCancel)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if release, _, err := admitContentFillForTest(bgCtx, "burst/v1, Resource=queued", held, fillClassBackground); err == nil {
			release()
		}
	}()
	if !fill710WaitFor(10*time.Second, func() bool { return ContentFillAdmissionStatsSnapshot().Queued["background"] == 1 }) {
		t.Fatalf("PRECONDITION: the background ticket did not queue")
	}

	live, obj, served := fill710Burst(store, lists, readers, r1Ctx)
	adm := ContentFillAdmissionStatsSnapshot()
	t.Logf("#710 PR-2a customer first-load burst (REPORT ONLY — accepted risk, bench run 8 decides): %d cold lists × %d items (envelope %d B), %d customer readers, soft limit %d MiB, starting ceiling %d MiB. "+
		"live-heap growth +%d MiB (heap/objects +%d); admission_over_ceiling_bytes_peak %d MiB; admitted-weight peak %d MiB; customer admission wait max %d ms (%d samples); background queued through it: %d",
		lists, perNS, env, readers, limit>>20, ceiling0>>20, live>>20, obj>>20, adm.OverCeilingPk>>20,
		contentFillAdmissionInflightWeightPeak.Load()>>20, adm.Wait["customer"].MaxMS, adm.Wait["customer"].Count, adm.Queued["background"])
	if served != readers {
		t.Fatalf("customer burst: %d of %d readers served", served, readers)
	}
	if w := adm.Wait["customer"]; w.Count == 0 || w.MaxMS != 0 {
		t.Fatalf("a customer flight waited for admission (%+v) — users never queue", w)
	}
	if q := adm.Queued; q["background"] != 1 || q["customer"] != 0 {
		t.Fatalf("queued %v after the burst: the background ticket must still wait, no customer ever queues", q)
	}
	if adm.OverCeilingPk <= 0 {
		t.Fatalf("PRECONDITION: the burst never pushed the admitted weight past the ceiling — the report shows nothing")
	}
}
