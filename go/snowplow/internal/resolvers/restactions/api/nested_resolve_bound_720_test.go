// nested_resolve_bound_720_test.go — #720 falsifiers: customer nested trees
// never wait on the nested-resolve admission gate, but they count, and how far
// they push the in-flight weight past the ceiling is visible.
//
// Owner ruling (Diego, 2026-10-15, #710): "No one waits in queue"; for this
// gate, "Remove it too". The gate now throttles background only.
//
//   - N1 customer never waits: zero headroom, a tree in flight and a background
//     tree queued; a customer tree is admitted at once, with no error even when
//     its ctx is near-expired or already done. Mutation "customer waits on the
//     ceiling" → RED.
//   - N2 customer counts: a background tree that fits on its own waits while a
//     customer tree is in flight, and admits when it releases. Mutation
//     "customer not counted" → RED.
//   - N3 background bounded: TestAggC1_ByteCeilingHeld_GREEN (background trees
//     only, peak under the ceiling, all complete) plus the classification arm
//     below (every background driver waits).
//   - N4 over-ceiling visible: a customer burst past the ceiling moves the gauge
//     and its peak, on expvar here and on OTLP in
//     internal/metrics/metrics_720_nested_over_ceiling_otlp_test.go. Mutation
//     "gauge not updated" → RED.
//
// Hermetic and in-process: the runtime-headroom seam drives the ceiling.
package api

import (
	"context"
	"encoding/json"
	"expvar"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// bgTreeCtx is an outermost BACKGROUND tree, as the refresher stamps it.
func bgTreeCtx() context.Context {
	return cache.WithBackgroundResolveOrigin(cache.WithNestedCallDepth(context.Background(), 0), cache.BackgroundOriginRefresher)
}

// custTreeCtx is an outermost CUSTOMER tree: a /call carries no background
// marker.
func custTreeCtx() context.Context {
	return cache.WithNestedCallDepth(context.Background(), 0)
}

// enterAsync enters the gate on its own goroutine. admitted closes when the
// gate returns; the result is then readable from the returned pointers.
func enterAsync(ctx context.Context) (admitted chan struct{}, rel *func(), err *error) {
	admitted = make(chan struct{})
	var r func()
	var e error
	go func() {
		r, e = enterNestedResolveUnit(ctx)
		close(admitted)
	}()
	return admitted, &r, &e
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

const n720Settle = 100 * time.Millisecond

// admitCustomerBounded enters a customer tree and fails (instead of hanging)
// if the gate makes it wait: under a "customer waits" mutation a customer with
// no deadline would otherwise block the arm until the package timeout.
func admitCustomerBounded(t *testing.T) func() {
	t.Helper()
	done, rel, err := enterAsync(custTreeCtx())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a customer tree WAITED on the nested gate (no return within 2s) — customers must never wait")
	}
	if *err != nil {
		t.Fatalf("customer tree got an admission error: %v", *err)
	}
	return *rel
}

// TestIssue720_N1_CustomerNeverWaits: zero headroom (live heap above the
// limit), one background tree in flight and a second background tree queued.
// A customer tree with a near-expired ctx, and another with an already-done
// ctx, are both admitted at once with no error. The queued background tree
// stays queued: the customer neither waits behind it nor admits it.
func TestIssue720_N1_CustomerNeverWaits(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	h := &headroomModel{
		limit:            1024 * mib,
		base:             2048 * mib, // above the limit: ceiling 0, zero headroom
		perTreeFootprint: 256 * mib,
	}
	setRuntimeSeamsForTest(h.limitFn, h.liveFn)
	pinEstUnit(h.perTreeFootprint)
	if c, unlimited := admissionCeiling(); unlimited || c != 0 {
		t.Fatalf("PRECONDITION: ceiling=%d unlimited=%v, want a zero-headroom finite ceiling", c, unlimited)
	}

	// Background A: the empty-gate escape admits it; it holds.
	relA, errA := enterNestedResolveUnit(bgTreeCtx())
	if errA != nil {
		t.Fatalf("setup: background A: %v", errA)
	}
	// Background Q queues behind A.
	qCtx, qCancel := context.WithCancel(bgTreeCtx())
	defer qCancel()
	qDone, qRel, qErr := enterAsync(qCtx)
	time.Sleep(n720Settle)
	if isClosed(qDone) {
		t.Fatalf("PRECONDITION: background Q was not queued (err=%v)", *qErr)
	}

	// Customer with a near-expired ctx: must return at once, with no error,
	// before its deadline.
	nearCtx, nearCancel := context.WithTimeout(custTreeCtx(), 80*time.Millisecond)
	defer nearCancel()
	start := time.Now()
	relC1, errC1 := enterNestedResolveUnit(nearCtx)
	waited := time.Since(start)
	if errC1 != nil {
		t.Fatalf("N1 CUSTOMER GOT AN ADMISSION ERROR after %v: %v — a customer tree must never wait on "+
			"the nested gate nor receive its 503", waited, errC1)
	}
	if nearCtx.Err() != nil {
		t.Fatalf("N1 CUSTOMER WAITED: the gate returned after %v, past the customer's 80ms deadline", waited)
	}

	// Customer with an already-done ctx: still admitted, still no error.
	doneCtx, doneCancel := context.WithCancel(custTreeCtx())
	doneCancel()
	relC2, errC2 := enterNestedResolveUnit(doneCtx)
	if errC2 != nil {
		t.Fatalf("N1 CUSTOMER with a done ctx got an admission error: %v — the gate must not turn a "+
			"customer's ctx into a 503", errC2)
	}

	w, n := inFlightWeightForTest()
	if n != 3 || w != 3*h.perTreeFootprint {
		t.Fatalf("N1: in flight = %d trees / %d MiB, want 3 / %d MiB (A + two customers; customers COUNT)",
			n, w/mib, 3*h.perTreeFootprint/mib)
	}
	if isClosed(qDone) {
		t.Fatalf("N1: queued background Q was admitted by a customer's arrival (err=%v) — the customer "+
			"must neither wait behind it nor let it past the ceiling", *qErr)
	}

	relC1()
	relC2()
	relA()
	select {
	case <-qDone:
	case <-time.After(3 * time.Second):
		t.Fatal("N1: background Q never admitted after every tree released (empty-gate escape)")
	}
	if *qErr != nil {
		t.Fatalf("N1: background Q: %v", *qErr)
	}
	(*qRel)()
}

// TestIssue720_N2_CustomerCountsAndSqueezesBackground: the live heap is held
// constant, so the only thing that can make the background tree wait is the
// accounted in-flight weight. Control: alone, the background tree fits and
// admits at once. With a customer tree in flight, the same background tree
// waits; when the customer releases, it admits.
func TestIssue720_N2_CustomerCountsAndSqueezesBackground(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	// ceiling = 1024 − 0 − 128 = 896 MiB. One 700 MiB tree fits; two do not.
	setRuntimeSeamsForTest(func() int64 { return 1024 * mib }, func() int64 { return 0 })
	pinEstUnit(700 * mib)

	// Control: the background tree fits on its own.
	relCtl, errCtl := enterNestedResolveUnit(bgTreeCtx())
	if errCtl != nil {
		t.Fatalf("control: background tree alone: %v", errCtl)
	}
	relCtl()

	// A customer tree in flight.
	relC, errC := enterNestedResolveUnit(custTreeCtx())
	if errC != nil {
		t.Fatalf("customer: %v", errC)
	}

	bgCtx, bgCancel := context.WithTimeout(bgTreeCtx(), 5*time.Second)
	defer bgCancel()
	bgDone, bgRel, bgErr := enterAsync(bgCtx)
	time.Sleep(n720Settle)
	if isClosed(bgDone) {
		if *bgErr == nil {
			(*bgRel)()
		}
		t.Fatalf("N2 CUSTOMER NOT COUNTED: a background tree admitted (err=%v) while a 700 MiB customer "+
			"tree was in flight under an 896 MiB ceiling — customer weight must squeeze background", *bgErr)
	}

	relC()
	select {
	case <-bgDone:
	case <-time.After(3 * time.Second):
		t.Fatal("N2: background tree never admitted after the customer released")
	}
	if *bgErr != nil {
		t.Fatalf("N2: background tree: %v", *bgErr)
	}
	(*bgRel)()
	if w, n := inFlightWeightForTest(); w != 0 || n != 0 {
		t.Fatalf("N2: in flight after all releases = %d trees / %d bytes, want 0/0", n, w)
	}
}

// TestIssue720_N3_EveryBackgroundDriverWaits: the classification is by driver.
// Each background marker (refresher/seed/prewarm-engine origin, the content
// prewarm, the phase-1 boot walk's prewarm path) makes a tree WAIT at zero
// headroom behind an in-flight tree; a plain customer ctx does not.
func TestIssue720_N3_EveryBackgroundDriverWaits(t *testing.T) {
	root := func() context.Context { return cache.WithNestedCallDepth(context.Background(), 0) }
	drivers := map[string]context.Context{
		"refresher":       cache.WithBackgroundResolveOrigin(root(), cache.BackgroundOriginRefresher),
		"cohort_seed":     cache.WithBackgroundResolveOrigin(root(), cache.BackgroundOriginCohortSeed),
		"prewarm_engine":  cache.WithBackgroundResolveOrigin(root(), cache.BackgroundOriginPrewarmEngineBoot),
		"content_prewarm": cache.WithPrewarmPath(cache.WithApistagePrewarm(root())), // as withContentPrewarmSAContext stamps it
		"boot_walk":       cache.WithPrewarmPath(root()),
	}
	if isBackgroundNestedTree(custTreeCtx()) {
		t.Fatal("a plain /call ctx must classify as a CUSTOMER tree")
	}
	for name, dctx := range drivers {
		t.Run(name, func(t *testing.T) {
			resetNestedResolveBoundForTest()
			t.Cleanup(resetNestedResolveBoundForTest)
			setRuntimeSeamsForTest(func() int64 { return 1024 * mib }, func() int64 { return 2048 * mib })
			pinEstUnit(256 * mib)

			if !isBackgroundNestedTree(dctx) {
				t.Fatalf("driver %s must classify as BACKGROUND", name)
			}
			relA, errA := enterNestedResolveUnit(custTreeCtx())
			if errA != nil {
				t.Fatalf("setup: %v", errA)
			}
			defer relA()
			ctx, cancel := context.WithTimeout(dctx, 120*time.Millisecond)
			defer cancel()
			rel, err := enterNestedResolveUnit(ctx)
			if err == nil {
				rel()
				t.Fatalf("N3: %s tree was admitted at zero headroom behind an in-flight tree — background "+
					"must stay bounded", name)
			}
		})
	}
}

// TestIssue720_N4_OverCeilingVisibleOnExpvar: customer trees past the ceiling
// move snowplow_nested_resolve_admission_over_ceiling_bytes (and its peak) by
// exactly the excess they cause; it is 0 while they fit, it is not charged to
// a lone background tree, it returns to 0 on release while the peak holds.
func TestIssue720_N4_OverCeilingVisibleOnExpvar(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)
	RegisterNestedAdmissionExpvarForTest()

	read := func(name string) int64 {
		t.Helper()
		v := expvar.Get(name)
		if v == nil {
			t.Fatalf("%s is not published", name)
		}
		var out int64
		if err := json.Unmarshal([]byte(v.String()), &out); err != nil {
			t.Fatalf("%s = %q is not an integer: %v", name, v.String(), err)
		}
		return out
	}
	const gauge = "snowplow_nested_resolve_admission_over_ceiling_bytes"
	const peak = "snowplow_nested_resolve_admission_over_ceiling_bytes_peak"
	check := func(step string, wantGauge, wantPeak int64) {
		t.Helper()
		s := NestedResolveAdmissionStatsSnapshot()
		if s.OverCeilingBytes != wantGauge || s.OverCeilingBytesPeak != wantPeak {
			t.Fatalf("N4 %s: snapshot over=%d MiB peak=%d MiB, want %d / %d MiB", step,
				s.OverCeilingBytes/mib, s.OverCeilingBytesPeak/mib, wantGauge/mib, wantPeak/mib)
		}
		if g, p := read(gauge), read(peak); g != wantGauge || p != wantPeak {
			t.Fatalf("N4 %s: expvar over=%d peak=%d, want %d / %d (expvar diverged from the gate)",
				step, g, p, wantGauge, wantPeak)
		}
	}

	var live int64 // the seam's live heap; changed only between gate calls
	setRuntimeSeamsForTest(func() int64 { return 1024 * mib }, func() int64 { return live })
	pinEstUnit(700 * mib)

	// A lone BACKGROUND tree that does not fit (empty-gate escape) is not a
	// customer excess.
	live = 2048 * mib
	relBg, errBg := enterNestedResolveUnit(bgTreeCtx())
	if errBg != nil {
		t.Fatalf("background: %v", errBg)
	}
	check("lone background past the ceiling", 0, 0)
	relBg()
	live = 0

	// ceiling = 896 MiB. One 700 MiB customer fits.
	rel1 := admitCustomerBounded(t)
	check("one customer that fits", 0, 0)
	rel2 := admitCustomerBounded(t)
	check("two customers", 1400*mib-896*mib, 1400*mib-896*mib)
	rel3 := admitCustomerBounded(t)
	check("three customers", 2100*mib-896*mib, 2100*mib-896*mib)

	rel3()
	check("after one release", 1400*mib-896*mib, 2100*mib-896*mib)
	rel2()
	rel1()
	check("after every release", 0, 2100*mib-896*mib)
}
