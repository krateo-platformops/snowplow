// prewarm_keepwarm_sweep_test.go — keepwarm sweep falsifiers (c1 cadence arms +
// the c2 sweep-set evolution of GTTL-3/ARM-SCOPE → C2-C1).
//
// Hermetic, -race, seams only (no cluster / apiserver). Serializes on
// engineLatchTestMu (shares the process engine singleton queue + seed counters
// + customerInFlight atomic with the sibling engine tests). Does NOT touch
// ./internal/rbac/... (destructive TestMain).
//
// ARMS:
//   GTTL-2  TestKeepwarmSweep_CadenceIsTTLTimesThreeQuarters — the cadence is
//           the TTL×3/4 design ratio derived from RESOLVED_CACHE_TTL_SECONDS,
//           not a magic duration.
//   GTTL-6  TestKeepwarmSweep_EnqueuesScopeNotStoreScan — a tick enqueues a
//           scopeKindKeepwarm on the engine (seed-side re-enqueue; the store is
//           never scanned). Coalesces on key()=="keepwarm".
//   ARM-KEEPWARM (cadence loop) TestKeepwarmSweep_TickerEnqueuesThenStopsOnCancel
//           — runKeepwarmSweepLoop enqueues on tick and exits on ctx cancel
//           (no goroutine leak).
//   C2-C1 / ARM-SCOPE (keepwarm c2 sweep-set, evolves GTTL-3) —
//           TestKeepwarmSweep_WidgetCapablePrefix_SeedsAllCapableNotWidgetless
//           (+ two mutations): keepwarm mode seeds the WIDGET-CAPABLE PREFIX
//           (W1 AND W2, both widgetMax>=1) and NOT the widget-less M
//           (widgetMax==0). Mutation (i) restore the c1 rank-1 bound → RED (W2
//           unseeded); mutation (ii) drop the widgetMax==0 break → RED (M seeded
//           = boot behavior leaking into keepwarm).
//   C2-C2 (machine-SA capability inclusion) — TestKeepwarmSweep_MachineSA... : a
//           machine SA WITH a widget binding IS swept (capability, not
//           login-ness). Mirrors the LIVE fresh2 portals-v1-3-5 SA.
//   GTTL-1 / ARM-BACKSTOP TestSeedPutGate_DeclinesOnStageError (+ mutation +
//           empty-result control) lives in phase1_seed_put_gate_test.go, re-run
//           under seedModeKeepwarm by C2-C6 in prewarm_keepwarm_c2_test.go.

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/apimachinery/pkg/runtime/schema"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// GTTL-2 — cadence == TTL×3/4 derived from RESOLVED_CACHE_TTL_SECONDS.
func TestKeepwarmSweep_CadenceIsTTLTimesThreeQuarters(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "3600")
	ttl := cache.ResolvedCacheTTL()
	if ttl != 3600*time.Second {
		t.Fatalf("ResolvedCacheTTL: want 3600s; got %v", ttl)
	}
	got := keepwarmSweepInterval()
	want := ttl * keepwarmCadenceNumerator / keepwarmCadenceDenominator // 3/4
	if got != want {
		t.Fatalf("keepwarmSweepInterval: want TTL×3/4 = %v; got %v", want, got)
	}
	// Concretely: 3600s × 3/4 = 2700s.
	if got != 2700*time.Second {
		t.Fatalf("keepwarmSweepInterval @3600s TTL: want 2700s; got %v", got)
	}

	// The c2 age-skip threshold = TTL − sweepInterval = TTL/4, DERIVED (not a
	// literal): 3600s − 2700s = 900s.
	if th := keepwarmAgeSkipThreshold(); th != 900*time.Second {
		t.Fatalf("keepwarmAgeSkipThreshold @3600s TTL: want TTL/4 = 900s; got %v", th)
	}

	// A different TTL re-derives (no hardcoded interval): 800s × 3/4 = 600s;
	// threshold 800−600 = 200s.
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "800")
	if got := keepwarmSweepInterval(); got != 600*time.Second {
		t.Fatalf("keepwarmSweepInterval @800s TTL: want 600s; got %v", got)
	}
	if th := keepwarmAgeSkipThreshold(); th != 200*time.Second {
		t.Fatalf("keepwarmAgeSkipThreshold @800s TTL: want TTL/4 = 200s; got %v", th)
	}
}

// GTTL-6 — a tick ENQUEUES a scopeKindKeepwarm on the engine singleton (the
// sweep is seed-side re-enqueue, not a store scan) and coalesces on "keepwarm".
func TestKeepwarmSweep_EnqueuesScopeNotStoreScan(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	drainSingletonPending() // clean slate

	enqueueKeepwarmSweep()
	enqueueKeepwarmSweep() // second tick coalesces (dedup on key()=="keepwarm")

	e := prewarmEngineSingleton()
	n := e.pendingLenForTest()
	// Drain the single coalesced scope to confirm it is the keepwarm kind
	// (the workqueue has no key-peek; presence == a drained keepwarm scope).
	s, ok := e.drainScopeForTest()
	hasKeepwarm := ok && s.kind == scopeKindKeepwarm

	if !hasKeepwarm {
		t.Fatalf("expected a pending scopeKindKeepwarm after enqueue; pending has no \"keepwarm\" key")
	}
	if n != 1 {
		t.Fatalf("expected the two ticks to COALESCE to 1 pending scope (dedup on key); got %d pending", n)
	}
	drainSingletonPending()
}

// ARM-KEEPWARM (cadence loop) — the ticker enqueues on each tick and the loop
// exits promptly on ctx cancel (no leak). Uses a short interval directly (not
// the production TTL×3/4) so the test is fast; the cadence VALUE is asserted by
// GTTL-2 above.
func TestKeepwarmSweep_TickerEnqueuesThenStopsOnCancel(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	drainSingletonPending()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runKeepwarmSweepLoop(ctx, 5*time.Millisecond)
		close(done)
	}()

	// Wait until at least one tick enqueued a keepwarm scope.
	deadline := time.Now().Add(2 * time.Second)
	enqueued := false
	for time.Now().Before(deadline) {
		e := prewarmEngineSingleton()
		// Only keepwarm scopes are enqueued in this test (clean slate), so a
		// non-empty queue with a keepwarm-kind head confirms the tick fired.
		if e.pendingLenForTest() > 0 {
			s, ok := e.drainScopeForTest()
			if ok && s.kind == scopeKindKeepwarm {
				enqueued = true
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if !enqueued {
		cancel()
		<-done
		t.Fatal("ARM-KEEPWARM: ticker did not enqueue a keepwarm scope within 2s")
	}

	cancel()
	select {
	case <-done:
		// loop exited on ctx cancel — no leak.
	case <-time.After(2 * time.Second):
		t.Fatal("ARM-KEEPWARM: runKeepwarmSweepLoop did not exit within 2s of ctx cancel (goroutine leak)")
	}
	drainSingletonPending()
}

// C2-C1 / ARM-SCOPE — the keepwarm c2 sweep-set evolution of GTTL-3.
//
// EXPECTATION INVERTS BY DESIGN (design §6 c1-arm table): the pre-c2 (c1) arm
// asserted the rank-1-only sweep bound seeded ONLY rank-1 (devs) and NOT rank-2
// (ops); both were widget-capable, so c2 (seedModeKeepwarm, widget-capable
// prefix) sweeps BOTH. The RED boundary MOVES from "rank-2" to "widgetMax==0".
// The new fixture adds a widget-LESS identity M (RA-only, widgetMax==0) so the
// boundary is exercisable.
//
// Fixture: two widget-capable identities on a widget's GVR — W1 (collapsed=200)
// and W2 (collapsed=5, a lower rank but still widget-capable) — and a
// widget-LESS identity M (collapsed=1344) that appears ONLY in an RA target set
// (widgetMax==0). keepwarm mode must seed W1 AND W2's widget targets and NONE of
// M's (M is widget-less → below the break).
//
// Mutation (i): restore the c1 rank-1 bound (`ri>0 break`) → RED (W2 unseeded).
// Mutation (ii): drop the widgetMax==0 break → RED (M's RA targets seeded =
// boot behavior leaking into keepwarm; the machine-SA-free fixture's M gets
// seeded).
func TestKeepwarmSweep_WidgetCapablePrefix_SeedsAllCapableNotWidgetless(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	w1 := eID{name: "w1", group: true, collapsed: 200}
	w2 := eID{name: "w2", group: true, collapsed: 5}
	m := eID{name: "m", group: true, collapsed: 1344} // widget-less: RA-only

	h := newNavWidgetHarvester()
	widgetGVR := eWidgetGVR("flexes")
	eHarvestWidget(h, "dashboard-flex", widgetGVR)
	widgets := h.snapshot()
	ras := []templatesv1.ObjectReference{eRA("bulk-ra")}

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(g schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		switch g {
		case widgetGVR:
			return eIdentityTargets(g, w1, w2) // widget-capable cohorts
		case eFanoutRAGVR:
			return eIdentityTargets(g, m) // M appears ONLY here → widgetMax==0
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevTGVR := restActionTargetGVRFn
	restActionTargetGVRFn = func(_ context.Context, _ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
		return eFanoutRAGVR, true // bulk-ra LISTs eFanoutRAGVR (M's bucket)
	}
	t.Cleanup(func() { restActionTargetGVRFn = prevTGVR })

	// run seeds under seedModeKeepwarm and records which identities seeded a
	// WIDGET target and which seeded an RA target (M only appears via the RA).
	run := func() (widgetSeen, raSeen map[string]bool) {
		widgetSeen, raSeen = map[string]bool{}, map[string]bool{}
		prevW := seedOneWidgetFn
		prevR := seedOneRestactionFn
		seedOneWidgetFn = func(ctx context.Context, _ navWidgetEntry, _ string, _ seedScopeMode) error {
			widgetSeen[eIdentityLabel(ctx)] = true
			return nil
		}
		seedOneRestactionFn = func(ctx context.Context, _ redact.Label, _ templatesv1.ObjectReference, _ string, _ seedScopeMode) error {
			raSeen[eIdentityLabel(ctx)] = true
			return nil
		}
		defer func() { seedOneWidgetFn, seedOneRestactionFn = prevW, prevR }()
		if err := seedScopeYielding(context.Background(), ras, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeKeepwarm); err != nil {
			t.Fatalf("seedScopeYielding(keepwarm) returned %v; want nil", err)
		}
		return widgetSeen, raSeen
	}

	// GREEN: keepwarm seeds BOTH widget-capable cohorts (W1 AND W2) and NONE of
	// the widget-less M's targets.
	widgetSeen, raSeen := run()
	if !widgetSeen["group:w1"] {
		t.Fatalf("C2-C1: keepwarm sweep did NOT seed W1 (widget-capable, rank 0); widgetSeen=%v", widgetSeen)
	}
	if !widgetSeen["group:w2"] {
		t.Fatalf("C2-C1: keepwarm sweep did NOT seed W2 (widget-capable, LOWER rank) — c2 sweeps the whole widget-capable prefix, not rank-1 only; widgetSeen=%v", widgetSeen)
	}
	if raSeen["group:m"] {
		t.Fatalf("C2-C1: keepwarm sweep seeded the WIDGET-LESS M (widgetMax==0) — the widget-capable prefix must break BEFORE M; raSeen=%v", raSeen)
	}
}

// C2-C2 (machine-SA capability inclusion; predicate honesty) — a machine SA
// carrying a widget binding (widgetMax>=1) IS swept by keepwarm. Pinned so the
// capability-not-login-ness semantics are explicit, not accidental (design §5
// C2-C2; the deferred (c) refinement's target). Fixture mirrors the LIVE fresh2
// instance: system:serviceaccount:krateo-system:portals-v1-3-5 with a widget
// binding.
func TestKeepwarmSweep_MachineSAWithWidgetBinding_IsSwept(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	// A machine SA (username form) with a widget binding — widgetMax==1.
	machineSA := eID{name: "system:serviceaccount:krateo-system:portals-v1-3-5", group: false, collapsed: 1}

	h := newNavWidgetHarvester()
	widgetGVR := eWidgetGVR("flexes")
	eHarvestWidget(h, "dashboard-flex", widgetGVR)
	widgets := h.snapshot()

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(g schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		if g == widgetGVR {
			return eIdentityTargets(g, machineSA)
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	seen := map[string]bool{}
	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, _ navWidgetEntry, _ string, _ seedScopeMode) error {
		seen[eIdentityLabel(ctx)] = true
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeKeepwarm); err != nil {
		t.Fatalf("seedScopeYielding(keepwarm) returned %v; want nil", err)
	}
	if !seen["user:system:serviceaccount:krateo-system:portals-v1-3-5"] {
		t.Fatalf("C2-C2: a machine SA WITH a widget binding (widgetMax>=1) must be swept — capability, not login-ness; seen=%v", seen)
	}
}
