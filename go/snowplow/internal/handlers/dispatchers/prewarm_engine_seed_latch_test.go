// prewarm_engine_seed_latch_test.go — #316: engine-path falsifier for
// seedScopeYielding's per-invocation re-enqueue latch (the PM-gate's
// REQUIRED post-ship item from 0.30.255 / #158).
//
// The legacy runPIPSeed path is covered by phase1_seed_classify_test.go's
// TestRunPIPSeed_DiscriminatesDenyVsOperational. This file is its ENGINE
// companion: it drives the REAL seedScopeYielding call site
// (prewarm_engine_boot.go:seedScopeYielding) and asserts the #158 hunk that
// the legacy test does NOT exercise — the per-invocation reEnqueued latch
// that coalesces N operational target failures into AT MOST ONE
// scopeKindBoot re-enqueue on the process prewarm engine
// (prewarm_engine_boot.go:352-380).
//
// SEAMS USED (mirror seedCohortFn / enumerateAggregatePrewarmTargetsFn):
//   - enumeratePrewarmTargetsForGVRFn — injects the per-binding target list
//     so the widget seed loop runs N iterations without a live RBAC
//     snapshot / built BindingsByGVR index.
//   - seedOneWidgetFn — injects the controlled per-target error class
//     (operational vs RBAC-deny) without an apiserver round-trip.
// Both are driven through the WIDGET loop (targetsFor(e.GVR, true) — the
// loop that passes haveGVR=true unconditionally, so no restActionTargetGVR /
// objects.Get apiserver dependency is needed). The widget loop invokes the
// SAME shared classifyEngineSeedErr closure + reEnqueued latch as the
// restaction loop (prewarm_engine_boot.go:461 calls the identical closure
// as :432), so the latch is fully covered via this path.
//
// PROCESS-GLOBAL STATE: the #158 engine re-enqueue targets the process
// singleton prewarmEngineSingleton() (prewarm_engine_boot.go:379), and the
// pip seed counters are package-level atomics. These tests therefore:
//   - serialize on engineLatchTestMu (no sibling test mutates the singleton
//     queue / customerInFlight concurrently),
//   - assert counter DELTAS (snapshot before/after) — never absolutes,
//   - drain the singleton pending queue to empty at entry (no worker runs on
//     the singleton in this package's test binary — verified: every
//     runWorker call in *_test.go uses a LOCAL &prewarmEngine{}), so the
//     pending map is a stable, inspectable substrate.
//
// Pure unit: no cluster, no informer, no apiserver, deterministic, -race
// clean. Does NOT touch ./internal/rbac/... (destructive TestMain).

package dispatchers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// engineLatchTestMu serializes the tests in this file: they share the
// process prewarmEngineSingleton() queue, the customerInFlightCount atomic,
// and the pip seed counters. Serializing keeps the queue-pending and
// call-count assertions deterministic without leaking state between tests.
var engineLatchTestMu sync.Mutex

// ── shared fixtures / helpers ────────────────────────────────────────────

// engineSeedWidgetGVR is an arbitrary GVR for the injected widget entry.
// The seam-injected enumerator ignores it; it only needs to be the GVR the
// widget loop passes to enumeratePrewarmTargetsForGVRFn(e.GVR, "list").
var engineSeedWidgetGVR = schema.GroupVersionResource{
	Group:    "widgets.krateo.io",
	Version:  "v1beta1",
	Resource: "tables",
}

// makeWidgetEntry builds one navWidgetEntry whose seed will be driven by
// the seedOneWidgetFn seam. The unstructured carries a ns/name so the
// classifier's target label is non-empty (prewarm_engine_boot.go:461 reads
// GetNamespace()+"/"+GetName()).
func makeWidgetEntry(ns, name string) navWidgetEntry {
	w := &unstructured.Unstructured{}
	w.SetNamespace(ns)
	w.SetName(name)
	w.SetGroupVersionKind(schema.GroupVersionKind{
		Group: engineSeedWidgetGVR.Group, Version: engineSeedWidgetGVR.Version, Kind: "Table",
	})
	return navWidgetEntry{W: w, GVR: engineSeedWidgetGVR}
}

// makeTargets returns n per-binding targets for the seam enumerator. Each
// gets a distinct Username so cohortLogLabel renders a distinct, non-empty
// label per target (Username non-empty path, phase1_pip_seed.go:1250-1252).
func makeTargets(n int) []cache.PrewarmTarget {
	out := make([]cache.PrewarmTarget, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, cache.PrewarmTarget{
			BindingUID: fmt.Sprintf("uid-%d", i),
			Subject:    cache.SubjectIdentity{Username: fmt.Sprintf("user-%d", i)},
			GVR:        engineSeedWidgetGVR,
			Verb:       "list",
		})
	}
	return out
}

// zeroCustomerInFlight drives customerInFlightCount to 0 so
// engineYieldCheckpoint (→ singleton.yieldToCustomer) is a fast no-op
// (prewarm_engine.go:436-438). Mirrors the existing prewarm_engine_test.go
// pattern.
func zeroCustomerInFlight() {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}
}

// drainSingletonPending empties the process engine's pending queue so a
// subsequent len(pending) assertion measures only THIS test's enqueues. No
// worker drains the singleton in this package's test binary (verified), so
// the queue is stable between our enqueue and our read. Returns the count
// drained (diagnostic only).
func drainSingletonPending() int {
	e := prewarmEngineSingleton()
	n := 0
	for {
		if _, ok := e.drainScopeForTest(); !ok {
			break
		}
		n++
	}
	return n
}

// singletonPendingBootCount returns how many pending boot-keyed scopes the
// process engine holds. The boot scope key is "boot"
// (prewarm_engine.go:184-188); enqueueScope stores by key into a
// map[string]prewarmScope (prewarm_engine.go:251-254) so all boot enqueues
// coalesce to one map entry.
func singletonPendingBootCount() int {
	e := prewarmEngineSingleton()
	// The boot scope coalesces to a single queue item on key()=="boot", so the
	// count is 0 or 1 — presence is equivalent to the old map-count.
	if e.pendingHasBootForTest() {
		return 1
	}
	return 0
}

// withSeamsAndCounters wires the two seams + log capture + counter snapshots
// for one test body. enumFn supplies the targets; seedFn supplies the
// per-target error class. It returns the post-run counter DELTAS, the engine
// enqueuedTotal delta, and the captured log text.
type seedRunResult struct {
	denyDelta  uint64
	opDelta    uint64
	grandDelta uint64
	enqDelta   uint64
	logText    string
}

func runSeedScopeYielding(t *testing.T,
	widgets []navWidgetEntry,
	enumFn func(schema.GroupVersionResource, string) []cache.PrewarmTarget,
	seedFn func(context.Context, navWidgetEntry, string, seedScopeMode) error,
) seedRunResult {
	t.Helper()

	zeroCustomerInFlight()

	// Capture logs for level/event assertions.
	buf := captureDefaultSlogForTest(t, slog.LevelInfo)

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = enumFn
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevSeed := seedOneWidgetFn
	seedOneWidgetFn = seedFn
	t.Cleanup(func() { seedOneWidgetFn = prevSeed })

	denyBefore := pipSeedRBACDenyTotal.Load()
	opBefore := pipSeedOperationalFailTotal.Load()
	grandBefore := pipBindingSetSeedFailuresTotal.Load()
	enqBefore := prewarmEngineSingleton().enqueuedTotal.Load()

	// No restactions — drive the widget loop only (it carries the same
	// shared classifier + latch). saEP/saRC/authnNS are inert: the seam
	// short-circuits before any of them is dereferenced for I/O.
	if err := seedScopeYielding(context.Background(),
		nil /* restactionRefs */, widgets,
		endpoints.Endpoint{}, nil /* saRC */, "test-authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil (per-target errors are non-fatal)", err)
	}

	return seedRunResult{
		denyDelta:  pipSeedRBACDenyTotal.Load() - denyBefore,
		opDelta:    pipSeedOperationalFailTotal.Load() - opBefore,
		grandDelta: pipBindingSetSeedFailuresTotal.Load() - grandBefore,
		enqDelta:   prewarmEngineSingleton().enqueuedTotal.Load() - enqBefore,
		logText:    buf.String(),
	}
}

// opErr is an operational (transient) failure — a wrapped ctx timeout, the
// canonical "apiserver pressure" shape classifySeedErr maps to operational
// (phase1_seed_classify_test.go's table pins this).
func opErr() error {
	return fmt.Errorf("resolve widget ns/name: %w", context.DeadlineExceeded)
}

// deny403 is an RBAC deny — a %w-wrapped 403 StatusError (the shape
// seedOneWidget would surface for a forbidden widget).
func deny403() error {
	return fmt.Errorf("resolve widget ns/name: %w",
		&apierrors.StatusError{ErrStatus: metav1.Status{Code: 403, Reason: metav1.StatusReasonForbidden}})
}

// ── (1) ONE operational failure → exactly one boot enqueue ───────────────

func TestSeedScopeYielding_OneOperationalFailure_EnqueuesExactlyOnce(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	drainSingletonPending()

	widgets := []navWidgetEntry{makeWidgetEntry("ns", "w0")}
	// ONE target, fails operationally.
	res := runSeedScopeYielding(t, widgets,
		func(schema.GroupVersionResource, string) []cache.PrewarmTarget { return makeTargets(1) },
		func(context.Context, navWidgetEntry, string, seedScopeMode) error { return opErr() },
	)

	if res.enqDelta != 1 {
		t.Errorf("engine enqueuedTotal delta = %d; want 1 (one operational target → exactly one boot re-enqueue)", res.enqDelta)
	}
	if res.opDelta != 1 {
		t.Errorf("operational counter delta = %d; want 1", res.opDelta)
	}
	if res.denyDelta != 0 {
		t.Errorf("rbac_deny counter delta = %d; want 0 (no deny occurred)", res.denyDelta)
	}
	// The enqueued scope must be a BOOT scope (not a GVR-discovered one).
	if got := singletonPendingBootCount(); got != 1 {
		t.Errorf("singleton pending boot-scope count = %d; want 1 (the re-enqueue must be a scopeKindBoot)", got)
	}
	// The operational branch emits a WARN event (not the deny Info) with the
	// re-enqueue effect string — prewarm_engine_boot.go:368-376.
	rec := findLogRecord(t, res.logText, "prewarm.engine.seed.operational_failure")
	if rec == nil {
		t.Fatalf("missing Warn event prewarm.engine.seed.operational_failure; logs:\n%s", res.logText)
	}
	if lvl, _ := rec["level"].(string); lvl != "WARN" {
		t.Errorf("operational_failure level = %q; want WARN", lvl)
	}
	if tgt, _ := rec["target"].(string); tgt != "user-0" {
		t.Errorf("operational_failure target(cohort-label) field = %q; want %q", tgt, "user-0")
	}
	// No expected_deny event for a pure-operational run.
	if d := findLogRecord(t, res.logText, "prewarm.engine.seed.expected_deny"); d != nil {
		t.Errorf("unexpected expected_deny event for a pure-operational run; logs:\n%s", res.logText)
	}
}

// ── (2) N operational failures in ONE invocation → STILL one enqueue ─────
//
// This is THE latch falsifier. reEnqueued (prewarm_engine_boot.go:352,
// 377-380) makes seedScopeYielding enqueue AT MOST ONCE per invocation
// regardless of how many targets fail operationally — so enqueuedTotal moves
// by exactly 1 even though the operational counter moves by N.

func TestSeedScopeYielding_NOperationalFailures_LatchEnqueuesExactlyOnce(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	drainSingletonPending()

	const n = 7
	widgets := []navWidgetEntry{makeWidgetEntry("ns", "w0")}
	res := runSeedScopeYielding(t, widgets,
		func(schema.GroupVersionResource, string) []cache.PrewarmTarget { return makeTargets(n) },
		func(context.Context, navWidgetEntry, string, seedScopeMode) error { return opErr() },
	)

	if res.opDelta != n {
		t.Errorf("operational counter delta = %d; want %d (every operational target must bump the counter)", res.opDelta, n)
	}
	if res.enqDelta != 1 {
		t.Errorf("engine enqueuedTotal delta = %d; want 1 — THE LATCH: %d operational failures in one "+
			"seedScopeYielding invocation must coalesce to exactly ONE boot re-enqueue (reEnqueued latch, "+
			"prewarm_engine_boot.go:352,377-380)", res.enqDelta, n)
	}
	if got := singletonPendingBootCount(); got != 1 {
		t.Errorf("singleton pending boot-scope count = %d; want 1 (N operational failures still leave one pending boot scope)", got)
	}
	// Back-compat sum invariant holds at N operational, 0 deny.
	if res.grandDelta != res.denyDelta+res.opDelta {
		t.Errorf("grand-total delta %d != deny %d + operational %d", res.grandDelta, res.denyDelta, res.opDelta)
	}
}

// ── (3) RBAC-deny target → ZERO enqueues, deny counter++, Info log ───────

func TestSeedScopeYielding_RBACDeny_NoEnqueue_BumpsDenyCounter(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	drainSingletonPending()

	widgets := []navWidgetEntry{makeWidgetEntry("ns", "w0")}
	res := runSeedScopeYielding(t, widgets,
		func(schema.GroupVersionResource, string) []cache.PrewarmTarget { return makeTargets(1) },
		func(context.Context, navWidgetEntry, string, seedScopeMode) error { return deny403() },
	)

	if res.denyDelta != 1 {
		t.Errorf("rbac_deny counter delta = %d; want 1 (a 403 target must bump rbac_deny)", res.denyDelta)
	}
	if res.opDelta != 0 {
		t.Errorf("operational counter delta = %d; want 0 (an RBAC deny is NOT operational)", res.opDelta)
	}
	if res.enqDelta != 0 {
		t.Errorf("engine enqueuedTotal delta = %d; want 0 (an RBAC deny must NOT re-enqueue a boot re-walk)", res.enqDelta)
	}
	if got := singletonPendingBootCount(); got != 0 {
		t.Errorf("singleton pending boot-scope count = %d; want 0 (deny-only run must enqueue nothing)", got)
	}

	// Info-level expected_deny event with the target label.
	rec := findLogRecord(t, res.logText, "prewarm.engine.seed.expected_deny")
	if rec == nil {
		t.Fatalf("missing Info event prewarm.engine.seed.expected_deny for the 403 target; logs:\n%s", res.logText)
	}
	if lvl, _ := rec["level"].(string); lvl != "INFO" {
		t.Errorf("expected_deny level = %q; want INFO (a genuine deny is expected, not a Warn)", lvl)
	}
	// classifyEngineSeedErr(kind="widget", label=ns/name, target=cohortLabel)
	// — prewarm_engine_boot.go:353,357-363. The COHORT/identity label is
	// logged under "target"; the RESOURCE (ns/name) is logged under the
	// kind-named key ("widget").
	if tgt, _ := rec["target"].(string); tgt != "user-0" {
		t.Errorf("expected_deny target(cohort-label) field = %q; want %q", tgt, "user-0")
	}
	if w, _ := rec["widget"].(string); w != "ns/w0" {
		t.Errorf("expected_deny widget(resource) field = %q; want %q", w, "ns/w0")
	}
	// No operational_failure event should have been emitted.
	if op := findLogRecord(t, res.logText, "prewarm.engine.seed.operational_failure"); op != nil {
		t.Errorf("unexpected operational_failure event for a pure-deny run; logs:\n%s", res.logText)
	}
}

// ── (4) grand total == rbac_deny + operational at every combination ──────
//
// Back-compat sum invariant (pipBindingSetSeedFailuresTotal is the kept
// roll-up; prewarm_engine_boot.go:354 bumps it for EVERY classified failure
// before the deny/operational split). Driven across the meaningful matrix.

func TestSeedScopeYielding_CounterSumInvariant(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	// Each case: a sequence of per-target outcomes. The seam returns the
	// outcome for the i-th target by Username suffix ("user-<i>").
	cases := []struct {
		name    string
		denies  int
		ops     int
		wantEnq uint64 // 1 iff at least one operational failure (latch), else 0
	}{
		{name: "1 deny 0 op", denies: 1, ops: 0, wantEnq: 0},
		{name: "0 deny 1 op", denies: 0, ops: 1, wantEnq: 1},
		{name: "2 deny 3 op", denies: 2, ops: 3, wantEnq: 1},
		{name: "3 deny 0 op", denies: 3, ops: 0, wantEnq: 0},
		{name: "0 deny 4 op", denies: 0, ops: 4, wantEnq: 1},
		{name: "5 deny 5 op", denies: 5, ops: 5, wantEnq: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drainSingletonPending()

			total := tc.denies + tc.ops
			widgets := []navWidgetEntry{makeWidgetEntry("ns", "w0")}

			// Build a target list; index < denies → deny, else → operational.
			targets := makeTargets(total)
			outcome := make(map[string]error, total)
			for i := 0; i < total; i++ {
				if i < tc.denies {
					outcome[targets[i].Subject.Username] = deny403()
				} else {
					outcome[targets[i].Subject.Username] = opErr()
				}
			}

			// The seam carries the target identity via the cohort ctx, but
			// seedOneWidgetFn receives only (ctx, entry, ns). We therefore
			// drive the per-target error by call order using a counter — the
			// widget loop calls seedOneWidgetFn once per target in slice
			// order (prewarm_engine_boot.go:452-455).
			var idx int
			res := runSeedScopeYielding(t, widgets,
				func(schema.GroupVersionResource, string) []cache.PrewarmTarget { return targets },
				func(context.Context, navWidgetEntry, string, seedScopeMode) error {
					u := targets[idx].Subject.Username
					idx++
					return outcome[u]
				},
			)

			if res.denyDelta != uint64(tc.denies) {
				t.Errorf("rbac_deny delta = %d; want %d", res.denyDelta, tc.denies)
			}
			if res.opDelta != uint64(tc.ops) {
				t.Errorf("operational delta = %d; want %d", res.opDelta, tc.ops)
			}
			// THE invariant.
			if res.grandDelta != res.denyDelta+res.opDelta {
				t.Errorf("grand-total delta %d != rbac_deny %d + operational %d (back-compat sum invariant violated)",
					res.grandDelta, res.denyDelta, res.opDelta)
			}
			if res.grandDelta != uint64(total) {
				t.Errorf("grand-total delta = %d; want %d (every classified failure bumps the roll-up once)", res.grandDelta, total)
			}
			// Latch: at most one enqueue, fired iff ≥1 operational.
			if res.enqDelta != tc.wantEnq {
				t.Errorf("engine enqueuedTotal delta = %d; want %d (latch: ≤1, fired iff ≥1 operational)", res.enqDelta, tc.wantEnq)
			}
		})
	}
}

// ── (5) two invocations → singleton pending["boot"] holds ONE entry ──────
//
// The reEnqueued latch is PER-INVOCATION (a local bool, prewarm_engine_boot.go:352),
// so two separate seedScopeYielding invocations each enqueue once →
// enqueuedTotal moves by 2. But both enqueue the SAME boot-keyed scope, and
// the engine queue dedups on key()=="boot" (prewarm_engine.go:184-188,
// 251-254: enqueueScope stores by key into map[string]prewarmScope), so the
// pending queue coalesces them to exactly ONE entry. This is the
// queue-dedup half of the storm bound (design §3.1) reached at the real
// singleton without standing up the engine worker.

func TestSeedScopeYielding_TwoInvocations_QueueCoalescesToOnePending(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	drainSingletonPending()

	enqBefore := prewarmEngineSingleton().enqueuedTotal.Load()

	widgets := []navWidgetEntry{makeWidgetEntry("ns", "w0")}
	enumFn := func(schema.GroupVersionResource, string) []cache.PrewarmTarget { return makeTargets(1) }
	seedFn := func(context.Context, navWidgetEntry, string, seedScopeMode) error { return opErr() }

	// Invocation 1 — fresh latch, enqueues one boot scope.
	runSeedScopeYielding(t, widgets, enumFn, seedFn)
	// Invocation 2 — fresh latch again, enqueues one boot scope.
	runSeedScopeYielding(t, widgets, enumFn, seedFn)

	// enqueuedTotal counted BOTH enqueues (cumulative, no dedup at the counter).
	if got := prewarmEngineSingleton().enqueuedTotal.Load() - enqBefore; got != 2 {
		t.Errorf("engine enqueuedTotal delta across two invocations = %d; want 2 "+
			"(per-invocation latch fires once each; the counter is cumulative)", got)
	}
	// ...but the pending queue coalesced them to ONE boot entry (key dedup).
	if got := singletonPendingBootCount(); got != 1 {
		t.Errorf("singleton pending boot-scope count after two invocations = %d; want 1 "+
			"(both boot enqueues coalesce on key()==\"boot\" — prewarm_engine.go:184-188,251-254)", got)
	}
	// And the queue holds exactly one entry total (no stray scope kinds).
	e := prewarmEngineSingleton()
	total := e.pendingLenForTest()
	if total != 1 {
		t.Errorf("singleton total pending entries = %d; want 1 (only the coalesced boot scope)", total)
	}

	// Sanity: the one entry dequeues as a boot scope.
	s, ok := prewarmEngineSingleton().drainScopeForTest()
	if !ok || s.kind != scopeKindBoot {
		t.Fatalf("expected one boot scope to dequeue, got %+v ok=%v", s, ok)
	}
}

// Compile-time guards that the seams have the exact signatures the
// production functions expose (so a signature drift breaks the build here,
// not silently at the swap site).
var (
	_ func(schema.GroupVersionResource, string) []cache.PrewarmTarget    = cache.EnumeratePrewarmTargetsForGVR
	_ func(context.Context, navWidgetEntry, string, seedScopeMode) error = seedOneWidget
)
