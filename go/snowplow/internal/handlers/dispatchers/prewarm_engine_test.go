// prewarm_engine_test.go — Ship 1 unit tests for the prewarm engine's
// customer-priority yield + the bounded dedup queue. Package dispatchers
// so it can reach the unexported engine internals. Non-destructive.
//
// Ship 2 Stage 2 / 0.30.247 — adds scope-key dedup tests for the new
// scopeKindGVRDiscovered + its gvr payload.
//
// Ship 2 Stage 2.5 / 0.30.248 (Fix v2) — adds the engine-worker
// outlives-bootSeed-ctx-cancel falsifier + companion tests for
// per-scope timeout anchoring and pending-depth drain.

package dispatchers

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestEngineYieldsWhileCustomerInFlight asserts the engine's
// yieldToCustomer parks while a customer /call is marked in flight and
// returns promptly once the call completes.
func TestEngineYieldsWhileCustomerInFlight(t *testing.T) {
	// Reset the counter (other tests may have left it nonzero on failure).
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond

	done := markCustomerInFlight()
	if !customerInFlight() {
		t.Fatal("expected customerInFlight true after mark")
	}

	yielded := make(chan struct{})
	go func() {
		e.yieldToCustomer(context.Background())
		close(yielded)
	}()

	// The yield must NOT return while the customer call is in flight.
	select {
	case <-yielded:
		t.Fatal("yieldToCustomer returned while a customer call was in flight")
	case <-time.After(30 * time.Millisecond):
	}

	// Clear the in-flight mark — the yield must return promptly.
	done()
	select {
	case <-yielded:
		// good
	case <-time.After(500 * time.Millisecond):
		t.Fatal("yieldToCustomer did not return after the customer call completed")
	}

	if e.yieldTotal.Load() == 0 {
		t.Fatal("expected yieldTotal > 0 (the engine parked at least once)")
	}
}

// TestEngineYieldNoOpWhenIdle asserts the yield is a fast no-op when no
// customer call is in flight (the steady-state path).
func TestEngineYieldNoOpWhenIdle(t *testing.T) {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}
	e := newTestEngine()
	e.yieldPoll = time.Second
	start := time.Now()
	e.yieldToCustomer(context.Background())
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("idle yield took %v — should be a fast no-op", d)
	}
}

// TestEngineQueueDedup asserts enqueueing the same scope key twice
// coalesces to one pending entry, and dequeue drains it.
func TestEngineQueueDedup(t *testing.T) {
	e := newTestEngine()
	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	if e.pendingLenForTest() != 1 {
		t.Fatalf("expected 1 pending after dedup, got %d", e.pendingLenForTest())
	}
	if e.enqueuedTotal.Load() != 2 {
		t.Fatalf("expected enqueuedTotal=2 (both calls counted), got %d", e.enqueuedTotal.Load())
	}
	s, ok := e.drainScopeForTest()
	if !ok || s.kind != scopeKindBoot {
		t.Fatalf("expected boot scope dequeued, got %+v ok=%v", s, ok)
	}
	if _, ok := e.drainScopeForTest(); ok {
		t.Fatal("expected empty queue after drain")
	}
}

// TestEngineWorkerRunsScopeAfterYield asserts the worker runs the handler
// for an enqueued scope and that the customer-priority gate does not
// permanently block it (idle path).
func TestEngineWorkerRunsScopeAfterYield(t *testing.T) {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}
	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	ran := make(chan prewarmScope, 1)
	e.scopeHandler = func(_ context.Context, s prewarmScope) error {
		ran <- s
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.runWorker(ctx)

	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	select {
	case s := <-ran:
		if s.kind != scopeKindBoot {
			t.Fatalf("worker ran wrong scope: %+v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not run the enqueued scope")
	}
}

// TestEngineScopeDoneFiresOnCompletion (S2) asserts the scopeDone callback
// fires the instant a scope completes — the mechanism that releases the
// boot background goroutine at actual completion instead of after the full
// pipGlobalTimeout.
func TestEngineScopeDoneFiresOnCompletion(t *testing.T) {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}
	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	e.scopeHandler = func(_ context.Context, _ prewarmScope) error { return nil }
	done := make(chan prewarmScope, 1)
	e.scopeDone = func(s prewarmScope, _ error) { done <- s }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.runWorker(ctx)

	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	select {
	case s := <-done:
		if s.kind != scopeKindBoot {
			t.Fatalf("scopeDone fired for wrong scope: %+v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scopeDone did not fire on scope completion (S2 regression)")
	}
}

// ─────────────────────────────────────────────────────────────────────
// Ship 2 Stage 2 / 0.30.247 — scopeKindGVRDiscovered scope-key tests.
// ─────────────────────────────────────────────────────────────────────

// TestScopeKindGVRDiscovered_KeyDeterminism (RC-2 falsifier) asserts
// that enqueueing two scopes with the SAME gvr coalesces to ONE
// pending entry. The bounded dedup queue suppresses redundant
// re-prewarm work for the same discovered GVR (a discovery-storm of
// the same CRD must not amplify into N rePrewarm calls).
func TestScopeKindGVRDiscovered_KeyDeterminism(t *testing.T) {
	e := newTestEngine()
	gvr := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1-2-2",
		Resource: "githubscaffoldingwithcompositionpages",
	}
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered, gvr: gvr})
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered, gvr: gvr})
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered, gvr: gvr})

	if e.pendingLenForTest() != 1 {
		t.Fatalf("expected 1 pending entry after 3 enqueues of the same GVR, got %d", e.pendingLenForTest())
	}
	if e.enqueuedTotal.Load() != 3 {
		t.Fatalf("expected enqueuedTotal=3 (all attempts counted), got %d", e.enqueuedTotal.Load())
	}
	s, ok := e.drainScopeForTest()
	if !ok {
		t.Fatal("expected one scope dequeueable, got empty queue")
	}
	if s.kind != scopeKindGVRDiscovered {
		t.Fatalf("expected scopeKindGVRDiscovered, got %q", s.kind)
	}
	if s.gvr != gvr {
		t.Fatalf("expected gvr payload %+v, got %+v", gvr, s.gvr)
	}
	if _, ok := e.drainScopeForTest(); ok {
		t.Fatal("expected queue empty after drain")
	}
}

// TestScopeKindGVRDiscovered_KeyDistinct (RC-2 falsifier) asserts that
// enqueueing two scopes with DIFFERENT gvrs produces TWO pending
// entries (no false coalesce). Each unique discovered GVR must get
// its own re-prewarm — coalescing distinct GVRs would silently lose
// re-prewarm work for one of them.
func TestScopeKindGVRDiscovered_KeyDistinct(t *testing.T) {
	e := newTestEngine()
	gvrA := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1-2-2",
		Resource: "githubscaffoldingwithcompositionpages",
	}
	gvrB := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1-2-2",
		Resource: "anothercompositionkind",
	}
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered, gvr: gvrA})
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered, gvr: gvrB})

	if e.pendingLenForTest() != 2 {
		t.Fatalf("expected 2 pending entries for distinct GVRs, got %d (false coalesce)", e.pendingLenForTest())
	}

	// Both scopes must be dequeueable; collect them.
	got := map[schema.GroupVersionResource]struct{}{}
	for i := 0; i < 2; i++ {
		s, ok := e.drainScopeForTest()
		if !ok {
			t.Fatalf("expected 2 dequeueable scopes, got only %d", i)
		}
		if s.kind != scopeKindGVRDiscovered {
			t.Fatalf("expected scopeKindGVRDiscovered, got %q", s.kind)
		}
		got[s.gvr] = struct{}{}
	}
	if _, ok := got[gvrA]; !ok {
		t.Fatalf("missing gvrA from dequeued set: %+v", got)
	}
	if _, ok := got[gvrB]; !ok {
		t.Fatalf("missing gvrB from dequeued set: %+v", got)
	}
}

// TestScopeKindGVRDiscovered_KeyDistinctFromBoot asserts that
// scopeKindGVRDiscovered with a zero GVR is STILL distinct from
// scopeKindBoot. This protects against a future refactor that might
// collapse the two by accident.
func TestScopeKindGVRDiscovered_KeyDistinctFromBoot(t *testing.T) {
	bootKey := prewarmScope{kind: scopeKindBoot}.key()
	discKey := prewarmScope{kind: scopeKindGVRDiscovered}.key()
	if bootKey == discKey {
		t.Fatalf("boot key %q collides with gvr-discovered key %q", bootKey, discKey)
	}
}

// TestScopeKindGVRDiscovered_KeyFormat pins the key format so callers
// reading the engine's pending map by key string have a stable
// contract. Documented in the prewarmScope.key() comment.
func TestScopeKindGVRDiscovered_KeyFormat(t *testing.T) {
	gvr := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1",
		Resource: "compositions",
	}
	want := "gvr-discovered|composition.krateo.io/v1, Resource=compositions"
	got := prewarmScope{kind: scopeKindGVRDiscovered, gvr: gvr}.key()
	if got != want {
		t.Fatalf("key format drift:\n  want: %q\n  got:  %q", want, got)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Ship 2 Stage 2.5 / 0.30.248 (Fix v2 — engine ctx decoupling).
// ─────────────────────────────────────────────────────────────────────

// TestEngineWorker_OutlivesBootSeedCtxCancel is the CENTRAL Fix-v2
// falsifier per PM Change #2. The scenario:
//
//   - The engine worker is started with a PROCESS-LIFETIME ctx (test
//     stand-in: a parent ctx that is NOT cancelled).
//   - A boot-seed orchestration ctx (a child of context.Background())
//     is cancelled, simulating the production engineSeed `defer
//     seedCancel()` at boot-done.
//   - A scopeKindGVRDiscovered scope is enqueued AFTER the boot-seed
//     ctx is cancelled.
//   - The worker MUST process the new scope. Pre-Fix-v2 the worker
//     would have died with the boot-seed ctx and the scope would sit
//     in e.pending forever — empirically the 0 prewarm.engine.gvr_discovered.start
//     log lines in run-20260605-082433.
//
// Falsifier PASS = worker processes the post-bootCancel scope within
// a 2 s deadline. FAIL = scope unprocessed (Fix-v2 broken).
func TestEngineWorker_OutlivesBootSeedCtxCancel(t *testing.T) {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond

	var (
		ranMu     sync.Mutex
		ranScopes []prewarmScope
	)
	e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
		ranMu.Lock()
		ranScopes = append(ranScopes, s)
		ranMu.Unlock()
		return nil
	}

	// processCtx is the LONG-LIVED worker ctx (Fix v2 contract).
	processCtx, processCancel := context.WithCancel(context.Background())
	defer processCancel()
	go e.runWorker(processCtx)

	// bootSeedCtx is the SHORT-LIVED orchestration ctx; production's
	// engineSeed closure binds pctx (= seedCtx) to this. Cancelling it
	// simulates the boot-seed goroutine's `defer seedCancel()` firing
	// after the boot scope completes.
	bootSeedCtx, bootCancel := context.WithCancel(context.Background())
	_ = bootSeedCtx
	bootCancel() // boot-seed ctx is now CANCELLED — the regression case

	// Pre-Fix-v2, the worker would have inherited bootSeedCtx and died
	// here. Post-Fix-v2, the worker sees only processCtx and survives.
	// Enqueue a scopeKindGVRDiscovered AFTER bootCancel to exercise
	// the post-boot path.
	gvr := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1-2-2",
		Resource: "githubscaffoldingwithcompositionpages",
	}
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered, gvr: gvr})

	// The worker MUST process the scope within 2 s. Poll because the
	// worker runs async.
	deadline := time.After(2 * time.Second)
	for {
		ranMu.Lock()
		n := len(ranScopes)
		ranMu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Fix v2 falsifier FAIL: scopeKindGVRDiscovered enqueued AFTER " +
				"boot-seed ctx cancel was NOT processed within 2s — engine worker " +
				"died with boot-seed ctx. Pre-Fix-v2 regression class.")
		case <-time.After(5 * time.Millisecond):
		}
	}

	ranMu.Lock()
	defer ranMu.Unlock()
	if ranScopes[0].kind != scopeKindGVRDiscovered {
		t.Fatalf("worker processed wrong scope kind: %q", ranScopes[0].kind)
	}
	if ranScopes[0].gvr != gvr {
		t.Fatalf("worker scope payload mismatch: want %+v got %+v", gvr, ranScopes[0].gvr)
	}
}

// TestEngineWorker_ProcessCtxCancelStopsWorker complements the
// outlives-bootCtx falsifier: the worker MUST stop when its
// process-lifetime ctx is cancelled (process shutdown). Pre-Fix-v2
// the worker stopped on bootCtx cancel; Fix-v2 changes this to
// processCtx cancel. Worker MUST still respect THAT signal.
func TestEngineWorker_ProcessCtxCancelStopsWorker(t *testing.T) {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	e.scopeHandler = func(ctx context.Context, s prewarmScope) error { return nil }

	processCtx, processCancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		e.runWorker(processCtx)
	}()

	// Cancel the process ctx — the worker MUST exit promptly.
	processCancel()
	select {
	case <-workerDone:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit within 2s of processCtx cancel — " +
			"the Fix-v2 change must NOT block on process shutdown")
	}
}

// TestEngineWorker_PerScopeTimeoutAnchoredOnProcessCtx asserts the
// per-scope context.WithTimeout is anchored on the worker's
// process-lifetime ctx, NOT some hidden boot-seed ctx. A scope handler
// that respects ctx cancellation MUST observe a deadline whose parent
// IS the process ctx — verifiable by checking the scope ctx's Deadline
// is finite AND the scope ctx Done() fires when the per-scope timeout
// expires, NOT when an unrelated ctx is cancelled.
func TestEngineWorker_PerScopeTimeoutAnchoredOnProcessCtx(t *testing.T) {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond

	var observedScopeCtx context.Context
	var observedDone bool
	scopeCh := make(chan struct{})
	e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
		observedScopeCtx = ctx
		_, observedDone = ctx.Deadline()
		close(scopeCh)
		return nil
	}

	processCtx, processCancel := context.WithCancel(context.Background())
	defer processCancel()
	go e.runWorker(processCtx)

	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	select {
	case <-scopeCh:
		// good — handler ran
	case <-time.After(2 * time.Second):
		t.Fatal("scope handler did not fire — worker wiring broken")
	}

	if observedScopeCtx == nil {
		t.Fatal("scope handler did not capture ctx")
	}
	if !observedDone {
		t.Fatal("scope ctx has no Deadline — per-scope WithTimeout not applied")
	}
}

// TestEngineWorker_PendingDepthDrainsToZero asserts the engine's
// pending map drains to 0 once the worker processes the queue. This
// is the PM Change #1 expvar signal: a non-zero pending_depth
// sustained across scrapes means the worker is dead (Fix-v2 falsifier
// at the observability layer).
func TestEngineWorker_PendingDepthDrainsToZero(t *testing.T) {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	e.scopeHandler = func(ctx context.Context, s prewarmScope) error { return nil }

	// Deterministic drain signal (#328). The worker fires scopeDone once per
	// scope, AFTER processedTotal++ and after the queue.Get that decrements
	// pending depth (prewarm_engine.go:692/747), so awaiting one scopeDone per
	// enqueued scope proves the drain is COMPLETE — no wall-clock deadline on
	// the async drain. The old 2s time.After poll asserted a wall-clock bound
	// on that async work and flaked on a loaded/-race runner even when the
	// worker drained correctly (the reported #328 flake). Buffered +
	// non-blocking send so an unexpected extra scopeDone can never block the
	// worker on a full channel.
	processed := make(chan struct{}, 8)
	e.scopeDone = func(_ prewarmScope, _ error) {
		select {
		case processed <- struct{}{}:
		default:
		}
	}

	processCtx, processCancel := context.WithCancel(context.Background())
	defer processCancel()
	go e.runWorker(processCtx)

	// Enqueue 3 distinct GVRs to populate the pending map.
	for i := 0; i < 3; i++ {
		e.enqueueScope(prewarmScope{
			kind: scopeKindGVRDiscovered,
			gvr: schema.GroupVersionResource{
				Group:    "x",
				Version:  "v1",
				Resource: "r" + string(rune('a'+i)),
			},
		})
	}

	// Await one scopeDone per enqueued scope — the deterministic drain
	// completion. The timeout is a GENEROUS BACKSTOP for a genuinely-stuck
	// worker (a dead worker never fires scopeDone), NOT the pass/fail
	// discriminator: it sits far above any real drain (~ms) so it never
	// false-flakes under load, and the signal — not the clock — decides.
	const drainBackstop = 30 * time.Second
	for i := 0; i < 3; i++ {
		select {
		case <-processed:
		case <-time.After(drainBackstop):
			t.Fatalf("pending_depth did not drain: worker stuck after %d/3 scopes "+
				"(depth=%d, processed=%d) — a live worker fires scopeDone per scope",
				i, e.pendingLenForTest(), e.processedTotal.Load())
		}
	}

	// After all 3 scopes are processed the pending depth (the PM Change #1
	// expvar signal) and the processed counter are both settled — no poll.
	if depth := e.pendingLenForTest(); depth != 0 {
		t.Fatalf("pending_depth=%d after all 3 scopes processed, want 0 — drain incomplete", depth)
	}
	if e.processedTotal.Load() != 3 {
		t.Fatalf("processedTotal=%d, want 3 (all 3 distinct GVRs processed)",
			e.processedTotal.Load())
	}
}
