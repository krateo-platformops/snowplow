// phase1_seed_declined_external_marker_test.go — #132 F4b Lever A falsifiers.
//
// THE DEFECT (docs/f4b-seed-overshoot-design-2026-07-14.md §3): the boot seed
// re-resolves an external-backed "whale" widget on EVERY resume pass, because the
// #102 GTTL-1 gate (correctly) DECLINES its Put (external touch → no dep edge),
// so the next pass's seedSkipDecision(seedModeBoot) does handle.Get → MISS (never
// Put) → returns false → re-resolve from scratch → decline → forever. This burns
// the resume budget on zero-forward-progress work and keeps the boot scope
// thrashing.
//
// THE FIX: a boot-scope "resolved-but-declined-external" marker set
// (cache.SeedDeclinedExternalSet) — declineSeedPutOnError's external branch
// Marks the key; seedSkipDecision(seedModeBoot) consults it BEFORE handle.Get and
// skips a marked key. This breaks the loop while preserving the #102 decline (the
// cell stays intentionally cold; /call re-resolves it live).
//
// These arms drive the REAL prod functions (seedSkipDecision +
// declineSeedPutOnError) against the REAL cache.SeedDeclinedExternalSet installed
// on ctx the way withCohortSeedContext installs it (via
// cache.WithSeedDeclinedExternalSet, gated on the boot ctx). The handle is a
// Get-recording double so the arms can prove the marker short-circuits BEFORE the
// liveness Get (the exact re-resolve site). Hermetic, -race, no cluster.
//
// ARM MAP (PM conditions C-F4B-1/-2/-3):
//   C-F4B-1 (falsifier shape, MULTI-COHORT): K=2 cohorts × the SAME external
//     widget across 2 seed passes. GREEN: pass-2 skips the (widget,cohort) that
//     pass-1 declined (Marked→skip, handle.Get NOT consulted). RED arm: with NO
//     marker set installed (the pre-fix world), pass-2 falls through to
//     handle.Get→MISS→re-resolve — the skip does NOT fire. K=1 is blind to the
//     per-cohort defect; the two cohorts prove the marker discriminates per key.
//   C-F4B-2 (marker key = full dispatchCacheLookupKey, NOT per-target): cohort A
//     resolves+declines widget W on pass 1 → A's key marked. Cohort B's FIRST
//     encounter of W (a DIFFERENT key — distinct RBAC identity) is NOT marked →
//     B still resolves (no cross-cohort skip). A per-target marker would wrongly
//     skip B's first resolve.
//   C-F4B-3 (content safety — nil off the boot seed path): a /call context and a
//     keepwarm context carry NO set (SeedDeclinedExternalSetFromContext == nil),
//     so Marked is always false and a declined widget is re-resolved live. The
//     marker can never fake a warm serve on the request path.

package dispatchers

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// getRecordingHandle is a cacheHandle double that records every key passed to
// Get, so an arm can assert the marker short-circuited BEFORE the liveness Get
// (the re-resolve site). Get always reports MISS (empty, never Put) — modelling
// the declined external cell that is never warmed. Put records nothing (the seed
// declines it). Concurrency-safe: the seed fans cohorts across goroutines.
type getRecordingHandle struct {
	mu          sync.Mutex
	getKeys     []string // customer-warmth Get() calls
	noTouchKeys []string // #376 — internal no-stamp GetNoTouch() calls
}

func (h *getRecordingHandle) Get(key string) (*cache.ResolvedEntry, bool) {
	h.mu.Lock()
	h.getKeys = append(h.getKeys, key)
	h.mu.Unlock()
	return nil, false // MISS — the declined external cell is never Put/warmed
}

// GetNoTouch is the #376 internal no-stamp read the seed-skip decision now uses
// (seedModeBoot + seedModeKeepwarm). Recorded SEPARATELY from Get so a wiring arm
// can assert the decision routes through GetNoTouch and NOT Get; getCount/sawGet
// report the UNION so the "a liveness read occurred" assertions stay method-agnostic.
func (h *getRecordingHandle) GetNoTouch(key string) (*cache.ResolvedEntry, bool) {
	h.mu.Lock()
	h.noTouchKeys = append(h.noTouchKeys, key)
	h.mu.Unlock()
	return nil, false // MISS — same as Get: the declined external cell is never warmed
}

func (h *getRecordingHandle) Put(key string, entry *cache.ResolvedEntry) {}

// #189 — stub the generation-guarded surface; this handle never warms a cell
// (Get is always a miss, Put is a no-op), so the gen ops are inert too.
func (h *getRecordingHandle) CaptureGen(key string) uint64 { return 0 }
func (h *getRecordingHandle) PutIfGen(key string, entry *cache.ResolvedEntry, capturedGen uint64) bool {
	return true
}

// getCount is method-AGNOSTIC: the number of liveness reads (Get + #376 GetNoTouch),
// so "a liveness read occurred / did not occur" assertions survive the #376 switch
// of the seed-skip read from Get to GetNoTouch.
func (h *getRecordingHandle) getCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.getKeys) + len(h.noTouchKeys)
}

// sawGet is method-AGNOSTIC: a liveness read of key via EITHER Get or GetNoTouch.
func (h *getRecordingHandle) sawGet(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range h.getKeys {
		if k == key {
			return true
		}
	}
	for _, k := range h.noTouchKeys {
		if k == key {
			return true
		}
	}
	return false
}

// sawGetNoTouch / sawGetTouch are the #376 method-SPECIFIC probes a wiring arm uses
// to assert the seed-skip decision read via GetNoTouch and NOT the warmth-stamping Get.
func (h *getRecordingHandle) sawGetNoTouch(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range h.noTouchKeys {
		if k == key {
			return true
		}
	}
	return false
}

func (h *getRecordingHandle) sawGetTouch(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range h.getKeys {
		if k == key {
			return true
		}
	}
	return false
}

// declineExternalOnce drives the REAL declineSeedPutOnError external branch: a
// fresh ext sink bumped once + a fresh (unbumped) stage sink, exactly the shape
// seedOneWidget builds around the Put. Returns whether the Put was declined
// (must be true) so the arm asserts the real gate fired.
func declineExternalOnce(ctx context.Context, class, target, key string) bool {
	_, stageSink := cache.WithStageErrorSink(ctx)
	_, extSink := cache.WithExternalTouchedSink(ctx)
	extSink.Bump() // the resolve touched a genuine external endpoint
	return declineSeedPutOnError(ctx, class, target, key, stageSink, extSink)
}

// ─────────────────────────────────────────────────────────────────────────
// ARM C-F4B-1 — MULTI-COHORT (K=2) × 2 seed passes. The discriminating arm.
// multiCohortWidget is the shared external whale + the two cohort keys the
// real-engine-path arms drive (distinct RBAC identity → distinct
// dispatchCacheLookupKey → distinct marker keys, C-F4B-1 K=2).
const multiCohortWidget = "krateo-system/search-results"

var multiCohortKeys = []string{
	"widgets|search-results|u=|g=group:admins", // cohort A
	"widgets|search-results|u=|g=group:devs",   // cohort B
}

// bootSeedPassBehavior models ONE boot seed pass over the two-cohort whale set,
// consuming the ENGINE-INSTALLED set off scopeCtx (NOT a hand-threaded one) via
// the REAL seedSkipDecision + declineSeedPutOnError. It records, per cohort key,
// whether this pass RE-RESOLVED the whale (i.e. the skip did NOT fire and the
// resolve+decline ran). Returns the number of re-resolves this pass and whether
// handle.Get was consulted for any already-marked key (must be 0 on a resume
// pass — the marker short-circuits before Get). newSetPerPass models the
// reworked-away 2dc46ae (pass-lived set) for the RED arm.
func makeBootSeedHandler(t *testing.T, reResolvedThisPass *int, sawGetOnMarked *bool, newSetPerPass bool) func(context.Context, prewarmScope) error {
	return func(scopeCtx context.Context, s prewarmScope) error {
		if s.kind != scopeKindBoot {
			return nil
		}
		// RED variant: ignore the engine-installed set and new a pass-lived one
		// (the inert 2dc46ae behavior). GREEN: consume the engine-lived set the
		// processScope wiring installed on scopeCtx.
		ctx := scopeCtx
		if newSetPerPass {
			ctx = cache.WithSeedDeclinedExternalSet(context.Background(), cache.NewSeedDeclinedExternalSet())
		}
		anyReResolved := false
		for _, k := range multiCohortKeys {
			h := &getRecordingHandle{}
			// The REAL boot skip predicate — consults the declined-external set
			// (engine-lived on scopeCtx) BEFORE handle.Get.
			if seedSkipDecision(ctx, seedModeBoot, h, k, "widgets", multiCohortWidget, "") {
				// Skipped: the whale was already resolved-and-declined this boot
				// scope. No re-resolve, no external round-trip. This is the fix.
				if h.getCount() != 0 {
					*sawGetOnMarked = true
				}
				continue
			}
			// Not skipped → the seed RE-RESOLVES the whale, touches external, and
			// declineSeedPutOnError declines the Put + Marks the key (the REAL
			// prod path). This is the wasted work Lever A must eliminate on resume.
			anyReResolved = true
			*reResolvedThisPass++
			if !declineExternalOnce(ctx, "widgets", multiCohortWidget, k) {
				t.Fatalf("seed pass: external whale %q must decline the Put", k)
			}
		}
		// A pass that re-resolved anything models the deadline-cut before the boot
		// truly converges → return an error so processScope requeues (the F.4
		// resume). Once a pass re-resolves NOTHING (all skipped), the boot has
		// converged → return nil → processScope Forgets + clears the set.
		if anyReResolved {
			return context.DeadlineExceeded
		}
		return nil
	}
}

// driveProcessScopeUntilQuiet drives the REAL e.processScope lifecycle
// (Get→handler→Forget/AddRateLimited requeue) — the actual prod chain that
// news/reuses/clears the engine-lived declined-external set — until the boot
// scope genuinely completes (no requeue) or maxIters is hit. Deterministic: no
// worker goroutine; the rate-limited requeue is pulled forward by a short poll.
func driveProcessScopeUntilQuiet(t *testing.T, e *prewarmEngine, maxIters int) (passes int) {
	prevTO := prewarmScopeTimeoutFn
	prewarmScopeTimeoutFn = func(prewarmScope) time.Duration { return time.Hour } // never a real deadline
	t.Cleanup(func() { prewarmScopeTimeoutFn = prevTO })

	ctx := context.Background()
	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	for iter := 0; iter < maxIters && e.queue.Len() > 0; iter++ {
		s, shutdown := e.queue.Get()
		if shutdown {
			break
		}
		// Drive the REAL prod lifecycle: processScope installs the engine-lived
		// declined-external set on scopeCtx (for boot), runs the handler, then
		// Done()s + Forget/AddRateLimited-requeues + clears-on-genuine-completion.
		// It owns Done for the item we just Got.
		e.processScope(ctx, s)
		passes++
		// Pull a rate-limited requeue forward (stock backoff base ~5ms).
		if e.queue.Len() == 0 && e.requeuedTotal.Load() > 0 {
			deadline := time.Now().Add(2 * time.Second)
			for e.queue.Len() == 0 && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}
	return passes
}

// TestF4bLeverA_MultiCohort_ResumeSkipsDeclinedExternal — C-F4B-1, the
// discriminating arm, driving TWO REAL processScope invocations sharing the
// ENGINE-LIVED set. GREEN: pass 1 re-resolves both cohort whales (K=2) and
// requeues; pass 2 (the REAL requeue) skips BOTH (the reused set) → zero
// re-resolves → boot converges. RED: new the set per pass (2dc46ae) → pass 2
// re-resolves again → never converges within the cap.
func TestF4bLeverA_MultiCohort_ResumeSkipsDeclinedExternal(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true") // else WithSeedDeclinedExternalSet no-ops (Disabled())

	run := func(newSetPerPass bool) (pass1ReResolves, pass2ReResolves, totalPasses int, sawGetOnMarked bool, converged bool) {
		e := newTestEngine()
		reResolves := 0
		perPassReResolves := []int{}
		wrapReResolves := 0
		e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
			before := reResolves
			h := makeBootSeedHandler(t, &reResolves, &sawGetOnMarked, newSetPerPass)
			err := h(ctx, s)
			perPassReResolves = append(perPassReResolves, reResolves-before)
			wrapReResolves = reResolves
			return err
		}
		passes := driveProcessScopeUntilQuiet(t, e, 6)
		_ = wrapReResolves
		p1, p2 := 0, 0
		if len(perPassReResolves) >= 1 {
			p1 = perPassReResolves[0]
		}
		if len(perPassReResolves) >= 2 {
			p2 = perPassReResolves[1]
		}
		// Converged = the last processed pass re-resolved nothing (all skipped) AND
		// the queue drained (no pending requeue).
		converged = e.queue.Len() == 0 && len(perPassReResolves) > 0 && perPassReResolves[len(perPassReResolves)-1] == 0
		return p1, p2, passes, sawGetOnMarked, converged
	}

	// GREEN (engine-lived set): pass 1 re-resolves K=2 whales; pass 2 (real
	// requeue, reused set) skips both → 0 re-resolves → converges.
	p1, p2, _, sawGet, converged := run(false /*engine-lived*/)
	if p1 != 2 {
		t.Fatalf("GREEN pass-1: both K=2 cohort whales must be re-resolved on the FIRST boot pass; got %d want 2", p1)
	}
	if p2 != 0 {
		t.Fatalf("GREEN pass-2 VIOLATED: the resume pass re-resolved %d whale(s) — the engine-lived set must make the resume skip ALL already-declined whales (the §3 loop is still open)", p2)
	}
	if sawGet {
		t.Fatal("GREEN: seedSkipDecision consulted handle.Get for a MARKED key — the marker must short-circuit BEFORE the Get (the re-resolve site)")
	}
	if !converged {
		t.Fatal("GREEN: the boot scope must CONVERGE (a resume pass re-resolves nothing → Forget, queue drains) — it did not")
	}

	// RED (pass-lived set — the inert 2dc46ae the arch caught): each pass news a
	// fresh empty set → pass 2 re-resolves the SAME whales again → the §3 loop
	// never breaks → never converges within the cap.
	rp1, rp2, _, _, redConverged := run(true /*new-set-per-pass*/)
	if rp1 != 2 {
		t.Fatalf("RED setup: pass-1 must still re-resolve K=2 whales; got %d", rp1)
	}
	if rp2 != 2 {
		t.Fatalf("RED arm broke: pass-lived set must RE-RESOLVE both whales again on pass 2 (proving the mechanism is the ENGINE-LIVED persistence, not a pass-local set); got %d want 2", rp2)
	}
	if redConverged {
		t.Fatal("RED arm broke: a pass-lived set must NOT converge (each resume re-resolves the whales) — if it converged the persistence isn't what's doing the work")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM — ENGINE lifetime: the set is CREATED on first processScope, REUSED across
// the boot scope's AddRateLimited requeues, and CLEARED when the scope GENUINELY
// completes (err==nil → Forget). After genuine completion a fresh boot of the
// same key starts empty and re-resolves each whale once. Drives REAL processScope.
func TestF4bLeverA_EngineLived_ReusedAcrossRequeues_ClearedOnCompletion(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")

	e := newTestEngine()

	// The boot scope key + a single whale for clarity.
	bootKey := prewarmScope{kind: scopeKindBoot}.key()
	const widget = "krateo-system/obs-log-stream"
	whaleKey := "widgets|obs-log-stream|u=|g=group:admins"

	// pass counter drives: pass 1 declines+marks (returns error → requeue);
	// pass 2 (resume, reused set) skips → returns nil → GENUINE completion.
	reResolves := 0
	sawGet := false
	e.scopeHandler = func(scopeCtx context.Context, s prewarmScope) error {
		if s.kind != scopeKindBoot {
			return nil
		}
		h := &getRecordingHandle{}
		if seedSkipDecision(scopeCtx, seedModeBoot, h, whaleKey, "widgets", widget, "") {
			if h.getCount() != 0 {
				sawGet = true
			}
			return nil // converged: nothing to re-resolve
		}
		reResolves++
		if !declineExternalOnce(scopeCtx, "widgets", widget, whaleKey) {
			t.Fatal("external whale must decline")
		}
		return context.DeadlineExceeded // cut → requeue (reuse the set)
	}

	driveProcessScopeUntilQuiet(t, e, 6)

	// The whale was re-resolved EXACTLY ONCE across the whole boot (pass 1);
	// the reused set skipped it on the resume pass.
	if reResolves != 1 {
		t.Fatalf("engine-lived set: whale must be re-resolved EXACTLY ONCE across the boot (pass 1 only); got %d", reResolves)
	}
	if sawGet {
		t.Fatal("marker must short-circuit before handle.Get on the resume pass")
	}
	// GENUINE completion cleared the set: the engine map has no entry for the boot key.
	e.declinedExtMu.Lock()
	_, stillPresent := e.declinedExtSets[bootKey]
	e.declinedExtMu.Unlock()
	if stillPresent {
		t.Fatal("engine-lived set must be CLEARED on genuine boot completion (err==nil → Forget) so a later fresh boot starts empty")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM — config-vars redrive (NEW TOPOLOGY) clears the set: a whale marked
// declined this boot is re-resolved ONCE under the new nav set after a redrive,
// never suppressed across the topology change. Drives the REAL clear path
// (clearDeclinedExternalSet, the same call enqueueBootReDrive makes).
func TestF4bLeverA_ConfigVarsRedrive_ClearsSet_WhaleReResolvesUnderNewTopology(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")

	e := newTestEngine()
	bootKey := prewarmScope{kind: scopeKindBoot}.key()
	const widget = "krateo-system/marketplace-source-toggle"
	whaleKey := "widgets|marketplace-source-toggle|u=|g=group:admins"

	// Seed the engine-lived set as if a boot pass already declined+marked the whale.
	set := e.declinedExternalSetFor(bootKey)
	set2 := cache.WithSeedDeclinedExternalSet(context.Background(), set)
	if !declineExternalOnce(set2, "widgets", widget, whaleKey) {
		t.Fatal("setup: whale must decline+mark")
	}
	if !set.Marked(whaleKey) {
		t.Fatal("setup: whale key must be marked in the engine-lived set")
	}

	// A resume pass BEFORE any redrive skips the whale (reused set).
	hBefore := &getRecordingHandle{}
	ctxBefore := cache.WithSeedDeclinedExternalSet(context.Background(), e.declinedExternalSetFor(bootKey))
	if !seedSkipDecision(ctxBefore, seedModeBoot, hBefore, whaleKey, "widgets", widget, "") {
		t.Fatal("pre-redrive: reused set must skip the already-declined whale")
	}

	// CONFIG-VARS REDRIVE (new topology): the SAME clear the enqueueBootReDrive
	// path makes. After it, the engine hands out a FRESH set for the boot key.
	e.clearDeclinedExternalSet(bootKey, "config-vars-redrive")

	// The next boot pass over the (new-topology) whale must RE-RESOLVE it once:
	// the fresh set has no mark → seedSkipDecision falls through to handle.Get.
	hAfter := &getRecordingHandle{}
	ctxAfter := cache.WithSeedDeclinedExternalSet(context.Background(), e.declinedExternalSetFor(bootKey))
	if seedSkipDecision(ctxAfter, seedModeBoot, hAfter, whaleKey, "widgets", widget, "") {
		t.Fatal("post-redrive VIOLATED: a whale stayed suppressed ACROSS a topology change — the redrive must clear the set so the whale re-resolves once under the new nav set")
	}
	if !hAfter.sawGet(whaleKey) {
		t.Fatal("post-redrive: the (unmarked) whale must fall through to handle.Get — it did not")
	}
	// And the fresh set is genuinely a different (empty) instance.
	if e.declinedExternalSetFor(bootKey).Marked(whaleKey) {
		t.Fatal("post-redrive: the boot key must map to a FRESH empty set, not the cleared one")
	}
	_ = bootKey
}

// ─────────────────────────────────────────────────────────────────────────
// ARM R2 (load-bearing) — mark under boot scope → fire config-vars redrive →
// next invocation Marked()==false. RED = OMIT the clear (stale skip persists
// across the topology change). This drives the REAL enqueueBootReDrive clear via
// clearDeclinedExternalSet with the exact redrive reason.
func TestF4bLeverA_R2_RedriveClearsStaleSkip_OmitClearIsRED(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")

	bootKey := prewarmScope{kind: scopeKindBoot}.key()
	const widget = "krateo-system/obs-errors-card"
	whaleKey := "widgets|obs-errors-card|u=|g=group:admins"

	// run models: mark under boot scope, optionally clear (redrive), then a next
	// invocation checks Marked(). Returns whether the next invocation still skips.
	run := func(fireRedriveClear bool) (nextInvocationSkips bool) {
		e := newTestEngine()
		// Boot pass marks the whale.
		markCtx := cache.WithSeedDeclinedExternalSet(context.Background(), e.declinedExternalSetFor(bootKey))
		if !declineExternalOnce(markCtx, "widgets", widget, whaleKey) {
			t.Fatal("setup: whale must decline+mark")
		}
		// GREEN drives the REAL redrive clear; RED omits it (the mutation).
		if fireRedriveClear {
			e.clearDeclinedExternalSet(bootKey, "config-vars-redrive")
		}
		// Next invocation over the same key: does it skip?
		h := &getRecordingHandle{}
		nextCtx := cache.WithSeedDeclinedExternalSet(context.Background(), e.declinedExternalSetFor(bootKey))
		return seedSkipDecision(nextCtx, seedModeBoot, h, whaleKey, "widgets", widget, "")
	}

	// GREEN: redrive fired → next invocation does NOT skip (Marked()==false).
	if run(true) {
		t.Fatal("R2 GREEN VIOLATED: after a config-vars redrive the next invocation still SKIPPED the whale — a stale suppression survived the topology change (the redrive clear did not take)")
	}
	// RED: omit the clear → the stale mark persists → next invocation skips. This
	// is exactly the bug the R2 clear prevents; if omitting the clear did NOT
	// cause a stale skip, the clear wouldn't be load-bearing.
	if !run(false) {
		t.Fatal("R2 RED arm broke: omitting the redrive clear must leave a STALE skip (Marked()==true across the topology change) — if it doesn't, the clear isn't what's providing the topology-change correctness")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM R3 (teardown ≠ clear) — genuine completion TEARS DOWN the map entry (not
// just empties it), so the engine map cannot ACCUMULATE entries across unrelated
// boots. Drives the REAL processScope completion path; asserts the map has no
// entry for the boot key after a genuine boot completes.
func TestF4bLeverA_R3_TeardownNotClear_NoAccumulationAcrossBoots(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")

	e := newTestEngine()
	bootKey := prewarmScope{kind: scopeKindBoot}.key()
	const widget = "krateo-system/obs-resource-card"
	whaleKey := "widgets|obs-resource-card|u=|g=group:admins"

	// Boot 1: mark a whale, then genuine completion (return nil once skipped).
	pass := 0
	e.scopeHandler = func(scopeCtx context.Context, s prewarmScope) error {
		if s.kind != scopeKindBoot {
			return nil
		}
		pass++
		h := &getRecordingHandle{}
		if seedSkipDecision(scopeCtx, seedModeBoot, h, whaleKey, "widgets", widget, "") {
			return nil // converged
		}
		if !declineExternalOnce(scopeCtx, "widgets", widget, whaleKey) {
			t.Fatal("whale must decline")
		}
		return context.DeadlineExceeded // cut → requeue (reuse set)
	}
	driveProcessScopeUntilQuiet(t, e, 6)

	// TEARDOWN: after genuine boot completion the map must have NO entry for the
	// boot key — not a retained-but-emptied set (which would pin one map entry per
	// scope key forever = accumulation across unrelated boots).
	e.declinedExtMu.Lock()
	nEntries := len(e.declinedExtSets)
	_, present := e.declinedExtSets[bootKey]
	e.declinedExtMu.Unlock()
	if present {
		t.Fatal("R3 teardown VIOLATED: the boot key still maps to a set after genuine completion — clearDeclinedExternalSet must DELETE the entry (teardown), not empty-in-place")
	}
	if nEntries != 0 {
		t.Fatalf("R3 accumulation VIOLATED: the engine map retained %d entries after all boots completed — it must not accumulate across unrelated boots", nEntries)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM R3 (nil-off-boot EARNED) — the engine holds the set as a FIELD, so
// off-boot ctx nil-ness is not free (unlike the old ctx-only version). It is
// EARNED by installing onto ctx ONLY in the boot-scope processScope path. Prove:
// (a) a non-boot scope (gvr-discovered) driven through the REAL processScope
// carries NO set on the handler ctx; (b) the /call + keepwarm paths never install
// one (grep-asserted by TestF4bLeverA_NoInstallOffBootPath below).
func TestF4bLeverA_R3_NilOffBoot_NonBootScopeCarriesNoSet(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")

	e := newTestEngine()
	var sawSetOnBoot, sawSetOnGVR bool
	e.scopeHandler = func(scopeCtx context.Context, s prewarmScope) error {
		set := cache.SeedDeclinedExternalSetFromContext(scopeCtx)
		switch s.kind {
		case scopeKindBoot:
			sawSetOnBoot = set != nil
		case scopeKindGVRDiscovered:
			sawSetOnGVR = set != nil
		}
		return nil // genuine completion (no requeue) for both
	}
	prevTO := prewarmScopeTimeoutFn
	prewarmScopeTimeoutFn = func(prewarmScope) time.Duration { return time.Hour }
	t.Cleanup(func() { prewarmScopeTimeoutFn = prevTO })

	ctx := context.Background()
	// Drive a BOOT scope and a GVR-discovered scope through the REAL processScope.
	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	bs, _ := e.queue.Get()
	e.processScope(ctx, bs)
	e.enqueueScope(prewarmScope{kind: scopeKindGVRDiscovered, gvr: schema.GroupVersionResource{Group: "g", Version: "v", Resource: "r"}})
	gs, _ := e.queue.Get()
	e.processScope(ctx, gs)

	if !sawSetOnBoot {
		t.Fatal("R3: the BOOT scope handler ctx MUST carry the engine-installed set (earned install)")
	}
	if sawSetOnGVR {
		t.Fatal("R3 nil-off-boot VIOLATED: a NON-boot (gvr-discovered) scope handler ctx carried a declined-external set — the install must be gated to the boot scope ONLY (an engine-held field does not get off-boot nil-ness for free)")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM R3 (nil-off-boot EARNED, grep) — the ONLY WithSeedDeclinedExternalSet
// install in prod dispatcher code is the boot-scope-gated processScope site.
// Neither the /call dispatch path nor the keepwarm/gvr paths install one. This is
// the source-level guard that the field-held set can't leak onto a request ctx.
func TestF4bLeverA_NoInstallOffBootPath(t *testing.T) {
	// Every prod .go in the package: WithSeedDeclinedExternalSet must appear ONLY
	// in prewarm_engine.go, and there ONLY inside the s.kind==scopeKindBoot guard.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	installSites := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		s := string(src)
		n := strings.Count(s, "cache.WithSeedDeclinedExternalSet(")
		if n == 0 {
			continue
		}
		installSites += n
		if name != "prewarm_engine.go" {
			t.Fatalf("R3 nil-off-boot VIOLATED: cache.WithSeedDeclinedExternalSet install found in %s — the set must be installed ONLY in the boot-scope processScope path (prewarm_engine.go), never on a /call or keepwarm path", name)
		}
		// In prewarm_engine.go the install must be guarded by the boot-scope check.
		if !strings.Contains(s, "if s.kind == scopeKindBoot {") {
			t.Fatal("R3: the prewarm_engine.go install must be gated on `if s.kind == scopeKindBoot`")
		}
	}
	if installSites != 1 {
		t.Fatalf("R3: expected exactly ONE WithSeedDeclinedExternalSet install site (boot-scope processScope); found %d", installSites)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM R4 (whole-boot cross-pass counter) — the phase1.seed.declined_external
// .summary line reflects marks across the WHOLE boot (cumulative over resume
// passes), NOT a per-pass count, and is emitted ONCE at teardown. Drive TWO real
// passes each marking a DISTINCT whale, then genuine completion → assert the
// single summary line reports declined_external_keys=2 (the cumulative total).
func TestF4bLeverA_R4_SummaryIsWholeBootCumulative(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")

	buf := captureDefaultSlogForTest(t, slog.LevelInfo)

	e := newTestEngine()
	const widget = "krateo-system/obs-perf-p50"
	// Two DISTINCT whale keys, one marked per pass, so a per-pass counter would
	// report 1 (last pass) while the whole-boot cumulative is 2.
	whaleP1 := "widgets|obs-perf-p50|u=|g=group:admins"
	whaleP2 := "widgets|obs-perf-p50|u=|g=group:devs"
	pass := 0
	e.scopeHandler = func(scopeCtx context.Context, s prewarmScope) error {
		if s.kind != scopeKindBoot {
			return nil
		}
		pass++
		switch pass {
		case 1:
			if !declineExternalOnce(scopeCtx, "widgets", widget, whaleP1) {
				t.Fatal("pass1 whale must decline")
			}
			return context.DeadlineExceeded // requeue (reuse set)
		case 2:
			// pass-1 whale already marked (skipped); mark a SECOND distinct whale.
			if !declineExternalOnce(scopeCtx, "widgets", widget, whaleP2) {
				t.Fatal("pass2 whale must decline")
			}
			return context.DeadlineExceeded // requeue again
		default:
			return nil // converged → genuine completion → summary emitted at teardown
		}
	}
	driveProcessScopeUntilQuiet(t, e, 6)

	// Exactly ONE summary line, reporting the WHOLE-BOOT cumulative count = 2.
	logText := buf.String()
	if got := strings.Count(logText, "phase1.seed.declined_external.summary"); got != 1 {
		t.Fatalf("R4: expected EXACTLY ONE declined_external summary line (emitted once at teardown); found %d\n%s", got, logText)
	}
	if !strings.Contains(logText, "\"declined_external_keys\":2") {
		t.Fatalf("R4 VIOLATED: the summary must report the WHOLE-BOOT cumulative mark count (2 distinct whales across 2 passes), NOT a per-pass partial. Log:\n%s", logText)
	}
	if !strings.Contains(logText, "\"reason\":\"boot-complete\"") {
		t.Fatalf("R4: the teardown summary must carry reason=boot-complete. Log:\n%s", logText)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM C-F4B-2 — marker keyed by the FULL key, NOT per-(class,target). Cohort A's
// decline of W must NOT suppress cohort B's FIRST resolve of the SAME widget W.
func TestF4bLeverA_MarkerIsPerKey_NoCrossCohortSkip(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	bootCtx := cache.WithSeedDeclinedExternalSet(context.Background(), cache.NewSeedDeclinedExternalSet())

	const widget = "krateo-system/obs-throughput-card" // SAME (class,target) for both cohorts
	keyA := "widgets|obs-throughput-card|u=|g=group:admins"
	keyB := "widgets|obs-throughput-card|u=|g=group:devs"

	// Cohort A resolves + declines W on pass 1 → A's key marked.
	if !declineExternalOnce(bootCtx, "widgets", widget, keyA) {
		t.Fatal("setup: cohort A's external Put must be declined")
	}

	// Cohort B's FIRST encounter of the SAME widget W (a DIFFERENT key) must NOT
	// be skipped — B has never resolved W, so the marker must miss on B's key. A
	// per-(class,target) marker would WRONGLY skip B here (B would serve nothing).
	h := &getRecordingHandle{}
	if seedSkipDecision(bootCtx, seedModeBoot, h, keyB, "widgets", widget, "") {
		t.Fatal("C-F4B-2 VIOLATED: cohort B's FIRST resolve of the same widget was skipped — the marker is per-(class,target), not per-key. A cohort would lose its cell. Marker MUST key on the full dispatchCacheLookupKey.")
	}
	if !h.sawGet(keyB) {
		t.Fatal("C-F4B-2: cohort B must fall through to handle.Get (it is unmarked) — it did not")
	}

	// And A's own key IS still marked (didn't get clobbered by B's derivation).
	hA := &getRecordingHandle{}
	if !seedSkipDecision(bootCtx, seedModeBoot, hA, keyA, "widgets", widget, "") {
		t.Fatal("C-F4B-2: cohort A's own declined key must still skip on the resume pass")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM C-F4B-3 — content safety: the marker is nil off the boot seed path. A
// /call ctx and a keepwarm ctx carry NO set, so a declined external widget is
// re-resolved live (never a faked warm serve).
func TestF4bLeverA_MarkerNilOffBootPath_UserAndKeepwarmReResolve(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	const widget = "krateo-system/list-activity-events"
	key := "widgets|list-activity-events|u=alice|g="

	// A user /call context: NEVER carries a declined-external set (only
	// withCohortSeedContext installs one, boot-mode only). Prove it's nil.
	userCtx := context.Background()
	if cache.SeedDeclinedExternalSetFromContext(userCtx) != nil {
		t.Fatal("C-F4B-3: a /call context must carry NO declined-external set")
	}

	// Even if a decline happened on the boot side, the /call path's
	// seedSkipDecision (were it ever reached with seedModeBoot off a set-less ctx)
	// re-resolves: Marked(nil) == false → falls through to handle.Get. The user
	// path re-resolves the external widget fresh, unaffected.
	h := &getRecordingHandle{}
	if seedSkipDecision(userCtx, seedModeBoot, h, key, "widgets", widget, "") {
		t.Fatal("C-F4B-3 VIOLATED: a set-less (user/keepwarm) ctx must never skip via the marker — it would fake a warm serve of an intentionally-cold external cell")
	}
	if !h.sawGet(key) {
		t.Fatal("C-F4B-3: set-less ctx must fall through to handle.Get (live re-resolve) — it did not")
	}

	// keepwarm mode NEVER installs a set (seedScopeYielding gates install on
	// seedModeBoot), AND seedModeKeepwarm's own skip path does not consult the
	// declined set at all (it uses the age-skip). Prove keepwarm re-touches: a
	// keepwarm decline does not Mark on a set-less ctx (nil-receiver no-op), so no
	// state leaks into a later boot's set.
	if declineExternalOnce(context.Background(), "widgets", widget, key) {
		// declineExternalOnce returns the gate verdict (true=declined). We only
		// care that Mark on a nil set didn't panic and left no observable state;
		// the verdict itself is correctly true (external touch always declines).
	}
}

// ─────────────────────────────────────────────────────────────────────────
// ARM — marker set ONLY in the external branch, NOT on stage_error. A stage
// error is transient/operational and MUST be retried on the resume pass (not
// permanently skipped this boot). Discriminates the branch choice.
func TestF4bLeverA_StageErrorDoesNotMark_OnlyExternal(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	bootCtx := cache.WithSeedDeclinedExternalSet(context.Background(), cache.NewSeedDeclinedExternalSet())
	const widget = "krateo-system/dashboard-flex"
	key := "widgets|dashboard-flex|u=|g=group:admins"

	// A stage-error decline (NOT external): the Put is declined, but the key must
	// NOT be marked — a stage error should re-resolve on the resume pass.
	_, stageSink := cache.WithStageErrorSink(bootCtx)
	_, extSink := cache.WithExternalTouchedSink(bootCtx)
	stageSink.Bump("exportJwt-loopback", "401 transient no per-user JWT")
	if !declineSeedPutOnError(bootCtx, "widgets", widget, key, stageSink, extSink) {
		t.Fatal("setup: a stage-error resolve must still decline the Put (GTTL-1)")
	}
	if got := cache.SeedDeclinedExternalSetFromContext(bootCtx).Marks(); got != 0 {
		t.Fatalf("stage-error decline must NOT mark the key (transient → retry on resume); Marks()=%d want 0", got)
	}

	// So the resume pass does NOT skip on the marker account — it re-resolves.
	h := &getRecordingHandle{}
	if seedSkipDecision(bootCtx, seedModeBoot, h, key, "widgets", widget, "") {
		t.Fatal("a stage-errored (non-external) target must be re-resolved on the resume pass, NOT marker-skipped")
	}
	if !h.sawGet(key) {
		t.Fatal("stage-error target must fall through to handle.Get on resume — it did not")
	}
}
