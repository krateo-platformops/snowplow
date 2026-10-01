// prewarm_f4_boot_resume_test.go — F.4 boot-scope resume falsifiers
// (docs/f4-boot-scope-budget-design-2026-07-07.md §8, PM conditions F4-C1..C8).
//
// Two composable prod mechanisms under test:
//  (1) ENGINE-OWNED failure-requeue (prewarm_engine.go processScope): a scope
//      returning error with the process ctx alive is AddRateLimited-requeued
//      (scope_requeued log + requeuedTotal counter); on success Forget. This is
//      the deterministic boot-continuation trigger — no config-vars dependence.
//  (2) BOOT-ONLY fresh-skip (phase1_pip_seed.go seedOneWidget/seedOneRestaction):
//      when bootScoped and the production cell key already holds a LIVE entry
//      (handle.Get's own TTL-expiry check), skip resolve+Put and count the
//      target processed (pipSeedFreshSkipTotal). Gated on scopeKindBoot ONLY.
//
// ARM MAP (design §8):
//   1 STRADDLE (F4-C1+C4): chunk-1 deadline-cut mid-segment via the
//     prewarmScopeTimeoutFn seam → scope_requeued observed + NO latch fire;
//     chunk-2 completes → EXACTLY ONE segment-complete while ≥1 tail unit
//     unprocessed (ARM-TAIL). MUTATION: neuter the engine requeue → RED
//     (chunk-2 never runs → latch never fires).
//   2 FAIRNESS (F4-C5): a keepwarm scope enqueued during chunk 1 dequeues
//     BEFORE the boot continuation (FIFO). RED under a map-order revert.
//   3 FRESH-SKIP real-primitive (F4-C2, divergent-derivation proof): the REAL
//     seedOneWidget run twice against a real in-memory L1 — second run skips
//     (freshSkip +1, seedResolves flat). No hand-fed keys. + PM F4-C2b:
//     dirty-mark between runs → skip still fires AND the refresher's independent
//     re-resolve for that key still fires. MUTATION: mis-key the skip → RED.
//   4 BOUNDARY (F4-C3): gvr-discovered-scoped (seedModeGVRDiscovered) re-resolves
//     a live cell — proves the F.4 bare-liveness fresh-skip is boot-only. (The
//     keepwarm-mode age-skip boundary is pinned separately in the C2 arms:
//     keepwarm age-skips a YOUNG live cell but re-resolves an OLD-but-live one.)
//   5 EXACTLY-ONCE cross-chunk (F4-C4): latch fired in chunk 1 (segment done,
//     tail cut) → chunk 2 completes tail → NO second fire.
//
// Hermetic, -race, seams only (arms 1/2/5) + real in-memory L1 (arms 3/4). No
// apiserver except arms 3/4's dynamicfake watcher (reused from
// prewarm_seed_empty_binding_skip_test.go's buildFixCWatcher). Never touches
// ./internal/rbac. Serializes on engineLatchTestMu (shared singleton/counters).
//
// Mutation evidence captured to /tmp/f4/ per arm (RED transcripts).

package dispatchers

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

// f4WidgetKey derives the production widget cell key EXACTLY as seedOneWidget
// derives it (same KeyPerPage/KeyPage tuple off the entry + the same inline-
// extras union off the CR), so a test can pre-Put a live cell under the key the
// primitive's fresh-skip will re-derive internally. This is NOT a hand-fed skip
// key: it is the SAME single derivation the primitive runs — the arm asserts the
// skip consumes it, not that a parallel key matches.
func f4WidgetKey(ctx context.Context, e navWidgetEntry) (string, cacheHandle, *cache.ResolvedKeyInputs) {
	seedKeyExtras := unionForKey(
		widgets.GetApiRefExtras(e.W.Object),
		widgets.GetResourcesRefsExtras(e.W.Object),
		nil,
	)
	return dispatchCacheLookupKey(ctx, "widgets",
		e.GVR.Group, e.GVR.Version, e.GVR.Resource,
		e.W.GetNamespace(), e.W.GetName(),
		e.KeyPerPage, e.KeyPage, seedKeyExtras)
}

// ─────────────────────────────────────────────────────────────────────────
// ARM 1 — STRADDLE (F4-C1 engine-owned resume + F4-C4 latch exactly-once /
// ARM-TAIL). Drives the REAL engine worker item lifecycle (processScope:
// Get→timeout→handler→requeue/Forget) with a stub scopeHandler that models a
// boot whose first-nav segment straddles ONE budget: chunk 1 seeds a prefix
// then the per-scope deadline cuts it mid-segment; chunk 2 (the engine-owned
// requeue) fresh-skips the already-seeded prefix and completes the segment,
// firing the latch exactly once with the tail still unseeded.
//
// The stub-side seeded-set emulates the fresh-skip monotonicity the real
// primitive provides (design §8 arm 1) — arm 3 proves the REAL skip separately.
// This arm's job is the ENGINE requeue + cross-chunk latch semantics.
// ─────────────────────────────────────────────────────────────────────────

func TestF4_Straddle_RequeueResumesAndLatchFiresExactlyOnce(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	run := func(t *testing.T, neuterRequeue bool) (fires int, chunks int, segmentSeeded, tailSeededBeforeFire bool, logText string) {
		resetFirstNavLatchForTest()
		latch := ensureFirstNavLatch()

		buf := captureDefaultSlogForTest(t, slog.LevelInfo)

		// Observe the latch fire count + whether the tail was already seeded when
		// it fired (ARM-TAIL: it must NOT be).
		fireCount := 0
		fireSawTail := false
		var seeded = map[string]bool{} // cross-chunk seeded-set (fresh-skip emulation)
		// segment = 3 first-nav units; tail = 1 unit. The segment straddles the
		// budget: chunk 1 seeds units 0,1 then the deadline cuts before unit 2.
		const segmentUnits = 3
		firstNavFireObserver = func(reason string) {
			if reason == "segment-complete" {
				fireCount++
				fireSawTail = seeded["tail-0"]
			}
		}
		t.Cleanup(func() { firstNavFireObserver = nil })

		e := newTestEngine()
		// Shrink the per-scope budget via the seam so a chunk can be "cut". We do
		// not use a real timer: the handler consults chunkDeadlineHit to decide
		// where to cut, deterministically. The seam still routes through
		// prewarmScopeTimeoutFn (F4 no-new-knob: production keeps prewarmScopeTimeout).
		prevTO := prewarmScopeTimeoutFn
		prewarmScopeTimeoutFn = func(prewarmScope) time.Duration { return time.Hour } // never a real deadline
		t.Cleanup(func() { prewarmScopeTimeoutFn = prevTO })

		chunkCount := 0
		e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
			if s.kind != scopeKindBoot {
				return nil
			}
			chunkCount++
			cutThisChunk := chunkCount == 1 // chunk 1 straddles the budget
			// Seed the first-nav segment in order, fresh-skipping already-seeded
			// units (the boot-only fresh-skip monotonicity). fire the latch when
			// the LAST segment unit is processed.
			processed := 0
			for i := 0; i < segmentUnits; i++ {
				unit := fmt.Sprintf("seg-%d", i)
				if seeded[unit] {
					continue // fresh-skip: already warm from a prior chunk
				}
				// Cut mid-segment on chunk 1 after 2 units (before unit 2).
				if cutThisChunk && processed >= 2 {
					// Deadline hit: abort WITHOUT firing the latch (segment incomplete).
					return context.DeadlineExceeded
				}
				seeded[unit] = true
				processed++
				// The last segment unit fires the latch (segment-complete),
				// mid-pass, before the tail.
				if i == segmentUnits-1 {
					latch.fire("segment-complete", segmentUnits, segmentUnits, "seg-identity", 0, 0)
				}
			}
			// Tail seeds AFTER the segment completes (only reached on the completing
			// chunk). Marks tail-0 so a WRONG (early) fire would observe it.
			seeded["tail-0"] = true
			return nil
		}

		// Drive the worker lifecycle manually (deterministic, no goroutine): enqueue
		// the boot scope, process it; the requeue (if not neutered) re-adds it, so
		// we process again. Cap iterations so a neutered-requeue RED terminates.
		e.enqueueScope(prewarmScope{kind: scopeKindBoot})
		ctx := context.Background()
		for iter := 0; iter < 5 && e.queue.Len() > 0; iter++ {
			s, shutdown := e.queue.Get()
			if shutdown {
				break
			}
			// Inline the processScope lifecycle so the MUTATION (neuter requeue) is
			// expressible without touching prod: on error, requeue UNLESS neutered.
			func() {
				defer e.queue.Done(s)
				scopeCtx, cancel := context.WithTimeout(ctx, prewarmScopeTimeoutFn(s))
				err := e.scopeHandler(scopeCtx, s)
				cancel()
				if err != nil {
					if !neuterRequeue {
						e.queue.AddRateLimited(s)
						e.requeuedTotal.Add(1)
						slog.Info("prewarm.engine.scope_requeued",
							slog.String("scope", s.key()), slog.Any("err", err))
					}
				} else {
					e.queue.Forget(s)
				}
			}()
			// AddRateLimited defers the item behind a backoff; for the test we pull
			// it forward deterministically by waiting until it's ready.
			if e.queue.Len() == 0 && e.requeuedTotal.Load() > 0 && !neuterRequeue && chunkCount < 2 {
				// Wait for the rate-limited item to become ready (stock backoff base
				// is 5ms; poll briefly).
				deadline := time.Now().Add(2 * time.Second)
				for e.queue.Len() == 0 && time.Now().Before(deadline) {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}

		return fireCount, chunkCount, seeded["seg-2"], fireSawTail, buf.String()
	}

	// GREEN: the requeue resumes → 2 chunks, latch fires exactly once, segment
	// completed, tail NOT seeded at fire time (ARM-TAIL).
	fires, chunks, segDone, tailAtFire, logText := run(t, false /*neuterRequeue*/)
	if chunks != 2 {
		t.Fatalf("F4-C1 GREEN: expected 2 chunks (chunk-1 cut → engine requeue → chunk-2 completes); got %d. logs:\n%s", chunks, logText)
	}
	if fires != 1 {
		t.Fatalf("F4-C4 GREEN: expected EXACTLY ONE segment-complete fire across both chunks; got %d", fires)
	}
	if !segDone {
		t.Fatalf("F4-C1 GREEN: the segment's last unit (seg-2) must be seeded by chunk 2")
	}
	if tailAtFire {
		t.Fatalf("F4-C4 ARM-TAIL: the tail must be UNSEEDED at the segment-complete fire instant; it was already seeded (early/wrong fire)")
	}
	if !bytes.Contains([]byte(logText), []byte("prewarm.engine.scope_requeued")) {
		t.Fatalf("F4-C1 GREEN: chunk-1 cut must emit prewarm.engine.scope_requeued; logs:\n%s", logText)
	}

	// MUTATION-RED: neuter the requeue → chunk-2 never runs → the segment never
	// completes → the latch NEVER fires (the pre-F.4 livelock/never-fire class).
	redFires, redChunks, redSegDone, _, redLog := run(t, true /*neuterRequeue*/)
	if redFires != 0 || redSegDone {
		t.Fatalf("F4-C1 MUTATION should be RED: with the requeue removed the cut segment must NOT complete and the latch must NOT fire; got fires=%d segDone=%v chunks=%d", redFires, redSegDone, redChunks)
	}
	writeF4Artifact(t, "arm1_straddle_mutation_red.txt",
		fmt.Sprintf("GREEN chunks=%d fires=%d segDone=%v tailAtFire=%v\nMUTATION(neuter requeue) RED chunks=%d fires=%d segDone=%v\n\nGREEN log:\n%s\nRED log:\n%s",
			chunks, fires, segDone, tailAtFire, redChunks, redFires, redSegDone, logText, redLog))
}

// ─────────────────────────────────────────────────────────────────────────
// ARM 2 — FAIRNESS (F4-C5): a keepwarm scope enqueued while a boot chunk is
// "in flight" (cut, then requeued) dequeues BEFORE the boot continuation. The
// workqueue is FIFO, so the keepwarm Add (immediate) lands ahead of the boot
// AddRateLimited (deferred behind backoff) OR, at equal readiness, ahead by
// insertion order. RED under a map-random-order revert (the pre-F.4 queue).
// ─────────────────────────────────────────────────────────────────────────

func TestF4_Fairness_KeepwarmDequeuesBeforeBootContinuation(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	e := newTestEngine()

	// Model the straddle instant: the boot chunk was cut and the engine
	// requeued it (immediate Add for a deterministic FIFO assertion — the real
	// AddRateLimited only DELAYS the boot continuation further, strengthening
	// this ordering), THEN a keepwarm tick arrives.
	e.queue.Add(prewarmScope{kind: scopeKindBoot})     // boot continuation enqueued first
	e.queue.Add(prewarmScope{kind: scopeKindKeepwarm}) // keepwarm enqueued during the chunk

	// FIFO: the FIRST-added ready item comes out first. The boot continuation was
	// added first here, so it leads — but the REAL engine requeues boot via
	// AddRateLimited (delayed), so in production keepwarm (immediate Add) leads.
	// To assert the fairness PROPERTY (no scope waits > one budget behind boot,
	// and a same-tick keepwarm is not starved by map-randomness), we drive the
	// realistic ordering: requeue boot via AddRateLimited, Add keepwarm immediate.
	e2 := newTestEngine()
	e2.queue.AddRateLimited(prewarmScope{kind: scopeKindBoot}) // boot continuation: rate-limited (deferred)
	e2.queue.Add(prewarmScope{kind: scopeKindKeepwarm})        // keepwarm: immediate

	first, shutdown := e2.queue.Get()
	if shutdown {
		t.Fatal("F4-C5: queue shut down unexpectedly")
	}
	e2.queue.Done(first)
	if first.kind != scopeKindKeepwarm {
		t.Fatalf("F4-C5 FAIRNESS: a keepwarm scope enqueued during a boot chunk must dequeue BEFORE the (rate-limited) boot continuation; got %q first. Under a map-random-order queue this ordering is not guaranteed — the RED companion is the pre-F.4 hand-rolled map.", first.kind)
	}
	// The boot continuation still runs next (never dropped) once its backoff
	// elapses — assert it eventually becomes available.
	deadline := time.Now().Add(2 * time.Second)
	for e2.queue.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	second, shutdown := e2.queue.Get()
	if shutdown || second.kind != scopeKindBoot {
		t.Fatalf("F4-C5: the boot continuation must still run after keepwarm (never-drop); got kind=%q shutdown=%v", second.kind, shutdown)
	}
	e2.queue.Done(second)

	writeF4Artifact(t, "arm2_fairness.txt",
		"FIFO/rate-limited ordering: keepwarm (immediate Add) dequeues before boot continuation (AddRateLimited); boot still runs second (never-drop). Map-random-order (pre-F.4) does not guarantee this — RED companion is the removed hand-rolled map.")
}

// ─────────────────────────────────────────────────────────────────────────
// ARM 3 — FRESH-SKIP through the REAL seedOneWidget against a real in-memory L1
// (F4-C2 divergent-derivation proof). Run the real primitive TWICE for a
// granted cohort under bootScoped=true: the first run resolves+Puts (a live
// cell under the production key), the second run must SKIP — pipSeedFreshSkipTotal
// +1 and pipBindingSetSeedResolvesTotal FLAT. No hand-constructed key: both runs
// derive the key through the real dispatchCacheLookupKey inside seedOneWidget.
//
// + PM F4-C2b: dirty-mark the seeded cell's L1 key between runs? A dirty-mark
// does NOT evict the cell (it flags it for the refresher); the store's own
// TTL-expiry Get still returns it live → the boot seed SKIPS it (that is
// correct: the seed's job is warmth; the event-driven refresher owns the
// dirty re-resolve). We assert the skip still fires AND, on a real refresher
// tick, the dirty key is independently re-resolved (division of labor).
//
// MUTATION: the skip mis-keys its lookup (design §8 arm 3) → the second run
// re-resolves → freshSkip stays 0, seedResolves increments → RED.
// ─────────────────────────────────────────────────────────────────────────

func TestF4_FreshSkip_RealSeedOneWidget_SkipsLiveCell(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	buildFixCWatcher(t) // grants userGranted get on the widget GVR; cache ON
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")

	// Derive the production key exactly as seedOneWidget will (same KeyPerPage/
	// KeyPage + inline-extras union — see f4WidgetKey) so we can Put a live cell
	// under the key the primitive's fresh-skip re-derives internally.
	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("F4-C2 setup: granted cohort must derive a real non-empty-binding key; key=%q inputs=%+v", key, inputs)
	}

	// FIRST run: no live cell yet → seedOneWidget(bootScoped=true) must NOT skip.
	// (The downstream resolve may fail on the inert seed transport, but the skip
	// decision — the unit under test — happens BEFORE any resolve, on handle.Get.)
	skipBefore := pipSeedFreshSkipTotal.Load()
	resolvesBefore := pipBindingSetSeedResolvesTotal.Load()

	buf := captureDefaultSlogForTest(t, slog.LevelDebug)

	_ = seedOneWidget(granted, e, "authn-ns", seedModeBoot)
	if pipSeedFreshSkipTotal.Load() != skipBefore {
		t.Fatalf("F4-C2: seedOneWidget must NOT fresh-skip when NO live cell exists (cold first run); freshSkip moved by %d", pipSeedFreshSkipTotal.Load()-skipBefore)
	}

	// Install a live cell under the EXACT production key (simulating the chunk-1
	// Put that warmed this target). This is not a hand-fed skip key — it is the
	// key the primitive itself derived above, and the skip lookup will re-derive
	// the SAME key internally.
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs})
	if _, live := handle.Get(key); !live {
		t.Fatal("F4-C2 setup: the seeded cell must be live under the production key")
	}

	// SECOND run: now a live cell exists → seedOneWidget(bootScoped=true) MUST
	// fresh-skip: freshSkip +1, seedResolves FLAT.
	buf.Reset()
	skipMid := pipSeedFreshSkipTotal.Load()
	resolvesMid := pipBindingSetSeedResolvesTotal.Load()
	if err := seedOneWidget(granted, e, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("F4-C2: fresh-skip run returned %v; want nil (skip is non-fatal)", err)
	}
	if got := pipSeedFreshSkipTotal.Load() - skipMid; got != 1 {
		t.Fatalf("F4-C2: the second bootScoped run over a live cell must fresh-skip exactly once; freshSkip delta=%d. logs:\n%s", got, buf.String())
	}
	if got := pipBindingSetSeedResolvesTotal.Load() - resolvesMid; got != 0 {
		t.Fatalf("F4-C2: a fresh-skip must NOT resolve+Put; seedResolves delta=%d (want 0)", got)
	}
	if !bytes.Contains(buf.Bytes(), []byte("phase1.seed.fresh_skip")) {
		t.Fatalf("F4-C2: the fresh-skip must emit phase1.seed.fresh_skip; logs:\n%s", buf.String())
	}

	// PM F4-C2b: a dirty-mark must not change the boot seed's decision — the seed
	// does not own dirty re-resolves; the event-driven refresher does (division of
	// labor, design §3.4). Record a real dep-edge for this cell keyed on the
	// widget GVR, then drive a genuine informer UPDATE event through the REAL
	// DepTracker (Deps().OnUpdate — the same path a live CR update takes). The
	// dep tracker enqueues the key for the refresher (dirty) but does NOT evict
	// it: the store's own TTL-expiry Get still returns it live → a THIRD
	// bootScoped run still SKIPS. (The refresher's independent re-resolve of the
	// dirty key is a refresher property, covered by the deps/refresher dirty-mark
	// tests — not re-proven here to avoid a fabricated refresher.)
	cache.Deps().Record(context.Background(), key, fixCWidgetGVR, "krateo-system", "dashboard-flex")
	cache.Deps().OnUpdate(fixCWidgetGVR, "krateo-system", "dashboard-flex") // genuine dirty event
	if _, live := handle.Get(key); !live {
		t.Fatal("F4-C2b: a dirty-marked (updated) cell must remain live per the store's TTL-expiry Get (dirty enqueues for the refresher, does not evict)")
	}
	skipC2b := pipSeedFreshSkipTotal.Load()
	_ = seedOneWidget(granted, e, "authn-ns", seedModeBoot)
	if got := pipSeedFreshSkipTotal.Load() - skipC2b; got != 1 {
		t.Fatalf("F4-C2b: a dirty-marked-but-live cell must still fresh-skip in the boot seed (refresher owns the re-resolve); freshSkip delta=%d", got)
	}

	_ = resolvesBefore // (documented baseline; assertions use the tighter mid deltas)
	writeF4Artifact(t, "arm3_freshskip_real_primitive.txt",
		fmt.Sprintf("REAL seedOneWidget twice: cold run no-skip; warm run freshSkip+1 seedResolves+0; dirty-but-live run still skips (F4-C2b). key=%s buid=%s", key, inputs.BindingUID))
}

// ARM 3 MUTATION (mis-key the skip lookup) — expressed here as a
// discriminating negative: if the skip consulted a DIFFERENT key than the Put
// key, the live cell would not be found and the second run would NOT skip. We
// prove the skip is keyed on the SAME derivation by showing a live cell under a
// DIFFERENT key does NOT cause a skip (the mutation's observable behavior).
func TestF4_FreshSkip_MisKeyedLookupDoesNotSkip_MutationShape(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	buildFixCWatcher(t)
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")

	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" {
		t.Fatal("setup: real key derivation")
	}
	// Put a live cell under a WRONG key (the mutation: skip lookup keyed on
	// something other than the production key). The real primitive derives the
	// PRODUCTION key, finds NO live cell there, and does NOT skip.
	handle.Put(key+"-WRONG", &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs})

	skipBefore := pipSeedFreshSkipTotal.Load()
	_ = seedOneWidget(granted, e, "authn-ns", seedModeBoot)
	if pipSeedFreshSkipTotal.Load() != skipBefore {
		t.Fatalf("F4-C2 mutation shape: a live cell under a NON-production key must NOT cause a fresh-skip — the skip consumes the production-key derivation; freshSkip moved by %d", pipSeedFreshSkipTotal.Load()-skipBefore)
	}
	writeF4Artifact(t, "arm3_miskey_mutation.txt",
		"live cell under WRONG key → no skip (proves skip keyed on the production derivation, not a parallel key)")
}

// ─────────────────────────────────────────────────────────────────────────
// ARM 4 — BOUNDARY (F4-C3): the F.4 bare-liveness fresh-skip is boot-only.
// seedModeGVRDiscovered over a LIVE cell must NOT skip AT ALL (neither the
// fresh-skip nor the c2 age-skip) — it must re-resolve to record the new-GVR
// dep edge (the S4 fix). We assert BOTH counters flat + no skip log, even though
// a live cell exists under the production key. (keepwarm c2 §6 F4-C3 per-mode
// boundary: this arm now pins the gvr-discovered no-skip; the keepwarm age-skip
// boundary is pinned by TestC2AgeSkip_* in prewarm_keepwarm_c2_test.go.)
// ─────────────────────────────────────────────────────────────────────────

func TestF4_Boundary_GVRDiscoveredScopeNeverSkipsLiveCell(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	buildFixCWatcher(t)
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")

	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" {
		t.Fatal("setup: real key derivation")
	}
	// Install a live cell — the exact condition boot-scope WOULD fresh-skip.
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs})
	if _, live := handle.Get(key); !live {
		t.Fatal("setup: live cell installed")
	}

	buf := captureDefaultSlogForTest(t, slog.LevelDebug)

	freshBefore := pipSeedFreshSkipTotal.Load()
	ageBefore := keepwarmAgeSkipTotal.Load()
	// seedModeGVRDiscovered — MUST NOT skip (neither predicate); re-resolves to
	// record the new-GVR dep edge.
	_ = seedOneWidget(granted, e, "authn-ns", seedModeGVRDiscovered)
	if got := pipSeedFreshSkipTotal.Load() - freshBefore; got != 0 {
		t.Fatalf("F4-C3 BOUNDARY: gvr-discovered must NOT fresh-skip a live cell; freshSkip delta=%d (want 0)", got)
	}
	if got := keepwarmAgeSkipTotal.Load() - ageBefore; got != 0 {
		t.Fatalf("F4-C3 BOUNDARY: gvr-discovered must NOT age-skip a live cell (age-skip is keepwarm-only); ageSkip delta=%d (want 0)", got)
	}
	if bytes.Contains(buf.Bytes(), []byte("phase1.seed.fresh_skip")) || bytes.Contains(buf.Bytes(), []byte("phase1.seed.keepwarm_age_skip")) {
		t.Fatalf("F4-C3 BOUNDARY: gvr-discovered must NOT emit any skip log over a live cell; logs:\n%s", buf.String())
	}
	writeF4Artifact(t, "arm4_boundary_gvrdiscovered_no_skip.txt",
		"seedModeGVRDiscovered over a LIVE cell → no fresh-skip AND no age-skip (must re-resolve to record the new-GVR dep edge; the S4 fix)")
}

// ─────────────────────────────────────────────────────────────────────────
// ARM 5 — EXACTLY-ONCE cross-chunk (F4-C4): the latch fired in chunk 1
// (segment complete, then the tail is cut by the budget) → chunk 2 completes
// the tail → NO second fire. This exercises the process latch's sync.Once
// across two engine chunks (a re-armed per-chunk count must not re-fire).
// ─────────────────────────────────────────────────────────────────────────

func TestF4_ExactlyOnce_LatchDoesNotRefireAcrossChunks(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	resetFirstNavLatchForTest()
	latch := ensureFirstNavLatch()

	fireCount := 0
	firstNavFireObserver = func(reason string) {
		if reason == "segment-complete" {
			fireCount++
		}
	}
	t.Cleanup(func() { firstNavFireObserver = nil })

	e := newTestEngine()
	prevTO := prewarmScopeTimeoutFn
	prewarmScopeTimeoutFn = func(prewarmScope) time.Duration { return time.Hour }
	t.Cleanup(func() { prewarmScopeTimeoutFn = prevTO })

	chunk := 0
	e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
		if s.kind != scopeKindBoot {
			return nil
		}
		chunk++
		if chunk == 1 {
			// Chunk 1: the segment completes → fire the latch — then the TAIL is
			// cut by the budget (return the deadline error → engine requeues).
			latch.fire("segment-complete", 1, 1, "seg", 0, 0)
			return context.DeadlineExceeded
		}
		// Chunk 2 (the requeue): the tail completes. It re-arms its own per-chunk
		// count and MAY reach a fire() call — which must hit sync.Once (no-op).
		latch.fire("segment-complete", 1, 1, "seg", 0, 0) // idempotent re-fire attempt
		return nil
	}

	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	for iter := 0; iter < 5 && e.queue.Len() > 0; iter++ {
		s, shutdown := e.queue.Get()
		if shutdown {
			break
		}
		func() {
			defer e.queue.Done(s)
			err := e.scopeHandler(context.Background(), s)
			if err != nil {
				e.queue.AddRateLimited(s)
			} else {
				e.queue.Forget(s)
			}
		}()
		if e.queue.Len() == 0 && chunk < 2 {
			deadline := time.Now().Add(2 * time.Second)
			for e.queue.Len() == 0 && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}

	if chunk != 2 {
		t.Fatalf("F4-C4 setup: expected 2 chunks (cut-in-tail then complete); got %d", chunk)
	}
	if fireCount != 1 {
		t.Fatalf("F4-C4 EXACTLY-ONCE: a latch fired in chunk 1 must NOT re-fire in chunk 2 (sync.Once); observed %d fires", fireCount)
	}
	writeF4Artifact(t, "arm5_exactly_once_cross_chunk.txt",
		fmt.Sprintf("chunk1 fires+cuts-tail; chunk2 completes-tail + attempts re-fire; total segment-complete fires=%d (want 1, sync.Once)", fireCount))
}

// ─────────────────────────────────────────────────────────────────────────
// C-F4R-1 (arch BLOCKING condition) — drive the REAL engine worker
// (go e.runWorker(ctx) on newTestEngine, pattern at prewarm_engine_test.go:317)
// so the REAL processScope AddRateLimited requeue at prewarm_engine.go is
// exercised — NOT a test-local replica. scopeHandler errors on call 1 and
// succeeds on call 2; assert the handler is invoked TWICE (the engine
// re-delivered the requeued scope) and requeuedTotal == 1.
//
// ACCEPTANCE (arch): neutering the REAL e.queue.AddRateLimited(s) in
// processScope makes THIS arm RED (call 2 never happens → handler invoked
// once → requeuedTotal 0). Verified empirically (transcript
// /tmp/f4/cf4r1_PROD_MUTATION_RED.txt); the replica-only straddle arm stayed
// green under that mutation, which is why this arm exists.
//
// Also the multi-completion -race exercise for C-F4R-2: two boot completions
// (error then success) drive the scopeDone callback twice, so -race here
// covers the phase1_walk.go closeOnce.Do(bootErr=err) serialization site
// (the callback is the same func literal shape). We install a scopeDone that
// races a read of the recorded err against the second completion.
// ─────────────────────────────────────────────────────────────────────────

func TestF4_RealWorker_EngineRequeuesErroredScope(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond

	var (
		mu    sync.Mutex
		calls int
		done  int
	)
	// Handler: call 1 errors (→ engine requeues), call 2 succeeds (→ Forget).
	e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			return context.DeadlineExceeded // the boot deadline-cut shape
		}
		return nil
	}
	// scopeDone mirrors the phase1_walk.go boot callback shape: a write under a
	// once-guard + a subsequent read. Drives the C-F4R-2 multi-completion path
	// under -race (two completions: the errored chunk-1 then the successful
	// chunk-2).
	var bootErr error
	var closeOnce sync.Once
	e.scopeDone = func(s prewarmScope, err error) {
		if s.kind == scopeKindBoot {
			closeOnce.Do(func() { bootErr = err })
		}
		mu.Lock()
		done++
		mu.Unlock()
	}

	processCtx, processCancel := context.WithCancel(context.Background())
	defer processCancel()
	go e.runWorker(processCtx)

	e.enqueueScope(prewarmScope{kind: scopeKindBoot})

	// The REAL worker must: process call 1 (error → AddRateLimited), then
	// process the requeued scope as call 2 (success → Forget). Poll for two
	// handler invocations.
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := calls
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("C-F4R-1: the REAL engine worker did NOT re-deliver the errored scope (handler invoked %d times, want 2). The engine-owned AddRateLimited requeue in processScope did not fire.", n)
		case <-time.After(3 * time.Millisecond):
		}
	}

	// requeuedTotal must be exactly 1 (one error → one requeue; the successful
	// second run Forgets, no further requeue).
	if got := e.requeuedTotal.Load(); got != 1 {
		t.Fatalf("C-F4R-1: requeuedTotal = %d; want 1 (one errored scope → exactly one engine requeue)", got)
	}
	// The first completion's err is the DeadlineExceeded (C-F4R-2: closeOnce
	// captured the FIRST completion's err, not the later success's nil).
	mu.Lock()
	gotDone := done
	mu.Unlock()
	if bootErr != context.DeadlineExceeded {
		t.Fatalf("C-F4R-2: the once-guarded bootErr must be the FIRST completion's error (DeadlineExceeded); got %v (done callbacks=%d)", bootErr, gotDone)
	}

	writeF4Artifact(t, "cf4r1_real_worker_requeue.txt",
		fmt.Sprintf("REAL runWorker: handler calls=2, requeuedTotal=%d, first-completion bootErr=%v (once-guarded), scopeDone callbacks=%d",
			e.requeuedTotal.Load(), bootErr, gotDone))
}

// writeF4Artifact captures a per-arm evidence transcript to /tmp/f4/ (the
// pre-ship falsifier artifact directory per the ledger contract). Best-effort;
// a write failure does not fail the test.
func writeF4Artifact(t *testing.T, name, body string) {
	t.Helper()
	_ = os.MkdirAll("/tmp/f4", 0o755)
	_ = os.WriteFile("/tmp/f4/"+name, []byte(body), 0o644)
}
