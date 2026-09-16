// prewarm_engine_boot.go — Ship 1: the rePrewarm core (boot scope) — the
// post-sync re-walk + the per-target-GVR-scoped per-cohort seed, unified
// into one scope-parameterised function.
//
// This is the function StartPrewarmEngine's worker invokes per dequeued
// scope. For the BOOT scope it:
//
//  1. RE-WALKS the 2 nav roots AFTER the sync barrier, with a FRESH
//     phase1Walker per root (new visited map) and the SAME walk() — so
//     harvestApiRef + harvestNavWidget re-fire over the now-populated
//     dynamic navmenu children (the boot-race fix). The harvesters are
//     SHARED BY REFERENCE with the boot pass so the re-walk's harvest
//     lands in the set the seed drains.
//  2. SETTLES the registered set once (settleRegisteredSet — single pass,
//     not a loop) so a CRD discovered by the re-walk has its informer
//     registered before the seed.
//  3. BUILDS the BindingsByGVR index over RegisteredGVRs() (the cohort
//     scoping substrate) — Ship 1 builds it here, on the engine path,
//     after the re-walk has discovered the navigated GVRs.
//  4. SEEDS per cohort with PER-TARGET-GVR scoping: the widget loop scopes
//     cohorts on the widget's GVR; the restaction loop scopes on the
//     RESTAction's TARGET GVR (the GVR it LISTs). Falls back to the global
//     EnumerateBindingSetClasses when the index yields nothing for a GVR
//     (safe over-inclusion).
//
// CUSTOMER PRIORITY. The seed loop calls engineYieldCheckpoint(ctx)
// between cohorts so a customer /call burst arriving mid-seed defers the
// remaining cohorts (project_c3_design_2026_05_27 B1). The seed itself
// keeps the per-cohort timeout (seedCohort) and per-target error
// containment unchanged.
//
// NO STATIC LIST (feedback_dynamic_cohort_prewarm_no_static_no_cold_fill).
// The roots come from the frontend config.json; the cohort set comes from
// the live BindingsByGVR index; the navigated GVRs come from the walk.
// Nothing is a Go literal.

package dispatchers

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	pmaps "github.com/krateo-platformops/plumbing/maps"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// rePrewarmDeps bundles the dependencies the rePrewarm core needs — the
// watcher, the navigation-roots lister + per-root resolver (so the
// re-walk uses the EXACT same walk() entrypoint as the boot pass), the
// shared harvesters the seed drains, and the SA config for the per-cohort
// seed context. Built once by Phase1Warmup and closed into the boot scope
// handler.
type rePrewarmDeps struct {
	rw        *cache.ResourceWatcher
	lister    rootsLister
	harvester *contentPrewarmHarvester
	navHarv   *navWidgetHarvester
	saEP      endpoints.Endpoint
	saRC      *rest.Config
	authnNS   string
}

// makeBootScopeHandler returns the engine scope handler closure for the
// boot scope, bound to deps. The engine worker calls it per dequeued
// scope; Ship 1 enqueued only scopeKindBoot. Ship 2 Stage 2 (0.30.247)
// adds scopeKindGVRDiscovered dispatch.
func makeBootScopeHandler(deps rePrewarmDeps) func(ctx context.Context, s prewarmScope) error {
	return func(ctx context.Context, s prewarmScope) error {
		switch s.kind {
		case scopeKindBoot:
			// seedModeBoot engages the F.4 fresh-skip so a deadline-cut boot
			// chunk's continuation skips already-live cells (all ranks).
			return rePrewarmBoot(ctx, deps, seedModeBoot)
		case scopeKindGVRDiscovered:
			return rePrewarmGVRDiscovered(ctx, deps, s.gvr)
		case scopeKindKeepwarm:
			// keepwarm c2 — the TTL-cadenced widget-capable-cohort keep-warm sweep.
			return rePrewarmKeepwarm(ctx, deps)
		default:
			// Ship 2 future scopes (scopeKindWidgetCR / scopeKindRBACShift)
			// land here when unwired.
			slog.Warn("prewarm.engine.unknown_scope",
				slog.String("subsystem", "cache"),
				slog.String("scope", s.key()),
			)
			return nil
		}
	}
}

// registerEngineGVRDiscoveredHook subscribes the engine to the
// cache-side GVR-discovered hook. Called once per process from
// StartPrewarmEngine inside startedOnce — BEFORE the engine worker
// spawns — so a GVR discovery firing during boot is queued, not
// dropped.
//
// The hook callback is intentionally TINY: it builds a prewarmScope
// and calls enqueueScope. The enqueue is O(1) (sync.Mutex critical
// section + buffered=1 channel send) and never blocks. The actual
// re-prewarm work runs on the engine worker goroutine, scoped through
// the customer-priority yield + bounded queue.
//
// CONTRACT: the cache fires this hook synchronously inside its
// discovery goroutine. Keep the callback non-blocking — see the
// RegisterGVRDiscoveredHook doc-comment for the cache-side guarantee
// that hook handlers must not block.
//
// Per task-194-ship-2-stage-2-empirical-trace-2026-06-04.md §5.2
// commit-4.
func registerEngineGVRDiscoveredHook(e *prewarmEngine) {
	cache.RegisterGVRDiscoveredHook(func(gvr schema.GroupVersionResource) {
		e.enqueueScope(prewarmScope{
			kind: scopeKindGVRDiscovered,
			gvr:  gvr,
		})
	})
}

// registerHarvesterGoneForgetHook subscribes BOTH Phase-1 harvesters to the
// cache-side GONE verdict (1.12.7 F6b). Sibling of
// registerEngineGVRDiscoveredHook above, wired from the same place and for the
// same reason: the import graph is one-way, so the cache cannot reach the
// harvesters and publishes a registry instead.
//
// WHAT IT FIXES. Both harvesters are append-only. A widget harvested once
// keeps its DeepCopy for the life of the process and the per-binding seed
// re-resolves THAT COPY on every pass; the seed never fetches the object, so a
// deleted widget can never 404 and no decline guard can fire. Eviction cannot
// win — the re-drive skips live cells, so a successful eviction is exactly what
// makes the cell eligible to be seeded again. This drops the copy on the
// authoritative verdict the process already derives.
//
// REMOVE ON POSITIVE EVIDENCE, NEVER ON ABSENCE. The cache fires this only for
// objAbsent — a servable informer whose indexer does not hold the object. An
// uncertain or degraded informer never reaches it, and a lossy walk never
// removes anything (see the rejected build-and-swap).
//
// CONTRACT: the cache fires this synchronously on its dep-event worker. The
// callback is a bounded map scan under each harvester's own mutex — no I/O and
// no apiserver call — so it satisfies the non-blocking requirement in
// RegisterGoneForgetHook's doc comment. Either harvester may be nil (prewarm
// off); both forget methods are nil-safe.
func registerHarvesterGoneForgetHook(deps rePrewarmDeps) {
	cache.RegisterGoneForgetHook(func(gvr schema.GroupVersionResource, namespace, name string) {
		// 1.12.7 review §2(a) — every verdict delivered, forgotten or not. This
		// is the denominator that tells a 0 in harvest_forgotten_total apart
		// from a dead hook.
		goneVerdictsTotal.Add(1)
		dropped := deps.navHarv.forgetCoordinate(gvr, namespace, name)
		dropped += deps.harvester.forgetCoordinate(gvr, namespace, name)
		if dropped == 0 {
			return // the common case: the gone object was never harvested
		}
		slog.Info("prewarm.harvest.forgot_gone_object",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("ns", namespace),
			slog.String("name", name),
			slog.Int("entries_dropped", dropped),
			slog.String("effect", "the harvested in-memory copy is gone, so the per-binding seed will "+
				"no longer re-resolve and re-write a cell for a deleted object"),
		)
	})
}

// rePrewarmGVRDiscovered is the Ship 2 Stage 2 sub-handler for
// scopeKindGVRDiscovered. Invoked once per (distinct) GVR discovered
// post-boot via the cache→dispatchers hook
// (cache.RegisterGVRDiscoveredHook). The bounded dedup queue at
// prewarm_engine.go:213-225 coalesces repeated enqueues for the same
// GVR within a short window.
//
// MECHANISM: invokes the SAME rePrewarmBoot core — no new harvester
// wiring, no parallel seed mechanism. Per the trace at §5.5 Step F,
// the rePrewarm core ALREADY:
//
//   - Re-walks the nav tree with a fresh visited set (so the iterator
//     at resolve.go:377-381 re-runs over the now-populated crds.items[]
//     — the H4 root cause site that previously short-circuited and
//     skipped dep recording is now non-empty).
//   - Re-builds + re-reads the BindingsByGVR index (now widened by the
//     AddNavigatedGVR call in discovery_lookup.go) — narrow-RBAC
//     cohorts are enumerated.
//   - Re-seeds each cohort via seedOneRestaction/seedOneWidget under
//     the cohort's identity → admin's BindingUID-keyed cell (and every
//     other cohort's cell) is re-resolved with non-empty tmp[] →
//     dep edge against the new GVR IS recorded → subsequent CR events
//     match.
//
// The discovered GVR is logged but not propagated through ctx today
// (no per-GVR filtering inside rePrewarmBoot at this point). A future
// optimisation could narrow the re-walk to roots that template-path
// against this GVR; until then the broad re-walk is the structurally-
// correct mechanism.
//
// SAFETY (R2 install-burst quantification per §5.3): the dedup keys
// distinct GVRs as distinct work items. A 10-CRD install burst
// produces 10 sequential rePrewarms (each yielding to customer /call
// between cohorts via engineYieldCheckpoint). Each rePrewarmBoot is
// bounded by ctx + the engine-side timeouts; the per-cohort
// seedCohort timeout (pipCohortTimeout) caps individual stalls.
func rePrewarmGVRDiscovered(ctx context.Context, deps rePrewarmDeps, gvr schema.GroupVersionResource) error {
	slog.Info("prewarm.engine.gvr_discovered.start",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("note", "Ship 2 Stage 2 — re-walk under cohort identities so iterator-empty short-circuit (resolve.go:377-381) no longer skips dep recording for this GVR"),
	)
	// seedModeGVRDiscovered: NO skip — gvr-discovered must RE-RESOLVE already-warm
	// cells so the dep edge against the newly-registered GVR is recorded (the S4
	// fix); any skip would reintroduce the defect (F4-C3 boundary).
	err := rePrewarmGVRDiscoveredSeed(ctx, deps)
	if err != nil {
		slog.Warn("prewarm.engine.gvr_discovered.incomplete",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.Any("err", err),
		)
		return err
	}
	slog.Info("prewarm.engine.gvr_discovered.complete",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
	)
	return nil
}

// rePrewarmBoot runs the full re-walk + index rebuild + per-identity seed under
// the given seedScopeMode. The SAME walk() is used (via the deps.lister + a
// fresh phase1Walker per root); the harvesters are shared by reference with the
// boot pass. The keepwarm sweep + gvr-discovered scope reuse this SAME core via
// rePrewarmKeepwarm / rePrewarmGVRDiscoveredSeed, so each gets the boot's dedup,
// NavOrder, BindingsByGVR index, and yield/memory bounds for free — they ARE the
// seed, just with a different (sweep-set × skip) mode.
//
// mode selects the seed policy in seedScopeYielding + the primitives:
//   - seedModeBoot: all ranks; F.4 fresh-skip (deadline-cut resume is
//     cost-proportional).
//   - seedModeKeepwarm: widget-capable prefix; age-skip (keepwarm c2 §4).
//   - seedModeGVRDiscovered: all ranks; no skip (record the new-GVR dep edge).
func rePrewarmBoot(ctx context.Context, deps rePrewarmDeps, mode seedScopeMode) error {
	return rePrewarmBootScoped(ctx, deps, mode)
}

// rePrewarmKeepwarm is the keepwarm-sweep handler (keepwarm c2): the SAME
// re-walk + seed core as boot, but seedModeKeepwarm bounds the seed to the
// WIDGET-CAPABLE PREFIX of `ranked` (every login-cohort-shaped identity, all
// pages) and applies the age-skip. Fires on the TTL×3/4 cadence
// (keepwarmTicker) so those cells are re-Put — resetting CreatedAt — before they
// lazy-expire at TTL. Re-resolving (not TTL-extending) preserves the §1.6
// staleness backstop by construction: every sweep Put is FRESH bytes, and the
// GTTL-1 error-aware Put-gate in the shared seed primitives declines a degraded
// re-resolve rather than overwrite a good warm entry.
func rePrewarmKeepwarm(ctx context.Context, deps rePrewarmDeps) error {
	return rePrewarmBootScoped(ctx, deps, seedModeKeepwarm)
}

// rePrewarmGVRDiscoveredSeed is the gvr-discovered seed entrypoint
// (seedModeGVRDiscovered: all ranks, NO skip). Split out from rePrewarmBoot's
// bool-arg form so the mode is explicit at the call site (the S4 fix depends on
// the no-skip semantics; naming it prevents a future edit passing the wrong
// mode).
func rePrewarmGVRDiscoveredSeed(ctx context.Context, deps rePrewarmDeps) error {
	return rePrewarmBootScoped(ctx, deps, seedModeGVRDiscovered)
}

// bootShouldWalk is the #135 F4b Lever B reuse predicate — the SINGLE source of
// the walk-vs-reuse decision (rePrewarmBootScoped calls this; the falsifier calls
// the SAME function, never a copy, so the test can't drift from prod — the #64/#66
// anti-shadow-drift lesson). Returns true = re-walk discovery, false = reuse the
// process-lived harvester snapshot.
//
//   - attempt==0  → pass 0: a fresh enqueue OR a genuine config-vars redrive that
//     reset the requeue count via forgetScope (enqueueBootReDrive). WALK.
//   - attempt>0   → an F.4 deadline-cut resume of the SAME topology. REUSE — UNLESS
//     harvestedCount==0 (a resume whose pass 0 never harvested: boot-race give-up
//     on absent config, or a cut before any harvest). An empty harvester has
//     nothing to reuse, so it must WALK (keeps the give-up→appear self-heal path
//     re-walking).
func bootShouldWalk(attempt, harvestedCount int) bool {
	return attempt == 0 || harvestedCount == 0
}

func rePrewarmBootScoped(ctx context.Context, deps rePrewarmDeps, mode seedScopeMode) error {
	log := slog.Default()
	start := time.Now()

	if deps.rw == nil || deps.lister == nil {
		return nil
	}

	// MEMORY SHAPE (informational). This engine boot seed runs the SERIAL
	// single-worker loop (seedScopeYielding — a plain for-loop with one
	// restactions.Resolve in flight at a time, yielding to customers via
	// engineYieldCheckpoint). That serial shape is what memory-bounds the
	// seed: warm-peak is bounded by a SINGLE resolve's weight, NOT a
	// concurrent fan-out. The seed's dominant allocation (seedRAFullListForWidget's
	// unpaginated full-list) is additionally bounded by the ADAPTIVE seed-unit
	// gate (enterSeedUnit, seed_bound.go — fold 2026-07-03 §3), which serializes
	// units against live GOMEMLIMIT headroom.
	//
	// FOLDED 2026-07-03 (docs/prewarm-engine-implicit-on-cache-2026-07-03.md §4):
	// the old latent hazard — the DEAD errgroup runPIPSeed path re-activating
	// when PREWARM_ENGINE_ENABLED=false — is GONE. runPIPSeed is DELETED and
	// PrewarmEngineEnabled() is implicit-on-cache, so the engine is the only
	// seed path whenever prewarm runs. The defensive
	// hazard_engine_disabled_but_boot_reached Warn that guarded that
	// now-unreachable state is removed with it.

	// ── (1) RE-WALK the nav roots AFTER the sync barrier. A FRESH walker
	// per root (new visited map — reusing the boot pass's visited would
	// short-circuit every child and descend nothing). The SAME walk() so
	// harvestApiRef + harvestNavWidget re-fire over the now-populated
	// dynamic navmenu children.
	// BOOT-RACE-TOLERANT (shape A §2.4 D3): mark the re-drive ctx as a
	// background resolve so the self-heal re-walk YIELDS memory + the C5
	// cold-fan-out admission race to live customer /call traffic (complements
	// engineYieldCheckpoint's between-cohort yield). A config-vars-driven
	// re-drive can fire long after Ready — while customers are actively
	// navigating — so it must not contend with them at the aggregate OOM
	// bound (the 1.5.28 adaptive aggregate). 1 line, ctx-only, no behaviour
	// change to accounting (background differs only at admission). See
	// cache.WithBackgroundResolve.
	ctx = cache.WithBackgroundResolve(ctx)
	rctx := withPhase1SAContext(ctx, deps.saEP, deps.saRC)

	// #135 F4b Lever B — SKIP the discovery re-walk on an F.4 deadline-cut RESUME
	// pass. The navWidgetHarvester + contentPrewarmHarvester are process-lived,
	// monotonic, first-write-wins accumulators (never cleared in prod), so on a
	// resume pass their snapshot() already holds the UNION of every prior pass —
	// re-walking rediscovers a set the shared harvester already has (~255s of pure
	// waste). Gate on attempt (the workqueue requeue count carried by processScope):
	//   - attempt==0 → pass 0 (fresh enqueue, OR a Forget-reset genuine config-vars
	//     redrive) → WALK the current config.json roots.
	//   - attempt>0 → deadline-cut resume of the SAME topology → REUSE the snapshot.
	// GUARD len(snapshot)==0: a resume whose pass 0 never harvested (boot-race
	// give-up on absent config, or a deadline-cut before ANY harvest) MUST still
	// walk — an empty harvester has nothing to reuse. This keeps the
	// give-up→appear→self-heal path (TestBootRace_ConfigVarsInformerDrivesReWalk)
	// re-walking. A genuine config-vars redrive resets attempt to 0 (Forget in
	// enqueueBootReDrive), so the NEW topology is always walked (design §3).
	attempt := cache.BootResumeAttemptFromContext(ctx)
	doWalk := bootShouldWalk(attempt, len(deps.navHarv.snapshot()))

	var roots []navigationRoot
	rewalked := 0
	if doWalk {
		var listErr error
		roots, listErr = deps.lister(rctx)
		if listErr != nil {
			log.Warn("prewarm.engine.boot.roots_list_failed",
				slog.String("subsystem", "cache"),
				slog.Any("err", listErr),
				slog.String("effect", "boot re-walk has no roots; first /call per cohort falls back to per-user resolve"),
			)
			return listErr
		}
		// #99b Fix 2 — reset the harvester's config-root index to -1 at the top of
		// this walk PASS so the per-root BeginRoot() below stamps RootIndex 0..N-1
		// in config.json order, exactly like the boot walk (phase1_walk.go:401).
		// WITHOUT the reset the re-walk would resume from the boot walk's final
		// curRoot=N-1 and a widget FIRST harvested only during this re-walk (the
		// 50K+ common case, where the effective harvest comes from the config-vars
		// redrive) would stamp N..2N-1, so its RootIndex would never be 0 → the
		// first-nav latch zero-fires (multi-root). First-write-wins dedupe
		// preserves any boot-walk stamp. Nil-safe. Inert on single-root config
		// (curRoot pinned 0 after the first BeginRoot); required for multi-root.
		//
		// #135 F4b Lever B — BeginWalk() lives INSIDE the walk branch: a reuse pass
		// does no BeginRoot(), so resetting curRoot to -1 (with nothing to re-stamp)
		// would only strand the index at -1. The existing RootIndex stamps must
		// stand; the seed drains the already-populated snapshot unchanged.
		deps.navHarv.BeginWalk()
	} else {
		// #135 F4b Lever B — F.4 RESUME pass: SKIP the ~255s discovery re-walk and
		// REUSE the process-lived harvester snapshot (already the UNION of every
		// prior pass — navWidgetHarvester is monotonic / first-write-wins / never
		// cleared in prod). `roots` stays nil → the walk loop below is a no-op →
		// step (4) seeds from deps.harvester.snapshot() + deps.navHarv.snapshot(),
		// which hold the accumulated targets. A genuine config-vars topology change
		// resets attempt→0 (forgetScope in enqueueBootReDrive), taking the doWalk
		// branch above and re-walking the new set; the len(snapshot)==0 guard in
		// doWalk keeps the boot-race give-up→appear self-heal path re-walking.
		log.Info("prewarm.engine.boot.rewalk_reused",
			slog.String("subsystem", "cache"),
			slog.Int("attempt", attempt),
			slog.Int("harvested_widgets", len(deps.navHarv.snapshot())),
			slog.String("effect", "#135 Lever B — F.4 resume reused the harvester snapshot instead of "+
				"re-walking discovery (~255s/pass saved); config-vars redrive resets attempt→0 to force a fresh walk"),
		)
	}
	for _, root := range roots {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// #99b Fix 2 — advance the config-root index before descending this
		// root (mirrors the boot walk's resolver closure, phase1_walk.go:401).
		// Roots iterate in config.json order every pass, so this stamps
		// RootIndex 0 for the first (default-route/dashboard) subtree. Nil-safe.
		deps.navHarv.BeginRoot()
		// FRESH walker per root — new visited map (phase1_walk.go:679).
		// Harvesters SHARED BY REFERENCE so the re-walk's harvest lands in
		// the set the seed drains.
		//
		// Ship 0.30.232 — THE BUG SITE: prior six HARD REVERTs (0.30.226
		// → 0.30.231) all traced back to THIS literal omitting rc. Now
		// constructed via newPhase1Walker, which makes rc a REQUIRED
		// positional parameter — the compiler refuses a missing
		// argument. deps.saRC is the same SA *rest.Config the seed pass
		// uses (set once in Phase1Warmup; non-nil per the rePrewarmDeps
		// contract).
		w := newPhase1Walker(deps.saRC, deps.authnNS,
			withApiRefHarvester(deps.harvester),
			withNavWidgetHarvester(deps.navHarv),
		)
		// Same walk() entrypoint + same root tuple as resolveNavigationRoot
		// (page 1, perPage prewarmPageLimit(), key tuple (-1,-1)) — the
		// Change A page-number fix is preserved.
		if err := w.walk(rctx, root.Root, root.GVR, 0, 1, prewarmPageLimit(), -1, -1); err != nil {
			log.Warn("prewarm.engine.boot.root_rewalk_failed",
				slog.String("subsystem", "cache"),
				slog.String("root", rootKey(root.Root)),
				slog.Any("err", err),
			)
			continue
		}
		rewalked++
	}

	// 1.12.7 / #220 — same observation for the engine re-walk. A root that
	// failed above `continue`s without incrementing rewalked, so rewalked <
	// len(roots) is exactly the partial-pass case that must not alarm.
	recordWalkCoverage(rewalked, rewalked == len(roots))

	// ── (2) SETTLE the registered set once (single pass, not a loop) so a
	// CRD the re-walk discovered has its informer registered before the
	// seed reads RegisteredGVRs().
	settleRegisteredSet(ctx, deps.rw)

	// ── (3) BUILD the BindingsByGVR index over the navigated GVRs. Ship 1
	// builds it here on the engine path, AFTER the re-walk discovered the
	// navigated GVRs. The build is ONCE (Gate-2); steady-state maintenance
	// is the delta hooks (bindings_by_gvr_delta.go).
	navigated := deps.rw.RegisteredGVRs()
	enrolled := cache.BuildBindingsByGVRIndex(navigated)
	log.Info("prewarm.engine.boot.index_built",
		slog.String("subsystem", "cache"),
		slog.Int("navigated_gvrs", len(navigated)),
		slog.Int("bindings_enrolled", enrolled),
	)

	// ── (4) SEED per cohort with PER-TARGET-GVR scoping.
	restactionRefs := deps.harvester.snapshot()
	widgetEntries := deps.navHarv.snapshot()

	// Proactive composition-page RESTAction seed (Option A) — UNION the
	// RBAC-reachable RESTAction refs into the harvester snapshot so the
	// per-composition click-through detail RESTActions (never reached by
	// the nav walk — the harvester gap) are warmed at boot. Default-OFF
	// (PROACTIVE_RA_SEED_ENABLED); flag-off the union is a no-op and the
	// snapshot is harvester-only (F-6 transparent). The added refs flow
	// through the SAME seedScopeYielding loop — each ref is scoped to its
	// own per-binding targets via restActionTargetGVR + per-RA target GVR,
	// so the per-binding cell-key sharing invariant is unchanged.
	if ProactiveRASeedEnabled() {
		restactionRefs = unionProactiveRARefs(restactionRefs, deps.rw, log)
	}

	log.Info("prewarm.engine.boot.rewalk_complete",
		slog.String("subsystem", "cache"),
		slog.Int("roots", len(roots)),
		slog.Int("rewalked", rewalked),
		slog.Int("restactions", len(restactionRefs)),
		slog.Int("widgets", len(widgetEntries)),
		slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
	)

	// P0 — seed under the SA-credentialed rctx (NOT the bare engine ctx).
	// restActionTargetGVR's objects.Get must carry the SA identity or
	// filterGetByRBAC fail-closes on a missing identity (objects/get.go:99-141)
	// → returns (zero,false) for EVERY restaction → cohortsFor silently
	// reverts to the global 34-cohort set, defeating the per-target-GVR
	// scoping. rctx is derived from ctx so the engine's cancel/timeout still
	// propagates; withCohortSeedContext OVERRIDES identity per cohort for the
	// actual seed, so the SA base is correct for both the derivation fetch
	// and as the per-cohort seed base.
	if err := seedScopeYielding(rctx, restactionRefs, widgetEntries, deps.saEP, deps.saRC, deps.authnNS, mode); err != nil {
		return err
	}

	log.Info("prewarm.engine.boot.complete",
		slog.String("subsystem", "cache"),
		slog.String("scope", mode.String()),
		slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
	)
	return nil
}

// seedScopeYielding seeds the per-user top-level L1 for the harvested
// restactions + widgets, PER-TARGET-GVR-scoped per-binding targets,
// yielding to customer /call between targets. Replaces the pre-ship
// per-cohort fan-out with per-binding fan-out via
// cache.EnumeratePrewarmTargetsForGVR.
//
// The two loops are SYMMETRIC:
//   - widget loop: targets scoped on the widget's GVR (e.GVR).
//   - restaction loop: targets scoped on the RESTAction's TARGET GVR
//     (derived from the RA's userAccessFilter). The apiRef/RAFullList
//     layer is bounded identically (~3-6 targets, not the v3 global ~34
//     cohort universe).
//
// Ship 0.30.242 H.c-layered Phase 2b — the v3 global-cohort fallback
// (`globalCohorts := cache.EnumerateBindingSetClasses()`) is REMOVED.
// Design §22.1.A NACK item #2: when the per-GVR enumerator returns
// empty (genuinely no authorising bindings), the seed loop SKIPS that
// GVR rather than fall back to a global universe — a fallback would
// have included identities that can't authorise the GVR (wasted seed +
// cells under wrong identity).
//
// Runtime-discovered RA targets (ResourcesFrom — no static literal) are
// skipped by the seed engine here; the customer /call resolves them
// cold-then-warm via the on-demand dispatcher (helpers.go's
// dispatchCacheLookupKey populates the cell at first call).
//
// enumeratePrewarmTargetsForGVRFn / seedOneWidgetFn are test seams over the
// two external dependencies seedScopeYielding consumes — the per-binding
// target SOURCE (cache.EnumeratePrewarmTargetsForGVR) and the per-target
// seed PRIMITIVE (seedOneWidget). Same 1-LOC `var fooFn = foo` pattern as
// seedCohortFn (phase1_pip_seed.go:640) and enumerateAggregatePrewarmTargetsFn
// (phase1_pip_seed.go:404). The engine-path re-enqueue-latch falsifier
// (#316) swaps them to drive the widget seed loop's classifyEngineSeedErr
// branch + reEnqueued latch end-to-end without a live cache/RBAC snapshot
// or apiserver. Production ALWAYS uses the real functions; the restaction
// loop and restActionTargetGVR are left direct (the widget loop exercises
// the SAME shared classifier closure + latch — design §3.1).
var enumeratePrewarmTargetsForGVRFn = cache.EnumeratePrewarmTargetsForGVR

// seedOneWidgetFn is a test seam over seedOneWidget — see
// enumeratePrewarmTargetsForGVRFn.
var seedOneWidgetFn = seedOneWidget

// restActionTargetGVRFn / seedOneRestactionFn are the restaction-loop
// mirrors of the widget seams above — same 1-LOC `var fooFn = foo` pattern.
// #42 introduces them so the first-nav ORDERING falsifier can drive BOTH
// loops (a high-fan-out restaction + a nav widget) in ONE hermetic run
// without a live apiserver: restActionTargetGVRFn stands in for the
// objects.Get-backed restActionTargetGVR (no apiserver), seedOneRestactionFn
// stands in for the per-target seed primitive. Production ALWAYS uses the
// real functions.
var restActionTargetGVRFn = restActionTargetGVR
var seedOneRestactionFn = seedOneRestaction

// #42 FIX-E: the seedClass / seedClassOrderFn class-ORDER seam (whole-widgets-
// class-then-whole-restactions-class dispatch) was REMOVED here. FIX-E replaced
// the class-major model with a rank-major, class-INTERLEAVED loop
// (seedScopeYielding below: per identity rank → widgets(r) in NavOrder →
// restactions(r)), so there is no longer a "class order" to vary — the classes
// interleave per rank. The 3 falsifiers that drove seedClassOrderFn
// (FirstNavFirst / CheapCohortFirst restactions-sort / widgets-sort) were
// deleted-with-migration; their guarded properties are re-covered by the FIX-E
// interleave falsifier (see prewarm_engine_seed_order_test.go migration note +
// the regression journal). Deleting the seam also removes test-only prod code
// (the #66 shadow class).

// mode (seedScopeMode) selects BOTH the SWEEP-SET bound and the per-target skip
// predicate; it replaces the pre-c2 (rank1Only, bootScoped) bool pair (which
// admitted an illegal 4th state and scattered skip semantics — keepwarm c2
// §4.1):
//
//   - seedModeBoot: ALL ranks; the seed primitives apply the F.4 fresh-skip
//     (a live cell is done → not re-resolved), so a deadline-cut boot chunk's
//     continuation is cost-proportional. UNCHANGED from bootScoped=true.
//   - seedModeKeepwarm: the WIDGET-CAPABLE PREFIX of `ranked` (widgetMax>=1) —
//     a CONTIGUOUS PREFIX post-Fix-3 (widgetMax DESC is the primary rank key),
//     so the sweep-set is a pure LOOP BOUND (`break` at the first widgetMax==0
//     entry); the primitives apply the AGE-SKIP (keepwarm c2 §4.2). This
//     SUPERSEDES the c1 rank-1 bound: every login-cohort-shaped (widget-capable)
//     identity is swept, not just ranked[0], so the admin + narrow cohorts
//     (widget-capable but rank≥1) are covered — the c2 cohort-coverage fix.
//   - seedModeGVRDiscovered: ALL ranks; NO skip (the primitives re-resolve
//     already-warm cells to record the new-GVR dep edge — the S4 fix). UNCHANGED
//     from bootScoped=false / gvr-discovered.
//
// The skip decision lives INSIDE seedOneWidgetFn / seedOneRestactionFn (via the
// shared seedSkipDecision) so it consumes the EXACT key the Put would use
// (single derivation site, F4-C2a); mode threads straight through.
func seedScopeYielding(ctx context.Context,
	restactionRefs []templatesv1.ObjectReference, widgetEntries []navWidgetEntry,
	saEP endpoints.Endpoint, saRC *rest.Config, authnNS string, mode seedScopeMode) error {

	log := slog.Default()

	// #130 F4 — install ONE per-seed-pass RA-resolve memo on ctx for the whole
	// pass. Every per-cohort cohortCtx (withCohortSeedContext → BuildContext)
	// inherits it, so the ~71 statistics/tag widgets that share a heavy
	// RESTAction (compositions-list / dashboard-data, ~18s of gojq over the 60K
	// benchapps array) resolve it ONCE per distinct (RA, identity, page, extras)
	// rather than 2× per widget per cohort. The memo lives ONLY for this
	// function's scope — nothing references it once seedScopeYielding returns
	// (C-F4-5 teardown: the next pass builds a fresh one). Keyed by the full
	// RBAC identity so it is per-cohort correct (C-F4-4). Inert under
	// Disabled(): WithSeedResolveMemo returns ctx unchanged (C-F4-9). copyFn =
	// plumbing/maps.DeepCopyJSON (JSON-native round-trip on every hit, C-F4-3).
	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
	ctx = cache.WithSeedResolveMemo(ctx, memo)
	defer func() {
		h, m := memo.Stats()
		if h+m == 0 {
			return // never consulted (cache-off / no heavy widgets this pass)
		}
		log.Info("phase1.seed.memo.summary",
			slog.String("subsystem", "cache"),
			slog.String("mode", mode.String()),
			slog.Uint64("memo_hits", h),
			slog.Uint64("memo_misses", m),
			slog.String("effect", "F4 per-seed-pass RA-resolve memo — each hit skipped a full "+
				"gojq-over-benchapps re-resolve of a heavy RESTAction shared across statistics/tag widgets"),
		)
	}()

	// #132 F4b Lever A — the boot-scope "resolved-but-declined-external" marker set
	// is ENGINE-LIVED, NOT newed here. A boot RESUME is a FRESH seedScopeYielding
	// call (AddRateLimited requeue → processScope → rePrewarmBoot →
	// rePrewarmBootScoped → seedScopeYielding), so newing the set here would start
	// it empty every resume pass and the §3 whale loop (a CROSS-PASS defect) would
	// never break — the 2dc46ae inert-rework the arch caught. Instead the engine
	// holds one set per boot-scope-key, created once and REUSED across the scope's
	// requeues, installed onto ctx in processScope (prewarm_engine.go), cleared on
	// genuine boot completion + config-vars redrive. seedSkipDecision +
	// declineSeedPutOnError read/write it via cache.SeedDeclinedExternalSetFromContext;
	// it is nil off the boot path (keepwarm/gvr-discovered/cache-off never carry
	// one → strict no-op). The phase1.seed.declined_external.summary line is
	// intentionally NOT emitted here per-pass (that would report a partial
	// per-seedScopeYielding-pass count); it is emitted ONCE at teardown in
	// clearDeclinedExternalSet, reading the WHOLE-BOOT cumulative Marks() off the
	// engine-lived set (R4 cross-pass counter semantics).

	// CTX-CANCEL ABORT OBSERVABILITY (fold 2026-07-03, §4.3b — migrated from the
	// deleted seedCohort's 0.30.191 Fix-C `phase1.cohort.abort` reporter). The
	// engine seed's ctx-cancel exits (boot budget / pipCohortTimeout / process
	// shutdown) previously just `return ctx.Err()` with no greppable line, so a
	// post-deploy "did the seed finish or get cut off?" grep had nothing to key
	// on. emitSeedAbort logs a single greppable `prewarm.seed.abort` line with
	// the same load-bearing fields Fix-C carried: phase (which loop was cut),
	// cause (the ctx error), targets_processed, elapsed_ms. Best-effort +
	// log-only — the seed is background, an abort is never fatal.
	start := time.Now()
	targetsProcessed := 0
	emitSeedAbort := func(phase string, cause error) {
		log.Warn("prewarm.seed.abort",
			slog.String("subsystem", "cache"),
			slog.String("phase", phase),
			slog.Any("cause", cause),
			slog.Int("targets_processed", targetsProcessed),
			slog.Int64("elapsed_ms", time.Since(start).Milliseconds()),
			slog.String("effect", "seed cut off by ctx cancel/deadline (boot budget / pipCohortTimeout / "+
				"shutdown); background best-effort — remaining targets fall back to per-user resolve at /call time"),
		)
	}

	// #158 (design §1.4 + §1.5 engine path) — classify per-target seed
	// failures instead of swallowing them. RBAC-deny → Info + rbac_deny
	// counter (NO re-enqueue). Operational → Warn + operational counter.
	//
	// #105 (design §5) — the RE-ENQUEUE DECISION MOVED OUT of this classifier
	// to the END of the pass (finalizeBootReEnqueue). Instead of the old
	// immediate `enqueueScope` on the first operational failure, an operational
	// failure now RECORDS the failing target into this pass's failedSet
	// (kind+"/"+label — the SAME identity the Warn logs, dedup'd across the
	// per-cohort fan-out of one target). The tail then uses the set-delta
	// (seededSet grew OR failedSet shrank) to bound the re-walk across passes:
	// an UNBOUNDED per-pass re-enqueue of a DETERMINISTIC failure (a jq-eval
	// error that fails identically every pass) starved the admin-cohort seed
	// budget forever (the #105 marketplace-detail amplifier). Recording (not
	// re-enqueueing) here is what lets the tail see "same failing set, no new
	// seeded target → no progress → stop after bootMaxNoProgressPasses." The
	// back-compat grand total pipBindingSetSeedFailuresTotal is bumped for
	// parity (= rbac_deny + operational).
	failedSet := map[string]struct{}{}
	classifyEngineSeedErr := func(kind, label, target string, err error) {
		pipBindingSetSeedFailuresTotal.Add(1)
		if classifySeedErr(err) == seedFailRBACDeny {
			pipSeedRBACDenyTotal.Add(1)
			slog.Info("prewarm.engine.seed.expected_deny",
				slog.String("subsystem", "cache"),
				slog.String("kind", kind),
				slog.String("target", target),
				slog.String(kind, label),
				slog.Any("err", err),
			)
			return
		}
		// Operational (incl. fail-loud default). Record the failing target into
		// this pass's failedSet (kind+"/"+label — NOT cohort-scoped: a
		// deterministic failure fails for every cohort, and the human-readable
		// given_up_targets list wants one dedup'd entry per target, not N). The
		// tail owns the re-enqueue (finalizeBootReEnqueue).
		pipSeedOperationalFailTotal.Add(1)
		failedSet[kind+"/"+label] = struct{}{}
		slog.Warn("prewarm.engine.seed.operational_failure",
			slog.String("subsystem", "cache"),
			slog.String("kind", kind),
			slog.String("target", target),
			slog.String(kind, label),
			slog.Any("err", err),
			slog.String("effect", "operational seed failure (NOT an RBAC deny); recorded into the boot "+
				"failed-set — a coalesced boot re-walk is enqueued at the pass tail (dedup on key()==\"boot\") "+
				"UNLESS the #105 set-delta bound has tripped (deterministic failer, no forward progress)"),
		)
	}

	// finalizeBootReEnqueue is the #105 pass-tail re-enqueue decision. Called at
	// each CLEAN (nil-return) completion of the boot / gvr-discovered / keepwarm
	// seed loop — NEVER on a ctx-cut abort (that returns ctx.Err() → the F.4
	// AddRateLimited resume, untouched — and the classifier never fires on a
	// ctx-error target either, so failedSet holds only genuine operational
	// failures). It coalesces N operational failures in one pass to AT MOST ONE
	// boot enqueue (dedup on key()=="boot"), exactly as the old per-pass
	// reEnqueued latch did — but the re-enqueue is now GATED by the cross-pass
	// convergence bound when a boot-convergence state is installed:
	//
	//   - state != nil (BOOT scope): evaluateBootProgress applies the set-delta
	//     bound. Re-enqueue while consecutiveNoProgressPasses < bootMaxNoProgressPasses;
	//     once the bound trips, DO NOT re-enqueue and emit
	//     prewarm.engine.boot.converged_with_skips (the terminal pass already ran
	//     to completion; the caller returns nil → boot.complete→Forget, C-105-5).
	//   - state == nil (gvr-discovered / keepwarm / cache-off / the pure-unit
	//     seed tests): LEGACY behavior — re-enqueue a boot scope iff ≥1
	//     operational failure this pass. Byte-identical to the pre-#105
	//     per-pass latch for every non-boot path.
	//
	// The re-enqueue targets the engine that OWNS this scope: in production the
	// process singleton (state.engine == prewarmEngineSingleton() when installed
	// by processScope; the singleton directly off the boot path); under a
	// falsifier's real local worker, that test engine (so N real re-invocations
	// can be driven and the bound observed to STOP).
	finalizeBootReEnqueue := func(ctx context.Context) {
		st := bootConvergenceStateFromContext(ctx)
		if st == nil {
			// Legacy: re-enqueue once iff any operational failure this pass. Same
			// storm-bound as before (queue dedups on key()=="boot").
			if len(failedSet) > 0 {
				prewarmEngineSingleton().enqueueScope(prewarmScope{kind: scopeKindBoot})
			}
			return
		}
		reEnqueue, givenUp, passes := st.evaluateBootProgress(failedSet)
		st.reEnqueuedLastPass = reEnqueue // processScope reads this to gate teardown
		if reEnqueue {
			st.engine.enqueueScope(prewarmScope{kind: scopeKindBoot})
			return
		}
		if len(givenUp) > 0 {
			// The set-delta bound tripped: boot converges WITH these items given
			// up. WARN so the operator still sees the deterministic failer (the
			// per-target operational_failure Warn also fired every pass) — this
			// stops the re-walk AMPLIFICATION, not the visibility (design §5 reco).
			log.Warn("prewarm.engine.boot.converged_with_skips",
				slog.String("subsystem", "cache"),
				slog.Any("given_up_targets", givenUp),
				slog.Int("passes", passes),
				slog.String("effect", "#105 bound: after bootMaxNoProgressPasses consecutive re-walks with no "+
					"forward set-progress (seeded-set did not grow AND failed-set did not shrink), the boot scope "+
					"stops re-enqueueing itself. These targets are given up (a deterministic seed failure — e.g. a "+
					"portal-RA jq error — that would fail identically forever); the terminal pass runs to "+
					"boot.complete so the admin-cohort seed the loop was resetting finally lands. Given-up cells "+
					"stay cold and fall back to per-user resolve at /call."),
			)
		}
	}

	// targetsFor resolves the per-binding target set for a target GVR.
	// Empty when (a) the index is not built (pre-readiness), (b) no
	// binding authorises (gvr, list), or (c) haveGVR=false (runtime-
	// discovered target — handled on-demand at /call time, no global
	// fallback per design §22.1.A item #2). The bool reports whether the
	// result was index-scoped (telemetry).
	targetsFor := func(gvr schema.GroupVersionResource, haveGVR bool) ([]seedTarget, bool) {
		if !haveGVR {
			return nil, false
		}
		raw := enumeratePrewarmTargetsForGVRFn(gvr, "list")
		if len(raw) == 0 {
			return nil, false
		}
		out := make([]seedTarget, 0, len(raw))
		for _, t := range raw {
			out = append(out, seedTarget{
				BindingUID:        t.BindingUID,
				Username:          t.Subject.Username,
				Groups:            append([]string(nil), t.Subject.Groups...),
				CollapsedBindings: t.CollapsedBindings,
			})
		}
		return out, true
	}

	// seedOneTarget runs one (target, layer) seed under a per-target
	// timeout (pipCohortTimeout — matches seedCohort's stuck-cohort
	// guard) and the cohort seed context. The per-target seed primitive
	// (seedOneRestaction / seedOneWidget) is passed as a closure so the
	// restaction + widget loops share the timeout + yield + error-
	// containment wrapper.
	seedOneTarget := func(c seedTarget, do func(cohortCtx context.Context) error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		engineYieldCheckpoint(ctx)
		cctx, cancel := context.WithTimeout(ctx, pipCohortTimeout)
		defer cancel()
		cohortCtx := withCohortSeedContext(cctx, c, saEP, saRC)
		// #46: the footprint bound (semaphore admission + per-unit HeapInuse
		// assert) lives in the SHARED primitives seedOneWidget/seedOneRestaction
		// (after their identity short-circuit), NOT here — both the engine
		// (this) and the legacy errgroup path funnel through those primitives,
		// so bounding the shared mechanism covers both with ONE insertion
		// (feedback_no_special_cases). seedOneTarget stays a thin timeout+yield
		// wrapper.
		return do(cohortCtx)
	}

	// ── #42 FIX-E: IDENTITY-RANK-MAJOR, CLASS-INTERLEAVED, FIRST-NAV-ORDERED seed.
	//
	// Precompute every widget's + restaction's per-binding target set once, then
	// seed RANK-MAJOR across BOTH classes: for each identity rank r (descending
	// dedup collapsed-binding count) → widgets(r) in FIRST-NAV WALK ORDER, then
	// restactions(r). This supersedes FIX-D (rank-major widgets-only) + Fix-A2
	// (within-rank count-sort): count≠cost was proven twice (A2 seeded a cheap
	// widget before the dashboard's own widgets), so the within-rank WIDGET order
	// is now the walk-derived NavOrder (roots in config.json order, depth-first —
	// dashboard/default-route subtree first). Interleaving restactions INTO each
	// rank (instead of after ALL widget ranks) is why restactions finally seed:
	// rank-1's RAs run right after rank-1's widgets, before rank-2 work. Within a
	// rank the restactions keep the ascending-len(targets) tiebreak (cheap RAs
	// before the high-fan-out apps/deployments tail).
	//
	// PURE ORDERING: the (unit×identity) seed SET is unchanged — same targetsFor,
	// same primitives, same enterSeedUnit per-unit bound; only the SEQUENCE. No
	// caps/skips/magic numbers, no static/name literals (rank metric =
	// CollapsedBindings; widget order = NavOrder; RA tiebreak = len+ns/name).
	type widgetSeed struct {
		e       navWidgetEntry
		targets []seedTarget
		scoped  bool
	}
	type restactionSeed struct {
		ref       templatesv1.ObjectReference
		targetGVR schema.GroupVersionResource
		targets   []seedTarget
		scoped    bool
	}

	// identityKey mirrors the dedup key (Username + US + sorted Groups) so an
	// identity is the SAME rank unit across every widget AND restaction.
	identityKey := func(c seedTarget) string {
		g := append([]string(nil), c.Groups...)
		sort.Strings(g)
		return c.Username + "\x1f" + strings.Join(g, "\x1f")
	}

	// (1) Precompute widget target sets (yield/abort-aware).
	widgetSeeds := make([]widgetSeed, 0, len(widgetEntries))
	for _, e := range widgetEntries {
		if ctx.Err() != nil {
			emitSeedAbort("widgets", ctx.Err())
			return ctx.Err()
		}
		engineYieldCheckpoint(ctx)
		targets, scoped := targetsFor(e.GVR, true)
		widgetSeeds = append(widgetSeeds, widgetSeed{e: e, targets: targets, scoped: scoped})
	}
	// FIRST-NAV WALK ORDER within rank (FIX-E, replaces the A2 count-sort):
	// ascending NavOrder, ties on ns/name for determinism.
	sort.SliceStable(widgetSeeds, func(i, j int) bool {
		if widgetSeeds[i].e.NavOrder != widgetSeeds[j].e.NavOrder {
			return widgetSeeds[i].e.NavOrder < widgetSeeds[j].e.NavOrder
		}
		ei, ej := widgetSeeds[i].e, widgetSeeds[j].e
		return ei.W.GetNamespace()+"/"+ei.W.GetName() < ej.W.GetNamespace()+"/"+ej.W.GetName()
	})

	// (2) Precompute restaction target sets (yield/abort-aware); ascending
	// len(targets) tiebreak within rank (cheap RAs before the fan-out tail).
	restactionSeeds := make([]restactionSeed, 0, len(restactionRefs))
	for _, ref := range restactionRefs {
		if ctx.Err() != nil {
			emitSeedAbort("restactions", ctx.Err())
			return ctx.Err()
		}
		engineYieldCheckpoint(ctx)
		targetGVR, haveTarget := restActionTargetGVRFn(ctx, ref)
		targets, scoped := targetsFor(targetGVR, haveTarget)
		restactionSeeds = append(restactionSeeds, restactionSeed{
			ref: ref, targetGVR: targetGVR, targets: targets, scoped: scoped,
		})
	}
	sort.SliceStable(restactionSeeds, func(i, j int) bool {
		if len(restactionSeeds[i].targets) != len(restactionSeeds[j].targets) {
			return len(restactionSeeds[i].targets) < len(restactionSeeds[j].targets)
		}
		ri, rj := restactionSeeds[i].ref, restactionSeeds[j].ref
		return ri.Namespace+"/"+ri.Name < rj.Namespace+"/"+rj.Name
	})

	// (3) FIX-3 RANK HYGIENE: rank the identities over BOTH classes' targets by
	// (widgetMax DESC, allMax DESC, identityKey ASC), where both counts are
	// MAX-FOLDS over every observation of the identity across the precomputed
	// target sets. This supersedes the FIX-D first-seen CollapsedBindings rank
	// (docs/fix3-rank-hygiene-design-2026-07-07.md):
	//
	//   - DETERMINISTIC. The old noteIdentity kept the FIRST-SEEN count (a
	//     map-iteration-order-dependent value: an identity carries a different
	//     count per GVR bucket, and restaction refs iterate a Go-map snapshot
	//     upstream), so ranked order flipped across boots. max is commutative/
	//     associative/idempotent → the fold is a pure function of the
	//     observation multiset, independent of widgetSeeds/restactionSeeds order
	//     and of Go map order. With the full-key comparator there is exactly one
	//     ranked order per (harvest, index) snapshot.
	//   - WIDGET-CAPABLE-FIRST as a tier, without a tier flag. CollapsedBindings
	//     is >=1 for every observation (prewarm_enumeration.go:207), so
	//     widgetMax>=1 IFF the identity appears in some widget target set.
	//     Sorting widgetMax DESC alone places every widget-capable identity
	//     strictly above every widget-less one (widgetMax==0). This fixes the
	//     boot600 pollution: a machine SA present ONLY in a 1,344-binding RA
	//     bucket (widgetMax 0) no longer out-ranks a login cohort's dashboard
	//     widgets — the prime seed slot goes to an identity that renders a page.
	//   - allMax is the secondary key: it orders the widget-less tail and breaks
	//     widget-count ties inside the widget-capable tier by breadth elsewhere.
	//     Genuine ties fall to identityKey ASC (total, deterministic, no
	//     starvation).
	//
	// PURE ORDERING (FIX-E invariant preserved): the seeded (unit x identity)
	// SET is unchanged — widget-less identities keep their RA seeds, ranked at
	// the tail; only the SEQUENCE changes. No caps/skips/static lists; the rank
	// metric stays data-derived CollapsedBindings.
	type rankedIdentity struct {
		key       string
		widgetMax int
		allMax    int
	}
	rankOf := map[string]rankedIdentity{}
	noteIdentity := func(c seedTarget, isWidget bool) {
		k := identityKey(c)
		ri := rankOf[k] // zero value {key:"", 0, 0} for a first observation
		ri.key = k
		if c.CollapsedBindings > ri.allMax {
			ri.allMax = c.CollapsedBindings
		}
		if isWidget && c.CollapsedBindings > ri.widgetMax {
			ri.widgetMax = c.CollapsedBindings
		}
		rankOf[k] = ri
	}
	for _, ws := range widgetSeeds {
		for _, c := range ws.targets {
			noteIdentity(c, true)
		}
	}
	for _, rs := range restactionSeeds {
		for _, c := range rs.targets {
			noteIdentity(c, false)
		}
	}
	// #130 F3b-r2 — the RootIndex==0 firstNavReachable rank tier is DELETED. The
	// seed ORDER is now a NavOrder total order (below), not a rank tier keyed on
	// the config-root==0 boolean partition (Diego's no-special-case ruling: a
	// data-order, never a page/route/dashboard concept). `ranked` keeps its
	// (widgetMax DESC, allMax DESC, key ASC) order — it survives ONLY as the
	// NavOrder tie-break (cohortRankIndex) so devs still slightly precede admins
	// at equal NavOrder, and as the keepwarm widget-capable-prefix loop bound.
	ranked := make([]rankedIdentity, 0, len(rankOf))
	for _, ri := range rankOf {
		ranked = append(ranked, ri)
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].widgetMax != ranked[j].widgetMax {
			return ranked[i].widgetMax > ranked[j].widgetMax
		}
		if ranked[i].allMax != ranked[j].allMax {
			return ranked[i].allMax > ranked[j].allMax
		}
		return ranked[i].key < ranked[j].key
	})
	// Ride-along observability (boot scope only): one line per ranked identity
	// so two consecutive boots' rank order can be diffed clean (R3-C8 d).
	// Suppressed for keepwarm to keep the sweep logs quiet (it runs on a cadence).
	if mode != seedModeKeepwarm {
		for r := range ranked {
			log.Info("prewarm.engine.seed.rank",
				slog.String("subsystem", "cache"),
				slog.Int("rank", r),
				slog.String("identity", ranked[r].key),
				slog.Int("widget_max", ranked[r].widgetMax),
				slog.Int("all_max", ranked[r].allMax),
			)
		}
	}

	// Emit per-unit target-count telemetry ONCE up front (the boot/gvr-discovered
	// seed loop below is NavOrder-flat and the keepwarm loop is rank-major, so the
	// *_targets line no longer pairs 1:1 with a contiguous per-unit block). The
	// root_index field is retained as a dead-stamp observability field only — no
	// seed-ordering or latch branch keys on it after F3b-r2 (design C-r2-6).
	for _, ws := range widgetSeeds {
		log.Info("prewarm.engine.seed.widget_targets",
			slog.String("subsystem", "cache"),
			slog.String("widget", ws.e.W.GetNamespace()+"/"+ws.e.W.GetName()),
			slog.String("gvr", ws.e.GVR.String()),
			slog.Bool("scoped", ws.scoped),
			slog.Int("targets", len(ws.targets)),
			slog.Int("nav_order", ws.e.NavOrder),
			slog.Int("root_index", ws.e.RootIndex),
		)
	}
	for _, rs := range restactionSeeds {
		log.Info("prewarm.engine.seed.restaction_targets",
			slog.String("subsystem", "cache"),
			slog.String("restaction", rs.ref.Namespace+"/"+rs.ref.Name),
			slog.String("target_gvr", rs.targetGVR.String()),
			slog.Bool("scoped", rs.scoped),
			slog.Int("targets", len(rs.targets)),
		)
	}

	// cohortAttempted counts per-cohort targets attempted (widgets + restactions)
	// for the keepwarm cohort_summary line (below). Incremented in the two
	// seed-target closures; snapshotted + reset at each cohort boundary. Safe as
	// a plain int: the keepwarm seed loop runs SERIALLY (one resolve in flight,
	// WithPrewarmIterSerial; engineYieldCheckpoint only DEFERS, never parallelises)
	// so there is no concurrent writer.
	cohortAttempted := 0

	// seedWidgetTarget / seedRestactionTarget — the per-target seed bodies,
	// shared by the rank-major loop. Each returns (abort bool, err) where abort
	// signals a ctx-cancel that must stop the whole seed.
	seedWidgetTarget := func(e navWidgetEntry, c seedTarget) bool {
		if ctx.Err() != nil {
			emitSeedAbort("widgets", ctx.Err())
			return true
		}
		engineYieldCheckpoint(ctx)
		err := seedOneTarget(c, func(cohortCtx context.Context) error {
			return seedOneWidgetFn(cohortCtx, e, authnNS, mode)
		})
		if err != nil && ctx.Err() != nil {
			emitSeedAbort("widgets", ctx.Err())
			return true
		}
		if err != nil {
			classifyEngineSeedErr("widget", e.W.GetNamespace()+"/"+e.W.GetName(), cohortLogLabel(c), err)
		}
		targetsProcessed++
		cohortAttempted++
		return false
	}
	seedRestactionTarget := func(ref templatesv1.ObjectReference, c seedTarget) bool {
		if ctx.Err() != nil {
			emitSeedAbort("restactions", ctx.Err())
			return true
		}
		engineYieldCheckpoint(ctx)
		err := seedOneTarget(c, func(cohortCtx context.Context) error {
			return seedOneRestactionFn(cohortCtx, cohortLogLabel(c), ref, authnNS, mode)
		})
		if err != nil && ctx.Err() != nil {
			emitSeedAbort("restactions", ctx.Err())
			return true
		}
		if err != nil {
			classifyEngineSeedErr("restaction", ref.Namespace+"/"+ref.Name, cohortLogLabel(c), err)
		}
		targetsProcessed++
		cohortAttempted++
		return false
	}

	// ── #99 FIX-F / #130 F3b-r2: FIRST-NAV READYZ LATCH ARMING. ──
	// The readyz gate flips when EVERY cohort's NAV WIDGETS have seeded — NOT the
	// whole seed (the RA content tail is excluded), NOT just the config-root==0
	// (dashboard) subtree (the deleted RootIndex latch). F3b-r2 replaces the
	// RootIndex==0 partition with the widget-vs-RA CLASS boundary — a structural
	// fact (a target either IS or ISN'T a nav widget), never a page/route/
	// dashboard concept (Diego's no-special-case ruling applied to the latch too,
	// design §3 "the latch"). Post-Fix-2 every nav widget is cheap (whale LISTs
	// serve from the synced informer), so waiting for ALL of them (not just
	// root-0) costs little and removes the special-case: it is a STRICT
	// GENERALIZATION of the old latch.
	//
	// navWidgetRemaining = the total count of (widget × cohort) units in the flat
	// NavOrder seed list (== len(flat) built below). The latch fires the instant
	// the LAST nav-widget unit of ANY cohort seeds (remaining==0) — i.e. "every
	// cohort's nav widgets are warm; the RA-backed content tail warms in
	// background." The MarkPhase1Done / PHASE1_TIMEOUT backstop is UNCHANGED (C2
	// liveness — a pathological widget cannot hang readiness forever).
	//
	// BOOT-ONLY. The latch singleton is armed only at boot (built by engineSeed
	// before the boot enqueue). In keepwarm/gvr-discovered re-walks currentFirstNav
	// Latch() returns the already-fired singleton, so every fire() call is a
	// sync.Once no-op with zero readiness value — and the keepwarm mode keeps its
	// own rank-major loop below, which never touches this latch. The latch is nil
	// under the pure-unit seed tests that call seedScopeYielding directly (no
	// engineSeed wrapper built it) → all fireFirstNav calls are nil-safe no-ops.
	latch := currentFirstNavLatch()
	latchStart := time.Now()
	fireFirstNav := func(reason string, navWidgets, unitsSeeded int) {
		if latch != nil {
			// segIdentity/segRank are no longer segment-scoped (the latch keys on
			// the whole nav-widget class, not one cohort's RootIndex==0 subtree):
			// segRank=-1 marks "not segment-scoped". navWidgets = the distinct nav
			// widgets across all cohorts; unitsSeeded = the total (widget × cohort)
			// units seeded when the latch fired.
			latch.fire(reason, navWidgets, unitsSeeded, "", -1, time.Since(latchStart))
		}
	}

	if mode == seedModeKeepwarm {
		// ── keepwarm: UNCHANGED rank-major loop (arch ruling B). ──
		// The keepwarm sweep set is the WIDGET-CAPABLE PREFIX of `ranked` (every
		// identity with widgetMax>=1). widgetMax DESC is the primary rank key
		// (Fix-3), so the widget-capable identities are a CONTIGUOUS PREFIX and the
		// sweep set is a pure LOOP BOUND: break at the first widgetMax==0 entry (the
		// widget-less RA-only tail, not covered by keepwarm). This break is
		// RootIndex-INDEPENDENT and load-bearing; the per-cohort cohort_summary
		// emission is a PM probe keyed on the cohort-rank boundary. Both are kept
		// verbatim. The latch is already fired at boot, so keepwarm arms/fires
		// nothing here.
		for ri := range ranked {
			if ranked[ri].widgetMax == 0 {
				break
			}
			rankKey := ranked[ri].key
			// keepwarm cohort_summary (PM probe (b), design §9): snapshot the
			// age-skip counter + attempted count at this cohort's start so the
			// per-cohort delta is exact. The keepwarm seed loop is the SOLE
			// keepwarm-mode caller and runs SERIALLY (one resolve in flight,
			// WithPrewarmIterSerial), so keepwarmAgeSkipTotal's delta across this
			// cohort's targets is precisely this cohort's age-skips.
			cohortAgeSkipStart := keepwarmAgeSkipTotal.Load()
			cohortAttempted = 0
			for _, ws := range widgetSeeds {
				e := ws.e
				for _, c := range ws.targets {
					if identityKey(c) != rankKey {
						continue
					}
					if seedWidgetTarget(e, c) {
						return ctx.Err()
					}
				}
			}
			for _, rs := range restactionSeeds {
				ref := rs.ref
				for _, c := range rs.targets {
					if identityKey(c) != rankKey {
						continue
					}
					if seedRestactionTarget(ref, c) {
						return ctx.Err()
					}
				}
			}
			// keepwarm cohort_summary (PM probe (b), design §9): ONE INFO line per
			// SWEPT cohort per sweep cycle — {identity, reseeds, age_skips}.
			ageSkips := keepwarmAgeSkipTotal.Load() - cohortAgeSkipStart
			reseeds := int64(cohortAttempted) - int64(ageSkips)
			if reseeds < 0 {
				reseeds = 0 // defensive: attempted is the loop's own count, cannot underflow in practice
			}
			log.Info("prewarm.keepwarm.cohort_summary",
				slog.String("subsystem", "cache"),
				slog.String("identity", rankKey),
				slog.Int("widget_max", ranked[ri].widgetMax),
				slog.Int64("reseeds", reseeds),
				slog.Int64("age_skips", int64(ageSkips)),
				slog.Int("attempted", cohortAttempted),
				slog.String("effect", "keepwarm sweep re-Put this cohort's quiet cells (reseeds) and elided its "+
					"young/churny cells (age_skips); one line per swept cohort per cycle (probe b)"),
			)
		}
		// #105: keepwarm carries no boot-convergence state → LEGACY re-enqueue
		// (re-enqueue a boot scope iff ≥1 operational failure this sweep). Byte-
		// identical to the pre-#105 per-pass reEnqueued latch for keepwarm.
		finalizeBootReEnqueue(ctx)
		return nil
	}

	// ── boot / gvr-discovered: NavOrder-FLAT seed (arch ruling B). ──
	// A SINGLE flat pass over ALL (widget × cohort) units, sorted by NavOrder ASC
	// (walk-discovery order = nearest-to-nav-entry first; the dashboard is the
	// low-NavOrder prefix by DATA, not by branch). PURE ORDERING (FIX-E
	// invariant): the seeded (unit × identity) SET is unchanged vs the old
	// rank-major loop; only the SEQUENCE changes from rank-major to NavOrder-major.
	// No caps/skips/static lists; no RootIndex.
	//
	// cohortRankIndex maps each ranked cohort's key → its index in `ranked`
	// (widgetMax DESC, allMax DESC, key ASC). It is the NavOrder tie-break so
	// equal-NavOrder widgets across cohorts interleave in the existing rank order
	// (devs before admins before masters), preserving largest-cohort-first WITHIN
	// a nav position without any special-case. A cohort that is NOT in `ranked`
	// (widget-less RA-only identity) has no widget units, so it never enters the
	// flat list; its RA seeds land in the RA tail below (unchanged).
	cohortRankIndex := make(map[string]int, len(ranked))
	for r := range ranked {
		cohortRankIndex[ranked[r].key] = r
	}
	type seedUnit struct {
		navOrder  int
		rankIndex int
		wsNSName  string
		e         navWidgetEntry
		target    seedTarget
	}
	flat := make([]seedUnit, 0)
	for _, ws := range widgetSeeds {
		nsName := ws.e.W.GetNamespace() + "/" + ws.e.W.GetName()
		for _, c := range ws.targets {
			ri, ok := cohortRankIndex[identityKey(c)]
			if !ok {
				// Not a ranked cohort — cannot happen for a widget target (every
				// widget target's identity is noted into rankOf above), but guard
				// so an unranked identity is never silently dropped from the SET.
				ri = len(ranked)
			}
			flat = append(flat, seedUnit{
				navOrder:  ws.e.NavOrder,
				rankIndex: ri,
				wsNSName:  nsName,
				e:         ws.e,
				target:    c,
			})
		}
	}
	sort.SliceStable(flat, func(i, j int) bool {
		if flat[i].navOrder != flat[j].navOrder {
			return flat[i].navOrder < flat[j].navOrder // (1) NavOrder ASC
		}
		if flat[i].rankIndex != flat[j].rankIndex {
			return flat[i].rankIndex < flat[j].rankIndex // (2) cohort rank ASC
		}
		return flat[i].wsNSName < flat[j].wsNSName // (3) ns/name ASC (determinism)
	})

	// Arm the all-nav-widget latch on the flat unit count. distinctNavWidgets is
	// the number of distinct nav widgets (for the fire-log observability field).
	navWidgetRemaining := len(flat)
	distinctNavWidgets := len(widgetSeeds)
	navUnitsTotal := navWidgetRemaining
	if navWidgetRemaining == 0 {
		// Provably-empty: no nav-widget units at all (all-tail topology, or the
		// walk reached no widget). There is nothing to warm on the nav-widget
		// path → fire so the latch never hangs to the PHASE1_TIMEOUT backstop.
		fireFirstNav("zero-nav-widgets", 0, 0)
	}

	// Seed the flat list in NavOrder order. SAME primitive (seedWidgetTarget),
	// SAME abort/yield semantics (return ctx.Err() on abort). Fire the latch the
	// instant the LAST nav-widget unit is PROCESSED (remaining==0) — before the
	// RA content tail runs, so readyz does not wait on the RA whale-fanout.
	// PROCESSED, NOT SUCCEEDED (C2 liveness, unchanged): a per-target failure is
	// classified+swallowed inside seedWidgetTarget, so the count still decrements
	// — a permanently-failing widget must not hang /readyz to the backstop.
	for i := range flat {
		if seedWidgetTarget(flat[i].e, flat[i].target) {
			return ctx.Err()
		}
		navWidgetRemaining--
		if navWidgetRemaining == 0 {
			fireFirstNav("segment-complete", distinctNavWidgets, navUnitsTotal)
		}
	}

	// RA tail — RAs carry no NavOrder (they are the background content layer, not
	// nav widgets); they seed AFTER the whole widget list in their existing
	// ascending-len order (restactionSeeds was sorted ascending-len above). The
	// tail is explicitly excluded from readiness (the latch already fired). Seed
	// rank-major across `ranked` so a lower-ranked cohort's RAs never precede a
	// higher-ranked cohort's — the RA within-class rank order is preserved.
	for ri := range ranked {
		rankKey := ranked[ri].key
		for _, rs := range restactionSeeds {
			ref := rs.ref
			for _, c := range rs.targets {
				if identityKey(c) != rankKey {
					continue
				}
				if seedRestactionTarget(ref, c) {
					return ctx.Err()
				}
			}
		}
	}
	// #105: the pass reached its CLEAN tail (no ctx-cut abort). Decide the
	// re-enqueue via the set-delta bound (boot scope) or the legacy per-pass
	// latch (gvr-discovered / cache-off / pure-unit tests). NIL return through
	// the existing boot.complete→Forget path either way (C-105-5) — never an
	// early-error-return that would divert to the scope_incomplete/AddRateLimited
	// door and re-open the loop.
	finalizeBootReEnqueue(ctx)
	return nil
}

// unionProactiveRARefs unions the RBAC-reachable RESTAction refs
// (cache.RBACReachableRestActionRefs — Option A: the RESTAction CRs some
// published binding grants `get` on, intersected with the boot-anchored
// RESTActions informer) into the nav-walk harvester snapshot, deduped by
// {ns,name} EXACTLY as the harvester dedups (phase1_content_prewarm.go).
//
// The proactive refs carry the SAME fixed RESTAction GVR coordinates the
// harvester writes (restActionGVR group/version/resource), so the
// downstream seedScopeYielding loop + objects.Get treat them identically.
//
// SEED-TARGETING ONLY (no special-case): the source is purely
// RBAC-derived (zero resource/name/path literal). Over-inclusion = wasted
// seed (benign); the per-request authz boundary is unchanged.
func unionProactiveRARefs(harvested []templatesv1.ObjectReference,
	rw *cache.ResourceWatcher, log *slog.Logger) []templatesv1.ObjectReference {

	proactive := cache.RBACReachableRestActionRefs(rw)
	if len(proactive) == 0 {
		log.Info("prewarm.engine.boot.proactive_ra_seed",
			slog.String("subsystem", "cache"),
			slog.Int("harvested", len(harvested)),
			slog.Int("proactive_reachable", 0),
			slog.Int("added", 0),
			slog.String("note", "no RBAC-reachable RESTAction refs (no binding grants get on restactions, or informer empty) — seed source unchanged"),
		)
		return harvested
	}

	out := unionRefsForTest(harvested, proactive)

	log.Info("prewarm.engine.boot.proactive_ra_seed",
		slog.String("subsystem", "cache"),
		slog.Int("harvested", len(harvested)),
		slog.Int("proactive_reachable", len(proactive)),
		slog.Int("added", len(out)-len(harvested)),
		slog.String("note", "RBAC-reachable RESTAction refs unioned into the boot seed source (Option A); the per-composition detail RESTActions the nav walk never reaches are now seeded"),
	)
	return out
}

// unionRefsForTest is the pure dedup core of unionProactiveRARefs — it
// appends the proactive refs (built as RESTAction ObjectReferences with the
// fixed GVR coordinates the harvester uses) onto the harvested slice, deduped
// by {ns,name} EXACTLY as the harvester dedups
// (phase1_content_prewarm.go). RBAC-source-independent so a falsifier can
// exercise the dedup without a live cluster. Named *ForTest because the
// falsifier is its only direct external caller; production reaches it through
// unionProactiveRARefs.
func unionRefsForTest(harvested []templatesv1.ObjectReference,
	proactive []cache.RestActionRef) []templatesv1.ObjectReference {

	seen := make(map[string]struct{}, len(harvested)+len(proactive))
	out := make([]templatesv1.ObjectReference, 0, len(harvested)+len(proactive))
	for _, r := range harvested {
		key := r.Namespace + "/" + r.Name
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, r)
	}
	for _, p := range proactive {
		key := p.Namespace + "/" + p.Name
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: p.Name, Namespace: p.Namespace},
			APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
			Resource:   restActionGVR.Resource,
		})
	}
	return out
}

// restActionTargetGVR derives the GVR a RESTAction LISTs from its
// userAccessFilter (verb/group/resource) — the no-special-case signal of
// what GVR the RA gates LIST on (e.g. compositions-panels' RA has
// userAccessFilter:{group:composition.krateo.io, resource:compositions}).
// Returns (gvr, true) when a static {group, resource} is declared on any
// api stanza; (zero, false) for a runtime-discovered target
// (ResourcesFrom — no static literal) or no userAccessFilter (the caller
// falls back to the global cohort set).
//
// The version is left "" — the BindingsByGVR index keys on {group,
// resource} (RBAC rules carry no version), so the version is irrelevant
// to cohort scoping.
func restActionTargetGVR(ctx context.Context, ref templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
	got := objects.Get(ctx, ref)
	if got.Err != nil || got.Unstructured == nil {
		return schema.GroupVersionResource{}, false
	}
	var cr templatesv1.RESTAction
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(got.Unstructured.Object, &cr); err != nil {
		return schema.GroupVersionResource{}, false
	}
	// Pick the first api stanza declaring a static userAccessFilter
	// resource. Multiple stanzas would each have their own target; the
	// dominant LIST target (the one the apiRef/RAFullList layer bounds on)
	// is the one carrying a userAccessFilter — they share the cohort set
	// because cohorts union over the bindings, and the index is scoped per
	// {group,resource}. Deterministic: sort stanzas by name first.
	apis := append([]*templatesv1.API(nil), cr.Spec.API...)
	sort.Slice(apis, func(i, j int) bool { return apis[i].Name < apis[j].Name })
	for _, a := range apis {
		if a == nil || a.UserAccessFilter == nil {
			continue
		}
		if a.UserAccessFilter.Resource == "" {
			// Runtime-discovered (ResourcesFrom) — no static literal to
			// scope on. Fall back to the global cohort set.
			continue
		}
		return schema.GroupVersionResource{
			Group:    a.UserAccessFilter.Group,
			Resource: a.UserAccessFilter.Resource,
		}, true
	}
	return schema.GroupVersionResource{}, false
}
