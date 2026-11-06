// prewarm.go — Tag 0.30.99 (Tag B of the unified prewarm+pivot-
// servability design): the startup navigation GVR-walk.
//
// Tag B premise (resting on Tag A): the resolver pivot can only serve a
// GVR whose informer is registered. Today the registered set at boot is
// just the four RBAC bootstrap GVRs (NewResourceWatcher) — every other
// GVR is registered lazily on first navigation touch (the resolver
// inner-call path `lazyRegisterInnerCallPaths`, the dispatcher
// dep-record path `ensureWatcherInformerForGVR`). That lazy path is
// correct but means the FIRST request for each GVR pays a cold informer
// (servable=false → apiserver fallthrough) until its informer syncs.
//
// PrewarmRegisterFromNavigation closes that cold window: at boot it
// walks the cluster's RESTAction inventory ONCE, derives the GVR set the
// navigation surface reaches (`spec.api[*].path` → GVR), and registers
// an informer for each via EnsureResourceType. By first user login the
// navigated set is registered and likely synced, so the pivot serves
// from a warm informer instead of falling through.
//
// CRITICAL — feedback_no_special_cases.md: there is ZERO hardcoded
// business-GVR list here. The GVR set is whatever
// CollectResourceTypesFromRestActions derives from the cluster's
// RESTActions — pure navigation-derived discovery. The only constant is
// the `restActionGVR` anchor needed to LIST RESTActions in the first
// place (the same anchor the inventory walker already uses) — that is
// the bare meta-query seed, not a per-resource policy.
//
// IMPLICIT-ON under CACHE_ENABLED (#130 1.7.5) — resolved by
// resolvePrewarmRegisterDefault (main.go): the walk runs by default
// whenever the cache subsystem is on; only an explicit
// PREWARM_REGISTER_ENABLED=false opts out. This inverts the pre-1.7.5
// default-OFF gate. The inversion is safe because the historical
// default-OFF rationale was TRACED to be inaccurate:
//
//   - The walk registers ONLY the STATIC (non-JQ-templated) inventory
//     GVR subset. CollectResourceTypesFromRestActions derives GVRs from
//     spec.api[*].path via ParseAPIServerPathToGVR, which REJECTS any
//     path carrying an unresolved `${...}` JQ template (inventory.go:171).
//     Every composition.krateo.io path is JQ-templated, so the walk
//     CANNOT register composition GVRs — the walk's set is the trivial
//     static/CRD-meta class, NOT the "same 58 GVRs" earlier comments
//     claimed. (The lazy dispatch path evaluates the JQ and derives the
//     composition GVR at request time; the eager walk cannot.)
//
//   - OOM-at-50K is NOT re-armed by this flag. The composition informers
//     (the OOM-relevant mass) are registered LAZILY via the resolver
//     inner-call DiscoverGroupResources / EnsureResourceType hook
//     (resolve.go) REGARDLESS of PREWARM_REGISTER_ENABLED. Flipping this
//     flag neither adds nor removes composition informers, so it does not
//     touch the 0.30.8/0.30.92 RSS surface at all.
//
//   - The 0.30.6/0.30.61 "no consumer reads the eagerly-registered
//     informers = pure apiserver overhead" note is OBSOLETE. Consumers
//     now exist: the #130 F1 confirm-prime batch (below) makes the
//     walk-registered GVRs conjunct-4 servable, the informer-serve branch
//     reads them, and the Phase 1 seed replays navigation against them
//     (#57: the pivot is implicit-on-cache; there is no separate
//     RESOLVER_USE_INFORMER flag).
//
// FIRE-AND-FORGET (the distinction from EagerRegisterAll): the walk
// calls EnsureResourceType per GVR and returns immediately — it does NOT
// WaitForCacheSync over the inventory. Each informer's initial LIST runs
// asynchronously on its own goroutine (the standard EnsureResourceType
// late-registration branch), so the walk adds no blocking bulk-sync
// startup-latency burst.

package cache

import (
	"context"
	"log/slog"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// PrewarmRegisterFromNavigation walks the cluster's RESTAction inventory
// and registers an informer for every GVR the navigation surface
// reaches. Fire-and-forget: it returns once every GVR has been handed to
// EnsureResourceType — it does NOT block on informer sync.
//
// Returns the number of distinct GVRs the walk registered (added=true
// from EnsureResourceType) and the number it found already registered
// (added=false — e.g. a GVR that overlaps the RBAC bootstrap set, or a
// duplicate within the inventory). A walk-error (LIST failure) is
// returned to the caller; the caller treats it as soft (log + continue,
// the lazy register-on-navigation fallback still covers every GVR on
// first request).
//
// Nil-receiver / passthrough / cache-disabled are no-ops: there is no
// informer factory to register against. dynClient must be the same
// dynamic client used to build the watcher.
//
// Concurrency: EnsureResourceType is idempotent + singleflighted under
// rw.mu; calling it once per inventory GVR here, and again later from
// the lazy paths, is safe — the later calls observe added=false.
func (rw *ResourceWatcher) PrewarmRegisterFromNavigation(ctx context.Context, dynClient dynamic.Interface) (registered, alreadyPresent int, err error) {
	if rw == nil {
		return 0, 0, nil
	}
	if rw.mode == modePassthrough {
		// No informers in passthrough mode — every read is apiserver-
		// routed, so there is nothing to prewarm.
		slog.Info("cache.prewarm.skipped",
			slog.String("subsystem", "cache"),
			slog.String("reason", "passthrough mode — no informer factory"),
		)
		return 0, 0, nil
	}
	if dynClient == nil {
		slog.WarnContext(ctx, "cache.prewarm.skipped",
			slog.String("subsystem", "cache"),
			slog.String("reason", "nil dynamic client"),
		)
		return 0, 0, nil
	}

	start := time.Now()

	// Navigation fan-out: the RESTAction inventory IS the navigation-
	// derived GVR set. Every RESTAction's spec.api[*].path is an
	// apiserver REST path the resolver dispatches an inner call against;
	// CollectResourceTypesFromRestActions parses each to a GVR. Widget
	// CRs reach K8s reads exclusively via apiRef→RESTAction, so the
	// RESTActions' inner-call GVRs are precisely the set widget
	// navigation touches — no separate widget-tree recursion is needed
	// to enumerate read GVRs (widget CR GVRs themselves register at
	// runtime on first dispatch via recordWidgetDeps's self-dep edge).
	inv, walkErr := CollectResourceTypesFromRestActions(ctx, dynClient)
	if walkErr != nil {
		slog.WarnContext(ctx, "cache.prewarm.inventory_walk_failed",
			slog.String("subsystem", "cache"),
			slog.Any("err", walkErr),
			slog.String("effect", "no startup prewarm; lazy register-on-navigation still covers every GVR on first request"),
		)
		return 0, 0, walkErr
	}

	// Fix #130 F1: collect the GVRs this walk newly registers so we can
	// prime ONE scoped conjunct-4 confirm over exactly that set after the
	// loop (below). Without the prime, a walk-registered GVR latches
	// typeConfirmed:false until the next discoveryRefreshInterval (30s)
	// ticker tick — the 1.7.3 serve_miss readout showed conjunct 4 was the
	// sole universal blocker of informer-serve at boot for every walk GVR.
	walkRegistered := make([]schema.GroupVersionResource, 0, len(inv))

	for _, gvr := range inv {
		if ctx.Err() != nil {
			// Boot context cancelled — stop early. Whatever we did not
			// reach is covered by the lazy register-on-navigation path.
			slog.WarnContext(ctx, "cache.prewarm.context_cancelled",
				slog.String("subsystem", "cache"),
				slog.Int("registered", registered),
				slog.Int("already_present", alreadyPresent),
				slog.Int("inventory_size", len(inv)),
			)
			return registered, alreadyPresent, ctx.Err()
		}
		// Fire-and-forget: EnsureResourceType registers + spawns the
		// informer's run-loop and sync-watcher, then returns. We
		// deliberately discard the sync channel — the boot sequence does
		// not block on these informers syncing. Tag A's four-conjunct
		// servable() gate ensures any request that lands before sync
		// falls through to apiserver.
		added, _ := rw.EnsureResourceType(gvr)
		if added {
			registered++
			walkRegistered = append(walkRegistered, gvr)
		} else {
			alreadyPresent++
		}
	}

	// Fix #130 F1: prime conjunct-4 (typeConfirmed) for the GVRs this walk
	// registered, in ONE scoped pass. ConfirmResourceTypes reuses
	// RefreshDiscovery's exact per-GV-deduped confirm body (no forked
	// predicate) — one ServerResourcesForGroupVersion per distinct
	// group/version, bounded by ctx, run off the lock. This lands well
	// before the walk's informers issue their first LIST (~seconds), so the
	// FIRST post-walk dispatch of these GVRs can take the informer-serve
	// branch instead of the live paged LIST fall-through.
	//
	// Scope is exactly the walk-registered set — the RBAC bootstrap GVRs are
	// already confirmed by StartDiscoveryRefresher's startup prime, and
	// already-present GVRs (added==false) were confirmed on their original
	// registration path. Offline (disco==nil) degrades to conjunct-4 true
	// per resourceTypeConfirmedLocked — ConfirmResourceTypes leaves
	// rw.confirmed untouched, so the degraded-true path is preserved.
	confirmStart := time.Now()
	rw.ConfirmResourceTypes(ctx, walkRegistered)

	slog.Info("cache.prewarm.completed",
		slog.String("subsystem", "cache"),
		slog.Int("inventory_size", len(inv)),
		slog.Int("registered", registered),
		slog.Int("already_present", alreadyPresent),
		slog.Int("confirm_primed", len(walkRegistered)),
		slog.Int64("confirm_ms", time.Since(confirmStart).Milliseconds()),
		slog.Int64("walk_ms", time.Since(start).Milliseconds()),
		slog.String("mode", "fire-and-forget informers; conjunct-4 primed synchronously — informer-serve alive at boot"),
	)
	return registered, alreadyPresent, nil
}
