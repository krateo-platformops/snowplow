// prewarm_engine.go — Ship 1 of the unified dynamic cohort-prewarm
// engine: the engine core (scope-parameterised rePrewarm), the bounded
// engine work queue, and the customer-priority-yield semaphore.
//
// ONE ENGINE (feedback_dynamic_cohort_prewarm_no_static_no_cold_fill).
// The boot prewarm-navigation AND (Ship 2) the handling of
// created/modified/deleted widgets+RESTActions + RBAC binding shifts use
// the SAME walk→harvest→resource-driven-cohort→seed logic. This file is
// that one engine. Ship 1 invokes it with the BOOT scope only; the queue
// + semaphore scaffolding is built now so Ship 2 wires runtime triggers
// in with no refactor.
//
// SCOPE-PARAMETERISED. rePrewarm(ctx, scope) takes a prewarmScope that
// declares which navigation roots to (re-)walk + which cohort source to
// seed against. The BOOT scope = the 2 nav roots (the frontend
// config.json INIT / ROUTES_LOADER widgets). For each root the engine
// constructs a FRESH phase1Walker (new visited map — phase1_walk.go:679;
// reusing an old visited short-circuits at the visited check and descends
// nothing) and calls the SAME walk() so harvestApiRef + harvestNavWidget
// re-fire unconditionally before widgets.Resolve. The re-walk MUST NOT
// bypass walk().
//
// WHY THE RE-WALK (project_prewarm_page_offset_bug_2026_05_28 post-deploy
// section). The boot walk at phase1WarmupWith Step 4 runs BEFORE Step 7
// WaitAllInformersSynced and is single-pass; the navmenu's children are
// DYNAMIC (resourcesRefsTemplate over the apiRef sidebar-nav-menu-items),
// so the pre-sync walk resolves them while the apiserver-fallthrough data
// is still empty → 0 children → only the 2 roots harvested
// (widgets:2/restactions:2). The engine's BOOT re-walk runs AFTER the
// sync barrier (data available) so the full nav tree is discovered + the
// per-cohort harvesters are populated for the seed.
//
// CUSTOMER PRIORITY (feedback_customer_priority_over_refresher,
// project_c3_design_2026_05_27 B1). Re-prewarm work YIELDS to in-flight
// customer /call. Every customer dispatch (restactions/widgets ServeHTTP)
// brackets its work with markCustomerInFlight/markCustomerDone; the
// engine worker checks customerInFlight() before each unit of seed work
// and parks on a short backoff while any customer call is in flight — the
// customer path keeps absolute priority. The engine NEVER holds a lock the
// customer path needs; the yield is a cooperative check, not a hard mutex.
//
// BOUNDED QUEUE (refresher.go:95 shape). The engine owns a
// workqueue.TypedRateLimitingInterface[prewarmScope] (F.4 / R1 — the
// hand-rolled pending-map+signal-channel it replaced was a
// half-reimplementation of exactly this). prewarmScope is comparable
// (string kind + GVR struct), so the item IS its own dedup key — the
// per-key coalescing the map gave us is preserved 1:1 because key() and
// item identity coincide. This gets FIFO ordering, AddRateLimited
// exponential-backoff requeue (client-go stock defaults, no new knob),
// Forget-on-success, and never-drop from tested client-go code. Widget CR
// changes / RBAC shifts (Ship 2) enqueue with the same idempotent dedup.
// The BOOT scope re-enqueues itself on a per-scope-budget deadline-cut
// (F.4 §3.1) so a cut boot chunk resumes deterministically.
//
// LIFECYCLE. The engine's workers run under a context bounded by the
// engine timeout (boot: pipGlobalTimeout); they exit on ctx cancel /
// queue ShutDown. No unbounded goroutine: worker count is fixed
// (GOMAXPROCS-bounded), and each rePrewarm unit is bounded by the
// per-cohort timeout (seedCohort's context.WithTimeout).

package dispatchers

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
)

// PrewarmEngineEnabled reports whether the unified dynamic cohort-prewarm
// engine runs. FOLDED 2026-07-03 (docs/prewarm-engine-implicit-on-cache-
// 2026-07-03.md): the standalone PREWARM_ENGINE_ENABLED env read is RETIRED
// (registered in cache.retiredFlags). The engine is now IMPLICIT-ON-CACHE — it
// runs exactly when prewarm runs, mirroring #57's PREWARM_ENABLED /
// RESOLVER_USE_INFORMER fold.
//
// Gate on cache.PrewarmEnabled() (== !cache.Disabled(), phase1.go:74-76), NOT a
// raw CACHE_ENABLED read: behaviorally identical today, but it keeps the engine
// bound to the PREWARM master gate so a future prewarm/cache split follows
// automatically. Reads as intent: "engine on iff prewarm on."
//
// The legacy runPIPSeed errgroup back-out (engine-off-cache-on) is DELETED and
// unreachable; the back-out lever is now CACHE_ENABLED=false.
func PrewarmEngineEnabled() bool {
	return cache.PrewarmEnabled() // implicit-on-cache (#57); was env "PREWARM_ENGINE_ENABLED"=="true"
}

// ProactiveRASeedEnabled reports whether the proactive composition-page
// RESTAction seed source (Option A) is on. When on, the engine boot seed UNIONS
// the RBAC-reachable RESTAction refs (cache.RBACReachableRestActionRefs) into
// the nav-walk harvester snapshot so per-composition click-through detail
// RESTActions (never reached by the nav walk) get warmed at boot. This widens
// only WHICH refs the seed loop iterates, never the per-request authz boundary.
//
// FOLDED 2026-07-03 (docs/prewarm-engine-implicit-on-cache-2026-07-03.md §2):
// the standalone PROACTIVE_RA_SEED_ENABLED env read is RETIRED (registered in
// cache.retiredFlags). It is now IMPLICIT-ON-CACHE — the proactive union runs
// whenever prewarm runs. Safe to fold ON given the adaptive seed bound (§3)
// now bounds the (widened) seed's dominant allocation.
func ProactiveRASeedEnabled() bool {
	return cache.PrewarmEnabled() // implicit-on-cache (#57); was env "PROACTIVE_RA_SEED_ENABLED"=="true"
}

// ─────────────────────────────────────────────────────────────────────
// Customer-priority signal. Every customer /call dispatch brackets its
// work with these. The engine yields while the counter is > 0.
// ─────────────────────────────────────────────────────────────────────

// customerInFlightCount tracks the number of customer /call dispatches
// currently executing. Incremented at restactions/widgets ServeHTTP entry,
// decremented (deferred) at exit. Read by the engine worker to decide
// whether to yield.
var customerInFlightCount atomic.Int64

// markCustomerInFlight is called at the top of a customer /call dispatch.
// Returns the matching done function (defer it). Cheap atomic — no lock.
func markCustomerInFlight() func() {
	customerInFlightCount.Add(1)
	return func() { customerInFlightCount.Add(-1) }
}

// customerInFlight reports whether any customer /call is currently
// executing. The engine worker yields while this is true.
func customerInFlight() bool {
	return customerInFlightCount.Load() > 0
}

// CustomerInFlight is the exported predicate the refresher subsystem
// injects via cache.SetCustomerInflightHook (Ship #98 / 0.30.215). It
// shares the same atomic counter as the prewarm engine's yield — a
// customer /call's ServeHTTP increment/decrement bracket
// (restactions.go:77, widgets.go:62) is now observed by BOTH the prewarm
// engine (boot re-seed scopes) AND the steady-state L1 refresher worker
// pool. One atomic-int64 Load per refresher yield tick (4 workers × 40
// Hz = 160 reads/s steady-state) — negligible cost, no cache-line
// contention on the read side.
func CustomerInFlight() bool {
	return customerInFlight()
}

// ─────────────────────────────────────────────────────────────────────
// prewarmScope — the unit of engine work. Declares what to (re-)walk and
// the cohort source for the seed. Ship 1 uses only scopeKindBoot; Ship 2
// adds scopeKindWidgetCR / scopeKindRBACShift (a comment placeholder is
// left so the wiring is obvious).
// ─────────────────────────────────────────────────────────────────────

type prewarmScopeKind string

const (
	// scopeKindBoot — the full boot re-walk of the 2 nav roots after the
	// sync barrier. Ship 1's only invocation.
	scopeKindBoot prewarmScopeKind = "boot"

	// scopeKindGVRDiscovered — Ship 2 Stage 2 / 0.30.247. Fires when a
	// new GVR is first registered post-boot via the synchronous
	// discovery path (cache.DiscoverGroupResources → EnsureResourceType
	// added==true). The discovery side wires this via
	// cache.RegisterGVRDiscoveredHook (gvr_discovered_hook.go); the
	// dispatchers-side hook handler at prewarm_engine_boot.go enqueues
	// a scope per discovered GVR. The engine's rePrewarm core then
	// re-walks the nav tree under each cohort identity (now reading
	// the freshly-widened BindingsByGVR index) so the iterator that
	// previously short-circuited at resolve.go:377-381 against
	// crds.items[] (because the new GVR didn't exist) now yields
	// non-empty tmp[] — the dep edge against the new GVR is recorded,
	// and subsequent CR ADD events propagate via the normal
	// OnAdd→onChange→dirty-mark→refresher path. Closes the canonical
	// S4 admin-path defect.
	scopeKindGVRDiscovered prewarmScopeKind = "gvr-discovered"

	// scopeKindKeepwarm — #102 c1 the TTL-cadenced quiet-page keep-warm sweep.
	// Fired by keepwarmTicker at TTL×3/4 (a design ratio derived from
	// RESOLVED_CACHE_TTL_SECONDS, no new env). Runs the SAME re-walk + seed core
	// as boot (rePrewarmKeepwarm → rePrewarmBootScoped) but the per-identity
	// seed is bounded to rank-1 (the 95%-mix cohort, ALL pages) so rank-1's
	// cells are re-Put — resetting CreatedAt — before they lazy-expire at TTL.
	// Coalesces on key()=="keepwarm": a tick arriving while a sweep still runs
	// dedups to at most one pending sweep. Per-scope timeout = the boot budget.
	scopeKindKeepwarm prewarmScopeKind = "keepwarm"

	// scopeKindRBACShift — #258. An RBAC sub-gen flush rotated one or more
	// subjects' keys; the engine scope-reseeds ONLY the affected resident
	// cohorts (targets whose folded subject is in the rotated set) so the
	// rotated identity's NEW-sub-gen SERVE key is warm on its next navigation.
	// Fired from cache.flushPendingSubGenBumps via cache.RegisterRBACShiftHook
	// (registerEngineRBACShiftHook). Coalesces on key()=="rbac-shift" with an
	// engine-side merge-accumulator of the rotated subjects (union-then-drain),
	// so a burst of flushes collapses to one scope and a rotation arriving
	// mid-reseed is merged + re-armed (no dropped event).
	scopeKindRBACShift prewarmScopeKind = "rbac-shift"

	// scopeKindLearnedClass — #262. A NEW learned identity class appeared (a
	// login or a membership change seen through a clientconfig Secret ADD /
	// UPDATE, or a new live identity). Payload-free and coalesced on
	// key()=="learned-class"; the new class keys wait in the cache registry's
	// pending set, drained when the scope runs (rePrewarmLearnedClasses), which
	// seeds them NavOrder first within the learned-class bound.
	scopeKindLearnedClass prewarmScopeKind = "learned-class"

	// Ship 2 (NOT wired this ship): scopeKindWidgetCR (a widget/RESTAction
	// CR add/update/delete re-walks that object's subtree). The engine queue +
	// rePrewarm core are built to accept it with no refactor.
)

// seedScopeMode is the per-scope seed policy the rePrewarm core threads through
// seedScopeYielding → seedOneWidget/seedOneRestaction. It REPLACES the two
// accreted bools (rank1Only, bootScoped) that used to thread the same path
// (keepwarm c2, design §4.1): those two bools admitted an ILLEGAL 4th state
// (rank1Only && bootScoped — a keepwarm that also fresh-skips) and scattered
// the per-mode skip semantics across call sites. The enum makes each mode's
// (sweep-set bound × skip predicate) a single closed choice, pinned at the type
// level; a cross-mode skip leak is impossible to express and is additionally
// caught by the per-mode falsifier arms (F-C2 (a)/(b)/(c)).
//
// The three modes and their (bound, skip) semantics:
//   - seedModeBoot: ALL ranks; F.4 live-cell fresh-skip (a live cell is done →
//     not re-resolved), so a deadline-cut boot chunk's continuation is
//     cost-proportional. UNCHANGED from the pre-c2 bootScoped=true behavior.
//   - seedModeKeepwarm: the WIDGET-CAPABLE PREFIX of `ranked` (widgetMax>=1,
//     a contiguous prefix post-Fix-3); AGE-SKIP (a young live cell is skipped,
//     an OLD live cell is re-resolved+re-Put). This SUPERSEDES the c1 rank-1
//     bound + the pre-c2 keepwarm no-skip (which restarted from rank-1 each
//     deadline-cut chunk — a non-completing loop at 60K, design §4.2).
//   - seedModeGVRDiscovered: ALL ranks; NO skip (it must RE-RESOLVE already-warm
//     cells to record the dep edge against the newly-registered GVR — the S4
//     fix; F4-C3 boundary). UNCHANGED from the pre-c2 gvr-discovered
//     bootScoped=false behavior.
type seedScopeMode int

const (
	seedModeBoot seedScopeMode = iota
	seedModeKeepwarm
	seedModeGVRDiscovered
	// seedModeRBACShift — #258. Reseed ONLY the targets whose folded subject
	// rotated in this flush (RotatedSubjectSet.Rotated), re-resolving each under
	// its own identity so dispatchCacheLookupKey stamps the NEW per-subject
	// RBACSubGenForSubject (the SERVE key). LIVENESS skip only (never an
	// age-skip): a live cell under the new-sub-gen key was minted after the
	// rotation, so a re-armed run reads it instead of re-resolving it
	// (seedSkipDecision); an absent key is resolved and minted.
	seedModeRBACShift
	// #378: there is no seed re-mint mode. The age re-mint is carried by the
	// refresher terminal (cache.ReplaceIfGenRefresh); the seed-path re-mint
	// (seedModeReMint / reseedFromInputs) was retired with zero production callers.
)

// String renders the mode for the seed's completion log (replaces the old
// bool→string map at rePrewarmBootScoped's boot.complete line).
func (m seedScopeMode) String() string {
	switch m {
	case seedModeBoot:
		return "boot"
	case seedModeKeepwarm:
		return "keepwarm"
	case seedModeGVRDiscovered:
		return "gvr-discovered"
	case seedModeRBACShift:
		return "rbac-shift"
	default:
		return "unknown"
	}
}

// prewarmScope is one engine work item.
//
// SCOPE-KIND-CARRIED PAYLOAD:
//   - scopeKindBoot: no payload (gvr left zero); one boot scope per process.
//   - scopeKindGVRDiscovered: gvr carries the discovered GVR; the engine
//     dedups on key() so multiple discovery events for the same GVR
//     coalesce to one queued scope.
type prewarmScope struct {
	kind prewarmScopeKind

	// gvr is the discovered GroupVersionResource for scopeKindGVRDiscovered.
	// Zero value for other scope kinds (carries no semantics). The engine's
	// rePrewarm core inspects this only when scope.kind == scopeKindGVRDiscovered.
	gvr schema.GroupVersionResource
}

// key returns the workqueue dedup key for this scope. Idempotent enqueue
// of the same key coalesces (refresher dedup semantics).
//
// KEY DERIVATION (RC-2 PM gate):
//   - scopeKindBoot: "boot" — single key, all boot enqueues coalesce.
//   - scopeKindGVRDiscovered: "gvr-discovered" — #643 PR-C: ONE key. The pass
//     ignores its GVR (it re-runs the whole seed), so N discovered GVRs fold
//     into one pending pass; the discovery epoch, not the item, records that
//     new edges are needed. The hook enqueues a payload-free item (the queue
//     dedups on the item itself), so this key and the item agree.
func (s prewarmScope) key() string {
	return string(s.kind)
}

// ─────────────────────────────────────────────────────────────────────
// prewarmEngine — the bounded queue + customer-priority worker pool. One
// per process; constructed lazily.
// ─────────────────────────────────────────────────────────────────────

type prewarmEngine struct {
	// scopeHandler runs one prewarmScope (the rePrewarm core, bound to the
	// boot harvesters + SA config at StartPrewarmEngine time). Set once at
	// start; read by workers.
	scopeHandler func(ctx context.Context, s prewarmScope) error

	// queue is the client-go rate-limiting workqueue (F.4 / R1). prewarmScope
	// is comparable so the item is its own dedup key — Add coalesces on scope
	// identity exactly as the old map keyed on key(). FIFO + AddRateLimited
	// backoff + Forget + never-drop + ShutDown come from client-go. Constructed
	// once in prewarmEngineSingleton.
	queue workqueue.TypedRateLimitingInterface[prewarmScope]

	// scopeDone, when set, is invoked by the worker after each scope is
	// processed (success or error) with the processed scope + its err. The
	// boot wiring uses it to release the engineSeed goroutine the instant
	// the boot scope completes, instead of holding for the full
	// pipGlobalTimeout (S2). Set once at start; read-only on the worker.
	scopeDone func(s prewarmScope, err error)

	startedOnce sync.Once

	// #132 F4b Lever A / #643 PR-B — the declined-external marker set is ONE
	// engine-wide, process-lived set (cache.EngineDeclinedExternalSet), installed
	// on every scope kind in processScope. It is no longer held per scope key here
	// and no longer torn down when boot completes; its marks retire themselves
	// (cache/declined_external_engine.go). A config-vars redrive still clears it.

	// #105 — the boot-convergence bookkeeping, ENGINE-LIVED per boot-scope-key
	// (same lifecycle discipline as declinedExtSets): created on the boot scope's
	// first processScope, REUSED across its AddRateLimited requeues so the
	// cross-pass set-delta (seeded-set grew / failed-set shrank) accumulates
	// across resume passes, and TORN DOWN on genuine boot completion (err==nil →
	// Forget) + config-vars redrive so a new nav topology re-arms with fresh
	// empty sets. Keyed by s.key() ("boot"). Guarded by bootConvMu.
	bootConvMu   sync.Mutex
	bootConvSets map[string]*bootConvergenceState

	// rbacShift is the #258 merge-accumulator of rotated subjects pending a
	// scoped reseed. The cache→engine RBAC-shift hook (registerEngineRBACShiftHook)
	// MERGEs each sub-gen flush's RotatedSubjectSet into it and enqueues the
	// payload-free scopeKindRBACShift scope (coalesces); rePrewarmRBACShift DRAINs
	// it (take-and-reset) when the scope runs. A rotation arriving mid-reseed
	// merges into the fresh post-drain set and re-arms the scope → exactly one
	// follow-up (≤2 reseeds per burst). Set once in prewarmEngineSingleton.
	rbacShift *cache.RBACShiftAccumulator

	// customer-priority yield knobs.
	yieldPoll time.Duration // how long a worker parks while a customer call is in flight

	// Falsifier / telemetry counters.
	enqueuedTotal  atomic.Uint64
	processedTotal atomic.Uint64
	yieldTotal     atomic.Uint64 // engine yields to a customer call
	requeuedTotal  atomic.Uint64 // F.4 — scopes engine-requeued after an error (AddRateLimited)

	// #643 PR-C — the gvr-discovered discovery epoch (bumped by the hook before
	// it enqueues), the GVRs discovered since the last pass (log only), and the
	// engine-lived resume sets per scope key (prewarm_pass_resume.go).
	gvrEpoch     atomic.Uint64
	gvrPendingMu sync.Mutex
	gvrPending   map[string]struct{}
	passDoneMu   sync.Mutex
	passDone     map[string]*passDoneSet
}

var (
	prewarmEngineInstance *prewarmEngine
	prewarmEngineOnce     sync.Once
)

// defaultEngineYieldPoll is how long an engine worker parks before
// re-checking customerInFlight when a customer call is in flight. Short
// enough that the engine resumes promptly once the burst clears; long
// enough that a sustained customer burst keeps the engine fully yielded.
const defaultEngineYieldPoll = 25 * time.Millisecond

func prewarmEngineSingleton() *prewarmEngine {
	prewarmEngineOnce.Do(func() {
		prewarmEngineInstance = &prewarmEngine{
			// F.4 / R1 — client-go rate-limiting workqueue with stock default
			// controller rate-limiter (exponential per-item backoff + overall
			// bucket). prewarmScope is comparable so it is its own dedup key.
			queue: workqueue.NewTypedRateLimitingQueue(
				workqueue.DefaultTypedControllerRateLimiter[prewarmScope](),
			),
			rbacShift: cache.NewRBACShiftAccumulator(),
			yieldPoll: defaultEngineYieldPoll,
		}
	})
	return prewarmEngineInstance
}

// enqueueScope adds a scope to the workqueue. Idempotent: prewarmScope is
// comparable and IS its own dedup key, so a scope already present (queued
// or being processed) coalesces exactly as the old map keyed on key()
// (Ship 1's scopes carry no payload, so it is a true no-op dedup; a
// scopeKindGVRDiscovered coalesces per-GVR). Never blocks — workqueue.Add
// is a mutex critical section. Immediate (not rate-limited) add: this is
// the fresh-arrival path; the engine-owned failure requeue uses
// AddRateLimited in runWorker.
func (e *prewarmEngine) enqueueScope(s prewarmScope) {
	e.queue.Add(s)
	e.enqueuedTotal.Add(1)
}

// forgetScope resets the workqueue's rate-limit / requeue history for s
// (NumRequeues(s) → 0) WITHOUT processing it. #135 F4b Lever B: a config-vars
// redrive is a GENUINE TOPOLOGY CHANGE, so the boot scope must re-dequeue at
// attempt==0 → rePrewarmBootScoped WALKS the new nav set instead of REUSING the
// prior topology's harvester snapshot. enqueueScope alone is a plain queue.Add,
// which does NOT reset NumRequeues; a redrive arriving while an F.4 deadline-cut
// requeue streak is in flight (NumRequeues>0) would otherwise keep attempt>0 and
// wrongly reuse the stale snapshot. enqueueBootReDrive pairs forgetScope +
// enqueueScope so the redrive re-walks. A plain F.4 deadline-cut does NOT call
// this — it AddRateLimited's inside processScope and keeps its NumRequeues
// (attempt>0 → reuse, the intended fast path). Never blocks (workqueue.Forget is
// a mutex critical section).
func (e *prewarmEngine) forgetScope(s prewarmScope) {
	e.queue.Forget(s)
}

// declinedExternalSetFor returns the engine-wide declined-external set (#643
// PR-B). The key argument is kept for the call sites that name the boot scope;
// every scope kind shares the one set.
func (e *prewarmEngine) declinedExternalSetFor(_ string) *cache.SeedDeclinedExternalSet {
	return cache.EngineDeclinedExternalSet()
}

// summarizeDeclinedExternalSet emits the phase1.seed.declined_external.summary
// line for the engine-wide set: the cumulative distinct keys ever marked and
// the keys held now. Emitted on boot completion (which no longer tears the set
// down, #643) and before a config-vars redrive clears it.
func (e *prewarmEngine) summarizeDeclinedExternalSet(key, reason string) {
	set := cache.EngineDeclinedExternalSet()
	if n := set.Marks(); n > 0 {
		slog.Info("phase1.seed.declined_external.summary",
			slog.String("subsystem", "cache"),
			slog.String("scope", key),
			slog.String("reason", reason),
			slog.Uint64("declined_external_keys", n),
			slog.Int64("held", set.Len()),
			slog.String("effect", "engine-wide declined-external set (#643): distinct (widget,cohort) keys "+
				"resolved-and-declined for an external touch since process start; every background pass "+
				"skips a held key instead of re-dialling it, until the mark retires itself"),
		)
	}
}

// clearDeclinedExternalSet CLEARS the engine-wide declined-external set. Called
// ONLY on a config-vars redrive (a new nav topology): a rare event, and the
// conservative choice — whatever a config change can alter, every external key
// is re-resolved once under the new topology. Boot completion no longer clears
// it (#643 PR-B: background passes need boot's marks). Concurrency-safe.
func (e *prewarmEngine) clearDeclinedExternalSet(key, reason string) {
	e.summarizeDeclinedExternalSet(key, reason)
	set := cache.EngineDeclinedExternalSet()
	cache.RecordDeclinedExternalRetired(cache.DeclinedExternalRetiredConfigRedrive, int(set.Len()))
	set.Reset()
}

// bootConvergenceStateForKey returns the engine-lived #105 boot-convergence
// state for scope key, creating it on first use. REUSED across the scope's
// AddRateLimited requeues (so a resume pass's set-delta compares against the
// cumulative prior* the earlier passes populated — the whole point of the
// cross-pass bound). Concurrency-safe. Only the boot scope calls this (the only
// scope whose seedScopeYielding consults a convergence state); other scope
// kinds never install one, so the tail re-enqueue falls back to the legacy
// per-pass latch off the boot path.
func (e *prewarmEngine) bootConvergenceStateForKey(key string) *bootConvergenceState {
	e.bootConvMu.Lock()
	defer e.bootConvMu.Unlock()
	if e.bootConvSets == nil {
		e.bootConvSets = make(map[string]*bootConvergenceState)
	}
	st, ok := e.bootConvSets[key]
	if !ok {
		st = newBootConvergenceState(e)
		e.bootConvSets[key] = st
	}
	return st
}

// clearBootConvergenceState TEARS DOWN (deletes) the engine-lived #105
// convergence state for scope key so the NEXT time that key is processed it
// starts from fresh empty sets (a new nav topology gets a genuine fresh attempt,
// and the map cannot accumulate one entry per scope-key forever). Called on a
// scope's GENUINE completion (err==nil → Forget) AND on a config-vars redrive.
// (Until #643 the declined-external set shared these teardown points; it is now
// engine-wide and only the redrive clears it.) NOT called on an AddRateLimited requeue
// (the resume must REUSE the state). Concurrency-safe; no-op if absent.
func (e *prewarmEngine) clearBootConvergenceState(key string) {
	e.bootConvMu.Lock()
	defer e.bootConvMu.Unlock()
	delete(e.bootConvSets, key)
}

// StartPrewarmEngine starts the engine worker(s) bound to the given scope
// handler. Idempotent — repeated calls are no-ops (the first wins). The
// handler is the rePrewarm core closed over the boot harvesters + SA
// config; the worker invokes it per dequeued scope, yielding to customer
// /call between scopes. The worker exits on ctx cancel.
//
// Ship 1 runs ONE worker (the boot re-walk is a single coherent pass).
// Ship 2 may raise the worker count for parallel runtime triggers; the
// queue's dedup keeps that safe.
//
// scopeDone (nilable) is invoked after each scope is processed — the boot
// wiring uses it to release the background goroutine the instant the boot
// scope completes (S2) rather than holding for the full pipGlobalTimeout.
//
// Ship 2 Stage 2 / 0.30.247 — wires the cache→engine hook for
// scopeKindGVRDiscovered. The hook subscribes here BEFORE the worker
// goroutine spawns so any GVR discovered during boot (lazy registers
// post-MarkEagerSet) is queued, not dropped. The wiring happens inside
// startedOnce so it runs exactly once per process.
//
// Ship 2 Stage 2.5 / 0.30.248 (Fix v2 — engine ctx decoupling). PM-required
// change #4: re-entry semantics — startedOnce makes the first
// StartPrewarmEngine(processCtx, ...) call canonical; subsequent calls
// (e.g. a future re-init) are NO-OPs that do NOT replace ctx/handler/
// scopeDone. The very first call's processCtx wins for the worker's
// lifetime; the first call's handler+scopeDone bind too. Re-entry MUST
// pass a process-lifetime ctx the first time (today: cacheCtx from
// main.go via dispatchers.SetEngineProcessContext).
//
// CTX CONTRACT (Fix v2). `ctx` is the PROCESS-LIFETIME context — typically
// cacheCtx from main.go. The worker goroutine runs until this context is
// cancelled (i.e. until process shutdown). It MUST NOT be the boot-seed
// goroutine's bounded context (which gets cancelled the instant the
// boot scope completes at engineSeed return, killing the worker
// 7-12 minutes before post-boot scopeKindGVRDiscovered events arrive at
// production scale — empirically traced in
// docs/task-194-s4-defect-trace-v2-2026-06-05.md §1.5).
//
// Individual scope executions are bounded by their own per-scope timeout
// derived inside runWorker via prewarmScopeTimeout(s); the per-scope
// timeout is anchored to the long-lived process ctx, not to any
// boot-seed orchestration ctx.
func StartPrewarmEngine(ctx context.Context, handler func(ctx context.Context, s prewarmScope) error, scopeDone func(s prewarmScope, err error)) {
	e := prewarmEngineSingleton()
	e.startedOnce.Do(func() {
		e.scopeHandler = handler
		e.scopeDone = scopeDone

		// Ship 2 Stage 2 — wire the cache→engine hook. The hook fires
		// from inside cache.DiscoverGroupResources (the `if added`
		// branch of EnsureResourceType for genuinely-new GVRs). The
		// callback is non-blocking: enqueueScope is O(1) — a single
		// workqueue.Add (a mutex critical section; F.4 / R1).
		//
		// REGISTRATION ORDER (PM observation 3, R4 startup-storm):
		// runs BEFORE `go e.runWorker(ctx)` below so a discovery firing
		// during the same goroutine that called StartPrewarmEngine
		// (e.g. via subsequent walker calls) IS queued, not dropped.
		// The registration is idempotent at the cache side (compares
		// fn pointer) so a future engine re-entry would not double-wire.
		registerEngineGVRDiscoveredHook(e)
		registerEngineRBACShiftHook(e)
		registerEngineLearnedClassHook(e)

		// Publish expvar counters — Fix v2 PM Change #1. Inside startedOnce
		// so initialisation runs exactly once.
		registerPrewarmEngineMetrics(e)

		go e.runWorker(ctx)
		slog.Info("prewarm.engine.started",
			slog.String("subsystem", "cache"),
			slog.String("queue", "workqueue-ratelimiting"), // F.4 / R1 — client-go typed workqueue
			slog.String("customer_priority", "yield-on-inflight"),
			slog.String("gvr_discovered_hook", "wired"),
			slog.String("ctx_lifetime", "process"),
		)
	})
}

// prewarmScopeTimeout returns the budget for one scope execution under
// the worker's per-scope context.WithTimeout. Anchored to the worker's
// long-lived process ctx — NOT to any boot-seed orchestration ctx
// (Fix v2 / 0.30.248).
//
// Boot scope: pipGlobalTimeout (8m) — matches the pre-Fix-v2
// boot-seed budget exactly. The boot rePrewarm's wall-clock shape is
// unchanged (a single coherent pass).
//
// gvr-discovered scope: pipGlobalTimeout (8m) — same as boot. The
// re-walk + per-target seed shape is identical to boot (one full
// re-walk of the nav tree + cohort seed). Architect Trace v2 §7 OQ-2
// proposes keeping at 8m uniformly until per-GVR re-walk timing data
// suggests tightening.
func prewarmScopeTimeout(s prewarmScope) time.Duration {
	return pipGlobalTimeout
}

// prewarmScopeTimeoutFn is a 1-LOC test seam over prewarmScopeTimeout — the
// SAME `var fooFn = foo` pattern as seedCohortFn (phase1_pip_seed.go) and the
// seedOneWidgetFn / enumeratePrewarmTargetsForGVRFn seams
// (prewarm_engine_boot.go). runWorker's per-scope context.WithTimeout routes
// through it so the F.4 straddle falsifier can shrink one chunk's budget to
// force a mid-segment deadline-cut without a live cluster or an 8-minute wait.
// PRODUCTION ALWAYS uses prewarmScopeTimeout (8m, F4-C6 no-new-knob — the
// budget literal is untouched); only a _test.go override reassigns this var.
var prewarmScopeTimeoutFn = prewarmScopeTimeout

// runWorker is the engine worker loop. It blocks on the workqueue's Get,
// runs each scope through the handler — YIELDING to any in-flight customer
// /call before each scope — and, on error with the process ctx still
// alive, ENGINE-OWNS the resume by requeueing the scope with rate-limited
// backoff (F.4 §3.1). Exits on ctx cancel (which ShutDowns the queue so
// the blocking Get unblocks).
//
// CTX CONTRACT (Fix v2 / 0.30.248). `ctx` is the worker's process-lifetime
// context passed from StartPrewarmEngine. The worker exits ONLY on
// process shutdown. Per-scope wall-clock bounds are derived inside the
// loop via context.WithTimeout(ctx, prewarmScopeTimeoutFn(s)) so a
// single misbehaving scope cannot stall the worker indefinitely; the
// scope ctx cancels the instant the scope returns (scopeCancel) so
// resources never leak.
//
// F.4 ENGINE-OWNED RESUME. A boot scope cut by its per-scope budget
// returns ctx.DeadlineExceeded → the worker requeues it (AddRateLimited)
// so the continuation chunk runs deterministically, with zero dependence
// on an external config-vars event (the pre-F.4 nondeterministic redrive
// trigger). Uniform across scope kinds: gvr-discovered and keepwarm
// errors requeue too. On success we Forget the item so its backoff
// history resets. Never-drop: retries space out via exponential backoff
// (client-go stock rate-limiter) but never stop while the process lives
// (F4-C8 futility bound — no hot loop).
func (e *prewarmEngine) runWorker(ctx context.Context) {
	// ShutDown the queue when the process ctx cancels so the blocking
	// queue.Get below returns shutdown==true and the worker exits. The
	// queue never delivers items after ShutDown.
	go func() {
		<-ctx.Done()
		e.queue.ShutDown()
	}()

	for {
		s, shutdown := e.queue.Get()
		if shutdown {
			return
		}
		e.processScope(ctx, s)
	}
}

// processScope runs one dequeued scope under the customer-priority yield +
// per-scope timeout, then Done()s it and either Forgets (success) or
// AddRateLimited-requeues (error, process alive) — the F.4 engine-owned
// resume. Split out from runWorker so the whole item lifecycle
// (Get→Done→Forget/requeue) is one auditable unit.
func (e *prewarmEngine) processScope(ctx context.Context, s prewarmScope) {
	// Done MUST be called for every Get, whatever the outcome, or the queue
	// leaks the "processing" mark and refuses to re-deliver the item.
	defer e.queue.Done(s)

	// #871 — measurement only: one scope is one prewarm unit. scopeCtx below
	// derives from this ctx, so the walk, its stage goroutines and every seed
	// target (relabelled seed by measuredSeedUnit) carry the label.
	ctx, endWork := workclass.Begin(ctx, workclass.Prewarm)
	defer endWork()

	// CUSTOMER PRIORITY — yield before running the scope while any customer
	// /call is in flight. The seed work inside the handler also yields
	// between cohorts (see seedScopeYielding), so a burst arriving mid-scope
	// is also deferred.
	e.yieldToCustomer(ctx)
	if e.scopeHandler == nil {
		e.queue.Forget(s)
		return
	}
	// Per-scope bound: anchor on the long-lived worker ctx so the scope
	// shares the worker's lifetime (boot/process), but cap the scope's own
	// wall-clock at prewarmScopeTimeoutFn(s) so a single misbehaving scope
	// cannot occupy the worker forever. prewarmScopeTimeoutFn is a 1-LOC
	// test seam over prewarmScopeTimeout (same pattern as seedCohortFn) so
	// the straddle falsifier can shrink the budget without a live cluster;
	// production ALWAYS uses prewarmScopeTimeout (8m, unchanged).
	budget := prewarmScopeTimeoutFn(s)
	scopeCtx, scopeCancel := context.WithTimeout(ctx, budget)
	// #643 PR-A — per-pass unit accounting, installed for EVERY scope kind. The
	// seed / reseed loops write planned+done into it; it is read below, after
	// the handler returns, to publish the pass detectors. Observation only.
	passStats := &prewarmPassStats{}
	scopeCtx = withPrewarmPassStats(scopeCtx, passStats)
	// #105 — the boot-convergence state handle (nil off the boot path), captured
	// here so the err==nil teardown branch can read reEnqueuedLastPass and clear
	// only on a GENUINE completion (see below).
	var bootConv *bootConvergenceState
	// #132 F4b Lever A / #643 PR-B — the engine-wide declined-external set rides EVERY scope kind
	// (boot, gvr-discovered, keepwarm, rbac-shift, learned-class), so a
	// background pass skips an external key an earlier pass already resolved and
	// declined instead of re-dialling it for every cohort. A /call and the
	// refresher never carry it (I-6).
	scopeCtx = cache.WithSeedDeclinedExternalSet(scopeCtx, e.declinedExternalSetFor(s.key()))
	// #643 PR-C — the gvr-discovered resume set (engine-lived per scope key,
	// reused across requeues), and the epoch the pass began under: a clean pass
	// clears the set only if no GVR was discovered while it ran.
	var passEpochAtStart uint64
	var passDone *passDoneSet
	var passDoneBefore uint64
	if s.kind == scopeKindGVRDiscovered {
		passEpochAtStart = e.gvrEpoch.Load()
		passDone = e.passDoneSetFor(s.key())
		passDoneBefore = passDone.marksTotal()
		scopeCtx = withPassDoneSet(scopeCtx, passDone)
		// The GVRs this coalesced pass answers — log only (on no key/label).
		n, gvrs := e.drainDiscoveredGVRs()
		slog.Info("prewarm.engine.gvr_discovered.pass",
			slog.String("subsystem", "cache"),
			slog.Uint64("epoch", passEpochAtStart),
			slog.Int("gvrs_since_last_pass", n),
			slog.Any("gvrs", gvrs),
			slog.Int("resume_records", e.passDoneSetFor(s.key()).len()),
		)
	}
	if s.kind == scopeKindBoot {
		// #105 — install the ENGINE-LIVED boot-convergence state for the boot scope
		// onto scopeCtx. Its BootSeededSet is reachable at the two seed success
		// sites (Mark) via cache.WithBootSeededSet; the whole state is reachable at
		// the seedScopeYielding tail (the set-delta re-enqueue decision) via
		// withBootConvergenceState. Created once (first processScope for this key),
		// REUSED across the scope's AddRateLimited requeues so the cross-pass
		// set-delta accumulates. Boot scope ONLY. Inert under Disabled()
		// (WithBootSeededSet returns ctx unchanged).
		bootConv = e.bootConvergenceStateForKey(s.key())
		scopeCtx = cache.WithBootSeededSet(scopeCtx, bootConv.seeded)
		scopeCtx = withBootConvergenceState(scopeCtx, bootConv)
		// #135 F4b Lever B — carry the workqueue's per-item requeue count onto the
		// boot scope ctx so rePrewarmBootScoped can distinguish pass 0
		// (attempt==0 → WALK) from an F.4 deadline-cut resume (attempt>0 → REUSE
		// the already-populated harvester snapshot, skip the ~255s re-walk). This
		// is the SAME NumRequeues the scope_requeued log reads below; Forget-on-
		// success (:637) + the config-vars-redrive Forget (phase1_configvars_watch.go)
		// reset it to 0 so a genuine topology change re-walks. Boot scope ONLY.
		scopeCtx = cache.WithBootResumeAttempt(scopeCtx, e.queue.NumRequeues(s))
	}
	passStart := time.Now()
	err := e.scopeHandler(scopeCtx, s)
	scopeCancel()
	passElapsed := time.Since(passStart)
	e.processedTotal.Add(1)
	// #643 PR-A — publish the pass detectors (pass_total{kind,outcome} + the
	// last-pass planned/done/budget/elapsed gauges). The outcome is computed once
	// here and reused by the log lines below so the counter and the log agree.
	outcome := recordPrewarmPass(string(s.kind), err, passStats, budget, passElapsed)

	switch {
	case err == nil && outcome == prewarmPassOutcomeComplete:
		// Info, not Warn: a completion is work, not a failure. The shipped signal
		// is snowplow_prewarm_pass_total{outcome=complete}.
		logPrewarmPass(slog.LevelInfo, "prewarm.pass.complete", s, outcome, passStats, budget, passElapsed)
	case err == nil:
		// A handler that re-enqueues itself on a cut (rbac-shift, learned-class)
		// returns nil, so scope_incomplete below never fires for it. Without this
		// line its cut passes would be invisible in the shipped logs.
		logPrewarmPass(slog.LevelWarn, "prewarm.pass.cut", s, outcome, passStats, budget, passElapsed)
	}

	if err != nil {
		slog.Warn("prewarm.engine.scope_incomplete",
			slog.String("subsystem", "cache"),
			slog.String("scope", s.key()),
			slog.Any("err", err),
			slog.String("kind", string(s.kind)),
			slog.String("outcome", outcome),
			slog.Int64("planned", passStats.plannedN()),
			slog.Int64("done", passStats.doneN()),
			slog.Int64("budget_ms", budget.Milliseconds()),
			slog.Int64("elapsed_ms", passElapsed.Milliseconds()),
		)
		// F.4 engine-owned failure-requeue. Only while the process ctx is
		// alive — on shutdown the scope error is the ctx cancel itself and a
		// requeue would race the ShutDown (the queue drops rate-limited adds
		// after ShutDown anyway, but skipping keeps the counter honest and
		// avoids a spurious scope_requeued line at teardown).
		if ctx.Err() == nil {
			// #643 PR-C (C4) — the coalesced gvr-discovered item's backoff resets
			// on PROGRESS: an attempt that made ≥1 accepted resume mark (an
			// accepted Put of a fresh resolve — a new unit OR a redo of a record
			// that went stale, e.g. after a mid-pass GVR discovery; resume SKIPS
			// make no mark) retries at the limiter's base delay. Only an attempt
			// with zero marks lets NumRequeues — the exponential backoff — grow
			// (genuine overload). The boot-only attempt counter (#135 Lever B) is
			// not affected: it is installed for boot scopes only.
			progress, newDone := false, uint64(0)
			if passDone != nil {
				newDone = passDone.marksTotal() - passDoneBefore
				if newDone > 0 {
					progress = true
					e.queue.Forget(s)
				}
			}
			e.queue.AddRateLimited(s)
			e.requeuedTotal.Add(1)
			slog.Info("prewarm.engine.scope_requeued",
				slog.String("subsystem", "cache"),
				slog.String("scope", s.key()),
				slog.Int("attempt", e.queue.NumRequeues(s)),
				slog.Bool("progress", progress),
				slog.Uint64("new_done", newDone),
				slog.Any("err", err),
				slog.String("effect", "engine-owned resume: cut/failed scope re-enqueued with "+
					"rate-limited backoff (F.4); a boot deadline-cut resumes as a continuation chunk, "+
					"no dependence on config-vars events"),
			)
		}
	} else {
		// Success — reset the item's backoff history (F4-C8: Forget-on-success
		// so a later transient failure starts from zero backoff, not the tail
		// of an old streak).
		e.queue.Forget(s)
		// #643 PR-C — a gvr-discovered pass that completed with the epoch
		// unchanged covered every GVR up to now: drop its resume set. If a GVR
		// arrived meanwhile, keep it (its records are stale against the new
		// epoch, and the coalesced re-run redoes every unit anyway).
		if s.kind == scopeKindGVRDiscovered && e.gvrEpoch.Load() == passEpochAtStart {
			e.clearPassDoneSet(s.key())
		}
		// #643 PR-B — boot completion no longer tears the declined-external set
		// down: keepwarm / gvr-discovered / rbac-shift passes need boot's marks.
		// Summarise it (boot scope only, once per completion).
		if s.kind == scopeKindBoot {
			e.summarizeDeclinedExternalSet(s.key(), "boot-complete")
		}
		// #105 — drop the engine-lived boot-convergence state ONLY on a GENUINE
		// completion: err==nil (this branch) AND the tail did NOT re-enqueue this
		// pass (a clean pass with no operational failures, or the bound
		// tripped/converged). The #105 re-enqueue is a NIL-return + plain Add (not
		// an err!=nil AddRateLimited), so a bare err==nil clear (as the
		// declined-external set uses) would wipe the cross-pass set-delta state on
		// EVERY re-walk pass and the bound could never accumulate. When the tail
		// DID re-enqueue (reEnqueuedLastPass), the boot is NOT done — keep the
		// state so the next pass's set-delta compares against the accumulated
		// prior*. On a fresh-boot/config-vars redrive the state is torn down via
		// clearBootConvergenceState at those explicit teardown points. Boot scope
		// only (bootConv nil otherwise).
		if bootConv != nil && !bootConv.reEnqueuedLastPass {
			e.clearBootConvergenceState(s.key())
		}
	}

	if e.scopeDone != nil {
		e.scopeDone(s, err)
	}
}

// yieldToCustomer parks the worker while any customer /call is in flight,
// re-checking every yieldPoll. Returns promptly once no customer call is
// in flight (or ctx is cancelled). This is the cooperative customer-
// priority yield: the engine never delays a customer call; it steps aside
// for the duration of the burst.
func (e *prewarmEngine) yieldToCustomer(ctx context.Context) {
	if !customerInFlight() {
		return
	}
	t := time.NewTicker(e.yieldPoll)
	defer t.Stop()
	for customerInFlight() {
		e.yieldTotal.Add(1)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// engineYieldCheckpoint is the per-cohort yield checkpoint the seed loop
// calls between cohorts so a customer burst arriving MID-scope is also
// deferred. It is the same cooperative yield as yieldToCustomer but
// callable from the seed package without exposing the engine struct.
func engineYieldCheckpoint(ctx context.Context) {
	prewarmEngineSingleton().yieldToCustomer(ctx)
}
