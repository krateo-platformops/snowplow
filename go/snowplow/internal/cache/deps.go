// deps.go — Tag 0.30.8: dependency-tracking layer for the L1 resolved-output cache.
//
// Per implementation plan §"Tag 0.30.8 — What's implemented" and binding
// memory rule feedback_l1_invalidation_delete_only.md:
//
//   - Records which L1 keys depend on which (gvr, namespace, name) tuples
//     (exact-object) and which (gvr, namespace, "*") tuples (list-scope).
//   - Four-bucket lookup on watcher events:
//       1. exact:        (gvr, ns,   name)
//       2. ns-list:      (gvr, ns,   "*")
//       3. cluster-name: (gvr, "",   name)
//       4. cluster-list: (gvr, "",   "*")
//     Union of dependent L1 keys is the action target.
//   - DELETE events evict each dependent L1 key from the resolved-output
//     cache (definite invalidation; the underlying object is gone).
//     DELETE is the ONLY authorised eviction trigger per the binding rule.
//   - UPDATE/PATCH events enqueue each dependent L1 key into the refresher
//     queue (stale-while-revalidate). NEVER evicts.
//   - ADD events are deliberately a no-op for the dep tracker. Pre-flight
//     falsifier on 0.30.7 (probe.log 2026-05-13) showed first nav after
//     namespace ADD already converges to 16/16 calls within 3 s — no
//     ADD-handler scope at this tag.
//
// Bounded: a single int cap (DEPS_MAX_RECORDS, default 1 000 000 forward
// records). Reaching the cap causes new Record calls to be silently
// dropped (cache stays correct via the time-to-live outer net) and the
// summary log emits a one-shot WARN. The cap is intentionally
// conservative — at ~100k L1 entries × ~10 inner-call edges each, the
// expected steady state sits at ~1M records.
//
// Concurrency: forward + reverse indexes are both sync.Map. Per-bucket
// L1-key sets are also sync.Map[l1Key]struct{}. No global mutex — every
// hot path is lock-free. Cleanup (RemoveL1Key) holds no global lock; it
// walks the reverse index for the dropped key and deletes from each
// referenced forward bucket independently.
//
// Why sync.Map (not map+mutex):
//   - hot path is "many readers (OnDelete/OnUpdate) + many writers
//     (Record on every resolve)" with disjoint keys. sync.Map's
//     space-time tradeoff fits this exactly.
//   - cleanup is rare (LRU evict / DELETE) and serial within an L1 key,
//     so the cost of sync.Map.Range is paid only at cleanup time.

package cache

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ctxKeyL1RecordType is the typed empty-struct context key used by
// WithL1KeyContext / L1KeyFromContext. Distinct unexported type so
// external packages cannot collide via raw string keys.
type ctxKeyL1RecordType struct{}

var ctxKeyL1Record = ctxKeyL1RecordType{}

// WithL1KeyContext returns a child context that carries l1Key as the
// resolved-output cache entry currently being populated. The resolver
// reads this via L1KeyFromContext during inner-call dispatch and records
// dep edges so DELETE events on the touched (gvr, ns, name) tuples evict
// the entry from L1.
//
// Empty l1Key is treated as "do not record" — the parent context is
// returned unchanged (saves an allocation and keeps the no-record
// invariant explicit at the call site).
//
// O15 (0.30.110): an empty l1Key is also a loud-fail signal. A caller
// reaching WithL1KeyContext with "" usually means the L1 key was never
// threaded through — a silent dep-recording regression. In production
// this WARNs and bumps recordDroppedNoKey; in test mode it panics so the
// regression cannot ship. The parent context is still returned unchanged
// either way so the no-record invariant downstream is preserved.
//
// Per plan §0.30.94 / Revision 19 "Resolver-side dep recording threaded
// via context.Context". Threading via context.Value avoids adding a
// *RecordingDeps parameter to every signature in the resolver call
// chain (api.Resolve → restactions.Resolve → httpcall.Do).
func WithL1KeyContext(ctx context.Context, l1Key string) context.Context {
	if l1Key == "" {
		loudFailEmptyL1Key("WithL1KeyContext")
		return ctx
	}
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyL1Record, l1Key)
}

// loudFailEmptyL1Key implements the O15 empty-l1Key contract: panic in
// test mode (so a missing-key regression cannot ship), WARN + counter in
// production (the TTL outer net keeps the cache correct). The counter
// lives on the Deps() singleton; Deps() is always non-nil.
func loudFailEmptyL1Key(callsite string) {
	Deps().recordDroppedNoKey.Add(1)
	if depsTestMode.Load() {
		panic("cache.deps: " + callsite + " called with empty l1Key — " +
			"the L1 key was not threaded through (O15 loud-fail, test mode)")
	}
	slog.Warn("deps.empty_l1_key",
		slog.String("subsystem", "cache"),
		slog.String("callsite", callsite),
		slog.String("hint", "dep edge dropped — L1 key was not threaded through; "+
			"TTL purge keeps cache correct but stale-while-revalidate is degraded for this entry"),
	)
}

// SetDepsTestMode flips the O15 test-mode toggle. Exported for the
// cross-package _test.go shim; production code MUST NOT call it. When
// true, an empty-l1Key Record/RecordList/WithL1KeyContext panics instead
// of WARNing. The toggle is a Go variable, never an env var, so a
// customer deployment can never enable process-killing behaviour.
func SetDepsTestMode(on bool) {
	depsTestMode.Store(on)
}

// L1KeyFromContext returns the L1 key attached to ctx by
// WithL1KeyContext. Returns "" when no key was attached (the resolver
// must treat empty as "do not record").
func L1KeyFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(ctxKeyL1Record).(string)
	return v
}

// NOTE — the Ship 0.30.118 WithRefreshBypass / RefreshBypassFromContext
// machinery was REMOVED in Ship F1 (0.30.119). It existed only because
// the Ship E api-stage entry was per-STAGE and the refresher's
// whole-RESTAction re-resolve self-hit it through its own stage loop.
// F1's api-stage entry is per-K8s-CALL content: the refresher
// re-dispatches the one K8s call directly (resolve_populate.go) — it
// never self-Gets a content entry — so the self-hit is structurally
// eliminated and the marker is dead code. Removed in F1, not left inert
// (team decision — no dead code).

// ctxKeyApistageContentResolveType is the typed context key for the
// Ship F1 (0.30.119) apistage-content-resolve marker. Distinct
// unexported type — same collision-safety as the other ctx keys.
type ctxKeyApistageContentResolveType struct{}

var ctxKeyApistageContentResolve = ctxKeyApistageContentResolveType{}

// WithApistageContentResolve returns a child context marked as an
// api-stage CONTENT resolve (Ship F1 — content-keyed cache + serve-time
// RBAC gate). The api-stage resolver sets it on the per-stage resolve
// context when apistage L1 is active; dispatchViaInformer consults it
// via ApistageContentResolveFromContext.
//
// THE INVARIANT it establishes: the api-stage L1 entry is IDENTITY-FREE
// content (Ship F1 ComputeKey drops Username/Groups from the apistage
// key) — it must therefore store UN-GATED content, never one user's
// RBAC-narrowed view. When this marker is set dispatchViaInformer SKIPS
// its inline filterListByRBAC / filterGetByRBAC so the dispatch returns
// the raw, un-narrowed indexer items / object. The per-user RBAC gate
// then runs at a SINGLE site in the stage loop — on BOTH the Get-hit and
// the fresh-dispatch-miss path — before the content lands in dict[id].
// That closes the hit-path leak (pre-F1 the inline gate fired only on a
// miss; a Get-hit served the previous resolver's narrowed view).
//
// Request-path resolves without apistage L1 never carry this marker, so
// dispatchViaInformer gates inline exactly as before — byte-identical.
//
// Mirrors WithRefreshBypass: a context.Value flag, no parameter threaded
// through the resolver call chain. A nil ctx is returned unchanged.
func WithApistageContentResolve(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyApistageContentResolve, true)
}

// ApistageContentResolveFromContext reports whether ctx was marked by
// WithApistageContentResolve — i.e. whether this dispatch feeds an
// identity-free api-stage content entry and must therefore return
// UN-GATED content (the per-user gate runs later, at the stage loop's
// single gate site).
func ApistageContentResolveFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyApistageContentResolve).(bool)
	return v
}

// ctxKeyApistagePrewarmType is the typed context key for the Ship F1
// prewarm skip-hook marker. F1 only defines it; Ship F2's SA prewarm
// walk sets it.
type ctxKeyApistagePrewarmType struct{}

var ctxKeyApistagePrewarm = ctxKeyApistagePrewarmType{}

// WithApistagePrewarm returns a child context marked as an api-stage
// PREWARM resolve — Ship F1 defines the marker; Ship F2's SA prewarm
// walk sets it.
//
// A prewarm resolve has NO requester: it resolves navigation as the
// snowplow ServiceAccount purely to POPULATE the identity-free content
// layer. The api-stage content pipeline (apistageContentServe) consults
// this via ApistagePrewarmFromContext: when set it does the content Get
// / un-gated dispatch / Put but SKIPS the per-user RBAC gate and the
// dict[id] assembly — there is no identity to gate against and the
// prewarm discards the resolved dict. Mirrors Ship E's
// (nil,nil)-on-refresh contract: the side effect (a warmed content
// entry) is the whole point; the resolved output is thrown away.
//
// In F1 itself nothing sets this — every resolve is a real request and
// the gate always runs. The marker + the skip-point exist now so F2 is
// a thin, low-risk addition.
func WithApistagePrewarm(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyApistagePrewarm, true)
}

// ctxKeyPrewarmIterSerialType is the typed context key for the Ship F2
// (0.30.125) serial-inner-call marker.
type ctxKeyPrewarmIterSerialType struct{}

var ctxKeyPrewarmIterSerial = ctxKeyPrewarmIterSerialType{}

// WithPrewarmIterSerial returns a child context marked so the RESTAction
// resolver runs inner-call fan-out SERIALLY — iterParallelism returns 1
// for a resolve under this context.
//
// Ship F2 (0.30.125): the SA content-population pass does the full
// per-namespace `dependsOn.iterator` fan-out — the #159 OOM territory.
// (Ship 0.30.127 deleted phase1IteratorCap, so every resolve expands the
// iterator fully; the content pass is no exception.) The content pass is
// behind the 503 readiness gate with no latency budget, so it trades
// wall-clock for peak RSS by forcing the fan-out serial. This is
// CONTEXT-SCOPED — a process-wide RESOLVER_ITER_PARALLELISM=1 would slow
// every real /call; the marker only narrows the prewarm pass.
func WithPrewarmIterSerial(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyPrewarmIterSerial, true)
}

// PrewarmIterSerialFromContext reports whether ctx was marked by
// WithPrewarmIterSerial — i.e. whether the resolver must run inner-call
// fan-out serially (iterParallelism == 1).
func PrewarmIterSerialFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyPrewarmIterSerial).(bool)
	return v
}

// ApistagePrewarmFromContext reports whether ctx was marked by
// WithApistagePrewarm — i.e. whether this is an SA prewarm resolve that
// populates the content layer without a per-user gate.
func ApistagePrewarmFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyApistagePrewarm).(bool)
	return v
}

// ctxKeyBackgroundResolveType is the typed context key marking a resolve as
// BACKGROUND (non-customer): the refresher re-resolve + the prewarm/seed
// content-population passes. A customer /call carries NO such marker.
type ctxKeyBackgroundResolveType struct{}

var ctxKeyBackgroundResolve = ctxKeyBackgroundResolveType{}

// WithBackgroundResolve marks ctx as a background (non-customer) resolve. Used
// by the aggregate cold-fan-out admission gate (nested_resolve_bound.go, C5)
// to give a CUSTOMER /call ABSOLUTE priority: a background tree YIELDS the
// admission race to any waiting/arriving customer tree (customer-preferring
// acquire) and never gets an honest-503 terminal (it has no browser deadline —
// it waits, ctx-bounded by its own ctx). It is NOT a per-tree RBAC/behaviour
// change: a background tree, ONCE ADMITTED, weighs against the aggregate
// exactly like a customer tree (the OOM floor is preserved — background differs
// only at admission, never in accounting). This is a code-internal signal, not
// an operator knob.
func WithBackgroundResolve(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyBackgroundResolve, true)
}

// BackgroundResolveFromContext reports whether ctx was marked by
// WithBackgroundResolve — i.e. whether this resolve is background (refresher /
// prewarm), which the aggregate admission gate de-prioritises behind customer
// /calls.
func BackgroundResolveFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyBackgroundResolve).(bool)
	return v
}

// ctxKeyInformerOnlyReadsType is the typed context key marking a ctx whose
// objects.Get reads MUST be informer-only — the apiserver fallthrough is
// unreachable-by-design and must not be attempted (#101).
type ctxKeyInformerOnlyReadsType struct{}

var ctxKeyInformerOnlyReads = ctxKeyInformerOnlyReadsType{}

// WithInformerOnlyReads marks ctx so objects.Get serves ONLY from the
// in-process informer cache: on any routed-branch fallthrough (GET-miss,
// not-servable, RBAC-denied, parse failure) it returns a NotFound-shaped Err
// INSTEAD of calling getFromAPIServer.
//
// THE DEFECT IT KILLS (#101, docs/refreshes-arming-endpoint-storm-trace-2026-07-04.md).
// The GET /refreshes arming path is authed by middleware.RefreshAuth, which
// attaches UserInfo but DELIBERATELY NO UserConfig (the route "needs no
// <user>-clientconfig Secret lookup", main.go). subscriptionKeyExtras calls
// objects.Get per widget/widgetContent coord to reconstruct the inline-extras
// union; on an informer GET-miss coord the routed branch falls through to
// getFromAPIServer, which dies INSTANTLY at the endpoint read (xcontext.UserConfig
// → "unable to get user endpoint" ERROR + 401-shaped Err) — no I/O, just noise.
// The coord is then fail-closed-skipped anyway. This marker makes the doomed
// fallthrough a quiet NotFound so the skip stays silent; the SERVE SET IS
// UNCHANGED (the fallthrough never succeeded on this endpoint-less route).
//
// It is a GENERIC capability like WithInternalRESTConfig — a ctx-scoped read
// mode, NOT a per-route/resource/user special-case in objects.Get
// (feedback_no_special_cases). Any endpoint-less internal driver that must not
// touch the apiserver can set it.
func WithInformerOnlyReads(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyInformerOnlyReads, true)
}

// InformerOnlyReadsFromContext reports whether ctx was marked by
// WithInformerOnlyReads — i.e. whether objects.Get must skip the apiserver
// fallthrough and return a NotFound-shaped Err instead.
func InformerOnlyReadsFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyInformerOnlyReads).(bool)
	return v
}

// ctxKeyRefreshTriggerGVRType is the typed context key for the R1 Layer 1
// refresh-trigger-GVR marker.
type ctxKeyRefreshTriggerGVRType struct{}

var ctxKeyRefreshTriggerGVR = ctxKeyRefreshTriggerGVRType{}

// WithRefreshTriggerGVR returns a child context carrying the GVR whose
// dirty-mark TRIGGERED this refresher re-resolve (R1 Layer 1). It is set
// ONLY by the refresher's re-resolve entry point (resolve_populate.go),
// NEVER on a request-path /call — so a marked ctx is structurally proof
// that this resolve is a refresher re-resolve driven by a specific GVR
// event.
//
// THE INVARIANT it establishes: during a refresher re-resolve triggered by
// GVR X, apistageContentServe must NOT serve a stale content HIT for a
// content entry whose OWN dep GVR == X — it must re-dispatch that unit
// fresh, so the whole-RA re-resolve consumes the FRESH input rather than a
// sibling stage's stale content snapshot (the content-shield defect, R1
// §3). The comparison is dep-edge equality (entry's GVR == trigger GVR),
// UNIFORM across every GVR — no per-resource/path special-case
// (feedback_no_special_cases). The request path never carries the marker,
// so apistageContentServe's HIT branch is byte-identical for real /call.
//
// A nil ctx is returned unchanged.
func WithRefreshTriggerGVR(ctx context.Context, gvr schema.GroupVersionResource) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyRefreshTriggerGVR, gvr)
}

// RefreshTriggerGVRFromContext returns the trigger GVR set by
// WithRefreshTriggerGVR and true, or a zero GVR and false when the ctx is
// not a refresher re-resolve (every request-path /call). apistageContentServe
// consults it to decide a forced-miss on dep-edge equality.
func RefreshTriggerGVRFromContext(ctx context.Context) (schema.GroupVersionResource, bool) {
	if ctx == nil {
		return schema.GroupVersionResource{}, false
	}
	v, ok := ctx.Value(ctxKeyRefreshTriggerGVR).(schema.GroupVersionResource)
	return v, ok
}

// Dependency env knobs.
const (
	envDepsMaxRecords = "DEPS_MAX_RECORDS"

	defaultDepsMaxRecords int64 = 1_000_000

	// listWildcard is the sentinel Name value indicating list-scope.
	// Picked as "*" to mirror the plan's prose ("name=*"). Real K8s
	// object names cannot contain "*" (validated by apiserver), so
	// there is no namespace collision.
	listWildcard = "*"
)

// depsTestMode, when true, makes the empty-l1Key loud-fail (O15) panic
// instead of WARN+counter. It is a package-private Go variable flipped
// ONLY by the *_test.go shim SetDepsTestMode — never read from an env
// var, so a customer can never accidentally turn process-killing
// behaviour on in production. Default false (production semantics).
var depsTestMode atomic.Bool

// DepKey identifies a (gvr, namespace, name) tuple in the dependency map.
// Name == "*" indicates the bucket is a list-scope dependency rather
// than an exact-object dependency.
type DepKey struct {
	GVR       schema.GroupVersionResource
	Namespace string
	Name      string
}

// keySet is the L1-key set stored under a forward DepKey bucket. A
// sync.Map plus an atomic counter (so we can prune empty buckets
// without scanning) keeps the cleanup path lock-free.
type keySet struct {
	keys  sync.Map // map[string]struct{}  (l1Key -> {})
	count atomic.Int64
}

// depSet is the DepKey set stored under a reverse l1Key index entry.
// Same shape as keySet — sync.Map + count.
type depSet struct {
	deps  sync.Map // map[DepKey]struct{}
	count atomic.Int64
}

// DepTracker is the package-private dependency map. The exported entry
// point is the package-level singleton accessed via Deps(); production
// code MUST NOT instantiate DepTracker directly so the eviction +
// refresher hooks share a single state.
type DepTracker struct {
	// forward: DepKey -> *keySet
	forward sync.Map
	// reverse: l1Key -> *depSet
	reverse sync.Map

	// totalRecords is the global record count — bounded by maxRecords.
	totalRecords atomic.Int64
	maxRecords   int64

	// Falsifier counters (atomic; safe to read without holding anything).
	recordTotal        atomic.Uint64
	recordDroppedCap   atomic.Uint64
	recordDroppedNoKey atomic.Uint64 // O15: Record*/WithL1KeyContext with empty l1Key
	evictDeleteTotal   atomic.Uint64 // L1 self-representation evictions (OnDelete ONLY — the H1 live discriminator)
	evictSelfGoneTotal atomic.Uint64 // 1.12.5 #187: evictions from a confirmed self-object 404 (EvictSelfGone)
	dirtyMarkTotal     atomic.Uint64 // dirty-marks (OnAdd/OnUpdate + OnDelete non-self)
	enqueueUpdateTotal atomic.Uint64 // refresh enqueues triggered by OnUpdate
	removeL1Total      atomic.Uint64 // RemoveL1Key calls (LRU + DELETE cleanup)

	// One-shot WARN flag for cap reached. We only want to log once
	// (process lifetime) so the log file doesn't fill with the same
	// line every record under steady-state pressure.
	capWarned atomic.Bool

	// store is the L1 resolved-output cache instance OnDelete evicts
	// from. Wired by SetDepTrackerStore; nil-safe (lookups still
	// record, but OnDelete becomes a no-op until the store is wired,
	// which is fine for unit tests that exercise the dep tracker
	// alone).
	storeMu sync.RWMutex
	store   *ResolvedCacheStore

	// enqueueFn is the refresher hook OnUpdate calls. Wired by
	// SetRefreshHook; nil-safe (OnUpdate becomes a no-op). R1 Layer 1: the
	// hook now also receives the GVR whose event triggered the dirty-mark,
	// so the refresher can carry it to the re-resolve (WithRefreshTriggerGVR)
	// for the dep-edge-equality forced-miss.
	enqueueMu sync.RWMutex
	enqueueFn func(l1Key string, triggerGVR schema.GroupVersionResource)
}

// depsInstance is the singleton — lazily initialised on first call to
// Deps(). The ResolvedCache singleton wiring (resolved.go) installs the
// cache store as soon as it constructs the cache; the refresher startup
// installs the enqueue hook.
var (
	depsInstance *DepTracker
	depsOnce     sync.Once
)

// Deps returns the process-wide dependency tracker, lazily initialising
// it on first use. Always non-nil — the tracker is cheap to allocate
// even when L1 is disabled (it just never sees Record calls).
func Deps() *DepTracker {
	depsOnce.Do(func() {
		depsInstance = newDepTracker(
			int64BytesFromEnv(envDepsMaxRecords, defaultDepsMaxRecords),
		)
	})
	return depsInstance
}

func newDepTracker(maxRecords int64) *DepTracker {
	if maxRecords <= 0 {
		maxRecords = defaultDepsMaxRecords
	}
	return &DepTracker{
		maxRecords: maxRecords,
	}
}

// SetStore wires the L1 resolved-output cache the tracker evicts from
// on DELETE events. Safe to call multiple times; later calls replace
// the earlier wiring (used by tests).
//
// Production wiring lives in ResolvedCache(): once the singleton is
// built, it calls Deps().SetStore(self). The cache then knows to call
// Deps().RemoveL1Key when LRU eviction drops an entry, so dep records
// don't outlive their L1 entry.
func (d *DepTracker) SetStore(s *ResolvedCacheStore) {
	d.storeMu.Lock()
	d.store = s
	d.storeMu.Unlock()
}

// SetRefreshHook wires the refresher enqueue function. Safe to call
// multiple times; later calls replace the earlier wiring.
//
// The hook is called with an L1 key string + the GVR whose event matched
// this dependent entry (R1 Layer 1 — the trigger GVR), for each dependent
// entry matched by OnUpdate/OnAdd/OnDelete. The refresher is responsible
// for dedup, ordering, and the actual re-resolve, and carries the trigger
// GVR to the re-resolve so apistageContentServe can force-miss a content
// entry keyed on that same GVR.
func (d *DepTracker) SetRefreshHook(fn func(l1Key string, triggerGVR schema.GroupVersionResource)) {
	d.enqueueMu.Lock()
	d.enqueueFn = fn
	d.enqueueMu.Unlock()
}

// Record stores an exact-object dependency edge: l1Key depends on
// (gvr, namespace, name). Idempotent: repeated calls with the same
// arguments are no-ops after the first. Sub-microsecond hot-path cost
// (two sync.Map.LoadOrStore + two atomic.Add).
//
// When the global record cap is reached, the call is silently dropped
// (counter `record_dropped_cap_total` increments). The first cap-hit
// also emits a one-shot WARN log line.
func (d *DepTracker) Record(l1Key string, gvr schema.GroupVersionResource, namespace, name string) {
	if d == nil {
		return
	}
	if l1Key == "" {
		// O15: a Record call with no L1 key is an unambiguous bug — a
		// DepKey with nowhere to attach it. Loud-fail.
		loudFailEmptyL1Key("Record")
		return
	}
	if name == "" {
		// Empty name + non-empty namespace is meaningless — guard
		// against accidental "ns-only" records. Callers wanting
		// list-scope must use RecordList explicitly.
		return
	}
	d.recordInternal(l1Key, DepKey{GVR: gvr, Namespace: namespace, Name: name})
}

// RecordList stores a list-scope dependency edge: l1Key depends on
// every object of (gvr) in namespace (or cluster-wide when namespace is
// ""). Internally encodes the bucket as (gvr, namespace, "*").
func (d *DepTracker) RecordList(l1Key string, gvr schema.GroupVersionResource, namespace string) {
	if d == nil {
		return
	}
	if l1Key == "" {
		// O15: same loud-fail as Record — a list-scope edge with no L1
		// key cannot be attached anywhere.
		loudFailEmptyL1Key("RecordList")
		return
	}
	d.recordInternal(l1Key, DepKey{GVR: gvr, Namespace: namespace, Name: listWildcard})
}

// recordInternal is the shared body of Record + RecordList. Idempotent;
// honours the global cap.
func (d *DepTracker) recordInternal(l1Key string, dk DepKey) {
	// Forward: DepKey -> *keySet[l1Key]
	ksI, _ := d.forward.LoadOrStore(dk, &keySet{})
	ks := ksI.(*keySet)
	if _, loaded := ks.keys.LoadOrStore(l1Key, struct{}{}); loaded {
		return // already recorded — idempotent no-op
	}
	// At this point we are committing a NEW edge. Bound-check first.
	if d.totalRecords.Load() >= d.maxRecords {
		// Cap reached — roll back the LoadOrStore on the forward side.
		// In the rare race where the cap moves between the load and the
		// add, we accept the off-by-one (worst case 1 extra record).
		ks.keys.Delete(l1Key)
		d.recordDroppedCap.Add(1)
		if d.capWarned.CompareAndSwap(false, true) {
			slog.Warn("deps.record.cap_reached",
				slog.String("subsystem", "cache"),
				slog.Int64("max_records", d.maxRecords),
				slog.String("hint", "subsequent records will be dropped silently — TTL purge keeps cache correct"),
			)
		}
		return
	}
	ks.count.Add(1)
	d.totalRecords.Add(1)
	d.recordTotal.Add(1)

	// Reverse: l1Key -> *depSet[DepKey]
	dsI, _ := d.reverse.LoadOrStore(l1Key, &depSet{})
	ds := dsI.(*depSet)
	if _, loaded := ds.deps.LoadOrStore(dk, struct{}{}); !loaded {
		ds.count.Add(1)
	}
}

// hasEdge reports whether l1Key is recorded under the forward bucket dk —
// i.e. whether an ABSENT verdict for dk can reach l1Key at all. An entry
// with no edge (its Record was dropped at DEPS_MAX_RECORDS — dropped_cap)
// is dirty-markable by nothing and evictable by nothing but its TTL; the
// reconcile audit (1.12.6 C3) counts such entries apart from divergence
// because submitting their coordinate would evict nothing. Lock-free
// (sync.Map loads).
func (d *DepTracker) hasEdge(l1Key string, dk DepKey) bool {
	if d == nil {
		return false
	}
	ksI, ok := d.forward.Load(dk)
	if !ok {
		return false
	}
	_, ok = ksI.(*keySet).keys.Load(l1Key)
	return ok
}

// objectState is what the dep-event worker derived about an object at the
// moment it processed the coordinate (1.12.6 C1, design §3.3). It is the
// ONLY input OnObjectEvent uses to choose between evict and dirty-mark.
type objectState uint8

const (
	// objUnknown — the informer indexer is not authoritative for the GVR
	// (torn down, not synced, watch broken, type unconfirmed, passthrough).
	// The worker requeues on the shared budget; OnObjectEvent never sees it.
	objUnknown objectState = iota
	// objExists — the informer is servable and its indexer holds the object.
	objExists
	// objAbsent — the informer is servable and its indexer does NOT hold the
	// object: deleted. The only state that evicts a self-representation.
	objAbsent
	// objUnknownDegraded — objUnknown for the whole requeue budget; the
	// worker degrades to a dirty-mark so the refresher's re-fetch decides
	// against the apiserver (a definite 404 evicts at the drop point).
	objUnknownDegraded
)

func (s objectState) String() string {
	switch s {
	case objExists:
		return "EXISTS"
	case objAbsent:
		return "ABSENT"
	case objUnknownDegraded:
		return "UNKNOWN_DEGRADED"
	default:
		return "UNKNOWN"
	}
}

// OnObjectEvent is the single decision site of the event-driven L1
// invalidation (1.12.6 C1). The dep-event worker calls it with the
// coordinate an informer event announced and the object's CURRENT state,
// probed from the informer at processing time — never with the event type.
//
// Every matched L1 entry falls into exactly one bucket (R2/R7 contract,
// unchanged since 0.30.110):
//
//  1. self-representation — the entry's OWN dispatched object is this
//     object. EVICT iff state == objAbsent (the object is gone). When the
//     object exists — a same-name recreate, a late duplicate DELETE, an
//     UPDATE — the entry is DIRTY-MARKED (stale-while-revalidate), never
//     evicted: this is what makes duplicates idempotent and a late DELETE
//     harmless to a correct fresh entry.
//  2. LIST-dep — matched via the (gvr, ns, "*") wildcard bucket. The
//     entry's own object is a DIFFERENT object; one member of a list it
//     depends on arrived, changed or left → DIRTY-MARK, whatever the state.
//  3. dependent-GET-dep — matched via an exact bucket but its own object is
//     a different object (a widget GET-depending on a RESTAction) →
//     DIRTY-MARK, whatever the state.
//
// Evictions are applied BEFORE dirty-marks, so a panic raised by the
// refresh hook (the one seam this function calls out through) cannot lose
// an eviction that was already due.
//
// Eviction stays DELETE-only in MEANING (feedback_l1_invalidation_delete_only):
// objAbsent IS the informer's DELETE fact, derived from state instead of
// carried by the message. Returns (evicted, dirtyMarked).
func (d *DepTracker) OnObjectEvent(gvr schema.GroupVersionResource, namespace, name string, state objectState) (int, int) {
	if d == nil {
		return 0, 0
	}
	matched := d.collectMatchesWithDep(gvr, namespace, name)
	if len(matched) == 0 {
		return 0, 0
	}
	d.storeMu.RLock()
	store := d.store
	d.storeMu.RUnlock()
	d.enqueueMu.RLock()
	enqueue := d.enqueueFn
	d.enqueueMu.RUnlock()

	self := DepKey{GVR: gvr, Namespace: namespace, Name: name}

	var toEvict, toMark []string
	for l1Key := range matched {
		if state == objAbsent && d.isSelfRepresentation(store, l1Key, self) {
			toEvict = append(toEvict, l1Key) // bucket 1, and ONLY when absent
			continue
		}
		toMark = append(toMark, l1Key) // buckets 2 + 3, and self-when-present
	}

	if len(toEvict) > 0 {
		d.runEvictionBatch(toEvict)
	}
	for _, l1Key := range toMark {
		if enqueue != nil {
			enqueue(l1Key, gvr)
		}
	}
	if n := len(toMark); n > 0 {
		d.dirtyMarkTotal.Add(uint64(n))
		if state != objAbsent {
			// enqueueUpdateTotal is retained as the pre-0.30.110 falsifier
			// name for ADD/UPDATE-driven refresh enqueues.
			d.enqueueUpdateTotal.Add(uint64(n))
		}
	}

	action := "refresh"
	if len(toEvict) > 0 {
		action = "evict+refresh"
	}
	slog.Info("cache_event.consumed",
		slog.String("subsystem", "cache"),
		slog.String("type", state.String()),
		slog.String("gvr", gvr.String()),
		slog.String("ns", namespace),
		slog.String("name", name),
		slog.String("action", action),
		slog.Int("l1_keys", len(matched)),
		slog.Int("evicted", len(toEvict)),
		slog.Int("dirty_marked", len(toMark)),
	)
	return len(toEvict), len(toMark)
}

// OnAdd is a shim over OnObjectEvent for callers that still speak in event
// types (the 0.30.110 test surface). Production handlers no longer call it:
// they enqueue the coordinate and the worker derives the state. An ADD
// means the object exists → dirty-mark every dependent, never evict.
// Returns the number of L1 keys dirty-marked.
func (d *DepTracker) OnAdd(gvr schema.GroupVersionResource, namespace, name string) int {
	_, marked := d.OnObjectEvent(gvr, namespace, name, objExists)
	return marked
}

// OnUpdate is a shim over OnObjectEvent (see OnAdd). An UPDATE means the
// object exists → dirty-mark every dependent (stale-while-revalidate),
// never evict. Returns the number of L1 keys dirty-marked.
func (d *DepTracker) OnUpdate(gvr schema.GroupVersionResource, namespace, name string) int {
	_, marked := d.OnObjectEvent(gvr, namespace, name, objExists)
	return marked
}

// OnDelete is a shim over OnObjectEvent (see OnAdd). A DELETE means the
// object is absent → evict its self-representation, dirty-mark buckets
// 2/3. Returns the number of L1 keys EVICTED.
func (d *DepTracker) OnDelete(gvr schema.GroupVersionResource, namespace, name string) int {
	evicted, _ := d.OnObjectEvent(gvr, namespace, name, objAbsent)
	return evicted
}

// isSelfRepresentation reports whether the L1 entry under l1Key is the
// resolved output of the `deleted` object itself (bucket 1). It reads
// the entry's Inputs from the store and compares (Group/Version/
// Resource, Namespace, Name).
//
// When the store is nil or the entry / its Inputs are unavailable, it
// returns false — the conservative direction: a non-self classification
// dirty-marks (stale-while-revalidate) rather than evicts. Missing an
// eviction merely leaves a stale entry until TTL; an over-eviction is
// the regression F2 catches.
func (d *DepTracker) isSelfRepresentation(store *ResolvedCacheStore, l1Key string, deleted DepKey) bool {
	if store == nil {
		return false
	}
	entry, ok := store.Get(l1Key)
	if !ok || entry == nil || entry.Inputs == nil {
		return false
	}
	in := entry.Inputs
	return in.Group == deleted.GVR.Group &&
		in.Version == deleted.GVR.Version &&
		in.Resource == deleted.GVR.Resource &&
		in.Namespace == deleted.Namespace &&
		in.Name == deleted.Name
}

// OnResourceTypeAvailable is invoked by the CRD-watch when a CRD newly
// appears at runtime (EnsureResourceType returned added==true for a
// genuinely-new GVR). D1 (Ship D, 0.30.114).
//
// A compositions-list resolve that ran BEFORE the CRD existed records a
// LIST-scope dep and caches `0 items`; once the CRD appears the cached
// result is stale-negative. This scans the forward index for every
// LIST-scope bucket matching gvr (every namespace AND the cluster-wide
// "" namespace) and dirty-marks the dependent L1 keys via the same
// refreshHook onChange uses.
//
// It deliberately ignores EXACT-object GET-dep buckets: an exact GET-dep
// for a named object that did not resolve before the CRD existed is not
// a stale-negative LIST and is left to OnAdd when the object itself
// arrives. Dirty-mark only — NEVER evicts.
//
// Returns the number of dependent L1 keys dirty-marked. AC-D4: a no-op
// (and idempotent) when no LIST-dep matches gvr.
func (d *DepTracker) OnResourceTypeAvailable(gvr schema.GroupVersionResource) int {
	if d == nil {
		return 0
	}
	matched := d.collectTypeMatches(gvr, true /* listOnly */)
	return d.dirtyMarkResourceType("CRD_ADD", gvr, matched)
}

// OnResourceTypeRemoved is invoked by the CRD-DELETE event bridge
// (triggerCRDDelete, crd_discovery_side_effect.go) when a CRD is removed
// at runtime — the original CRD-watch was deleted at v6 (0.30.223); the
// bridge replaced it at Ship L (0.30.246). D2 (Ship D, 0.30.114).
//
// Unlike OnDelete (a single object's DELETE), a CRD removal is a
// TYPE-removal — every L1 entry that LIST-depends on the GVR, OR
// GET-depends on any named object of the GVR, is now stale. This scans
// every forward bucket whose DepKey.GVR == gvr (LIST wildcard AND exact
// GET buckets, all namespaces) and dirty-marks the dependent L1 keys.
//
// Dirty-mark only — NEVER evicts, even a self-representation entry:
// feedback_l1_invalidation_delete_only.md authorises eviction ONLY for a
// single object's DELETE. A CRD removal mirrors OnDelete's non-self
// dependent-bucket handling (stale-while-revalidate).
//
// Returns the number of dependent L1 keys dirty-marked. AC-D4: a no-op
// (and idempotent) when no dep matches gvr.
func (d *DepTracker) OnResourceTypeRemoved(gvr schema.GroupVersionResource) int {
	if d == nil {
		return 0
	}
	matched := d.collectTypeMatches(gvr, false /* listOnly */)
	return d.dirtyMarkResourceType("CRD_DELETE", gvr, matched)
}

// OnResourceTypeSchemaRelisted is invoked by the CRD schema-widen relist
// (triggerCRDSchemaRelist, crd_discovery_side_effect.go) when a CRD's
// structural schema CHANGED at runtime and its data informer was relisted.
// The GVR is NOT being removed — its informer is re-LISTing under the now-
// wider schema — but every L1 entry that LIST- or GET-depends on the GVR was
// resolved against the PRE-widen (pruned) objects and is now stale, so it
// must dirty-mark the same dependent-bucket set OnResourceTypeRemoved does.
//
// Mechanically identical to OnResourceTypeRemoved (same collectTypeMatches
// scan, same dirty-mark-only, NEVER-evict contract) — it differs ONLY in the
// telemetry label: it logs cache_event.consumed type=SCHEMA_RELIST, not
// type=CRD_DELETE, so the relist's dirty-mark does not masquerade as a CRD
// deletion in logs/metrics. dirtyMarkResourceType already parameterises the
// event type, so this is a label-only divergence.
//
// Returns the number of dependent L1 keys dirty-marked. A no-op (and
// idempotent) when no dep matches gvr.
func (d *DepTracker) OnResourceTypeSchemaRelisted(gvr schema.GroupVersionResource) int {
	if d == nil {
		return 0
	}
	matched := d.collectTypeMatches(gvr, false /* listOnly */)
	return d.dirtyMarkResourceType("SCHEMA_RELIST", gvr, matched)
}

// dirtyMarkResourceType dirty-marks every L1 key in matched via the
// refreshHook — the shared body of OnResourceTypeAvailable +
// OnResourceTypeRemoved. NEVER evicts. Returns the number marked.
func (d *DepTracker) dirtyMarkResourceType(eventType string, gvr schema.GroupVersionResource, matched map[string]struct{}) int {
	if len(matched) == 0 {
		return 0
	}
	d.enqueueMu.RLock()
	enqueue := d.enqueueFn
	d.enqueueMu.RUnlock()

	marked := 0
	for l1Key := range matched {
		if enqueue != nil {
			enqueue(l1Key, gvr)
		}
		marked++
	}
	if marked > 0 {
		d.dirtyMarkTotal.Add(uint64(marked))
	}
	slog.Info("cache_event.consumed",
		slog.String("subsystem", "cache"),
		slog.String("type", eventType),
		slog.String("gvr", gvr.String()),
		slog.String("action", "refresh"),
		slog.Int("l1_keys", marked),
	)
	return marked
}

// collectTypeMatches scans the forward index for every bucket whose
// GVR == gvr and returns the union of dependent L1 keys. The CRD-watch
// lifecycle scan — it matches by GVR alone (every namespace), unlike
// collectMatches which point-looks-up a specific (gvr, ns, name) tuple.
//
//   - listOnly == true (D1, CRD-add): only LIST-scope buckets
//     (Name == listWildcard) — a stale-negative LIST is the only entry
//     a CRD-add can invalidate.
//   - listOnly == false (D2, CRD-delete): every bucket — LIST wildcard
//     AND exact GET — since a type-removal invalidates both.
//
// A forward-index Range is O(distinct DepKeys); CRD-add/delete is a rare
// event so the scan cost is paid only at CRD-lifecycle time, never on a
// resolver hot path.
func (d *DepTracker) collectTypeMatches(gvr schema.GroupVersionResource, listOnly bool) map[string]struct{} {
	out := map[string]struct{}{}
	d.forward.Range(func(k, v any) bool {
		dk := k.(DepKey)
		if dk.GVR != gvr {
			return true
		}
		if listOnly && dk.Name != listWildcard {
			return true
		}
		v.(*keySet).keys.Range(func(kk, _ any) bool {
			out[kk.(string)] = struct{}{}
			return true
		})
		return true
	})
	return out
}

// collectMatches returns the union of dependent L1 keys across the four
// bucket forms. Retained as the bare-set form for onChange (ADD/UPDATE),
// which dirty-marks every match uniformly and has no need for the
// matching DepKey.
func (d *DepTracker) collectMatches(gvr schema.GroupVersionResource, namespace, name string) map[string]struct{} {
	out := map[string]struct{}{}
	for k := range d.collectMatchesWithDep(gvr, namespace, name) {
		out[k] = struct{}{}
	}
	return out
}

// collectMatchesWithDep returns the union of dependent L1 keys across
// the four bucket forms, each paired with the DepKey it matched
// through. When an L1 key matches more than one bucket, an EXACT-object
// bucket takes precedence over a LIST wildcard bucket — so OnDelete's
// classification sees the most specific dependency form.
func (d *DepTracker) collectMatchesWithDep(gvr schema.GroupVersionResource, namespace, name string) map[string]DepKey {
	out := map[string]DepKey{}
	addAll := func(dk DepKey) {
		ksI, ok := d.forward.Load(dk)
		if !ok {
			return
		}
		ks := ksI.(*keySet)
		ks.keys.Range(func(k, _ any) bool {
			l1 := k.(string)
			prev, seen := out[l1]
			// Exact (Name != listWildcard) beats wildcard; once an
			// exact match is recorded it is never downgraded.
			if !seen || (prev.Name == listWildcard && dk.Name != listWildcard) {
				out[l1] = dk
			}
			return true
		})
	}
	// Exact buckets first so they win the precedence check; wildcard
	// buckets only fill in keys not already matched exactly.
	addAll(DepKey{GVR: gvr, Namespace: namespace, Name: name})
	if namespace != "" {
		addAll(DepKey{GVR: gvr, Namespace: "", Name: name})
	}
	addAll(DepKey{GVR: gvr, Namespace: namespace, Name: listWildcard})
	if namespace != "" {
		addAll(DepKey{GVR: gvr, Namespace: "", Name: listWildcard})
	}
	return out
}

// runEvictionBatch drops each self-representation L1 key from the store
// and clears its dep records. Counts evictDeleteTotal — self-evictions
// only, per the R2/R7 counter contract.
func (d *DepTracker) runEvictionBatch(keys []string) {
	d.storeMu.RLock()
	store := d.store
	d.storeMu.RUnlock()

	evicted := 0
	for _, l1Key := range keys {
		if store != nil {
			if store.deleteForDep(l1Key) {
				evicted++
			}
		}
		d.RemoveL1Key(l1Key) // clear forward + reverse records
	}
	if evicted > 0 {
		d.evictDeleteTotal.Add(uint64(evicted))
	}
}

// EvictSelfGone evicts ONE L1 key whose own object has been observed
// gone by a path other than an informer DELETE event — today the sole
// caller is the refresher, whose re-fetch of the entry's OWN object came
// back with a definite apiserver 404 (1.12.5 / #187 (i)).
//
// WHY IT ROUTES THROUGH THE TRACKER. resolved.go states the invariant
// that DELETE-driven eviction flows through the DepTracker so
// RemoveL1Key runs alongside the store delete and the entry's dep
// records do not outlive it. A direct store.deleteForDep here would
// leave orphaned forward/reverse edges behind. This is runEvictionBatch
// for a single key.
//
// IT COUNTS ON ITS OWN COUNTER, NOT evictDeleteTotal (architect
// Finding 2). The first implementation folded it in, reasoning that an
// operator wants one number for "entries that left because their object
// went away". That breaks the H1 live discriminator: the documented
// procedure is to delete a throwaway CR with a live L1 entry and watch
// evict_delete_total — frozen means the DELETE bridge is dead. On a
// cluster that deletes CRs the self-404 path fires constantly (that is
// the point of the fix), so a folded counter moves for two unrelated
// reasons and the procedure stops working. One counter, one meaning:
// evict_delete_total stays informer-DELETE-driven, evictSelfGoneTotal
// carries this path.
//
// SCOPE. This is the SELF object only. A NotFound on an INNER call is
// the bucket-2/3 dirty-mark class (a child vanishing must never evict
// the parent) and never reaches here: the caller gates on the re-fetch
// of the entry's own CR, which happens before the resolve runs.
//
// Returns true iff an entry was actually removed from the store, so the
// caller can log exactly once per genuine eviction.
func (d *DepTracker) EvictSelfGone(l1Key string) bool {
	if d == nil || l1Key == "" {
		return false
	}
	d.storeMu.RLock()
	store := d.store
	d.storeMu.RUnlock()

	evicted := false
	if store != nil {
		evicted = store.deleteForDep(l1Key)
	}
	d.RemoveL1Key(l1Key)
	if evicted {
		d.evictSelfGoneTotal.Add(1)
	}
	return evicted
}

// RemoveL1Key drops every dep record associated with l1Key. Invoked by
// the L1 store's LRU eviction (and TTL eviction, and DELETE-driven
// eviction inside OnDelete) so dep records don't outlive their L1
// entry.
//
// Cheap: O(deps-of-this-key) sync.Map.Delete operations. No global
// lock.
func (d *DepTracker) RemoveL1Key(l1Key string) {
	if d == nil || l1Key == "" {
		return
	}
	dsI, ok := d.reverse.LoadAndDelete(l1Key)
	if !ok {
		return
	}
	ds := dsI.(*depSet)
	ds.deps.Range(func(k, _ any) bool {
		dk := k.(DepKey)
		if ksI, ok := d.forward.Load(dk); ok {
			ks := ksI.(*keySet)
			if _, hit := ks.keys.LoadAndDelete(l1Key); hit {
				newCount := ks.count.Add(-1)
				d.totalRecords.Add(-1)
				// Prune empty bucket — keeps the forward map from
				// growing unboundedly under churn. The check-then-
				// delete race is benign: a concurrent Record that
				// hits the deleted bucket simply LoadOrStores a
				// fresh keySet.
				if newCount == 0 {
					d.forward.CompareAndDelete(dk, ks)
				}
			}
		}
		return true
	})
	d.removeL1Total.Add(1)
}

// DepStats is a snapshot of the falsifier counters. All numbers are
// atomic and may drift by a single call between fields.
type DepStats struct {
	TotalRecords       int64
	MaxRecords         int64
	RecordTotal        uint64
	RecordDroppedCap   uint64
	RecordDroppedNoKey uint64 // O15: empty-l1Key Record*/WithL1KeyContext
	EvictDeleteTotal   uint64 // self-representation evictions from an informer DELETE only
	EvictSelfGoneTotal uint64 // 1.12.5 #187: evictions from a confirmed self-object 404
	DirtyMarkTotal     uint64 // dirty-marks (ADD/UPDATE + DELETE non-self)
	EnqueueUpdateTotal uint64
	RemoveL1Total      uint64
}

func (d *DepTracker) Stats() DepStats {
	if d == nil {
		return DepStats{}
	}
	return DepStats{
		TotalRecords:       d.totalRecords.Load(),
		MaxRecords:         d.maxRecords,
		RecordTotal:        d.recordTotal.Load(),
		RecordDroppedCap:   d.recordDroppedCap.Load(),
		RecordDroppedNoKey: d.recordDroppedNoKey.Load(),
		EvictDeleteTotal:   d.evictDeleteTotal.Load(),
		EvictSelfGoneTotal: d.evictSelfGoneTotal.Load(),
		DirtyMarkTotal:     d.dirtyMarkTotal.Load(),
		EnqueueUpdateTotal: d.enqueueUpdateTotal.Load(),
		RemoveL1Total:      d.removeL1Total.Load(),
	}
}

// resetDepsForTest tears the singleton down so each test sees a clean
// tracker. Exported only via the *_test.go shim — production code MUST
// NOT call this. Also clears the O15 test-mode toggle so a test that
// forgot to reset it cannot leak panic-on-empty-key into the next test.
func resetDepsForTest() {
	depsInstance = nil
	depsOnce = sync.Once{}
	depsTestMode.Store(false)
}

// ResetDepsForTest is the exported variant that lives outside _test.go
// so external packages (e.g., internal/handlers/dispatchers tests) can
// reset the singleton between cases. Production code MUST NOT call
// this; build tags would be cleaner but Go's module layout makes
// cross-package test helpers via _test.go awkward.
//
// Also tears down the informer→DepTracker bridge (0.30.110) so a
// cross-package test cannot leak the DELETE-eviction worker goroutine
// or stale bridge counters into the next case.
func ResetDepsForTest() {
	// Stop the dep-event worker FIRST: it reads Deps() on its own goroutine
	// (1.12.6 C1 — every event, not only DELETEs), so resetting the tracker
	// while it drains is a data race the -race detector reports.
	resetDepWatchForTest()
	resetDepsForTest()
}

// CollectMatchesForTest exposes the package-private collectMatches for
// cross-package tests. Returns the union of dependent L1 keys across
// the four bucket forms. Production code MUST NOT call this.
func (d *DepTracker) CollectMatchesForTest(gvr schema.GroupVersionResource, namespace, name string) map[string]struct{} {
	if d == nil {
		return nil
	}
	return d.collectMatches(gvr, namespace, name)
}

// envInt64 is a typed helper that re-uses int64FromEnv from resolved.go.
// Kept here as a thin wrapper purely for readability of the constants
// block above.
var _ = strconv.ParseInt // touched by int64FromEnv via resolved.go
var _ = os.Getenv        // same — int parsing lives in resolved.go
