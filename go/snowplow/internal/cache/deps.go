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
	// #375 (option c) — a resolve that sets an L1 key is a resolve-terminal-Put ENTRY.
	// Install the per-resolve dep-gen sink HERE (WithDepGenSink captures startSeq at entry,
	// before any dep read) so EVERY such resolve is guarded STRUCTURALLY — no per-entry
	// drift to enumerate. Each distinct resolve (distinct l1Key) gets its own sink; nested
	// inner-call dispatch PRESERVES the parent ctx (one sink per resolve). A gen-guarded Put
	// whose ctx has no sink (a resolve that Put without setting an L1 key — e.g. a content-
	// populate path keyed directly) is caught by remarkIfDepsMoved's nil-sink detector.
	return WithDepGenSink(context.WithValue(ctx, ctxKeyL1Record, l1Key))
}

// DepGenEpochNow returns the current dep-event sequence (#375). A customer handler
// captures it at ENTRY, BEFORE its first read of the dispatched CR (fetchObjectFn), and
// hands it to WithL1KeyContextFromEpoch: the CR is read before the L1 key is known, so
// WithL1KeyContext's own capture would come too late to see an edit of that CR.
func DepGenEpochNow() uint64 { return depEventSeq.Load() }

// DepGenStartSeqFromContext returns the startSeq of ctx's dep-gen sink and true, or 0
// and false when ctx carries no sink (#375 C2). A NESTED resolve whose input was read
// under the ENCLOSING resolve (apiref's RAFullList cell: the RESTAction is fetched under
// the widget ctx before fullCtx exists) hands it to WithL1KeyContextFromEpoch, so its own
// sink starts no later than that read.
func DepGenStartSeqFromContext(ctx context.Context) (uint64, bool) {
	s := depGenSinkFromContext(ctx)
	if s == nil {
		return 0, false
	}
	return s.startSeq, true
}

// WithL1KeyContextFromEpoch is WithL1KeyContext for a resolve whose entry reads came
// BEFORE the key was known (#375, CR self-dep, TL ruling): the sink's startSeq is the
// handler-entry epoch, and pre lists the coordinates already read under it — the
// dispatched CR's self coordinate — which the handler otherwise Records only AFTER the
// accepted Put. An edit of the CR anywhere in [fetch, Put] then remarks the Put.
func WithL1KeyContextFromEpoch(ctx context.Context, l1Key string, epoch uint64, pre ...DepKey) context.Context {
	if l1Key == "" {
		loudFailEmptyL1Key("WithL1KeyContextFromEpoch")
		return ctx
	}
	if ctx == nil {
		return ctx
	}
	if now := depEventSeq.Load(); epoch > now {
		epoch = now
	}
	return context.WithValue(context.WithValue(ctx, ctxKeyL1Record, l1Key), ctxKeyDepGenSink, &depGenSink{
		startSeq: epoch,
		deps:     append([]DepKey(nil), pre...),
		parent:   depGenSinkFromContext(ctx),
	})
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

// refreshTriggerSet is the multi-GVR form of the R1 Layer 1 marker (#375 B): the
// refresher accumulates EVERY GVR that dirty-marked / remarked a key before its dequeue
// (pre-#375 a second mark overwrote the first, so only the last GVR force-missed).
type refreshTriggerSet []schema.GroupVersionResource

// WithRefreshTriggerGVRs is WithRefreshTriggerGVR for a SET of trigger GVRs (#375 B):
// apistageContentServe force-misses a content cell whose own GVR is ANY member. Zero
// GVRs are dropped; an empty set returns ctx unchanged (no forced miss).
func WithRefreshTriggerGVRs(ctx context.Context, gvrs []schema.GroupVersionResource) context.Context {
	if ctx == nil {
		return ctx
	}
	set := make(refreshTriggerSet, 0, len(gvrs))
	for _, g := range gvrs {
		if !g.Empty() && !containsGVR(set, g) {
			set = append(set, g)
		}
	}
	switch len(set) {
	case 0:
		return ctx
	case 1:
		return context.WithValue(ctx, ctxKeyRefreshTriggerGVR, set[0])
	}
	return context.WithValue(ctx, ctxKeyRefreshTriggerGVR, set)
}

// RefreshTriggerGVRFromContext returns the trigger GVR set by
// WithRefreshTriggerGVR and true, or a zero GVR and false when the ctx is
// not a refresher re-resolve (every request-path /call). For a multi-GVR
// trigger set (WithRefreshTriggerGVRs) it returns the first member; use
// RefreshTriggerHas for the membership test.
func RefreshTriggerGVRFromContext(ctx context.Context) (schema.GroupVersionResource, bool) {
	if ctx == nil {
		return schema.GroupVersionResource{}, false
	}
	switch v := ctx.Value(ctxKeyRefreshTriggerGVR).(type) {
	case schema.GroupVersionResource:
		return v, true
	case refreshTriggerSet:
		if len(v) > 0 {
			return v[0], true
		}
	}
	return schema.GroupVersionResource{}, false
}

// RefreshTriggerHas reports whether gvr is a refresh trigger of this re-resolve
// (single marker or any member of a trigger set). A zero gvr never matches.
// apistageContentServe consults it to decide a forced-miss on dep-edge equality.
func RefreshTriggerHas(ctx context.Context, gvr schema.GroupVersionResource) bool {
	if ctx == nil || gvr.Empty() {
		return false
	}
	switch v := ctx.Value(ctxKeyRefreshTriggerGVR).(type) {
	case schema.GroupVersionResource:
		return v == gvr
	case refreshTriggerSet:
		return containsGVR(v, gvr)
	}
	return false
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
	// #375 (option c) — the generation of the LAST dep-event that touched this
	// coordinate's bucket, stamped from the monotonic depEventSeq by the handler-only
	// bumpCoordinateGen (R1: only on a real post-indexer OnObjectEvent, never on the
	// read-only collectMatchesWithDep callers). A resolve captures startSeq at ENTRY;
	// at its accepted Put, lastBumpSeq > startSeq for any recorded dep ⇒ the dep moved
	// DURING the resolve ⇒ PUT-THEN-REMARK. Atomic; read lock-free by the Put-check.
	lastBumpSeq atomic.Uint64
}

// depEventSeq is the process-global monotonic dep-event sequence (#375 option c).
// bumpCoordinateGen advances it once per real OnObjectEvent and stamps the value onto
// the changed coordinate's forward buckets' lastBumpSeq. A resolve snapshots it at ENTRY
// (startSeq, via WithDepGenSink); the accepted-Put check compares recorded deps'
// lastBumpSeq against startSeq (strictly >, since the seq is monotone).
var depEventSeq atomic.Uint64

// depGenSink is the per-resolve dep-generation sink (#375 option c). Installed at a
// resolve-terminal-Put ENTRY via WithDepGenSink, which captures startSeq BEFORE any dep
// read. recordInternal appends each recorded DepKey (gated on the sink being present, like
// the activeCaptures tap). remarkIfDepsMoved reads it on the accepted Put.
type depGenSink struct {
	startSeq uint64
	mu       sync.Mutex
	deps     []DepKey
	// parent is the enclosing resolve's sink when this is a CHILD sink installed by
	// WithContentDepGenSink (#375 A): a content cell Put nested inside an outer resolve.
	// recordInternal appends to the child AND every ancestor, so the outer resolve's
	// Put-check still sees every dep its nested content reads recorded.
	parent *depGenSink
	// seen dedups deps (a resolve re-Records the same coordinate many times, and every
	// Record is appended to each ancestor sink). Lazily initialised under mu.
	seen map[DepKey]struct{}
	// #375 C3 — the post-Put window. Set by remarkIfDepsMoved on an ACCEPTED gen-guarded
	// Put checked against this sink: putKey is the key that was Put, checkedSeq the
	// dep-event seq at the check, putRemarked whether that check (or a later re-check)
	// already remarked putKey. A dep Recorded for putKey AFTER the Put (the content /
	// customer handlers Record only on accept, #189) has no edge during
	// [check, Record): an event there dirty-marks nothing on a cold cell. recordInternal
	// therefore re-checks such a Record against checkedSeq and remarks putKey once.
	putKey      string
	checkedSeq  uint64
	putRemarked bool
}

// addDepLocked appends dk unless already present. Caller holds s.mu.
func (s *depGenSink) addDepLocked(dk DepKey) {
	if s.seen == nil {
		s.seen = make(map[DepKey]struct{}, len(s.deps)+4)
		for _, d := range s.deps {
			s.seen[d] = struct{}{}
		}
	}
	if _, dup := s.seen[dk]; dup {
		return
	}
	s.seen[dk] = struct{}{}
	s.deps = append(s.deps, dk)
}

type ctxKeyDepGenSinkType struct{}

var ctxKeyDepGenSink = ctxKeyDepGenSinkType{}

// WithDepGenSink installs a per-resolve dep-gen sink on ctx and snapshots startSeq at
// resolve ENTRY (before any data read) — the (c) capture point. Call ONCE at each
// resolve-terminal-Put entry (resolve.go, apiref, resolve_populate, boot seed, reseed
// core). A gen-guarded Put whose ctx has NO sink is a drifted entry (see remarkIfDepsMoved).
func WithDepGenSink(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	// Chain to an enclosing resolve's sink (a nested WithL1KeyContext — resolve:true
	// nested dispatch, apiref's RAFullList fullCtx): the nested resolve's Records also
	// reach the outer resolve's Put-check, because the outer body embeds the nested output.
	return context.WithValue(ctx, ctxKeyDepGenSink, &depGenSink{
		startSeq: depEventSeq.Load(),
		parent:   depGenSinkFromContext(ctx),
	})
}

// WithContentDepGenSink installs a CHILD dep-gen sink for an identity-free CONTENT cell
// (apistage content / cluster_list collapse cell) whose fetch + gen-guarded Put run NESTED
// inside an outer resolve (#375 A, TL ruling). Call it at the content cell's own resolve
// entry — BEFORE its data read. It:
//   - captures the cell's OWN startSeq (the (c) capture point for the cell);
//   - PRE-DECLARES the cell's own coordinate (gvr, namespace, name; name "" = LIST
//     wildcard) — the cell Records its dep only AFTER its accepted Put (#189: no edge for
//     a refused Put), so without the pre-declaration the Put-check could never see it;
//   - chains to the outer sink (parent), so Records made under the child still reach the
//     outer resolve's Put-check.
//
// The content PutIfGen then checks exactly the cell's own coordinate against the cell's
// own entry seq — no false positives from the outer resolve's unrelated deps.
func WithContentDepGenSink(ctx context.Context, gvr schema.GroupVersionResource, namespace, name string) context.Context {
	if ctx == nil {
		return ctx
	}
	if name == "" {
		name = listWildcard
	}
	return context.WithValue(ctx, ctxKeyDepGenSink, &depGenSink{
		startSeq: depEventSeq.Load(),
		deps:     []DepKey{{GVR: gvr, Namespace: namespace, Name: name}},
		parent:   depGenSinkFromContext(ctx),
	})
}

func depGenSinkFromContext(ctx context.Context) *depGenSink {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKeyDepGenSink).(*depGenSink)
	return s
}

// unguardedPutHook is a test-build seam: when non-nil it fires on a nil-sink accepted
// gen-guarded Put (a drifted resolve-entry). Production leaves it nil (the drift is
// handled by the detector counter + a one-shot remark); the test build sets it to fail
// loudly so a missed resolve-entry sink-install goes RED in CI (#375 (B)).
//
// Atomic so a test can install/clear it while Puts run on other goroutines (-race).
var unguardedPutHook atomic.Pointer[func(l1Key string)]

// depGenRemarkObserver is a test-build seam (nil in production): when non-nil it is told
// every PUT-THEN-REMARK remarkIfDepsMoved fires, with the reason ("moved" | "nil_sink").
// The remark itself (EnqueueRefresh) is unchanged; this only lets arms COUNT remarks on a
// channel distinct from the dirty-mark fan-out (which goes through enqueueFn).
var depGenRemarkObserver atomic.Pointer[func(l1Key, reason string)]

func observeDepGenRemark(l1Key, reason string) {
	if fn := depGenRemarkObserver.Load(); fn != nil {
		(*fn)(l1Key, reason)
	}
}

// depSet is the DepKey set stored under a reverse l1Key index entry.
// Same shape as keySet — sync.Map + count.
type depSet struct {
	deps  sync.Map // map[DepKey]struct{}
	count atomic.Int64
}

// captureSlot holds the STACK of OPEN capture buffers for one l1Key (issue
// #277 / edge-3 — DECISION 1, [C2-B]). Each *[]DepKey is a distinct capture's
// buffer; recordInternal's tapCapture appends every DepKey recorded under the
// key to EVERY buffer in the stack, so an outer (nested) capture observes an
// inner capture's edges, and two concurrent non-nested captures each observe
// the key's edges (benign over-capture — same key ⇒ same edge set ⇒ idempotent
// replay). slot.mu serialises append/tap/prune; the buffer POINTER is the
// capture handle so EndCapture is unambiguous even with duplicate edge sets.
type captureSlot struct {
	mu   sync.Mutex
	bufs []*[]DepKey
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

	// captures is the edge-3 serve-seam dep-capture registry (issue #277 + the
	// C2 carrier): l1Key -> *captureSlot. recordInternal taps every OPEN buffer
	// for the key at the TOP (before the dedup early-return, [C2-A]), so a
	// capture observes every edge recorded under the key while it is open —
	// INCLUDING edges written by ReplayEdges, which routes through
	// Record/RecordList→recordInternal ([C1-A]). This is how a memo-MISS body
	// produced by the 4a fast path (whose edge-3 is REPLAYED, not resolved)
	// still yields a NON-EMPTY captured dep set for the seed-resolve memo.
	captures sync.Map
	// activeCaptures is the lock-free fast-path gate: recordInternal Loads it
	// once and skips the whole tap path (and its map lookup) whenever no
	// capture is open anywhere, so the hot record path pays a single atomic
	// load in the overwhelmingly common no-capture case.
	activeCaptures atomic.Int64

	// #375 (B) — DETECTOR: accepted gen-guarded Puts whose resolve ctx carried NO
	// dep-gen sink (a resolve-entry that drifted = didn't install one). Expected 0;
	// non-zero ⇒ an unguarded resolve path. Hand-wired to OTLP + expvar.
	unguardedPutTotal atomic.Uint64

	// #375 (option c, fix ii — TL ruling) — per-GVR floor: the depEventSeq of the LAST
	// OnObjectEvent on ANY coordinate of the GVR (gvr -> *atomic.Uint64). bumpCoordinateGen
	// stamps only EXISTING forward buckets, and an empty bucket is pruned, so a COLD
	// coordinate (no dependent at event time) would otherwise be created later by
	// recordInternal with lastBumpSeq=0 and a [entry,Record] churn on it would go unseen.
	// A newly created bucket inherits this floor instead. Bounded by the number of GVRs
	// that ever produced a dep event (not a per-coordinate map).
	gvrLastBump sync.Map

	// totalRecords is the global record count — bounded by maxRecords.
	totalRecords atomic.Int64
	maxRecords   int64

	// coordinates is the number of live buckets in the forward index, i.e.
	// DISTINCT dependency coordinates (GVR, namespace, name). #239.
	//
	// WHY IT IS COUNTED RATHER THAN DERIVED. The #239 scaling conclusion —
	// that dirty-mark fan-out grows LINEARLY with cohort count, because
	// coordinates are a property of the CLUSTER while L1 entries are a
	// property of COHORTS x WIDGETS — rests on a coordinate count of ~364
	// that was DERIVED as edges divided by measured fan-out, never measured.
	// That single number decides whether the finding is a scaling wall or
	// arithmetic, and the issue names instrumenting it as the thing that would
	// falsify the conclusion: if a real count showed coordinates growing with
	// cohorts, the scaling conclusion collapses.
	//
	// READ IT AGAINST `records`, ALWAYS. Alone it says nothing, and its zero
	// is not a health reading: zero coordinates with zero records is an empty
	// tracker (cache off, or boot). The PAIR is the instrument —
	// records/coordinates is the fan-out multiplier, and it is that ratio's
	// behaviour as cohorts grow that answers #239.
	//
	// O(1), maintained at the two bucket-lifecycle sites rather than by
	// ranging the forward map: a sync.Map walk over 90K+ edges on every OTLP
	// collection is not a cost a gauge should carry.
	//
	// EXACT ACROSS THE CAP ROLLBACK (#242). recordInternal LoadOrStores a
	// forward bucket and, if the record cap is then hit, rolls back the KEY
	// inside it; the rollback also prunes the bucket WE just created when no
	// concurrent Record has committed into it (!loadedBucket && count==0,
	// CompareAndDelete-gated) and undoes the matching coordinates.Add(1). So an
	// empty keySet is never left behind and `coordinates` counts only live
	// buckets, whatever dropped_cap is.
	coordinates atomic.Int64

	// Falsifier counters (atomic; safe to read without holding anything).
	recordTotal        atomic.Uint64
	recordDroppedCap   atomic.Uint64
	recordDroppedNoKey atomic.Uint64 // O15: Record*/WithL1KeyContext with empty l1Key
	evictDeleteTotal   atomic.Uint64 // L1 self-representation evictions (OnDelete ONLY — the H1 live discriminator)
	evictSelfGoneTotal atomic.Uint64 // 1.12.5 #187: evictions from a confirmed self-object 404 (EvictSelfGone)
	// 1.12.7 observability — onObjectEventDegradedNoEvict counts DEGRADED
	// verdicts that reached at least one dependent entry and therefore evicted
	// NOTHING. The degrade itself is already logged at WARN and counted
	// (probe_unknown_degraded_total); what was invisible is its CONSEQUENCE —
	// that an eviction decision was deferred to the refresher for entries that
	// actually exist. Counted per EVENT, and only when the match set is
	// non-empty: a degraded verdict naming a coordinate nothing depends on
	// cost nothing, and counting it would bury the cases that did.
	onObjectEventDegradedNoEvict atomic.Uint64
	// 1.12.6 C4: evictions at the refresher drop point after a deterministic
	// NON-404 failure (403/500/timeout/parse/not-servable) exhausted the
	// requeue budget under the breaker (EvictDropPoint). Kept apart from
	// evictSelfGoneTotal on purpose: that counter's documented meaning is
	// "the object is confirmed gone", and during an apiserver outage — the
	// incident the breaker exists for — a folded number would read as "CRs
	// are being deleted". One counter, one meaning (PM verify, Track B).
	evictDropPointTotal atomic.Uint64
	dirtyMarkTotal      atomic.Uint64 // dirty-marks (OnAdd/OnUpdate + OnDelete non-self)
	enqueueUpdateTotal  atomic.Uint64 // refresh enqueues triggered by OnUpdate
	removeL1Total       atomic.Uint64 // RemoveL1Key calls (LRU + DELETE cleanup)

	// #239 dirty-mark attribution: the {path,cause,class} distribution, per-
	// submit-source counts, fan-out denominator and /debug/vars-only GVR
	// drill-down. recordDirtyMarks is the SINGLE writer of dirtyMarkTotal AND
	// the buckets, so Σ buckets == dirtyMarkTotal by construction.
	dmAttr *dmAttribution

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
	// mergeTriggersFn (#375 B) merges one more trigger GVR into l1Key's pending
	// trigger set WITHOUT enqueueing — so a remark carrying several moved GVRs is still
	// ONE enqueue. Wired by the refresher via SetRefreshTriggerMergeHook; nil-safe.
	mergeTriggersFn func(l1Key string, triggerGVR schema.GroupVersionResource)
}

// SetRefreshTriggerMergeHook installs the refresher's trigger-set merge (#375 B).
// Used together with SetRefreshHook by StartRefresher.
func (d *DepTracker) SetRefreshTriggerMergeHook(fn func(l1Key string, triggerGVR schema.GroupVersionResource)) {
	d.enqueueMu.Lock()
	d.mergeTriggersFn = fn
	d.enqueueMu.Unlock()
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
		dmAttr:     newDMAttribution(),
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
	// 1.12.7 F6a — the store strips dep edges inside its delete primitive and
	// must strip them from THIS tracker, not from the process singleton. A
	// hook, not a back-pointer: the store needs the operation, not the owner.
	//
	// A nil store installs nothing — there is no store to wire, and the store
	// this tracker was previously pointed at is not reachable from here to
	// unwire. That is harmless: an unhooked store still strips, through the
	// singleton fallback in stripDepEdges. No production caller passes nil; the
	// single production call site is resolved.go's, with a real store.
	if s != nil {
		s.setDepStripHook(d.RemoveL1Key)
	}
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
func (d *DepTracker) Record(ctx context.Context, l1Key string, gvr schema.GroupVersionResource, namespace, name string) {
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
	d.recordInternal(ctx, l1Key, DepKey{GVR: gvr, Namespace: namespace, Name: name})
}

// RecordList stores a list-scope dependency edge: l1Key depends on
// every object of (gvr) in namespace (or cluster-wide when namespace is
// ""). Internally encodes the bucket as (gvr, namespace, "*").
func (d *DepTracker) RecordList(ctx context.Context, l1Key string, gvr schema.GroupVersionResource, namespace string) {
	if d == nil {
		return
	}
	if l1Key == "" {
		// O15: same loud-fail as Record — a list-scope edge with no L1
		// key cannot be attached anywhere.
		loudFailEmptyL1Key("RecordList")
		return
	}
	d.recordInternal(ctx, l1Key, DepKey{GVR: gvr, Namespace: namespace, Name: listWildcard})
}

// recordInternal is the shared body of Record + RecordList. Idempotent;
// honours the global cap.
func (d *DepTracker) recordInternal(ctx context.Context, l1Key string, dk DepKey) {
	// #375 (option c) — append the recorded dep to the per-resolve sink (installed at
	// resolve ENTRY via WithDepGenSink), gated like the capture tap below. The accepted-
	// Put check (remarkIfDepsMoved) compares each recorded dep's bucket lastBumpSeq to
	// startSeq. Appended BEFORE the dedup early-return so the resolve's dep set is complete.
	var recheck *depGenSink
	for s := depGenSinkFromContext(ctx); s != nil; s = s.parent {
		s.mu.Lock()
		s.addDepLocked(dk)
		if recheck == nil && s.putKey != "" && s.putKey == l1Key && !s.putRemarked {
			recheck = s // #375 C3: a Record for a key this sink's resolve already Put
		}
		s.mu.Unlock()
	}
	if recheck != nil {
		// After the edge is in place (deferred past the forward insert below), so an
		// event is then either seen by this re-check or dirty-marks through the edge.
		defer d.recheckAfterPutRecord(recheck, l1Key, dk)
	}
	// [C2-A] edge-3 capture tap — at the VERY TOP, BEFORE the forward
	// LoadOrStore and the idempotent dedup early-return below. An idempotent
	// re-Record (an edge the key already holds) still BELONGS in an open
	// capture: the memo's stored dep set must be COMPLETE, and a replayed edge
	// the widget key happened to already carry would otherwise be dropped. The
	// tap is gated on the lock-free activeCaptures counter so the common
	// no-capture record pays only one atomic load.
	if d.activeCaptures.Load() > 0 {
		d.tapCapture(l1Key, dk)
	}
	// Forward: DepKey -> *keySet[l1Key]
	ksI, loadedBucket := d.forward.LoadOrStore(dk, &keySet{})
	ks := ksI.(*keySet)
	if !loadedBucket {
		// A coordinate nothing depended on until now (#239).
		d.coordinates.Add(1)
		// #375 fix (ii) — a COLD bucket inherits the GVR's last-bump floor, read AFTER the
		// bucket is published: bumpCoordinateGen raises the floor BEFORE it Loads buckets,
		// so either its Load sees this bucket or this read sees its floor (seq-cst atomics).
		storeMaxSeq(&ks.lastBumpSeq, d.gvrLastBumpSeq(dk.GVR))
	}
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
		// #242 — if WE created this bucket (!loadedBucket, so WE did the
		// coordinates.Add(1) above) and no concurrent Record has committed an
		// edge into it (ks.count is still 0 — the commit path at ks.count.Add(1)
		// runs AFTER this cap check), the bucket is a phantom the coordinates
		// gauge would over-read forever. Prune it, gating coordinates.Add(-1) on
		// the CompareAndDelete success so a losing racer cannot double-count —
		// the count-guarded pattern RemoveL1Key uses (#239). The residual
		// check-then-delete race is the identical benign one accepted there.
		if !loadedBucket && ks.count.Load() == 0 && d.forward.CompareAndDelete(dk, ks) {
			d.coordinates.Add(-1)
		}
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

// bumpCoordinateGen is the #375 (option c) handler-only gen bump (R1). It advances the
// global depEventSeq ONCE and stamps the new value onto every EXISTING forward bucket the
// changed coordinate matches — exact + ns-wildcard + list-wildcard — mirroring
// collectMatchesWithDep's addAll bucket set, so a resolve that recorded ANY of those
// (incl. a LIST edge whose individual member it never read) sees the move. MUST be called
// ONLY from the post-indexer mutation handler (OnObjectEvent), BEFORE the dirty-mark
// fan-out (R2: Store-before-mark), and NEVER from the read-only collectMatchesWithDep
// callers (:943 membership, :1326 iterate) — a read-path bump would be a spurious gen move.
func (d *DepTracker) bumpCoordinateGen(gvr schema.GroupVersionResource, namespace, name string) {
	if d == nil {
		return
	}
	s := depEventSeq.Add(1)
	fireDepBumpHook("after_add") // test seam (nil in prod): the torn [Add→Store] window
	// #375 fix (ii) — raise the per-GVR floor FIRST (even when no bucket exists yet), so a
	// bucket created after this point by a racing recordInternal inherits s (see there).
	fI, _ := d.gvrLastBump.LoadOrStore(gvr, new(atomic.Uint64))
	storeMaxSeq(fI.(*atomic.Uint64), s)
	stamp := func(dk DepKey) {
		if ksI, ok := d.forward.Load(dk); ok {
			storeMaxSeq(&ksI.(*keySet).lastBumpSeq, s)
		}
	}
	stamp(DepKey{GVR: gvr, Namespace: namespace, Name: name})
	if namespace != "" {
		stamp(DepKey{GVR: gvr, Namespace: "", Name: name})
	}
	stamp(DepKey{GVR: gvr, Namespace: namespace, Name: listWildcard})
	if namespace != "" {
		stamp(DepKey{GVR: gvr, Namespace: "", Name: listWildcard})
	}
	fireDepBumpHook("after_store") // test seam (nil in prod): the R2 [Store→mark] window
}

// depBumpHook is a test-build seam (nil in production) fired inside bumpCoordinateGen at
// the two #375 (D) windows: "after_add" (depEventSeq advanced, buckets not yet stamped —
// the torn [Add→Store] window) and "after_store" (buckets stamped, dirty-mark fan-out not
// yet run — the R2 window). The R2 / torn-window arms land a Put there.
var depBumpHook atomic.Pointer[func(stage string)]

func fireDepBumpHook(stage string) {
	if fn := depBumpHook.Load(); fn != nil {
		(*fn)(stage)
	}
}

// bumpResourceTypeGen is bumpCoordinateGen's TYPE-LEVEL twin (#375, TL ruling): the four
// GVR-lifecycle dirty-mark sources (OnResourceTypeAvailable / Removed / SchemaRelisted /
// StoreRepaired) mark every dependent of a whole GVR without an OnObjectEvent, so they
// must advance the generation too, or a resolve in flight across a CRD add / delete /
// schema relist / store repair could have its type-level mark consumed while it is
// non-resident and its Put-check see no move. It advances depEventSeq ONCE, raises the
// GVR floor (cold buckets), and stamps every bucket of the GVR the matching
// collectTypeMatches scan would fan out to (LIST buckets only when listOnly). Called by
// those handlers BEFORE collectTypeMatches + the fan-out (R2). R1: each caller fires
// after the new data is readable — see the enumeration in the #375 PR body.
func (d *DepTracker) bumpResourceTypeGen(gvr schema.GroupVersionResource, listOnly bool) {
	if d == nil {
		return
	}
	s := depEventSeq.Add(1)
	fireDepBumpHook("after_add")
	fI, _ := d.gvrLastBump.LoadOrStore(gvr, new(atomic.Uint64))
	storeMaxSeq(fI.(*atomic.Uint64), s)
	d.forward.Range(func(k, v any) bool {
		dk := k.(DepKey)
		if dk.GVR == gvr && (!listOnly || dk.Name == listWildcard) {
			storeMaxSeq(&v.(*keySet).lastBumpSeq, s)
		}
		return true
	})
	fireDepBumpHook("after_store")
}

// gvrLastBumpSeq returns the per-GVR floor (#375 fix ii): the seq of the last dep event
// on any coordinate of gvr, 0 when the GVR never produced one.
func (d *DepTracker) gvrLastBumpSeq(gvr schema.GroupVersionResource) uint64 {
	if fI, ok := d.gvrLastBump.Load(gvr); ok {
		return fI.(*atomic.Uint64).Load()
	}
	return 0
}

// storeMaxSeq raises a to v if v is larger (monotone CAS-max). Two handlers can stamp
// out of seq order; a plain Store could LOWER a floor/bucket below a seq a cold-bucket
// inheritor or a Put-check needs to see.
func storeMaxSeq(a *atomic.Uint64, v uint64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

// remarkIfDepsMoved is the #375 (option c) PUT-THEN-REMARK check, called on an ACCEPTED
// gen-guarded Put (PutIfGen/ReplaceIfGen; PutRAFullListIfGen via PutIfGen), AFTER putCoreLocked + the
// store lock is released (the enqueue must not run under c.mu). Semantics:
//   - NIL sink (drifted resolve-entry: ctx never installed one) → remark THIS key ONCE
//     (fail-fresh) + unguardedPutTotal++ (detector) + fire unguardedPutHook (test-build
//     RED). NOT startSeq=0 (that would remark EVERY Put = amplification).
//   - EMPTY sink (legit no-deps resolve) → no remark.
//   - any recorded dep that moved since startSeq → remark THIS key ONCE, keeping the
//     just-Put body warm. "Moved" = its bucket's lastBumpSeq > startSeq, or — when the
//     bucket is gone (pruned between Record and Put) — its GVR floor > startSeq
//     (conservative, fix ii).
//
// The remark goes through the SAME refresher hook the dirty-mark fan-out uses
// (enqueueRemark) — one enqueue per Put, carrying EVERY moved dep's GVR as refresh
// triggers (#375 B): the re-resolve force-misses the content cells of those GVRs
// (apistageContentServe forceContentMiss) instead of re-reading a content cell that may
// itself be stale, and a cluster_list cell keeps its high-priority tier.
func (d *DepTracker) remarkIfDepsMoved(ctx context.Context, l1Key string) {
	if d == nil || l1Key == "" {
		return
	}
	s := depGenSinkFromContext(ctx)
	if s == nil {
		d.unguardedPutTotal.Add(1)
		if fn := unguardedPutHook.Load(); fn != nil {
			(*fn)(l1Key)
		}
		observeDepGenRemark(l1Key, "nil_sink")
		d.enqueueRemark(l1Key, nil) // fail-fresh: the one drifted key
		return
	}
	s.mu.Lock()
	deps := append([]DepKey(nil), s.deps...)
	// #375 C3 — remember this accepted Put so a Record for l1Key landing after the check
	// can be re-checked (recheckAfterPutRecord). checkedSeq is taken BEFORE the dep scan:
	// an event racing the scan is seen by the scan or by the re-check, never by neither.
	s.putKey = l1Key
	s.checkedSeq = depEventSeq.Load()
	s.putRemarked = false
	s.mu.Unlock()
	var moved []schema.GroupVersionResource
	for _, dk := range deps {
		if d.depMovedSince(dk, s.startSeq) && !containsGVR(moved, dk.GVR) {
			moved = append(moved, dk.GVR)
		}
	}
	if len(moved) > 0 {
		s.mu.Lock()
		s.putRemarked = true
		s.mu.Unlock()
		observeDepGenRemark(l1Key, "moved")
		d.enqueueRemark(l1Key, moved) // once per Put, not per dep
	}
}

// recheckAfterPutRecord is #375 C3: dk was just Recorded for l1Key, which s's resolve
// already Put and checked (the content / customer handlers Record only after an accepted
// Put, #189). An event on dk in [Put-check, Record) found no edge (a cold cell has none
// until this Record), so its dirty-mark was lost. The new bucket inherits the GVR floor
// (fix ii), so dk reads as moved since checkedSeq exactly when such an event happened.
// Then remark l1Key ONCE (putRemarked), carrying dk's GVR. Idempotent with a dirty-mark
// that does find the edge (an event after the Record): both enqueue the same key.
func (d *DepTracker) recheckAfterPutRecord(s *depGenSink, l1Key string, dk DepKey) {
	s.mu.Lock()
	checked := s.checkedSeq
	done := s.putRemarked || s.putKey != l1Key
	s.mu.Unlock()
	if done || !d.depMovedSince(dk, checked) {
		return
	}
	s.mu.Lock()
	if s.putRemarked {
		s.mu.Unlock()
		return
	}
	s.putRemarked = true
	s.mu.Unlock()
	observeDepGenRemark(l1Key, "moved_after_put")
	d.enqueueRemark(l1Key, []schema.GroupVersionResource{dk.GVR})
}

func containsGVR(gs []schema.GroupVersionResource, g schema.GroupVersionResource) bool {
	for _, x := range gs {
		if x == g {
			return true
		}
	}
	return false
}

// depMovedSince reports whether dk saw a dep event after seq: its bucket's lastBumpSeq,
// or the GVR floor when no bucket exists (fix ii, conservative).
func (d *DepTracker) depMovedSince(dk DepKey, seq uint64) bool {
	if ksI, ok := d.forward.Load(dk); ok {
		return ksI.(*keySet).lastBumpSeq.Load() > seq
	}
	return d.gvrLastBumpSeq(dk.GVR) > seq
}

// enqueueRemark schedules the PUT-THEN-REMARK refresh of l1Key ONCE through the
// refresher's dirty-mark hook (trigger GVRs merged into the key's trigger set + tier
// routing), falling back to EnqueueRefresh when no hook is installed (refresher not
// started). No triggers (nil-sink) → one hook call with the zero GVR (never force-misses).
func (d *DepTracker) enqueueRemark(l1Key string, triggers []schema.GroupVersionResource) {
	d.enqueueMu.RLock()
	fn := d.enqueueFn
	mfn := d.mergeTriggersFn
	d.enqueueMu.RUnlock()
	if fn == nil {
		EnqueueRefresh(l1Key)
		return
	}
	if len(triggers) == 0 {
		fn(l1Key, schema.GroupVersionResource{})
		return
	}
	// Merge all but the last trigger into the key's pending trigger set WITHOUT
	// enqueueing, then one hook call (merge-last + enqueue) — one remark per Put.
	if mfn != nil {
		for _, g := range triggers[:len(triggers)-1] {
			mfn(l1Key, g)
		}
	}
	fn(l1Key, triggers[len(triggers)-1])
}

// ─────────────────────────────────────────────────────────────────────────
// edge-3 serve-seam dep capture / replay (issue #277 + the C2 carrier)
// ─────────────────────────────────────────────────────────────────────────

// tapCapture appends dk to every OPEN capture buffer registered under l1Key.
// Called from recordInternal's top only when activeCaptures>0. Lock-free map
// Load + a brief per-slot lock; a no-slot key is a single map miss.
func (d *DepTracker) tapCapture(l1Key string, dk DepKey) {
	slotI, ok := d.captures.Load(l1Key)
	if !ok {
		return
	}
	slot := slotI.(*captureSlot)
	slot.mu.Lock()
	for _, buf := range slot.bufs {
		*buf = append(*buf, dk)
	}
	slot.mu.Unlock()
}

// BeginCapture opens a fresh capture for l1Key and returns its buffer POINTER
// as the handle. Every edge recorded under l1Key (via Record/RecordList→
// recordInternal, including ReplayEdges) until the matching EndCapture is
// appended to this buffer. Pairs 1:1 with EndCapture; nil-safe on an empty key
// (a no-op handle the caller's EndCapture also no-ops).
//
// DECISION 1 [C2-B]: a per-key STACK, re-entrancy + concurrency safe. The
// LoadOrStore/Lock/re-validate loop closes the prune race with a concurrent
// EndCapture that deletes an emptied slot (see EndCapture): if the slot we
// loaded was pruned before we appended, we retry with a fresh one.
func (d *DepTracker) BeginCapture(l1Key string) *[]DepKey {
	buf := &[]DepKey{}
	if d == nil || l1Key == "" {
		return buf
	}
	for {
		slotI, _ := d.captures.LoadOrStore(l1Key, &captureSlot{})
		slot := slotI.(*captureSlot)
		slot.mu.Lock()
		// Re-validate the slot is still the one installed under l1Key. An
		// EndCapture may have pruned it (CompareAndDelete under slot.mu)
		// between our LoadOrStore and our Lock; appending to a detached slot
		// would make it invisible to tapCapture (which Loads by key).
		if cur, ok := d.captures.Load(l1Key); !ok || cur != slotI {
			slot.mu.Unlock()
			continue
		}
		slot.bufs = append(slot.bufs, buf)
		// N4: bump the gate UNDER slot.mu, before unlock. Once the buffer is
		// publicly consumable (a concurrent tapCapture that acquires slot.mu
		// after this unlock), activeCaptures is already ≥1 — closing the benign
		// window where a record on the same key read the gate at 0 while the
		// buffer was already registered.
		d.activeCaptures.Add(1)
		slot.mu.Unlock()
		return buf
	}
}

// EndCapture closes the capture identified by handle h under l1Key and returns
// the edges it observed (*h). Removes h from the key's stack by pointer
// identity and prunes the slot when it empties. nil-safe: a nil/foreign handle
// returns nil without touching activeCaptures, so an unbalanced call cannot
// drive the gate negative.
func (d *DepTracker) EndCapture(l1Key string, h *[]DepKey) []DepKey {
	if d == nil || h == nil {
		return nil
	}
	removed := false
	if slotI, ok := d.captures.Load(l1Key); ok {
		slot := slotI.(*captureSlot)
		slot.mu.Lock()
		for i := len(slot.bufs) - 1; i >= 0; i-- {
			if slot.bufs[i] == h {
				slot.bufs = append(slot.bufs[:i], slot.bufs[i+1:]...)
				removed = true
				break
			}
		}
		// Prune the emptied slot UNDER slot.mu (CompareAndDelete only when it
		// is still the installed slot), so BeginCapture's re-validate is the
		// sole synchronisation point for the detach.
		if len(slot.bufs) == 0 {
			d.captures.CompareAndDelete(l1Key, slotI)
		}
		slot.mu.Unlock()
	}
	if removed {
		d.activeCaptures.Add(-1)
	}
	return *h
}

// ReplayEdges records each edge in `edges` under dst, routing EXCLUSIVELY
// through Record/RecordList→recordInternal ([C1-A] HARD INVARIANT — never a
// direct forward/reverse map write), so an active capture on dst observes the
// replay (proved by F-INV). Idempotent (recordInternal dedups). For each edge
// it first ensures the backing GVR's informer exists so a later event on the
// replayed coordinate actually fires. nil-safe on dst=="" (never routes an
// empty key through the loudFail).
func (d *DepTracker) ReplayEdges(ctx context.Context, dst string, edges []DepKey) {
	if d == nil || dst == "" {
		return
	}
	for _, e := range edges {
		d.ensureInformer(e.GVR)
		if e.Name == listWildcard {
			d.RecordList(ctx, dst, e.GVR, e.Namespace)
		} else {
			d.Record(ctx, dst, e.GVR, e.Namespace, e.Name)
		}
	}
}

// ensureInformer registers (idempotently, singleflighted) the informer for gvr
// so a future event on a replayed coordinate reaches the dep tracker. Mirrors
// the dispatcher's ensureWatcherInformerForGVR (deps_extract.go): nil-safe when
// the global watcher is absent (cache-off / unit tests without a watcher).
func (d *DepTracker) ensureInformer(gvr schema.GroupVersionResource) {
	rw := Global()
	if rw == nil {
		return
	}
	rw.EnsureResourceType(gvr)
}

// RangeEdges calls fn for each l1Key in the reverse index with a snapshot of
// its edges, until fn returns false. Lock-free (sync.Map.Range over the reverse
// index + each key's depSet). Read-only — the /debug/deps diagnostic's scan
// primitive. The CALLER bounds how many keys it inspects (the diagnostic caps
// the sample); this never holds a store/serve mutex.
func (d *DepTracker) RangeEdges(fn func(l1Key string, edges []DepKey) bool) {
	if d == nil {
		return
	}
	d.reverse.Range(func(k, v any) bool {
		ds := v.(*depSet)
		var edges []DepKey
		ds.deps.Range(func(dk, _ any) bool {
			edges = append(edges, dk.(DepKey))
			return true
		})
		return fn(k.(string), edges)
	})
}

// RangeKeys calls fn for each l1Key in the reverse index, until fn returns
// false. Lock-free; unlike RangeEdges it does NOT materialize each key's edge
// slice, so a scan that needs only the key (e.g. the /debug/deps stale-risk
// filter) pays no per-key allocation. Read-only. The CALLER bounds how many
// keys it inspects.
func (d *DepTracker) RangeKeys(fn func(l1Key string) bool) {
	if d == nil {
		return
	}
	d.reverse.Range(func(k, _ any) bool {
		return fn(k.(string))
	})
}

// EdgesUnder returns a snapshot of the dependency edges recorded under l1Key
// (the reverse index), or nil. Read-only. Used by the 4a serve seam to replay
// the raKey cell's backing edges onto the widget key (C2) and by the
// /debug/deps diagnostic.
func (d *DepTracker) EdgesUnder(l1Key string) []DepKey {
	if d == nil || l1Key == "" {
		return nil
	}
	dsI, ok := d.reverse.Load(l1Key)
	if !ok {
		return nil
	}
	ds := dsI.(*depSet)
	var out []DepKey
	ds.deps.Range(func(k, _ any) bool {
		out = append(out, k.(DepKey))
		return true
	})
	return out
}

// hasEdge reports whether an ABSENT verdict for dk can reach l1Key at all:
// whether l1Key is among the keys the worker's own match
// (collectMatchesWithDep — the four bucket forms, exact and wildcard,
// namespaced and namespace-stripped) returns for that coordinate. It is
// deliberately the SAME lookup OnObjectEvent uses, not a one-bucket probe,
// so a change to how Record derives its coordinate cannot silently turn
// evictable entries into no-edge ones (arch N10 minor). An entry with no
// edge (its Record was dropped at DEPS_MAX_RECORDS — dropped_cap) is
// dirty-markable by nothing and evictable by nothing but its TTL; the
// reconcile audit (1.12.6 C3) counts such entries apart from divergence
// because submitting their coordinate would evict nothing. Lock-free
// (sync.Map loads and ranges).
func (d *DepTracker) hasEdge(l1Key string, dk DepKey) bool {
	if d == nil {
		return false
	}
	_, ok := d.collectMatchesWithDep(dk.GVR, dk.Namespace, dk.Name)[l1Key]
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
//
// 1.12.7 F6b — this is also where the GONE verdict leaves the cache. The
// objAbsent branch fires notifyObjectGone (gone_forget_hook.go) so the
// Phase-1 harvesters drop the in-memory copy they would otherwise replay
// into L1 forever. See the fire site for why it precedes the no-matches
// return.
func (d *DepTracker) OnObjectEvent(gvr schema.GroupVersionResource, namespace, name string, state objectState) (int, int) {
	if d == nil {
		return 0, 0
	}
	// 1.12.7 F6b — carry the GONE verdict to whoever holds a harvested
	// in-memory copy of this object, BEFORE the no-matches return below.
	// The placement is load-bearing: the population this exists for is
	// precisely the one with no L1 dependency edge left (a harvested widget
	// whose cell was already evicted has nothing in `matched`), so firing
	// after that return would miss every entry that matters. Fires ONLY on
	// objAbsent — a servable informer whose indexer does not hold the object.
	// objUnknown / objUnknownDegraded must never empty a warm set.
	if state == objAbsent {
		notifyObjectGone(gvr, namespace, name)
	}
	// #375 (option c) R1/R2 — advance the per-coordinate gen BEFORE the dirty-mark
	// fan-out below (R2: Store-before-mark), ONLY here in the post-indexer mutation
	// handler (R1). A resolve that recorded any of this coordinate's buckets and has
	// not yet done its accepted Put will see lastBumpSeq > startSeq and self-remark.
	d.bumpCoordinateGen(gvr, namespace, name)
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
	classCounts := map[string]int{} // #239: self / list_dep / exact_dep of the marked keys
	for l1Key, dk := range matched {
		isSelf := d.isSelfRepresentation(store, l1Key, self)
		if state == objAbsent && isSelf {
			toEvict = append(toEvict, l1Key) // bucket 1, and ONLY when absent
			continue
		}
		toMark = append(toMark, l1Key) // buckets 2 + 3, and self-when-present
		classCounts[dirtyMarkClass(dk, isSelf)]++
	}

	// 1.12.7 observability — an UNKNOWN_DEGRADED verdict structurally cannot
	// evict (the eviction arm above is guarded on objAbsent), so every entry
	// it matched is dirty-marked and left resident for the refresher to decide
	// against the apiserver. Count that consequence here, where it is a fact
	// about this call rather than an inference from two counters.
	if state == objUnknownDegraded {
		d.onObjectEventDegradedNoEvict.Add(1)
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
		// #239 — single writer of dirtyMarkTotal + the {path,cause,class} buckets.
		d.recordDirtyMarks(dmPathObject, dirtyMarkCauseForState(state), classCounts, gvr)
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
	// #376 — GetNoTouch: this is the dirty-mark fan-out self-representation PROBE
	// (reads only entry.Inputs to compare GVR), fired for every resident matched key
	// on every dep event — NOT a customer read. It must not stamp lastRead/hitTotal/
	// MoveToFront (the dominant source of faked #315/#316 warmth, ~222.9:1 fan-out).
	entry, ok := store.GetNoTouch(l1Key)
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
	d.bumpResourceTypeGen(gvr, true) // #375 R1/R2: handler-only, BEFORE the fan-out
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
	d.bumpResourceTypeGen(gvr, false) // #375 R1/R2: handler-only, BEFORE the fan-out
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
	d.bumpResourceTypeGen(gvr, false) // #375 R1/R2: handler-only, BEFORE the fan-out
	matched := d.collectTypeMatches(gvr, false /* listOnly */)
	return d.dirtyMarkResourceType("SCHEMA_RELIST", gvr, matched)
}

// OnResourceTypeStoreRepaired is the #237 deliverable B sibling of
// OnResourceTypeSchemaRelisted: the same dirty-mark-only, never-evict body,
// reached when a relist was fired because the informer STORE was found to
// disagree with the apiserver — not because a CRD's schema changed.
//
// It exists for one reason: the event label. The relist machinery is shared
// (relistGVRForRepair, crd_discovery_side_effect.go), so a store repair that
// reused OnResourceTypeSchemaRelisted would log cache_event.consumed
// type=SCHEMA_RELIST and the event log would attribute a store divergence to a
// CRD schema widen. That is the same misattribution this whole issue is about:
// #237 spent two sessions on a defect whose instruments named the wrong cause.
// One label, one caller each, so a non-zero bucket names the code path.
func (d *DepTracker) OnResourceTypeStoreRepaired(gvr schema.GroupVersionResource) int {
	if d == nil {
		return 0
	}
	d.bumpResourceTypeGen(gvr, false) // #375 R1/R2: handler-only, BEFORE the fan-out
	matched := d.collectTypeMatches(gvr, false /* listOnly */)
	return d.dirtyMarkResourceType("STORE_REPAIR", gvr, matched)
}

// dirtyMarkResourceType dirty-marks every L1 key in matched via the
// refreshHook — the shared body of OnResourceTypeAvailable +
// OnResourceTypeRemoved. NEVER evicts. Returns the number marked.
func (d *DepTracker) dirtyMarkResourceType(eventType string, gvr schema.GroupVersionResource, matched map[string]DepKey) int {
	if len(matched) == 0 {
		return 0
	}
	d.enqueueMu.RLock()
	enqueue := d.enqueueFn
	d.enqueueMu.RUnlock()

	marked := 0
	classCounts := map[string]int{} // #239: list_dep vs exact_dep (type deps have no self class)
	for l1Key, dk := range matched {
		if enqueue != nil {
			enqueue(l1Key, gvr)
		}
		classCounts[dirtyMarkClass(dk, false)]++
		marked++
	}
	if marked > 0 {
		// #239 — single writer of dirtyMarkTotal + the {path,cause,class} buckets.
		d.recordDirtyMarks(dmPathType, dirtyMarkCauseForEventType(eventType), classCounts, gvr)
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
func (d *DepTracker) collectTypeMatches(gvr schema.GroupVersionResource, listOnly bool) map[string]DepKey {
	out := map[string]DepKey{}
	d.forward.Range(func(k, v any) bool {
		dk := k.(DepKey)
		if dk.GVR != gvr {
			return true
		}
		if listOnly && dk.Name != listWildcard {
			return true
		}
		v.(*keySet).keys.Range(func(kk, _ any) bool {
			l1 := kk.(string)
			// #239 — keep the most specific bucket per key (exact beats list) so
			// the type-path dirty-mark is classified list_dep vs exact_dep.
			if prev, seen := out[l1]; !seen || (prev.Name == listWildcard && dk.Name != listWildcard) {
				out[l1] = dk
			}
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
// and clears its dep records (1.12.7 F6a: inside the delete primitive, under
// the same store-mutex hold). Counts evictDeleteTotal — self-evictions
// only, per the R2/R7 counter contract.
func (d *DepTracker) runEvictionBatch(keys []string) {
	d.storeMu.RLock()
	store := d.store
	d.storeMu.RUnlock()

	var gone []string
	for _, l1Key := range keys {
		if store != nil {
			// 1.12.7 F6a — deleteForDep strips the dep edges under the same
			// store-mutex hold as the index delete. There is deliberately NO
			// trailing RemoveL1Key here: the old unconditional strip ran
			// outside the lock and, on a key a concurrent resolve had already
			// re-Put, removed the NEW entry's freshly recorded edges, leaving
			// a resident entry no repair path could reach.
			if store.deleteForDep(l1Key) {
				gone = append(gone, l1Key)
			}
		}
	}
	if len(gone) > 0 {
		d.evictDeleteTotal.Add(uint64(len(gone)))
	}
	// 1.12.6 item 7 (C10): tell the armed frontend the body is gone. Runs
	// AFTER every lock this path takes is released (d.storeMu above,
	// c.mu inside deleteForDep) — the hub takes its own h.mu and the
	// per-subscriber pmu, so the eviction path never nests a hub lock
	// under a store lock (S10). Only keys that actually left the store
	// publish; a key with no armed subscriber costs one map read.
	for _, l1Key := range gone {
		publishEvictionFn(l1Key)
	}
}

// publishEvictionFn is the C10 publish hook the two DELETE-semantics
// eviction sites call (runEvictionBatch, EvictSelfGone). A var seam ONLY so
// the S10 arm can install a probe that asserts the store lock is free at
// the call (repo idiom: refreshCoalesceWindowFn); production never
// reassigns it. TTL / LRU / max-age evictions (resolved.go) deliberately do
// NOT route here — design §6.2.5 / S1d.
var publishEvictionFn = PublishEviction

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
//
// 404 ONLY. The 1.12.6 C4 drop point — a deterministic NON-404 failure that
// exhausted the same budget under the breaker — takes EvictDropPoint, the
// same mechanism on its own counter, so evict_self_gone_total keeps meaning
// "the object is confirmed gone" even during an apiserver outage.
func (d *DepTracker) EvictSelfGone(l1Key string, gone *ResolvedKeyInputs) bool {
	// #216 — fire the object-level gone-forget from the coordinate the CALLER holds.
	// EvictSelfGone has exactly one production caller (refresher.go, the drop-point
	// self-404 arm), which still has the 404'd entry's Inputs in scope — that IS the
	// confirmed-gone self-object coordinate. The forget target is the OBJECT (this is
	// the confirmed-404 arm), not the L1 cell, so fire UNCONDITIONALLY: before
	// evictSelfEntry and REGARDLESS of its result. A concurrent LRU/TTL evict may
	// already have removed the cell, but the object is still confirmed gone and its
	// harvested copies must be dropped (notifyObjectGone / gone_forget_hook.go), or
	// the next seed pass re-Puts the deleted object's content for the pod's life. This
	// is idempotent (the double-forget arm proves it) and confirmed-404-only (ONLY
	// here, NEVER EvictDropPoint — a 403 / 500 / timeout is not a deletion), so it can
	// never wrongly forget a live object. Sourcing the coordinate from the caller (not
	// a store peek) means the forget no longer depends on the entry still being
	// resident, so the LRU/TTL pre-eviction race is ELIMINATED. nil / empty → skip.
	if gone != nil {
		gvr := schema.GroupVersionResource{Group: gone.Group, Version: gone.Version, Resource: gone.Resource}
		if !(gvr.Empty() && gone.Namespace == "" && gone.Name == "") {
			notifyObjectGone(gvr, gone.Namespace, gone.Name)
		}
	}

	if !d.evictSelfEntry(l1Key, &d.evictSelfGoneTotal) {
		return false
	}

	// 1.12.6 item 7 (C10): the object is CONFIRMED gone (404) — tell the
	// armed frontend. Gated on evictSelfEntry==true (the publish is about the
	// CELL / subscriber, unlike the object-level forget above). Same hook and lock
	// discipline as runEvictionBatch (c.mu released inside deleteForDep, d.storeMu
	// released inside evictSelfEntry). Deliberately in THIS arm and not in
	// evictSelfEntry: EvictDropPoint shares the body for a NON-404 failure (403 /
	// 500 / timeout under the breaker), which is not a deletion — publishing it
	// would tell every armed tab its widgets were deleted during an apiserver
	// outage (S1d pins it silent).
	publishEvictionFn(l1Key)
	return true
}

// EvictDropPoint is EvictSelfGone for the 1.12.6 C4 drop point: the entry's
// refresh failed deterministically for a NON-404 reason (403 / 500 / timeout
// / parse / not-servable) across the whole requeue budget and the breaker
// granted a token. Same store delete, same dep-edge cleanup, same "true iff
// an entry was removed" contract — counted on evict_drop_point_total so
// neither evict_delete_total (the H1 discriminator) nor
// evict_self_gone_total (the confirmed-404 signal) moves for it.
func (d *DepTracker) EvictDropPoint(l1Key string) bool {
	return d.evictSelfEntry(l1Key, &d.evictDropPointTotal)
}

// evictSelfEntry is the shared body of EvictSelfGone / EvictDropPoint. It
// deletes the entry through store.deleteForDep (which also bumps the STORE's
// evict_delete_total under snowplow_resolved_cache, clears the C4 suppression
// marker and — 1.12.7 F6a — strips the key's dep records under the same hold),
// bumping counter only when an entry was actually removed. It never touches the
// tracker's evictDeleteTotal.
func (d *DepTracker) evictSelfEntry(l1Key string, counter *atomic.Uint64) bool {
	if d == nil || l1Key == "" {
		return false
	}
	d.storeMu.RLock()
	store := d.store
	d.storeMu.RUnlock()

	evicted := false
	if store != nil {
		// 1.12.7 F6a — the edge strip is inside deleteForDep, under the same
		// hold as the index delete. No trailing strip here either; see
		// runEvictionBatch.
		evicted = store.deleteForDep(l1Key)
	}
	if evicted {
		counter.Add(1)
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
					// #239: decrement only when WE removed the bucket, so a
					// concurrent losing CompareAndDelete cannot double-count
					// the coordinate's disappearance.
					if d.forward.CompareAndDelete(dk, ks) {
						d.coordinates.Add(-1)
					}
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
	TotalRecords int64
	MaxRecords   int64
	// Coordinates is the distinct-dependency-coordinate count (#239). Read it
	// against TotalRecords — the RATIO is the fan-out multiplier and neither
	// number means anything alone.
	Coordinates         int64
	RecordTotal         uint64
	RecordDroppedCap    uint64
	RecordDroppedNoKey  uint64 // O15: empty-l1Key Record*/WithL1KeyContext
	EvictDeleteTotal    uint64 // self-representation evictions from an informer DELETE only
	EvictSelfGoneTotal  uint64 // 1.12.5 #187: evictions from a confirmed self-object 404
	EvictDropPointTotal uint64 // 1.12.6 C4: evictions at the drop point after a NON-404 deterministic failure (breaker-granted)
	DirtyMarkTotal      uint64 // dirty-marks (ADD/UPDATE + DELETE non-self)
	EnqueueUpdateTotal  uint64
	RemoveL1Total       uint64
	// 1.12.7: degraded verdicts that reached dependents and evicted nothing.
	OnObjectEventDegradedNoEvict uint64
	// #375 (B): accepted gen-guarded Puts whose resolve ctx carried NO dep-gen sink
	// (a drifted resolve-entry). DETECTOR — expected 0.
	UnguardedPutTotal uint64
}

func (d *DepTracker) Stats() DepStats {
	if d == nil {
		return DepStats{}
	}
	return DepStats{
		TotalRecords:                 d.totalRecords.Load(),
		MaxRecords:                   d.maxRecords,
		Coordinates:                  d.coordinates.Load(),
		RecordTotal:                  d.recordTotal.Load(),
		RecordDroppedCap:             d.recordDroppedCap.Load(),
		RecordDroppedNoKey:           d.recordDroppedNoKey.Load(),
		EvictDeleteTotal:             d.evictDeleteTotal.Load(),
		EvictSelfGoneTotal:           d.evictSelfGoneTotal.Load(),
		EvictDropPointTotal:          d.evictDropPointTotal.Load(),
		DirtyMarkTotal:               d.dirtyMarkTotal.Load(),
		EnqueueUpdateTotal:           d.enqueueUpdateTotal.Load(),
		OnObjectEventDegradedNoEvict: d.onObjectEventDegradedNoEvict.Load(),
		RemoveL1Total:                d.removeL1Total.Load(),
		UnguardedPutTotal:            d.unguardedPutTotal.Load(),
	}
}

// UnguardedPutTotal is the #375 (B) DETECTOR accessor: the number of ACCEPTED
// gen-guarded Puts (PutIfGen / ReplaceIfGen / PutRAFullListIfGen) whose resolve ctx
// carried NO dep-gen sink — i.e. a resolve entry that never went through
// WithL1KeyContext / WithDepGenSink. Each such Put was remarked once (fail-fresh), but a
// non-zero value means a resolve path is outside the dep-generation guard. Expected 0.
// Published on expvar (snowplow_deps.unguarded_put_total) and OTLP
// (snowplow_deps_unguarded_put_total).
func UnguardedPutTotal() uint64 {
	return Deps().unguardedPutTotal.Load()
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
	// Order is load-bearing (1.12.6 item 7 gate + #206, -race at -count=3):
	//  1. stop + JOIN the resolved_cache.summary goroutine FIRST. It is a
	//     PURE READER that starts no workers, and each tick reads THREE
	//     subsystems — Deps().Stats(), the refresher, AND DepWatch
	//     (DepWatchStatsSnapshot). Quiescing it before any teardown
	//     eliminates the reader-vs-teardown window for ALL THREE by
	//     construction — in particular vs resetDepWatchForTest (step 3),
	//     which replaces depWatchOnce while a tick could still be inside
	//     DepWatchStatsSnapshot. (The 300s default hid this; #248's
	//     1s-cadence wiring test removes that improbability.)
	//  2. stop + JOIN the watcher bound to the bridge — its informer
	//     handlers captured the bridge singleton at registration, and an
	//     ADD they deliver after the singleton is replaced would start a
	//     worker nobody can stop any more (an orphan that keeps reading
	//     Deps() under the next test's reset);
	//  3. stop + join the dep-event worker: it reads Deps() on its own
	//     goroutine (1.12.6 C1 — every event, not only DELETEs);
	//  4. only then write the tracker fields.
	stopResolvedCacheSummaryForTest()
	stopBoundDepWatcherForTest()
	resetDepWatchForTest()
	resetDepsForTest()
}

// stopBoundDepWatcherForTest stops the ResourceWatcher currently bound to
// the dep-watch bridge (the one whose informer handlers feed it) and
// blocks until every goroutine that watcher spawned has exited
// (ResourceWatcher.Stop joins the factory and the watcher-owned goroutines),
// so no handler can reach the bridge after the reset that follows.
// Idempotent (Stop is). A bare watcher built by struct literal in a test
// (no NewResourceWatcher: nil stopCh, no factory, no goroutines) has
// nothing to join and is skipped — Stop would close a nil channel.
// Test-only — production never resets the bridge.
func stopBoundDepWatcherForTest() {
	w := depWatchInstance
	if w == nil {
		return
	}
	rw := w.watcher.Load()
	if rw == nil {
		return
	}
	rw.mu.RLock()
	bare := rw.stopCh == nil
	rw.mu.RUnlock()
	if bare {
		return
	}
	rw.Stop()
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
