// informer_dispatch.go — Tag 0.30.95 resolver pivot.
//
// Routes resolver GET reads to the in-process informer cache whenever
// the cache subsystem is on. The pivot eliminates per-call apiserver
// round-trips for the K8s read shapes A-D in the resolver
// (compositions-list, sidebar widgets, resourceRefs targets, etc.):
// under apiserver-routed dispatch each inner-call cost a full TLS
// handshake + apiserver LIST/GET; under the pivot the same call is
// served from the indexer in O(1) (GET) or O(N) over the namespace
// partition (LIST), with zero network I/O.
//
// #57 — implicit-on-cache. The pivot was originally gated by a
// standalone `RESOLVER_USE_INFORMER` env flag (staged-rollout default
// OFF at 0.30.95 → bench-only 0.30.96 → prod 0.30.97). That flag was
// folded away in Task #57: the pivot is now implicit under the single
// CACHE_ENABLED master gate (`resolverUseInformer()` returns
// `!cache.Disabled()`). The staged rollout is complete and production
// runs cache-on, so the flag had no remaining purpose. A stale
// RESOLVER_USE_INFORMER in the environment is ignored (main.go's
// retired-flag audit warns once).
//
//   - cache ON  — pivot active. dispatchViaInformer serves the call when
//                 it can; falls through to apiserver for the gated edge
//                 cases (verb gate, subresource paths, external paths,
//                 passthrough mode, unsynced informer, metadata-only
//                 routed GVR, 404).
//   - cache OFF — pivot is a no-op; every call takes the apiserver branch.
//                 The architect's falsifier R-FALSE-1 ("binary with the
//                 pivot inactive is byte-identical to the apiserver path")
//                 is preserved by the cache-off toggle. Note Gate-4 below
//                 ALSO re-checks cache.Disabled() as defense-in-depth.
//
// Per `feedback_no_special_cases.md`: the pivot is uniform across GVRs.
// No per-resource carve-out. The gate is verb (GET only) + path-shape
// (apiserver-routed only) + informer state (synced, full Unstructured)
// — all are predicate inputs, not switch arms.

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/env"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// envSyncWaitMS is the Ship 0.30.121 R2-b knob: the maximum time a Gate-6
// dispatch will block waiting for a freshly-registered GVR's informer to
// finish its initial WaitForCacheSync. Default 0 = disabled = today's
// behaviour (no wait — fall straight through to the apiserver). When set
// positive, a Gate-6 miss does ONE bounded channel-select wait, then
// re-checks servability: a synced informer serves the request from cache
// instead of paying a second apiserver LIST. The wait is single-attempt
// and hard-capped — no unbounded goroutine, no retry loop.
const envSyncWaitMS = "RESOLVER_SYNC_WAIT_MS"

// syncWaitBudget returns the R2-b bounded sync-wait duration, 0 when
// disabled (the default). A non-positive env value disables the wait.
func syncWaitBudget() time.Duration {
	ms := env.Int(envSyncWaitMS, 0)
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// resolverUseInformer reports whether the resolver pivot is active.
// Folded in Task #57 to be implicit-on-cache: it returns
// `!cache.Disabled()` — the pivot is on iff the cache subsystem is on.
// The standalone RESOLVER_USE_INFORMER env flag was retired (see the
// package-doc #57 note); the sole production caller (resolve.go) now
// reads this bool directly. Cheap enough to read per dispatch
// (delegates to cache.Disabled()).
func resolverUseInformer() bool {
	return !cache.Disabled()
}

// subresourceSuffixes lists the apiserver subresource path tails that
// cannot be served from the informer cache:
//
//   - /status       — typed-RBAC writers + scaling subresources.
//   - /scale        — autoscaler workflows.
//   - /log, /exec   — Pod subresources (streaming; no cache shape).
//   - /binding      — legacy scheduler-side bindings.
//   - /proxy        — service/pod proxy passthrough.
//
// The list is hardcoded but the matcher is uniform: every entry is a
// suffix check, no per-resource branching. Adding a future subresource
// here is a single-line append.
var subresourceSuffixes = []string{
	"/status",
	"/scale",
	"/log",
	"/exec",
	"/binding",
	"/proxy",
}

// hasSubresourceSuffix returns true when path ends with one of the
// well-known apiserver subresource tails. Strips an optional trailing
// slash and the query string before matching.
//
// We match on the trailing path segment (not arbitrary substring) so
// resource names that contain the suffix string (e.g. a Deployment
// literally named "status") are not false-positive-rejected.
func hasSubresourceSuffix(path string) bool {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	path = strings.TrimRight(path, "/")
	for _, sfx := range subresourceSuffixes {
		if strings.HasSuffix(path, sfx) {
			return true
		}
	}
	return false
}

// readerFromBytes adapts a byte slice to io.ReadCloser so the pivot
// branch can invoke `call.ResponseHandler` (which expects an
// io.ReadCloser) with cache-served bytes. The handler reads to EOF and
// closes — bytes.NewReader is io.Reader-only; we wrap with NopCloser.
func readerFromBytes(b []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(b))
}

// listKindForResource synthesizes the LIST envelope's `kind` field
// from a GVR. The K8s apiserver returns "<Kind>List" (Kind from the
// CRD's spec.names.kind, e.g. compositions → CompositionList). The
// resolver doesn't have direct access to that Kind at the lister
// boundary; we synthesize from gvr.Resource by capitalising the
// first letter and appending "List" (compositions → CompositionsList,
// panels → PanelsList).
//
// The shape is NOT 1:1 with apiserver — apiserver uses the singular
// Kind, we use the plural Resource. But the rule is uniform across
// every GVR (no per-resource carve-out), deterministic, and unique.
// Per PM 2026-05-15: defensive against widget JQ expressions that
// may key off `.kind`; using the simplified "List" form would risk
// breaking any such filter without warning.
//
// Empty resource is defensive-handled by returning "List" — only
// reachable from malformed callers; production callers always have a
// non-empty Resource (predicate gates would have rejected the call
// upstream).
//
// Per `feedback_no_special_cases.md`: the rule is `upper(Resource[0]) +
// Resource[1:] + "List"` for every GVR. No switch arms.
func listKindForResource(resource string) string {
	if resource == "" {
		return "List"
	}
	// ASCII-uppercase the first byte; K8s resource names are
	// guaranteed lowercase ASCII per the CRD naming spec
	// (kubernetes.io/docs/concepts/overview/working-with-objects/names/).
	first := resource[0]
	if first >= 'a' && first <= 'z' {
		first -= 'a' - 'A'
	}
	return string(first) + resource[1:] + "List"
}

// marshalAsList produces the apiserver-shaped LIST envelope:
//
//	{
//	  "apiVersion": "<group>/<version>",   // or "v1" for the core group
//	  "kind":       "<R>List",              // synthesized via listKindForResource
//	  "items":      [ ... unstructured objects ... ]
//	}
//
// Per PM 2026-05-15 (DEV-Q2 resolution): kind is the typed `<R>List`
// form rather than the simplified "List". Widget JQ filters that key
// off `.kind` see a stable, predictable shape that won't trip on the
// pivot vs apiserver branch. The synthesized kind is derived uniformly
// from gvr.Resource (see listKindForResource).
//
// For the empty case (zero items), we still emit `"items": []` so the
// JQ `.items[]` iterator produces an empty stream rather than a null —
// matches apiserver behaviour for an empty namespace LIST.
//
// The apiVersion follows the apiserver convention: core group "" uses
// "v1"; group "g" uses "g/v". GVRs we receive carry these fields
// directly so no special-case is needed.
func marshalAsList(apiVersion, listKind string, items []*unstructured.Unstructured) ([]byte, error) {
	itemList := make([]any, 0, len(items))
	for _, it := range items {
		if it != nil {
			itemList = append(itemList, it.Object)
		}
	}
	envelope := map[string]any{
		"apiVersion": apiVersion,
		"kind":       listKind,
		"items":      itemList,
	}
	return json.Marshal(envelope)
}

// marshalAsListRaw builds the SAME envelope marshalAsList builds, from
// per-item JSON the informer indexer already holds — #578.
//
// BYTE-IDENTICAL TO marshalAsList, BY CONSTRUCTION. Three properties make that
// true, and TestIssue578_MarshalAsListRaw_ByteIdenticalToMarshalAsList is the
// falsifier that keeps them true:
//
//  1. KEY ORDER. json.Marshal emits a map's keys in sorted order, so
//     marshalAsList produces apiVersion, items, kind. This function emits that
//     same order literally. (Writing them in the "natural" apiVersion/kind/items
//     order would be semantically identical JSON but NOT byte-identical, and
//     byte-parity is what makes this change safe to land without re-blessing
//     every golden.)
//  2. PER-ITEM BYTES. A bytesObject's `raw` is json.Marshal of the same
//     Unstructured map that marshalAsList would have marshalled, so each item's
//     bytes match key-for-key. Integer fidelity survives the comparison because
//     the decode side uses sigs.k8s.io/json's PreserveInts behaviour rather than
//     landing every number on float64.
//  3. STRIPPING. managedFields and the last-applied annotation are removed by
//     defaultStripUnstructured BEFORE newBytesObject marshals, so the stored
//     bytes are already in the stripped shape the serve path expects. Nothing is
//     reintroduced by concatenating them.
//
// The empty case still emits `"items":[]` (never null) so a JQ `.items[]`
// iterator yields an empty stream, matching marshalAsList and the apiserver.
//
// The returned buffer is freshly allocated and owned by the caller; the input
// slices are only read (they alias immutable bytesObject arrays).
func marshalAsListRaw(apiVersion, listKind string, raws [][]byte) ([]byte, error) {
	// Marshal the two strings rather than quoting them by hand so that any
	// character needing escaping is escaped EXACTLY as encoding/json would.
	av, err := json.Marshal(apiVersion)
	if err != nil {
		return nil, err
	}
	lk, err := json.Marshal(listKind)
	if err != nil {
		return nil, err
	}

	const (
		pfxAPIVersion = `{"apiVersion":`
		pfxItems      = `,"items":[`
		pfxKind       = `],"kind":`
		suffix        = `}`
	)

	// Exact size: no growth reallocation on the hot path.
	n := len(pfxAPIVersion) + len(av) + len(pfxItems) + len(pfxKind) + len(lk) + len(suffix)
	for _, r := range raws {
		n += len(r)
	}
	if len(raws) > 1 {
		n += len(raws) - 1 // the separating commas
	}

	buf := make([]byte, 0, n)
	buf = append(buf, pfxAPIVersion...)
	buf = append(buf, av...)
	buf = append(buf, pfxItems...)
	for i, r := range raws {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, r...)
	}
	buf = append(buf, pfxKind...)
	buf = append(buf, lk...)
	buf = append(buf, suffix...)
	return buf, nil
}

// dispatchViaInformer attempts to serve `call` from the informer cache.
// Returns (rawBytes, true) on success — caller feeds the bytes to
// `call.ResponseHandler`. Returns (nil, false) for any gate that
// requires the apiserver branch:
//
//   - non-GET verb (POST/PUT/PATCH/DELETE — informer is read-only)
//   - subresource path (.../status, .../scale, .../log, .../exec, ...)
//   - non-apiserver path (external URL, unparseable, JQ-leaked `${...}`)
//   - passthrough mode (cache=off; no informer to serve from)
//   - informer not yet registered for this GVR
//   - informer not yet synced (would return empty silently)
//   - metadata-only routed GVR (PartialObjectMetadata cannot satisfy
//     resolver reads — only carries metadata, not spec/status)
//   - GET-by-name 404 (preserves apiserver's error envelope shape)
//   - GET-by-name RBAC-denied / no-identity / evaluator error
//     (Tag 0.30.101 filterGetByRBAC — apiserver's per-user token
//     applies the authoritative 403)
//
// On the served path, LIST output is wrapped in the apiserver LIST
// envelope via marshalAsList; GET-by-name output is the bare object
// (apiserver shape — single Unstructured serialised as-is).
//
// Tag 0.30.100: the served LIST branch additionally runs every item
// through filterListByRBAC — a post-LIST per-item RBAC check against
// the in-process typed-RBAC indexer — before marshalling. The pivot
// bypasses the per-user `<username>-clientconfig` token, so without
// this filter an apiserver-routed LIST with no userAccessFilter stanza
// over-exposes every object to a narrow-RBAC user. A LIST request with
// no identity on the context fails closed (served=false → apiserver
// fallthrough).
//
// Tag 0.30.101: the served GET-by-name branch runs the hit object
// through filterGetByRBAC — the GET-verb sibling of the same check.
// Without it the pivot's GET branch over-exposes a single object the
// same way: a narrow-RBAC user GETting a known name in a namespace
// they have no `get` grant for would receive it. A denied GET, a
// missing identity, or an evaluator error all fail closed
// (served=false → apiserver fallthrough).
//
// Per `feedback_cache_must_not_constrain_jq.md`: the envelope bytes
// MUST be byte-equivalent to apiserver for the JQ pipeline to remain
// invariant. The shape we produce is the canonical Unstructured shape
// for items and the canonical LIST envelope for the wrapper.
//
// Concurrency: safe for parallel callers (informer indexer is RWMutex-
// protected internally; env-var read is atomic in Go's envcache).
//
// `dispatchViaInformer` does NOT mutate the dict — that responsibility
// belongs to `call.ResponseHandler` (the lambda the resolver constructs
// that wraps jsonHandler under dictMu). The pivot only returns bytes;
// the caller honours dictMu through the same handler.
// dispatchViaInformerFn is the #189 TEST SEAM for the apistage content-serve MISS
// resolve. Production is dispatchViaInformer; the apistage MISS branch calls
// through this var so a test can inject a DELETE-eviction of the content key
// BETWEEN CaptureGen and PutIfGen (the resurrection-race window) — the same
// injectable-dispatch-seam idiom as dispatchers/dispatch_seams.go's
// widgetsResolveFn. Never reassigned in production; only the apistage MISS site
// reads it (the other dispatchViaInformer callers call the func directly).
var dispatchViaInformerFn = dispatchViaInformer

func dispatchViaInformer(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
	log := xcontext.Logger(ctx)

	// 0.30.96: lazy-start the informer_dispatch.summary goroutine the
	// first time the pivot is exercised (sync.Once-bounded; never
	// started when RESOLVER_USE_INFORMER stays off for the process
	// lifetime). The caller only invokes dispatchViaInformer when the
	// flag is "true", so reaching here means the pivot is active.
	startDispatchSummary()

	// Gate 1: verb. Informer cache is read-only — POST/PUT/PATCH/
	// DELETE all require the apiserver. The default verb is GET when
	// call.Verb is nil (httpcall convention).
	if v := ptr.Deref(call.Verb, http.MethodGet); v != http.MethodGet {
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-2 sub-reason: write verb on the inner-call path.
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerWriteVerb, "")
		return nil, false
	}

	// Gate 2: subresource path. Status/scale/log/exec/binding/proxy
	// reads have no informer-cache shape and must hit the apiserver.
	if hasSubresourceSuffix(call.Path) {
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-2 sub-reason: subresource (permanent allowance).
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerSubresource, "")
		return nil, false
	}

	// Gate 3: parse path → GVR + namespace + name. Non-apiserver
	// paths (external URLs, JQ-leaked `${...}`, unrecognised shapes)
	// return ok=false from ParseAPIServerPathToDep and we fall back.
	gvr, namespace, name, parseOK := cache.ParseAPIServerPathToDep(call.Path)
	if !parseOK {
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-2 sub-reason: external URL / unparseable. Both
		// the "external URL" and the "unparseable" cases reach here;
		// using ReasonInformerUnparseable as the umbrella keeps the
		// closed enum tight (the design's §F-2 lists both as
		// permanent allowances — they cannot be cache-served).
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerUnparseable, "")
		return nil, false
	}

	// Gate 4: cache mode. modePassthrough has no informers; nothing
	// to serve from. cache.Disabled() also implies no watcher.
	rw := cache.Global()
	if rw == nil || rw.IsPassthrough() || cache.Disabled() {
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-2 sub-reason: passthrough watcher (cache=off /
		// degraded mode). RecordApiserverFallthrough short-circuits on
		// Disabled() upstream — the call is no-op in cache=off.
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerPassthrough, gvr.String())
		return nil, false
	}

	// Gate 5: metadata-only routed GVRs. PartialObjectMetadata
	// informers carry ObjectMeta only — no spec, no status. Serving
	// such a read would return shape-incompatible bytes. The pivot
	// MUST fall through to the apiserver for these GVRs.
	//
	// #342 — this gate is LIVE-but-always-FALSE in production: rw.IsMetadataOnly
	// delegates to shouldUseMetadataOnly, which is constant-false by
	// construction (no GVR reaches a `return true`). See the #197 RESOLUTION
	// header at internal/cache/cache_mode.go for why it is inert-not-dormant
	// and the KEEP-DOCUMENTED ruling. The gate is retained as a correctness
	// guard should a future ship re-introduce a non-RBAC metadata-only route.
	if rw.IsMetadataOnly(gvr) {
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-2 sub-reason: metadata-only GVR (permanent
		// allowance — the informer carries no full-object shape).
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerMetadataOnly, gvr.String())
		return nil, false
	}

	// Gate 5a (#398): non-default response REPRESENTATION. The informer holds
	// full objects and can only answer in the default JSON shape; a call whose
	// Accept header asks for another representation (`as=PartialObjectMetadata`,
	// `as=Table`, ...) must reach the apiserver, which honours it natively.
	// General for every GVR — this is how a portal asks "does this object
	// exist?" without receiving its body.
	if wantsNonDefaultRepresentation(call.Headers) {
		dispatchInformerFallthrough.Add(1)
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerRepresentation, gvr.String())
		return nil, false
	}

	// Gate 5b (#398): sensitive resource (cache.IsSensitiveResource — core
	// v1/secrets). Never informed (EnsureResourceType refuses it), so never
	// served here: fall straight through to the apiserver under the caller's own
	// credentials, with a reason of its own rather than a perpetual
	// "not-synced", and without touching EnsureResourceType.
	if cache.IsSensitiveResource(gvr) {
		dispatchInformerFallthrough.Add(1)
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerSensitive, gvr.String())
		return nil, false
	}

	// Gate 6: informer registered + synced. If the GVR is not yet
	// registered, fire EnsureResourceType (sub-microsecond singleflight
	// when already registered; lazy registration when not) so a
	// SUBSEQUENT call after sync completes can serve.
	//
	// Ship 0.30.121 R2-b — bounded single-attempt sync-wait. By default
	// (RESOLVER_SYNC_WAIT_MS=0) an unsynced GVR falls straight through to
	// the apiserver, byte-identical to pre-0.30.121: pre-sync reads would
	// return empty slices indistinguishable from a real "no objects"
	// answer. When the knob is set positive, this dispatch does ONE
	// channel-select wait — hard-capped at the budget, no retry loop, no
	// goroutine — on the per-GVR sync channel EnsureResourceType returns.
	// After the wait it re-checks IsServable: a synced informer serves
	// the request from cache (eliminating a second apiserver LIST — the
	// ~5.81 GiB httpcall.Do alloc line); a still-unsynced informer falls
	// through exactly as before. The Gate-6 fall-through itself is
	// preserved unconditionally — serving an unsynced empty list broke S4
	// (regression journal 2026-05-15).
	if !rw.IsSynced(gvr) {
		// Best-effort lazy registration. EnsureResourceType is idempotent
		// (singleflight under rw.mu); duplicate calls are sub-microsecond
		// no-ops. It returns the per-GVR sync channel — closed once that
		// informer's initial WaitForCacheSync completes.
		added, syncCh := rw.EnsureResourceTypeFor(ctx, gvr)

		// Fix #1 (1b) — stale-delete confirm-on-register
		// (docs/rca-stale-delete-compositiondefinitions-informer-2026-06-25.md §6).
		// When THIS dispatch is the one that lazily registers the GVR
		// (added==true) on a LIST (name==""), prime ONE scoped conjunct-3/4
		// confirmation pass so the GVR does not have to wait a full 30s
		// discovery-refresh tick to become servable, and a transient
		// boot-time discovery flap self-corrects. ConfirmResourceType reuses
		// RefreshDiscovery's confirm body (no forked predicate) and writes
		// rw.confirmed / rw.watchBroken under rw.mu.
		//
		// LIST-ONLY (feedback_bounding_mechanism_discipline + the 1.5.1
		// boot-OOM): gated on name=="" so the GET-by-name child-resource
		// fan-out never primes a confirm — no child informer is forced.
		// `added`-gated so it is a one-shot per registration, not per
		// dispatch — a duplicate dispatch returns added==false and skips it.
		if added && name == "" {
			rw.ConfirmResourceType(ctx, gvr)
		}

		// R2-b bounded single-attempt sync-wait. servedAfterWait is set
		// only when the budget is positive AND the informer became
		// servable within it — that case proceeds to the served path; in
		// every other case the dispatch falls through to the apiserver.
		servedAfterWait := false
		if budget := syncWaitBudget(); budget > 0 && syncCh != nil {
			timer := time.NewTimer(budget)
			select {
			case <-syncCh:
				// Initial WaitForCacheSync completed within the budget.
				timer.Stop()
			case <-ctx.Done():
				// Request cancelled — abandon the wait, fall through.
				timer.Stop()
			case <-timer.C:
				// Budget exhausted — informer still not synced.
			}
			// Re-check servability ONCE after the bounded wait. A now-
			// servable GVR proceeds to the served path below (no second
			// apiserver LIST).
			if rw.IsServable(gvr) {
				log.Debug("informer_dispatch.sync_wait.served",
					slog.String("gvr", gvr.String()),
					slog.String("ns", namespace),
					slog.String("path", call.Path),
				)
				dispatchInformerSyncWaitServed.Add(1)
				servedAfterWait = true
			}
		}

		if !servedAfterWait {
			log.Debug("informer_dispatch.fallthrough.not_synced",
				slog.String("gvr", gvr.String()),
				slog.String("ns", namespace),
				slog.String("name", name),
				slog.String("path", call.Path),
			)
			dispatchInformerFallthrough.Add(1)
			// Ship D — F-2 sub-reason: GVR not yet synced (Gate 6 —
			// the RESOLVER_SYNC_WAIT_MS budget expired without sync).
			cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotSynced, gvr.String())
			return nil, false
		}
	}

	// Served path. Two shapes: LIST (name=="") and GET-by-name.
	apiVersion := gvr.Version
	if gvr.Group != "" {
		apiVersion = gvr.Group + "/" + gvr.Version
	}

	if name == "" {
		// LIST. namespace="" means cluster-wide. 0.30.97: serve via
		// ListObjectsServable so the registered+synced check and the
		// indexer read are ONE atomic-ish operation — no check-then-act
		// gap between an IsSynced precheck and a separate ListObjects
		// call. `servable=false` (unregistered GVR, or a registered
		// informer whose initial LIST has not completed) MUST fall
		// through to the apiserver: an empty slice from an unsynced or
		// unregistered informer is indistinguishable from a genuine
		// "no objects" answer, and silently serving it broke the
		// Compositions feature at S4 (regression journal 2026-05-15).
		// A genuinely-empty-but-synced informer still returns
		// `servable=true` with an empty slice — that is a real answer
		// the watcher can vouch for.
		// #578 — ZERO-DECODE ENVELOPE for the identity-free content
		// substrate. When this resolve is an api-stage CONTENT resolve the
		// inline RBAC gate is SKIPPED by design (see the Ship F1 note
		// below): the entry must store UN-GATED content, so nothing on
		// this path reads a per-item field. That makes the decoded
		// map trees pure waste — the envelope is a concatenation of the
		// per-item JSON the indexer already holds.
		//
		// This is a structural discriminant, not a resource carve-out
		// (feedback_no_special_cases): the predicate is "does this call
		// need per-item access?", answered by the SAME
		// ApistageContentResolveFromContext that already decides whether
		// the gate runs. Every other caller keeps the decoded path below,
		// unchanged, because filterListByRBAC genuinely reads
		// it.GetNamespace() per item.
		//
		// Measured motivation (krateo-057, portal IDLE 38 min, so this is
		// refresher cost not serve cost): bytesObject.Decode 28.0% and
		// parseListEnvelope 27.6% of a 30s CPU profile, 568 MB/s of
		// allocation, pod pinned at 98.75% of a 4-CPU limit with ZERO
		// users. #578.
		if cache.ApistageContentResolveFromContext(ctx) {
			raws, servable := rw.ListRawServable(gvr, namespace)
			if !servable {
				log.Debug("informer_dispatch.fallthrough.list_not_servable",
					slog.String("gvr", gvr.String()),
					slog.String("ns", namespace),
					slog.String("path", call.Path),
					slog.String("shape", "raw"),
				)
				dispatchInformerFallthrough.Add(1)
				cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, gvr.String())
				return nil, false
			}
			raw, err := marshalAsListRaw(apiVersion, listKindForResource(gvr.Resource), raws)
			if err != nil {
				log.Warn("informer_dispatch.list_marshal_failed",
					slog.String("gvr", gvr.String()),
					slog.String("ns", namespace),
					slog.String("shape", "raw"),
					slog.Any("err", err),
				)
				dispatchInformerFallthrough.Add(1)
				cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, gvr.String())
				return nil, false
			}
			log.Debug("informer_dispatch.list_served",
				slog.String("gvr", gvr.String()),
				slog.String("ns", namespace),
				slog.String("shape", "raw"),
				slog.Int("items", len(raws)),
				slog.Int("bytes", len(raw)),
			)
			dispatchInformerListServed.Add(1)
			dispatchInformerListServedRaw.Add(1)
			return raw, true
		}

		items, servable := rw.ListObjectsServable(gvr, namespace)
		if !servable {
			log.Debug("informer_dispatch.fallthrough.list_not_servable",
				slog.String("gvr", gvr.String()),
				slog.String("ns", namespace),
				slog.String("path", call.Path),
			)
			dispatchInformerFallthrough.Add(1)
			// Ship D — F-2 sub-reason: LIST informer unservable (the
			// post-Gate-6 re-check; servable means registered AND
			// HasSynced).
			cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, gvr.String())
			return nil, false
		}

		// Tag 0.30.100: post-LIST per-item RBAC filter. The informer
		// partition is RBAC-blind — dispatchViaInformer never reads
		// call.Endpoint, so it bypasses the per-user `<username>-
		// clientconfig` bearer token that narrows the apiserver path.
		// For an apiserver-routed LIST with no userAccessFilter stanza
		// (e.g. compositions-list) that token was the ONLY RBAC gate;
		// without this filter a narrow-RBAC user sees every object
		// (0.30.99 Phase-6 "Finding 1"). filterListByRBAC drops items
		// the user has no `list` grant for. FAIL-CLOSED: a missing
		// identity returns served=false → apiserver fallthrough (the
		// apiserver's per-user token narrows correctly); a per-item
		// EvaluateRBAC error drops that item.
		//
		// Ship F1 (0.30.119): SKIP the inline gate for an api-stage
		// CONTENT resolve (cache.ApistageContentResolveFromContext). The
		// api-stage L1 entry is identity-free and must store UN-GATED
		// content; gating here would bake one user's narrowed view into
		// the shared entry. The api-stage stage loop runs the gate at a
		// single site — on the Get-hit AND the miss path — before the
		// content reaches dict[id]. Marked dispatches return the raw
		// indexer items.
		if !cache.ApistageContentResolveFromContext(ctx) {
			var identityOK bool
			items, identityOK = filterListByRBAC(ctx, gvr, items)
			if !identityOK {
				dispatchInformerFallthrough.Add(1)
				// Ship D — F-2 sub-reason: RBAC denied at LIST gate
				// (missing identity or evaluator error → fail-closed).
				cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerRBACDeny, gvr.String())
				return nil, false
			}
		}

		raw, err := marshalAsList(apiVersion, listKindForResource(gvr.Resource), items)
		if err != nil {
			log.Warn("informer_dispatch.list_marshal_failed",
				slog.String("gvr", gvr.String()),
				slog.String("ns", namespace),
				slog.Any("err", err),
			)
			dispatchInformerFallthrough.Add(1)
			// Ship D — F-2 sub-reason: marshal failure is a defensive
			// fall-through (should never fire in practice; the WARN log
			// is the operator signal). Classify under "not-servable"
			// since we can't serve usable bytes.
			cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, gvr.String())
			return nil, false
		}
		log.Debug("informer_dispatch.list_served",
			slog.String("gvr", gvr.String()),
			slog.String("ns", namespace),
			slog.Int("items", len(items)),
			slog.Int("bytes", len(raw)),
		)
		dispatchInformerListServed.Add(1)
		return raw, true
	}

	// GET-by-name. 0.30.97: re-check servability immediately before
	// the indexer read. The Gate-6 IsSynced precheck above and this
	// GetObject call are otherwise two separate lock acquisitions —
	// the informer's registered/synced state could flip between them.
	// IsServable (registered AND HasSynced) and GetObject share the
	// same gi handle's HasSynced observation, so a not-yet-fully-synced
	// GVR can never serve a stale/partial object here. `servable=false`
	// falls through to the apiserver, same as the LIST branch.
	if !rw.IsServable(gvr) {
		log.Debug("informer_dispatch.fallthrough.get_not_servable",
			slog.String("gvr", gvr.String()),
			slog.String("ns", namespace),
			slog.String("name", name),
		)
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-2 sub-reason: GET informer unservable.
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, gvr.String())
		return nil, false
	}
	// The indexer returns (obj, true) on hit and (nil, false) on
	// miss. A miss MUST fall through to apiserver — apiserver returns
	// a 404 with a specific Status envelope that the JQ pipeline
	// expects (Status kind, code:404). Serving a synthetic 404 here
	// would either swallow the shape or duplicate it; the cleanest
	// contract is "miss = let apiserver answer".
	obj, hit := rw.GetObject(gvr, namespace, name)
	if !hit {
		log.Debug("informer_dispatch.get_miss_fallthrough",
			slog.String("gvr", gvr.String()),
			slog.String("ns", namespace),
			slog.String("name", name),
		)
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-6 allowed-fallthrough: GET-miss → let apiserver
		// answer 404. Documented contract: the apiserver returns the
		// canonical Status envelope (Status kind, code:404) that the
		// JQ pipeline expects; synthesizing it here would either lose
		// the shape or duplicate it.
		cache.RecordApiserverFallthrough(ctx, cache.ReasonGetMissLetApiserver404, gvr.String())
		return nil, false
	}

	// Tag 0.30.101: GET-verb RBAC check — the GET-by-name sibling of the
	// Tag-0.30.100 post-LIST per-item filter. The pivot bypasses the
	// per-user `<username>-clientconfig` bearer token (dispatchViaInformer
	// never reads call.Endpoint), so without this check a narrow-RBAC
	// user GETting a known object name in a namespace they have no `get`
	// grant for would receive the raw informer object. filterGetByRBAC
	// runs the object through a `get`-verb EvaluateRBAC against the
	// in-process typed-RBAC indexer. FAIL-CLOSED: a denied GET, a missing
	// identity, or an evaluator error all return false → apiserver
	// fallthrough (the apiserver's per-user token correctly 403s).
	//
	// Ship F1 (0.30.119): SKIP the inline gate for an api-stage CONTENT
	// resolve (same rationale as the LIST branch above) — the api-stage
	// stage loop gates the Get-hit and the miss path at a single site.
	if !cache.ApistageContentResolveFromContext(ctx) {
		if !filterGetByRBAC(ctx, gvr, obj) {
			dispatchInformerFallthrough.Add(1)
			// Ship D — F-2 sub-reason: GET-verb RBAC denied (or
			// missing identity / evaluator error → fail-closed). The
			// apiserver's per-user token will correctly 403.
			cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerRBACDeny, gvr.String())
			return nil, false
		}
	}

	raw, err := json.Marshal(obj.Object)
	if err != nil {
		log.Warn("informer_dispatch.get_marshal_failed",
			slog.String("gvr", gvr.String()),
			slog.String("ns", namespace),
			slog.String("name", name),
			slog.Any("err", err),
		)
		dispatchInformerFallthrough.Add(1)
		// Ship D — F-2 sub-reason: GET marshal failure (defensive;
		// same posture as the LIST-marshal branch).
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, gvr.String())
		return nil, false
	}
	log.Debug("informer_dispatch.get_served",
		slog.String("gvr", gvr.String()),
		slog.String("ns", namespace),
		slog.String("name", name),
		slog.Int("bytes", len(raw)),
	)
	dispatchInformerGetServed.Add(1)
	return raw, true
}

// wantsNonDefaultRepresentation reports whether any Accept header in headers
// asks for a non-default representation via the `as=` media-type parameter
// (e.g. `application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1`).
// Headers are "Name: value" strings (httpcall convention); the name match is
// case-insensitive.
func wantsNonDefaultRepresentation(headers []string) bool {
	for _, h := range headers {
		name, value, ok := strings.Cut(h, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "Accept") {
			continue
		}
		for _, mediaType := range strings.Split(value, ",") {
			for _, param := range strings.Split(mediaType, ";")[1:] {
				if k, _, ok := strings.Cut(strings.TrimSpace(param), "="); ok && strings.EqualFold(k, "as") {
					return true
				}
			}
		}
	}
	return false
}
