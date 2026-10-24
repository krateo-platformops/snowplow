// apistage.go — Ship F1 (0.30.119): content-keyed api-stage L1.
//
// Ship E (0.30.116) keyed the api-stage L1 per-STAGE and per-USER, storing
// each stage's post-filter, N-way-merged dict[id]. Ship F1 reshapes it to
// the CONTENT-KEYED model:
//
//   - Per-K8s-CALL granularity. The content entry is the raw apiserver
//     envelope of ONE dispatch (dispatchViaInformer's return), keyed
//     (gvr, namespace, name-or-empty). A stage with an iterator fans into
//     N calls -> N content entries. One entry per distinct (gvr, ns,
//     [name]) is SHARED across stages, RESTActions, and users.
//
//   - IDENTITY-FREE. K8s RBAC is a binary gate on (gvr, ns, [name]) units
//     — it never filters items or shapes content, so the raw envelope of
//     a K8s call is identity-invariant. ComputeKey drops Username/Groups
//     for CacheEntryClassApistage (a per-class key shape, not a
//     per-resource switch — feedback_no_special_cases). The per-user
//     narrowing moves to the serve-time RBAC gate (resolve.go).
//
//   - Stores the RAW, UN-GATED envelope. The dispatch is un-gated
//     (cache.WithApistageContentResolve makes dispatchViaInformer skip its
//     inline filterListByRBAC/filterGetByRBAC); the per-user gate runs at
//     a single site in resolve.go, on the Get-hit AND the miss path,
//     before the stage filter produces dict[id]. Narrowed content is
//     never stored — no cross-user leak.
//
// EVERYTHING here is gated by cache.ApistageL1Enabled() — on with the
// cache subsystem (folded under the master gate per #57). With the
// resolved-output store off the resolver is byte-identical to 0.30.118
// (AC-F1.1).

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/pprof"
	"sync/atomic"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/listenvelope"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// contentKeyInputs assembles the cache.ResolvedKeyInputs for ONE K8s call
// — the Ship F1 content-keyed unit. The key is (gvr, namespace,
// name-or-empty) under CacheEntryClassApistage; ComputeKey drops the
// identity fields for that class, so the entry is identity-free and
// shared by every user the serve-time gate admits.
//
// name=="" is a LIST call; a non-empty name is a GET-by-name. The
// (gvr, ns, name) tuple fully identifies the K8s call — the stage
// filter (applied post-Get) and the stage-input (which determines WHICH
// calls createRequestOptions emits, each already captured by its own
// tuple) deliberately do NOT enter the content key.
//
// Username/Groups are left zero — ComputeKey ignores them for the
// apistage class. They are omitted entirely rather than threaded-and-
// ignored so the content nature of the key is explicit at the call site.
func contentKeyInputs(gvr schema.GroupVersionResource, namespace, name string) cache.ResolvedKeyInputs {
	return contentKeyInputsQ(gvr, namespace, name, "")
}

// contentKeyInputsQ is contentKeyInputs for a call that carries a query string
// (#618). query is the CANONICAL form planListQuery produced — never the raw
// call query — so two spellings of the same selector share a cell, and a
// selector call never shares the unselected call's cell (ComputeKey folds a
// non-empty Query behind its own sentinel). The (gvr, ns, name) tuple alone
// does NOT identify a K8s LIST once it carries a selector.
func contentKeyInputsQ(gvr schema.GroupVersionResource, namespace, name, query string) cache.ResolvedKeyInputs {
	return cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassApistage,
		Group:           gvr.Group,
		Version:         gvr.Version,
		Resource:        gvr.Resource,
		Namespace:       namespace,
		Name:            name,
		Query:           query,
	}
}

// gateContentEnvelope applies the serve-time per-user RBAC gate to a raw
// apiserver envelope retrieved from (or about to be stored in) the
// identity-free content layer — the Ship F1 single gate site. It runs on
// BOTH the content-Get-hit path AND the un-gated-dispatch-miss path,
// before the envelope reaches the stage's jsonHandler/filter, so the
// hit-path leak is closed (pre-F1 the inline gate fired only on a miss).
//
// `call` identifies the K8s call: a LIST when ParseAPIServerPathToDep
// yields name=="", a GET-by-name otherwise. The gate is the exact same
// filterListByRBAC / filterGetByRBAC the inline dispatch used pre-F1 —
// pure functions of (raw items, request identity, live RBAC store) — so
// gating the stored raw envelope yields byte-identical narrowing to a
// fresh inline-gated dispatch (AC-F1.2).
//
// Returns (gatedEnvelope, served):
//   - served==false — fail-closed: no identity on ctx, or a GET denied.
//     The caller treats this exactly as dispatchViaInformer's
//     served=false — fall through to the apiserver branch, whose
//     per-user token narrows correctly.
//   - served==true  — gatedEnvelope is the RBAC-narrowed bytes to feed
//     the stage's ResponseHandler.
func gateContentEnvelope(
	ctx context.Context,
	call interface {
		GetPath() string
		GetVerb() string
	},
	raw []byte,
	uafActive bool,
) (any, bool) {
	gvr, _, name, parseOK := cache.ParseAPIServerPathToDep(call.GetPath())
	if !parseOK {
		// Not an apiserver GVR path — the content layer never produced
		// this; pass the bytes through un-gated (defensive — the caller
		// only invokes the gate for content-served calls). Decode to a
		// value so the return type matches the gated paths (Ship 0.30.128
		// P-CORE-2).
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, false
		}
		return v, true
	}
	// SECURITY (#58, UAF-AWARE — symmetric with the pre-parsed-LIST branch):
	// a UAF step is served UN-narrowed here; the per-user narrowing is the
	// UAF refilter DOWNSTREAM (which diverges from raw filterGet/ListByRBAC).
	// Applying raw RBAC to a UAF step would strip the UAF-intended scope.
	// So for a UAF step (LIST or GET-by-name) decode + pass through; only a
	// NON-UAF step takes the raw-RBAC gate (which is the over-serve fix for
	// LIST + the unchanged GET narrowing for no-UAF GET).
	if uafActive {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, false
		}
		return v, true
	}
	if name == "" {
		// LIST envelope: {apiVersion, kind, items:[...]}.
		return gateListEnvelope(ctx, gvr, raw)
	}
	// GET-by-name: raw is a single object.
	return gateGetEnvelope(ctx, gvr, raw)
}

// parsedListEnvelope is the Ship 0.30.121 R3 pre-parsed form of a LIST
// content envelope: the items decoded ONCE into []*unstructured plus the
// envelope's apiVersion/kind. parseListEnvelope produces it at the
// content-entry Put site; gateListItems consumes it on every Get-hit so
// the per-hit json.Unmarshal (the ~1.73 GiB double-unmarshal) is gone.
type parsedListEnvelope struct {
	items      []*unstructured.Unstructured
	apiVersion string
	kind       string
}

// parseListEnvelopeHookForTest observes every envelope parse. TEST-ONLY (nil
// in production, one atomic load per parse): the #882 herd arm counts parses
// through a seam it owns, never through snowplow's own fill metrics.
var parseListEnvelopeHookForTest atomic.Pointer[func(schema.GroupVersionResource)]

// parseListEnvelope decodes a raw LIST envelope ONCE — Ship 0.30.121 R3.
// Called at the content-entry Put site so the parsed items can be stored
// on the ResolvedEntry; the content-gate then runs filterListByRBAC over
// the stored items and skips re-unmarshalling on every hit. Returns ok=false
// on a malformed envelope (the caller stores RawJSON only — the gate then
// falls back to the unmarshal path, byte-identical to pre-0.30.121).
func parseListEnvelope(gvr schema.GroupVersionResource, raw []byte) (parsedListEnvelope, bool) {
	if h := parseListEnvelopeHookForTest.Load(); h != nil {
		(*h)(gvr) // TEST-ONLY: the #882 herd arm's own parse seam
	}
	var envelope struct {
		APIVersion string           `json:"apiVersion"`
		Kind       string           `json:"kind"`
		Items      []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return parsedListEnvelope{}, false
	}
	items := make([]*unstructured.Unstructured, 0, len(envelope.Items))
	for _, it := range envelope.Items {
		// Ship 2a (0.30.209) — strip managedFields ONCE here, at the
		// item-materialisation site where `it` is still PRIVATE (freshly
		// json.Unmarshal'd, not yet aliased by any serve). After this the
		// shared entry.Items carry no managedFields, so the serve path
		// needs no per-serve removeManagedFields walk (dropped in
		// resolve.go) — that walk wrote the SHARED item maps once the
		// envelope went shallow, racing concurrent reads.
		stripManagedFields(it)
		items = append(items, &unstructured.Unstructured{Object: it})
	}
	apiVersion := envelope.APIVersion
	if apiVersion == "" {
		apiVersion = apiVersionForGVR(gvr)
	}
	kind := envelope.Kind
	if kind == "" {
		kind = listKindForResource(gvr.Resource)
	}
	return parsedListEnvelope{items: items, apiVersion: apiVersion, kind: kind}, true
}

// stripManagedFields removes metadata.managedFields from a single item
// map IN PLACE. It MUST be called only at item-materialisation sites
// (parseListEnvelope, validateClusterListShape, gateGetEnvelope,
// gateListEnvelope), where the item map is freshly json.Unmarshal'd and
// still private — never on a map already aliased by a serve. After this,
// the shared entry.Items maps carry no managedFields, which is the
// invariant the Ship 2a shallow envelope relies on to drop the per-serve
// removeManagedFields walk (resolve.go).
//
// managedFields is a wire field on every apiserver object's metadata; it
// is large, server-managed, and never consumed by widgets/RESTActions, so
// snowplow has always stripped it before serving. Pre-Ship-2a the strip
// happened per-serve on a private deep copy (removeManagedFields(dict));
// Ship 2a moves it to load-time so the shared items are stripped once.
// decodeListItem decodes ONE list item exactly as parseListEnvelope decodes an
// element of the envelope: encoding/json into map[string]any (every number a
// float64), then stripManagedFields. #854: the incremental list cell decodes a
// changed object with this function, so its items are the full path's items.
func decodeListItem(raw []byte) (*unstructured.Unstructured, error) {
	var it map[string]any
	if err := json.Unmarshal(raw, &it); err != nil {
		return nil, err
	}
	stripManagedFields(it)
	return &unstructured.Unstructured{Object: it}, nil
}

// listCellFuncs hands the incremental list cell the full fill's own functions
// for this stage: the per-item decoder, the query predicate and the envelope
// writer over the dispatch's apiVersion and list kind (#854).
func listCellFuncs(gvr schema.GroupVersionResource, q listQuery) cache.ListCellFuncs {
	apiVersion := gvr.Version
	if gvr.Group != "" {
		apiVersion = gvr.Group + "/" + gvr.Version
	}
	listKind := listKindForResource(gvr.Resource)
	return cache.ListCellFuncs{
		Decode: decodeListItem,
		Match:  q.matchRaw,
		Marshal: func(raws [][]byte) ([]byte, error) {
			return marshalAsListRaw(apiVersion, listKind, raws)
		},
	}
}

func stripManagedFields(obj map[string]any) {
	if obj == nil {
		return
	}
	md, ok := obj["metadata"].(map[string]any)
	if !ok {
		return
	}
	delete(md, "managedFields")
}

// gateListItems runs filterListByRBAC over an ALREADY-PARSED item slice
// and assembles the narrowed set into the apiserver-shaped envelope as a
// DECODED structured value (map[string]any) — Ship 0.30.128 P-CORE-2.
//
// Pre-0.30.128 this re-marshalled the narrowed set to []byte
// (marshalAsList) so the stage jsonHandler could io.ReadAll +
// json.Unmarshal it straight back — a redundant marshal+unmarshal on
// EVERY content-cache hit. It now returns the envelope value directly;
// jsonHandlerValue consumes it with no marshal and no unmarshal. The
// envelope map is built exactly as marshalAsList builds it
// (apiVersion/kind/items), so the eventual served body is byte-identical
// (AC-128.4) — only the decode timing moves.
func gateListItems(ctx context.Context, gvr schema.GroupVersionResource, parsed parsedListEnvelope) (any, bool) {
	kept, identityOK := filterListByRBAC(ctx, gvr, parsed.items)
	if !identityOK {
		// FAIL-CLOSED: no identity — same contract as the inline gate's
		// served=false (caller falls through to the apiserver).
		return nil, false
	}
	return listEnvelopeValue(parsed.apiVersion, parsed.kind, kept), true
}

// listEnvelopeValue assembles the apiserver-shaped LIST envelope as a
// decoded map[string]any — the structured equivalent of marshalAsList's
// input, returned WITHOUT the json.Marshal. Field set + ordering of the
// map are irrelevant to the served body: the stage filter / jsonHandler
// operate on the decoded value and re-encode canonically downstream.
//
// Ship 2a (0.30.209) — SHALLOW envelope. Background: the `items` here is
// the cached content entry's R3 pre-parsed Items, whose it.Object
// map[string]any are OWNED by entry.Items and SHARED across every
// content-Get-hit. From 0.30.130 to 0.30.208 this site deep-copied the
// assembled envelope (maps.DeepCopyJSON, then CopyJSONMap) so each serve
// got a PRIVATE tree gojq could mutate freely — the per-call isolation
// the pre-0.30.128 marshal+unmarshal round-trip provided. That deep copy
// was 45.9% of serve-path allocations / ~20% GC at scale (155 MB/serve),
// and existed ONLY to absorb gojq's in-place writers of the input:
// normalizeNumbers (gone — upstream removed it in v0.12.18; the fork is
// v0.12.19) and deleteEmpty (delpaths/del), whose copy-on-write left
// ALIASED sibling sub-trees and recursed-and-wrote them in place.
//
// Ship 2a removes that last writer: the gojq fork's deleteEmpty is now
// allocator-aware (gojq/func.go) — it CoW-copies any non-gojq-allocated
// node instead of writing it, exactly like the update* family. With NO
// gojq path able to write the input, this site hands back a SHALLOW
// envelope: a fresh outer map + a fresh []any whose elements ALIAS the
// shared it.Object. The served tree is read-only and per-request (it
// flows to the HTTP response; it is never Put into a shared cache), so
// aliasing the shared items is safe. The -race destructive-serve
// falsifier is the hard gate (ship2a CoW falsifier +
// apistage_concurrent_isolation_test). P-CORE-1 (decode-once-at-entry-
// load) is untouched.
func listEnvelopeValue(apiVersion, listKind string, items []*unstructured.Unstructured) map[string]any {
	itemList := make([]any, 0, len(items))
	for _, it := range items {
		if it != nil {
			// SHALLOW: alias the shared it.Object (read-only). gojq cannot
			// mutate it — its only input-writer (deleteEmpty) is now
			// allocator-aware (CoW for non-allocated nodes).
			itemList = append(itemList, it.Object)
		}
	}
	return listenvelope.New(apiVersion, listKind, itemList) // E6 (#674): envelope_e6_test.go
}

// gateListEnvelope unmarshals a LIST envelope, runs filterListByRBAC over
// its items with the request identity, and re-marshals the narrowed set
// into the SAME apiserver-shaped envelope (marshalAsList) so the stage
// jsonHandler/filter sees an identical shape to a fresh dispatch.
//
// Ship 0.30.121 R3 — this remains the fallback unmarshal path, used when
// the content entry carries no pre-parsed Items (a malformed-at-Put
// envelope, or a refresh path that stored RawJSON only). The hot path
// (a content-Get-hit on an entry with Items) goes through gateListItems
// and never reaches here. Output is byte-identical between the two.
func gateListEnvelope(ctx context.Context, gvr schema.GroupVersionResource, raw []byte) (any, bool) {
	parsed, ok := parseListEnvelope(gvr, raw)
	if !ok {
		// A malformed stored envelope cannot be vouched for — fail closed.
		return nil, false
	}
	return gateListItems(ctx, gvr, parsed)
}

// gateGetEnvelope runs filterGetByRBAC on a single-object envelope.
// A denied / no-identity object is fail-closed (served=false) — the
// caller falls through to the apiserver, whose per-user token 403s.
func gateGetEnvelope(ctx context.Context, gvr schema.GroupVersionResource, raw []byte) (any, bool) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false
	}
	// Ship 2a (0.30.209) — the GET-by-name path does NOT flow through the
	// LIST item-strip; strip managedFields here too so dropping the
	// per-serve removeManagedFields walk (resolve.go) does not change the
	// served wire shape for a managedFields-bearing GET. `obj` is freshly
	// json.Unmarshal'd and private at this point.
	stripManagedFields(obj)
	u := &unstructured.Unstructured{Object: obj}
	if !filterGetByRBAC(ctx, gvr, u) {
		return nil, false
	}
	// Ship D.4.2 (0.30.149) — apistage GET-by-name partial-shape
	// guard. EMPIRICALLY GROUNDED at the 0.30.148 burst evidence
	// (site=13 success-path exit): 10/250 served objects for
	// `/v1, Resource=configmaps` had `obj_apiVersion_present=false`
	// AND `obj_apiVersion_is_empty_string=false` AND
	// `obj_apiVersion_type=<nil>`. The map literally LACKS the key
	// (not present-but-empty, not present-but-null).
	//
	// Defect flow (TRACED at design §1.5):
	//   apiserver elides per-item TypeMeta on core-group LIST
	//   responses (k8s wire convention)
	//   → streaming_list.go:507 captures item bytes verbatim
	//   → bytesObject's b.raw lacks apiVersion
	//   → bytesObject.Decode produces map[string]any without
	//     apiVersion key
	//   → dispatchViaInformer's json.Marshal(obj.Object) emits bytes
	//     without apiVersion
	//   → apistage Put stores them
	//   → apistage Get + gateGetEnvelope decodes back
	//   → obj["apiVersion"] is Go nil (untyped nil from absent
	//     map key).
	//
	// PREDICATE: Go nil-check (`obj["apiVersion"] == nil`), NOT D.4's
	// `obj["apiVersion"].(string) == ""`. The narrower predicate fires
	// ONLY on the empirically-observed defect class (present=false).
	// D.4's predicate would have ALSO fired on a present-but-empty-
	// string case that DOES NOT empirically exist (0/250 site=13
	// fires had is_empty_string=true). `feedback_empirical_apiserver
	// _probe_for_predicate_design` requires the predicate's blast
	// radius bounded by empirically observed cases.
	//
	// Truth table:
	//   - key absent → obj["apiVersion"]==any(nil) → predicate fires
	//     ✓ (the empirical defect class).
	//   - key present, JSON null → obj["apiVersion"]==nil → predicate
	//     fires ✓ (defensive; hypothetical, not empirically observed
	//     but caught by the same code).
	//   - key present, string "v1" → obj["apiVersion"]=="v1" → does
	//     NOT fire ✓.
	//   - key present, empty string "" → obj["apiVersion"]=="" →
	//     does NOT fire ✓ (D.4 false-positive class; empirically
	//     never observed; D.4.2 explicitly avoids).
	//
	// Returns (nil, false) — same shape as the filterGetByRBAC deny
	// branch above. apistageContentServe sees served=false; resolve.go
	// falls through to dispatchViaInternalRESTConfig / httpcall.Do; the
	// fresh apiserver GET-by-name response (verified §1.5: kubectl
	// get --raw returns apiVersion + kind populated) feeds the
	// downstream filter with a fully-populated map. ZERO new caller-
	// side code path.
	//
	// CACHE ENTRY PRESERVED: no eviction per
	// feedback_l1_invalidation_delete_only — the partial-shape entry
	// stays in the apistage cache; the next informer UPDATE event
	// dirty-marks the entry; the refresher re-Puts the entry, which
	// will pick up the apiserver's now-fresh GET-by-name shape; the
	// guard ceases firing for that (gvr, ns, name) tuple.
	if obj["apiVersion"] == nil || obj["kind"] == nil {
		cache.RecordApiserverFallthrough(ctx, cache.ReasonApistageGetPartialShape, gvr.String())
		return nil, false
	}
	// Ship 0.30.128 P-CORE-2: return the decoded object value, not the
	// raw bytes — jsonHandlerValue consumes it with no re-unmarshal.
	return obj, true
}

// apiVersionForGVR renders a GVR's apiVersion: "v1" for the core group,
// "group/version" otherwise — the apiserver convention marshalAsList
// expects.
func apiVersionForGVR(gvr schema.GroupVersionResource) string {
	return listenvelope.APIVersionFor(gvr.Group, gvr.Version) // shared with internal/cache (#674)
}

// callPathVerb adapts an httpcall.RequestOptions to the narrow interface
// gateContentEnvelope consumes — keeps the gate helper free of the
// plumbing httpcall import.
type callPathVerb struct {
	path string
	verb string
}

func (c callPathVerb) GetPath() string { return c.path }
func (c callPathVerb) GetVerb() string {
	if c.verb == "" {
		return http.MethodGet
	}
	return c.verb
}

// apistageContentServe is the Ship F1 content-keyed serve pipeline for
// one K8s call. It is invoked from resolve.go's g.Go worker when the
// resolver pivot is on AND the content-keyed api-stage L1 is enabled.
//
// Returns (value, served, ok):
//   - ok==false  — the content layer cannot serve this call (it is not
//     pivot-servable: a write verb, an external URL, a metadata-only
//     GVR, a pre-sync informer, a non-apiserver path). The caller falls
//     through to the non-pivot dispatch branches, byte-identical to a
//     flag-off resolve.
//   - ok==true, served==false — fail-closed: no identity on ctx, or a
//     GET the requester is denied. The caller falls through to the
//     apiserver branch, whose per-user token narrows correctly.
//   - ok==true, served==true — `value` is the RBAC-GATED envelope as an
//     already-DECODED structured value (Ship 0.30.128 P-CORE-2). The
//     caller feeds it to jsonHandlerValue — no marshal, no unmarshal.
//
// Pipeline (per the F1 spec):
//  1. content key from call.Path via ParseAPIServerPathToDep.
//  2. content Get — HIT: stored entry (R3 pre-parsed Items reused).
//     MISS: dispatch UN-GATED (WithApistageContentResolve) + Put.
//  3. gate with the request identity, returning the DECODED envelope
//     value (the single F1 gate site — runs on hit AND miss).
//
// On the prewarm path (cache.ApistagePrewarmFromContext, wired by F2)
// step 3 is skipped — the prewarm resolve has no requester; it only
// populates the content entry. F1 leaves the skip-point; F2 sets the
// marker.
// uafActive (#58): does the OWNING stage carry a userAccessFilter? It selects
// how a pre-parsed LIST content cell is gated at serve time (see the
// gate-selection comment at the serve branch below). A NON-UAF LIST is
// narrowed by the requester's RAW RBAC (filterListByRBAC via gateListItems);
// a UAF LIST is served UN-narrowed here and narrowed by the UAF refilter
// downstream — the two narrowings DIVERGE for real UAF steps (verb/resource/
// namespace-source differ), so applying raw RBAC to a UAF step would wrongly
// strip the UAF-intended scope (the Diego/arch UAF verdict).
//
// #710 PR-2a — err is non-nil only when the call's content fill could not be
// admitted under the memory ceiling before its deadline
// (ErrContentFillAdmissionTimeout, D1): the caller records a stage error. It
// must NOT fall through to another dispatch branch, which would rebuild the
// whole list the gate refused.
//
// A JOINER of a flight that was never admitted, whose own ctx is still alive,
// does not inherit that failure — someone else's deadline produced it. It makes
// one more attempt: a hit if the pressure cleared and a cell landed, else a
// flight of its own (or a fresh one it joins), under its own budget.
func apistageContentServe(
	ctx context.Context,
	store *cache.ResolvedCacheStore,
	call httpcall.RequestOptions,
	uafActive bool,
) (value any, served bool, ok bool, err error) {
	value, served, ok, err = apistageContentServeAttempt(ctx, store, call, uafActive, true)
	if errors.Is(err, errInheritedAdmissionTimeout) || errors.Is(err, errJoinedMovedFlight) {
		value, served, ok, err = apistageContentServeAttempt(ctx, store, call, uafActive, false)
	}
	return value, served, ok, err
}

// errInheritedAdmissionTimeout is a joiner's signal to retry (never returned
// from apistageContentServe).
var errInheritedAdmissionTimeout = errors.New("apistage content fill: joined flight was not admitted")

// errJoinedMovedFlight (#882 F2) is the signal of a joiner whose list moved
// after the flight began and whose flight ended without an incremental cell to
// bring up to its own seq: the flight's body may predate an edit it must see,
// so it makes one more attempt under the epoch rule (never returned from
// apistageContentServe).
var errJoinedMovedFlight = errors.New("apistage content fill: joined a moved list and the flight anchored no cell")

// apistageContentServeAttempt is one attempt of apistageContentServe.
// mayRetry lets a joiner of a never-admitted flight ask for a second attempt.
func apistageContentServeAttempt(
	ctx context.Context,
	store *cache.ResolvedCacheStore,
	call httpcall.RequestOptions,
	uafActive bool,
	mayRetry bool,
) (value any, served bool, ok bool, err error) {
	log := xcontext.Logger(ctx)

	// Ship 0.30.193 Checkpoint 3 — per-call apistageContentServe
	// instrumentation. The sink is installed ONLY on the PIP seed paths
	// (seedOneRestaction, seedOneWidget); production /call requests
	// have no sink → PIPStageTimingSinkFrom returns nil → every
	// Accumulate* below is a nil-receiver no-op (zero overhead beyond
	// one ctx.Value lookup + one nil-check per call).
	//
	// hitObserved is set by the hit branch below; on every return we
	// record (hitObserved, time.Since(t0)). For non-pivot-servable
	// short-circuit returns (write verbs, malformed paths) hitObserved
	// stays false; the sample is recorded as a "miss" (no
	// dispatch/Put — sub-µs cost). That's fine: the sentinel cohort's
	// allCompositions stage is pivot-servable on the cluster-list
	// collapse path; the early-return branches are zero-cost noise.
	pipSink := cache.PIPStageTimingSinkFrom(ctx)
	t0 := time.Now()
	var hitObserved bool
	defer func() {
		pipSink.AccumulateContentServe(hitObserved, time.Since(t0).Milliseconds())
	}()

	// Content-keyed entries describe apiserver GET/LIST calls only — the
	// same shape dispatchViaInformer can serve. A write verb or a
	// non-apiserver path is not a content unit.
	if ptr.Deref(call.Verb, http.MethodGet) != http.MethodGet {
		return nil, false, false, nil
	}
	gvr, ns, name, parseOK := cache.ParseAPIServerPathToDep(call.Path)
	if !parseOK {
		return nil, false, false, nil
	}
	isList := name == ""
	// #618 — classify the query BEFORE any cell is read or written. A query
	// the informer cannot reproduce exactly is not a content unit: return
	// ok=false so the caller dispatches to the apiserver with the query
	// intact. Reading a cell first would serve this call the unselected (or a
	// differently-selected) list, which is the defect.
	query := planListQuery(call.Path, isList, ns != "")
	if !query.applicable {
		recordListQuery(query)
		cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerListQuery, gvr.String())
		return nil, false, false, nil
	}
	contentKey := cache.ComputeKey(contentKeyInputsQ(gvr, ns, name, query.canon))

	// Step 2 — content Get / un-gated dispatch + Put.
	//
	// Ship 0.30.121 R3 — for a LIST content entry the items are parsed
	// ONCE here (at the Put site, or carried on the hit entry) so the
	// per-hit gate skips json.Unmarshal. `parsed`/`haveParsed` carry the
	// pre-parsed form when available; `envelope` carries the raw bytes
	// for the GET path and the unmarshal fallback.
	// Ship GMC / 0.30.174 — `entryRef` carries the *cache.ResolvedEntry
	// pointer (HIT: from store.Get; MISS: the just-Put entry) so the
	// per-cohort gate memo (CohortGates) can attach to the entry's own
	// lifetime.
	var envelope []byte
	var parsed parsedListEnvelope
	var haveParsed bool
	var entryRef *cache.ResolvedEntry
	// R1 Layer 1 — refresh-context content-bypass (force a MISS on a content
	// HIT whose OWN dep GVR is the GVR that TRIGGERED this refresher
	// re-resolve). Without it, a whole-RA refresh re-resolve serves this
	// stage from its STORED content bytes (the HIT branch below short-
	// circuits the fresh fetch — the content-shield, R1 §3), so the
	// re-resolve consumes a stale sibling snapshot and re-stores a degraded
	// result. forceContentMiss is true ONLY when:
	//   - ctx carries a refresh trigger GVR (WithRefreshTriggerGVR, set ONLY
	//     by the refresher's re-resolve entry point — NEVER a request-path
	//     /call, so the HIT branch is byte-identical for real traffic), AND
	//   - that trigger GVR == this content unit's own GVR (dep-edge
	//     equality — UNIFORM, no per-resource/path special-case,
	//     feedback_no_special_cases).
	// On a forced miss we fall through to the MISS branch, which re-dispatches
	// fresh + re-Puts, so the whole-RA re-resolve reads the FRESH input.
	// #376 — refresherDriven distinguishes a refresher whole-RA re-resolve (which set
	// WithRefreshTriggerGVR, NEVER a request-path /call) from a real customer serve.
	// forceContentMiss is the R1 content-shield (this unit's GVR is a trigger →
	// re-dispatch fresh). #375 B — membership in the refresher's trigger SET (every
	// GVR that marked or remarked the key before this dequeue), not equality with a
	// single last-write GVR.
	_, refresherDriven := cache.RefreshTriggerGVRFromContext(ctx)
	// #710 PR-2b — a debt's refill always re-dispatches (it exists because the
	// cell is known-stale).
	forceContentMiss := contentDebtRefillFromContext(ctx)
	// #765 option A — the content stamp. A refresher re-resolve whose trigger set
	// holds this cell's GVR no longer discards the cell unconditionally: it re-fills
	// only when the cell's read may be OLDER than a trigger it consumed
	// (cache.ContentStampCovers against cache.RefreshTriggerAsOf), and otherwise
	// reuses it. N dependents refreshed after one change share one re-fill. The R1
	// shield's guarantee — a refreshed cell is never built from a snapshot older
	// than the change that triggered it — is exactly the stamp check.
	var stamp contentStampGate
	if !forceContentMiss && refresherDriven && cache.RefreshTriggerHas(ctx, gvr) {
		stamp = contentStampGate{on: true, need: cache.RefreshTriggerAsOf(ctx), viewed: cache.ViewedRefreshFromContext(ctx)}
	}
	// #376 — the content cell is stamped ONLY by a true customer serve. On
	// forceContentMiss the entry is discarded + re-dispatched fresh anyway → skip the
	// read entirely (no stamp, no wasted Get). Any OTHER refresher re-resolve read (a
	// sibling stage whose GVR is not a trigger) is served-as-input but INTERNAL →
	// GetNoTouch. A real /call is the serve → Get (the one legitimate #315/#316
	// warmth stamp).
	//
	// #552 — TWO DIFFERENT QUESTIONS, TWO DIFFERENT PREDICATES. The paragraph
	// above is the intent; until #552 the code asked only one question. Keep them
	// apart:
	//
	//   - forceContentMiss is TRIGGER EQUALITY: does this cell's own GVR appear
	//     in the refresher's trigger set (R1 §3 content-shield / #375 B). It must
	//     stay on refresherDriven — keying it on background-ness would force-miss
	//     every background read, which is #544.
	//   - the READ PRIMITIVE is BACKGROUND-NESS: is a customer being served. A
	//     trigger GVR is not that question. A refresher dequeue with no trigger
	//     (ra_full_list_slice.go sliceability re-verify — the dominant source at
	//     ≥397,043 accepted submits — resolved.go's proactive pass, deps.go's
	//     no-hook fallback), the cohort seed and the engine boot re-drive are all
	//     background and all used to fall through to store.Get, which stamps
	//     lastRead, moves the LRU front, bumps hit_total AND trips
	//     noteServeWhileDirty → stale_served_total. That is a background read
	//     accounted as a customer serve, on the four counters that gate #354.
	//
	// cache.BackgroundResolveFromContext is the right predicate because the marker
	// means exactly this and nothing else: "A customer /call carries NO such
	// marker" (cache/deps.go), it is the meaning rbac.MustRegateSADial already
	// depends on under the #268/#269 drift-guard with a producer census behind it,
	// and it is already classified readOnly443 in the #443 inertness census.
	//
	// #547 (owner ruling 3) — a KNOWN-STALE content cell (a refresh of it ended
	// without an accepted re-Put) is READ THROUGH for EVERY reader, customer or
	// background: it is a resolve INPUT, and serving it would bake a stale
	// substrate into a fresh-looking widget/RA cell. The MISS fall-through below
	// is the one K8s call per read the owner accepted while the informer is not
	// servable; the informer's re-fire refreshes the cell.
	// #710 — which case sent this reader into the fill (apistage_fill_metrics.go).
	fillReason := contentFillAbsent
	// #854 — an incremental list cell (cache.ListCell): the generation this
	// reader is served, taken through Snapshot(need) so its stamp and its data
	// are one value. need is the refresh's trigger seq for a refresher, the
	// current dep-event seq for every other reader (#854 C5: a customer is never
	// served a generation older than what a fill now would read). A cell that
	// unanchors on this read is not served: the reader takes the full fill,
	// which re-anchors it.
	var inc cache.ListView
	var incOK bool
	acceptInc := func(e *cache.ResolvedEntry, need uint64) bool {
		lc := e.ListCell()
		if lc == nil {
			return true
		}
		v, ok := lc.Snapshot(need)
		if !ok {
			fillReason = contentFillReanchor
			return false
		}
		inc, incOK = v, true
		return true
	}
	readContent := func(k string) (*cache.ResolvedEntry, bool) {
		// #710 PR-2b (D2, owner ruling "show then replace") — an ARMABLE customer
		// read of a KNOWN-STALE cell is served the cell at once and opens (or
		// joins) its debt: an immediate refill at top priority, then a push to
		// this customer's cell (apistage_stale_debt.go). Decided, read and
		// registered in one critical section. Any other reader falls through to
		// #547's read-through below.
		if sink, ok := staleServeEligible(ctx); ok && !forceContentMiss && !stamp.on {
			if e, served := serveStaleWithDebt(ctx, sink, store, k, call, gvr.String(), cache.DepKey{GVR: gvr, Namespace: ns, Name: name}); served {
				return e, true
			}
		}
		switch {
		case forceContentMiss:
			fillReason = contentFillRefreshForced
			return nil, false
		case cache.KnownStaleReadThrough(k):
			fillReason = contentFillKnownStale
			return nil, false
		case stamp.on:
			// #765 — a refresher read is never a customer serve: no warmth stamp
			// (#376/#552). Through cache.ServeCellIf (the #643 served-cell
			// path): the cell is served — and noted — only if its content stamp
			// covers the refresh's triggers.
			e, hit, served := cache.ServeCellIf(ctx, store, k, func(e *cache.ResolvedEntry) bool {
				if e.ListCell() != nil {
					// #854 — the generation's own stamp: Snapshot(stamp.need)
					// returns a generation complete as of at least stamp.need (it
					// drains pending when the published one is older), so the
					// stamp covers the refresh's triggers by construction. Not
					// ContentStampCovers: its asOf==0 "unknown" rule is about a
					// fill's read start, and a generation built before any dep
					// event is legitimately complete as of seq 0.
					return acceptInc(e, stamp.need) && inc.AsOf() >= stamp.need
				}
				return cache.ContentStampCovers(gvr, ns, name, e.ContentAsOf, stamp.need)
			}, cache.DepKey{GVR: gvr, Namespace: ns, Name: name})
			if !hit {
				return nil, false
			}
			if !served {
				if fillReason != contentFillReanchor {
					fillReason = contentFillRefreshForced
				}
				return nil, false
			}
			noteContentRefreshReused(gvr.String())
			return e, true
		case cache.BackgroundResolveFromContext(ctx) || cache.Inert(ctx):
			// #443 (e) — an inert (dry-run) resolve reads the cell as input
			// but must not stamp it warm either.
			// #643 — through ServeCell: the served cell (and the dep it stands
			// for) is noted on the resolve's sink. #854 — ServeCellAccept, so an
			// incremental cell that unanchors on this read is not noted.
			e, hit, served := cache.ServeCellAccept(ctx, store, k, false, func(e *cache.ResolvedEntry) bool {
				return acceptInc(e, cache.DepGenEpochNow())
			}, cache.DepKey{GVR: gvr, Namespace: ns, Name: name})
			return e, hit && served
		default:
			e, hit, served := cache.ServeCellAccept(ctx, store, k, true, func(e *cache.ResolvedEntry) bool {
				return acceptInc(e, cache.DepGenEpochNow())
			}, cache.DepKey{GVR: gvr, Namespace: ns, Name: name})
			return e, hit && served
		}
	}
	// #882 F2 — a fill of this call would anchor an incremental cell: a LIST,
	// not a dry run, anchoring available on a servable GVR.
	anchorable := isList && !cache.Inert(ctx) && cache.ListCellAnchorable(gvr)
	if entry, hit := readContent(contentKey); hit && entry != nil {
		hitObserved = true // Ship 0.30.193 C3 — observed cache hit.
		// #618 — a HIT serves this call's selector from its own cell; the MISS
		// branch counts through dispatchViaInformer instead.
		recordListQuery(query)
		envelope = entry.RawJSON
		entryRef = entry
		if lc := entry.ListCell(); lc != nil && !incOK {
			// #854 — the #710 stale-serve path read the entry without a
			// Snapshot: take one now (a cell that unanchored meanwhile serves
			// its last generation, as a stale serve serves its last fill).
			if v, ok := lc.Snapshot(cache.DepGenEpochNow()); ok {
				inc, incOK = v, true
			}
		}
		// R3: a LIST entry stored with pre-parsed Items — gate directly
		// over them, no re-unmarshal. An entry without Items (legacy /
		// refresh-stored / malformed-at-Put) falls back to the RawJSON
		// unmarshal path below.
		// #578: materialise on FIRST SERVE. EnsureItems honours a Put-time
		// Items slice first (so an entry stored by an older binary, or by a
		// path that still fills it, behaves exactly as before), then a
		// published memo, and only parses when neither exists. The parse
		// therefore happens at most once per refresh generation and ONLY for
		// cells somebody actually reads — where the pre-#578 Put-time parse
		// ran for every rebuild of every cell, read or not.
		if isList && incOK {
			// #854 — an incremental cell: this generation's shared items; the
			// envelope is built only for an empty list (the unparsed fallback
			// below needs bytes).
			if items := inc.Items(); len(items) > 0 {
				parsed = parsedListEnvelope{items: items, apiVersion: inc.APIVersion(), kind: inc.Kind()}
				haveParsed = true
			} else {
				envelope = inc.Raw()
			}
		} else if isList {
			if items, av, k, ok := entry.EnsureItems(func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
				p, pOK := parseListEnvelope(gvr, raw)
				return p.items, p.apiVersion, p.kind, pOK
			}); ok && len(items) > 0 {
				parsed = parsedListEnvelope{
					items:      items,
					apiVersion: av,
					kind:       k,
				}
				haveParsed = true
			}
		}
		// Ship 0.30.212 — idempotent re-record on HIT. Required to converge
		// after rollout for entries Put under an earlier binary (no dep
		// edge) OR via the cluster_list collapse path. sync.Map.LoadOrStore
		// semantics make this a no-op for already-present edges — sub-µs
		// hot-path cost per Deps.Record/RecordList doc.
		if isList {
			cache.Deps().RecordList(ctx, contentKey, gvr, ns)
			// Fix #1 (1a) — stale-delete heal/re-touch
			// (docs/rca-stale-delete-compositiondefinitions-informer-2026-06-25.md).
			// A LIST served from this content HIT short-circuits BEFORE
			// dispatchViaInformer, so once the entry was Put while the GVR
			// was not-servable nothing ever re-touches the informer to
			// confirm it — the GVR latches not-servable, its data informer
			// never delivers DELETEs, and the dependent resolved-output LIST
			// entry (e.g. /blueprints) stays stale to TTL. Re-touch the
			// informer here so a registered-but-unconfirmed GVR keeps being
			// nudged onto the discovery-confirm path:
			//
			//   - EnsureResourceType is singleflight-idempotent (sub-µs map
			//     check when already registered — watcher.go:625); it does
			//     NOT change what is served (still this content HIT). This
			//     keeps the GVR registered + on the 30s discovery-refresh
			//     confirm path (servable.go:208), which confirms every
			//     registered GVR — so even a content-only-served LIST is
			//     confirmed within one tick (≤30s) instead of latching
			//     forever. That is the heal.
			//
			// NO per-HIT ConfirmResourceType here (architect N1, gate):
			// ConfirmResourceType runs an UNCACHED ServerResourcesForGroupVersion
			// discovery call, and for a GVR latched not-servable (the bug
			// scenario) `!IsServable` stays true so EVERY content-HIT LIST
			// would issue one — a discovery-storm amplifier on the same GVR
			// class as regression #42 (feedback_bounding_mechanism_discipline:
			// cost MUST be cost-proportional, not per-request-worst-case). The
			// sub-tick prime lives SOLELY in 1b (confirm-on-register, one-shot
			// per registration); here we only keep the GVR registered + on the
			// ticker, which heals within ≤1 tick. The HIT path therefore stays
			// the O(1) EnsureResourceType map check.
			//
			// LIST-ONLY (feedback_bounding_mechanism_discipline + the 1.5.1
			// boot-OOM): this is inside `if isList` (name==""), so the
			// GET-by-name child-resource fan-out (the OOM path) is NEVER
			// re-touched — no child informer is ever forced. AFTER the cache
			// read.
			if rw := cache.Global(); rw != nil {
				rw.EnsureResourceTypeFor(ctx, gvr)
			}
		} else {
			cache.Deps().Record(ctx, contentKey, gvr, ns, name)
		}
		log.Debug("apistage.content_hit",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("ns", ns),
			slog.String("name", name),
			slog.Bool("preparsed", haveParsed),
			slog.String("key_hash", contentKey),
		)
	} else if flight, fillCtx, lead, movedJoin := enterContentFlight(ctx, contentKey, gvr, ns, name, fillReason, stamp,
		anchorable, anchorable && mayRetry); !lead {
		// MISS, JOINER (#710 PR-1, apistage_fill_flight.go) — an in-flight fill of
		// this key started after this reader's coordinate last moved, so its body is
		// as fresh as this reader's own fill would be. Serve the flight's envelope
		// and shared parsed items through this reader's own gate below, exactly as a
		// content HIT serves entry.Items.
		//
		// #882 F2 — or the flight anchors an incremental LIST cell and this reader
		// may snapshot-join (anchorable, on its first attempt): then it joined
		// whatever its list did, and is served the cell brought up to its own seq.
		res, ok := flight.wait(ctx)
		if res.err != nil {
			// #710 PR-2a (D1) — the flight was never admitted. Its leader's
			// deadline, not this reader's: while this reader's own request is
			// alive it tries once more (apistageContentServe); else it errors.
			if mayRetry && ctx.Err() == nil {
				return nil, false, false, errInheritedAdmissionTimeout
			}
			return nil, false, true, res.err
		}
		if !ok {
			// The flight was not servable (its leader fell through too), or this
			// reader's own ctx ended while it waited.
			return nil, false, false, nil
		}
		// #882 F2 — the flight's incremental cell, brought up to THIS reader's
		// own seq (its content sink's entry seq, taken before it joined): every
		// edit up to that seq is in the generation it is served, whatever the
		// flight's own read saw. Snapshot drains only the edits since the
		// flight's last generation.
		servedAsOf := flight.startSeq
		fromCell := false
		if res.cell != nil && isList {
			need, _ := cache.DepGenStartSeqFromContext(fillCtx)
			if v, vok := res.cell.Snapshot(need); vok {
				if items := v.Items(); len(items) > 0 {
					parsed = parsedListEnvelope{items: items, apiVersion: v.APIVersion(), kind: v.Kind()}
					haveParsed = true
				} else {
					envelope = v.Raw()
				}
				servedAsOf, fromCell = v.AsOf(), true
			}
		}
		if !fromCell {
			if movedJoin {
				// Joined a list that moved after the flight began, and there is no
				// cell to bring up to this reader's seq: the flight's body may miss
				// an edit this reader must see. One more attempt (epoch rule).
				if mayRetry && ctx.Err() == nil {
					return nil, false, false, errJoinedMovedFlight
				}
				return nil, false, false, nil
			}
			envelope, parsed, haveParsed = res.envelope, res.parsed, res.haveParsed
		}
		if res.stored {
			// The dep edge the leader Recorded on its accepted Put, on THIS reader's
			// resolve sink, so its own cell's Put-check sees the coordinate (#375).
			if isList {
				cache.Deps().RecordList(ctx, contentKey, gvr, ns)
			} else {
				cache.Deps().Record(ctx, contentKey, gvr, ns, name)
			}
			entryRef = res.entry
		}
		noteFillOutcome(ctx, res.stored, servedAsOf)
	} else {
		// MISS, LEADER — dispatch UN-GATED so the stored envelope is identity-free.
		// #710 PR-1 — every exit publishes the flight's outcome to its joiners (a
		// zero result = not servable); the success path publishes the result right
		// after the Put, before this reader's own gate.
		defer flight.complete(contentFlightResult{})
		// #710 PR-1 — the fill runs DETACHED from this reader's cancellation, bounded
		// by the server write deadline: a leader whose request goes away cannot fail
		// the readers that joined its flight.
		fillCtx, cancel := detachedFillContext(fillCtx)
		defer cancel()
		// #189 — capture the content key's generation BEFORE the dispatch/resolve
		// so the tail PutIfGen refuses (rather than resurrecting a pre-delete
		// body) if the underlying object is DELETE-evicted during the resolve.
		contentGen0 := store.CaptureGen(contentKey)
		// #375 (A) — the content cell's OWN resolve entry: fillCtx carries the CHILD
		// dep-gen sink enterContentFlight installed BEFORE the data read below,
		// pre-declaring the cell's own coordinate (its dep is Recorded only AFTER the
		// accepted Put, #189). The tail PutIfGen then remarks the content key iff its
		// own coordinate churned in [read, Put]; Records made under the child still
		// propagate to the outer resolve's sink.
		// Block-scoped shadow: the child sink covers exactly this MISS branch's read → Put
		// → Record; the gate below keeps the caller's ctx.
		ctx := fillCtx
		// #710 — count the fill and hold its envelope bytes in flight until
		// this serve returns; the dispatch and the parse run under pprof
		// labels so a CPU profile names the GVR, reason and RESTAction.
		fill := beginContentFill(ctx, contentKey, gvr.String(), ns, isList, fillReason)
		defer fill.end()
		fillLabels := fill.pprofLabels()
		// #710 PR-2a — memory admission for this flight (apistage_fill_admission.go),
		// BEFORE the dispatch builds the envelope: a queued flight holds a ticket,
		// never a parked envelope. It waits with a PREDICTED weight (the resident
		// cell's raw size, else the partition's item count × the GVR's bytes per
		// item, …), bounded by the detached fill ctx (the write deadline), while
		// the fills in flight would exceed the shared ceiling. Joiners pay nothing.
		gate := theContentFillGate()
		if aerr := gate.admit(ctx, flight.ticket, gvr.String(), predictFillRaw(store, contentKey, gvr, ns, isList)); aerr != nil {
			// D1 — never admitted before the deadline: this reader and every
			// joiner error. Nothing is dispatched, stored or served stale.
			flight.complete(contentFlightResult{err: aerr})
			return nil, false, true, aerr
		}
		admitted := true
		defer func() {
			if admitted {
				gate.release(flight.ticket, false) // an early exit: nothing decoded
			}
		}()
		// #882 F2 — a LIST fill's incremental cell starts watching its GVR BEFORE
		// the dispatch lists the informer, so every edit during the fill is
		// recorded in its pending (an edit the anchor then takes from the
		// listing, never an abort). nil when no cell can anchor here; abandoned
		// on every exit that does not anchor it.
		var listFill *cache.ListCellFill
		if isList && !cache.Inert(ctx) {
			listFill = cache.BeginListCell(contentKey, gvr, ns, listCellFuncs(gvr, query))
			defer listFill.Abandon()
		}
		var dispatched []byte
		var dispatchedOK bool
		pprof.Do(ctx, fillLabels, func(ctx context.Context) {
			dispatched, dispatchedOK = dispatchViaInformerFn(
				cache.WithApistageContentResolve(ctx), call)
		})
		if !dispatchedOK {
			// Not pivot-servable — the content layer cannot serve it.
			fill.notServable()
			return nil, false, false, nil
		}
		fill.dispatched(dispatched)
		// The exact size is known: the held weight becomes exact, in place.
		gate.reconcile(flight.ticket, int64(len(dispatched)))
		envelope = dispatched
		// #765 — the content stamp: this cell is fresh as of the flight's entry
		// seq, captured (WithContentDepGenSink) before the data read above. A
		// snapshot stored while the informer is not servable may be degraded
		// (R1 Layer 2): it stays unstamped (0), so a refresher always re-fills it.
		servable := cache.Global().IsServable(gvr)
		var contentAsOf uint64
		if servable {
			contentAsOf = flight.startSeq
		}
		// uaf-scope-waiver: PRE-refilter substrate, same reasoning as cluster_list.go — the raw per-K8s-call envelope, identity-free by key, narrowed only downstream on the way to the requester. Never holds refilter output.
		// scope-waiver:TTLOverride: apistage identity-free content substrate — sets its OWN data-plane CATALOG_UNSERVABLE TTLOverride CONDITIONALLY on newEntry below (not a keyed literal element); NOT the UAF-cap class (no BindingUID / no per-user refilter output) — uaf_shortttl.go R-d-4 SITE MAP.
		newEntry := &cache.ResolvedEntry{
			RawJSON:     dispatched,
			Inputs:      ptrTo(contentKeyInputsQ(gvr, ns, name, query.canon)),
			ContentAsOf: contentAsOf,
		}
		// R1 Layer 2 (#36): bounded-staleness backstop. If this content
		// entry is being stored while its GVR informer is NOT servable
		// (registered-but-not-synced / watch-broken / unconfirmed — e.g. the
		// core.krateo.io discovery flap that re-latches not-servable), it may
		// be a degraded / not-fully-resolved snapshot. Stamp the short
		// CATALOG_UNSERVABLE_TTL_SECONDS so it self-evicts within the bound
		// even if a dirty-mark / refresh-ordering gap is missed. Uniform
		// over every GVR (no path/resource special-case — feedback_no_special_cases);
		// disabled (TTLOverride stays 0 → standard TTL) when the env is unset.
		if ttl := cache.CatalogUnservableTTL(); ttl > 0 && !servable {
			newEntry.TTLOverride = ttl
		}
		var anchoredCell *cache.ListCell
		// R3: parse the LIST envelope ONCE and attach the items to the
		// entry, so every future content-Get-hit gates without a
		// re-unmarshal. A malformed envelope (parseOK=false) stores
		// RawJSON only — the gate then takes the unmarshal fallback.
		if isList {
			var p parsedListEnvelope
			var parseOK bool
			pprof.Do(ctx, fillLabels, func(context.Context) {
				p, parseOK = parseListEnvelope(gvr, dispatched)
			})
			if parseOK {
				fill.parsed(len(p.items))
				gate.noteItems(gvr.String(), int64(len(dispatched)), len(p.items))
				newEntry.Items = p.items
				newEntry.ItemsAPIVersion = p.apiVersion
				newEntry.ItemsKind = p.kind
				parsed = p
				haveParsed = true
				// #854 — anchor an incremental cell on this fill: from here on
				// every informer event on the GVR is applied to the stored list
				// instead of re-filling it. Only for a servable GVR (the cell
				// trusts its informer). #882 F2 — an edit during the fill is a
				// pending key the anchor takes from the informer, never an
				// abort; Anchor returns nil only when the envelope cannot be
				// proven to be this informer's, and the entry then stays a plain
				// fill. A dry run (cache.Inert) persists nothing: no listFill.
				if servable {
					if cell := listFill.Anchor(dispatched, p.items, p.apiVersion, p.kind); cell != nil {
						cell.AttachStore(store)
						anchoredCell = cell
						newEntry = cache.NewListCellEntry(cell, newEntry.Inputs)
					}
				}
			}
		}
		// #189 — generation-guarded write: refuse (do not resurrect a pre-delete
		// body) if a DELETE-eviction bumped the content key's generation during
		// the resolve. A cold fill (key absent, gen 0) still inserts. On refusal
		// the current request STILL serves `dispatched` below (envelope / parsed
		// are already set); only the CACHE fill + its dep-record are skipped.
		accepted := store.PutIfGen(ctx, contentKey, newEntry, contentGen0)
		if !accepted && anchoredCell != nil {
			anchoredCell.Close() // #854 — no entry was stored for it
		}
		if accepted {
			// Ship 0.30.212 — wire informer-event invalidation for this content
			// entry. Without a dep edge an informer ADD/UPDATE/DELETE on the
			// underlying objects can never dirty-mark this cell, leaving it
			// TTL-stale-forever (F-4 defect). Symmetric with dispatcher L1
			// dep-record at resolve.go:546-562 and widget_content.go:267
			// recordWidgetDeps (AC-G.5 pattern). `isList` was computed above
			// on the same `name == ""` predicate the content layer keys on.
			// Idempotent (sync.Map.LoadOrStore) and sub-µs (two atomic.Add).
			// #189 — dep-Record + entryRef only on an accepted write: a refused
			// PutIfGen stored no cell, so there is nothing to dep-track and no
			// entry for the cohort-gate memo to attach to.
			if isList {
				cache.Deps().RecordList(ctx, contentKey, gvr, ns)
			} else {
				cache.Deps().Record(ctx, contentKey, gvr, ns, name)
			}
			entryRef = newEntry
			log.Debug("apistage.content_store",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.Bool("preparsed", haveParsed),
				slog.String("key_hash", contentKey),
			)
		} else {
			log.Debug("apistage.content_store_refused_gen_moved",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("ns", ns),
				slog.String("name", name),
				slog.String("key_hash", contentKey),
				slog.String("effect", "#189 DELETE-eviction during resolve — not resurrecting; request still served from the fresh resolve, cache fill skipped"),
			)
		}
		// #710 — the fill's duration: dispatch start to parse + Put done.
		fill.done()
		// #710 PR-2a — the decoded tree is stored (live heap the ceiling reads) or
		// dropped: release the admission, calibrating this GVR's expansion.
		gate.release(flight.ticket, true)
		admitted = false
		// #710 PR-1 — hand the result to the joiners now: the envelope and the
		// parsed items are immutable from here on.
		noteFillOutcome(ctx, entryRef != nil, flight.startSeq)
		if entryRef != nil {
			// #740 R1 — ANY accepted fill at or after a debt's mark retires it.
			retireStaleDebtOnFill(contentKey, flight.startSeq)
		}
		var flightCell *cache.ListCell
		if entryRef != nil && anchoredCell != nil {
			flightCell = anchoredCell // #882 F2 — joiners bring it up to their own seq
		}
		flight.complete(contentFlightResult{
			ok:         true,
			envelope:   envelope,
			parsed:     parsed,
			haveParsed: haveParsed,
			stored:     entryRef != nil,
			entry:      entryRef,
			cell:       flightCell,
		})
	}

	// Step 3 — the per-user RBAC gate (skipped on the F2 prewarm path, and on a
	// #710 PR-2b debt refill: neither has a requester).
	if cache.ApistagePrewarmFromContext(ctx) || contentDebtRefillFromContext(ctx) {
		// Prewarm: no requester — populate the content entry only, do not
		// gate or feed dict[id]. served=false tells the caller not to use
		// the bytes (the prewarm resolve discards dict).
		return nil, false, true, nil
	}
	// R3: a LIST with pre-parsed items gates unmarshal-free; everything
	// else (GET-by-name, or a LIST whose envelope failed to pre-parse)
	// takes gateContentEnvelope's RawJSON path. Ship 0.30.128 P-CORE-2:
	// the gate returns the DECODED envelope value, so the caller feeds it
	// via jsonHandlerValue with no marshal + no unmarshal.
	//
	// SECURITY (#58 — cross-tenant LIST over-serve, UAF-AWARE fix). The
	// apistage content cell is populated SA-MAXIMAL / UN-GATED (the MISS
	// dispatch runs under WithApistageContentResolve, which SKIPS
	// dispatchViaInformer's inline filterListByRBAC — informer_dispatch.go
	// :482/575), so the stored items are the full SA-visible set. The
	// 0.30.242 "narrowed-at-populate, no serve-refilter" claim was WRONG
	// (the cell's apistage BindingUID is "" — contentKeyInputs never sets it
	// — so the key is identity-invariant; nothing narrows at populate). The
	// per-user narrowing MUST run at SERVE — but HOW depends on the stage:
	//
	//   - NON-UAF LIST → gateListItems (filterListByRBAC: the requester's
	//     RAW RBAC, verb=list, the LISTed GVR, item namespace). Restores the
	//     pre-0.30.242 gated serve; `serveParsedListEnvelope` (no filter) was
	//     the over-serve regression → a narrow tenant got the FULL list.
	//
	//   - UAF LIST → serveParsedListEnvelope UNCHANGED (un-narrowed here).
	//     A UAF step is called with ELEVATED privilege precisely because the
	//     requester LACKS the raw RBAC; the per-user narrowing is the UAF
	//     refilter DOWNSTREAM (refilter.go evalSingle: uaf.Verb, uaf.Resource,
	//     NamespaceFrom(item)), which DIVERGES from raw filterListByRBAC for
	//     real UAF steps (verb=create, resource swap, ns from .metadata.name).
	//     Routing a UAF LIST through gateListItems would apply raw RBAC the
	//     requester does not have → strip the UAF-intended scope to
	//     near-empty → the refilter cannot recover it → BREAK UAF. So UAF
	//     LISTs pass through un-narrowed for the refilter to handle (exactly
	//     the pre-0.30.242 UAF behaviour). The TestFalsifier58 UAF-arm (a
	//     DIVERGENT UAF) is the proof this branch is required.
	_ = entryRef
	var gated any
	var gateOK bool
	if haveParsed {
		if uafActive {
			gated, gateOK = serveParsedListEnvelope(parsed)
		} else {
			gated, gateOK = gateListItems(ctx, gvr, parsed)
		}
	} else {
		gated, gateOK = gateContentEnvelope(ctx, callPathVerb{path: call.Path}, envelope, uafActive)
	}
	if !gateOK {
		// Fail-closed — no identity / GET denied. Fall through to apiserver.
		return nil, false, true, nil
	}
	return gated, true, true, nil
}

// serveParsedListEnvelope returns the cell's pre-parsed list items as
// a decoded envelope value with served=true. Ship 0.30.242 H.c-layered
// Phase 2b replacement for the deleted gateListItemsWithMemo: the cell
// holds items that THIS BindingUID's authorisation already permitted
// at populate time (the cell key folds BindingUID, design §3.4), so
// the serve path does NOT re-filter.
//
// The returned envelope is a map shaped like the apiserver's LIST
// response: {"apiVersion", "kind", "items"} with items being the
// []any extraction of the parsed.items slice. The caller
// (jsonHandlerValue) treats this as the decoded envelope.
//
// served=false ONLY when parsed.items is nil/empty — defensive guard
// against a malformed cell. The fail-closed branch falls through to
// apiserver — same shape as gateContentEnvelope.
func serveParsedListEnvelope(parsed parsedListEnvelope) (any, bool) {
	if parsed.items == nil {
		return nil, false
	}
	items := make([]any, 0, len(parsed.items))
	for _, it := range parsed.items {
		if it != nil {
			items = append(items, it.Object)
		}
	}
	return listenvelope.New(parsed.apiVersion, parsed.kind, items), true // E6 (#674): envelope_e6_test.go
}

// ptrTo returns a pointer to v — a local generic helper so the content
// Put can attach a fresh *ResolvedKeyInputs without an addressable temp.
func ptrTo[T any](v T) *T { return &v }

// RefreshContentEntry re-dispatches the single K8s call an api-stage
// content entry caches and returns the fresh RAW envelope — the Ship F1
// refresher seam.
//
// `inputs` is a content-keyed cache.ResolvedKeyInputs
// (CacheEntryClass=="apistage", Group/Version/Resource + Namespace +
// Name-or-empty). The refresh:
//   - reconstructs the apiserver path for that (gvr, ns, name) call;
//   - dispatches it UN-GATED via dispatchViaInformer (the same
//     WithApistageContentResolve marker the request path uses), so the
//     re-Put envelope stays identity-free;
//   - returns the raw envelope for resolveAndPopulateL1 to Put under the
//     content key.
//
// There is NO whole-RESTAction re-run and NO self-hit: a content entry
// is re-dispatched directly, never self-Got. Returns (nil, nil) when the
// call is not pivot-servable (the refresher then skips-to-TTL, not an
// error).
func RefreshContentEntry(ctx context.Context, inputs cache.ResolvedKeyInputs) ([]byte, error) {
	gvr := schema.GroupVersionResource{
		Group:    inputs.Group,
		Version:  inputs.Version,
		Resource: inputs.Resource,
	}
	// #618 — re-apply the cell's selector: the refreshed body must be the
	// same SELECTED list the cell was keyed for, never the unselected one.
	path := withQuery(apiserverPathFor(gvr, inputs.Namespace, inputs.Name), inputs.Query)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: path,
			Verb: ptr.To(http.MethodGet),
		},
	}
	raw, served := dispatchViaInformer(cache.WithApistageContentResolve(ctx), call)
	if !served {
		// Not pivot-servable (pre-sync informer, metadata-only GVR, …).
		// 1.12.6 C4 (§6.2 row 9): TYPED, not (nil, nil). The (nil, nil)
		// skip-to-TTL left the apistage carrier with one layer where every
		// other class has two — no 404 discrimination, the entry resident
		// until TTL no matter how many refreshes failed. As an error it
		// enters the refresher's requeue budget like every other
		// deterministic failure and reaches the drop point (evict behind
		// the breaker). A transient pre-sync window clears inside the
		// budget and never gets there.
		return nil, fmt.Errorf("content %s/%s/%s: %w", inputs.Resource, inputs.Namespace, inputs.Name, cache.ErrContentNotServable)
	}
	return raw, nil
}

// ParseListEnvelopeForRefresh — Ship #97 (0.30.214). Pure helper the
// refresher's Put site at resolve_populate.go:255 calls when the entry
// being Put is an apistage-class LIST so it can populate
// ResolvedEntry.Items + ItemsAPIVersion + ItemsKind. Without this, the
// refresher writes RawJSON-only entries, the R3 fast-path predicate at
// apistage.go:487 (`len(entry.Items) > 0`) evaluates false on every
// subsequent content-Get-hit, and the gate falls through to
// gateListEnvelope → parseListEnvelope on the customer request goroutine
// — the 45% cum CPU long-pole captured in the 0.30.212 pre-fix profile
// (ship-97-prefix-falsifier-2026-05-31).
//
// Returns (nil, "", "", false) for:
//   - GET-by-name (inputs.Name != ""), where Items is not the right shape;
//   - a LIST whose envelope fails to parse (e.g. a non-LIST shape that
//     leaked into the apistage class — caller stores RawJSON only and
//     the gate then takes the unmarshal fallback, byte-identical to the
//     pre-fix path).
//
// Pure function over (inputs, raw) — no context. The refresher's
// WithUserInfo / SA-transport identity is unchanged (the parse runs
// inside the refresher goroutine; the resulting Items slice is the
// same Items the MISS branch at apistage.go:530-538 populates). The
// parse cost moves from "per Get-hit on request goroutines" to "per
// Put on refresher goroutines" — strictly lower rate by construction
// (Puts << Gets in steady-state) and consistent with the customer-
// priority invariant (`feedback_customer_priority_over_refresher`).
func ParseListEnvelopeForRefresh(inputs cache.ResolvedKeyInputs, raw []byte) (
	items []*unstructured.Unstructured, apiVersion, kind string, ok bool,
) {
	if inputs.Name != "" {
		// GET-by-name — not a LIST. R3 fast-path keys on LIST envelopes only.
		return nil, "", "", false
	}
	gvr := schema.GroupVersionResource{
		Group:    inputs.Group,
		Version:  inputs.Version,
		Resource: inputs.Resource,
	}
	parsed, parseOK := parseListEnvelope(gvr, raw)
	if !parseOK {
		// Malformed-at-Put envelope. Caller stores RawJSON only — the
		// content gate then takes the unmarshal fallback at hit time,
		// byte-identical to pre-fix behaviour.
		return nil, "", "", false
	}
	return parsed.items, parsed.apiVersion, parsed.kind, true
}

// apiserverPathFor reconstructs the apiserver REST path for a K8s call.
// name=="" yields a collection (LIST) path; a non-empty name yields the
// object (GET-by-name) path. Core group ("") uses the /api/v1 prefix;
// a named group uses /apis/<group>/<version>.
func apiserverPathFor(gvr schema.GroupVersionResource, namespace, name string) string {
	var b []byte
	if gvr.Group == "" {
		b = append(b, "/api/"...)
		b = append(b, gvr.Version...)
	} else {
		b = append(b, "/apis/"...)
		b = append(b, gvr.Group...)
		b = append(b, '/')
		b = append(b, gvr.Version...)
	}
	if namespace != "" {
		b = append(b, "/namespaces/"...)
		b = append(b, namespace...)
	}
	b = append(b, '/')
	b = append(b, gvr.Resource...)
	if name != "" {
		b = append(b, '/')
		b = append(b, name...)
	}
	return string(b)
}
