// deps_extract.go — Tag 0.30.8: widget dep-edge extraction for the L1
// resolved-output cache dep tracker.
//
// Per implementation plan §"Tag 0.30.8 Three-edge dependency recording
// specification":
//
//   Edge type 1 (Widget → resourcesRefs RENDER-ONLY, STATIC declarative).
//   Walks `spec.resourcesRefs.items[]` and emits one Record per item
//   whose `id` is NOT in the action-ref skip set derived from
//   `status.widgetData.actions[*].resourceRefId`.
//
//   Edge type 2 (Widget → apiRef → RestAction, STATIC declarative).
//   Reads `spec.apiRef` and emits one Record on
//   (restactions.templates.krateo.io, apiRef.namespace, apiRef.name).
//
//   Edge type 3 (RestAction → inner K8s call, DYNAMIC) is OUT OF SCOPE
//   at this tag — it would require a *RecordingDeps context threaded
//   through internal/resolvers/restactions/api/resolve.go. Deferred to
//   a future sub-ship.
//
// Action-ref filter (Revision 14, CORRECTNESS-CRITICAL): an item whose
// `id` matches any `resourceRefId` value inside
// `status.widgetData.actions.<actionType>[*].resourceRefId` is an
// action-only ref (e.g., a "View Logs" button target). Tracking those
// would cause spurious L1 invalidations. The filter is conservative:
// when actions cannot be parsed (e.g., the resolver short-circuited
// before populating widgetData), the skip set is empty and every ref
// is treated as render-eligible. That over-records (a small refresher
// cost) rather than under-records (which would risk stale data).
//
// Concurrency: helpers read fields from a *unstructured.Unstructured
// the caller owns; no mutation; safe to call from the dispatcher's
// goroutine.

package dispatchers

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/krateo-platformops/plumbing/maps"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// dispatcherLazyRegisterSlowThreshold mirrors the resolver-side
// ceiling. Anything over 250ms means rw.mu was contended (or
// factory.ForResource was unexpectedly slow). The hot path is expected
// to be sub-millisecond.
const dispatcherLazyRegisterSlowThreshold = 250 * time.Millisecond

// restActionGVR is the canonical GVR for the RestAction CR (edge type 2).
var restActionGVR = schema.GroupVersionResource{
	Group:    "templates.krateo.io",
	Version:  "v1",
	Resource: "restactions",
}

// recordListDepFn is the list-scope (name="*" wildcard) dep-record seam
// used by recordWidgetDeps for a NAME-LESS resourcesRefs target (a ref
// that displays "all of kind X in namespace Y" — the /call path carries
// resource+apiVersion but no name). Production wires it to the real
// DepTracker.RecordList; the ONLY reassigner is the _test.go falsifier,
// which transiently neuters it to a by-name Record to prove the RED
// (a by-name edge wildcard-misses a sibling object's reconcile → zero
// delivery). A package var (not a parameter) so recordWidgetDeps's
// signature is untouched; production never reassigns it.
//
// Idiom-match: resolveOnceFn / paginationFetchPageFn in this package.
var recordListDepFn = func(ctx context.Context, deps *cache.DepTracker, l1Key string, gvr schema.GroupVersionResource, namespace string) {
	deps.RecordList(ctx, l1Key, gvr, namespace)
}

// recordWidgetDeps walks the resolved widget object and records dep
// edges into the cache.DepTracker. l1Key is the L1 entry's cache key
// the dependent edges are recorded against.
//
// Recorded edges (this tag):
//   * Self-dep: widget CR (gvr, ns, name) → l1Key. Ensures DELETE on
//     the widget itself evicts its own cache entry.
//   * apiRef → restactions(GVR=restactions.templates.krateo.io,
//     ns=apiRef.namespace, name=apiRef.name).
//   * Each render-eligible resourcesRefs item → l1Key, filtered by
//     status.widgetData.actions.<actionType>[*].resourceRefId.
//
// Per feedback_restaction_no_widget_logic.md: this code lives in the
// dispatchers package (HTTP wiring), not the resolver, so widget
// equivalence remains the widget canonicalization layer's job. The
// dispatcher only RECORDS deps; it does not interpret widget output.
//
// Returns nil to allow chaining; counters track failures via the dep
// tracker's Stats.
func recordWidgetDeps(ctx context.Context, log *slog.Logger, l1Key string, gvr schema.GroupVersionResource, w *unstructured.Unstructured) {
	if l1Key == "" || w == nil {
		return
	}
	deps := cache.Deps()

	// Self-dep: widget CR → its own L1 entry. DELETE on the widget
	// evicts the cache entry.
	//
	// 0.30.9 Sub-scope B: ensure the informer for gvr is registered
	// BEFORE recording the dep edge. Without this, the dep tracker
	// would record a forward edge whose corresponding DELETE/UPDATE
	// events the watcher has never wired — eviction would be silent
	// (the 0.30.8 dormancy bug). EnsureResourceType is idempotent +
	// singleflight under rw.mu; concurrent first-readers share a
	// single informer.
	ensureWatcherInformerForGVR(gvr)
	deps.Record(ctx, l1Key, gvr, w.GetNamespace(), w.GetName())

	// Edge type 2: spec.apiRef → RestAction.
	if apiRefName, apiRefNS, ok := readApiRef(w); ok {
		ensureWatcherInformerForGVR(restActionGVR)
		deps.Record(ctx, l1Key, restActionGVR, apiRefNS, apiRefName)
	}

	// Edge type 1: status.resourcesRefs.items[], filtered by
	// status.widgetData.actions.
	skipIDs := extractActionRefIDs(w)
	for _, ref := range extractResourcesRefs(w) {
		if ref.ID != "" && skipIDs[ref.ID] {
			// Action-only ref — skip per Revision 14 filter.
			continue
		}
		refGVR, ok := parseGVR(ref.APIVersion, ref.Resource)
		if !ok {
			continue
		}
		ensureWatcherInformerForGVR(refGVR)
		if ref.Name == "" {
			// List-scope dep (e.g., a ref that targets "all of kind X
			// in namespace Y"). Record with name="*" (via the
			// recordListDepFn seam — prod default is deps.RecordList).
			recordListDepFn(ctx, deps, l1Key, refGVR, ref.Namespace)
			continue
		}
		deps.Record(ctx, l1Key, refGVR, ref.Namespace, ref.Name)
	}
}

// ensureWatcherInformerForGVR is the dispatcher-side adapter to the
// 0.30.9 Sub-scope B lazy-registration API. Looks up the global
// watcher (returns silently if cache is disabled, i.e. nil watcher)
// and calls EnsureResourceType so the informer for gvr is wired
// before we record any dep edge that references it.
//
// Idempotent + singleflight (the watcher's rw.mu serialises
// concurrent first-readers for the same GVR — the lock is the
// singleflight primitive per plan §"Singleflight on
// EnsureResourceType").
//
// The returned sync channel is intentionally discarded by the
// dispatcher: per plan §"Critical constraint" (Revision 17 binding),
// the FIRST read for a previously-unseen GVR uses the apiserver
// result; the L1 entry is populated regardless of whether the
// informer has synced, BUT the dep tracker records the forward edge
// so the very next UPDATE/DELETE will fire correctly. Subsequent
// warm reads see the now-live informer.
func ensureWatcherInformerForGVR(gvr schema.GroupVersionResource) {
	rw := cache.Global()
	if rw == nil {
		return
	}
	// 0.30.92: time the singleflight to surface rw.mu contention. The
	// resolver-side analogue in restactions/api/resolve.go emits the
	// same WARN on the inner-call path.
	start := time.Now()
	_, _ = rw.EnsureResourceType(gvr)
	if elapsed := time.Since(start); elapsed > dispatcherLazyRegisterSlowThreshold {
		slog.Warn("cache.lazy_register.slow",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.Duration("elapsed", elapsed),
			slog.Duration("threshold", dispatcherLazyRegisterSlowThreshold),
			slog.String("call_site", "dispatchers.recordWidgetDeps"),
		)
	}
}

// recordRestActionSelfDep records the self-dep edge for a RestAction
// dispatch. Kept as a separate exported helper so the restactions.go
// dispatcher doesn't reach into deps directly.
func recordRestActionSelfDep(ctx context.Context, l1Key string, gvr schema.GroupVersionResource, ns, name string) {
	if l1Key == "" {
		return
	}
	cache.Deps().Record(ctx, l1Key, gvr, ns, name)
}

// readApiRef returns (name, namespace, ok) from a widget's spec.apiRef.
func readApiRef(w *unstructured.Unstructured) (string, string, bool) {
	if w == nil {
		return "", "", false
	}
	name, _, _ := unstructured.NestedString(w.Object, "spec", "apiRef", "name")
	ns, _, _ := unstructured.NestedString(w.Object, "spec", "apiRef", "namespace")
	if name == "" {
		return "", "", false
	}
	if ns == "" {
		// apiRef in same namespace as the widget by Krateo convention.
		ns = w.GetNamespace()
	}
	return name, ns, true
}

// resourceRefLite is the minimal subset of ResourceRef we need.
type resourceRefLite struct {
	ID         string
	APIVersion string
	Resource   string
	Namespace  string
	Name       string
}

// extractResourcesRefs reads the UNION of the widget's
// status.resourcesRefs.items[] (the resolved, request-extras-driven
// displayed refs the resolver writes — see resolve.go status
// SetNestedField + the extractResourcesRefsItems precedent at
// phase1_walk.go:1567) AND spec.resourcesRefs.items[] (any
// author-declared static refs) and returns the lite triples.
//
// (#61, 1.5.8) Decoding the status `path` is LOAD-BEARING: a composition-
// DETAIL widget resolves its displayed target into status with an EMPTY
// spec. A spec-only read — OR a naive status FIELD read (the items have no
// such fields) — recorded NO dep edge on the displayed resource → the
// armed top-level L1 key was never dirty-marked on its reconcile →
// /refreshes delivered zero events (broken 1.5.5–1.5.7). The union (not a
// flip to status-only) preserves genuinely-static spec refs (resolve.go
// guards the status write with len(results)>0, so an unresolved widget is
// spec-only).
func extractResourcesRefs(w *unstructured.Unstructured) []resourceRefLite {
	if w == nil {
		return nil
	}
	out := make([]resourceRefLite, 0, 8)
	seen := map[resourceRefLite]struct{}{}
	appendUnique := func(r resourceRefLite) {
		if _, dup := seen[r]; dup {
			return
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}

	// (a) status items: PATH-DECODED (ResourceRefResult has no inline gvr
	// fields; the displayed target is encoded in path as /call?resource=…).
	if arr, ok, err := maps.NestedSlice(w.Object, "status", "resourcesRefs", "items"); ok && err == nil {
		for _, raw := range arr {
			m, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			pathStr, _ := m["path"].(string)
			if pathStr == "" {
				continue
			}
			ref, ok := objects.ParseCallPathToObjectRef(pathStr)
			if !ok {
				// External link / non-/call path / missing resource|apiVersion.
				continue
			}
			r := resourceRefLite{
				APIVersion: ref.APIVersion,
				Resource:   ref.Resource,
				Namespace:  ref.Namespace,
				Name:       ref.Name,
			}
			if id, ok := m["id"].(string); ok {
				r.ID = id
			}
			appendUnique(r)
		}
	}

	// (b) spec items: FIELD-READ (static author refs carry inline fields).
	if arr, ok, err := maps.NestedSlice(w.Object, "spec", "resourcesRefs", "items"); ok && err == nil {
		for _, raw := range arr {
			m, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			r := resourceRefLite{}
			if v, ok := m["id"].(string); ok {
				r.ID = v
			}
			if v, ok := m["apiVersion"].(string); ok {
				r.APIVersion = v
			}
			if v, ok := m["resource"].(string); ok {
				r.Resource = v
			}
			if v, ok := m["namespace"].(string); ok {
				r.Namespace = v
			}
			if v, ok := m["name"].(string); ok {
				r.Name = v
			}
			appendUnique(r)
		}
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// extractActionRefIDs walks status.widgetData.actions and returns the
// set of resourceRefId values found across all action entries. Per
// Revision 14, the resolver populates this status during Resolve; if
// status is empty (resolver failed before reaching action wiring), the
// set is empty and every spec.resourcesRefs item is treated as a
// render dep.
//
// The walker tolerates two shapes:
//   1. actions.<actionType> = []{resourceRefId: "..."} (a slice).
//   2. actions.<actionType> = {resourceRefId: "..."}   (a map).
// The Krateo widget contract has historically used both; the prewarm
// helper this lifts from also handled both shapes.
func extractActionRefIDs(w *unstructured.Unstructured) map[string]bool {
	out := map[string]bool{}
	if w == nil {
		return out
	}
	actions, ok, err := maps.NestedMap(w.Object, "status", "widgetData", "actions")
	if !ok || err != nil {
		return out
	}
	for _, raw := range actions {
		switch v := raw.(type) {
		case []any:
			for _, item := range v {
				if m, ok := item.(map[string]any); ok {
					if id, ok := m["resourceRefId"].(string); ok && id != "" {
						out[id] = true
					}
				}
			}
		case map[string]any:
			if id, ok := v["resourceRefId"].(string); ok && id != "" {
				out[id] = true
			}
		}
	}
	return out
}

// parseGVR maps (apiVersion, resource) to a GroupVersionResource.
// Returns ok=false when either input is malformed.
//   - "v1" → group="", version="v1"
//   - "apps/v1" → group="apps", version="v1"
//   - "templates.krateo.io/v1" → group="templates.krateo.io", version="v1"
func parseGVR(apiVersion, resource string) (schema.GroupVersionResource, bool) {
	if apiVersion == "" || resource == "" {
		return schema.GroupVersionResource{}, false
	}
	gvr := schema.GroupVersionResource{Resource: resource}
	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) == 1 {
		// Core group ("v1").
		gvr.Version = parts[0]
	} else {
		gvr.Group = parts[0]
		gvr.Version = parts[1]
	}
	if gvr.Version == "" {
		return schema.GroupVersionResource{}, false
	}
	return gvr, true
}
