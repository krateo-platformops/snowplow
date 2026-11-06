// widgets_inline.go — #72 Phase 1: inline-rendered-children PRODUCER.
//
// After widgets.Resolve returns and BEFORE the parent envelope is encoded
// (widgets.go), walk the just-built status.resourcesRefs.items[]; for each item
// whose source ref was Inline AND Allowed AND verb==GET, resolve the child
// server-side UNDER THE REQUESTING USER'S ctx via ResolveNestedCall (in-package
// — no import cycle, C-INLINE-0) and embed the resolved child envelope into
// items[i].rendered.
//
// WHY THIS PLACEMENT (dispatcher, not resolver — C-INLINE-0): resolvers/widgets
// importing dispatchers.ResolveNestedCall closes a Go import cycle (dispatchers
// already imports widgets). Here ResolveNestedCall is in-package, AND the ctx
// already carries WithUserInfo + the SA transport + WithL1KeyContext(cacheKey)
// (widgets.go) — so the §5 dep-edge preservation (child resource dep-edges the
// PARENT L1 key, via the L1KeyFromContext ResolveNestedCall preserves) is
// automatic, and RBAC is enforced under the requesting identity by
// ResolveNestedCall's checkDispatchRBAC gate (C-INLINE-1's runtime half).
//
// A1 (C-INLINE-0 / task #30) — AS-BUILT IS PATH (i) decode-into-map, the
// design's documented FALLBACK, NOT the doc's preferred RawMessage (ii).
// ResolveNestedCall returns the child already encoded once; we json.Unmarshal
// those bytes into a map[string]any and splice the MAP into the parent dict.
// Path (ii) (splice the json.RawMessage verbatim) encodes fine but PANICS the
// parent's unstructured deep-copy on the cache-Put/refresher path
// (runtime.DeepCopyJSONValue rejects json.RawMessage) — caught pre-ship by the
// §3/§5 falsifier (controller-gen deepcopy-gen also can't handle the field).
// Path (i) keeps the carrier a standard map (deep-copy-safe). Cost: a bounded
// per-child decode + the parent's SINGLE encode re-encodes the tree once (NOT a
// second independent whole-parent encode per child) — LINEAR in child count
// (§6 hermetic proxy: alloc ratio ~8 for 8× children). The full 50K RSS arm
// (tester Phase 3) gates the re-encode constant at scale.
//
// DEPTH — ONE LEVEL HARD (C-INLINE-2): this walk resolves an inline ref's child
// but does NOT re-enter the inline-walk on that child's own inline refs — the
// recursive arm is gated OFF until the grandchild Edge-type-3 dep-edge is proven
// (the one-level case rides recordWidgetDeps' path-decode union, #61; the
// grandchild case is only comment-asserted). ResolveNestedCall's own
// NestedCallMaxDepth remains the safety ceiling for apiRef-transitive chains.
//
// DEFAULT-OFF / byte-identical: when no resourcesRefs item carries inline (the
// only state until a widget authors inline:true), this walk makes ZERO changes
// — no `rendered` field is written, the served shape is exactly today's.
package dispatchers

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/krateo-platformops/plumbing/maps"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// embedInlineChildren mutates res.Object in place: for each
// status.resourcesRefs.items[] entry flagged inline+allowed+GET, resolve the
// child via ResolveNestedCall under ctx and set items[i]["rendered"] to the
// resolved envelope (as json.RawMessage — single-encode). perPage/page/extras
// are the parent request's, threaded into the child resolve so an inline grid
// embeds the SAME page the parent requested (bounding rides the declared
// slice/pager, no magic cap). Returns the number of children embedded (0 when
// no inline ref → byte-identical to today).
//
// Errors resolving an individual child are logged and SKIPPED (the item keeps
// its path so an old SPA still follows it; the parent envelope is still served).
// A child the user cannot read returns a forbidden-class error from
// ResolveNestedCall → skipped → no rendered body → no leak.
func embedInlineChildren(ctx context.Context, log *slog.Logger, res *unstructured.Unstructured, perPage, page int, extras map[string]any) int {
	if res == nil {
		return 0
	}
	// NoCopy: we mutate the item maps IN PLACE (they are the live references
	// inside res.Object), so the embedded `rendered` lands directly in the dict
	// that encodeResolvedJSON will encode — no write-back needed.
	items, ok, err := maps.NestedSliceNoCopy(res.Object, "status", "resourcesRefs", "items")
	if !ok || err != nil || len(items) == 0 {
		return 0
	}

	embedded := 0
	for i := range items {
		m, ok := items[i].(map[string]any)
		if !ok {
			continue
		}
		// Only inline + allowed + GET refs are embedded. The inline flag was
		// carried from the source ref onto the ResourceRefResult
		// (resourcesrefs/resolve.go) and marshalled into the item map.
		inline, _ := m["inline"].(bool)
		allowed, _ := m["allowed"].(bool)
		verb, _ := m["verb"].(string)
		if !inline || !allowed || verb != "GET" {
			continue
		}
		pathStr, _ := m["path"].(string)
		ref, ok := objects.ParseCallPathToObjectRef(pathStr)
		if !ok {
			// Non-/call path (external link / missing resource|apiVersion) —
			// nothing to resolve in-process; leave the item as-is.
			continue
		}

		// Resolve the child UNDER ctx (per-user identity + SA transport +
		// L1KeyFromContext already threaded). ResolveNestedCall gates RBAC
		// (checkDispatchRBAC) + bounds depth + preserves the parent L1 key for
		// the §5 dep-edge. ONE LEVEL: ResolveNestedCall resolves THIS child; it
		// does not re-enter embedInlineChildren on the child's own inline refs.
		childBytes, cerr := ResolveNestedCall(ctx, ref, perPage, page, extras)
		if cerr != nil {
			log.WarnContext(ctx, "inline child resolve failed; serving parent without this rendered child",
				slog.String("id", asString(m["id"])),
				slog.String("path", pathStr),
				slog.Any("err", cerr),
				slog.String("effect", "item keeps its path (old SPA follows it); no rendered body embedded"),
			)
			continue
		}

		// A1 (path i — decode-into-map, the design's documented fallback).
		// The RawMessage-verbatim splice (path ii) ENCODES fine but PANICS the
		// unstructured deep-copy (runtime.DeepCopyJSONValue rejects
		// json.RawMessage) — and res IS deep-copied on the cache Put / refresher
		// path, so path ii is unsound here. Decode the child bytes into a
		// standard map[string]any so res.Object stays a pure JSON-value tree
		// (deep-copy-safe); the parent's single encodeResolvedJSON re-encodes it
		// once with the parent. Cost: a per-child decode+re-encode (NOT a second
		// INDEPENDENT encode of the whole parent) — gated by the §6 RSS bench.
		var childMap map[string]any
		if uerr := json.Unmarshal(childBytes, &childMap); uerr != nil {
			log.WarnContext(ctx, "inline child decode failed; serving parent without this rendered child",
				slog.String("id", asString(m["id"])),
				slog.String("path", pathStr),
				slog.Any("err", uerr),
			)
			continue
		}
		// Mutated in place on the live item map (NoCopy slice above).
		m["rendered"] = childMap
		embedded++
	}
	return embedded
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

// hasInlineGETRef reports whether the widget CR declares any
// spec.resourcesRefs.items[] ref with inline:true whose verb is GET (or
// unset → defaults to GET on resolve). PRE-RESOLVE — reads the fetched widget
// CR's spec directly (no apiserver round-trip), symmetric with the existing
// pre-resolve classifiers. This is the C-INLINE-1 RBAC-sensitivity signal: a
// widget that will embed a server-resolved child `rendered` body MUST route to
// the per-user `widgets` L1, never the shared identity-free `widgetContent`
// cell (or the SA-maximal child render leaks cross-user).
//
// Scope: the STATIC spec.resourcesRefs.items[] (where `inline` is a real field).
// resourcesRefsTemplate refs are jq-templated and have no static pre-resolve
// inline field to read here; a templated inline ref would still be embedded at
// resolve time, but the classification signal is the static declaration — which
// is what an author sets to opt a widget into inline. (If a future template
// surface needs inline, the classifier extends there; today no template carries
// it.)
func hasInlineGETRef(obj map[string]any) bool {
	if obj == nil {
		return false
	}
	arr, ok, err := maps.NestedSliceNoCopy(obj, "spec", "resourcesRefs", "items")
	if !ok || err != nil {
		return false
	}
	for _, raw := range arr {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		inline, _ := m["inline"].(bool)
		if !inline {
			continue
		}
		// verb unset → resolves to GET (mapVerbs default includes get); an
		// explicit non-GET verb is not an inline-render candidate.
		v, _ := m["verb"].(string)
		if v == "" || v == "get" || v == "GET" {
			return true
		}
	}
	return false
}
