// walk_declined_external.go — #643 PR-B2: the discovery walk consults and marks
// the SAME engine-wide declined-external set (DES) the seed does since PR-B.
//
// WHY. After the 1.12.46 roll the seed skipped its marked keys, yet
// external_fetch_total{background} stayed ~60/min: every engine pass that walks
// (each boot attempt 0, each gvr-discovered pass) re-resolved every walked
// widget, and a widget whose apiRef RESTAction dials an external endpoint
// re-dialled it on every walk. That resolve's identity-free content Put is
// already declined for an external touch (populateWidgetContentL1, the same
// GTTL-1 rule as the seed), so re-resolving warms nothing.
//
// KEY. The walk marks and consults wcKey — widgetContentL1Key, the key its
// populate Puts under (identity-free). wcKey == "" (layer off) → nothing to
// mark, the walk always resolves.
//
// MARK (same rule as the seed, brief §1(c) as amended): only on an OBSERVED
// external touch (the ExternalTouchedSink on the walk's resolve ctx), never on
// apiref.DeclaresExternalEndpoint (I-5). The mark records the same facts as the
// seed's — deps + entry stamp, served cells, RA uid/RV, endpoint Secret RVs —
// and, new for the walk:
//
//   - the node's CHILD REFS. The walk resolve is also DISCOVERY: its
//     status.resourcesRefs.items drive recordWalkChildren and the descent. A
//     skip REPLAYS the recorded children, so the harvest, the coverage
//     recording and the descent are identical to a resolve. A mark carrying no
//     recorded walk output (one the seed took) cannot be replayed; the walk
//     resolves instead.
//   - the walked widget's own uid + resourceVersion. The walk holds the LIVE
//     widget (re-read every walk), so a resourcesRefs / resourcesRefsTemplate
//     edit retires the mark (reason widget_changed) and the new child set is
//     resolved, never a stale one replayed.
//
// ERRORED RESOLVES (architect ruling (c), #643). A resolve that touched external
// and then ERRORED is marked only when the live widget is a structural LEAF (it
// declares no spec.resourcesRefs items and no spec.resourcesRefsTemplate): for
// a leaf "no children" holds whether or not the fetch succeeds, so replaying the
// error path (no populate, no children, no descent) loses nothing. A widget
// that declares refs is NOT marked — a later walk whose fetch succeeds must
// still discover its subtree — and is counted on
// snowplow_prewarm_walk_errored_unmarkable_total. Depth 0 (a root) never marks
// on error: the caller needs the error.
//
// NO PAGINATION JOB ON A SKIP. Pages > 1 of an external-touched apiRef are
// declined by the same external gate in the deep-page drain
// (withWidgetContentPutSinks is shared), so skipping their collection loses no
// Put.
//
// RETIRE. Every PR-B rule applies unchanged (Put-unmark at putCoreLocked
// except ExternalTTL, RA/Secret RV, GONE, d4 on uncovered deps, served cells via
// CellPutSeq), plus widget_changed. Each is counted on
// snowplow_prewarm_declined_external_retired_total{reason}.
//
// I-6. The set is installed only by the engine's processScope and by the
// pre-engine phase1 boot walk (ruling (b)); a /call and the refresher never
// carry it, so the request path always resolves.

package dispatchers

import (
	"context"
	"expvar"
	"log/slog"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// walkReplay is what a walk skip replays.
type walkReplay struct {
	errored  bool
	children []navChildRef
}

var (
	// walkSkipDeclinedExternal is the walk's POSITIVE CONTROL: walked widgets
	// whose resolve was skipped on a held mark (and whose children were
	// replayed). After a roll it must read > 0, or the skip never fired.
	walkSkipDeclinedExternal atomic.Uint64
	// walkErroredUnmarkable counts external-touched walk resolves that errored
	// on a widget declaring children: not marked (ruling (c)), so re-dialled.
	walkErroredUnmarkable atomic.Uint64
)

// WalkSkipDeclinedExternalTotal / WalkErroredUnmarkableTotal are the two
// counters' readings (expvar + metrics.go).
func WalkSkipDeclinedExternalTotal() uint64 { return walkSkipDeclinedExternal.Load() }
func WalkErroredUnmarkableTotal() uint64    { return walkErroredUnmarkable.Load() }

func resetWalkDeclinedExternalCountersForTest() {
	walkSkipDeclinedExternal.Store(0)
	walkErroredUnmarkable.Store(0)
}

// walkDeclineUnit names the walked widget and its apiRef RESTAction so the mark
// records the coordinates GONE and the RA re-check retire on.
func walkDeclineUnit(gvr schema.GroupVersionResource, in *unstructured.Unstructured) declineUnit {
	u := declineUnit{widget: cache.DepKey{GVR: gvr, Namespace: in.GetNamespace(), Name: in.GetName()}}
	if ref, err := widgets.GetApiRef(in.Object); err == nil && ref.Name != "" {
		u.ra = raObjectRef(ref.Namespace, ref.Name)
	}
	return u
}

// widgetDeclaresNoChildren is the structural LEAF test of ruling (c): the live
// widget declares no spec.resourcesRefs items and no spec.resourcesRefsTemplate.
// Anything present but not an empty list reads as "declares children" — doubt
// resolves toward re-resolving.
func widgetDeclaresNoChildren(in *unstructured.Unstructured) bool {
	if in == nil {
		return false
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(in.Object, "spec", "resourcesRefsTemplate"); found {
		return false
	}
	items, found, _ := unstructured.NestedFieldNoCopy(in.Object, "spec", "resourcesRefs", "items")
	if !found {
		return true
	}
	sl, ok := items.([]any)
	return ok && len(sl) == 0
}

// walkMarkDeclinedExternal is the walk's mark site, called after the resolve
// with the resolve ctx (it carries the sinks, the dep-gen sink and the decline
// unit). errored: the resolve returned an error at depth > 0.
func walkMarkDeclinedExternal(resCtx context.Context, key string, in *unstructured.Unstructured,
	uid, rv string, children []navChildRef, errored bool) {
	if key == "" {
		return
	}
	set := cache.SeedDeclinedExternalSetFromContext(resCtx)
	if set == nil {
		return
	}
	if cache.ExternalTouchedSinkFromContext(resCtx).Count() == 0 {
		return // observed touch only (I-5)
	}
	if errored && !walkErroredMarkableFn(in) {
		walkErroredUnmarkable.Add(1)
		return
	}
	m := buildDeclinedExternalMark(resCtx)
	m.WalkRecorded = true
	m.WalkErrored = errored
	m.WidgetUID, m.WidgetResourceVersion = uid, rv
	if !errored {
		m.WalkChildren = make([]cache.WalkChildRef, 0, len(children))
		for _, c := range children {
			m.WalkChildren = append(m.WalkChildren, cache.WalkChildRef{ID: c.ID, Path: c.Path, Verb: c.Verb, Allowed: c.Allowed})
		}
	}
	set.MarkWith(key, m)
}

// walkDeclinedExternalReplay is the walk's consult. It returns the replay and
// true iff key holds a walk-recorded mark whose every fact still holds against
// the LIVE widget. A mark whose facts changed is retired here (counted by
// reason) and the widget is resolved; a mark with no walk output is left for
// the resolve to replace.
func walkDeclinedExternalReplay(ctx context.Context, key string, in *unstructured.Unstructured, depth int) (walkReplay, bool) {
	set := cache.SeedDeclinedExternalSetFromContext(ctx)
	m, ok := set.Lookup(key)
	if !ok || !m.WalkRecorded {
		return walkReplay{}, false
	}
	if m.WalkErrored && depth == 0 {
		return walkReplay{}, false
	}
	reason := ""
	if !walkWidgetVersionHoldsFn(m, in) {
		reason = cache.DeclinedExternalRetiredWidgetChanged
	} else if holds, r := declinedExternalMarkHolds(ctx, m); !holds {
		reason = r
	}
	if reason != "" {
		if set.Unmark(key) {
			cache.RecordDeclinedExternalRetired(reason, 1)
		}
		slog.Default().Debug("phase1.walk.declined_external.retired",
			slog.String("subsystem", "cache"),
			slog.Int("depth", depth),
			slog.String("reason", reason),
			slog.String("effect", "a fact the walk's declined-external mark was taken under changed; the mark is "+
				"retired and the widget is resolved (its children re-discovered)"),
		)
		return walkReplay{}, false
	}
	r := walkReplay{errored: m.WalkErrored}
	if !m.WalkErrored {
		r.children = make([]navChildRef, 0, len(m.WalkChildren))
		for _, c := range m.WalkChildren {
			r.children = append(r.children, navChildRef{ID: c.ID, Path: c.Path, Verb: c.Verb, Allowed: c.Allowed})
		}
		r.children = walkReplayChildrenFn(r.children)
	}
	walkSkipDeclinedExternal.Add(1)
	return r, true
}

// walkWidgetVersionHolds is the widget uid+RV fold: the mark holds only for the
// widget object it was taken on.
func walkWidgetVersionHolds(m *cache.DeclinedExternalMark, in *unstructured.Unstructured) bool {
	if m == nil || in == nil {
		return false
	}
	return string(in.GetUID()) == m.WidgetUID && in.GetResourceVersion() == m.WidgetResourceVersion
}

// The three walk-skip rules, as seams ONLY so the W2-m / W3-m / (c)-iii
// mutation arms can remove one at a time and prove each is load-bearing.
// Production never reassigns them.
var (
	walkReplayChildrenFn     = func(c []navChildRef) []navChildRef { return c }
	walkWidgetVersionHoldsFn = walkWidgetVersionHolds
	walkErroredMarkableFn    = widgetDeclaresNoChildren
)

// walkDeclinedExternalSkipEnabled is the W2 differential's switch: the arm
// forces the walk's consult OFF to compare discovery with and without the
// skip. Production never reassigns it.
var walkDeclinedExternalSkipEnabled = true

// CFG-1: published only with the cache on.
func init() {
	if cache.Disabled() {
		return
	}
	expvar.Publish("snowplow_prewarm_walk_skip_declined_external_total", expvar.Func(func() any {
		return WalkSkipDeclinedExternalTotal()
	}))
	expvar.Publish("snowplow_prewarm_walk_errored_unmarkable_total", expvar.Func(func() any {
		return WalkErroredUnmarkableTotal()
	}))
}
