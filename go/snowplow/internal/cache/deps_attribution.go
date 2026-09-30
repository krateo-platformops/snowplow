// deps_attribution.go — #239: dirty-mark attribution.
//
// dirtyMarkTotal was a single global counter: it could not say whether a
// dirty-mark came from an object event or a type event, from an add/update, a
// delete or a degraded probe, nor which entry CLASS it marked — so a 3× rise
// could not be told apart from benign fan-out. This splits it into two axes:
//
//   EMIT (post-dedup, OTLP-alertable): a {path, cause, class} distribution.
//     path  = object_event | type_event
//     cause = add_update|delete|degraded (object) or crd_add|crd_delete|
//             schema_relist|store_repair (type)
//     class = self | list_dep | exact_dep
//   SUBMIT (pre-dedup): a per-source count at submitDepEvent (watch |
//     relist_bridge | reconcile) — the ONLY place "which mechanism submits the
//     most events" is soundly answerable, because the workqueue dedups
//     same-coordinate submits ACROSS sources so the emit site cannot know the
//     submitter.
//
// Plus a fan-out denominator (events that dirty-marked >=1) so mean fan-out =
// dirtyMarkTotal / dirty_mark_events_total, and a /debug/vars-only per-GVR
// drill-down map (kept OFF OTLP — hundreds of GVRs would explode cardinality).
//
// SUM INVARIANT BY CONSTRUCTION: recordDirtyMarks is the SINGLE writer of both
// dirtyMarkTotal and the per-bucket counters, bumping them together, so
// Σ buckets == dirtyMarkTotal always. A future emit site that forgot to route
// through here breaks the invariant, which the falsifier pins.

package cache

import (
	"sort"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Attribution label values — also the OTLP attribute strings.
const (
	dmPathObject = "object_event"
	dmPathType   = "type_event"

	dmCauseAddUpdate    = "add_update"
	dmCauseDelete       = "delete"
	dmCauseDegraded     = "degraded"
	dmCauseCRDAdd       = "crd_add"
	dmCauseCRDDelete    = "crd_delete"
	dmCauseSchemaRelist = "schema_relist"
	dmCauseStoreRepair  = "store_repair"

	dmClassSelf     = "self"
	dmClassListDep  = "list_dep"
	dmClassExactDep = "exact_dep"

	dmSourceWatch        = "watch"
	dmSourceRelistBridge = "relist_bridge"
	dmSourceReconcile    = "reconcile"

	// dmGVRCap bounds the /debug/vars-only per-GVR drill-down; overflow folds
	// into dmGVROther (the fallthroughCells cardinality idiom).
	dmGVRCap   = 256
	dmGVROther = "__other__"
)

// dmBucket is one {path, cause, class} attribution coordinate.
type dmBucket struct{ Path, Cause, Class string }

// DirtyMarkBucket is the exported snapshot row (expvar + OTLP).
type DirtyMarkBucket struct {
	Path  string `json:"path"`
	Cause string `json:"cause"`
	Class string `json:"class"`
	Count uint64 `json:"count"`
}

// dmAttribution holds the #239 counters. byBucket and bySource are populated
// once at construction and never re-keyed, so increments/reads are lock-free.
type dmAttribution struct {
	byBucket     map[dmBucket]*atomic.Uint64
	bySource     map[string]*atomic.Uint64
	events       atomic.Uint64 // events (object or type) that dirty-marked >=1
	unattributed atomic.Uint64 // guard: a class combo not pre-registered; must stay 0

	gvrMu sync.Mutex
	gvr   map[string]uint64 // /debug/vars-only drill-down, capped at dmGVRCap
}

// dmValidBuckets is the EXACT set of {path,cause,class} combos the classifier
// can emit. Pre-registering exactly these keeps every increment lock-free and
// lets the sum-invariant falsifier assert Σ buckets == total with no unattributed
// stragglers.
//
//   - object_event: add_update & degraded reach every class; delete never marks
//     self (a self entry is EVICTED on absence, never dirty-marked).
//   - type_event: no self class (a type-scope dep is list or exact); crd_add is
//     list-only (a stale-negative LIST is all a CRD-add can invalidate).
func dmValidBuckets() []dmBucket {
	var out []dmBucket
	for _, c := range []string{dmClassSelf, dmClassListDep, dmClassExactDep} {
		out = append(out, dmBucket{dmPathObject, dmCauseAddUpdate, c})
		out = append(out, dmBucket{dmPathObject, dmCauseDegraded, c})
	}
	out = append(out,
		dmBucket{dmPathObject, dmCauseDelete, dmClassListDep},
		dmBucket{dmPathObject, dmCauseDelete, dmClassExactDep},
		dmBucket{dmPathType, dmCauseCRDAdd, dmClassListDep},
	)
	for _, cause := range []string{dmCauseCRDDelete, dmCauseSchemaRelist, dmCauseStoreRepair} {
		out = append(out,
			dmBucket{dmPathType, cause, dmClassListDep},
			dmBucket{dmPathType, cause, dmClassExactDep},
		)
	}
	return out
}

func newDMAttribution() *dmAttribution {
	a := &dmAttribution{
		byBucket: map[dmBucket]*atomic.Uint64{},
		bySource: map[string]*atomic.Uint64{},
		gvr:      map[string]uint64{},
	}
	for _, b := range dmValidBuckets() {
		a.byBucket[b] = new(atomic.Uint64)
	}
	for _, s := range []string{dmSourceWatch, dmSourceRelistBridge, dmSourceReconcile} {
		a.bySource[s] = new(atomic.Uint64)
	}
	return a
}

// dirtyMarkCauseForState maps the probe verdict to the object-event cause label.
func dirtyMarkCauseForState(state objectState) string {
	switch state {
	case objAbsent:
		return dmCauseDelete
	case objUnknownDegraded:
		return dmCauseDegraded
	default: // objExists (add/update)
		return dmCauseAddUpdate
	}
}

// dirtyMarkCauseForEventType maps dirtyMarkResourceType's event label to a cause.
func dirtyMarkCauseForEventType(eventType string) string {
	switch eventType {
	case "CRD_ADD":
		return dmCauseCRDAdd
	case "CRD_DELETE":
		return dmCauseCRDDelete
	case "SCHEMA_RELIST":
		return dmCauseSchemaRelist
	case "STORE_REPAIR":
		return dmCauseStoreRepair
	default:
		return eventType
	}
}

// recordDirtyMarks is the SINGLE writer of dirtyMarkTotal and the per-bucket
// attribution counters — it bumps both together so Σ buckets == dirtyMarkTotal
// by construction. counts maps class -> number of L1 keys dirty-marked under
// (path, cause). It also bumps the fan-out event denominator (once per event
// that marked >=1) and the /debug/vars GVR drill-down.
func (d *DepTracker) recordDirtyMarks(path, cause string, counts map[string]int, gvr schema.GroupVersionResource) {
	if d == nil {
		return
	}
	total := 0
	for class, n := range counts {
		if n <= 0 {
			continue
		}
		total += n
		if c := d.dmAttr.byBucket[dmBucket{path, cause, class}]; c != nil {
			c.Add(uint64(n))
		} else {
			d.dmAttr.unattributed.Add(uint64(n)) // guard: must stay 0
		}
	}
	if total == 0 {
		return
	}
	d.dirtyMarkTotal.Add(uint64(total))
	d.dmAttr.events.Add(1)
	d.dmAttr.addGVR(gvr.String(), total)
}

// addGVR records n dirty-marks against gvr in the capped drill-down map.
func (a *dmAttribution) addGVR(gvr string, n int) {
	a.gvrMu.Lock()
	defer a.gvrMu.Unlock()
	if _, ok := a.gvr[gvr]; !ok && len(a.gvr) >= dmGVRCap {
		a.gvr[dmGVROther] += uint64(n)
		return
	}
	a.gvr[gvr] += uint64(n)
}

// recordSubmitSource bumps the pre-dedup submit-source counter. Called from
// submitDepEvent (deps_watch.go) BEFORE the workqueue dedups the coordinate.
func (d *DepTracker) recordSubmitSource(source string) {
	if d == nil || d.dmAttr == nil {
		return
	}
	if c := d.dmAttr.bySource[source]; c != nil {
		c.Add(1)
	}
}

// dirtyMarkClass classifies one dirty-marked L1 key. Exact-object beats list
// (collectMatchesWithDep already resolved dk to the most specific bucket), and a
// self-representation entry (its cached OUTPUT is the object itself) is broken
// out from an ordinary exact GET-dependent.
func dirtyMarkClass(dk DepKey, isSelf bool) string {
	if dk.Name == listWildcard {
		return dmClassListDep
	}
	if isSelf {
		return dmClassSelf
	}
	return dmClassExactDep
}

// --- snapshots (expvar + OTLP range the SAME accessors — C7 anti-drift) ------

func (d *DepTracker) dirtyMarkAttributionSnapshot() []DirtyMarkBucket {
	if d == nil || d.dmAttr == nil {
		return nil
	}
	out := make([]DirtyMarkBucket, 0, len(d.dmAttr.byBucket))
	for b, c := range d.dmAttr.byBucket {
		out = append(out, DirtyMarkBucket{b.Path, b.Cause, b.Class, c.Load()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		if out[i].Cause != out[j].Cause {
			return out[i].Cause < out[j].Cause
		}
		return out[i].Class < out[j].Class
	})
	return out
}

func (d *DepTracker) dirtyMarkSubmitSourceSnapshot() map[string]uint64 {
	if d == nil || d.dmAttr == nil {
		return nil
	}
	out := make(map[string]uint64, len(d.dmAttr.bySource))
	for s, c := range d.dmAttr.bySource {
		out[s] = c.Load()
	}
	return out
}

func (d *DepTracker) dirtyMarkEventsTotal() uint64 {
	if d == nil || d.dmAttr == nil {
		return 0
	}
	return d.dmAttr.events.Load()
}

func (d *DepTracker) dirtyMarkUnattributedTotal() uint64 {
	if d == nil || d.dmAttr == nil {
		return 0
	}
	return d.dmAttr.unattributed.Load()
}

func (d *DepTracker) dirtyMarkGVRSnapshot() map[string]uint64 {
	if d == nil || d.dmAttr == nil {
		return nil
	}
	d.dmAttr.gvrMu.Lock()
	defer d.dmAttr.gvrMu.Unlock()
	out := make(map[string]uint64, len(d.dmAttr.gvr))
	for k, v := range d.dmAttr.gvr {
		out[k] = v
	}
	return out
}

// --- package-level accessors the metrics/expvar layer ranges over ------------

// DirtyMarkAttributionSnapshot returns the {path,cause,class} distribution of
// dirty-marks (OTLP + /debug/vars). #239.
func DirtyMarkAttributionSnapshot() []DirtyMarkBucket { return Deps().dirtyMarkAttributionSnapshot() }

// DirtyMarkSubmitSourceSnapshot returns the pre-dedup per-source submit counts.
func DirtyMarkSubmitSourceSnapshot() map[string]uint64 { return Deps().dirtyMarkSubmitSourceSnapshot() }

// DirtyMarkEventsTotal returns the number of events that dirty-marked >=1 (the
// fan-out denominator; mean fan-out = dirty_mark_total / this).
func DirtyMarkEventsTotal() uint64 { return Deps().dirtyMarkEventsTotal() }

// DirtyMarkUnattributedTotal returns dirty-marks whose class combo was not
// pre-registered — a classifier bug; must read 0.
func DirtyMarkUnattributedTotal() uint64 { return Deps().dirtyMarkUnattributedTotal() }

// DirtyMarkGVRSnapshot returns the /debug/vars-only per-GVR drill-down (capped).
func DirtyMarkGVRSnapshot() map[string]uint64 { return Deps().dirtyMarkGVRSnapshot() }

// DirtyMarkAttributionExpvar is the /debug/vars view: the OTLP {path,cause,class}
// distribution PLUS the /debug/vars-only per-GVR drill-down, the pre-dedup submit
// sources, the fan-out denominator and the unattributed guard.
func DirtyMarkAttributionExpvar() any {
	return map[string]any{
		"by_bucket":     DirtyMarkAttributionSnapshot(),
		"submit_source": DirtyMarkSubmitSourceSnapshot(),
		"events_total":  DirtyMarkEventsTotal(),
		"unattributed":  DirtyMarkUnattributedTotal(),
		"by_gvr":        DirtyMarkGVRSnapshot(),
	}
}
