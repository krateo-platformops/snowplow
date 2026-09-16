// harvest_inspect.go — 1.12.7 observability. A read-only, METADATA-ONLY view
// of what the Phase-1 harvesters are holding.
//
// WHY IT EXISTS. 1.12.7 F6b/F6c made a confirmed-gone object drop out of both
// harvesters, which is what stops a deleted widget being re-resolved into L1
// forever. Nothing exposed that state, so the fix could be verified in tests
// and nowhere else: on a live cluster there was no way to answer "is this
// coordinate still held?" — the one question the acceptance step has to
// settle. snowplow_phase1_harvest_forgotten_total says a forget HAPPENED; this
// says what the harvester holds NOW, which is what you check after a delete.
//
// METADATA ONLY, AND STRUCTURALLY SO. Every accessor here returns counts and
// booleans. Nothing returns, or can return, any part of a harvested object:
// the harvested value is a whole widget CR (spec, status, resolved rows), it
// is per-identity-resolved downstream, and the standing rule is that the debug
// surface never dumps content — metadata and hashes only. A coordinate and a
// boolean answer the question without carrying a single byte of the captured
// object, so the guard here is the return TYPE, not a reviewer's care.
package dispatchers

import (
	"sync/atomic"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// harvestInspectSource is the pair of live harvesters. Stored as one pointer
// so the two can never be observed half-swapped.
type harvestInspectSource struct {
	nav    *navWidgetHarvester
	apiRef *contentPrewarmHarvester
}

// liveHarvesters is set once at boot, where the engine is wired. nil before
// then, and nil for the whole process when prewarm is off — every accessor
// reports ok=false rather than pretending the set is empty, because "no
// harvester" and "an empty harvester" are different answers to the acceptance
// question and conflating them would let a misconfigured pod read as healthy.
var liveHarvesters atomic.Pointer[harvestInspectSource]

// publishHarvestersForInspection records the live harvesters for the debug
// surface. Called from the same boot site as the gone-forget hook, with the
// same instances the engine seeds from, so what the surface reports is exactly
// what the seed will read.
func publishHarvestersForInspection(nav *navWidgetHarvester, apiRef *contentPrewarmHarvester) {
	if nav == nil && apiRef == nil {
		return
	}
	liveHarvesters.Store(&harvestInspectSource{nav: nav, apiRef: apiRef})
}

// HarvestCounts returns how many entries each harvester currently holds.
//
// navEntries counts harvested ENTRIES, not distinct widgets: one widget is
// held once per pagination tuple it was reached under, and the seed writes a
// cell per entry, so entries is the number that matters. apiRefs counts
// distinct RESTAction references (deduped by namespace/name).
//
// ok is false when no harvester has been published (prewarm off, or boot has
// not reached the engine yet).
func HarvestCounts() (navEntries, apiRefs int, ok bool) {
	src := liveHarvesters.Load()
	if src == nil {
		return 0, 0, false
	}
	return len(src.nav.snapshot()), len(src.apiRef.snapshot()), true
}

// HarvestHoldsCoordinate reports whether each harvester still holds the given
// coordinate — the direct answer to "did the forget land?".
//
// The nav answer matches on the harvested CR's own GVR + namespace + name, so
// it is true if ANY pagination tuple for that widget is still held. The apiRef
// answer matches the RESTAction reference by namespace/name and is only ever
// true for the RESTAction GVR, which is the only kind that harvester holds.
//
// ok is false when no harvester has been published; the two booleans are then
// meaningless and must not be read as "not held".
func HarvestHoldsCoordinate(gvr schema.GroupVersionResource, namespace, name string) (nav, apiRef, ok bool) {
	src := liveHarvesters.Load()
	if src == nil {
		return false, false, false
	}
	for _, e := range src.nav.snapshot() {
		if e.GVR == gvr && e.W != nil && e.W.GetNamespace() == namespace && e.W.GetName() == name {
			nav = true
			break
		}
	}
	if gvr == restActionGVR {
		for _, r := range src.apiRef.snapshot() {
			if r.Namespace == namespace && r.Name == name {
				apiRef = true
				break
			}
		}
	}
	return nav, apiRef, true
}

// ResetHarvestInspectionForTest clears the published harvesters.
func ResetHarvestInspectionForTest() { liveHarvesters.Store(nil) }

// HarvestCoordinateForTest is a plain coordinate a cross-package test can
// build without reaching into the unexported harvester types.
type HarvestCoordinateForTest struct {
	GVR       schema.GroupVersionResource
	Namespace string
	Name      string
}

// PublishHarvestersForInspectionForTest stands the surface up from plain
// coordinates, so a handler test in another package can exercise the real
// accessors without booting the prewarm engine. Production publishes from the
// boot site; this only fills the same pointer.
func PublishHarvestersForInspectionForTest(navWidgets []HarvestCoordinateForTest, apiRefs []HarvestCoordinateForTest) {
	nav := newNavWidgetHarvester()
	for _, c := range navWidgets {
		u := &unstructured.Unstructured{}
		u.SetNamespace(c.Namespace)
		u.SetName(c.Name)
		u.SetGroupVersionKind(schema.GroupVersionKind{Group: c.GVR.Group, Version: c.GVR.Version, Kind: "Flex"})
		nav.harvestNavWidget(u, c.GVR, -1, -1, -1, -1)
	}
	content := newContentPrewarmHarvester()
	for _, c := range apiRefs {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
			"kind":       "Flex",
			"metadata":   map[string]any{"name": "carrier", "namespace": c.Namespace},
			"spec": map[string]any{
				"apiRef": map[string]any{"name": c.Name, "namespace": c.Namespace},
			},
		}}
		content.harvestApiRef(u)
	}
	publishHarvestersForInspection(nav, content)
}
