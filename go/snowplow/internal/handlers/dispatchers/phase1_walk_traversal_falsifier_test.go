// phase1_walk_traversal_falsifier_test.go — Ship 0.30.127 falsifier for
// the Phase-1 discovery-walk traversal fix.
//
// The full root cause of F2's warmed=2 is the phase1IteratorCap
// truncation (proven hermetically against the REAL createRequestOptions
// in internal/resolvers/restactions/api/phase1_itercap_falsifier_test.go
// — pre-fix the cap drops krateo-system, post-fix the iterator expands
// fully and reaches it). This file covers the dispatchers-side pieces of
// the 0.30.127 fix that the api-package falsifier cannot:
//
//   - the SLICE fix: the walk derives a child's pagination from its
//     `/call` Path with prewarmPaginationFor — the same query parser
//     (paginationFromQuery) the dispatcher applies to the browser's request
//     of that URL — so a declared slice reaches widgets.Resolve AND the cell
//     key;
//   - (#966) a child whose Path carries no slice is walked UNPAGINATED, as
//     the browser requests it. The bounded PREWARM_PAGE_LIMIT default that
//     0.30.127 applied here stored a perPage=5 body under the unpaginated
//     key; it is retired;
//   - the harvester collecting an apiRef (compositions-list) once the
//     walk descends to the widget that declares it.
//
// phase1Walker.walk drives widgets.Resolve + objects.Get (a live
// apiserver) so it cannot run hermetically end-to-end; these tests pin
// the new mechanism at the smallest faithful seam.

package dispatchers

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// dashboardTablePath is the `/call` Path of the Dashboard's
// compositions table — the widget that carries spec.apiRef:
// compositions-list. The Dashboard chain declares slice{page:1,perPage:5}
// (page.compositions-page / dashboard widgets), so the resolved child
// Path carries page=1&perPage=5.
const dashboardTablePath = "/call?resource=tables&apiVersion=widgets.templates.krateo.io/v1beta1" +
	"&name=dashboard-compositions-panel-row-table&namespace=krateo-system&page=1&perPage=5"

// noSlicePath is a `/call` Path with no page/perPage — a widget whose ref
// declares no slice. The browser requests it unpaginated, so the walk
// resolves and keys it unpaginated (#966).
const noSlicePath = "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1beta1" +
	"&name=dashboard-compositions-panel&namespace=krateo-system"

// TestFAL127_SliceExtractedFromChildPath proves the SLICE fix: the walk
// reads the page/perPage a child `/call` Path carries (prewarmPaginationFor,
// the derivation every walk writer uses), so it honours the widget's DECLARED
// pagination.
func TestFAL127_SliceExtractedFromChildPath(t *testing.T) {
	pg := prewarmPaginationFor(dashboardTablePath)
	if !pg.paginated() {
		t.Fatalf("FAL-127: the Dashboard table's /call Path declares page=1&perPage=5 — "+
			"prewarmPaginationFor must extract it, got the unpaginated %+v", pg)
	}
	if pg.Page != 1 || pg.PerPage != 5 {
		t.Fatalf("FAL-127: extracted page/perPage = %d/%d, want 1/5 (the declared slice)",
			pg.Page, pg.PerPage)
	}
}

// TestPrewarmPagination966_NoSliceIsUnpaginated pins the #966 replacement of
// the bounded PREWARM_PAGE_LIMIT default: a child Path with no declared slice
// gives the walk the UNPAGINATED pagination — what paginationInfo derives for
// the browser's request of that URL — and a PREWARM_PAGE_LIMIT left in the
// environment changes nothing (it is a retired flag).
func TestPrewarmPagination966_NoSliceIsUnpaginated(t *testing.T) {
	t.Setenv("PREWARM_PAGE_LIMIT", "25")
	for _, path := range []string{noSlicePath, ""} {
		if got := prewarmPaginationFor(path); got != unpaginatedPrewarm || got.paginated() {
			t.Fatalf("#966: prewarmPaginationFor(%q) = %+v, want the unpaginated %+v — a URL with no "+
				"page/perPage is requested unpaginated, so its cell must be resolved unpaginated", path, got, unpaginatedPrewarm)
		}
	}
	if got := prewarmPaginationFor(dashboardTablePath); got != (prewarmPagination{PerPage: 5, Page: 1}) || !got.paginated() {
		t.Fatalf("#966: a declared slice must give exactly that pagination, got %+v", got)
	}
}

// TestFAL127_HarvesterCollectsCompositionsList verifies ONE narrow
// property: given a Dashboard Table widget CR, harvestApiRef records its
// spec.apiRef (compositions-list / blueprints-list) into the
// content-prewarm data-source set by name.
//
// SCOPE — what this test does NOT prove (PM directive): it HAND-FEEDS the
// Table widgets to harvestApiRef. It does NOT exercise phase1Walker.walk
// and therefore does NOT prove the walk TRAVERSES the navigation tree
// down to the Dashboard table — which is the actual 0.30.127 bug. The
// walk drives widgets.Resolve + objects.Get (a live apiserver) and
// cannot run hermetically. The real warmed-SET-by-name acceptance
// criterion — `compositions-list` AND `blueprints-list` present in the
// harvested/warmed set — is an ON-CLUSTER AC, verified post-deploy from
// the `content.prewarm.completed` log's warmed set. This hermetic suite
// pins the mechanism's pieces (iterator expansion in the api-package
// falsifier; slice extraction + this recording check); it does NOT, on
// its own, satisfy the warmed-set AC.
func TestFAL127_HarvesterCollectsCompositionsList(t *testing.T) {
	h := newContentPrewarmHarvester()

	// The Dashboard compositions table CR — spec.apiRef → compositions-list.
	dashTable := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Table",
		"metadata": map[string]any{
			"namespace": "krateo-system",
			"name":      "dashboard-compositions-panel-row-table",
		},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": "compositions-list", "namespace": "krateo-system"},
		},
	}}
	// The Dashboard blueprints table CR — spec.apiRef → blueprints-list.
	blueprintsTable := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Table",
		"metadata": map[string]any{
			"namespace": "krateo-system",
			"name":      "dashboard-blueprints-panel-row-table",
		},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": "blueprints-list", "namespace": "krateo-system"},
		},
	}}

	h.harvestApiRef(dashTable)
	h.harvestApiRef(blueprintsTable)

	got := map[string]bool{}
	for _, ref := range h.snapshot() {
		got[ref.Namespace+"/"+ref.Name] = true
	}
	// Recording check ONLY — given the two Table widgets, harvestApiRef
	// records both apiRefs by name. This is NOT the warmed-set AC (which
	// is on-cluster — see the function doc): it pins that harvestApiRef
	// reads spec.apiRef.{name,namespace} correctly for a Table widget.
	if !got["krateo-system/compositions-list"] {
		t.Fatalf("FAL-127: harvestApiRef must record compositions-list from the "+
			"Dashboard table widget's spec.apiRef; got %v", got)
	}
	if !got["krateo-system/blueprints-list"] {
		t.Fatalf("FAL-127: harvestApiRef must record blueprints-list from the "+
			"Dashboard blueprints table widget's spec.apiRef; got %v", got)
	}
}
