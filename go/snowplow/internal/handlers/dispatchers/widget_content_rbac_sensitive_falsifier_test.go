// widget_content_rbac_sensitive_falsifier_test.go — task #69 falsifier for
// the RBAC-sensitivity routing fix (closes a cross-user leak class).
//
// THE LEAK CLASS. The identity-free `widgetContent` L1 cell is shared
// across users (keyed by widget+pagination, NOT identity). The serve-time
// gate (gateWidgetEnvelope) only re-derives
// status.resourcesRefs.items[].allowed per requester — it NEVER narrows
// status.widgetData. So a dashboard piechart/table that renders ENTIRELY
// from status.widgetData (series.total, data=${.list}) computed by an
// apiRef RA that aggregates cross-namespace would serve EVERY user the
// SA-maximal full aggregate → cross-user leak.
//
// THE FIX. isRBACSensitiveApiRefWidget classifies apiRef-driven
// render-template widgets (SHAPE-based, no widget-name/GVR literal). At
// serve time the entire identity-free `widgetContent` lookup block is
// skipped for a classified widget — control falls through to the
// per-cohort `widgets` L1 (dispatchCacheLookupKey), which is RBAC-correct
// by construction (each cohort resolves the apiRef RA under its OWN
// identity → narrowed at resolve; no shared cell, no serve-gate, no leak).
// At boot/populate, populateWidgetContentL1 early-returns for a classified
// widget so the identity-free cell is NEVER written.
//
// Self-validated in-process (NEVER `go test ./internal/rbac/...` against
// the live kubeconfig — destructive). The LIVE falsifier (admin renders
// 39,560 / cyberjoker renders 0; raw bytes widgetData.series.total==0 for
// cyberjoker = no leak) runs post-deploy by the tester.
//
//   - TestRBACSensitive_PredicateTruthTable
//   - TestRBACSensitive_PopulateSkipsClassifiedWidget (counter + no entry)
//   - TestRBACSensitive_PopulateStillStoresUnclassified (no over-fire)
//   - TestRBACSensitive_ClassificationSupersedesEmptyShellGuard

package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// widgetWithTemplates builds a widget CR `.Object` map with the declared
// spec shape the predicate reads: apiRef name presence, a
// spec.widgetDataTemplate (the piechart/table render path), and a
// spec.resourcesRefsTemplate (the datagrid render path). `items` controls
// the resolved status.resourcesRefs.items count so the empty-shell-guard
// interaction can be exercised independently of classification.
func widgetWithTemplates(apiRefName string, hasWidgetDataTpl, hasResourcesRefsTpl bool, items int) *unstructured.Unstructured {
	spec := map[string]any{}
	if apiRefName != "" {
		spec["apiRef"] = map[string]any{"name": apiRefName, "namespace": "krateo-system"}
	}
	if hasWidgetDataTpl {
		spec["widgetDataTemplate"] = []any{
			map[string]any{"forPath": "series.total", "expression": ".total"},
		}
	}
	if hasResourcesRefsTpl {
		spec["resourcesRefsTemplate"] = []any{
			map[string]any{
				"iterator": ".compositionspanels",
				"template": map[string]any{
					"id":         "${ .metadata.name }",
					"apiVersion": "widgets.templates.krateo.io/v1beta1",
					"resource":   "panels",
					"namespace":  "${ .metadata.namespace }",
					"name":       "${ .metadata.name }",
					"verb":       "GET",
				},
			},
		}
	}
	refItems := make([]any, 0, items)
	for i := 0; i < items; i++ {
		refItems = append(refItems, map[string]any{
			"id": "panel", "path": "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1beta1&namespace=bench-ns-01&name=p", "verb": "GET", "allowed": true,
		})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "PieChart",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "dashboard-piechart"},
		"spec":       spec,
		"status": map[string]any{
			"resourcesRefs": map[string]any{"items": refItems},
		},
	}}
}

// TestRBACSensitive_PredicateTruthTable pins the classification predicate.
// True IFF a non-empty spec.apiRef AND at least one render template
// (widgetDataTemplate OR resourcesRefsTemplate).
func TestRBACSensitive_PredicateTruthTable(t *testing.T) {
	cases := []struct {
		name           string
		apiRefName     string
		hasWidgetData  bool
		hasResRefs     bool
		wantClassified bool
	}{
		// apiRef + widgetDataTemplate — the dashboard piechart/table (the
		// leak class: renders from status.widgetData, never gate-narrowed).
		{"apiRef+widgetDataTemplate (piechart/table)", "compositions-list", true, false, true},
		// apiRef + resourcesRefsTemplate — the datagrid.
		{"apiRef+resourcesRefsTemplate (datagrid)", "compositions-panels", false, true, true},
		// apiRef + both — classified.
		{"apiRef+both templates", "compositions-list", true, true, true},
		// no apiRef + a template — NOT classified (identity-invariant static
		// widget; keeps using the identity-free layer).
		{"no-apiRef + widgetDataTemplate (static)", "", true, false, false},
		{"no-apiRef + resourcesRefsTemplate (static)", "", false, true, false},
		// apiRef + NO template — NOT classified (e.g. a widget whose apiRef
		// only drives metadata, not the rendered output).
		{"apiRef + no template", "compositions-list", false, false, false},
		// pure-static widget — NOT classified.
		{"no-apiRef + no template (static)", "", false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := widgetWithTemplates(c.apiRefName, c.hasWidgetData, c.hasResRefs, 0).Object
			if got := isRBACSensitiveApiRefWidget(obj); got != c.wantClassified {
				t.Fatalf("isRBACSensitiveApiRefWidget(%s) = %v; want %v", c.name, got, c.wantClassified)
			}
		})
	}
	// Nil-safety.
	if isRBACSensitiveApiRefWidget(nil) {
		t.Fatalf("isRBACSensitiveApiRefWidget(nil) must be false")
	}
}

// widgetWithInlineSpecRef builds a widget CR `.Object` whose SPEC declares one
// resourcesRefs item, optionally inline + with the given verb, and optionally
// an apiRef. This is the C-INLINE-1 (#72) classification surface: the predicate
// reads spec.resourcesRefs.items[] for an inline GET ref.
func widgetWithInlineSpecRef(inline bool, verb, apiRefName string) map[string]any {
	spec := map[string]any{
		"resourcesRefs": map[string]any{
			"items": []any{
				map[string]any{
					"id": "child", "apiVersion": "widgets.templates.krateo.io/v1beta1",
					"resource": "panels", "namespace": "demo", "name": "child-1",
					"inline": inline, "verb": verb,
				},
			},
		},
	}
	if apiRefName != "" {
		spec["apiRef"] = map[string]any{"name": apiRefName, "namespace": "krateo-system"}
	}
	return map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "detail-card"},
		"spec":       spec,
	}
}

// TestRBACSensitive_InlineGETRef_C_INLINE_1 — the #72 C-INLINE-1 arm: a widget
// bearing an inline GET resourcesRefs ref MUST classify RBAC-sensitive so its
// embedded `rendered` child never lands in the shared identity-free
// widgetContent cell. The discriminating case is the NO-apiRef inline widget:
// it must STILL classify true — which only holds because hasInlineGETRef is an
// OR-clause BEFORE the apiRef.Name=="" short-circuit. (RED arm: move the clause
// AFTER the short-circuit → this case returns false → shared cell → leak.)
func TestRBACSensitive_InlineGETRef_C_INLINE_1(t *testing.T) {
	cases := []struct {
		name           string
		inline         bool
		verb           string
		apiRefName     string
		wantClassified bool
	}{
		// THE discriminator: inline GET ref, NO apiRef → MUST be classified
		// (only true if the OR-clause runs before the apiRef short-circuit).
		{"no-apiRef + inline GET ref (C-INLINE-1 discriminator)", true, "GET", "", true},
		// inline GET ref WITH apiRef → classified (both clauses agree).
		{"apiRef + inline GET ref", true, "GET", "compositions-list", true},
		// inline ref but NON-GET → not an inline-render candidate; no apiRef/tpl
		// → not classified.
		{"no-apiRef + inline POST ref", true, "POST", "", false},
		// non-inline GET ref, no apiRef → static widget, not classified
		// (byte-identical to today; the inline clause does not over-fire).
		{"no-apiRef + non-inline GET ref", false, "GET", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := widgetWithInlineSpecRef(c.inline, c.verb, c.apiRefName)
			if got := isRBACSensitiveApiRefWidget(obj); got != c.wantClassified {
				t.Fatalf("isRBACSensitiveApiRefWidget(%s) = %v; want %v "+
					"(C-INLINE-1: an inline GET ref must route to the per-user widgets L1, never the shared cell)",
					c.name, got, c.wantClassified)
			}
		})
	}
}

// TestRBACSensitive_ServeRoutingDecision — the serve path gates the ENTIRE
// identity-free `widgetContent` lookup block on
// `!isRBACSensitiveApiRefWidget(got.Unstructured.Object)` (widgets.go). This
// asserts the routing decision over a FETCHED-CR-shaped object (the exact
// shape `got.Unstructured` carries at serve time: apiVersion/kind/metadata/
// spec — populated by fetchObject BEFORE Resolve, so the predicate runs
// pre-resolve with no extra apiserver round-trip). A `true` verdict means
// the identity-free lookup is SKIPPED and control falls through to the
// per-cohort `widgets` L1 (RBAC-narrowed); `false` means the identity-free
// path is taken (unchanged pre-fix behaviour).
func TestRBACSensitive_ServeRoutingDecision(t *testing.T) {
	// A fetched widget CR carries spec but NO resolved status yet (Resolve
	// has not run). The predicate keys only on spec, so an absent status is
	// irrelevant — assert that explicitly.
	classified := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "PieChart",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "dashboard-piechart"},
		"spec": map[string]any{
			"apiRef":             map[string]any{"name": "compositions-list", "namespace": "krateo-system"},
			"widgetDataTemplate": []any{map[string]any{"forPath": "series.total", "expression": ".total"}},
		},
		// No status — pre-resolve shape.
	}}
	if !isRBACSensitiveApiRefWidget(classified.Object) {
		t.Fatalf("SERVE ROUTING: an apiRef+widgetDataTemplate piechart (pre-resolve, no status) "+
			"MUST classify → identity-free lookup SKIPPED → per-cohort widgets L1")
	}

	// An unclassified static widget (no apiRef) MUST keep using the
	// identity-free path.
	unclassified := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "nav-panel"},
		"spec":       map[string]any{}, // no apiRef, no template
	}}
	if isRBACSensitiveApiRefWidget(unclassified.Object) {
		t.Fatalf("SERVE ROUTING: a static widget (no apiRef) MUST NOT classify → "+
			"keeps using the identity-free widgetContent layer")
	}
}

// TestRBACSensitive_PopulateSkipsClassifiedWidget — the boot/populate path
// MUST NOT write the identity-free cell for a classified widget, and the
// RBAC-sensitivity guard counter advances.
func TestRBACSensitive_PopulateSkipsClassifiedWidget(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	RegisterWidgetContentMetricsForTest()

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "piecharts"}
	in := newUnstructuredWidget("krateo-system", "dashboard-piechart")
	// Classified AND populated (5 items) — proves classification supersedes
	// any "populated, so store it" path: a classified widget is NEVER
	// stored, even with a full resolved list.
	res := widgetWithTemplates("compositions-list", true, false, 5)

	beforeStore := cache.ResolvedCache().Stats().WidgetContentStoreTotal
	beforeSkip := widgetContentSkippedRBACSensitiveTotal.Load()

	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	if got := cache.ResolvedCache().Stats().WidgetContentStoreTotal; got != beforeStore {
		t.Fatalf("LEAK GUARD FAIL: a classified RBAC-sensitive widget was STORED into the "+
			"identity-free cell (WidgetContentStoreTotal %d -> %d). It renders from "+
			"status.widgetData, which the serve-gate never narrows → cross-user leak.",
			beforeStore, got)
	}
	if got := widgetContentSkippedRBACSensitiveTotal.Load(); got != beforeSkip+1 {
		t.Fatalf("RBAC-sensitivity skip counter did not advance (%d -> %d)", beforeSkip, got)
	}

	key := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: "krateo-system", Name: "dashboard-piechart",
		PerPage: 5, Page: 1,
	})
	if _, hit := cache.ResolvedCache().Get(key); hit {
		t.Fatalf("LEAK GUARD FAIL: an identity-free entry was created for a classified widget")
	}
}

// TestRBACSensitive_PopulateStillStoresUnclassified — an unclassified
// widget (e.g. apiRef present but NO render template, or no apiRef) that
// resolved with a populated list is STILL stored into the identity-free
// cell. The guard must not over-fire and strand the identity-free fast
// path for widgets that are safe to share.
func TestRBACSensitive_PopulateStillStoresUnclassified(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
	in := newUnstructuredWidget("krateo-system", "nav-panel")
	// apiRef present but NO render template → unclassified. Populated list
	// (3 items) so the empty-shell guard does not fire either.
	res := widgetWithTemplates("some-action-ra", false, false, 3)
	res.Object["metadata"] = map[string]any{"namespace": "krateo-system", "name": "nav-panel"}

	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	key := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: "krateo-system", Name: "nav-panel",
		PerPage: 5, Page: 1,
	})
	if _, hit := cache.ResolvedCache().Get(key); !hit {
		t.Fatalf("an UNCLASSIFIED widget (no render template) MUST still be stored into the "+
			"identity-free cell; the RBAC-sensitivity guard over-fired")
	}
}

// TestRBACSensitive_ClassificationSupersedesEmptyShellGuard — a classified
// widget that ALSO matches the empty-shell shape (apiRef +
// resourcesRefsTemplate + empty resolved items) is skipped via the
// RBAC-sensitivity guard, NOT the empty-shell guard. The RBAC counter
// advances; the empty-shell counter does NOT (classification runs first
// because it supersedes — a classified widget never touches the cell, empty
// or populated). This proves the ordering is correct and the two guards do
// not double-count.
func TestRBACSensitive_ClassificationSupersedesEmptyShellGuard(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	RegisterWidgetContentMetricsForTest()

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "datagrids"}
	in := newUnstructuredWidget("krateo-system", "compositions-page-datagrid")
	// apiRef + resourcesRefsTemplate + empty resolved items: matches BOTH
	// the RBAC-sensitivity classification AND the empty-shell poison shape.
	res := widgetWithTemplates("compositions-panels", false, true, 0)
	res.Object["metadata"] = map[string]any{"namespace": "krateo-system", "name": "compositions-page-datagrid"}

	beforeRBAC := widgetContentSkippedRBACSensitiveTotal.Load()
	beforeEmpty := widgetContentSkippedEmptyShellTotal.Load()

	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	if got := widgetContentSkippedRBACSensitiveTotal.Load(); got != beforeRBAC+1 {
		t.Fatalf("expected the RBAC-sensitivity guard to fire FIRST (%d -> %d)", beforeRBAC, got)
	}
	if got := widgetContentSkippedEmptyShellTotal.Load(); got != beforeEmpty {
		t.Fatalf("the empty-shell guard must NOT also fire (double-count); (%d -> %d)", beforeEmpty, got)
	}
}
