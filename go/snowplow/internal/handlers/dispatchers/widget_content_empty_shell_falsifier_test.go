// widget_content_empty_shell_falsifier_test.go — Ship 1.3 falsifier for
// the identity-free widget-content empty-poison fix (lever 1 + lever 2).
//
// THE DEFECT. admin's compositions datagrid rendered EMPTY (0 panels)
// although 39,560 compositions-panels exist. The identity-free widgetContent
// cell the frontend hits at (perPage=5, page=1) was POISONED with an empty
// envelope at boot (the apiRef RA was transiently empty during the SA walk),
// and the refresher RE-POISONED it every cycle because it re-resolved the
// identity-free cell under the EMPTY RepresentativeUsername → CohortNSACL("")
// → permitAll=false → status.resourcesRefs.items dropped to 0 → empty
// re-stored. >3,100 refresher cycles never corrected it.
//
// THE FIX, two levers, both falsified here in-process (no remote cluster —
// never `go test ./internal/rbac/...` against the live kubeconfig):
//
//   LEVER 2 (durable). resolveAndPopulateL1 refreshes the identity-free
//   classes (widgetContent / apistage) under the SA CANONICAL identity
//   (phase1SAUsername — Ship 1.1 made the SA's CohortNSACL `*/*`
//   permitAll=true) instead of the empty representative tuple. The
//   refresher then POPULATES the SA-maximal shell (full resourcesRefs.items)
//   rather than re-poisoning it. The serve-time gate (gateWidgetEnvelope)
//   narrows per-user via the `allowed` flag — admin true, cyberjoker false.
//     - TestLever2_WidgetContentRefreshUsesSACanonicalIdentity
//     - TestLever2_ApistageRefreshUsesSACanonicalIdentity
//     - TestLever2_PerCohortClassesStillUseRepresentativeIdentity (regression
//       guard — the leak invariant: widgets/restactions/RAFullList MUST NOT
//       get the SA override; they stay per-cohort, RBAC-narrowed)
//     - TestLever2_NilSAFallsBackToRepresentative (graceful degradation)
//     - TestIsIdentityFreeClass (the predicate's truth table)
//
//   LEVER 1 (defense). populateWidgetContentL1 refuses to store a transient-
//   empty POISON shell: an apiRef+resourcesRefsTemplate widget that resolved
//   with empty status.resourcesRefs.items. Keyed ONLY on resourcesRefs.items
//   (Diego directive — status.widgetData.items is NEVER a data signal).
//     - TestLever1_ShouldSkipEmptyWidgetShell (truth table)
//     - TestLever1_PopulateSkipsEmptyPoisonShell (integration + counter)
//     - TestLever1_PopulateDoesNotStorePopulatedDatagrid (task #69: a
//       populated apiRef+template datagrid is RBAC-sensitive → NOT stored
//       into the identity-free cell; routed to the per-cohort widgets L1)

package dispatchers

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// canonicalSAUsernameForTest is the canonical username phase1RealSAToken's
// JWT `sub` claim decodes to — see phase1_sa_username_test.go.
const canonicalSAUsernameForTest = "system:serviceaccount:krateo-system:snowplow"

// captureRefreshIdentity drives resolveAndPopulateL1 for `inputs` under the
// given SA pair and returns the (username, groups) the re-resolve seam was
// handed on its context. A pre-existing entry is seeded so the post-resolve
// liveness check passes.
func captureRefreshIdentity(t *testing.T, inputs cache.ResolvedKeyInputs, saEP *endpoints.Endpoint, saRC *rest.Config) (string, []string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	c := cache.ResolvedCache()
	key := cache.ComputeKey(inputs)
	c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"stale":1}`), Inputs: &inputs})

	var gotUser string
	var gotGroups []string
	restore := setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		if ui, err := xcontext.UserInfo(ctx); err == nil {
			gotUser = ui.Username
			gotGroups = ui.Groups
		}
		return []byte(`{"fresh":1}`), nil
	})
	t.Cleanup(restore)

	if err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC); err != nil {
		t.Fatalf("resolveAndPopulateL1 error: %v", err)
	}
	return gotUser, gotGroups
}

// --- LEVER 2 -------------------------------------------------------------

// TestLever2_WidgetContentRefreshUsesSACanonicalIdentity is the headline
// lever-2 falsifier. A widgetContent entry carries NO representative
// identity (identity-free class). Pre-1.3 the refresh ran as the empty
// username → CohortNSACL("") → permitAll=false → re-poison. Post-1.3 the
// refresh MUST run under the SA canonical username so CohortNSACL yields
// `*/*` permitAll=true and the cell is POPULATED, not re-poisoned.
func TestLever2_WidgetContentRefreshUsesSACanonicalIdentity(t *testing.T) {
	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "datagrids",
		Namespace:       "krateo-system",
		Name:            "compositions-page-datagrid",
		PerPage:         5,
		Page:            1,
		// RepresentativeUsername/Groups intentionally EMPTY — the poison
		// condition the fix corrects.
	}
	saEP := &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: phase1RealSAToken}
	saRC := &rest.Config{Host: "https://kubernetes.default.svc"}

	gotUser, gotGroups := captureRefreshIdentity(t, inputs, saEP, saRC)

	if gotUser != canonicalSAUsernameForTest {
		t.Fatalf("LEVER 2 FAIL: widgetContent refresh ran as %q; want the SA canonical "+
			"identity %q. An empty username drives CohortNSACL(\"\") -> permitAll=false -> "+
			"re-poison (the >3,100-cycle defect).", gotUser, canonicalSAUsernameForTest)
	}
	// SA identity is username-only (mirrors withPhase1SAContext) — no groups.
	if len(gotGroups) != 0 {
		t.Fatalf("LEVER 2: SA refresh identity must be username-only (the SA grant lands "+
			"via its ServiceAccount-kind binding, matched by username); got groups=%v", gotGroups)
	}
}

// TestLever2_ApistageRefreshUsesSACanonicalIdentity — the apistage cell is
// the OTHER identity-free class (per-K8s-call content), served through its
// own per-user gate (gateContentEnvelope / filterListByRBAC). Its refresh
// MUST likewise run under the SA canonical identity.
func TestLever2_ApistageRefreshUsesSACanonicalIdentity(t *testing.T) {
	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassApistage,
		Group:           "composition.krateo.io",
		Version:         "v1",
		Resource:        "compositions",
		Namespace:       "",
		Name:            "",
		Stage:           "stage-0|filterhash|dicthash",
	}
	saEP := &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: phase1RealSAToken}
	saRC := &rest.Config{Host: "https://kubernetes.default.svc"}

	gotUser, _ := captureRefreshIdentity(t, inputs, saEP, saRC)

	if gotUser != canonicalSAUsernameForTest {
		t.Fatalf("LEVER 2 FAIL: apistage refresh ran as %q; want the SA canonical identity %q",
			gotUser, canonicalSAUsernameForTest)
	}
}

// TestLever2_PerBindingClassesStillUseRepresentativeIdentity is the LEAK
// REGRESSION GUARD. The per-binding identity-bound classes (widgets /
// restactions / RAFullList) are keyed by BindingUID and MUST refresh
// under the binding's REPRESENTATIVE identity — NEVER the SA override.
// If lever 2 leaked into these classes, a per-binding cell would be
// re-resolved with SA-maximal visibility and then served (no per-user
// gate on the per-binding widgets class) → cross-binding data leak
// (feedback_l1_per_user_keyed_never_cohort). This guards that boundary.
//
// Ship 0.30.242 H.c-layered Phase 2c — renamed from
// PerCohortClassesStillUseRepresentativeIdentity to reflect the
// per-binding granularity that replaced per-cohort keying. Test body
// semantics unchanged; field rename from BindingSetHash → BindingUID.
func TestLever2_PerBindingClassesStillUseRepresentativeIdentity(t *testing.T) {
	saEP := &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: phase1RealSAToken}
	saRC := &rest.Config{Host: "https://kubernetes.default.svc"}

	for _, class := range []string{"widgets", "restactions", cache.CacheEntryClassRAFullList} {
		t.Run(class, func(t *testing.T) {
			inputs := cache.ResolvedKeyInputs{
				CacheEntryClass:        class,
				Group:                  "widgets.templates.krateo.io",
				Version:                "v1beta1",
				Resource:               "panels",
				Namespace:              "krateo-system",
				Name:                   "some-panel",
				BindingUID:             "uid-c01dface",
				RepresentativeUsername: "cyberjoker",
				RepresentativeGroups:   []string{"devs"},
			}
			gotUser, gotGroups := captureRefreshIdentity(t, inputs, saEP, saRC)
			if gotUser != "cyberjoker" {
				t.Fatalf("LEAK GUARD FAIL: per-cohort class %q refreshed as %q; want the "+
					"cohort representative %q. The SA override MUST be confined to the "+
					"identity-free (gated) classes — leaking it here is a cross-cohort "+
					"data-leak (feedback_l1_per_user_keyed_never_cohort).",
					class, gotUser, "cyberjoker")
			}
			if len(gotGroups) != 1 || gotGroups[0] != "devs" {
				t.Fatalf("LEAK GUARD: per-cohort class %q lost its representative groups; got %v",
					class, gotGroups)
			}
		})
	}
}

// TestLever2_NilSAFallsBackToRepresentative — graceful degradation: with no
// SA endpoint (unit test / outside-cluster) an identity-free refresh falls
// back to the (empty) representative tuple, the unchanged pre-1.3 posture.
// The fix never invents an identity when the SA token is unavailable.
func TestLever2_NilSAFallsBackToRepresentative(t *testing.T) {
	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "datagrids",
		Namespace:       "krateo-system",
		Name:            "compositions-page-datagrid",
		PerPage:         5,
		Page:            1,
	}
	// nil SA pair.
	gotUser, _ := captureRefreshIdentity(t, inputs, nil, nil)
	if gotUser != "" {
		t.Fatalf("nil SA endpoint must fall back to the (empty) representative identity; got %q", gotUser)
	}
}

// TestLever2_SATokenAbsentFallsBackToRepresentative — a non-nil SA endpoint
// whose token is empty/non-canonical also falls back (phase1SAUsername
// returns ok=false). Belt-and-suspenders for the degraded posture.
func TestLever2_SATokenAbsentFallsBackToRepresentative(t *testing.T) {
	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "datagrids",
		Namespace:       "krateo-system",
		Name:            "compositions-page-datagrid",
		PerPage:         5,
		Page:            1,
	}
	saEP := &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: ""}
	gotUser, _ := captureRefreshIdentity(t, inputs, saEP, &rest.Config{})
	if gotUser != "" {
		t.Fatalf("empty/non-canonical SA token must fall back to the representative identity; got %q", gotUser)
	}
}

// TestIsIdentityFreeClass pins the predicate's truth table — the single
// source of truth for which classes get the lever-2 SA override.
func TestIsIdentityFreeClass(t *testing.T) {
	cases := []struct {
		class string
		want  bool
	}{
		{cache.CacheEntryClassWidgetContent, true},
		{cache.CacheEntryClassApistage, true},
		{"widgets", false},
		{"restactions", false},
		{cache.CacheEntryClassRAFullList, false},
		{"", false},
	}
	for _, c := range cases {
		if got := isIdentityFreeClass(c.class); got != c.want {
			t.Errorf("isIdentityFreeClass(%q) = %v; want %v", c.class, got, c.want)
		}
	}
}

// --- LEVER 1 -------------------------------------------------------------

// widgetWithShape builds a resolved widget envelope with the declared spec
// shape (apiRef name, resourcesRefsTemplate presence) and the resolved
// status.resourcesRefs.items count the test wants.
func widgetWithShape(apiRefName string, hasTemplate bool, resourcesRefsItems int) *unstructured.Unstructured {
	spec := map[string]any{}
	if apiRefName != "" {
		spec["apiRef"] = map[string]any{"name": apiRefName, "namespace": "krateo-system"}
	}
	if hasTemplate {
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
	items := make([]any, 0, resourcesRefsItems)
	for i := 0; i < resourcesRefsItems; i++ {
		items = append(items, map[string]any{
			"id": "panel", "path": "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1beta1&namespace=bench-ns-01&name=p", "verb": "GET", "allowed": true,
		})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "DataGrid",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "compositions-page-datagrid"},
		"spec":       spec,
		"status": map[string]any{
			"resourcesRefs": map[string]any{"items": items},
		},
	}}
}

// TestLever1_ShouldSkipEmptyWidgetShell is the predicate truth table. The
// guard fires ONLY for the poison SHAPE: apiRef + resourcesRefsTemplate +
// empty resolved status.resourcesRefs.items.
func TestLever1_ShouldSkipEmptyWidgetShell(t *testing.T) {
	cases := []struct {
		name       string
		apiRefName string
		hasTpl     bool
		items      int
		wantSkip   bool
	}{
		// THE POISON SHAPE — the defect: apiRef+template-driven, resolved empty.
		{"poison: apiRef+tpl+empty", "compositions-panels", true, 0, true},
		// Populated — the corrected cell — MUST be stored.
		{"populated: apiRef+tpl+5items", "compositions-panels", true, 5, false},
		// No apiRef — an empty result is authoritative, NOT a poison shape.
		{"no apiRef + tpl + empty", "", true, 0, false},
		// No template — list built only from static spec.resourcesRefs;
		// empty is authoritative.
		{"apiRef + no tpl + empty", "compositions-panels", false, 0, false},
		// No apiRef, no template, empty — a pure-static widget — preserved.
		{"static-only + empty", "", false, 0, false},
		// apiRef + tpl but non-empty (1 item) — populated, preserved.
		{"apiRef+tpl+1item", "compositions-panels", true, 1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := widgetWithShape(c.apiRefName, c.hasTpl, c.items)
			if got := shouldSkipEmptyWidgetShell(res); got != c.wantSkip {
				t.Fatalf("shouldSkipEmptyWidgetShell(%s) = %v; want %v", c.name, got, c.wantSkip)
			}
		})
	}
	// Nil-safety.
	if shouldSkipEmptyWidgetShell(nil) {
		t.Fatalf("shouldSkipEmptyWidgetShell(nil) must be false")
	}
}

// TestLever1_PopulateSkipsEmptyPoisonShell — integration: the boot-walk Put
// site MUST NOT store the poison shell.
//
// task #69 RECONCILIATION. The fixture is an apiRef + resourcesRefsTemplate
// datagrid. That shape is now classified RBAC-sensitive
// (isRBACSensitiveApiRefWidget), and the RBAC-sensitivity guard runs BEFORE
// the empty-shell guard in populateWidgetContentL1 — so the datagrid is now
// skipped via the RBAC route, NOT the empty-shell route. The CUSTOMER-FACING
// outcome is identical (no poison entry lands in the identity-free cell);
// only the mechanism changed (classification supersedes the empty-shell
// path, because every apiRef+resourcesRefsTemplate widget — the entire
// empty-shell domain — is also RBAC-sensitive). The empty-shell PREDICATE
// itself is unchanged and still unit-tested by
// TestLever1_ShouldSkipEmptyWidgetShell. We assert the RBAC counter here.
func TestLever1_PopulateSkipsEmptyPoisonShell(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	RegisterWidgetContentMetricsForTest()

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "datagrids"}
	in := newUnstructuredWidget("krateo-system", "compositions-page-datagrid")
	res := widgetWithShape("compositions-panels", true, 0) // apiRef+resourcesRefsTemplate, empty

	beforeStore := cache.ResolvedCache().Stats().WidgetContentStoreTotal
	beforeRBAC := widgetContentSkippedRBACSensitiveTotal.Load()

	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	if got := cache.ResolvedCache().Stats().WidgetContentStoreTotal; got != beforeStore {
		t.Fatalf("LEVER 1 FAIL: a poison/RBAC-sensitive shell was STORED (WidgetContentStoreTotal %d -> %d). "+
			"An apiRef+template-driven datagrid at the frontend's (perPage=5,page=1) tuple "+
			"must NEVER land in the identity-free cell.", beforeStore, got)
	}
	// task #69: the datagrid is now caught by the RBAC-sensitivity guard
	// (which runs first), not the empty-shell guard.
	if got := widgetContentSkippedRBACSensitiveTotal.Load(); got != beforeRBAC+1 {
		t.Fatalf("task #69: RBAC-sensitivity skip counter did not advance (%d -> %d)", beforeRBAC, got)
	}

	// And nothing landed under the identity-free key.
	key := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: "krateo-system", Name: "compositions-page-datagrid",
		PerPage: 5, Page: 1,
	})
	if _, hit := cache.ResolvedCache().Get(key); hit {
		t.Fatalf("LEVER 1 FAIL: an entry was created under the identity-free key for an apiRef+template datagrid")
	}
}

// TestLever1_PopulateDoesNotStorePopulatedDatagrid — task #69 INVERSION of
// the former TestLever1_PopulateStoresPopulatedShell.
//
// PRE-FIX expectation: a POPULATED apiRef+resourcesRefsTemplate datagrid was
// stored into the identity-free cell (the lever-1 guard fired only on the
// EMPTY shape). That is EXACTLY the cross-user leak class task #69 closes: a
// populated datagrid resolved under the SA-maximal walk identity would be
// served, shared, to every user — and gateWidgetEnvelope only narrows
// resourcesRefs.items[].allowed, never status.widgetData. So a POPULATED
// apiRef+template widget MUST NOT be stored into the identity-free cell; it
// routes to the per-cohort `widgets` L1 (RBAC-narrowed at resolve).
func TestLever1_PopulateDoesNotStorePopulatedDatagrid(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	RegisterWidgetContentMetricsForTest()

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "datagrids"}
	in := newUnstructuredWidget("krateo-system", "compositions-page-datagrid")
	res := widgetWithShape("compositions-panels", true, 5) // apiRef+resourcesRefsTemplate, POPULATED

	beforeRBAC := widgetContentSkippedRBACSensitiveTotal.Load()
	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	if got := widgetContentSkippedRBACSensitiveTotal.Load(); got != beforeRBAC+1 {
		t.Fatalf("task #69: a POPULATED apiRef+template datagrid must be skipped via the "+
			"RBAC-sensitivity guard (%d -> %d)", beforeRBAC, got)
	}
	key := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: "krateo-system", Name: "compositions-page-datagrid",
		PerPage: 5, Page: 1,
	})
	if _, hit := cache.ResolvedCache().Get(key); hit {
		t.Fatalf("LEAK GUARD FAIL: a POPULATED apiRef+template datagrid was stored into the "+
			"identity-free cell — the cross-user leak class task #69 closes")
	}
}

// TestLever1_PopulateStoresStaticOnlyEmptyWidget — a widget with NO apiRef
// (e.g. a pure piechart) that resolved with empty resourcesRefs.items is NOT
// a poison shape and MUST be stored (the guard is confined to the apiRef+
// template-driven shape).
func TestLever1_PopulateStoresStaticOnlyEmptyWidget(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "piecharts"}
	in := newUnstructuredWidget("krateo-system", "dashboard-piechart")
	res := widgetWithShape("", false, 0) // no apiRef, no template, empty
	// Re-stamp identity so the key matches.
	res.Object["metadata"] = map[string]any{"namespace": "krateo-system", "name": "dashboard-piechart"}

	populateWidgetContentL1(context.Background(), gvr, in, 5, 1, res, 0)

	key := cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: "krateo-system", Name: "dashboard-piechart",
		PerPage: 5, Page: 1,
	})
	if _, hit := cache.ResolvedCache().Get(key); !hit {
		t.Fatalf("a static-only (no apiRef/template) widget that resolved empty MUST be stored; "+
			"the lever-1 guard wrongly fired on a non-poison shape")
	}
}
