// issue403_widget_uaf_preresolve_skip_test.go — #403: the widget seed skips the
// resolve when its apiRef'd RESTAction DECLARES a userAccessFilter.
//
// WHAT CHANGED. Before #403 the widget seed resolved every (widget × cohort)
// unit and THEN declined the Put when the resolve bumped the UAFTouchedSink. For
// a widget whose spec.apiRef names a UAF-declaring RA, apiref.Resolve bumps the
// sink UNCONDITIONALLY (bumpUAFSinkIfDeclared, apiref/resolve.go) as soon as its
// objects.Get of the RA succeeds — so the decline is certain before the resolve
// runs. On 057 that is 908 resolve-then-discard units per boot (deterministic
// across boots). The seed now checks the declaration first and skips.
//
// THE HARD REQUIREMENT IS ZERO COLD NAVIGATIONS, so most of this file is the
// falsifier that the skip never fires where a cell WOULD have been written:
//
//   - skip arm: UAF-declaring RA → no resolve, no cell, decline + skip counters.
//   - must-resolve arms (each RED under an over-broad skip): plain RA, no
//     apiRef, missing RA, an identity that cannot GET the RA, and the nested
//     chain (plain RA whose resolve still bumps the sink → resolve-then-decline).
//   - parity arm: over the whole matrix, the L1 state after the seed is the
//     SAME with the pre-check as with it disabled (the pre-#403 path).
//   - agreement arm: the pre-check's verdict equals what the REAL
//     apiref.Resolve does to the sink, per RA in the matrix — the pre-check is
//     not allowed to drift from the bump it predicts.

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	i403Group       = "portal"
	i403PanelsGroup = "panels-only" // may read the widget but NOT the RESTAction
	i403User        = "alice"
	i403RAUAF       = "i403-uaf"
	i403RAPlain     = "i403-plain"
	i403RAMissing   = "i403-missing"
)

func i403RA(name string, uaf bool) *unstructured.Unstructured {
	step := map[string]any{"name": "list-namespaces", "path": "/api/v1/namespaces"}
	if uaf {
		step["userAccessFilter"] = map[string]any{
			"verb": "get", "group": "", "resource": "configmaps", "namespaceFrom": ".metadata.name",
		}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": h1NS},
		"spec":       map[string]any{"api": []any{step}},
	}}
}

// i403Watcher publishes RBAC (group portal: get/list restactions + panels;
// group panels-only: panels only) and the two RA CRs, and makes restactions
// SERVABLE so objects.Get answers from the informer — the same source the
// production seed reads.
func i403Watcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	rbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR: "RESTActionList", h1WidgetGVR: "PanelList",
		crbGVR: "ClusterRoleBindingList", crGVR: "ClusterRoleList",
		rbGVR: "RoleBindingList", rGVR: "RoleList",
	}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "i403-portal-reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs: []string{"get", "list"}, APIGroups: []string{h1RAGVR.Group, h1WidgetGVR.Group},
				Resources: []string{h1RAGVR.Resource, h1WidgetGVR.Resource},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "i403-portal", UID: types.UID("uid-i403-portal")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: i403Group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "i403-portal-reader"},
		},
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "i403-panels-reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs: []string{"get", "list"}, APIGroups: []string{h1WidgetGVR.Group}, Resources: []string{h1WidgetGVR.Resource},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "i403-panels", UID: types.UID("uid-i403-panels")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: i403PanelsGroup}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "i403-panels-reader"},
		},
		i403RA(i403RAUAF, true),
		i403RA(i403RAPlain, false),
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
	if _, ch := rw.EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !rw.IsServable(h1RAGVR) {
		if time.Now().After(deadline) {
			t.Fatalf("PRE: restactions must be servable so objects.Get serves the RA from the informer")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func i403Entry(apiRefName string) navWidgetEntry {
	spec := map[string]any{}
	if apiRefName != "" {
		spec["apiRef"] = map[string]any{"name": apiRefName, "namespace": h1NS}
	}
	return navWidgetEntry{
		W: h1WidgetUnstructured(spec), GVR: h1WidgetGVR,
		PerPage: -1, Page: -1, KeyPerPage: -1, KeyPage: -1,
	}
}

func i403SeedCtx(groups ...string) context.Context {
	return withCohortSeedContext(context.Background(),
		seedTarget{Username: i403User, Groups: groups}, endpoints.Endpoint{}, nil)
}

// i403Key derives the production widgets key the seed Puts under, and fails
// if the cohort would short-circuit at the #95 empty-binding guard (the arm
// would then prove nothing about the pre-check).
func i403Key(t *testing.T, ctx context.Context, e navWidgetEntry) (string, cacheHandle) {
	t.Helper()
	key, handle, inputs := dispatchCacheLookupKey(ctx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, e.W.Object, nil))
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: live non-empty-BindingUID widgets key required; key=%q inputs=%+v", key, inputs)
	}
	return key, handle
}

// i403ProdLikeResolver is the resolver seam modelled on the PRODUCTION
// contract: it bumps the UAF sink exactly when the real apiref.Resolve would —
// the widget's apiRef RA fetched under THIS ctx declares a userAccessFilter —
// plus forceBump to model a nested RA→RA chain the declaration cannot see.
func i403ProdLikeResolver(calls *int, forceBump bool) func(context.Context, widgets.ResolveOptions) (*widgets.Widget, error) {
	return func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		*calls++
		if ref, err := widgets.GetApiRef(opts.In.Object); err == nil && ref.Name != "" && ref.Namespace != "" {
			if res := objects.Get(ctx, ref); res.Err == nil && res.Unstructured != nil {
				var ra templatesv1.RESTAction
				if runtime.DefaultUnstructuredConverter.FromUnstructured(res.Unstructured.Object, &ra) == nil &&
					ra.HasUserAccessFilterStage() {
					cache.BumpUAFTouched(ctx)
				}
			}
		}
		if forceBump {
			cache.BumpUAFTouched(ctx)
		}
		out := opts.In.DeepCopy()
		_ = unstructured.SetNestedField(out.Object, "rows", "status", "widgetData", "rows")
		return out, nil
	}
}

func TestIssue403_WidgetSeed_UAFDeclaredApiRef_SkipsResolve(t *testing.T) {
	i403Watcher(t)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)
	skips0 := Phase1SeedWidgetUAFPreResolveSkips()

	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	calls := 0
	widgetsResolveFn = i403ProdLikeResolver(&calls, false)

	ctx := i403SeedCtx(i403Group)
	e := i403Entry(i403RAUAF)
	key, handle := i403Key(t, ctx, e)
	if err := seedOneWidget(ctx, e, h1NS, seedModeBoot); err != nil {
		t.Fatalf("seedOneWidget: %v (a skip returns nil, like the decline it replaces)", err)
	}
	if calls != 0 {
		t.Fatalf("#403 RED: the widget's apiRef RESTAction declares a userAccessFilter, so the widgets cell is "+
			"declined for every identity — yet the seed still ran the resolve (%d call(s)) only to discard it", calls)
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("a UAF-declared widget must never be Put (1.12.3 A-1/R-1)")
	}
	if got := cache.WidgetsUAFPutDeclined(); got != 1 {
		t.Fatalf("widgets_uaf_put_declined_total must still count the declined unit once; got %d", got)
	}
	if got := Phase1SeedWidgetUAFPreResolveSkips() - skips0; got != 1 {
		t.Fatalf("snowplow_phase1_seed_widget_uaf_preresolve_skip_total must tick once; got %d", got)
	}
}

// TestIssue403_WidgetSeed_MustResolveArms is the zero-cold-navigation
// falsifier: in every case the pre-check must NOT fire, and wherever the
// pre-#403 seed wrote a cell, a cell is written.
func TestIssue403_WidgetSeed_MustResolveArms(t *testing.T) {
	i403Watcher(t)
	cache.RegisterUAFPutDeclineMetricsForTest()

	cases := []struct {
		name      string
		apiRef    string
		groups    []string
		forceBump bool // nested chain: plain RA whose resolve still refilters
		wantCell  bool
	}{
		{name: "plain RA", apiRef: i403RAPlain, groups: []string{i403Group}, wantCell: true},
		{name: "no apiRef", apiRef: "", groups: []string{i403Group}, wantCell: true},
		{name: "missing RA", apiRef: i403RAMissing, groups: []string{i403Group}, wantCell: true},
		{name: "cohort cannot GET the UAF RA", apiRef: i403RAUAF, groups: []string{i403PanelsGroup}, wantCell: true},
		{name: "nested UAF chain behind a plain RA", apiRef: i403RAPlain, groups: []string{i403Group}, forceBump: true, wantCell: false},
	}
	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache.ResetResolvedCacheForTest()
			cache.ResetUAFPutDeclineCountersForTest()
			skips0 := Phase1SeedWidgetUAFPreResolveSkips()
			calls := 0
			widgetsResolveFn = i403ProdLikeResolver(&calls, tc.forceBump)
			ctx := i403SeedCtx(tc.groups...)
			e := i403Entry(tc.apiRef)
			key, handle := i403Key(t, ctx, e)
			if err := seedOneWidget(ctx, e, h1NS, seedModeBoot); err != nil {
				t.Fatalf("seedOneWidget: %v", err)
			}
			if calls != 1 {
				t.Fatalf("ZERO-COLD RED: the pre-check skipped a unit it cannot prove is declined (resolver calls=%d, want 1)", calls)
			}
			if got := Phase1SeedWidgetUAFPreResolveSkips() - skips0; got != 0 {
				t.Fatalf("ZERO-COLD RED: the pre-resolve skip counter ticked (%d) on a must-resolve unit", got)
			}
			_, resident := handle.Get(key)
			if resident != tc.wantCell {
				t.Fatalf("cell resident=%v, want %v", resident, tc.wantCell)
			}
		})
	}
}

// TestIssue403_WidgetSeed_ParityWithPreCheckDisabled: over the whole matrix the
// post-seed L1 state (resident or not, and the bytes) is identical with and
// without the pre-check. A skip that loses a cell makes the two runs differ.
func TestIssue403_WidgetSeed_ParityWithPreCheckDisabled(t *testing.T) {
	i403Watcher(t)
	cache.RegisterUAFPutDeclineMetricsForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	type unit struct {
		apiRef string
		groups []string
	}
	matrix := []unit{
		{i403RAUAF, []string{i403Group}}, {i403RAPlain, []string{i403Group}}, {"", []string{i403Group}},
		{i403RAMissing, []string{i403Group}}, {i403RAUAF, []string{i403PanelsGroup}}, {i403RAPlain, []string{i403PanelsGroup}},
	}
	type outcome struct {
		resident bool
		raw      string
		declined uint64
	}
	run := func(t *testing.T, precheck bool, u unit) outcome {
		t.Helper()
		// Restore on RETURN (not t.Cleanup, which would leak the override into
		// the next run of the same test and make both arms take one path).
		origPre := seedWidgetApiRefDeclaresUAFFn
		defer func() { seedWidgetApiRefDeclaresUAFFn = origPre }()
		if !precheck {
			seedWidgetApiRefDeclaresUAFFn = func(context.Context, *unstructured.Unstructured) bool { return false }
		}
		origRes := widgetsResolveFn
		defer func() { widgetsResolveFn = origRes }()
		calls := 0
		widgetsResolveFn = i403ProdLikeResolver(&calls, false)
		cache.ResetResolvedCacheForTest()
		cache.ResetUAFPutDeclineCountersForTest()
		ctx := i403SeedCtx(u.groups...)
		e := i403Entry(u.apiRef)
		key, handle := i403Key(t, ctx, e)
		if err := seedOneWidget(ctx, e, h1NS, seedModeBoot); err != nil {
			t.Fatalf("seedOneWidget: %v", err)
		}
		got, ok := handle.Get(key)
		o := outcome{resident: ok, declined: cache.WidgetsUAFPutDeclined()}
		if ok {
			o.raw = string(got.RawJSON)
		}
		return o
	}
	sawDeclined, sawResident := false, false
	for _, u := range matrix {
		ref, pre := run(t, false, u), run(t, true, u)
		if ref != pre {
			t.Fatalf("PARITY RED for apiRef=%q groups=%v: pre-#403 seed -> %+v, with pre-check -> %+v", u.apiRef, u.groups, ref, pre)
		}
		if !ref.resident && ref.declined == 1 {
			sawDeclined = true
		}
		if ref.resident {
			sawResident = true
		}
	}
	if !sawDeclined || !sawResident {
		t.Fatalf("PRECONDITION: the matrix must contain both a UAF-declined and a resident unit, else parity is vacuous "+
			"(declined=%v resident=%v)", sawDeclined, sawResident)
	}
}

// TestIssue403_PreCheckAgreesWithRealApirefBump: for each RA in the matrix the
// pre-check's verdict equals whether the REAL apiref.Resolve bumps the sink
// under the same cohort ctx. The pre-check predicts that bump; it must not
// drift from it.
func TestIssue403_PreCheckAgreesWithRealApirefBump(t *testing.T) {
	i403Watcher(t)
	for _, tc := range []struct {
		apiRef string
		groups []string
	}{
		{i403RAUAF, []string{i403Group}}, {i403RAPlain, []string{i403Group}},
		{i403RAMissing, []string{i403Group}}, {i403RAUAF, []string{i403PanelsGroup}},
	} {
		ctx := i403SeedCtx(tc.groups...)
		e := i403Entry(tc.apiRef)
		fall0 := objects.ObjectsGetStatsSnapshot().ApiserverFallthrough
		pre := seedWidgetApiRefDeclaresUAF(ctx, e.W)
		if d := objects.ObjectsGetStatsSnapshot().ApiserverFallthrough - fall0; d != 0 {
			t.Fatalf("COST RED apiRef=%q groups=%v: the pre-check took %d apiserver fallthrough(s); it must be "+
				"informer-only so it never adds a round-trip the resolve then repeats", tc.apiRef, tc.groups, d)
		}

		rctx, sink := cache.WithUAFTouchedSink(ctx)
		rctx, cancel := context.WithTimeout(rctx, 10*time.Second)
		ref, _ := widgets.GetApiRef(e.W.Object)
		_, _ = apiref.Resolve(rctx, apiref.ResolveOptions{ApiRef: ref, AuthnNS: h1NS, PerPage: -1, Page: -1})
		cancel()
		bumped := sink.Count() > 0
		if pre != bumped {
			t.Fatalf("DRIFT RED apiRef=%q groups=%v: pre-check=%v but the real apiref.Resolve bumped=%v", tc.apiRef, tc.groups, pre, bumped)
		}
		if tc.apiRef == i403RAUAF && tc.groups[0] == i403Group && !bumped {
			t.Fatal("PRECONDITION: the UAF RA must bump the real sink, else the agreement is vacuous")
		}
	}
}
