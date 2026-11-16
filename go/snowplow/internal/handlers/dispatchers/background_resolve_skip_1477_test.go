package dispatchers

// background_resolve_skip_1477_test.go — #1477 (b): every background resolve
// site skips a resolve that needs request extras it does not carry, and counts
// it in snowplow_prewarm_resolve_skipped_total{site,reason}.
//
// The shape is 057's: a widget whose apiRef RESTAction builds its spoke
// endpointRef name from `.name`. Each arm drives the REAL site function —
// phase1Walker.walk, seedOneWidget, prewarmOneRESTAction — with only the
// RESTAction read (backgroundRESTActionGetFn) and the resolve beyond the skip stood in,
// and reads whether the resolve was REACHED, not a counter alone:
//   - the widget with no inline extras: not resolved, counted once at the site;
//   - the same widget with an inline spec.apiRef.extras.name: resolved, not
//     counted (the inline key is what the template reads);
//   - the content prewarm resolves the harvested RESTAction with no extras at
//     all, so it skips the RESTAction whatever the widgets carry inline.
// Hermetic, no cluster.

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

const ra1477 = "cluster-compositions-1477"

// spokeRA1477 is a RESTAction whose spokeCrds stage selects its endpoint from
// the request extra `name`; a static control has a literal name.
func spokeRA1477(endpointName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": ra1477, "namespace": h1NS},
		"spec": map[string]any{
			"api": []any{
				map[string]any{"name": "target", "path": "/apis/clusters.example.io/v1/namespaces/krateo-system/clusters"},
				map[string]any{
					"name":        "spokeCrds",
					"path":        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
					"dependsOn":   map[string]any{"name": "target"},
					"endpointRef": map[string]any{"name": endpointName, "namespace": h1NS},
				},
			},
		},
	}}
}

const templatedEndpoint1477 = `${ .name + "-endpoint" }`

// widget1477 is a widget whose apiRef names ra1477; inline non-nil adds
// spec.apiRef.extras.
func widget1477(inline map[string]any) *unstructured.Unstructured {
	apiRef := map[string]any{"name": ra1477, "namespace": h1NS}
	if inline != nil {
		apiRef["extras"] = inline
	}
	return h1WidgetUnstructured(map[string]any{"apiRef": apiRef})
}

// serveRA1477 makes backgroundRESTActionGetFn return the RESTAction for ra1477.
// The returned func reports, per read so far, whether it was informer-only.
func serveRA1477(t *testing.T, endpointName string) (informerOnly func() []bool) {
	t.Helper()
	orig := backgroundRESTActionGetFn
	t.Cleanup(func() { backgroundRESTActionGetFn = orig })
	var reads []bool
	backgroundRESTActionGetFn = func(ctx context.Context, ref templatesv1.ObjectReference) objects.Result {
		reads = append(reads, cache.InformerOnlyReadsFromContext(ctx))
		if ref.Name != ra1477 || ref.Namespace != h1NS {
			return objects.Result{}
		}
		return objects.Result{GVR: restActionGVR, Unstructured: spokeRA1477(endpointName)}
	}
	return func() []bool { return append([]bool(nil), reads...) }
}

// The walk's and the widget seed's read of the apiRef RESTAction is
// informer-only: the predicate never costs an apiserver round trip.
func TestIssue1477_WidgetPredicateReadsInformerOnly(t *testing.T) {
	reads := serveRA1477(t, templatedEndpoint1477)
	walk1477(t, widget1477(nil))
	got := reads()
	if len(got) != 1 || !got[0] {
		t.Fatalf("#1477 RED: the walk's predicate read must be one informer-only read; got %v", got)
	}
}

func skipped1477(site string) uint64 {
	return backgroundResolveSkipped[site][bgSkipReasonNeedsRequestExtras].Load()
}

// TestIssue1477_Predicate pins the one predicate over the declared template
// and the carried extras.
func TestIssue1477_Predicate(t *testing.T) {
	ra := func(name string) *templatesv1.RESTAction {
		return &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: []*templatesv1.API{
			{Name: "target"},
			{Name: "spokeCrds", EndpointRef: &templatesv1.Reference{Name: name, Namespace: h1NS}},
		}}}
	}
	cases := []struct {
		name    string
		ep      string
		carried map[string]any
		want    bool
	}{
		{"reads .name, carried nothing", templatedEndpoint1477, nil, true},
		{"reads .name, carried name", templatedEndpoint1477, map[string]any{"name": "spoke-a"}, false},
		{"reads .name, carried another key", templatedEndpoint1477, map[string]any{"namespace": "x"}, true},
		{"reads an upstream stage's output", `${ .target.items[0].name }`, nil, false},
		{"reads the pagination block", `${ .slice.page | tostring }`, nil, false},
		{"reads its input whole", `${ tojson }`, map[string]any{"name": "spoke-a"}, true},
		{"literal name", "spoke-a-endpoint", nil, false},
	}
	for _, c := range cases {
		if got := restactionNeedsRequestExtras(ra(c.ep), c.carried); got != c.want {
			t.Errorf("%s: restactionNeedsRequestExtras = %v; want %v", c.name, got, c.want)
		}
	}
	if restactionNeedsRequestExtras(nil, nil) {
		t.Error("a nil RESTAction needs nothing")
	}
}

// walk1477 runs the REAL walk over w and reports whether it reached the
// widget's apiRef resolve.
func walk1477(t *testing.T, w *unstructured.Unstructured) (resolved bool) {
	t.Helper()
	t.Cleanup(widgets.SetResolveApiRefForTest(func(context.Context, widgets.ResolveOptions) (map[string]any, error) {
		resolved = true
		return map[string]any{}, nil
	}))
	t.Cleanup(widgets.SetValidateObjectStatusForTest(func(context.Context, *rest.Config, map[string]any) error { return nil }))
	if err := newPhase1Walker(nil, h1NS).walk(context.Background(), w, h1WidgetGVR, 1, unpaginatedPrewarm); err != nil {
		t.Fatalf("walk: %v", err)
	}
	return resolved
}

func TestIssue1477_Walk_SkipsWidgetNeedingExtras_ResolvesInlineSupplied(t *testing.T) {
	serveRA1477(t, templatedEndpoint1477)

	before := skipped1477(bgSkipSiteWalk)
	if walk1477(t, widget1477(nil)) {
		t.Fatal("#1477 RED: the walk resolved a widget whose RESTAction's endpointRef reads `.name`, which the walk does not carry")
	}
	if got := skipped1477(bgSkipSiteWalk) - before; got != 1 {
		t.Fatalf("walk skip counted %d times; want 1", got)
	}

	before = skipped1477(bgSkipSiteWalk)
	if !walk1477(t, widget1477(map[string]any{"name": "spoke-a"})) {
		t.Fatal("#1477 RED: the walk skipped a widget whose inline apiRef extras supply `.name`")
	}
	if got := skipped1477(bgSkipSiteWalk) - before; got != 0 {
		t.Fatalf("a resolved widget was counted as skipped (%d)", got)
	}
}

// Control: a literal endpointRef is resolved by the walk.
func TestIssue1477_Walk_StaticEndpointResolved(t *testing.T) {
	serveRA1477(t, "spoke-a-endpoint")
	if !walk1477(t, widget1477(nil)) {
		t.Fatal("control: the walk skipped a widget whose RESTAction has a literal endpointRef")
	}
}

// seed1477 runs the REAL seedOneWidget over w for a granted cohort and reports
// whether it reached the widget resolve.
func seed1477(t *testing.T, w *unstructured.Unstructured) (resolved bool) {
	t.Helper()
	seedCtx := withCohortSeedContext(context.Background(),
		seedTarget{Username: a1Alice, Groups: []string{a1Group}}, endpoints.Endpoint{}, nil)
	key, handle, inputs := dispatchCacheLookupKey(seedCtx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(seedCtx, w.Object, nil))
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: the cohort must derive a live widgets key, else seedOneWidget short-circuits "+
			"before the #1477 check; key=%q inputs=%+v", key, inputs)
	}
	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	widgetsResolveFn = func(context.Context, widgets.ResolveOptions) (*widgets.Widget, error) {
		resolved = true
		return h1WidgetUnstructured(map[string]any{}), nil
	}
	entry := navWidgetEntry{W: w, GVR: h1WidgetGVR, Pagination: unpaginatedPrewarm}
	if err := seedOneWidget(seedCtx, entry, h1NS, seedModeBoot); err != nil {
		t.Fatalf("seedOneWidget: %v", err)
	}
	return resolved
}

func TestIssue1477_WidgetSeed_SkipsWidgetNeedingExtras_ResolvesInlineSupplied(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	serveRA1477(t, templatedEndpoint1477)

	before := skipped1477(bgSkipSiteWidgetSeed)
	if seed1477(t, widget1477(nil)) {
		t.Fatal("#1477 RED: the widget seed resolved a widget whose RESTAction's endpointRef reads `.name`, which the seed does not carry")
	}
	if got := skipped1477(bgSkipSiteWidgetSeed) - before; got != 1 {
		t.Fatalf("widget-seed skip counted %d times; want 1", got)
	}

	before = skipped1477(bgSkipSiteWidgetSeed)
	if !seed1477(t, widget1477(map[string]any{"name": "spoke-a"})) {
		t.Fatal("#1477 RED: the widget seed skipped a widget whose inline apiRef extras supply `.name`")
	}
	if got := skipped1477(bgSkipSiteWidgetSeed) - before; got != 0 {
		t.Fatalf("a resolved widget was counted as skipped (%d)", got)
	}
}

// prewarm1477 runs the REAL prewarmOneRESTAction over ra1477 and reports
// whether it reached the resolve.
func prewarm1477(t *testing.T) (resolved bool, err error) {
	t.Helper()
	orig := restactionsResolveFn
	t.Cleanup(func() { restactionsResolveFn = orig })
	restactionsResolveFn = func(context.Context, restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		resolved = true
		return nil, nil
	}
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: ra1477, Namespace: h1NS},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
	_, err = prewarmOneRESTAction(context.Background(), ref, h1NS)
	return resolved, err
}

func TestIssue1477_ContentPrewarm_SkipsRESTActionNeedingExtras(t *testing.T) {
	serveRA1477(t, templatedEndpoint1477)
	before := skipped1477(bgSkipSiteContentPrewarm)
	resolved, err := prewarm1477(t)
	if resolved {
		t.Fatal("#1477 RED: the content prewarm resolved a RESTAction whose endpointRef reads `.name` with no extras")
	}
	if !errors.Is(err, errContentPrewarmNeedsRequestExtras) {
		t.Fatalf("prewarmOneRESTAction err = %v; want the needs-request-extras skip", err)
	}
	if got := skipped1477(bgSkipSiteContentPrewarm) - before; got != 1 {
		t.Fatalf("content-prewarm skip counted %d times; want 1", got)
	}
}

// Control: a literal endpointRef is resolved by the content prewarm.
func TestIssue1477_ContentPrewarm_StaticEndpointResolved(t *testing.T) {
	serveRA1477(t, "spoke-a-endpoint")
	resolved, err := prewarm1477(t)
	if err != nil || !resolved {
		t.Fatalf("control: the content prewarm did not resolve a RESTAction with a literal endpointRef (resolved=%v err=%v)", resolved, err)
	}
}

// The RESTAction seed's #113 skip is counted in the same counter.
func TestIssue1477_RestactionSeed_SkipCounted(t *testing.T) {
	const user = "userGranted1477"
	buildGrantedRestactionWatcher(t, user)
	origPut, origGet := seedRestactionResolveAndPutFn, seedObjectsGetFn
	t.Cleanup(func() { seedRestactionResolveAndPutFn, seedObjectsGetFn = origPut, origGet })
	putReached := false
	seedRestactionResolveAndPutFn = func(
		_, _ context.Context, _ *templatesv1.RESTAction, _ templatesv1.ObjectReference,
		_, _ string, _ cacheHandle, _ *cache.ResolvedKeyInputs, _ objects.Result,
		_ *cache.StageErrorSink, _ *cache.ExternalTouchedSink,
	) error {
		putReached = true
		return nil
	}
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result { return fetchedRestaction(true) }
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "hub-spoke-ra", Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
	before := skipped1477(bgSkipSiteRestactionSeed)
	if err := seedOneRestaction(seedCohortCtx(user), ref, "krateo-system", seedModeBoot); err != nil {
		t.Fatalf("seedOneRestaction: %v", err)
	}
	if putReached {
		t.Fatal("precondition: the templated RESTAction reached the Put; the #113 skip did not fire")
	}
	if got := skipped1477(bgSkipSiteRestactionSeed) - before; got != 1 {
		t.Fatalf("restaction-seed skip counted %d times; want 1", got)
	}
}

// The counter publishes every {site, reason} cell, zeros included.
func TestIssue1477_SkipCounterPublishesEveryCell(t *testing.T) {
	snap := BackgroundResolveSkipSnapshot()
	if len(snap) != len(backgroundResolveSkipSites)*len(backgroundResolveSkipReasons) {
		t.Fatalf("snapshot has %d cells; want %d", len(snap), len(backgroundResolveSkipSites)*len(backgroundResolveSkipReasons))
	}
	before := skipped1477(bgSkipSiteWalk)
	bumpBackgroundResolveSkip("not-a-site", bgSkipReasonNeedsRequestExtras)
	bumpBackgroundResolveSkip(bgSkipSiteWalk, "not-a-reason")
	if skipped1477(bgSkipSiteWalk) != before {
		t.Fatal("a label outside the closed sets moved a cell")
	}
}
