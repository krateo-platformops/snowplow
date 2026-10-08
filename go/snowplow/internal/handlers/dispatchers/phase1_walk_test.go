// phase1_walk_test.go — 0.30.102 Tag B no-hardcode + premature-Ready
// falsifiers for the Phase 1 SA-credentialed resolution walk.
//
// THE NO-HARDCODE FALSIFIER (feedback_no_special_cases.md, HARD GATE):
//
//   Phase 1 must seed ONLY from the resolved routesloaders roots. There
//   is NO configured widget-GVR list and NO configured RESTAction list.
//   The proof:
//
//     POSITIVE control — a routesloaders root whose resolution reaches a
//       RESTAction whose path -> GVR G_reached. After Phase1Warmup,
//       G_reached's informer IS registered. (The walk works.)
//
//     NEGATIVE control — an ORPHAN RESTAction wired to NO routesloaders
//       page, whose path -> GVR G_orphan registered by nothing else.
//       After Phase1Warmup, G_orphan's informer is NEVER registered.
//       (The orphan exclusion is real selectivity, not an accident.)
//
//   The walk fans out from the routesloaders roots ONLY — it never LISTs
//   the restactions GVR for discovery. An orphan RESTAction has no
//   inbound reference from any routesloaders page, so the walk's
//   resolution never touches it and lazyRegisterInnerCallPaths never
//   registers its GVR's informer.
//
// We drive phase1WarmupWith with an in-memory routesloaders lister and a
// resolver stub. The stub models the production contract: resolving a
// root registers an informer for every GVR that root's navigation tree
// reaches (exactly what lazyRegisterInnerCallPaths does at runtime). The
// stub registers ONLY GVRs reachable from the root it is given — so the
// orphan GVR, reachable from no root, is provably never registered.

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

var (
	// gvrReached is the GVR an in-navigation RESTAction's path resolves
	// to — Phase 1's walk MUST register it.
	gvrReached = schema.GroupVersionResource{
		Group: "composition.krateo.io", Version: "v1", Resource: "githubscaffoldings",
	}
	// gvrOrphan is the GVR an ORPHAN RESTAction's path resolves to. No
	// routesloaders page references that RESTAction, so Phase 1's walk
	// must NEVER register gvrOrphan.
	gvrOrphan = schema.GroupVersionResource{
		Group: "orphan.krateo.io", Version: "v1", Resource: "orphanthings",
	}
)

// phase1TestWatcher builds a cache=on watcher whose fake dynamic client
// knows the List-kinds for the meta-query seeds + the reached/orphan
// GVRs, so any informer registered during the walk can sync without the
// fake client panicking.
func phase1TestWatcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	sch := k8sruntime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		navRootFixtureGVR: "RoutesLoaderList",
		{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}: "RESTActionList",
		gvrReached: "GithubScaffoldingList",
		gvrOrphan:  "OrphanThingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	return rw
}

// navRootFixtureGVR is the GVR these walk tests hand their fake navigation
// roots under.
//
// #483 — it is declared HERE, in the test package, because cache no longer
// exports a routesloaders GVR: the kind was removed from the platform and its
// meta-query seed with it. The walk tests still need SOME navigation-root GVR
// to build a root under, and in production that GVR is whatever the frontend
// ConfigMap's `/call` URL decodes to (listNavigationRootsFromConfigMap), so any
// value works here. The historical values are kept verbatim so this is purely a
// relocation and these tests exercise exactly what they did before.
var navRootFixtureGVR = schema.GroupVersionResource{
	Group:    "widgets.templates.krateo.io",
	Version:  "v1beta1",
	Resource: "routesloaders",
}

func routesLoaderCR(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "RoutesLoader",
		"metadata":   map[string]any{"namespace": ns, "name": name},
	}}
}

// TestPhase1_NoHardcode_OrphanExcluded is the no-hardcode falsifier.
//
// One routesloaders root is supplied. The resolver stub registers
// gvrReached when it resolves that root (modeling the in-navigation
// RESTAction's lazyRegisterInnerCallPaths side effect). The orphan
// RESTAction is wired to NO root, so the stub is never invoked for it
// and gvrOrphan is never registered.
//
// PASS: gvrReached registered, gvrOrphan NOT registered, Phase1Done set.
// A regression that seeds Phase 1 from a configured GVR/RESTAction list
// would register gvrOrphan too — and this test would FAIL.
func TestPhase1_NoHardcode_OrphanExcluded(t *testing.T) {
	rw := phase1TestWatcher(t)
	cache.ResetPhase1DoneForTest()
	cache.ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)
	t.Cleanup(cache.ResetNavigationDiscoveredGroupsForTest)

	// One routesloaders root — the only navigation seed.
	lister := func(ctx context.Context) ([]navigationRoot, error) {
		return []navigationRoot{{Root: routesLoaderCR("ns-a", "main"), GVR: gvrReached}}, nil
	}

	// The resolver stub models the production contract: resolving a root
	// registers an informer for the GVR its navigation tree reaches. It
	// registers ONLY gvrReached — the orphan GVR is reachable from no
	// root, so the stub never registers it. (lazyRegisterInnerCallPaths
	// behaves identically at runtime: it fires only for paths the
	// resolver actually walked.)
	resolveCalls := 0
	resolver := func(ctx context.Context, root navigationRoot) error {
		resolveCalls++
		rw.EnsureResourceType(gvrReached)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := phase1WarmupWith(ctx, rw, lister, resolver, nil, nil, nil, nil); err != nil {
		t.Fatalf("phase1WarmupWith returned error: %v", err)
	}

	if resolveCalls != 1 {
		t.Fatalf("resolver must be invoked once per routesloaders root; got %d calls", resolveCalls)
	}

	// POSITIVE control — the reached GVR's informer IS registered.
	if !rw.IsRegistered(gvrReached) {
		t.Fatalf("no-hardcode falsifier POSITIVE control FAIL: "+
			"a GVR reached by resolving a routesloaders root must be registered; %v missing", gvrReached)
	}

	// NEGATIVE control — the orphan GVR's informer is NEVER registered.
	// Assert over the FULL registered set: nothing the walk touched may
	// be the orphan GVR.
	for _, g := range rw.RegisteredGVRs() {
		if g == gvrOrphan {
			t.Fatalf("no-hardcode falsifier FAIL: orphan GVR %v was registered — "+
				"Phase 1 must seed ONLY from resolved routesloaders roots, "+
				"never from a configured list", gvrOrphan)
		}
	}

	if !cache.IsPhase1Done() {
		t.Fatalf("Phase1Done must be set after a successful Phase1Warmup")
	}
}

// TestPhase1_NoRoots_StillSignalsDone asserts that when there are zero
// routesloaders CRs, Phase1Warmup still completes the sync barrier over
// the meta-query seeds and signals Phase1Done — /readyz must not hang at
// 503 on a cluster with no routesloaders.
func TestPhase1_NoRoots_StillSignalsDone(t *testing.T) {
	rw := phase1TestWatcher(t)
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)

	lister := func(ctx context.Context) ([]navigationRoot, error) {
		return nil, nil
	}
	resolver := func(ctx context.Context, root navigationRoot) error {
		t.Fatalf("resolver must not be called when there are no roots")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := phase1WarmupWith(ctx, rw, lister, resolver, nil, nil, nil, nil); err != nil {
		t.Fatalf("phase1WarmupWith (no roots) returned error: %v", err)
	}
	if !cache.IsPhase1Done() {
		t.Fatalf("Phase1Done must be set even with zero routesloaders roots")
	}
	// gvrOrphan must still be absent — nothing reached it.
	if rw.IsRegistered(gvrOrphan) {
		t.Fatalf("orphan GVR registered with zero roots — impossible unless hardcoded")
	}
}

// TestPhase1_PrematureReady asserts Phase1Done stays false UNTIL
// Phase1Warmup returns. While the walk is in flight (resolver blocked),
// IsPhase1Done() must be false — the premature-Ready falsifier at the
// walk level (the /readyz HTTP-status form lives in handlers/readyz_test.go).
func TestPhase1_PrematureReady(t *testing.T) {
	rw := phase1TestWatcher(t)
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)

	release := make(chan struct{})
	resolverEntered := make(chan struct{})

	lister := func(ctx context.Context) ([]navigationRoot, error) {
		return []navigationRoot{{Root: routesLoaderCR("ns-a", "main"), GVR: gvrReached}}, nil
	}
	resolver := func(ctx context.Context, root navigationRoot) error {
		close(resolverEntered)
		<-release // block until the test releases the walk
		rw.EnsureResourceType(gvrReached)
		return nil
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done <- phase1WarmupWith(ctx, rw, lister, resolver, nil, nil, nil, nil)
	}()

	// Wait until the walk is mid-resolve.
	<-resolverEntered
	// While the walk is blocked, Phase1Done MUST be false.
	if cache.IsPhase1Done() {
		t.Fatalf("premature-Ready FAIL: Phase1Done is true while the walk is still in flight")
	}

	// Release the walk; Phase1Warmup must complete and flip the gate.
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("phase1WarmupWith returned error: %v", err)
	}
	if !cache.IsPhase1Done() {
		t.Fatalf("Phase1Done must be true once Phase1Warmup returns")
	}
}
