package dispatchers

// prewarm_waits_1233_test.go — #1233 / #506: every writer that runs at boot
// waits for the complete RBAC snapshot before it starts, so its writes are not
// refused across the gap and lost (nothing redoes them). One arm per wait: the
// phase-1 walk (step 7.1), a widget seed unit (seedOneWidget, also reached by the
// GVR-discovered hook outside phase 1), the content prewarm pass and the
// cluster-list prewarm. (seedOneRestaction's is
// TestIssue1233_ASeedWaitsForTheCompleteSnapshotAndLosesNoCell.)
//
// Each arm opens the gap over a synced environment (gap1233): until release, the
// process's watcher is one whose initial roles LIST is held, so its initial
// publish is not done, and the published snapshot is the environment's own
// marked not Synced. The arm checks the unit has not started, releases, then
// checks the unit ran and stored with no write refused.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// gap1233 reopens the boot gap over the synced environment whose watcher is env.
// The process's watcher becomes one with no objects whose initial roles LIST is
// held, so the initial-publish-done signal WaitRBACSnapshotComplete waits on is
// not given, and the published snapshot is env's own marked not Synced. release
// lets the LIST through. Inside that watcher's initial publish, before the
// signal, env becomes the process's watcher again and republishes its complete
// snapshot, so a unit that waited resumes on exactly the environment it was set
// up for.
func gap1233(t *testing.T, env *cache.ResourceWatcher) (release func()) {
	t.Helper()
	if env == nil || !cache.RBACSnapshotComplete() {
		t.Fatal("PRECONDITION: gap1233 needs a synced environment with a complete RBAC snapshot")
	}
	cache.PartialRBACSnapshotForTest(t)

	scheme := runtime.NewScheme()
	kinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, kinds)
	var released atomic.Bool
	var held atomic.Int64
	dyn.PrependReactor("list", "roles", func(k8stesting.Action) (bool, runtime.Object, error) {
		if released.Load() {
			return false, nil, nil
		}
		held.Add(1)
		return true, nil, errors.New("roles LIST held by the test (#1233)")
	})
	t.Cleanup(cache.SetInitialPublishBeforeIndexHookForTest(func() {
		cache.SetGlobal(env)
		cache.RebuildRBACSnapshotForTest(env)
	}))
	booting, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || booting == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", booting, err)
	}
	t.Cleanup(booting.Stop)
	cache.SetGlobal(booting)
	t.Cleanup(func() { cache.SetGlobal(env) })
	t.Cleanup(func() { released.Store(true) })
	waitUntil1233(t, 10*time.Second, "the roles LIST to be held", func() bool { return held.Load() > 0 })
	if cache.RBACSnapshotComplete() {
		t.Fatal("PRECONDITION: the snapshot must not be complete while the roles LIST is held")
	}
	return func() { released.Store(true) }
}

// notYet1233 fails if done delivers within the observation window: the unit ran
// while the snapshot was incomplete.
func notYet1233(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("#1233 RED: %s ran to its end on the incomplete snapshot", what)
	case <-time.After(300 * time.Millisecond):
	}
}

// ran1233 waits for the unit to end after the release.
func ran1233(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not run within 30s of the roles LIST completing", what)
	}
}

func resident1233() int { return cache.ResolvedCache().Len() }

// Step 7.1: the phase-1 walk calls its writers (here the content-prewarm step)
// only after the initial publish, even though every informer, the RBAC ones
// included, has synced: the walk's own informer barrier does not cover the
// publish.
func TestIssue1233_TheWalkCallsItsWritersOnlyAfterTheCompleteSnapshot(t *testing.T) {
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	a1BuildTwoTenantWatcher(t)
	env := cache.Global()
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)
	release := gap1233(t, env)

	declined0 := cache.PutDeclinedRBACIncompleteTotal()
	var called atomic.Bool
	var completeAtCall, storedAtCall atomic.Bool
	contentWarm := func(ctx context.Context) {
		called.Store(true)
		completeAtCall.Store(cache.RBACSnapshotComplete())
		// The step's write: a shared widgetContent cell.
		in := h1WidgetUnstructured(map[string]any{})
		res := h1WidgetUnstructured(map[string]any{})
		_ = unstructured.SetNestedField(res.Object, "content", "status", "widgetData", "label")
		populateWidgetContentL1(cache.WithPrewarmPath(withWidgetContentPutSinks(ctx)), h1WidgetGVR, in, -1, -1, res, 0)
		key, _ := widgetContentL1Key(h1WidgetGVR, h1NS, h1WName, -1, -1)
		_, ok := cache.ResolvedCache().Get(key)
		storedAtCall.Store(ok)
	}
	lister := func(context.Context) ([]navigationRoot, error) { return nil, nil }
	resolver := func(context.Context, navigationRoot) error { return nil }

	done := make(chan struct{})
	var walkErr error
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		walkErr = phase1WarmupWith(ctx, env, lister, resolver, contentWarm, nil, nil, nil)
	}()

	notYet1233(t, done, "the phase-1 walk")
	if called.Load() {
		t.Fatal("#1233 RED: the walk called its content-prewarm step before the complete snapshot")
	}
	release()
	ran1233(t, done, "the phase-1 walk")
	if walkErr != nil {
		t.Fatalf("phase1WarmupWith: %v", walkErr)
	}
	if !called.Load() || !completeAtCall.Load() {
		t.Fatalf("the walk must call the step on the complete snapshot (called=%v complete=%v)", called.Load(), completeAtCall.Load())
	}
	if !storedAtCall.Load() {
		t.Fatal("#1233 RED: the step's write was refused after the walk waited")
	}
	if got := cache.PutDeclinedRBACIncompleteTotal() - declined0; got != 0 {
		t.Fatalf("no write may be refused, %d were", got)
	}
}

// seedOneWidget waits for the complete snapshot, then seeds its cell. The engine's
// GVR-discovered hook reaches this unit outside phase 1, so the walk's wait does
// not cover it.
func TestIssue1233_AWidgetSeedWaitsForTheCompleteSnapshotAndLosesNoCell(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	env := cache.Global()
	var resolves atomic.Int64
	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	widgetsResolveFn = func(_ context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		resolves.Add(1)
		return h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}}), nil
	}
	release := gap1233(t, env)

	label := cache.BackgroundOriginCohortSeed + "/widgets"
	declined0 := cache.PutDeclinedRBACIncompleteTotal()
	e := reseedWidgetEntry()
	done := make(chan struct{})
	var seedErr error
	go func() {
		defer close(done)
		seedErr = seedOneWidget(reseedSeedCtx(), e, h1NS, seedModeBoot)
	}()

	notYet1233(t, done, "the widget seed unit")
	if resolves.Load() != 0 || cache.PutDeclinedRBACIncompleteTotal() != declined0 {
		t.Fatalf("#1233 RED: the widget seed unit started on the incomplete snapshot (resolves=%d, refusals=%d)",
			resolves.Load(), cache.PutDeclinedRBACIncompleteTotal()-declined0)
	}
	release()
	ran1233(t, done, "the widget seed unit")
	if seedErr != nil {
		t.Fatalf("seedOneWidget: %v", seedErr)
	}
	key, handle, _ := reseedWidgetKey(t, reseedSeedCtx(), e)
	if _, ok := handle.GetNoTouch(key); !ok {
		t.Fatal("#1233 RED: the seeded widget cell is not resident after the snapshot completed")
	}
	if got := cache.PutDeclinedRBACIncompleteTotal() - declined0; got != 0 {
		t.Fatalf("no write of the seed may be refused, %d were (%s: %d)", got, label, declined1233(label))
	}
}

// The content prewarm pass waits for the complete snapshot, then resolves every
// harvested data source and stores its cells (here one namespaced list stage).
func TestIssue1233_TheContentPrewarmWaitsForTheCompleteSnapshotAndLosesNoCell(t *testing.T) {
	nsPath := "/apis/" + h643ThingsGVR.Group + "/" + h643ThingsGVR.Version + "/namespaces/" + h643NS + "/" + h643ThingsGVR.Resource
	env := setup1233Prewarm(t, h643Opts{cohorts: 1, things: 2, extra: []h643Extra{{
		widget: "ct-w", ra: "ct-ra",
		api: []any{h643OwnStage("ns", nsPath)},
	}}})
	for _, w := range env.widgets {
		env.deps.harvester.harvestApiRef(w.W)
	}
	if len(env.deps.harvester.snapshot()) == 0 {
		t.Fatal("PRECONDITION: the pass needs a harvested data source")
	}
	release := gap1233(t, env.rw)

	declined0 := cache.PutDeclinedRBACIncompleteTotal()
	resident0 := resident1233()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runContentPrewarmPass(context.Background(), env.deps.harvester, env.deps.saEP, env.deps.saRC, env.deps.authnNS)
	}()

	notYet1233(t, done, "the content prewarm pass")
	if resident1233() != resident0 || cache.PutDeclinedRBACIncompleteTotal() != declined0 {
		t.Fatalf("#1233 RED: the content prewarm started on the incomplete snapshot (resident %d -> %d, refusals=%d)",
			resident0, resident1233(), cache.PutDeclinedRBACIncompleteTotal()-declined0)
	}
	release()
	ran1233(t, done, "the content prewarm pass")
	if resident1233() <= resident0 {
		t.Fatalf("#1233 RED: the content prewarm stored nothing after the snapshot completed (resident %d -> %d)", resident0, resident1233())
	}
	if got := cache.PutDeclinedRBACIncompleteTotal() - declined0; got != 0 {
		t.Fatalf("no write of the pass may be refused, %d were", got)
	}
}

// The cluster-list prewarm waits for the complete snapshot, then populates the
// cluster-wide list cell its harvested RESTAction reads.
func TestIssue1233_TheClusterListPrewarmWaitsForTheCompleteSnapshotAndLosesNoCell(t *testing.T) {
	listPath := "/apis/" + h643ThingsGVR.Group + "/" + h643ThingsGVR.Version + "/" + h643ThingsGVR.Resource
	env := setup1233Prewarm(t, h643Opts{cohorts: 1, things: 2, extra: []h643Extra{{
		widget: "cl-w", ra: "cl-ra",
		api: []any{h643OwnStage("all", listPath)},
	}}})
	for _, w := range env.widgets {
		env.deps.harvester.harvestApiRef(w.W)
	}
	if len(env.deps.harvester.snapshot()) == 0 {
		t.Fatal("PRECONDITION: the prewarm needs a harvested RESTAction")
	}
	prewarm := makeClusterListPrewarmFn(env.deps.harvester, env.deps.saEP, env.deps.saRC, env.deps.authnNS)
	release := gap1233(t, env.rw)

	declined0 := cache.PutDeclinedRBACIncompleteTotal()
	resident0 := resident1233()
	done := make(chan struct{})
	go func() {
		defer close(done)
		prewarm(context.Background())
	}()

	notYet1233(t, done, "the cluster-list prewarm")
	if resident1233() != resident0 || cache.PutDeclinedRBACIncompleteTotal() != declined0 {
		t.Fatalf("#1233 RED: the cluster-list prewarm started on the incomplete snapshot (resident %d -> %d, refusals=%d)",
			resident0, resident1233(), cache.PutDeclinedRBACIncompleteTotal()-declined0)
	}
	release()
	ran1233(t, done, "the cluster-list prewarm")
	if resident1233() <= resident0 {
		t.Fatalf("#1233 RED: the cluster-list prewarm stored nothing after the snapshot completed (resident %d -> %d)", resident0, resident1233())
	}
	if got := cache.PutDeclinedRBACIncompleteTotal() - declined0; got != 0 {
		t.Fatalf("no write of the prewarm may be refused, %d were", got)
	}
}

// setup1233Prewarm is the #643 environment as the walk's prewarm sees it
// (b2Setup's shape): discovery and CRD answers on, and the snowplow SA bound to
// the reader role, so objects.Get and the resolves run under the SA.
func setup1233Prewarm(t *testing.T, o h643Opts) *h643Env {
	t.Helper()
	o.walk = true
	o.typed = append(o.typed, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "b2-sa-reader"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "h643-reader"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: b2SAName, Namespace: h643AuthnNS}},
	})
	env := h643Setup(t, o)
	env.deps.saEP.Token = b2SAToken()
	return env
}
