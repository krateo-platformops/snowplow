// readyz_401_test.go — #401: a boot whose service-account endpoint cannot be
// built must still reach a readiness decision. In production this is reachable
// with an EMPTY token file or a missing/unreadable ca.crt (an absent token makes
// rest.InClusterConfig fail first, so the watcher is nil and main.go's safety
// net flips). The arm points the token path at an absent file only as the
// hermetic way to make dynamic.ServiceAccountEndpoint fail: the same error
// branch. Before #401, Phase1Warmup returned saErr without
// cache.MarkPhase1Done, and main.go's safety net does not flip when the cache is
// on with a watcher, so /readyz stayed 503 forever.
//
// The arm drives the REAL dispatchers.Phase1Warmup (not phase1WarmupWith) with a
// real watcher installed as cache.Global() and the SA token path pointed at a
// file that does not exist, then reads the REAL /readyz handler.
//
// RED before #401: Phase1Warmup returns, /readyz is still 503, no exit outcome,
// backstop counter unchanged.

package handlers

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	idynamic "github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

func TestIssue401_MissingSAEndpoint_ReachesReadyDegraded(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetPhase1DoneForTest()
	dispatchers.ResetPhase1ReadinessExitForTest()
	t.Cleanup(func() {
		cache.ResetPhase1DoneForTest()
		dispatchers.ResetPhase1ReadinessExitForTest()
	})

	restoreSA := idynamic.SetServiceAccountTokenPathForTest(filepath.Join(t.TempDir(), "absent-token"))
	t.Cleanup(restoreSA)
	if _, err := idynamic.ServiceAccountEndpoint(); err == nil {
		t.Fatal("precondition: the SA endpoint must be unavailable for this arm")
	}

	wctx, wcancel := context.WithCancel(context.Background())
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
	}
	rw, err := cache.NewResourceWatcher(wctx, dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds))
	if err != nil || rw == nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
	})

	if code, _ := readyzRaw(t); code != http.StatusServiceUnavailable {
		t.Fatalf("precondition: /readyz before Phase1Warmup = %d, want 503", code)
	}

	backstopBefore := dispatchers.ReadinessBackstopFired()

	// The phase-1 budget (main.go gives Phase1Warmup PHASE1_TIMEOUT_SECONDS);
	// a short one here. The decision must be reached well inside it.
	budget := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- dispatchers.Phase1Warmup(ctx, &rest.Config{Host: "https://127.0.0.1:1"}, "krateo-system")
	}()
	var warmErr error
	select {
	case warmErr = <-done:
	case <-time.After(budget + 2*time.Second):
		t.Fatal("#401: Phase1Warmup did not return within the phase-1 budget")
	}
	if warmErr == nil {
		t.Fatal("#401: Phase1Warmup must still return the SA-endpoint error for main.go's WARN line")
	}

	code, body := readyzRaw(t)
	if code != http.StatusOK {
		t.Fatalf("#401 RED: a missing SA endpoint left /readyz at %d (never-Ready); want 200 Ready-degraded; body=%v", code, body)
	}
	if body["outcome"] != "boot_aborted" {
		t.Fatalf("#401: ready body outcome = %v, want boot_aborted; body=%v", body["outcome"], body)
	}
	if got := dispatchers.ReadinessBackstopFired() - backstopBefore; got != 1 {
		t.Fatalf("#401: snowplow_readyz_backstop_fired delta = %d, want exactly 1 for one degraded release", got)
	}

	// main.go's safety net / a later flip site must not record a second exit.
	dispatchers.RecordPhase1ReadinessExitNoSeed()
	if got := dispatchers.Phase1ReadinessExitOutcome(); got != "boot_aborted" {
		t.Fatalf("#401: the readiness exit must be recorded exactly once; outcome now %q", got)
	}
}
