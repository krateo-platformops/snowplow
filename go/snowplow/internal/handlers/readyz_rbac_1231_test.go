package handlers

// readyz_rbac_1231_test.go — #1231: Ready requires the complete RBAC snapshot.
//
// A real ResourceWatcher over a fake dynamic client whose RBAC LISTs the arm
// holds open, with Phase 1 already done (as it is once main.go's 60 s
// WaitForCacheSync times out and Phase 1 returns). Through the real /readyz and
// /health handlers:
//
//	R1  with all four LISTs held for longer than main.go's 60 s wait, /readyz
//	    is never 200, and names what it waits on; /health is 200 throughout.
//	R2  each of the four LISTs, held alone, keeps /readyz 503.
//	R3  /readyz is 200 as soon as the last LIST completes.
//	R4  without a watcher (cache off) readiness is Phase1Done's alone.
//	R5  Ready waits for the bindings-index build #819 owes before Ready, which
//	    the first complete publish runs when Phase 1 flipped first.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

var x1231RBAC = []string{"roles", "rolebindings", "clusterroles", "clusterrolebindings"}

// x1231Watcher builds the process watcher with the LISTs of held refused until
// released, Phase 1 done, and no snapshot published. release(r) lets r's next
// LIST through.
func x1231Watcher(t *testing.T, held ...string) (release func(string)) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	kinds := map[schema.GroupVersionResource]string{}
	for _, r := range x1231RBAC {
		kinds[schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}] = map[string]string{
			"roles": "RoleList", "rolebindings": "RoleBindingList",
			"clusterroles": "ClusterRoleList", "clusterrolebindings": "ClusterRoleBindingList",
		}[r]
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, kinds)
	var mu sync.Mutex
	open := map[string]bool{}
	for _, r := range held {
		r := r
		open[r] = true
		dyn.PrependReactor("list", r, func(k8stesting.Action) (bool, runtime.Object, error) {
			mu.Lock()
			defer mu.Unlock()
			if !open[r] {
				return false, nil, nil
			}
			return true, nil, errors.New(r + " LIST held by the test (#1231)")
		})
	}

	prev := cache.RBACSnapshotForTest()
	cache.PublishRBACSnapshotForTest(nil)
	cache.ResetRBACCompleteForTest()
	cache.ResetBindingsByGVRIndexForTest() // the index is process-global; an earlier test may have built it
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	cache.SetGlobal(rw)
	cache.ResetPhase1DoneForTest()
	dispatchers.ResetPhase1ReadinessExitForTest()
	dispatchers.RecordPhase1ReadinessExitNoSeed()
	cache.MarkPhase1Done()
	t.Cleanup(func() {
		cache.SetGlobal(nil)
		rw.Stop()
		cache.PublishRBACSnapshotForTest(prev)
		cache.ResetRBACCompleteForTest()
		cache.ResetBindingsByGVRIndexForTest()
		cache.ResetPhase1DoneForTest()
		dispatchers.ResetPhase1ReadinessExitForTest()
	})
	return func(r string) {
		mu.Lock()
		open[r] = false
		mu.Unlock()
	}
}

type x1231Probe struct {
	code int
	body readyzInfo
}

func x1231Ready(t *testing.T) x1231Probe {
	t.Helper()
	rec := httptest.NewRecorder()
	ReadyCheck()(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var b readyzInfo
	if err := json.NewDecoder(rec.Body).Decode(&b); err != nil {
		t.Fatalf("decode /readyz: %v", err)
	}
	return x1231Probe{rec.Code, b}
}

func x1231Live(t *testing.T) int {
	t.Helper()
	rec := httptest.NewRecorder()
	HealthCheck("", "", nil)(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	return rec.Code
}

// x1231Hold samples both probes for d: /readyz must never be 200, /health
// always is. Returns the last /readyz answer.
func x1231Hold(t *testing.T, d time.Duration) x1231Probe {
	t.Helper()
	var last x1231Probe
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(250 * time.Millisecond) {
		last = x1231Ready(t)
		if last.code == http.StatusOK {
			t.Fatalf("RED: /readyz is 200 before the initial RBAC publish is done (a LIST held, or the publish unfinished): %+v", last.body)
		}
		if code := x1231Live(t); code != http.StatusOK {
			t.Fatalf("RED: /health is %d while the RBAC sync is slow; liveness must not depend on it", code)
		}
	}
	return last
}

// x1231PromptAfter bounds how long after the last informer reports synced the
// pod may still be NotReady: the complete snapshot is published by one
// goroutine woken by that sync, so it is a scheduling delay, not a wait.
const x1231PromptAfter = 5 * time.Second

// x1231ReadyAfterSync waits for every RBAC informer to sync (the reflector
// retries a refused LIST with backoff, so this can take tens of seconds) and
// then for /readyz to turn 200. /readyz must not be 200 before the last informer
// synced, and must be 200 within x1231PromptAfter of it. /health stays 200.
func x1231ReadyAfterSync(t *testing.T) time.Duration {
	t.Helper()
	var syncedAt time.Time
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if syncedAt.IsZero() && len(cache.UnsyncedRBACInformers()) == 0 {
			syncedAt = time.Now()
		}
		p := x1231Ready(t)
		if code := x1231Live(t); code != http.StatusOK {
			t.Fatalf("RED: /health is %d during the RBAC sync", code)
		}
		if p.code != http.StatusOK {
			if !syncedAt.IsZero() && time.Since(syncedAt) > x1231PromptAfter {
				t.Fatalf("RED: every RBAC informer synced %s ago and /readyz is still %d: %+v", time.Since(syncedAt), p.code, p.body)
			}
			continue
		}
		if syncedAt.IsZero() {
			t.Fatalf("RED: /readyz is 200 before every RBAC informer synced: %+v", p.body)
		}
		if !p.body.RBACSnapshotComplete || p.body.Status != "ready" || len(p.body.UnsyncedRBACInformers) != 0 {
			t.Fatalf("200 with an incomplete body: %+v", p.body)
		}
		return time.Since(syncedAt)
	}
	t.Fatalf("the RBAC informers did not sync within 2 minutes of release: %v", cache.UnsyncedRBACInformers())
	return 0
}

// R1 + R3: past main.go's 60 s wait, with every RBAC LIST held, the pod is not
// Ready, says why, and stays alive. Released one by one, it is not Ready until
// the last one completes, and is Ready then.
func TestIssue1231_NotReadyPastSixtySecondsWhileRBACListsAreHeld(t *testing.T) {
	release := x1231Watcher(t, x1231RBAC...)
	last := x1231Hold(t, 61*time.Second)
	if last.body.Reason != ReasonRBACSnapshotIncomplete || !last.body.Phase1Done || last.body.RBACSnapshotComplete {
		t.Fatalf("RED: the warming body does not say it waits on RBAC: %+v", last.body)
	}
	got := append([]string(nil), last.body.UnsyncedRBACInformers...)
	sort.Strings(got)
	want := append([]string(nil), x1231RBAC...)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("RED: the body names %v as unsynced, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RED: the body names %v as unsynced, want %v", got, want)
		}
	}

	// Release all but the last and wait for them to sync: still not Ready.
	for _, r := range x1231RBAC[:len(x1231RBAC)-1] {
		release(r)
	}
	lastHeld := x1231RBAC[len(x1231RBAC)-1]
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(250 * time.Millisecond) {
		u := cache.UnsyncedRBACInformers()
		if len(u) == 1 && u[0] == lastHeld {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the released informers did not sync: %v", u)
		}
		if p := x1231Ready(t); p.code == http.StatusOK {
			t.Fatalf("RED: /readyz is 200 while %s is held: %+v", lastHeld, p.body)
		}
	}
	if p := x1231Hold(t, time.Second); len(p.body.UnsyncedRBACInformers) != 1 || p.body.UnsyncedRBACInformers[0] != lastHeld {
		t.Fatalf("the body names %v as unsynced, want [%s]", p.body.UnsyncedRBACInformers, lastHeld)
	}
	release(lastHeld)
	t.Logf("Ready %s after the last RBAC informer synced", x1231ReadyAfterSync(t))
	if code := x1231Live(t); code != http.StatusOK {
		t.Fatalf("/health is %d once Ready", code)
	}
}

// R2: each of the four LISTs, held alone, keeps the pod NotReady; it is Ready as
// soon as that one completes.
func TestIssue1231_EachRBACListAloneKeepsThePodNotReady(t *testing.T) {
	for _, held := range x1231RBAC {
		t.Run(held, func(t *testing.T) {
			release := x1231Watcher(t, held)
			p := x1231Hold(t, 2*time.Second)
			if len(p.body.UnsyncedRBACInformers) != 1 || p.body.UnsyncedRBACInformers[0] != held {
				t.Fatalf("RED: the body names %v as unsynced, want [%s]", p.body.UnsyncedRBACInformers, held)
			}
			release(held)
			x1231ReadyAfterSync(t)
		})
	}
}

// R5: between the complete publish and the deferred bindings-index build the pod
// is not Ready; it is once the build has run.
func TestIssue1231_ReadyWaitsForTheDeferredBindingsIndexBuild(t *testing.T) {
	entered := make(chan struct{})
	proceed := make(chan struct{})
	restore := cache.SetInitialPublishBeforeIndexHookForTest(func() {
		close(entered)
		<-proceed
	})
	t.Cleanup(restore)
	release := x1231Watcher(t, "roles")
	// Registered after the watcher, so it runs before the watcher stops: a
	// failing assertion below must not leave the publish goroutine blocked in
	// the hook, or the watcher's Stop would wait on it forever.
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(proceed) }) })
	if cache.BindingsByGVRIndexBuilt() {
		t.Fatal("PRE: the index must not be built before the first complete snapshot")
	}
	release("roles")
	select {
	case <-entered:
	case <-time.After(2 * time.Minute):
		t.Fatal("the initial publish never reached the deferred build")
	}
	if s := cache.RBACSnapshotForTest(); s == nil || !s.Synced {
		t.Fatalf("PRE: the complete snapshot is live at the hook: %+v", s)
	}
	p := x1231Hold(t, time.Second)
	if p.body.Reason != ReasonRBACSnapshotIncomplete {
		t.Fatalf("the warming reason in the gap: %+v", p.body)
	}
	unblock.Do(func() { close(proceed) })
	x1231ReadyAfterSync(t)
	if !cache.BindingsByGVRIndexBuilt() {
		t.Fatal("RED: Ready before the bindings-index build #819 owes")
	}
}

// R4: without a watcher there is no in-process evaluator to wait for.
func TestIssue1231_WithoutAWatcherReadinessIsPhase1Alone(t *testing.T) {
	cache.SetGlobal(nil)
	cache.ResetPhase1DoneForTest()
	dispatchers.ResetPhase1ReadinessExitForTest()
	t.Cleanup(func() {
		cache.ResetPhase1DoneForTest()
		dispatchers.ResetPhase1ReadinessExitForTest()
	})
	if p := x1231Ready(t); p.code != http.StatusServiceUnavailable {
		t.Fatalf("PRE: before Phase 1, /readyz is %d", p.code)
	}
	dispatchers.RecordPhase1ReadinessExitNoSeed()
	cache.MarkPhase1Done()
	if p := x1231Ready(t); p.code != http.StatusOK || !p.body.RBACSnapshotComplete {
		t.Fatalf("RED: with no watcher and Phase 1 done, /readyz is %d: %+v", p.code, p.body)
	}
}
