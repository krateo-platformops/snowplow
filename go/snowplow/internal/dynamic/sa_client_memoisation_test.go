// sa_client_memoisation_test.go — Ship 0.30.167 falsifier.
//
// LOAD-BEARING for the parallelism regression fix introduced by 0.30.166
// #307 amend. The 0.30.166 dispatcher attach invokes
// idynamic.ServiceAccountRESTConfig() PER REQUEST. The pre-0.30.167
// implementation calls rest.InClusterConfig() afresh on every call —
// rest.InClusterConfig reads the SA token + CA from disk and parses the
// CA PEM each time. Under concurrent /call load this serialises through
// the disk + parse path on every goroutine, defeating the per-request
// parallelism the cache-off path is supposed to deliver.
//
// THE PARALLELISM REGRESSION SURFACE (architect §3 contention mechanism A):
// every cache-off /call dispatch builds a fresh rest.Config + re-reads
// the SA token + re-parses the CA PEM, even though the SA pair is
// IMMUTABLE process-wide. The fix mirrors saEndpointInstance + saEndpointMu
// at sa_client.go:55-58 — memoise the *rest.Config behind a sync.Mutex so
// subsequent calls return the SAME pointer, eliminating the per-request
// disk + parse cost.
//
// Falsifier red/green (Step 3 of the falsifier-first protocol):
//   - PRE-FIX: ServiceAccountRESTConfig() returns a fresh *rest.Config on
//     every call. The pointer-identity test below FAILS — the synthetic
//     singleton plant is the only way to assert post-fix shape, and the
//     unfixed code path simply never reads the cached value.
//   - POST-FIX: ServiceAccountRESTConfig() reads the cached singleton
//     under saRestConfigMu and returns the same pointer on subsequent
//     calls — the pointer-identity test PASSES.

package dynamic

import (
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"k8s.io/client-go/rest"
)

// TestServiceAccountRESTConfig_Memoised pins the memoisation contract:
// repeated calls to ServiceAccountRESTConfig() MUST return the SAME
// *rest.Config pointer on success. The test plants a synthetic singleton
// (the post-fix code path's cached value) and verifies the function
// returns it verbatim without rebuilding via rest.InClusterConfig().
//
// PRE-FIX (un-modified sa_client.go:117-123): the function ignores any
// cached value and calls rest.InClusterConfig() afresh — the planted
// singleton is shadowed, the returned pointer is fresh (or the call
// errors out-of-cluster), and the assertion FAILS.
//
// POST-FIX (memoised pattern mirroring saEndpointInstance at lines
// 55-58): the function reads saRestConfigInstance under saRestConfigMu
// first; if non-nil, returns the cached pointer; the assertion PASSES.
func TestServiceAccountRESTConfig_Memoised(t *testing.T) {
	resetSARestConfigForTest()
	defer resetSARestConfigForTest()

	// Plant a synthetic singleton — the post-fix cached value.
	planted := &rest.Config{
		Host:        "https://kubernetes.default.svc",
		BearerToken: "planted-test-token",
	}
	saRestConfigMu.Lock()
	saRestConfigInstance = planted
	saRestConfigMu.Unlock()

	// Call 1: must return the planted singleton.
	rc1, err := ServiceAccountRESTConfig()
	if err != nil {
		t.Fatalf("ServiceAccountRESTConfig call 1 errored: %v — memoisation NOT engaged "+
			"(pre-fix code path falls through to rest.InClusterConfig which errors "+
			"out-of-cluster, ignoring the planted singleton)", err)
	}
	if rc1 != planted {
		t.Fatalf("ServiceAccountRESTConfig call 1 did not return the planted singleton — "+
			"pre-fix code path builds a fresh *rest.Config per call.\n"+
			"  got:     %p (Host=%q)\n"+
			"  planted: %p (Host=%q)",
			rc1, rc1.Host, planted, planted.Host)
	}

	// Call 2: must return the SAME pointer.
	rc2, err := ServiceAccountRESTConfig()
	if err != nil {
		t.Fatalf("ServiceAccountRESTConfig call 2 errored: %v", err)
	}
	if rc2 != planted {
		t.Fatalf("ServiceAccountRESTConfig call 2 returned a DIFFERENT pointer — "+
			"memoisation broken across calls.\n"+
			"  got:     %p\n"+
			"  planted: %p",
			rc2, planted)
	}

	// Pointer identity transitively: rc1 == rc2.
	if rc1 != rc2 {
		t.Fatalf("ServiceAccountRESTConfig produced two distinct pointers across "+
			"successive calls (rc1=%p rc2=%p) — the singleton is not shared", rc1, rc2)
	}

	// N successive calls — strongest pin against silent regression.
	for i := 0; i < 10; i++ {
		rcN, err := ServiceAccountRESTConfig()
		if err != nil {
			t.Fatalf("ServiceAccountRESTConfig iteration %d errored: %v", i, err)
		}
		if rcN != planted {
			t.Fatalf("ServiceAccountRESTConfig iteration %d returned a different "+
				"pointer (%p) than planted (%p) — memoisation regressed mid-loop",
				i, rcN, planted)
		}
	}
}

// TestServiceAccountRESTConfig_ConcurrentMemoisation pins the race-free
// concurrent-call contract. 8 goroutines × 100 calls each: under -race
// the test MUST NOT report a data race, and every returned pointer MUST
// be the SAME planted singleton.
//
// PRE-FIX: every call invokes rest.InClusterConfig() which performs its
// own internal disk reads + CA parse. While that has its own internal
// synchronization, it doesn't return a cached pointer — every call
// allocates a fresh *rest.Config. Under -race the unfixed path FAILS
// the pointer-identity check (all rc returns are distinct).
//
// POST-FIX: saRestConfigMu serialises the first-call build, subsequent
// calls hit the cached saRestConfigInstance under the same mutex,
// returning the SAME pointer. -race reports clean.
func TestServiceAccountRESTConfig_ConcurrentMemoisation(t *testing.T) {
	resetSARestConfigForTest()
	defer resetSARestConfigForTest()

	planted := &rest.Config{
		Host:        "https://kubernetes.default.svc",
		BearerToken: "planted-concurrent-token",
	}
	saRestConfigMu.Lock()
	saRestConfigInstance = planted
	saRestConfigMu.Unlock()

	const goroutines = 8
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	errs := make(chan error, goroutines*iterations)
	mismatches := make(chan *rest.Config, goroutines*iterations)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				rc, err := ServiceAccountRESTConfig()
				if err != nil {
					errs <- err
					return
				}
				if rc != planted {
					mismatches <- rc
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	close(mismatches)

	for err := range errs {
		t.Errorf("concurrent ServiceAccountRESTConfig errored: %v — memoisation not "+
			"engaged (pre-fix path falls through to rest.InClusterConfig which "+
			"errors out-of-cluster)", err)
	}
	for mismatch := range mismatches {
		t.Errorf("concurrent ServiceAccountRESTConfig returned a DIFFERENT pointer "+
			"(%p) than planted (%p) — memoisation broken under concurrent load",
			mismatch, planted)
	}
}

// TestServiceAccountRESTConfig_DisablesRateLimit pins the 0.30.168 contract
// (no client-side rate limit on the SA config, so server-side Priority &
// Fairness is authoritative; docs/parallelism-2x-ceiling-diagnosis-2026-05-22.md
// §6, §7 F1) at its #920 home. Before #920 this function set rc.QPS=-1 itself;
// it now takes the posture from credhttp.InClusterConfig, which applies it to
// every config snowplow builds, and credhttp's own tests prove that every client
// built from that config has no limiter
// (TestIssue920_EveryClientFromEveryConstructorHasNoClientRateLimit).
//
// What remains to prove HERE is the wiring: the singleton is built by
// credhttp.InClusterConfig and by nothing else. A seam pointed at another
// builder (for example a raw rest.InClusterConfig, whose QPS of 0 means 5/s)
// would bring the limit back, and this test goes RED.
func TestServiceAccountRESTConfig_DisablesRateLimit(t *testing.T) {
	got := reflect.ValueOf(inClusterConfigFn).Pointer()
	want := reflect.ValueOf(credhttp.InClusterConfig).Pointer()
	if got != want {
		t.Fatalf("the SA singleton is not built by credhttp.InClusterConfig (seam points at %s); "+
			"only that constructor removes client-go's 5/s client-side limit (#920)",
			runtime.FuncForPC(got).Name())
	}
}

// TestServiceAccountRESTConfig_ErrorPathDoesNotPoisonSingleton is the
// symmetric counterpart to TestServiceAccountEndpoint_ErrorPathDoesNotPoisonSingleton
// at sa_client_test.go:41-60 — locks in the contract that a transient
// rest.InClusterConfig failure (e.g., missing SA volume at boot) must
// NOT be cached, so a subsequent successful call can populate the
// singleton.
//
// Pre-fix (no memoisation): trivially passes — every call goes through
// rest.InClusterConfig, no singleton involvement.
//
// Post-fix: the implementation MUST only set saRestConfigInstance on
// successful build, never on error. The test exercises the error path
// out-of-cluster and asserts the singleton stays nil.
func TestServiceAccountRESTConfig_ErrorPathDoesNotPoisonSingleton(t *testing.T) {
	resetSARestConfigForTest()
	defer resetSARestConfigForTest()

	// Out-of-cluster: rest.InClusterConfig fails (no KUBERNETES_SERVICE_HOST/PORT).
	// Skip if the test host is itself a pod with the env set.
	rc, err := ServiceAccountRESTConfig()
	if err == nil {
		t.Skipf("test environment has a usable in-cluster rest.Config (rc=%+v); "+
			"cannot exercise the error-path-no-poison invariant", rc)
	}

	// Singleton MUST still be nil — error path did not cache.
	saRestConfigMu.Lock()
	got := saRestConfigInstance
	saRestConfigMu.Unlock()

	if got != nil {
		t.Fatalf("rest.InClusterConfig error path poisoned the singleton "+
			"(saRestConfigInstance=%+v); a future successful boot would never "+
			"re-attempt the build and the SA *rest.Config would stay broken", got)
	}

	// A second call must still error (no cached success to short-circuit).
	if _, err := ServiceAccountRESTConfig(); err == nil {
		t.Fatalf("second call after error path succeeded — the error path "+
			"must NOT cache anything, but the second call returned success")
	}
}
