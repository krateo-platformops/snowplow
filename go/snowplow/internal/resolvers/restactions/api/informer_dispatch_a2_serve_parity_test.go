// informer_dispatch_a2_serve_parity_test.go — 1.12.8 audit A2 SERVE-PATH
// parity falsifier: the gate must remove CHURN and nothing else.
//
// A2 stops registering informers for resources discovery declares without
// list+watch. The claim that makes it safe is that the customer's SERVED
// CONTENT is unaffected, and that claim is not self-evident: A2 changes what
// the cache does at registration time, and the cache is on the serve path.
//
// TWO HALVES, because each catches a different way of being wrong:
//
//	PART 1 — the REFUSED coordinate. Before A2 it registered and was never
//	         servable; after A2 it never registers. Both must produce the
//	         IDENTICAL dispatch result — (nil, false), i.e. the cache
//	         contributes ZERO bytes and 100% of the customer's content comes
//	         from the unchanged apiserver fall-through. Catches "the gate
//	         started serving/withholding something for this coordinate".
//	PART 2 — a HEALTHY coordinate. Its served bytes must be equivalent with
//	         the gate ON and OFF. Catches the far worse failure: a gate whose
//	         blast radius leaks onto resources it was never meant to touch.
//	         PART 1 alone cannot see that — (nil,false) == (nil,false) is
//	         satisfied by a build that has stopped serving EVERYTHING.
//
// THE "BEFORE" WORLD IS REAL, NOT SIMULATED. It is produced two ways at once:
// SKIP_UNSERVED_GROUP_INFORMERS=false restores pre-A2 register-unconditionally,
// and a WATCH REACTOR makes the fake apiserver REFUSE the watch exactly as a
// verbs:["get","list"] resource does. So the reflector really does LIST and
// then fail WATCH, the production WatchErrorHandler really fires, and conjunct
// 3 (WatchHealthy) really goes false — the state 057 is in today. The test
// asserts that state was reached before it measures the serve result, so it
// cannot pass by accident on a merely not-yet-synced informer
// (feedback_falsifier_must_drive_real_boundary_not_install_crossed_state).
//
// The dispatch is driven through dispatchViaInformer, the same seam the
// existing informer_dispatch tests use — the real serve path, not a seam
// built for this test.

package api

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// a2ParityUnwatchableGVR is the REAL 057 coordinate and the REAL verb set:
// metrics.k8s.io/v1beta1 pods advertises ["get","list"] with no `watch`.
var a2ParityUnwatchableGVR = schema.GroupVersionResource{
	Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods",
}

const (
	a2ParityUnwatchablePath = "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods"
	a2ParityHealthyPath     = "/apis/templates.krateo.io/v1/namespaces/default/restactions"
)

// a2ParityDisco reports the two wire shapes this test needs, and reports every
// group in play as SERVED so the #119 GROUP gate stays inert and the RESOURCE
// gate is the only thing under measurement.
type a2ParityDisco struct{}

func (a2ParityDisco) ServerGroups() (*metav1.APIGroupList, error) {
	out := &metav1.APIGroupList{}
	for _, g := range []string{
		"rbac.authorization.k8s.io", "templates.krateo.io", "metrics.k8s.io",
	} {
		out.Groups = append(out.Groups, metav1.APIGroup{Name: g})
	}
	return out, nil
}

func (a2ParityDisco) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	list := &metav1.APIResourceList{GroupVersion: gv}
	switch gv {
	case "metrics.k8s.io/v1beta1":
		// The live shape: get+list, NO watch.
		list.APIResources = []metav1.APIResource{{
			Name: "pods", Namespaced: true, Kind: "PodMetrics",
			Verbs: metav1.Verbs{"get", "list"},
		}}
	case "templates.krateo.io/v1":
		// A fully watchable customer resource — the control.
		list.APIResources = []metav1.APIResource{{
			Name: "restactions", Namespaced: true, Kind: "RestAction",
			Verbs: metav1.Verbs{"get", "list", "watch"},
		}}
	}
	return list, nil
}

// errA2ParityWatchRefused is what an apiserver effectively tells a reflector
// that asks to WATCH a resource declaring only get+list.
var errA2ParityWatchRefused = errors.New("a2-parity: the apiserver does not support watch on this resource")

// newA2ParityWatcher builds a watcher in one of the two worlds.
//
// gateOn=false is the genuine PRE-A2 world: the kill-switch is off, so the GVR
// registers unconditionally, AND the fake apiserver refuses its WATCH, so the
// informer reaches the never-servable state that 057 is in today.
func newA2ParityWatcher(t *testing.T, gateOn bool, seed ...runtime.Object) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	if gateOn {
		t.Setenv("SKIP_UNSERVED_GROUP_INFORMERS", "true")
	} else {
		t.Setenv("SKIP_UNSERVED_GROUP_INFORMERS", "false")
	}

	// Both gate memos are process-global; reset so each world reads its own
	// discovery double rather than the previous world's.
	cache.ResetServedGroupsMemoForTest()
	cache.ResetResourceVerbsMemoForTest()
	t.Cleanup(cache.ResetServedGroupsMemoForTest)
	t.Cleanup(cache.ResetResourceVerbsMemoForTest)

	listKinds := dispatchTestListKinds()
	listKinds[a2ParityUnwatchableGVR] = "PodMetricsList"

	seed = append(seed, dispatchAdminRBACSeed()...)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		dispatchTestScheme(), listKinds, seed...)

	// THE REAL MECHANISM: refuse WATCH for this resource only, so the
	// reflector LISTs and then fails WATCH exactly as it does on 057. Scoped
	// to "pods" so the RBAC informers — which must sync for the dispatch's
	// per-item filter — are untouched.
	dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
		return true, nil, errA2ParityWatchRefused
	})

	rw, err := cache.NewResourceWatcher(t.Context(), dyn)
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

	// Wired AFTER construction: the constructor's eager RBAC registration must
	// not be exposed to this double (it runs with disco==nil and therefore
	// fail-safe registers, which is the production ordering too).
	rw.SetDiscoveryClient(a2ParityDisco{})

	added, syncCh := rw.EnsureResourceType(dispatchTestGVR)
	if !added {
		t.Fatalf("setup: the healthy control GVR must register in BOTH worlds; got added=false")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("setup: healthy informer did not sync within 5s")
	}

	syncCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(syncCtx, 5*time.Second); err != nil {
		t.Fatalf("setup: WaitForCacheSync (RBAC informers): %v", err)
	}

	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// TestA2_ServePathParity_RefusedCoordinateAndHealthyNeighbour is PART 1 + PART 2.
func TestA2_ServePathParity_RefusedCoordinateAndHealthyNeighbour(t *testing.T) {
	seed := []runtime.Object{
		newTestRestActionRuntimeObject("default", "a", "alpha"),
		newTestRestActionRuntimeObject("default", "b", "bravo"),
	}

	// --- WORLD "BEFORE" (pre-A2): registers, then WATCH fails for real. ----
	rwBefore := newA2ParityWatcher(t, false, seed...)

	// First dispatch drives the lazy register (the real spawn-site funnel).
	_, _ = dispatchViaInformer(dispatchCtx(), buildCall(http.MethodGet, a2ParityUnwatchablePath))

	if !rwBefore.IsRegistered(a2ParityUnwatchableGVR) {
		t.Fatalf("PRE-A2 world invalid: with SKIP_UNSERVED_GROUP_INFORMERS=false the unwatchable "+
			"GVR MUST register unconditionally — that is the state this arm compares against. "+
			"snap=%+v", rwBefore.ServabilitySnapshotFor(a2ParityUnwatchableGVR))
	}

	// Wait for the REAL watch failure to land (conjunct 3 goes false). Without
	// this the (nil,false) below could come from the not-yet-synced gate
	// instead of the never-servable state, and the arm would prove nothing.
	deadline := time.Now().Add(5 * time.Second)
	var snapBefore cache.ServabilitySnapshot
	for time.Now().Before(deadline) {
		snapBefore = rwBefore.ServabilitySnapshotFor(a2ParityUnwatchableGVR)
		if snapBefore.Registered && !snapBefore.WatchHealthy {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !snapBefore.Registered || snapBefore.WatchHealthy {
		t.Fatalf("PRE-A2 world invalid: the reflector's WATCH must actually FAIL so conjunct 3 "+
			"(WatchHealthy) goes false — that is the 057 state being compared against, and this "+
			"arm must not measure a merely not-yet-synced informer. snap=%+v", snapBefore)
	}
	if rwBefore.IsServable(a2ParityUnwatchableGVR) {
		t.Fatalf("PRE-A2 world invalid: a registered GVR whose WATCH failed must NOT be servable; "+
			"snap=%+v", snapBefore)
	}

	rawBefore, servedBefore := dispatchViaInformer(
		dispatchCtx(), buildCall(http.MethodGet, a2ParityUnwatchablePath))

	healthyBefore, healthyServedBefore := dispatchViaInformer(
		dispatchCtx(), buildCall(http.MethodGet, a2ParityHealthyPath))
	if !healthyServedBefore {
		t.Fatalf("PRE-A2 world invalid: the HEALTHY control coordinate must serve from the "+
			"informer, otherwise PART 2 compares two non-answers; raw=%q", string(healthyBefore))
	}

	// --- WORLD "AFTER" (A2 gate on): never registers. ---------------------
	rwAfter := newA2ParityWatcher(t, true, seed...)

	_, _ = dispatchViaInformer(dispatchCtx(), buildCall(http.MethodGet, a2ParityUnwatchablePath))

	if rwAfter.IsRegistered(a2ParityUnwatchableGVR) {
		t.Fatalf("A2 world invalid: the unwatchable GVR must NOT be registered with the gate on; "+
			"snap=%+v", rwAfter.ServabilitySnapshotFor(a2ParityUnwatchableGVR))
	}

	rawAfter, servedAfter := dispatchViaInformer(
		dispatchCtx(), buildCall(http.MethodGet, a2ParityUnwatchablePath))

	healthyAfter, healthyServedAfter := dispatchViaInformer(
		dispatchCtx(), buildCall(http.MethodGet, a2ParityHealthyPath))

	// --- PART 1: the refused coordinate serves IDENTICALLY. ---------------
	if servedBefore != servedAfter {
		t.Fatalf("PART 1 FAIL: the serve DECISION for the refused coordinate changed: "+
			"before served=%v, after served=%v. A2 must change churn, not what the customer gets",
			servedBefore, servedAfter)
	}
	if servedAfter {
		t.Fatalf("PART 1 FAIL: the refused coordinate must fall through to the apiserver " +
			"(served=false) in BOTH worlds; got served=true")
	}
	if len(rawBefore) != 0 || len(rawAfter) != 0 {
		t.Fatalf("PART 1 FAIL: the cache must contribute ZERO bytes for this coordinate in both "+
			"worlds, so 100%% of the served content comes from the unchanged apiserver "+
			"fall-through; got before=%d bytes, after=%d bytes", len(rawBefore), len(rawAfter))
	}

	// --- PART 2: the healthy neighbour's CONTENT is unchanged. ------------
	if !healthyServedAfter {
		t.Fatalf("PART 2 FAIL: the gate leaked onto a fully watchable resource — it served from " +
			"the informer before A2 and does not after. This is the blast-radius failure PART 1 " +
			"cannot see")
	}
	if !equivalentListEnvelopes(healthyBefore, healthyAfter) {
		t.Fatalf("PART 2 FAIL: the SERVED CONTENT of a healthy coordinate differs across the "+
			"gate.\nbefore: %s\nafter:  %s", string(healthyBefore), string(healthyAfter))
	}
}
