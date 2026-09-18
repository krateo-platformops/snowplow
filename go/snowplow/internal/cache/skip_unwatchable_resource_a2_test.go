// skip_unwatchable_resource_a2_test.go — audit A2 unwatchable-RESOURCE
// pre-check falsifier (1.12.8).
//
// THE DEFECT: EnsureResourceType gated only on whether the GROUP is served
// (#119, groupAuthoritativelyAbsent), never on what the RESOURCE can actually
// DO. metrics.k8s.io/v1beta1 pods+nodes advertise ["get","list"] with NO
// `watch`: the reflector LISTs fine and then fails WATCH forever, conjunct 3
// (watchBroken) never clears, the GVR is never servable, and every dep-event
// probe on its coordinates is structurally unanswerable. Measured on 057 over
// ~14 minutes: +236,300 dirty-marks, +236,329 refresher enqueues, +0 evictions,
// with 92.5% of ALL dep-event probes returning UNKNOWN.
//
// THE FIX: resourceAuthoritativelyUnwatchable (servable.go), ||-joined with the
// existing group gate via unregisterableReason, using client-go's own
// discovery.SupportsAllVerbs predicate.
//
// THE CRUX — fail-safe open on uncertainty, IDENTICAL to the #119 neighbour.
// EXACTLY ONE state skips: resource PRESENT in a SUCCESSFUL list, Verbs
// non-empty, verbs lacking list+watch. Every other state REGISTERS: nil disco,
// discovery error, nil list, resource ABSENT, empty Verbs, verbs with
// list+watch. A false-skip of a genuinely watchable resource silently drops
// real data forever — strictly worse than the churn being removed.
//
// The ABSENT arm is the one that discriminates. A "skip everything" or
// blanket-refusing implementation passes the metrics arm AND the watchable arm
// can be made to pass by inverting, but reading ABSENT as unwatchable converts
// this fail-safe-OPEN check into a fail-safe-CLOSED one and breaks the S4
// post-startup-CRD heal path (#50/#116).

package cache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// --- the live wire shapes, transcribed from the cluster ---------------------

// a2MetricsPods / a2MetricsNodes are the REAL coordinates and the REAL verb
// sets captured from 057 via `kubectl get --raw /apis/metrics.k8s.io/v1beta1`:
//
//	{"name":"nodes","namespaced":false,"kind":"NodeMetrics","verbs":["get","list"]}
//	{"name":"pods", "namespaced":true, "kind":"PodMetrics", "verbs":["get","list"]}
var (
	a2MetricsPods = schema.GroupVersionResource{
		Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods",
	}
	a2MetricsNodes = schema.GroupVersionResource{
		Group: "metrics.k8s.io", Version: "v1beta1", Resource: "nodes",
	}
	// a2SAR is the stray reflector the same gate should also drop: its only
	// verb is `create` (`kubectl get --raw /apis/authorization.k8s.io/v1`).
	a2SAR = schema.GroupVersionResource{
		Group: "authorization.k8s.io", Version: "v1", Resource: "subjectaccessreviews",
	}
	// a2Watchable is a normal, fully-watchable custom resource under a SERVED
	// group — the anti-trivial control. Same group as the unwatchable ones is
	// not required; what matters is that its verbs include list+watch.
	a2Watchable = schema.GroupVersionResource{
		Group: "served.krateo.io", Version: "v1", Resource: "watchables",
	}
	// a2Absent is under a SERVED group but is NOT in discovery's resource list
	// — the post-startup-CRD shape that MUST still register.
	a2Absent = schema.GroupVersionResource{
		Group: "served.krateo.io", Version: "v1", Resource: "notyetpublished",
	}
	// a2NoVerbs is present in the list but declares NO verbs — aggregated
	// servers sometimes omit the field. "No information" is not "cannot watch".
	a2NoVerbs = schema.GroupVersionResource{
		Group: "served.krateo.io", Version: "v1", Resource: "verbless",
	}
)

func a2ListKinds() map[schema.GroupVersionResource]string {
	m := servableListKinds()
	m[a2MetricsPods] = "PodMetricsList"
	m[a2MetricsNodes] = "NodeMetricsList"
	m[a2SAR] = "SubjectAccessReviewList"
	m[a2Watchable] = "WatchableList"
	m[a2Absent] = "NotYetPublishedList"
	m[a2NoVerbs] = "VerblessList"
	return m
}

// a2Disco is the discovery double. Unlike skip119Disco it carries the VERBS
// each resource advertises, which is the axis under test. Every group it is
// asked about is reported SERVED, so the #119 group gate never fires and these
// arms isolate the RESOURCE-level decision.
type a2Disco struct {
	mu sync.Mutex
	// verbs: gv -> resourceName -> verbs. A resource ABSENT from the inner map
	// is absent from the returned list (the post-startup-CRD shape).
	verbs map[string]map[string][]string
	// resErr, when non-nil, makes ServerResourcesForGroupVersion return it.
	resErr error
	// nilList, when true, makes it return (nil, nil) — the nil-list shape.
	nilList  bool
	resCalls int
}

func (d *a2Disco) ServerGroups() (*metav1.APIGroupList, error) {
	d.mu.Lock()
	names := make([]string, 0, len(d.verbs))
	for gv := range d.verbs {
		names = append(names, gv)
	}
	d.mu.Unlock()

	// Report EVERY group these arms use as served, so the group gate is inert
	// and the resource gate is what is being measured.
	list := &metav1.APIGroupList{}
	for _, g := range []string{"metrics.k8s.io", "authorization.k8s.io", "served.krateo.io"} {
		list.Groups = append(list.Groups, metav1.APIGroup{Name: g})
	}
	_ = names
	return list, nil
}

func (d *a2Disco) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	d.mu.Lock()
	d.resCalls++
	err := d.resErr
	nilList := d.nilList
	// Copy the inner map under the lock — the confirm path calls this from a
	// background goroutine, so handing out the live map races (the lesson
	// skip119Disco records at its own copy site).
	res := make(map[string][]string, len(d.verbs[gv]))
	for name, vs := range d.verbs[gv] {
		res[name] = append([]string(nil), vs...)
	}
	d.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if nilList {
		return nil, nil
	}

	list := &metav1.APIResourceList{GroupVersion: gv}
	for name, vs := range res {
		list.APIResources = append(list.APIResources, metav1.APIResource{
			Name: name, Namespaced: true, Kind: "X", Verbs: metav1.Verbs(vs),
		})
	}
	return list, nil
}

func (d *a2Disco) setVerbs(gv, resource string, verbs []string) {
	d.mu.Lock()
	if d.verbs == nil {
		d.verbs = map[string]map[string][]string{}
	}
	if d.verbs[gv] == nil {
		d.verbs[gv] = map[string][]string{}
	}
	d.verbs[gv][resource] = verbs
	d.mu.Unlock()
}

func (d *a2Disco) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.resCalls
}

func newA2Watcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	// Both memos are process-global; reset so each arm reads its own double.
	cache.ResetServedGroupsMemoForTest()
	cache.ResetResourceVerbsMemoForTest()
	t.Cleanup(cache.ResetServedGroupsMemoForTest)
	t.Cleanup(cache.ResetResourceVerbsMemoForTest)

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), a2ListKinds())
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

// a2FullDisco builds the double with the live wire shapes for every arm.
func a2FullDisco() *a2Disco {
	d := &a2Disco{}
	// The real metrics shape: get+list, NO watch.
	d.setVerbs("metrics.k8s.io/v1beta1", "pods", []string{"get", "list"})
	d.setVerbs("metrics.k8s.io/v1beta1", "nodes", []string{"get", "list"})
	// The real SAR shape: create only.
	d.setVerbs("authorization.k8s.io/v1", "subjectaccessreviews", []string{"create"})
	// A normal watchable resource.
	d.setVerbs("served.krateo.io/v1", "watchables", []string{"get", "list", "watch"})
	// Present but verb-less.
	d.setVerbs("served.krateo.io/v1", "verbless", []string{})
	// a2Absent is deliberately NOT set — absent from the list.
	return d
}

// --- ARM 1: the RED arm. The real metrics wire shape is NOT registered. -----
func TestA2_Arm1_UnwatchableMetricsNotRegistered(t *testing.T) {
	rw := newA2Watcher(t)
	rw.SetDiscoveryClient(a2FullDisco())

	for _, gvr := range []schema.GroupVersionResource{a2MetricsPods, a2MetricsNodes} {
		added, _ := rw.EnsureResourceType(gvr)
		if added {
			t.Fatalf("ARM1 FAIL: %s advertises [get list] with no watch — want added=false, got true", gvr)
		}
		if rw.IsRegistered(gvr) {
			t.Fatalf("ARM1 FAIL: %s must NOT be registered; an informer on it can never become servable", gvr)
		}
	}
}

// --- ARM 2: ANTI-TRIVIAL. A watchable resource IS registered. ---------------
//
// Without this, "skip everything" passes ARM1. The gate must DISCRIMINATE, not
// blanket-refuse.
func TestA2_Arm2_WatchableResourceStillRegisters(t *testing.T) {
	rw := newA2Watcher(t)
	rw.SetDiscoveryClient(a2FullDisco())

	added, _ := rw.EnsureResourceType(a2Watchable)
	if !added {
		t.Fatalf("ARM2 FAIL: %s advertises [get list watch] — want added=true, got false", a2Watchable)
	}
	if !rw.IsRegistered(a2Watchable) {
		t.Fatalf("ARM2 FAIL: %s must be registered", a2Watchable)
	}
}

// --- ARM 3: THE DISCRIMINATING ARM. Absent != unwatchable. -----------------
//
// A resource ABSENT from a SUCCESSFUL discovery list is the post-startup-CRD
// shape (#50/#116): discovery has not published it yet. It MUST register so the
// confirm ticker can heal it. Reading absence as "cannot watch" turns this
// fail-safe-OPEN check into a fail-safe-CLOSED one and silently drops a real
// CRD forever. This is the arm a blanket-refusing implementation fails.
func TestA2_Arm3_AbsentResourceStillRegisters(t *testing.T) {
	rw := newA2Watcher(t)
	rw.SetDiscoveryClient(a2FullDisco())

	added, _ := rw.EnsureResourceType(a2Absent)
	if !added {
		t.Fatalf("ARM3 FAIL: %s is ABSENT from a successful discovery list (a not-yet-published CRD) "+
			"— it MUST register; absent is not unwatchable", a2Absent)
	}
	if !rw.IsRegistered(a2Absent) {
		t.Fatalf("ARM3 FAIL: %s must be registered", a2Absent)
	}
}

// --- ARM 4: fail-safe open — EMPTY verbs register. --------------------------
func TestA2_Arm4_EmptyVerbsStillRegisters(t *testing.T) {
	rw := newA2Watcher(t)
	rw.SetDiscoveryClient(a2FullDisco())

	added, _ := rw.EnsureResourceType(a2NoVerbs)
	if !added {
		t.Fatalf("ARM4 FAIL: %s declares NO verbs — that is no information, not 'cannot watch'; "+
			"it must register", a2NoVerbs)
	}
}

// --- ARM 5: fail-safe open — discovery ERROR registers. ---------------------
func TestA2_Arm5_DiscoveryErrorStillRegisters(t *testing.T) {
	rw := newA2Watcher(t)
	d := a2FullDisco()
	d.resErr = errA2Discovery
	rw.SetDiscoveryClient(d)

	added, _ := rw.EnsureResourceType(a2MetricsPods)
	if !added {
		t.Fatalf("ARM5 FAIL: discovery errored — uncertainty must NEVER skip, want added=true")
	}
}

// --- ARM 6: fail-safe open — a NIL list registers. --------------------------
func TestA2_Arm6_NilListStillRegisters(t *testing.T) {
	rw := newA2Watcher(t)
	d := a2FullDisco()
	d.nilList = true
	rw.SetDiscoveryClient(d)

	added, _ := rw.EnsureResourceType(a2MetricsPods)
	if !added {
		t.Fatalf("ARM6 FAIL: discovery returned a nil list — uncertainty must NEVER skip, want added=true")
	}
}

// --- ARM 7: fail-safe open — NO discovery client at all registers. ---------
func TestA2_Arm7_NoDiscoveryClientStillRegisters(t *testing.T) {
	rw := newA2Watcher(t) // deliberately no SetDiscoveryClient

	added, _ := rw.EnsureResourceType(a2MetricsPods)
	if !added {
		t.Fatalf("ARM7 FAIL: no discovery surface — want added=true (register), got false")
	}
}

// --- ARM 8: THE STALENESS BOUND. A resource that GAINS watch is admitted. ---
//
// This is the arm that catches a memo caching the SKIP VERDICT rather than the
// DISCOVERY DATA. A verdict-cache survives its own evidence: the resource would
// keep being skipped forever because the cached answer, not the cached input,
// is consulted.
//
// Both halves are asserted, because each alone is satisfiable by a broken memo:
//
//	within the TTL  → the stale skip still holds (the memo is doing its job)
//	after expiry    → fresh discovery is re-read and the resource REGISTERS
//
// Expiry is driven by ExpireResourceVerbsMemoForTest, which BACKDATES the
// timestamp and leaves the map populated, so the production wholesale-expiry
// branch actually executes. Resetting the map to nil instead would bypass the
// very branch under test.
func TestA2_Arm8_ResourceGainingWatchIsAdmittedWithinOneTTL(t *testing.T) {
	rw := newA2Watcher(t)
	d := a2FullDisco()
	rw.SetDiscoveryClient(d)

	// Skipped on the live shape.
	if added, _ := rw.EnsureResourceType(a2MetricsPods); added {
		t.Fatalf("ARM8 setup FAIL: want the unwatchable shape skipped first")
	}

	// The apiserver now advertises watch — but we are still INSIDE the TTL, so
	// the memo legitimately holds the old answer.
	d.setVerbs("metrics.k8s.io/v1beta1", "pods", []string{"get", "list", "watch"})
	if added, _ := rw.EnsureResourceType(a2MetricsPods); added {
		t.Fatalf("ARM8 FAIL (within TTL): the memo should still hold its snapshot; " +
			"if this registers, the memo is not bounding discovery load at all")
	}

	// TTL elapses → the memo must re-read discovery and admit the resource.
	cache.ExpireResourceVerbsMemoForTest()
	added, _ := rw.EnsureResourceType(a2MetricsPods)
	if !added {
		t.Fatalf("ARM8 FAIL (after TTL): a resource that GAINED watch must be admitted within one TTL. " +
			"A permanent false-skip is data dropped forever — the memo is caching the VERDICT, not the DATA")
	}
	if !rw.IsRegistered(a2MetricsPods) {
		t.Fatalf("ARM8 FAIL: want the now-watchable resource registered")
	}
}

// --- ARM 9: the side effect to confirm — the stray SAR reflector. ----------
//
// /debug/servable showed authorization.k8s.io/v1 subjectaccessreviews as a
// REGISTERED informer (hasSynced=false, watchBroken=true, idx=0) — A1's api
// step lazily registering its GVR. Its only verb is `create`, so the same gate
// must drop it and stop that share of the 5,696 watch errors.
func TestA2_Arm9_CreateOnlyResourceNotRegistered(t *testing.T) {
	rw := newA2Watcher(t)
	rw.SetDiscoveryClient(a2FullDisco())

	added, _ := rw.EnsureResourceType(a2SAR)
	if added {
		t.Fatalf("ARM9 FAIL: %s advertises only [create] — want added=false", a2SAR)
	}
	if rw.IsRegistered(a2SAR) {
		t.Fatalf("ARM9 FAIL: the stray subjectaccessreviews reflector must not be registered")
	}
}

// --- ARM 10: the kill-switch still restores the old behaviour. -------------
//
// SKIP_UNSERVED_GROUP_INFORMERS=false must register unconditionally, exactly as
// before A2 — the A2 resource check shares that existing switch rather than
// introducing a second flag.
func TestA2_Arm10_FlagOffRegistersUnconditionally(t *testing.T) {
	t.Setenv("SKIP_UNSERVED_GROUP_INFORMERS", "false")
	rw := newA2Watcher(t)
	rw.SetDiscoveryClient(a2FullDisco())

	added, _ := rw.EnsureResourceType(a2MetricsPods)
	if !added {
		t.Fatalf("ARM10 FAIL: with the pre-check off, the unwatchable GVR must register unconditionally")
	}
}

// --- ARM 11: the memo bounds discovery load. -------------------------------
//
// The miss on a skipped GVR is PERMANENT — it never registers — so every
// dispatch that touches it re-enters the gate. Without a memo that is one raw
// apiserver discovery round-trip PER DISPATCH, which would trade ~280
// dirty-marks/sec for a discovery call per /call. Assert on the call COUNT,
// which is the only thing that distinguishes the two.
func TestA2_Arm11_MemoBoundsDiscoveryRoundTrips(t *testing.T) {
	rw := newA2Watcher(t)
	d := a2FullDisco()
	rw.SetDiscoveryClient(d)

	// Drive the real boundary repeatedly (≥2 real invocations).
	const touches = 25
	for i := 0; i < touches; i++ {
		if added, _ := rw.EnsureResourceType(a2MetricsPods); added {
			t.Fatalf("ARM11 FAIL: the unwatchable GVR registered on touch %d", i)
		}
	}

	// One group/version, one TTL window ⇒ at most one fetch for it. Allow a
	// small allowance for the confirm path's own independent calls on OTHER
	// group/versions, but nothing approaching `touches`.
	if got := d.calls(); got >= touches {
		t.Fatalf("ARM11 FAIL: %d discovery round-trips for %d touches — the memo is not bounding "+
			"per-miss cost; an unmemoised gate issues one apiserver call per dispatch", got, touches)
	}
}

// errA2Discovery is the injected discovery failure for the fail-safe-open arm.
var errA2Discovery = &a2Err{}

type a2Err struct{}

func (e *a2Err) Error() string { return "a2: injected discovery failure" }
