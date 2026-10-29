// crd_schema_relist_test.go — falsifier for
// followup-crd-schema-widen-informer-relist.
//
// THE BUG (empirically observed in the PR#33 inline-extras FRONTEND E2E,
// docs/pr33-inline-extras-frontend-e2e-2026-06-19.md): under CACHE_ENABLED
// the data informer caches apiserver-PRUNED objects when it LIST/WATCHed
// under a NARROWER structural schema. Widening the CRD at runtime (adding a
// property / flipping x-kubernetes-preserve-unknown-fields) does NOT relist
// the running informer — EnsureResourceType is registration-idempotent
// (watcher.go) — so objects.Get keeps serving pruned objects until a manual
// pod bounce. The CRD-UPDATE path invalidated the discovery cache + the
// compiled-schema VALIDATION memo (Tasks #322/#323) but NOT the data informer.
//
// THE FIX: triggerCRDSchemaRelist (crd_discovery_side_effect.go) detects a
// real structural-schema delta (fingerprint over spec.versions[].{name,
// schema}) and, ONLY then, relists each served+registered GVR via
// RemoveResourceType + EnsureResourceType + OnResourceTypeSchemaRelisted
// (dirty-mark dependent L1; logs SCHEMA_RELIST, not CRD_DELETE).
// Schema-delta-gated so benign churn does not thrash.
//
// These tests drive the REAL deps_watch UpdateFunc → bounded worker →
// triggerCRDDiscovery → triggerCRDSchemaRelist path (not the function in
// isolation), with a stub-REST watcher so the relisted informer is actually
// reconstructed. Run under -race.

package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// dirtyRecorder captures the L1 keys the DepTracker dirty-marks via the
// refresher enqueue hook (deps.go SetRefreshHook), so a test can assert a
// specific key was marked. Concurrency-safe (the relist runs on the worker
// goroutine; the test reads from the test goroutine).
type dirtyRecorder struct {
	mu   sync.Mutex
	keys map[string]int
}

func newDirtyRecorder() *dirtyRecorder { return &dirtyRecorder{keys: map[string]int{}} }
func (r *dirtyRecorder) hook() func(string, schema.GroupVersionResource) {
	return func(k string, _ schema.GroupVersionResource) { r.mu.Lock(); r.keys[k]++; r.mu.Unlock() }
}
func (r *dirtyRecorder) marked(k string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.keys[k] > 0
}

// crdBytesObjWithSchema builds a CRD *bytesObject for a single served version
// whose spec.versions[0].schema.openAPIV3Schema is the supplied JSON object
// literal. `extra` is an arbitrary additional top-level spec field (e.g. a
// printer-column / conversion blob) used to prove that churn OUTSIDE the
// schema subtree does NOT change the fingerprint.
func crdBytesObjWithSchema(t *testing.T, name, group, plural, version, openAPIV3SchemaJSON, extraSpecJSON string) *bytesObject {
	t.Helper()
	extra := ""
	if extraSpecJSON != "" {
		extra = "," + extraSpecJSON
	}
	raw := []byte(`{
		"apiVersion":"apiextensions.k8s.io/v1",
		"kind":"CustomResourceDefinition",
		"metadata":{"name":"` + name + `"},
		"spec":{
			"group":"` + group + `",
			"names":{"plural":"` + plural + `","kind":"X"},
			"versions":[{"name":"` + version + `","served":true,"storage":true,"schema":{"openAPIV3Schema":` + openAPIV3SchemaJSON + `}}]` + extra + `
		}
	}`)
	bo, err := newBytesObjectFromRaw(raw)
	if err != nil {
		t.Fatalf("crdBytesObjWithSchema: newBytesObjectFromRaw failed: %v", err)
	}
	return bo
}

// narrow vs widened openAPIV3Schema literals for a button-like widget CRD.
// The widened one adds spec.apiRef.extras with preserve-unknown — exactly the
// PR#33 change that the apiserver stops pruning once present.
const (
	narrowButtonSchema = `{"type":"object","properties":{"spec":{"type":"object","properties":{` +
		`"apiRef":{"type":"object","properties":{"name":{"type":"string"},"namespace":{"type":"string"}}}}}}}`
	widenedButtonSchema = `{"type":"object","properties":{"spec":{"type":"object","properties":{` +
		`"apiRef":{"type":"object","properties":{"name":{"type":"string"},"namespace":{"type":"string"},` +
		`"extras":{"type":"object","x-kubernetes-preserve-unknown-fields":true}}}}}}}`
)

// TestCRDSchemaWiden_RelistsRunningInformer — THE falsifier. A CRD UPDATE that
// WIDENS the structural schema of an already-registered GVR must relist that
// GVR's data informer (so it re-LISTs under the new schema and stops serving
// pruned objects), and must dirty-mark dependent L1.
func TestCRDSchemaWiden_RelistsRunningInformer(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv(envCompositionStreamingList, "true")
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil) // discovery soft-skips (no SA rc) — irrelevant to the relist path

	// The widget GVR whose CRD schema we widen.
	target := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "buttons"}

	// Stub-REST watcher that ALSO watches the CRD meta-GVR, so depEventHandlers
	// for the CRD GVR fire the lifecycle side-effect, and EnsureResourceType
	// can reconstruct `target` without connecting.
	crdGVR := CRDGVRForTest()
	rw := newRouteRaceWatcher(t, true, crdGVR, target)
	t.Cleanup(func() { rw.Stop() })
	// Pre-close the CRD meta-GVR syncCh so the CRD AddFunc passes the R1
	// post-sync gate (addEventPostSync) and the lifecycle side-effect fires —
	// same arrangement the schema-invalidation tests use.
	crdSync := make(chan struct{})
	close(crdSync)
	rw.mu.Lock()
	rw.syncCh[crdGVR] = crdSync
	rw.mu.Unlock()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })

	// Register the target GVR informer (the "running" informer that listed
	// under the NARROW schema) and confirm it is registered.
	rw.EnsureResourceType(target)
	if !rw.IsRegistered(target) {
		t.Fatalf("precondition: target GVR not registered after EnsureResourceType")
	}

	// Record a dependent L1 edge + wire a dirty-mark recorder so we can assert
	// the relist dirty-marks that entry (via OnResourceTypeSchemaRelisted → refresher).
	Deps().Record(context.Background(), "l1-widget-key", target, "demo-system", "button-x")
	rec := newDirtyRecorder()
	Deps().SetRefreshHook(rec.hook())

	handlers := rw.depEventHandlers(crdGVR)

	// (1) First observe the CRD at the NARROW schema (seeds the fingerprint).
	narrowCRD := crdBytesObjWithSchema(t, "buttons.widgets.templates.krateo.io",
		"widgets.templates.krateo.io", "buttons", "v1beta1", narrowButtonSchema, "")
	handlers.AddFunc(narrowCRD)
	if !WaitCRDDiscoveryProcessedForTest(1, 2000) {
		t.Fatalf("worker did not process CRD ADD: %s", crdDiscoveryStatsString())
	}

	// (2) Now WIDEN the schema via an UPDATE — the load-bearing event.
	widenedCRD := crdBytesObjWithSchema(t, "buttons.widgets.templates.krateo.io",
		"widgets.templates.krateo.io", "buttons", "v1beta1", widenedButtonSchema, "")
	handlers.UpdateFunc(narrowCRD, widenedCRD)
	if !WaitCRDDiscoveryProcessedForTest(2, 2000) {
		t.Fatalf("worker did not process CRD UPDATE: %s", crdDiscoveryStatsString())
	}

	s := CRDDiscoveryStatsSnapshot()
	if s.SchemaRelistsFired != 1 {
		t.Fatalf("SchemaRelistsFired=%d want 1 — a structural-schema WIDEN of a registered "+
			"GVR MUST relist its data informer (else objects.Get keeps serving pre-widen "+
			"PRUNED objects until a manual bounce). counters: %s",
			s.SchemaRelistsFired, crdDiscoveryStatsString())
	}

	// #870 — the widen BUILDS the replacement beside the live informer and
	// swaps it in; the GVR stays registered and servable throughout.
	if !rw.IsRegistered(target) {
		t.Fatalf("after the widen the target GVR is no longer registered — a schema build must " +
			"never tear the live informer down")
	}
	// This fixture's REST client never connects, so the new copy cannot sync:
	// the build stays OPEN (it ends on an event — its sync — never a timer),
	// with the live informer still registered. The swap itself is pinned on a
	// syncing fixture (TestStoreSwap_K5_SchemaWidenByContent, K16b).
	if rw.storeBuildForTest(target) == nil {
		t.Fatalf("the widen did not open a schema build for the registered GVR")
	}

	// #870 — and it marks ONLY what changed: nothing is marked when the build
	// is requested (a whole-type mark is gone, X2).
	time.Sleep(100 * time.Millisecond)
	if rec.marked("l1-widget-key") {
		t.Fatalf("#870 RED: the widen dirty-marked a dependent whose object did not change — " +
			"a whole-type mark is back")
	}
}

// TestCRDSchemaUnchanged_NoRelist — the THRASH GUARD. A CRD UPDATE that does
// NOT change the structural schema (only churn outside spec.versions[].schema,
// e.g. a printer-column / conversion field) must NOT relist — otherwise every
// controller status/printer patch would tear down + re-LIST the informer.
func TestCRDSchemaUnchanged_NoRelist(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv(envCompositionStreamingList, "true")
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil)

	target := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "buttons"}
	crdGVR := CRDGVRForTest()
	rw := newRouteRaceWatcher(t, true, crdGVR, target)
	t.Cleanup(func() { rw.Stop() })
	// Pre-close the CRD meta-GVR syncCh so the CRD AddFunc passes the R1
	// post-sync gate (addEventPostSync) and the lifecycle side-effect fires —
	// same arrangement the schema-invalidation tests use.
	crdSync := make(chan struct{})
	close(crdSync)
	rw.mu.Lock()
	rw.syncCh[crdGVR] = crdSync
	rw.mu.Unlock()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	rw.EnsureResourceType(target)

	handlers := rw.depEventHandlers(crdGVR)

	// Seed at a schema.
	v1 := crdBytesObjWithSchema(t, "buttons.widgets.templates.krateo.io",
		"widgets.templates.krateo.io", "buttons", "v1beta1", widenedButtonSchema, "")
	handlers.AddFunc(v1)
	if !WaitCRDDiscoveryProcessedForTest(1, 2000) {
		t.Fatalf("worker did not process CRD ADD: %s", crdDiscoveryStatsString())
	}
	relistsAfterSeed := CRDDiscoveryStatsSnapshot().SchemaRelistsFired

	// UPDATE with the SAME schema but a CHANGED out-of-schema field (a
	// conversion stanza) — benign churn that must NOT relist.
	v2 := crdBytesObjWithSchema(t, "buttons.widgets.templates.krateo.io",
		"widgets.templates.krateo.io", "buttons", "v1beta1", widenedButtonSchema,
		`"conversion":{"strategy":"None"}`)
	handlers.UpdateFunc(v1, v2)
	if !WaitCRDDiscoveryProcessedForTest(2, 2000) {
		t.Fatalf("worker did not process CRD UPDATE: %s", crdDiscoveryStatsString())
	}

	s := CRDDiscoveryStatsSnapshot()
	if s.SchemaRelistsFired != relistsAfterSeed {
		t.Fatalf("SchemaRelistsFired went %d→%d on an UNCHANGED-schema UPDATE — the thrash "+
			"guard failed; benign CRD churn must not relist the informer",
			relistsAfterSeed, s.SchemaRelistsFired)
	}
	if s.SchemaUnchanged < 1 {
		t.Fatalf("SchemaUnchanged=%d want >=1 — the unchanged-schema UPDATE should have hit "+
			"the thrash guard", s.SchemaUnchanged)
	}
}
