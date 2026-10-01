// issue218_crd_relist_warmth_test.go — #218: the CRD-re-list warmth amplifier.
//
// triggerCRDDiscovery fired three GLOBAL invalidations on EVERY CRD ADD/UPDATE,
// including the ~9.5min idle re-list (UpdateFunc Sync, ResyncPeriod:0):
//   DiscoverGroupResourcesFresh (memcache wipe) — observed via DiscoveryInvoked
//   invalidateSADiscovery       (SA discovery/RESTMapper wipe) — SA spy
//   invalidateCRDSchemaMemo     (compiled-schema memo)         — schema spy
// The two-fingerprint gate fires only what changed; an idle re-list fires none.
//
// 6 arms, all driving the REAL submitCRDLifecycleEvent path:
//   A idle re-list → NOTHING fires (the amplifier fix).
//   B schema change → schema memo fires (+ discovery via the fail-safe union).
//   C discovery-identity change (a version served:true→false) → discovery fires,
//     schema memo does NOT. THE correctness arm — RED under a naive single
//     schema-fingerprint gate (which would miss the served-flag change).
//   D status/printer/conversion churn → NOTHING fires (the thrash guard). Conversion
//     (spec.conversion) is apiserver-transparent: not in discovery, the RESTMapper,
//     or the schema memo, so it is excluded from BOTH fingerprints — proven a no-op.
//   E delete+recreate → the recreate re-fires (DELETE drops the fingerprint).
//   F status-subresource toggle → discovery fires, schema memo does NOT: #282
//     routes /call to <plural>/status, so its presence is served-GVR identity,
//     not schema. RED under a fingerprint that omits subresources.

package cache

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

// --- builders --------------------------------------------------------------

// crdVer218 builds one spec.versions[] entry with a controllable served/storage
// flag and a one-property openAPIV3Schema (schemaProp varies the schema fp).
func crdVer218(name string, served, storage bool, schemaProp string) map[string]any {
	return map[string]any{
		"name":    name,
		"served":  served,
		"storage": storage,
		"schema": map[string]any{"openAPIV3Schema": map[string]any{
			"type":       "object",
			"properties": map[string]any{schemaProp: map[string]any{"type": "string"}},
		}},
	}
}

func crd218(name, group string, versions []map[string]any, status map[string]any) *unstructured.Unstructured {
	vs := make([]any, 0, len(versions))
	for _, v := range versions {
		vs = append(vs, v)
	}
	obj := map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"group":    group,
			"names":    map[string]any{"plural": "widgets", "singular": "widget", "kind": "Widget", "listKind": "WidgetList"},
			"scope":    "Namespaced",
			"versions": vs,
		},
	}
	if status != nil {
		obj["status"] = status
	}
	return &unstructured.Unstructured{Object: obj}
}

// --- harness (reuses the existing SA + schema invalidator recorders) --------

func calls218(r *invalidatorRecorder) int { c, _, _ := r.snapshot(); return c }

// setup218 wires a clean CRD-discovery singleton + both invalidator spies + a
// fake SA rest config (so the gate is reached past the no-SA-rc degraded path).
func setup218(t *testing.T) (sa, sch *invalidatorRecorder) {
	t.Helper()
	withCleanCRDDiscovery(t)
	crdDiscoverySingleton().ResetCRDDiscoveryFingerprintsForTest()
	sa = withRecordingInvalidator(t)
	sch = withRecordingSchemaInvalidator(t)
	SetProcessSARestConfig(&rest.Config{Host: "https://fake.test"})
	t.Cleanup(func() { SetProcessSARestConfig(nil) })
	return sa, sch
}

// drive218 submits one CRD lifecycle event through the REAL enqueue path and
// waits for the single worker to process it.
func drive218(t *testing.T, n *int, obj *unstructured.Unstructured, kind crdLifecycleKind) {
	t.Helper()
	*n++
	crdDiscoverySingleton().submitCRDLifecycleEvent(obj, kind)
	if !WaitCRDDiscoveryProcessedForTest(uint64(*n), 2000) {
		t.Fatalf("worker did not process event #%d within 2s: %s", *n, crdDiscoveryStatsString())
	}
}

func di218() int { return int(CRDDiscoveryStatsSnapshot().DiscoveryInvoked) }

// --- arms ------------------------------------------------------------------

func TestIssue218_A_IdleRelistFiresNothing(t *testing.T) {
	sa, sch := setup218(t)
	crd := crd218("widgets.g218.io", "g218.io", []map[string]any{crdVer218("v1", true, true, "foo")}, nil)

	n := 0
	drive218(t, &n, crd, crdLifecycleUpdate) // first observation → fires
	if calls218(sa) != 1 || calls218(sch) != 1 || di218() != 1 {
		t.Fatalf("precondition: first delivery must fire all three (sa=%d schema=%d di=%d, want 1/1/1)",
			calls218(sa), calls218(sch), di218())
	}

	// The IDENTICAL CRD again — the ~9.5min idle re-list. NOTHING must fire.
	drive218(t, &n, crd, crdLifecycleUpdate)
	if calls218(sa) != 1 {
		t.Fatalf("RED #218: an idle re-list fired the SA discovery invalidator (calls=%d, want 1) — the "+
			"~9.5min CRD re-list re-pays the global discovery/RESTMapper wipe", calls218(sa))
	}
	if calls218(sch) != 1 {
		t.Fatalf("RED #218: an idle re-list fired the schema-memo invalidator (%d, want 1)", calls218(sch))
	}
	if di218() != 1 {
		t.Fatalf("RED #218: DiscoveryInvoked moved on an idle re-list (%d, want 1) — the memcache wipe re-ran", di218())
	}
	if noop := CRDDiscoveryStatsSnapshot().CRDDiscoveryNoop; noop != 1 {
		t.Fatalf("crd_discovery_noop=%d after one idle re-list, want 1", noop)
	}
}

func TestIssue218_B_SchemaChangeFiresSchemaMemoAndDiscovery(t *testing.T) {
	sa, sch := setup218(t)
	name, group := "widgets.g218.io", "g218.io"
	n := 0
	drive218(t, &n, crd218(name, group, []map[string]any{crdVer218("v1", true, true, "foo")}, nil), crdLifecycleUpdate)
	saBefore, schBefore, diBefore := calls218(sa), calls218(sch), di218()

	// SCHEMA changed (prop foo→bar); the served-GVR shape is IDENTICAL.
	drive218(t, &n, crd218(name, group, []map[string]any{crdVer218("v1", true, true, "bar")}, nil), crdLifecycleUpdate)

	if calls218(sch) != schBefore+1 {
		t.Fatalf("RED #218: a schema change did NOT fire the schema-memo invalidator (%d→%d)", schBefore, calls218(sch))
	}
	if calls218(sa) != saBefore+1 {
		t.Fatalf("RED #218: a schema change did NOT fire the SA discovery invalidator via the fail-safe "+
			"union (%d->%d) — shared caches hold OpenAPI too, so a schema change must invalidate them", saBefore, calls218(sa))
	}
	if di218() != diBefore+1 {
		t.Fatalf("RED #218: a schema change did NOT re-run discovery via the union (%d→%d)", diBefore, di218())
	}
}

func TestIssue218_C_DiscoveryIdentityChangeFiresDiscoveryNotSchema(t *testing.T) {
	sa, sch := setup218(t)
	name, group := "widgets.g218.io", "g218.io"
	n := 0
	drive218(t, &n, crd218(name, group, []map[string]any{
		crdVer218("v1", true, true, "foo"), crdVer218("v2", true, false, "foo"),
	}, nil), crdLifecycleUpdate)
	saBefore, schBefore, diBefore := calls218(sa), calls218(sch), di218()

	// v2 served:true→false. The DISCOVERY IDENTITY changed; the SCHEMA (per-version
	// openAPIV3Schema) is byte-identical.
	drive218(t, &n, crd218(name, group, []map[string]any{
		crdVer218("v1", true, true, "foo"), crdVer218("v2", false, false, "foo"),
	}, nil), crdLifecycleUpdate)

	if calls218(sa) != saBefore+1 {
		t.Fatalf("RED #218 (CORRECTNESS): a version served:true->false did NOT fire the SA discovery "+
			"invalidator (calls %d->%d) — discovery + the RESTMapper are now stale for that GVR. This is "+
			"exactly what a naive single-schema-fingerprint gate would miss (the served flag is not in the "+
			"schema fp).", saBefore, calls218(sa))
	}
	if di218() != diBefore+1 {
		t.Fatalf("RED #218 (CORRECTNESS): a served-flag change did NOT re-run discovery (%d->%d)", diBefore, di218())
	}
	if calls218(sch) != schBefore {
		t.Fatalf("#218: the schema-memo invalidator fired on a served-flag change (%d->%d) — the schema "+
			"did not change; only the discovery pair should fire", schBefore, calls218(sch))
	}
}

func TestIssue218_D_StatusChurnFiresNothing(t *testing.T) {
	sa, sch := setup218(t)
	name, group := "widgets.g218.io", "g218.io"
	n := 0
	drive218(t, &n, crd218(name, group, []map[string]any{crdVer218("v1", true, true, "foo")}, nil), crdLifecycleUpdate)
	saBefore, schBefore, diBefore := calls218(sa), calls218(sch), di218()

	// status churn (outside spec.{group,names,scope,versions}) AND a spec.conversion
	// change (freshness-audit: apiserver-transparent, read by neither consumer) —
	// so neither the schema nor the discovery-identity fingerprint moves.
	churned := crd218(name, group, []map[string]any{crdVer218("v1", true, true, "foo")},
		map[string]any{"acceptedNames": map[string]any{"plural": "widgets"}, "conditions": []any{}})
	churned.Object["spec"].(map[string]any)["conversion"] = map[string]any{
		"strategy": "Webhook",
		"webhook": map[string]any{
			"conversionReviewVersions": []any{"v1"},
			"clientConfig":             map[string]any{"service": map[string]any{"namespace": "default", "name": "conv"}},
		},
	}
	drive218(t, &n, churned, crdLifecycleUpdate)

	if calls218(sa) != saBefore {
		t.Fatalf("RED #218: status churn fired the SA discovery invalidator (%d->%d) — the thrash guard failed", saBefore, calls218(sa))
	}
	if calls218(sch) != schBefore || di218() != diBefore {
		t.Fatalf("RED #218: status churn fired schema (%d->%d) or discovery (%d->%d) — thrash guard failed",
			schBefore, calls218(sch), diBefore, di218())
	}
}

func TestIssue218_E_DeleteDropsFingerprintSoRecreateRefires(t *testing.T) {
	setup218(t)
	name, group := "widgets.g218.io", "g218.io"
	crd := crd218(name, group, []map[string]any{crdVer218("v1", true, true, "foo")}, nil)

	n := 0
	drive218(t, &n, crd, crdLifecycleAdd)    // fires (di=1)
	drive218(t, &n, crd, crdLifecycleUpdate) // idle no-op (di=1)
	if di218() != 1 {
		t.Fatalf("precondition: DiscoveryInvoked=%d after add+idle, want 1", di218())
	}

	drive218(t, &n, crd, crdLifecycleDelete) // triggerCRDDelete -> drops the fingerprints
	drive218(t, &n, crd, crdLifecycleAdd)    // RECREATE (identical spec) -> must re-fire

	if di218() != 2 {
		t.Fatalf("RED #218: a CRD delete+recreate did NOT re-run discovery (DiscoveryInvoked=%d, want 2) — "+
			"the DELETE must drop the fingerprint, else the recreate matches the stale fp and no-ops, leaving "+
			"the discovery cache / RESTMapper without the recreated GVR", di218())
	}
}

func TestIssue218_F_StatusSubresourceToggleFiresDiscoveryNotSchema(t *testing.T) {
	sa, sch := setup218(t)
	name, group := "widgets.g218.io", "g218.io"
	n := 0
	// v1 WITHOUT a status subresource.
	drive218(t, &n, crd218(name, group, []map[string]any{crdVer218("v1", true, true, "foo")}, nil), crdLifecycleUpdate)
	saBefore, schBefore, diBefore := calls218(sa), calls218(sch), di218()

	// Enable the per-version status subresource. The apiserver now publishes
	// widgets/status as its OWN discovery resource (#282 can route /call to it) —
	// a served-GVR-surface change. The schema (openAPIV3Schema) is byte-identical,
	// so this is discovery identity, not schema.
	withStatus := crd218(name, group, []map[string]any{crdVer218("v1", true, true, "foo")}, nil)
	withStatus.Object["spec"].(map[string]any)["versions"].([]any)[0].(map[string]any)["subresources"] =
		map[string]any{"status": map[string]any{}}
	drive218(t, &n, withStatus, crdLifecycleUpdate)

	if calls218(sa) != saBefore+1 {
		t.Fatalf("RED #218 (CORRECTNESS): enabling a status subresource did NOT fire the SA discovery "+
			"invalidator (calls %d->%d) — widgets/status is a new served discovery resource that #282 routes "+
			"to, so the discovery surface + RESTMapper are now stale. A fingerprint omitting subresources would "+
			"silently miss this (no fail-safe: the versions subtree parsed fine).", saBefore, calls218(sa))
	}
	if di218() != diBefore+1 {
		t.Fatalf("RED #218 (CORRECTNESS): a status-subresource toggle did NOT re-run discovery (%d->%d)", diBefore, di218())
	}
	if calls218(sch) != schBefore {
		t.Fatalf("#218: the schema-memo invalidator fired on a subresource toggle (%d->%d) — the schema did "+
			"not change; only the discovery pair should fire", schBefore, calls218(sch))
	}
}
