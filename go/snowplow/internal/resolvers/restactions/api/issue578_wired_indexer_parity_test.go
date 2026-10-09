//go:build unit || integration

package api

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestIssue578_WiredIndexer_RawEnvelopeMatchesDecodedEnvelope closes the gap
// my unit tests leave open.
//
// issue578_envelope_parity_test.go proves marshalAsListRaw and marshalAsList
// agree when handed EQUIVALENT inputs that the test itself constructs. That
// says nothing about the WIRING: whether ListRawServable reads the same
// indexer partition, in the same order, through the same servability gate, as
// the ListObjectsServable it replaces. A bug there (wrong namespace index,
// dropped items, a different ordering) would leave both unit suites green and
// serve a different list in production.
//
// This drives a REAL ResourceWatcher over a fake dynamic client, lets its
// informers sync, then runs BOTH paths against that one live indexer and
// compares the finished envelopes byte for byte.
//
// Ordering note: both paths enumerate the same `store.List()` / `ByIndex`
// result in the same order, so byte equality is the correct assertion. If the
// indexer ever returned a nondeterministic order this test would flake rather
// than pass wrongly — which is the failure direction we want.
func TestIssue578_WiredIndexer_RawEnvelopeMatchesDecodedEnvelope(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	mk := func(ns, name string, data map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": name, "namespace": ns},
			"data":       data,
		}}
	}

	seed := []runtime.Object{
		mk("ns-a", "cm-1", map[string]any{"k": "v1"}),
		mk("ns-a", "cm-2", map[string]any{"k": "v2", "nested": map[string]any{"deep": "yes"}}),
		mk("ns-b", "cm-3", map[string]any{"k": "v3"}),
	}

	// The watcher starts its own RBAC informers (and a namespaces informer via
	// the heal re-touch); the fake client PANICS on a list-kind it was not told
	// about, so every GVR the watcher may register must be declared here. Same
	// set the #58 UAF harness declares, for the same reason.
	sch := runtime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	lk := map[schema.GroupVersionResource]string{
		gvr: "ConfigMapList",
		{Group: "", Version: "v1", Resource: "namespaces"}:                                   "NamespaceList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, lk, seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Register the GVR's informer and wait for ITS initial LIST, not just the
	// factory's: EnsureResourceTypeFor hands back a per-GVR sync channel, and
	// a watcher-wide WaitForCacheSync can return before a lazily-added
	// informer has drained (the check-then-act gap 0.30.97 closed).
	_, syncCh := rw.EnsureResourceTypeFor(ctx, gvr)
	if syncCh != nil {
		select {
		case <-syncCh:
		case <-ctx.Done():
			t.Fatalf("informer for %s never synced: %v", gvr, ctx.Err())
		}
	}
	if err := rw.WaitForCacheSync(ctx, 10*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}

	apiVersion := "v1"
	listKind := listKindForResource(gvr.Resource)

	for _, ns := range []string{"", "ns-a", "ns-b"} {
		label := ns
		if label == "" {
			label = "cluster-wide"
		}
		t.Run(label, func(t *testing.T) {
			items, servableDecoded := rw.ListObjectsServable(gvr, ns)
			raws, servableRaw := rw.ListRawServable(gvr, ns)

			// The servability VERDICT must agree. If one path can vouch for an
			// answer and the other cannot, they are not reading the same gate.
			if servableDecoded != servableRaw {
				t.Fatalf("servability disagreed: decoded=%v raw=%v", servableDecoded, servableRaw)
			}
			if !servableRaw {
				t.Fatalf("informer not servable after WaitForCacheSync — the test is not "+
					"exercising the indexer (ns=%q)", ns)
			}

			// Non-vacuity: the partition must actually contain what we seeded,
			// otherwise two empty lists would compare equal and prove nothing.
			wantCount := map[string]int{"": 3, "ns-a": 2, "ns-b": 1}[ns]
			if len(items) != wantCount {
				t.Fatalf("decoded partition has %d items, want %d — seed or namespace "+
					"index is not what this test assumes", len(items), wantCount)
			}
			if len(raws) != len(items) {
				t.Fatalf("partition SIZE differs: decoded=%d raw=%d — ListRawServable is "+
					"not reading the same partition", len(items), len(raws))
			}

			// ORDER IS NOT COMPARABLE ACROSS TWO CALLS, and that is a property of
			// the indexer, not of this change. Both paths enumerate
			// `store.List()` / `store.ByIndex(...)`, which are backed by a Go map,
			// so two separate reads of the SAME partition can return the same
			// items in a different order. Measured: over 6 runs of this test an
			// order-sensitive assertion failed on the cluster-wide partition once
			// and on ns-a twice, passing the other runs — i.e. it flakes rather
			// than holds.
			//
			// (That nondeterminism is PRE-EXISTING: listFromIndexer has always
			// iterated the same map-backed output, so the stored apistage LIST
			// envelope's item order already varies between rebuilds of identical
			// data. See #578 — it is worth fixing on its own, because a
			// deterministically-ordered envelope would let the refresher compare
			// rebuilt bytes to stored bytes and skip a no-op Put entirely.)
			//
			// So the wiring claim is asserted order-independently: the two paths
			// must yield the SAME MULTISET of per-item JSON. That still catches
			// every wiring defect this test exists for — wrong partition, wrong
			// namespace index, dropped or duplicated items, different content —
			// without asserting a guarantee the indexer does not make.
			wantItems := make([]string, 0, len(items))
			for _, it := range items {
				b, err := json.Marshal(it.Object)
				if err != nil {
					t.Fatalf("marshal decoded item: %v", err)
				}
				wantItems = append(wantItems, string(b))
			}
			gotItems := make([]string, 0, len(raws))
			for _, r := range raws {
				gotItems = append(gotItems, string(r))
			}
			sort.Strings(wantItems)
			sort.Strings(gotItems)
			if !reflect.DeepEqual(wantItems, gotItems) {
				t.Fatalf("WIRED partition contents differ for ns=%q\n want %v\n  got %v",
					ns, wantItems, gotItems)
			}

			// And the ENVELOPE assembly around those items must still be
			// byte-identical. Feeding both builders the same canonical order
			// isolates the assembly from the indexer's ordering.
			canon := make([]*unstructured.Unstructured, 0, len(gotItems))
			canonRaw := make([][]byte, 0, len(gotItems))
			for _, s := range gotItems {
				var m map[string]any
				if err := json.Unmarshal([]byte(s), &m); err != nil {
					t.Fatalf("re-parse canonical item: %v", err)
				}
				canon = append(canon, &unstructured.Unstructured{Object: m})
				canonRaw = append(canonRaw, []byte(s))
			}
			wantBytes, err := marshalAsList(apiVersion, listKind, canon)
			if err != nil {
				t.Fatalf("marshalAsList: %v", err)
			}
			gotBytes, err := marshalAsListRaw(apiVersion, listKind, canonRaw)
			if err != nil {
				t.Fatalf("marshalAsListRaw: %v", err)
			}
			if string(gotBytes) != string(wantBytes) {
				t.Fatalf("WIRED envelope differs for ns=%q\n want %s\n  got %s", ns, wantBytes, gotBytes)
			}
		})
	}
}
