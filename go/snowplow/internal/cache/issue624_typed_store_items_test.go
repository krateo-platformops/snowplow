// issue624_typed_store_items_test.go — #624 falsifier.
//
// THE DEFECT. The four RBAC GVRs carry a typed-converting transform
// (strip.go typedResourceOverrides), so their indexer holds *rbacv1.Role,
// *rbacv1.RoleBinding, *rbacv1.ClusterRole and *rbacv1.ClusterRoleBinding.
// The serve paths rendered indexer slots through a type switch that knew
// only *bytesObject and *unstructured.Unstructured; every other type fell
// into `default: return nil, false` and was DROPPED. The GVR is servable,
// so the caller received ([], true): an empty list served as the truth.
//
// THE PROPERTY. For every typed kind, on both read branches (decoded:
// ListObjectsServable; raw #578: ListRawServable) and on the GET-by-name
// path, what the informer serves must EQUAL what the apiserver returns.
// And a slot that cannot be rendered must fail the whole read CLOSED
// (served=false, so the caller falls through) and be counted — never
// silently shrink the list.
package cache

import (
	"context"
	"encoding/json"
	"expvar"
	"reflect"
	"sort"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// issue624ListKinds declares every list kind the watcher may register in a
// fake-dynamic run: the four RBAC kinds (eager) and namespaces (heal touch).
func issue624ListKinds() map[schema.GroupVersionResource]string {
	m := rbacListKinds()
	m[schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}] = "NamespaceList"
	return m
}

// issue624Seed is two namespaces' worth of every RBAC kind, with labels,
// annotations and non-trivial rules/subjects so a lossy conversion shows.
func issue624Seed() []k8sruntime.Object {
	tm := func(kind string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: kind}
	}
	om := func(ns, name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Namespace:   ns,
			Name:        name,
			Labels:      map[string]string{"krateo.io/managed-by": "portal-access", "i": name},
			Annotations: map[string]string{"note": "issue624-" + name},
		}
	}
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"*"}, Verbs: []string{"get", "list"}},
		{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}, ResourceNames: []string{"one"}},
	}
	subj := func(n string) []rbacv1.Subject {
		return []rbacv1.Subject{
			{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: n},
			{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: n + "-group"},
		}
	}
	var out []k8sruntime.Object
	for _, ns := range []string{"ns-a", "ns-b"} {
		out = append(out,
			&rbacv1.Role{TypeMeta: tm("Role"), ObjectMeta: om(ns, "role-"+ns), Rules: rules},
			&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: om(ns, "rb-"+ns), Subjects: subj("u-" + ns),
				RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "role-" + ns}},
		)
	}
	for _, n := range []string{"c1", "c2"} {
		out = append(out,
			&rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: om("", "cr-"+n), Rules: rules},
			&rbacv1.ClusterRoleBinding{TypeMeta: tm("ClusterRoleBinding"), ObjectMeta: om("", "crb-"+n), Subjects: subj("u-" + n),
				RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cr-" + n}},
		)
	}
	return out
}

// issue624Normalize renders an object map through encoding/json so the two
// sides compare as JSON values (int64 vs float64 and map-vs-typed origins
// cannot produce a false mismatch).
func issue624Normalize(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", raw, err)
	}
	return m
}

func issue624MustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func issue624SortByKey(items []map[string]any) {
	key := func(m map[string]any) string {
		md, _ := m["metadata"].(map[string]any)
		ns, _ := md["namespace"].(string)
		n, _ := md["name"].(string)
		return ns + "/" + n
	}
	sort.SliceStable(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
}

// TestIssue624_TypedRBACStore_ServedEqualsApiserver drives a REAL watcher
// over a fake dynamic client: the four RBAC informers are registered eagerly
// and their transform stores typed objects — the production shape.
func TestIssue624_TypedRBACStore_ServedEqualsApiserver(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(newTestSchemeRBAC(), issue624ListKinds(), issue624Seed()...)
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 10*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}

	for _, gvr := range rbacTypedGVRs {
		gvr := gvr
		// Precondition: the store really holds a TYPED object for this GVR —
		// otherwise this arm would be testing the Unstructured path and prove
		// nothing about #624.
		rw.mu.RLock()
		gi := rw.informers[gvr]
		rw.mu.RUnlock()
		if gi == nil {
			t.Fatalf("%s: no informer registered", gvr.Resource)
		}
		stored := gi.Informer().GetIndexer().List()
		if len(stored) == 0 {
			t.Fatalf("%s: indexer empty — seed did not reach the store", gvr.Resource)
		}
		for _, s := range stored {
			switch s.(type) {
			case *bytesObject, *unstructured.Unstructured:
				t.Fatalf("%s: store holds %T, want a typed object — the arm is not exercising #624", gvr.Resource, s)
			}
		}

		namespaces := []string{""}
		if gvr.Resource == "roles" || gvr.Resource == "rolebindings" {
			namespaces = append(namespaces, "ns-a", "ns-b")
		}
		for _, ns := range namespaces {
			label := gvr.Resource + "/" + ns
			if ns == "" {
				label = gvr.Resource + "/cluster-wide"
			}
			t.Run(label, func(t *testing.T) {
				var api *unstructured.UnstructuredList
				var err error
				if ns == "" {
					api, err = dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
				} else {
					api, err = dyn.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
				}
				if err != nil {
					t.Fatalf("apiserver list: %v", err)
				}
				want := make([]map[string]any, 0, len(api.Items))
				for i := range api.Items {
					want = append(want, issue624Normalize(t, issue624MustJSON(t, api.Items[i].Object)))
				}
				issue624SortByKey(want)
				if len(want) == 0 {
					t.Fatalf("apiserver returned 0 items — non-vacuity precondition failed")
				}

				// Decoded branch.
				items, ok := rw.ListObjectsServable(gvr, ns)
				if !ok {
					t.Fatalf("decoded: served=false on a synced typed GVR — want served=true with the items")
				}
				gotDecoded := make([]map[string]any, 0, len(items))
				for _, it := range items {
					gotDecoded = append(gotDecoded, issue624Normalize(t, issue624MustJSON(t, it.Object)))
				}
				issue624SortByKey(gotDecoded)
				if !reflect.DeepEqual(gotDecoded, want) {
					t.Fatalf("decoded branch != apiserver\n got (%d): %s\nwant (%d): %s",
						len(gotDecoded), issue624MustJSON(t, gotDecoded), len(want), issue624MustJSON(t, want))
				}

				// Raw (#578) branch.
				raws, ok := rw.ListRawServable(gvr, ns)
				if !ok {
					t.Fatalf("raw: served=false on a synced typed GVR — want served=true with the items")
				}
				gotRaw := make([]map[string]any, 0, len(raws))
				for _, r := range raws {
					gotRaw = append(gotRaw, issue624Normalize(t, r))
				}
				issue624SortByKey(gotRaw)
				if !reflect.DeepEqual(gotRaw, want) {
					t.Fatalf("raw branch != apiserver\n got (%d): %s\nwant (%d): %s",
						len(gotRaw), issue624MustJSON(t, gotRaw), len(want), issue624MustJSON(t, want))
				}

				// Branch BYTE parity: the raw item must be exactly what the
				// decoded branch marshals, item for item (#578's contract).
				if len(raws) != len(items) {
					t.Fatalf("branch sizes differ: decoded=%d raw=%d", len(items), len(raws))
				}
				for i := range raws {
					if d := issue624MustJSON(t, items[i].Object); string(d) != string(raws[i]) {
						t.Fatalf("item %d bytes differ between branches:\n decoded=%s\n     raw=%s", i, d, raws[i])
					}
				}

				// GET-by-name: every apiserver item is served, and equal.
				for i := range api.Items {
					a := &api.Items[i]
					got, hit := rw.GetObject(gvr, a.GetNamespace(), a.GetName())
					if !hit {
						t.Fatalf("GetObject(%s/%s): miss on a stored typed object", a.GetNamespace(), a.GetName())
					}
					g := issue624Normalize(t, issue624MustJSON(t, got.Object))
					w := issue624Normalize(t, issue624MustJSON(t, a.Object))
					if !reflect.DeepEqual(g, w) {
						t.Fatalf("GetObject(%s/%s) != apiserver\n got: %s\nwant: %s",
							a.GetNamespace(), a.GetName(), issue624MustJSON(t, g), issue624MustJSON(t, w))
					}
				}
			})
		}
	}
}

// newTestSchemeRBAC is the scheme the fake dynamic client needs to accept
// typed RBAC seeds.
func newTestSchemeRBAC() *k8sruntime.Scheme {
	s := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(s)
	return s
}

// TestIssue624_Guard_EveryTypedOverrideIsServable is the structural guard:
// it walks typedResourceOverrides ITSELF, so a typed kind added later is
// covered without anyone editing this test. For each override it builds a
// minimal object, runs the REAL transform (as the informer would), stores
// the output in a real indexer, and requires all three serve paths to render
// it. A new typed kind whose output the serve paths cannot render fails here
// by name instead of shipping as a silently empty list.
func TestIssue624_Guard_EveryTypedOverrideIsServable(t *testing.T) {
	if len(typedResourceOverrides) == 0 {
		t.Fatal("typedResourceOverrides is empty — guard would be vacuous")
	}
	for gvr, transform := range typedResourceOverrides {
		gvr, transform := gvr, transform
		t.Run(gvr.Resource, func(t *testing.T) {
			rw, idx, stop := newIndexerBackedWatcher(t, gvr)
			defer stop()

			apiVersion := gvr.Version
			if gvr.Group != "" {
				apiVersion = gvr.Group + "/" + gvr.Version
			}
			// apiVersion is set, kind deliberately is NOT: the converter must
			// recover the kind from the Go type, not from whatever the slot
			// happened to carry.
			in := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": apiVersion,
				"metadata":   map[string]any{"name": "guard-obj", "namespace": "guard-ns"},
			}}
			out, _, _, typedKind, _ := transform(in.DeepCopy())
			if _, still := out.(*unstructured.Unstructured); still {
				t.Fatalf("transform for %s returned Unstructured (conversion failed) — guard cannot exercise the typed shape", typedKind)
			}
			if err := idx.Add(out); err != nil {
				t.Fatalf("indexer add: %v", err)
			}

			items, ok := rw.ListObjectsServable(gvr, "")
			if !ok || len(items) != 1 {
				t.Fatalf("%s (%T): decoded list served=%v items=%d — want served=true, 1 item", gvr, out, ok, len(items))
			}
			if items[0].GetName() != "guard-obj" || items[0].GetAPIVersion() != apiVersion || items[0].GetKind() == "" {
				t.Fatalf("%s: rendered item lost identity: name=%q apiVersion=%q kind=%q",
					gvr, items[0].GetName(), items[0].GetAPIVersion(), items[0].GetKind())
			}
			raws, ok := rw.ListRawServable(gvr, "")
			if !ok || len(raws) != 1 {
				t.Fatalf("%s (%T): raw list served=%v items=%d — want served=true, 1 item", gvr, out, ok, len(raws))
			}
			if d := issue624MustJSON(t, items[0].Object); string(d) != string(raws[0]) {
				t.Fatalf("%s: raw item != decoded item:\n decoded=%s\n     raw=%s", gvr, d, raws[0])
			}
			if got, hit := rw.GetObject(gvr, "guard-ns", "guard-obj"); !hit || got.GetName() != "guard-obj" {
				t.Fatalf("%s: GetObject hit=%v", gvr, hit)
			}
		})
	}
}

// TestIssue624_UnrenderableSlot_FailsClosedAndCounts — the other half of the
// contract. A slot no serve path can render (here a metadata-only
// PartialObjectMetadata, which would serve a truncated object as the whole
// one) must make the read NOT servable — never a shorter list served as the
// truth — and must be counted, per site and GVR.
func TestIssue624_UnrenderableSlot_FailsClosedAndCounts(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.krateo.io", Version: "v1", Resource: "widgets624"}
	rw, idx, stop := newIndexerBackedWatcher(t, gvr)
	defer stop()

	good := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "example.krateo.io/v1", "kind": "Widget624",
		"metadata": map[string]any{"name": "good", "namespace": "ns"},
	}}
	bad := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "ns"}}
	if err := idx.Add(good); err != nil {
		t.Fatal(err)
	}
	if err := idx.Add(bad); err != nil {
		t.Fatal(err)
	}

	cell := func(site string) uint64 {
		for _, c := range UnrenderableStoreItemsSnapshot() {
			if c.Site == site && c.GVR == gvr.String() {
				return c.Count
			}
		}
		return 0
	}
	total0 := UnrenderableStoreItemsTotal()
	dec0, raw0, get0 := cell(unrenderableSiteListDecoded), cell(unrenderableSiteListRaw), cell(unrenderableSiteGet)

	if items, ok := rw.ListObjectsServable(gvr, "ns"); ok || items != nil {
		t.Fatalf("decoded: served=%v items=%d — an unrenderable slot must fail the list CLOSED", ok, len(items))
	}
	if raws, ok := rw.ListRawServable(gvr, ""); ok || raws != nil {
		t.Fatalf("raw: served=%v items=%d — an unrenderable slot must fail the list CLOSED", ok, len(raws))
	}
	if _, hit := rw.GetObject(gvr, "ns", "bad"); hit {
		t.Fatalf("GetObject of an unrenderable slot must miss")
	}
	// The renderable neighbour is still served by GET — fail-closed is per
	// read, not a poisoned GVR.
	if got, hit := rw.GetObject(gvr, "ns", "good"); !hit || got.GetName() != "good" {
		t.Fatalf("GetObject(good) hit=%v", hit)
	}

	if d := cell(unrenderableSiteListDecoded) - dec0; d != 1 {
		t.Fatalf("list_decoded cell delta = %d, want 1", d)
	}
	if d := cell(unrenderableSiteListRaw) - raw0; d != 1 {
		t.Fatalf("list_raw cell delta = %d, want 1", d)
	}
	if d := cell(unrenderableSiteGet) - get0; d != 1 {
		t.Fatalf("get cell delta = %d, want 1", d)
	}
	if d := UnrenderableStoreItemsTotal() - total0; d != 3 {
		t.Fatalf("total delta = %d, want 3", d)
	}

	// expvar surface: both keys published and reading the same counters.
	registerUnrenderableStoreItemsExpvar()
	if v := expvar.Get("snowplow_informer_unrenderable_store_items_total"); v == nil || v.String() == "0" {
		t.Fatalf("expvar total missing or zero after 3 fail-closed reads: %v", v)
	}
	cells := expvar.Get("snowplow_informer_unrenderable_store_items_cells")
	if cells == nil {
		t.Fatal("expvar cells key missing")
	}
	var m map[string]uint64
	if err := json.Unmarshal([]byte(cells.String()), &m); err != nil {
		t.Fatalf("cells not a JSON map: %v", err)
	}
	if m[unrenderableSiteGet+"|"+gvr.String()] == 0 {
		t.Fatalf("expvar cells lack the get|%s cell: %v", gvr, m)
	}
}
