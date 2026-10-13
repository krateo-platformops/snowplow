//go:build integration
// +build integration

// issue624_rbac_kind_integration_test.go — #624 parity arm against a REAL
// apiserver (kind).
//
// WHY A KIND ARM. The fake-dynamic arms in issue624_typed_store_items_test.go
// seed objects the test itself wrote, so they only carry the fields the
// test thought of: no aggregationRule, no apiserver-defaulted fields, no
// controller-written rules. A lossy typed→Unstructured converter (one that
// drops aggregationRule, say) passes every fake arm. Here the expected
// value is whatever a real kube-apiserver returns, for every object it
// holds — the kind bootstrap set (admin/edit/view are aggregated
// ClusterRoles) plus the fixtures below.
//
// THE PROPERTY, for each of the four RBAC GVRs (typed stores), cluster-wide
// and — for the namespaced two — per namespace:
//   - decoded LIST (ListObjectsServable) == apiserver LIST, item for item,
//     after the uniform strip (defaultStripUnstructured);
//   - raw LIST (ListRawServable) == apiserver LIST, same comparison;
//   - GET by name (GetObject) == apiserver GET, same comparison;
//   - decoded and raw items are byte-identical.
//
// CLUSTER. Its own kind cluster, krateo-<pid> (#522: a per-process name, so
// the Destroy in Cleanup can only ever delete this process's cluster). The
// cache package already has a TestMain (featuregate_watchlist_test.go), so
// the cluster is created and destroyed inside the test.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/e2e-framework/support/kind"
)

const issue624KindNS = "issue624-kind"

func issue624KindGVR(res string) schema.GroupVersionResource {
	for _, g := range rbacTypedGVRs {
		if g.Resource == res {
			return g
		}
	}
	panic("no rbac gvr " + res)
}

// issue624KindCanon renders a value through encoding/json into a generic map,
// so typed-origin and apiserver-origin values compare as JSON values.
func issue624KindCanon(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func issue624KindKey(ns, name string) string { return ns + "/" + name }

// issue624KindStripped is the apiserver object as the cache must serve it:
// the uniform strip every GVR gets, then a JSON round-trip.
func issue624KindStripped(u *unstructured.Unstructured) (map[string]any, error) {
	c := u.DeepCopy()
	defaultStripUnstructured(c)
	return issue624KindCanon(c.Object)
}

// issue624KindFixtures creates the shapes the bootstrap set lacks or that a
// lossy converter is most likely to mangle.
func issue624KindFixtures(t *testing.T, ctx context.Context, dyn dynamic.Interface) {
	t.Helper()
	create := func(res, ns string, obj map[string]any) {
		t.Helper()
		u := &unstructured.Unstructured{Object: obj}
		r := dyn.Resource(issue624KindGVR(res))
		var err error
		if ns != "" {
			_, err = r.Namespace(ns).Create(ctx, u, metav1.CreateOptions{})
		} else {
			_, err = r.Create(ctx, u, metav1.CreateOptions{})
		}
		if err != nil {
			t.Fatalf("create %s %s/%s: %v", res, ns, u.GetName(), err)
		}
	}
	nsGVR := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	if _, err := dyn.Resource(nsGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": issue624KindNS},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	const api = "rbac.authorization.k8s.io/v1"
	// Aggregation source + the aggregated ClusterRole whose rules the
	// clusterrole-aggregation controller fills in.
	create("clusterroles", "", map[string]any{"apiVersion": api, "kind": "ClusterRole",
		"metadata": map[string]any{"name": "issue624-agg-source", "labels": map[string]any{"issue624/aggregate": "true"}},
		"rules":    []any{map[string]any{"apiGroups": []any{"composition.krateo.io"}, "resources": []any{"*"}, "verbs": []any{"get", "list", "watch"}}}})
	create("clusterroles", "", map[string]any{"apiVersion": api, "kind": "ClusterRole",
		"metadata": map[string]any{"name": "issue624-aggregated"},
		"aggregationRule": map[string]any{"clusterRoleSelectors": []any{
			map[string]any{"matchLabels": map[string]any{"issue624/aggregate": "true"}},
		}}})
	// Empty ClusterRole: no rules at all.
	create("clusterroles", "", map[string]any{"apiVersion": api, "kind": "ClusterRole",
		"metadata": map[string]any{"name": "issue624-empty"}})
	// A portal-style grant: labelled Role with resourceNames and a
	// last-applied annotation (exercises the strip), bound to a
	// ServiceAccount and a User.
	create("roles", issue624KindNS, map[string]any{"apiVersion": api, "kind": "Role",
		"metadata": map[string]any{"name": "issue624-grant", "namespace": issue624KindNS,
			"labels":      map[string]any{"krateo.io/managed-by": "portal-access"},
			"annotations": map[string]any{"kubectl.kubernetes.io/last-applied-configuration": "{}", "keep": "me"}},
		"rules": []any{
			map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}, "resourceNames": []any{"one"}},
			map[string]any{"apiGroups": []any{"composition.krateo.io"}, "resources": []any{"*"}, "verbs": []any{"get", "list"}},
		}})
	create("rolebindings", issue624KindNS, map[string]any{"apiVersion": api, "kind": "RoleBinding",
		"metadata": map[string]any{"name": "issue624-grant", "namespace": issue624KindNS,
			"labels": map[string]any{"krateo.io/managed-by": "portal-access"}},
		"subjects": []any{
			map[string]any{"kind": "ServiceAccount", "name": "issue624-sa", "namespace": issue624KindNS},
			map[string]any{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": "alice"},
		},
		"roleRef": map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "issue624-grant"}})
	create("clusterrolebindings", "", map[string]any{"apiVersion": api, "kind": "ClusterRoleBinding",
		"metadata": map[string]any{"name": "issue624-aggregated"},
		"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "issue624-sa", "namespace": issue624KindNS}},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "issue624-aggregated"}})

	// Wait for the aggregation controller to fill the aggregated role, so
	// the fixture carries BOTH aggregationRule and controller-written rules.
	deadline := time.Now().Add(60 * time.Second)
	for {
		u, err := dyn.Resource(issue624KindGVR("clusterroles")).Get(ctx, "issue624-aggregated", metav1.GetOptions{})
		if err == nil {
			if rules, _, _ := unstructured.NestedSlice(u.Object, "rules"); len(rules) > 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("aggregated ClusterRole never received rules from the aggregation controller (last err %v)", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// issue624KindCompare compares one (gvr, namespace) read on all three serve
// paths against the apiserver. It returns the mismatches (empty = parity)
// and the set of keys served.
func issue624KindCompare(ctx context.Context, rw *ResourceWatcher, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns string) ([]string, map[string]map[string]any) {
	var errs []string
	fail := func(f string, a ...any) {
		errs = append(errs, fmt.Sprintf("%s ns=%q: ", gvr.Resource, ns)+fmt.Sprintf(f, a...))
	}

	var api *unstructured.UnstructuredList
	var err error
	if ns == "" {
		api, err = dyn.Resource(gvr).List(ctx, metav1.ListOptions{})
	} else {
		api, err = dyn.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		fail("apiserver LIST: %v", err)
		return errs, nil
	}
	want := map[string]map[string]any{}
	for i := range api.Items {
		m, err := issue624KindStripped(&api.Items[i])
		if err != nil {
			fail("canon apiserver item: %v", err)
			return errs, nil
		}
		want[issue624KindKey(api.Items[i].GetNamespace(), api.Items[i].GetName())] = m
	}

	items, ok := rw.ListObjectsServable(gvr, ns)
	if !ok {
		fail("decoded LIST served=false")
		return errs, nil
	}
	raws, ok := rw.ListRawServable(gvr, ns)
	if !ok {
		fail("raw LIST served=false")
		return errs, nil
	}
	if len(items) != len(api.Items) || len(raws) != len(api.Items) {
		fail("COUNT apiserver=%d decoded=%d raw=%d", len(api.Items), len(items), len(raws))
	}

	served := map[string]map[string]any{}
	n := len(items)
	if len(raws) < n {
		n = len(raws)
	}
	for i := 0; i < n; i++ {
		it := items[i]
		k := issue624KindKey(it.GetNamespace(), it.GetName())

		// decoded vs raw: byte-identical.
		d, err := json.Marshal(it.Object)
		if err != nil {
			fail("%s marshal decoded: %v", k, err)
			continue
		}
		if string(d) != string(raws[i]) {
			fail("%s BYTES differ decoded vs raw\n decoded=%s\n     raw=%s", k, d, raws[i])
		}

		w, present := want[k]
		if !present {
			fail("%s served but not on the apiserver", k)
			continue
		}
		// decoded LIST == apiserver.
		dm, err := issue624KindCanon(it.Object)
		if err != nil {
			fail("%s canon decoded: %v", k, err)
			continue
		}
		if !reflect.DeepEqual(dm, w) {
			gb, _ := json.Marshal(dm)
			wb, _ := json.Marshal(w)
			fail("%s decoded LIST VALUE differs from apiserver\n got %s\nwant %s", k, gb, wb)
		}
		// raw LIST == apiserver.
		var rm map[string]any
		if err := json.Unmarshal(raws[i], &rm); err != nil {
			fail("%s raw item not JSON: %v", k, err)
		} else if !reflect.DeepEqual(rm, w) {
			wb, _ := json.Marshal(w)
			fail("%s raw LIST VALUE differs from apiserver\n got %s\nwant %s", k, raws[i], wb)
		}
		served[k] = dm

		// GET == apiserver GET.
		var apiGet *unstructured.Unstructured
		if it.GetNamespace() == "" {
			apiGet, err = dyn.Resource(gvr).Get(ctx, it.GetName(), metav1.GetOptions{})
		} else {
			apiGet, err = dyn.Resource(gvr).Namespace(it.GetNamespace()).Get(ctx, it.GetName(), metav1.GetOptions{})
		}
		if err != nil {
			fail("%s apiserver GET: %v", k, err)
			continue
		}
		wg, err := issue624KindStripped(apiGet)
		if err != nil {
			fail("%s canon apiserver GET: %v", k, err)
			continue
		}
		got, hit := rw.GetObject(gvr, it.GetNamespace(), it.GetName())
		if !hit || got == nil {
			fail("%s GET hit=false", k)
			continue
		}
		gm, err := issue624KindCanon(got.Object)
		if err != nil {
			fail("%s canon GET: %v", k, err)
			continue
		}
		if !reflect.DeepEqual(gm, wg) {
			gb, _ := json.Marshal(gm)
			wb, _ := json.Marshal(wg)
			fail("%s GET VALUE differs from apiserver GET\n got %s\nwant %s", k, gb, wb)
		}
	}
	return errs, served
}

func TestIssue624_Kind_RBACServeParityWithRealApiserver(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	ctx := context.Background()
	clusterName := fmt.Sprintf("krateo-%d", os.Getpid())
	provider := kind.NewProvider().WithName(clusterName)
	cctx, ccancel := context.WithTimeout(ctx, 6*time.Minute)
	defer ccancel()
	kubeconfig, err := provider.Create(cctx)
	if err != nil {
		t.Fatalf("create kind cluster %s: %v", clusterName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer dcancel()
		if err := provider.Destroy(dctx); err != nil {
			t.Logf("destroy kind cluster %s: %v", clusterName, err)
		}
	})

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	// Per-item apiserver GETs: lift the client-side limiter (default 5 QPS).
	cfg.QPS, cfg.Burst = 100, 200
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	issue624KindFixtures(t, ctx, dyn)

	rw, err := NewResourceWatcher(ctx, dyn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rw.Stop)
	sctx, scancel := context.WithTimeout(ctx, 90*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 90*time.Second); err != nil {
		t.Fatal(err)
	}

	// Non-vacuity: the stores really hold TYPED objects (the #624 shape),
	// one per kind — otherwise this arm would test the Unstructured path.
	for _, c := range []struct {
		res, ns, name string
		want          any
	}{
		{"roles", issue624KindNS, "issue624-grant", &rbacv1.Role{}},
		{"rolebindings", issue624KindNS, "issue624-grant", &rbacv1.RoleBinding{}},
		{"clusterroles", "", "issue624-aggregated", &rbacv1.ClusterRole{}},
		{"clusterrolebindings", "", "issue624-aggregated", &rbacv1.ClusterRoleBinding{}},
	} {
		var obj any
		deadline := time.Now().Add(30 * time.Second)
		for {
			o, ok := rw.GetTypedObject(issue624KindGVR(c.res), c.ns, c.name)
			if ok {
				obj = o
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s %s/%s never reached the typed store", c.res, c.ns, c.name)
			}
			time.Sleep(200 * time.Millisecond)
		}
		if reflect.TypeOf(obj) != reflect.TypeOf(c.want) {
			t.Fatalf("%s store slot is %T, want %T — the arm would not exercise the typed path", c.res, obj, c.want)
		}
	}

	type read struct {
		gvr schema.GroupVersionResource
		ns  string
	}
	var reads []read
	for _, gvr := range rbacTypedGVRs {
		reads = append(reads, read{gvr, ""})
		if gvr.Resource == "roles" || gvr.Resource == "rolebindings" {
			for _, ns := range []string{issue624KindNS, "default", "kube-system", "kube-public"} {
				reads = append(reads, read{gvr, ns})
			}
		}
	}

	// The apiserver keeps moving (controllers update bootstrap objects), so
	// each read is compared until it reaches parity or the deadline passes;
	// a converter that loses a field never reaches parity.
	total := 0
	allServed := map[string]map[string]map[string]any{}
	for _, r := range reads {
		name := r.gvr.Resource + "/" + r.ns
		if r.ns == "" {
			name = r.gvr.Resource + "/cluster-wide"
		}
		t.Run(name, func(t *testing.T) {
			deadline := time.Now().Add(30 * time.Second)
			attempts := 0
			for {
				attempts++
				errs, served := issue624KindCompare(ctx, rw, dyn, r.gvr, r.ns)
				if len(errs) == 0 {
					// An empty namespace ("default" holds no RBAC objects
					// on kind) is a valid parity case: [] == []. The
					// cluster-wide reads and the fixture namespace must
					// not be vacuous.
					if len(served) == 0 && (r.ns == "" || r.ns == issue624KindNS) {
						t.Fatalf("no items served — the read is vacuous")
					}
					total += len(served)
					if r.ns == "" {
						allServed[r.gvr.Resource] = served
					}
					t.Logf("parity: %d items (attempt %d)", len(served), attempts)
					return
				}
				if time.Now().After(deadline) {
					sort.Strings(errs)
					if len(errs) > 20 {
						errs = append(errs[:20], fmt.Sprintf("... and %d more", len(errs)-20))
					}
					t.Fatalf("no parity after %d attempts:\n%s", attempts, strings.Join(errs, "\n"))
				}
				time.Sleep(500 * time.Millisecond)
			}
		})
	}

	// Each fixture is present in the cluster-wide served list with the
	// shape that makes it a fixture (so parity above covered it).
	t.Run("fixtures-covered", func(t *testing.T) {
		mustHave := func(res, key string) map[string]any {
			t.Helper()
			m, ok := allServed[res][key]
			if !ok {
				t.Fatalf("fixture %s %s not in the served cluster-wide list", res, key)
			}
			return m
		}
		agg := mustHave("clusterroles", "/issue624-aggregated")
		if _, ok := agg["aggregationRule"]; !ok {
			t.Fatalf("served aggregated ClusterRole has no aggregationRule: %v", agg)
		}
		if rules, _ := agg["rules"].([]any); len(rules) == 0 {
			t.Fatalf("served aggregated ClusterRole has no rules: %v", agg)
		}
		empty := mustHave("clusterroles", "/issue624-empty")
		if rules, _ := empty["rules"].([]any); len(rules) != 0 {
			t.Fatalf("served empty ClusterRole has rules: %v", empty)
		}
		mustHave("roles", issue624KindNS+"/issue624-grant")
		rb := mustHave("rolebindings", issue624KindNS+"/issue624-grant")
		subj, _ := rb["subjects"].([]any)
		sa := false
		for _, s := range subj {
			if sm, _ := s.(map[string]any); sm["kind"] == "ServiceAccount" && sm["namespace"] == issue624KindNS {
				sa = true
			}
		}
		if !sa {
			t.Fatalf("served RoleBinding lacks its ServiceAccount subject: %v", rb)
		}
		mustHave("clusterrolebindings", "/issue624-aggregated")
	})

	t.Logf("compared %d items on decoded LIST, raw LIST and GET against the real apiserver", total)
}
