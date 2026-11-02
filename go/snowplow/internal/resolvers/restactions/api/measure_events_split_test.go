//go:build unit

package api

// MEASUREMENT HARNESS (not a gate): how the events RESTAction's resolve time
// splits between its stages. Run:
//
//	SNOWPLOW_CRD_CHARTS=/tmp/snowplow-runs/606/<run>/crd-charts \
//	  go test -tags unit -run TestMeasureEventsSplit -v ./internal/resolvers/restactions/api/

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

var (
	mCRDGVR  = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	mCompGVR = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1-2-2", Resource: "githubscaffoldingwithcompositionpages"}
)

func mLoadCRDs(t *testing.T, root string) ([]runtime.Object, int) {
	var out []runtime.Object
	total := 0
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !(strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml")) {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil || !strings.Contains(string(b), "kind: CustomResourceDefinition") {
			return nil
		}
		for _, doc := range strings.Split(string(b), "\n---") {
			if strings.Contains(doc, "{{") || !strings.Contains(doc, "CustomResourceDefinition") {
				continue
			}
			var m map[string]any
			if yaml.Unmarshal([]byte(doc), &m) != nil || m["kind"] != "CustomResourceDefinition" {
				continue
			}
			js, _ := json.Marshal(m)
			total += len(js)
			out = append(out, &unstructured.Unstructured{Object: m})
		}
		return nil
	})
	return out, total
}

func mCompositionCRD() runtime.Object {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": mCompGVR.Resource + "." + mCompGVR.Group},
		"spec": map[string]any{
			"group":    mCompGVR.Group,
			"names":    map[string]any{"plural": mCompGVR.Resource, "kind": "GithubScaffoldingWithCompositionPage"},
			"scope":    "Namespaced",
			"versions": []any{map[string]any{"name": mCompGVR.Version, "served": true, "storage": true}},
		},
		"status": map[string]any{"acceptedNames": map[string]any{"plural": mCompGVR.Resource}},
	}}
}

func mComposition(ns string, i int) runtime.Object {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": mCompGVR.Group + "/" + mCompGVR.Version, "kind": "GithubScaffoldingWithCompositionPage",
		"metadata": map[string]any{
			"name": fmt.Sprintf("comp-%05d", i), "namespace": ns,
			"uid":    fmt.Sprintf("uid-%05d", i),
			"labels": map[string]any{"krateo.io/composition-id": fmt.Sprintf("cid-%05d", i)},
		},
		"spec": map[string]any{"app": map[string]any{"service": map[string]any{"port": int64(31180), "type": "NodePort"}},
			"git": map[string]any{"toRepo": map[string]any{"name": fmt.Sprintf("repo-%d", i), "org": "bench"}}},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True", "reason": "Available"}}},
	}}
}

func mRule(name string, gvr schema.GroupVersionResource) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{gvr.Group}, Resources: []string{"*"}, Verbs: []string{"get", "list"}}},
	}
}

func mCRB(name, role, group string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: "rbac.authorization.k8s.io", Name: group}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

func mRB(ns, role, group string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "rb-" + group},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: "rbac.authorization.k8s.io", Name: group}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

func mEventsStages() []*templates.API {
	return []*templates.API{
		{Name: "crds", Path: "/apis/apiextensions.k8s.io/v1/customresourcedefinitions", Verb: ptr.To("GET"),
			Filter:          ptr.To(`[.crds.items[] | select(.spec.group == "composition.krateo.io") | {plural: .status.acceptedNames.plural, version: ((([.spec.versions[] | select(.served and .storage)])[0]) // (([.spec.versions[] | select(.served)])[0])).name}]`),
			ContinueOnError: ptr.To(true)},
		{Name: "comps", DependsOn: &templates.Dependency{Name: "crds", Iterator: ptr.To(".crds")},
			Path: `${ "/apis/composition.krateo.io/" + .version + "/" + .plural }`, Verb: ptr.To("GET"),
			Filter:          ptr.To(`[.comps.items[]? | {name: .metadata.name, namespace: .metadata.namespace, compositionId: (.metadata.labels["krateo.io/composition-id"] // "")}]`),
			ContinueOnError: ptr.To(true)},
		{Name: "getEvents", Path: "/events", Verb: ptr.To("GET"), ContinueOnError: ptr.To(true)},
	}
}

const mEventsFilter = `( [ (.comps // [])[] | select((.compositionId // "") != "")
        | { key: .compositionId, value: { ns: .namespace, name: .name } } ]
      | from_entries ) as $byCid
    | {
      list: (
        ( .getEvents | if type == "array" then . elif type == "object" then (.resources // .items // []) else [] end )
        | group_by((.involvedObject.uid // "") + "|" + (.reason // "") + "|" + (.message // ""))
        | map(max_by(.lastTimestamp // .eventTime // .firstTimestamp // ""))
        | sort_by(.lastTimestamp // .eventTime // .firstTimestamp // "")
        | reverse
        | .[0:50]
        | map(. + { route: ( ($byCid[(.compositionId // "")] // null) as $c
                             | (.involvedObject.kind // "") as $k
                             | (.involvedObject.name // "") as $n
                             | (.involvedObject.namespace // "") as $ons
                             | if $k == "CompositionDefinition" and $n != "" and $ons != ""
                               then ("/blueprints/" + $ons + "/" + $n)
                               elif $c != null then ("/compositions/" + $c.ns + "/" + $c.name)
                               else "" end ) })
      )
    }`

func mEventsBody(n int) []byte {
	evs := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		evs = append(evs, map[string]any{
			"metadata":       map[string]any{"uid": fmt.Sprintf("ev-%d", i), "creationTimestamp": "2026-11-01T07:00:00Z"},
			"involvedObject": map[string]any{"apiVersion": mCompGVR.Group + "/" + mCompGVR.Version, "kind": "GithubScaffoldingWithCompositionPage", "name": fmt.Sprintf("comp-%05d", i), "namespace": "bench-ns-01", "uid": fmt.Sprintf("uid-%05d", i)},
			"reason":         "CompositionUpdated", "message": fmt.Sprintf("composition %d reconciled", i), "type": "Normal",
			"compositionId": fmt.Sprintf("cid-%05d", i),
			"lastTimestamp": fmt.Sprintf("2026-11-01T07:%02d:%02dZ", (i/60)%60, i%60),
			"source":        map[string]any{"component": "core-provider"},
		})
	}
	b, _ := json.Marshal(evs)
	return b
}

func TestMeasureEventsSplit(t *testing.T) {
	root := os.Getenv("SNOWPLOW_CRD_CHARTS")
	if root == "" {
		t.Skip("SNOWPLOW_CRD_CHARTS unset: measurement harness only")
	}
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()

	crds, crdBytes := mLoadCRDs(t, root)
	seed := append([]runtime.Object{}, crds...)
	seed = append(seed, mCompositionCRD())
	nComps := 50000
	for i := 0; i < nComps; i++ {
		seed = append(seed, mComposition(fmt.Sprintf("bench-ns-%02d", i%50+1), i))
	}
	seed = append(seed, mRule("crd-reader", mCRDGVR), mRule("comp-reader", mCompGVR),
		mCRB("devs-crds", "crd-reader", "devs"), mCRB("admins-comps", "comp-reader", "admins"),
		mRB("bench-ns-01", "comp-reader", "team-01"))
	t.Logf("CRDs loaded: %d (+1 composition CRD), %d JSON bytes; compositions: %d", len(crds), crdBytes, nComps)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		mCRDGVR: "CustomResourceDefinitionList", mCompGVR: "GithubScaffoldingWithCompositionPageList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("watcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	for _, g := range []schema.GroupVersionResource{mCRDGVR, mCompGVR} {
		_, ch := rw.EnsureResourceType(g)
		select {
		case <-ch:
		case <-time.After(120 * time.Second):
			t.Fatalf("sync %s", g)
		}
	}
	ctx0, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx0, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	nEvents := 200
	body := mEventsBody(nEvents)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	t.Logf("external /events body: %d events, %d bytes", nEvents, len(body))

	stages := mEventsStages()
	resolveAs := func(user jwtutil.UserInfo, items []*templates.API) map[string]any {
		ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(user))
		ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
		return Resolve(ctx, ResolveOptions{RC: &rest.Config{}, Items: items,
			RESTActionNamespace: "krateo-dashboard", RESTActionName: "events"})
	}
	filter := func(dict map[string]any) string {
		s, err := jqutil.Eval(context.Background(), jqutil.EvalOptions{Query: mEventsFilter, Data: dict, ModuleLoader: jqsupport.ModuleLoader()})
		if err != nil {
			t.Fatalf("filter: %v", err)
		}
		return s
	}
	med := func(d []time.Duration) time.Duration {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		return d[len(d)/2]
	}
	users := []struct {
		label string
		u     jwtutil.UserInfo
	}{
		{"devs+team (cluster-wide comps LIST forbidden, like every browser fresh class)", jwtutil.UserInfo{Username: "u-dev", Groups: []string{"devs", "team-01"}}},
		{"admin (cluster-wide comps LIST allowed: 50K)", jwtutil.UserInfo{Username: "u-admin", Groups: []string{"devs", "admins"}}},
	}
	for ui, uc := range users {
		iters := 400
		if ui == 1 {
			iters = 15
		}
		// warm content cells
		for i := 0; i < 3; i++ {
			d := resolveAs(uc.u, stages)
			_ = filter(d)
		}
		d := resolveAs(uc.u, stages)
		comps, _ := d["comps"].([]any)
		crdsOut, _ := json.Marshal(d["crds"])
		t.Logf("[%s] crds out=%s comps items=%d comps type=%T", uc.label, crdsOut, len(comps), d["comps"])
		var t1, t2, t3, tf []time.Duration
		for i := 0; i < iters; i++ {
			s := time.Now()
			resolveAs(uc.u, stages[:1])
			t1 = append(t1, time.Since(s))
			s = time.Now()
			resolveAs(uc.u, stages[:2])
			t2 = append(t2, time.Since(s))
			s = time.Now()
			dd := resolveAs(uc.u, stages)
			t3 = append(t3, time.Since(s))
			s = time.Now()
			_ = filter(dd)
			tf = append(tf, time.Since(s))
		}
		m1, m2, m3, mf := med(t1), med(t2), med(t3), med(tf)
		t.Logf("[%s] median over %d: crds=%v  +comps=%v  +getEvents(local, 0ms server)=%v  topFilter=%v  total(resolve+filter)=%v",
			uc.label, iters, m1, m2-m1, m3-m2, mf, m3+mf)
	}
}
