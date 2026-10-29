//go:build integration
// +build integration

// list_query_618_kind_test.go — #618: informer-served LIST parity against a
// REAL apiserver (the kind cluster this package's TestMain brings up).
//
// The unit arms (list_query_618_falsifier_test.go) use hand-written expected
// sets. This arm uses the only fully independent oracle: the apiserver itself,
// asked with the EXACT same path + query. For every query the informer serves,
// the served item set must equal the apiserver's; for every query it must not
// serve, dispatchViaInformer must return served=false.
//
// It also records the apiserver evidence behind the one non-obvious rule in
// list_query.go: metadata.namespace is NOT applied on a cluster-wide path,
// because for some kinds (Node, Namespace) the apiserver rejects that field
// label with a 400 rather than filtering.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestIssue618_Kind_InformerParityWithRealApiserver(t *testing.T) {
	cmGVR := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	nss := []string{"q618-a", "q618-b"}

	f := features.New("618-informer-parity").
		Assess("served LIST equals the apiserver's for the same query", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			rc := cfg.Client().RESTConfig()
			cs, err := kubernetes.NewForConfig(rc)
			if err != nil {
				t.Fatal(err)
			}
			// #634: the namespace names are fixed (the query table below names
			// them), so a previous -count iteration's — or a crashed earlier
			// run's — namespace may still be terminating. Creating objects in it
			// is then forbidden. Wait for it to be gone before re-creating it,
			// and on cleanup wait for our own delete to finish, so the next
			// iteration starts from no namespace at all.
			for _, ns := range nss {
				q618WaitNamespaceGone(ctx, t, cs, ns, false)
			}
			t.Cleanup(func() {
				for _, ns := range nss {
					_ = cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
				}
				for _, ns := range nss {
					q618WaitNamespaceGone(context.Background(), t, cs, ns, true)
				}
			})
			for _, ns := range nss {
				_, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
				if err != nil && !apierrors.IsAlreadyExists(err) {
					t.Fatal(err)
				}
				for i, l := range []map[string]string{
					{"q618": "1", "env": "prod", "tier": "web"},
					{"q618": "1", "env": "dev", "tier": "db"},
					{"q618": "1", "env": "test"},
				} {
					cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
						Namespace: ns, Name: ns + "-w" + string(rune('1'+i)), Labels: l}}
					if _, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
						t.Fatal(err)
					}
				}
			}

			t.Setenv("CACHE_ENABLED", "true")
			t.Setenv("RESOLVED_CACHE_ENABLED", "true")
			dyn, err := dynamic.NewForConfig(rc)
			if err != nil {
				t.Fatal(err)
			}
			rw, err := cache.NewResourceWatcher(context.Background(), dyn)
			if err != nil || rw == nil {
				t.Fatalf("NewResourceWatcher: %v", err)
			}
			t.Cleanup(rw.Stop)
			if _, syncCh := rw.EnsureResourceType(cmGVR); syncCh != nil {
				select {
				case <-syncCh:
				case <-time.After(60 * time.Second):
					t.Fatal("configmaps informer did not sync")
				}
			}
			sctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			if err := rw.WaitForCacheSync(sctx, 60*time.Second); err != nil {
				t.Fatal(err)
			}
			cache.SetGlobal(rw)
			t.Cleanup(func() { cache.SetGlobal(nil) })
			deadline := time.Now().Add(30 * time.Second)
			for !rw.IsServable(cmGVR) && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
			}

			// The apiserver, asked the identical path + query.
			oracle := func(pathQuery string) ([]string, int) {
				res := cs.CoreV1().RESTClient().Get().RequestURI(pathQuery).Do(ctx)
				var code int
				res.StatusCode(&code)
				raw, err := res.Raw()
				if err != nil {
					return nil, code
				}
				return q618KindNames(t, raw, nss), code
			}

			admin := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: "kubernetes-admin", Groups: []string{"system:masters"}}))
			branches := map[string]context.Context{
				"decoded_rbac_branch":    admin,
				"raw_zero_decode_branch": cache.WithApistageContentResolve(admin),
			}

			cl := "/api/v1/configmaps"
			nsp := "/api/v1/namespaces/q618-b/configmaps"
			esc := url.QueryEscape
			served := []string{
				cl + "?labelSelector=q618%3D1",
				cl + "?labelSelector=" + esc("q618=1,env=prod"),
				cl + "?labelSelector=" + esc("q618=1,env in (prod,dev)"),
				cl + "?labelSelector=" + esc("q618=1,env notin (prod)"),
				cl + "?labelSelector=" + esc("q618=1,tier"),
				cl + "?labelSelector=" + esc("q618=1,!tier"),
				cl + "?labelSelector=" + esc("q618=1,env!=prod,tier"),
				cl + "?labelSelector=" + esc("q618=1") + "&fieldSelector=" + esc("metadata.name=q618-b-w2"),
				cl + "?labelSelector=" + esc("q618=1") + "&fieldSelector=" + esc("metadata.name!=q618-a-w1"),
				nsp,
				nsp + "?labelSelector=" + esc("env=prod"),
				nsp + "?fieldSelector=" + esc("metadata.namespace=q618-b"),
				nsp + "?fieldSelector=" + esc("metadata.namespace!=q618-b"),
				nsp + "?fieldSelector=" + esc("metadata.namespace=q618-b,metadata.name=q618-b-w3"),
				nsp + "?fieldSelector=" + esc("metadata.name=kube-root-ca.crt"),
			}
			for _, pq := range served {
				want, code := oracle(pq)
				if code != http.StatusOK {
					t.Fatalf("oracle %s: HTTP %d", pq, code)
				}
				for name, bctx := range branches {
					raw, ok, _ := dispatchViaInformer(bctx, buildCall(http.MethodGet, pq))
					if !ok {
						t.Fatalf("%s %s: not informer-served", name, pq)
					}
					got := q618KindNames(t, raw, nss)
					if strings.Join(got, ",") != strings.Join(want, ",") {
						t.Fatalf("RED #618 %s %s: informer served %v, the apiserver returned %v", name, pq, got, want)
					}
					t.Logf("RUN parity %s %s: %d items, equal to the apiserver", name, pq, len(got))
				}
			}

			fall := []string{
				cl + "?labelSelector=q618%3D1&limit=1",
				cl + "?fieldSelector=" + esc("metadata.namespace=q618-a"),
				cl + "?fieldSelector=" + esc("metadata.name=q618-a-w1,data.x=1"),
				nsp + "?resourceVersion=0",
				nsp + "/q618-b-w1?labelSelector=env%3Dprod",
			}
			for _, pq := range fall {
				for name, bctx := range branches {
					if _, ok, _ := dispatchViaInformer(bctx, buildCall(http.MethodGet, pq)); ok {
						t.Fatalf("RED #618 %s %s: must fall through to the apiserver", name, pq)
					}
				}
			}
			// limit is a real apiserver page: fewer items than the selector
			// matches, which the informer could never reproduce.
			if got, _ := oracle(cl + "?labelSelector=q618%3D1&limit=1"); len(got) != 1 {
				t.Fatalf("oracle limit=1 returned %d items", len(got))
			}

			// The design evidence: metadata.namespace on a cluster-scoped kind.
			res := cs.CoreV1().RESTClient().Get().RequestURI("/api/v1/nodes?fieldSelector=" + esc("metadata.namespace=x")).Do(ctx)
			var code int
			res.StatusCode(&code)
			t.Logf("RUN evidence: GET /api/v1/nodes?fieldSelector=metadata.namespace=x -> HTTP %d (err=%v)", code, res.Error())
			if code != http.StatusBadRequest {
				t.Fatalf("the namespace_field_cluster_path rule assumes the apiserver 400s metadata.namespace on nodes; got HTTP %d", code)
			}
			return ctx
		}).Feature()

	testenv.Test(t, f)
}

// q618WaitNamespaceGone blocks until namespace ns does not exist (#634).
// With mustBeGone=false (setup), a namespace that exists and is NOT being
// deleted is left alone and the call returns at once (setup reuses it, as
// before); only a terminating one is waited out. With mustBeGone=true
// (cleanup, after our Delete), any remaining namespace is waited out. A namespace that outlives the deadline fails
// the test: proceeding would reproduce the "being terminated" flake with a
// less obvious message.
func q618WaitNamespaceGone(ctx context.Context, t *testing.T, cs kubernetes.Interface, ns string, mustBeGone bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		got, err := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			t.Fatalf("get namespace %s: %v", ns, err)
		}
		if !mustBeGone && got.DeletionTimestamp == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %s still present (phase=%s) after 2m; the next iteration would hit 'being terminated'", ns, got.Status.Phase)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// q618KindNames returns "<ns>/<name>" for items in the test namespaces.
func q618KindNames(t *testing.T, raw []byte, nss []string) []string {
	t.Helper()
	var env struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	keep := map[string]bool{}
	for _, n := range nss {
		keep[n] = true
	}
	var out []string
	for _, it := range env.Items {
		if keep[it.Metadata.Namespace] {
			out = append(out, it.Metadata.Namespace+"/"+it.Metadata.Name)
		}
	}
	sort.Strings(out)
	return out
}
