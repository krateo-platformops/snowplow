package handlers

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	idynamic "github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// #970 — the Export button carries the widget's extras (its on-screen
// filters) as ?extras=<json> on /export. /export forwards the parameter to
// its in-process /call re-dispatch, so the file holds exactly the rows the
// widget read shows under the same filters, and the re-dispatch lands on the
// same resolved-cache cell as that read.
//
// Through the REAL chain, cache on: Export(Dispatcher(dispatchers.All())(Call()))
// over a ResourceWatcher that serves the RESTAction, the ConfigMaps its one
// stage lists and the RBAC objects. The stage is served from the informer
// under the caller's RBAC; nothing is dialed (the service-account config
// points at an unreachable address, so a dial would fail the arm). The
// RESTAction's output is a function of its extras: spec.filter keeps the
// ConfigMaps of the extras' team.

var (
	raGVR970 = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	cmGVR970 = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
)

const extrasNS970 = "krateo-system"

// raFilter970 keeps the listed ConfigMaps whose team is the extras' team
// and, on a paginated request, the window .slice asks for.
const raFilter970 = `. as $x | {rows: ((.cms // []) | map(select(.team == $x.team)) | if $x.slice then .[$x.slice.offset:($x.slice.offset + $x.slice.perPage)] else . end)}`

const cmFilter970 = `[.cms.items[] | {id: .metadata.name, team: .data.team}]`

func seedExtrasRESTAction(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "")
	prevTM := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prevTM) })

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		raGVR970: "RESTActionList",
		cmGVR970: "ConfigMapList",
	}
	rule := []rbacv1.PolicyRule{
		{Verbs: []string{"get", "list"}, APIGroups: []string{raGVR970.Group}, Resources: []string{raGVR970.Resource}},
		{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"configmaps"}},
	}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "ra-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "devs-ra", UID: types.UID("uid-devs-ra")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", Name: "devs"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader"},
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "templates.krateo.io/v1",
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": "team-rows", "namespace": extrasNS970, "uid": "uid-team-rows"},
			"spec": map[string]any{
				"api": []any{map[string]any{
					"name": "cms", "path": "/api/v1/namespaces/team-a/configmaps", "filter": cmFilter970,
				}},
				"filter": raFilter970,
			},
		}},
	}
	for i := range 12 {
		seed = append(seed, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": fmt.Sprintf("cm-%02d", i), "namespace": "team-a"},
			"data":     map[string]any{"team": []string{"red", "green", "blue"}[i%3]},
		}})
	}
	t.Cleanup(idynamic.SetServiceAccountRESTConfigForTest(&rest.Config{Host: "https://127.0.0.1:1"}))

	// The caller's clientconfig, from the Secrets informer (the production
	// cache-on path). Its server is unreachable: the stage must be served
	// from the ConfigMaps informer, never dialed.
	t.Setenv("AUTHN_NAMESPACE", extrasNS970)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	t.Cleanup(func() {
		cache.ResetSecretsInformerForTest()
		cache.ResetSecretsSnapshotForTest()
	})
	t.Cleanup(cache.SetSecretsClientForTest(k8sfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "usera-clientconfig", Namespace: extrasNS970},
		Data:       map[string][]byte{"server-url": []byte("https://127.0.0.1:1"), "token": []byte("token-userA")},
	})))
	sctx, scancel := context.WithCancel(context.Background())
	t.Cleanup(scancel)
	if err := cache.StartSecretsInformer(sctx, nil, extrasNS970); err != nil {
		t.Fatalf("StartSecretsInformer: %v", err)
	}

	cache.ResetRefreshBroadcasterForTest()
	cache.ResetDepsForTest()
	cache.ResetResolvedCacheForTest()
	t.Cleanup(func() {
		cache.ResetDepsForTest()
		cache.ResetRefreshBroadcasterForTest()
		cache.ResetResolvedCacheForTest()
	})

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer syncCancel()
	if err := rw.WaitForCacheSync(syncCtx, 10*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	_, _ = rw.EnsureResourceType(raGVR970)
	_, _ = rw.EnsureResourceType(cmGVR970)
	_ = rw.WaitForCacheSync(syncCtx, 10*time.Second)
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
	deadline := time.Now().Add(10 * time.Second)
	for !rw.IsServable(raGVR970) || !rw.IsServable(cmGVR970) {
		if time.Now().After(deadline) {
			t.Fatal("the restactions and configmaps GVRs never became servable from the informer")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if store := cache.ResolvedCache(); store != nil {
		cache.Deps().SetStore(store)
	}
}

// serveAsDev serves target on h as userA (group devs).
func serveAsDev(h http.Handler, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithAccessToken("token-userA"),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://127.0.0.1:1"}),
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// l1Hits is the restactions L1 hit count the dispatcher publishes.
func l1Hits(t *testing.T) float64 {
	t.Helper()
	v := expvar.Get("snowplow_dispatch_l1_lookups")
	if v == nil {
		return 0
	}
	// {"<handler>|<gvr>": {"hit_total": n, ...}}
	var cells map[string]map[string]float64
	if err := json.Unmarshal([]byte(v.String()), &cells); err != nil {
		t.Fatalf("snowplow_dispatch_l1_lookups: %v (%s)", err, v.String())
	}
	var hits float64
	for cell, c := range cells {
		if strings.HasPrefix(cell, "restactions|") {
			hits += c["hit_total"]
		}
	}
	return hits
}

func TestExport970_ExtrasReachTheCallAndItsCacheCell(t *testing.T) {
	seedExtrasRESTAction(t)
	dispatchers.RegisterL1LookupMetricsForTest()

	chain := Dispatcher(dispatchers.All())(Call())
	// The export's re-dispatches, observed on their way through the real
	// chain: query and the refresh key the dispatcher stamps.
	type seen struct {
		extras, key string
	}
	var inner []seen
	spy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chain.ServeHTTP(w, r)
		inner = append(inner, seen{extras: r.URL.Query().Get("extras"), key: w.Header().Get("X-Snowplow-Refresh-Key")})
	})
	export := Export(spy)

	coords := "apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + extrasNS970 + "&name=team-rows"
	type row struct {
		ID   string `json:"id"`
		Team string `json:"team"`
	}
	callRows := func(rec *httptest.ResponseRecorder) []row {
		t.Helper()
		var ra struct {
			Status struct {
				Rows []row `json:"rows"`
			} `json:"status"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &ra) != nil {
			t.Fatalf("/call: %d %s", rec.Code, rec.Body.String())
		}
		return ra.Status.Rows
	}

	keys := map[string]string{}
	for _, team := range []string{"green", "blue"} {
		extras := url.QueryEscape(`{"team":"` + team + `"}`)
		// The widget read: the same window the export's first internal
		// page asks for (page=1&perPage=500).
		read := serveAsDev(chain, "/call?"+coords+"&extras="+extras+"&page=1&perPage="+itoa(exportPageSize))
		want := callRows(read)
		readKey := read.Header().Get("X-Snowplow-Refresh-Key")
		keys[team] = readKey
		if len(want) != 4 || readKey == "" {
			t.Fatalf("%s: /call gave %d rows and key %q; want the 4 %s rows and a stored cell: %s", team, len(want), readKey, team, read.Body.String())
		}
		for _, r := range want {
			if r.Team != team {
				t.Fatalf("%s: /call row %+v ignores the extras", team, r)
			}
		}

		inner = nil
		hitsBefore := l1Hits(t)
		storeBefore := cache.ResolvedCache().Len()
		rec := serveAsDev(export, "/export?"+coords+"&extras="+extras+"&format=json&path=.status.rows")
		var got []row
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil {
			t.Fatalf("%s: /export: %d %s", team, rec.Code, rec.Body.String())
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: /export rows %+v, want exactly the widget /call's %+v", team, got, want)
		}
		if len(inner) == 0 || inner[0].extras != `{"team":"`+team+`"}` {
			t.Fatalf("%s: the export's re-dispatch carried extras %+v, want the request's", team, inner)
		}
		if inner[0].key != readKey {
			t.Errorf("%s: the export's re-dispatch used cache key %q, the widget read %q", team, inner[0].key, readKey)
		}
		if hits := l1Hits(t) - hitsBefore; hits < 1 {
			t.Errorf("%s: the export's re-dispatch was not served from the widget read's cell (L1 hits +%v)", team, hits)
		}
		if n := cache.ResolvedCache().Len(); n != storeBefore {
			t.Errorf("%s: the export stored %d new cells; it must reuse the read's", team, n-storeBefore)
		}
	}
	// Control: the extras are part of the cell identity, so the two teams'
	// reads are two cells — a reused key above is the extras' own cell.
	if keys["green"] == keys["blue"] {
		t.Errorf("green and blue reads share key %q: extras do not reach the key, so the reuse check proves nothing", keys["green"])
	}
}

// TestExport970_ExtrasOnEveryPage — an export that walks several internal
// pages carries the extras on EVERY re-dispatch, and each page lands on the
// cell of a /call of that same window with those extras; the file is the
// concatenation of those /call pages.
func TestExport970_ExtrasOnEveryPage(t *testing.T) {
	seedExtrasRESTAction(t)
	dispatchers.RegisterL1LookupMetricsForTest()
	setExportPaging(t, 2, 100) // 4 rows per team: pages 1 and 2 full, page 3 short

	chain := Dispatcher(dispatchers.All())(Call())
	type seen struct{ extras, page, perPage, key string }
	var inner []seen
	spy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chain.ServeHTTP(w, r)
		q := r.URL.Query()
		inner = append(inner, seen{q.Get("extras"), q.Get("page"), q.Get("perPage"), w.Header().Get("X-Snowplow-Refresh-Key")})
	})
	export := Export(spy)
	coords := "apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + extrasNS970 + "&name=team-rows"
	type row struct {
		ID   string `json:"id"`
		Team string `json:"team"`
	}
	for _, team := range []string{"green", "blue"} {
		raw := `{"team":"` + team + `"}`
		extras := url.QueryEscape(raw)
		var want []row
		keyByPage := map[string]string{}
		for p := 1; p <= 3; p++ {
			rec := serveAsDev(chain, "/call?"+coords+"&extras="+extras+"&page="+itoa(p)+"&perPage=2")
			var ra struct {
				Status struct {
					Rows []row `json:"rows"`
				} `json:"status"`
			}
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &ra) != nil {
				t.Fatalf("%s: /call page %d: %d %s", team, p, rec.Code, rec.Body.String())
			}
			want = append(want, ra.Status.Rows...)
			keyByPage[itoa(p)] = rec.Header().Get("X-Snowplow-Refresh-Key")
		}
		if len(want) != 4 {
			t.Fatalf("%s: the paged /call gave %d rows, want 4 (the RESTAction must slice)", team, len(want))
		}

		inner = nil
		hitsBefore := l1Hits(t)
		storeBefore := cache.ResolvedCache().Len()
		rec := serveAsDev(export, "/export?"+coords+"&extras="+extras+"&format=json&path=.status.rows")
		var got []row
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &got) != nil {
			t.Fatalf("%s: /export: %d %s", team, rec.Code, rec.Body.String())
		}
		if len(inner) < 2 {
			t.Fatalf("%s: the export walked %d page(s), want at least 2", team, len(inner))
		}
		for _, s := range inner {
			if s.extras != raw {
				t.Errorf("%s: page %s's re-dispatch carried extras %q, want %q", team, s.page, s.extras, raw)
			}
			if s.perPage != "2" || s.key == "" || s.key != keyByPage[s.page] {
				t.Errorf("%s: page %s (perPage %s) used key %q, the /call of that window %q", team, s.page, s.perPage, s.key, keyByPage[s.page])
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: export rows %+v, want the /call pages %+v", team, got, want)
		}
		if hits := l1Hits(t) - hitsBefore; hits < float64(len(inner)) {
			t.Errorf("%s: %v of %d re-dispatched pages were served from the /call cells", team, hits, len(inner))
		}
		if n := cache.ResolvedCache().Len(); n != storeBefore {
			t.Errorf("%s: the export stored %d new cells; every page must reuse its window's", team, n-storeBefore)
		}
	}
}

// TestExport970_InvalidExtrasIs400LikeCall — extras that are not valid JSON
// are refused by /export exactly as by /call (the same status and Status
// body, passed through), for an explicit window and for the full list; the
// export never goes ahead unfiltered.
func TestExport970_InvalidExtrasIs400LikeCall(t *testing.T) {
	seedExtrasRESTAction(t)
	chain := Dispatcher(dispatchers.All())(Call())
	export := Export(chain)
	coords := "apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + extrasNS970 + "&name=team-rows"
	bad := url.QueryEscape(`{"team":`)
	for _, window := range []string{"&page=1&perPage=5", ""} {
		callWindow := window
		if callWindow == "" {
			callWindow = "&page=1&perPage=" + itoa(exportPageSize) // what the full list's first page asks
		}
		call := serveAsDev(chain, "/call?"+coords+"&extras="+bad+callWindow)
		exp := serveAsDev(export, "/export?"+coords+"&extras="+bad+"&format=json"+window)
		if call.Code != http.StatusBadRequest {
			t.Fatalf("window %q: /call answered %d to invalid extras, want 400", window, call.Code)
		}
		if exp.Code != call.Code || exp.Body.String() != call.Body.String() {
			t.Errorf("window %q: /export answered %d %q, want /call's %d %q", window, exp.Code, exp.Body.String(), call.Code, call.Body.String())
		}
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
