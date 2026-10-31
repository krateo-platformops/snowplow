package api

// issue1017_endpoint_serve_test.go — #1017: a stage whose named endpointRef
// points at ANOTHER cluster, with an apiserver-shaped namespaced path, must be
// answered by that cluster. Snowplow's own cells (content cell, informer
// dispatch, cluster-list collapse, internal/in-process resolve) hold THIS
// cluster's objects; serving such a stage from them answers with the wrong
// cluster's data.
//
// Two clusters: "local" is the watcher's fake cluster (widget-team-a in
// team-a), whose informer is synced and whose cells the arms warm; "remote"
// is an httptest apiserver that answers the same path with remote-widget.
// The arms read which cluster's object the stage's result carries, and count
// the remote's requests.
//
// ARMS (all RED on main 85658423 except E, F, R*; E and F document facts)
//   A / A2  a GET stage to the remote, cell warm / cold: content cell and
//           informer dispatch.
//   B   a GET iterator to the remote on a warm cluster-list cell: no collapse;
//       the remote receives the per-item call.
//   C   a resolve:true GET to the remote: no in-process resolve.
//   D   a GET to the remote on a background driver (cache on): answered by
//       the remote, nothing reaches this cluster (incl. the discovery hop).
//   D2  the same with the cache off: the internal-rest-config dispatch does
//       not send it to this cluster with the ServiceAccount.
//   E   escalation probe: hub rows reach only identities whose hub RBAC
//       admits them (wrong data, not escalation, on main).
//   F   057's spokeCrds stage shape (templated ref, path without /apis): sent
//       to the spoke on a customer call and on a background re-resolve.
//   G   a discovery-shaped GET to the remote: not answered by this cluster's
//       discovery client.
//   H   a remote stage on a background driver runs no lazy-register
//       discovery hop on this cluster.
//   R..R5  an own-cluster stage (no endpointRef) keeps every optimisation:
//       informer/cell, internal dispatch, in-process resolve, collapse, and
//       the lazy register.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	ns1017     = "krateo-system"
	remote1017 = "remote-cluster-1017"
	path1017   = "/apis/widgets.krateo.io/v1/namespaces/team-a/widgets"
)

// ownClusterCtx marks ctx the way runStage marks a stage whose endpoint is
// snowplow's own cluster (the requester's clientconfig or the ServiceAccount):
// the tests that call a serve site directly model such a stage.
func ownClusterCtx(ctx context.Context) context.Context {
	return credhttp.WithHeldCredential(ctx)
}

// srv1017 is an apiserver that answers every GET with a WidgetList holding
// one object named after the cluster, and counts its requests.
type srv1017 struct {
	srv     *httptest.Server
	mu      sync.Mutex
	reqs    []string
	secrets map[string]string // Secret name in ns1017 -> its server-url (served when set)
}

func newSrv1017(t *testing.T, cluster string) *srv1017 {
	t.Helper()
	s := &srv1017{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		req := r.Method + " " + r.URL.Path + " auth=" + r.Header.Get("Authorization")
		if acting := r.Header.Get("Impersonate-User"); acting != "" {
			req += " acting=" + acting
		}
		s.reqs = append(s.reqs, req)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if name, ok := strings.CutPrefix(r.URL.Path, "/api/v1/namespaces/"+ns1017+"/secrets/"); ok {
			s.mu.Lock()
			url, known := s.secrets[name]
			s.mu.Unlock()
			if !known {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			b, _ := json.Marshal(corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns1017},
				Data:       map[string][]byte{"server-url": []byte(url), "token": []byte(name + "-tok")},
			})
			_, _ = w.Write(b)
			return
		}
		if strings.Contains(r.URL.Path, "/restactions/") {
			_, _ = w.Write([]byte(`{"apiVersion":"templates.krateo.io/v1","kind":"RESTAction","metadata":{"name":"ra","namespace":"krateo-system"},"status":{"from":"` + cluster + `"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","metadata":{"resourceVersion":"1"},"items":[` +
			`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget","metadata":{"name":"` + cluster + `-widget","namespace":"team-a"}}]}`))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *srv1017) n() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.reqs)
}

// setup1017 builds the local cache-on cluster (the user may list widgets
// cluster-wide) and publishes the user's clientconfig (pointing at a local
// apiserver stand-in) and a named endpoint Secret pointing at the remote.
func setup1017(t *testing.T) (ctx context.Context, local, remote *srv1017) {
	t.Helper()
	newClusterListWatcher(t)
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	local, remote = newSrv1017(t, "local"), newSrv1017(t, "remote")
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	own := kubeutil.MakeDNS1123Compatible(clusterListBroadUser) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		own: {ObjectMeta: metav1.ObjectMeta{Namespace: ns1017, Name: own},
			Data: map[string][]byte{"server-url": []byte(local.srv.URL), "token": []byte("tok")}},
		remote1017: {ObjectMeta: metav1.ObjectMeta{Namespace: ns1017, Name: remote1017},
			Data: map[string][]byte{"server-url": []byte(remote.srv.URL), "token": []byte("remote-tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest(ns1017)
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	return ctxWithUser(clusterListBroadUser), local, remote
}

func remoteStage1017(name, path string) *templates.API {
	return &templates.API{
		Name:            name,
		Path:            path,
		Verb:            ptr.To(http.MethodGet),
		EndpointRef:     &templates.Reference{Name: remote1017, Namespace: ns1017},
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To(name + "Err"),
	}
}

func resolve1017(t *testing.T, ctx context.Context, rc string, stage *templates.API) string {
	t.Helper()
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{Host: rc},
		AuthnNS:             ns1017,
		Items:               []*templates.API{stage},
		RESTActionNamespace: ns1017,
		RESTActionName:      "ra-1017",
	})
	b, _ := json.Marshal(dict[stage.Name])
	return string(b)
}

// wantRemote fails when the stage's result is not the remote cluster's.
func wantRemote1017(t *testing.T, site, got string, remote *srv1017, remote0 int) {
	t.Helper()
	switch {
	case strings.Contains(got, "widget-team-a"):
		t.Fatalf("#1017 %s CONFIRMED: a stage whose endpointRef names another cluster was answered from THIS cluster's cells (%s); the remote saw %d request(s)",
			site, got, remote.n()-remote0)
	case !strings.Contains(got, "remote"):
		t.Fatalf("#1017 %s: the stage's result carries neither cluster's object: %s", site, got)
	}
	if remote.n() == remote0 {
		t.Fatalf("#1017 %s: the remote saw no request", site)
	}
}

func TestIssue1017_A_ARemoteGetIsAnsweredByTheRemote(t *testing.T) {
	ctx, local, remote := setup1017(t)
	// Warm this cluster's content cell for the same path, as any local reader
	// of team-a widgets would.
	local0 := resolve1017(t, ctx, local.srv.URL, &templates.API{Name: "localRead", Path: path1017,
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("localReadErr")})
	if !strings.Contains(local0, "widget-team-a") {
		t.Fatalf("setup: the local read did not see the local cluster's widget: %s", local0)
	}
	r0 := remote.n()
	got := resolve1017(t, ctx, local.srv.URL, remoteStage1017("remoteRead", path1017))
	wantRemote1017(t, "A (content cell / informer)", got, remote, r0)
}

func TestIssue1017_A2_ARemoteGetIsAnsweredByTheRemoteWithNoWarmCell(t *testing.T) {
	ctx, local, remote := setup1017(t)
	r0 := remote.n()
	got := resolve1017(t, ctx, local.srv.URL, remoteStage1017("remoteRead", path1017))
	wantRemote1017(t, "A2 (informer dispatch, cold cell)", got, remote, r0)
}

func TestIssue1017_B_ARemoteIteratorIsNotCollapsedIntoThisCluster(t *testing.T) {
	ctx, local, remote := setup1017(t)
	withClusterListCollapseEnabledForTest(t)
	ResetClusterListAsyncStateForTest()
	t.Cleanup(ResetClusterListAsyncStateForTest)
	read := &templates.API{Name: "s", Path: `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets" }`,
		Verb: ptr.To(http.MethodGet), DependsOn: &templates.Dependency{Iterator: ptr.To(`[{"ns":"team-a"}]`)}}
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	call := buildClusterListCall(read, endpoints.Endpoint{}, f1WidgetsGVR)
	if !populateClusterListCellSync(ctx, clusterListLogger(t), read, f1WidgetsGVR, contentKey, call, cache.ResolvedCache()) {
		t.Fatal("setup: the cluster-list cell could not be populated")
	}
	stage := remoteStage1017("remoteIter", read.Path)
	stage.DependsOn = &templates.Dependency{Iterator: ptr.To(`[{"ns":"team-a"}]`)}
	r0 := remote.n()
	got := resolve1017(t, ctx, local.srv.URL, stage)
	wantRemote1017(t, "B (cluster-list collapse)", got, remote, r0)
	// Not collapsed at all: the remote receives the stage's own per-item
	// call, never a cluster-scope LIST the stage did not ask for.
	remote.mu.Lock()
	reqs := append([]string(nil), remote.reqs[r0:]...)
	remote.mu.Unlock()
	if len(reqs) != 1 || !strings.HasPrefix(reqs[0], "GET "+path1017+" ") {
		t.Fatalf("#1017 B RED: the remote-targeted iterator was collapsed: the remote received %v, want exactly its per-item GET %s", reqs, path1017)
	}
}

func TestIssue1017_C_ARemoteResolveTrueIsNotResolvedInProcess(t *testing.T) {
	ctx, local, remote := setup1017(t)
	stub := &seamStub{ret: []byte(`{"kind":"RESTAction","status":{"from":"local-in-process"}}`)}
	stub.install(t)
	// A single widget by name: the in-process resolve's resource gate admits
	// any "widgets" resource, and this GVR's informer is the local one.
	stage := remoteStage1017("ra", path1017+"/widget-team-a")
	stage.Resolve = ptr.To(true)
	r0 := remote.n()
	got := resolve1017(t, ctx, local.srv.URL, stage)
	if stub.calls.Load() != 0 || strings.Contains(got, "local-in-process") {
		t.Fatalf("#1017 C CONFIRMED: a resolve:true GET whose endpointRef names another cluster was resolved in process against THIS cluster (seam calls %d, result %s); the remote saw %d request(s)",
			stub.calls.Load(), got, remote.n()-r0)
	}
}

func TestIssue1017_D_ARemoteGetOnABackgroundDriverIsAnsweredByTheRemote(t *testing.T) {
	ctx, local, remote := setup1017(t)
	ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{Host: local.srv.URL})
	r0, l0 := remote.n(), local.n()
	got := resolve1017(t, ctx, local.srv.URL, remoteStage1017("bg", path1017))
	if strings.Contains(got, "local-widget") || local.n() > l0 {
		local.mu.Lock()
		reqs := append([]string(nil), local.reqs[l0:]...)
		local.mu.Unlock()
		t.Fatalf("#1017 D CONFIRMED: on a background driver, a GET whose endpointRef names another cluster was answered by THIS cluster (%s); remote requests %d, local requests %v",
			got, remote.n()-r0, reqs)
	}
	wantRemote1017(t, "D (internal dispatch)", got, remote, r0)
}

// TestIssue1017_D2_TheInternalDispatchDoesNotAnswerARemoteStage: cache off (so
// no cell can answer), on a background driver's context whose internal rest
// config is THIS cluster's ServiceAccount. A GET stage whose endpointRef names
// another cluster must be sent to that cluster with that cluster's credential;
// the internal dispatch must not answer it from this cluster with the SA.
func TestIssue1017_D2_TheInternalDispatchDoesNotAnswerARemoteStage(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	local, remote := newSrv1017(t, "local"), newSrv1017(t, "remote")
	local.secrets = map[string]string{remote1017: remote.srv.URL}
	ctx := cache.WithInternalRESTConfig(ctxWithUser(clusterListBroadUser),
		&rest.Config{Host: local.srv.URL, BearerToken: "local-sa-token"})
	got := resolve1017(t, ctx, local.srv.URL, remoteStage1017("bg", path1017))
	local.mu.Lock()
	lreqs := append([]string(nil), local.reqs...)
	local.mu.Unlock()
	remote.mu.Lock()
	rreqs := append([]string(nil), remote.reqs...)
	remote.mu.Unlock()
	t.Logf("local apiserver saw %v; remote saw %v; result %s", lreqs, rreqs, got)
	for _, r := range lreqs {
		if strings.Contains(r, "/widgets") {
			t.Fatalf("#1017 D2 CONFIRMED: the remote stage's GET was sent to THIS cluster's apiserver: %q (all local: %v; remote: %v)", r, lreqs, rreqs)
		}
	}
	if !strings.Contains(got, "remote-widget") {
		t.Fatalf("#1017 D2: the stage's result is not the remote cluster's: %s (local %v, remote %v)", got, lreqs, rreqs)
	}
}

// TestIssue1017_E_EscalationProbe_HubRowsReachOnlyIdentitiesThatMayReadThem:
// on a background re-resolve (the refresher's shape: background mark + the
// SA's internal rest config) of a stage whose endpointRef names another
// cluster, the hub's rows are filtered by the CONTEXT identity's hub RBAC.
// A cohort identity with no hub grant for the resource receives no hub row
// (no escalation); one with the grant receives the hub's rows (wrong data).
// This arm documents the escalation question on main; it does not fail on
// the wrong-data half, which arms A-D cover.
func TestIssue1017_E_EscalationProbe_HubRowsReachOnlyIdentitiesThatMayReadThem(t *testing.T) {
	_, local, _ := setup1017(t)
	bg := func(user string) context.Context {
		ctx := cache.WithBackgroundResolve(ctxWithUser(user))
		return cache.WithInternalRESTConfig(ctx, &rest.Config{Host: local.srv.URL, BearerToken: "local-sa-token"})
	}
	hub := func(s string) bool {
		return strings.Contains(s, "widget-team-a") || strings.Contains(s, "local-widget")
	}

	none := resolve1017(t, bg("nobody-1017"), local.srv.URL, remoteStage1017("bgNone", path1017))
	t.Logf("identity WITHOUT a hub grant: %s", none)
	if hub(none) {
		t.Fatalf("#1017 E ESCALATION: an identity with no hub grant for widgets received hub rows on a background re-resolve of a remote stage: %s", none)
	}
	granted := resolve1017(t, bg(clusterListBroadUser), local.srv.URL, remoteStage1017("bgGranted", path1017))
	t.Logf("identity WITH a hub grant: %s (hub rows: %v)", granted, hub(granted))
}

// TestIssue1017_F_The057SpokeCrdsShapeIsSentToTheSpoke pins the one 057
// stage with a named endpointRef on an apiserver-like path:
// krateo-clusters/cluster-compositions, stage spokeCrds — a GET of
// /apiextensions.k8s.io/v1/customresourcedefinitions (no /apis prefix)
// through a TEMPLATED ref `<name>-endpoint`. That path is not an apiserver
// path to snowplow's parsers (no /api/ or /apis/ prefix), so no cell,
// informer, collapse, internal/discovery dispatch, in-process resolve or
// lazy-register hop takes it: it is sent to the spoke, on a customer call and
// on a background re-resolve alike.
func TestIssue1017_F_The057SpokeCrdsShapeIsSentToTheSpoke(t *testing.T) {
	ctx, local, remote := setup1017(t)
	const spokePath = "/apiextensions.k8s.io/v1/customresourcedefinitions"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		"spoke1-endpoint": {ObjectMeta: metav1.ObjectMeta{Namespace: ns1017, Name: "spoke1-endpoint"},
			Data: map[string][]byte{"server-url": []byte(remote.srv.URL), "token": []byte("spoke-tok")}},
	}})
	stage := &templates.API{
		Name: "spokeCrds",
		Path: spokePath,
		Verb: ptr.To(http.MethodGet),
		EndpointRef: &templates.Reference{
			Name:      `${ ((.name // "") | gsub("[^a-zA-Z0-9.-]"; "")) + "-endpoint" }`,
			Namespace: ns1017,
		},
		ContinueOnError: ptr.To(true),
		ErrorKey:        ptr.To("spokeCrdsErr"),
	}
	run := func(c context.Context) string {
		dict := Resolve(c, ResolveOptions{
			RC: &rest.Config{Host: local.srv.URL}, AuthnNS: ns1017, Items: []*templates.API{stage},
			RESTActionNamespace: "krateo-clusters", RESTActionName: "cluster-compositions",
			Extras: map[string]any{"name": "spoke1"},
		})
		b, _ := json.Marshal(dict["spokeCrds"])
		return string(b)
	}
	for _, c := range []struct {
		name string
		ctx  context.Context
	}{
		{"customer call", ctx},
		{"background re-resolve", cache.WithInternalRESTConfig(cache.WithBackgroundResolve(ctx),
			&rest.Config{Host: local.srv.URL, BearerToken: "local-sa-token"})},
	} {
		r0, l0 := remote.n(), local.n()
		got := run(c.ctx)
		local.mu.Lock()
		lreqs := append([]string(nil), local.reqs[l0:]...)
		local.mu.Unlock()
		if !strings.Contains(got, "remote") || remote.n() == r0 {
			t.Errorf("%s: the spokeCrds stage was not answered by the spoke: %s (spoke requests %d)", c.name, got, remote.n()-r0)
		}
		for _, r := range lreqs {
			if strings.Contains(r, "customresourcedefinitions") || strings.Contains(r, "apiextensions") {
				t.Errorf("%s: the spokeCrds stage reached the hub's apiserver: %q", c.name, r)
			}
		}
		t.Logf("%s: spoke requests %d, hub requests %v", c.name, remote.n()-r0, lreqs)
	}
}

// --- per-site arms of the fix (targetsOwnCluster) ---------------------------

// TestIssue1017_G_ARemoteDiscoveryGetIsAnsweredByTheRemote: a discovery-shaped
// path through a named endpointRef is answered by that endpoint, never by
// this cluster's discovery client (the ServiceAccount).
func TestIssue1017_G_ARemoteDiscoveryGetIsAnsweredByTheRemote(t *testing.T) {
	ctx, local, remote := setup1017(t)
	prev := saRESTConfigForDiscoveryFn
	saRESTConfigForDiscoveryFn = func() (*rest.Config, error) {
		return &rest.Config{Host: local.srv.URL, BearerToken: "local-sa-token"}, nil
	}
	t.Cleanup(func() { saRESTConfigForDiscoveryFn = prev; resetDiscoveryClientCacheForTest() })
	resetDiscoveryClientCacheForTest()
	r0, l0 := remote.n(), local.n()
	resolve1017(t, ctx, local.srv.URL, remoteStage1017("disc", "/apis/widgets.krateo.io/v1"))
	local.mu.Lock()
	lreqs := append([]string(nil), local.reqs[l0:]...)
	local.mu.Unlock()
	for _, r := range lreqs {
		if strings.Contains(r, "/apis/widgets.krateo.io/v1") {
			t.Fatalf("#1017 G RED: a discovery GET through a named endpointRef was answered by THIS cluster's discovery client: %q", r)
		}
	}
	if remote.n() == r0 {
		t.Fatal("#1017 G: the remote saw no request")
	}
}

// TestIssue1017_H_ARemoteStageRunsNoLazyRegisterDiscoveryHop: on a background
// driver (cache on, the ServiceAccount's internal rest config), a stage whose
// endpoint names another cluster triggers no group discovery on this cluster.
func TestIssue1017_H_ARemoteStageRunsNoLazyRegisterDiscoveryHop(t *testing.T) {
	ctx, local, _ := setup1017(t)
	var hops []string
	var mu sync.Mutex
	prev := discoverGroupResourcesFn
	discoverGroupResourcesFn = func(_ context.Context, rc *rest.Config, group string) (int, error) {
		mu.Lock()
		hops = append(hops, group+" via "+rc.Host)
		mu.Unlock()
		return 0, nil
	}
	t.Cleanup(func() { discoverGroupResourcesFn = prev })
	bg := cache.WithInternalRESTConfig(cache.WithBackgroundResolve(ctx),
		&rest.Config{Host: local.srv.URL, BearerToken: "local-sa-token"})
	resolve1017(t, bg, local.srv.URL, remoteStage1017("bgRemote", path1017))
	mu.Lock()
	defer mu.Unlock()
	if len(hops) != 0 {
		t.Fatalf("#1017 H RED: a remote-targeted stage ran the lazy-register discovery hop on this cluster with the ServiceAccount: %v", hops)
	}
}

// TestIssue1017_R_AnOwnClusterGetIsStillServedFromTheCells: a stage with no
// endpointRef (the requester's own clientconfig: this cluster) is still
// answered by this cluster's informer, sending nothing to any apiserver.
func TestIssue1017_R_AnOwnClusterGetIsStillServedFromTheCells(t *testing.T) {
	ctx, local, remote := setup1017(t)
	r0, l0 := remote.n(), local.n()
	got := resolve1017(t, ctx, local.srv.URL, &templates.API{Name: "own", Path: path1017,
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("ownErr")})
	if !strings.Contains(got, "widget-team-a") {
		t.Fatalf("#1017 R RED: an own-cluster GET was not answered from this cluster's informer: %s", got)
	}
	if remote.n() != r0 || local.n() != l0 {
		t.Fatalf("#1017 R RED: an own-cluster GET reached an apiserver (remote %d, local %d); the informer must answer it",
			remote.n()-r0, local.n()-l0)
	}
}

// TestIssue1017_R2_AnOwnClusterGetOnABackgroundDriverStillUsesTheInternalDispatch:
// cache off, background driver: a stage with no endpointRef is still answered
// by the internal-rest-config dispatch (the ServiceAccount, this cluster).
func TestIssue1017_R2_AnOwnClusterGetOnABackgroundDriverStillUsesTheInternalDispatch(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	local := newSrv1017(t, "local")
	own := kubeutil.MakeDNS1123Compatible(clusterListBroadUser) + "-clientconfig"
	local.secrets = map[string]string{own: local.srv.URL}
	ctx := cache.WithInternalRESTConfig(ctxWithUser(clusterListBroadUser),
		&rest.Config{Host: local.srv.URL, BearerToken: "local-sa-token"})
	resolve1017(t, ctx, local.srv.URL, &templates.API{Name: "own", Path: path1017,
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("ownErr")})
	local.mu.Lock()
	defer local.mu.Unlock()
	for _, r := range local.reqs {
		if strings.HasPrefix(r, "GET "+path1017+" auth=Bearer local-sa-token") {
			return
		}
	}
	t.Fatalf("#1017 R2 RED: an own-cluster GET on a background driver was not answered by the internal dispatch (requests %v)", local.reqs)
}

// TestIssue1017_R3_AnOwnClusterResolveTrueIsStillResolvedInProcess: a
// resolve:true GET with no endpointRef is still substituted by the nested
// resolve.
func TestIssue1017_R3_AnOwnClusterResolveTrueIsStillResolvedInProcess(t *testing.T) {
	ctx, local, _ := setup1017(t)
	stub := &seamStub{ret: []byte(`{"kind":"Widget","status":{"from":"in-process"}}`)}
	stub.install(t)
	got := resolve1017(t, ctx, local.srv.URL, &templates.API{Name: "own", Path: path1017 + "/widget-team-a",
		Resolve: ptr.To(true), ContinueOnError: ptr.To(true), ErrorKey: ptr.To("ownErr")})
	if stub.calls.Load() != 1 || !strings.Contains(got, "in-process") {
		t.Fatalf("#1017 R3 RED: an own-cluster resolve:true GET was not resolved in process (seam calls %d, result %s)", stub.calls.Load(), got)
	}
}

// TestIssue1017_R4_AnOwnClusterIteratorStillCollapses: a GET iterator with no
// endpointRef still takes the cluster-list collapse on a warm cell.
func TestIssue1017_R4_AnOwnClusterIteratorStillCollapses(t *testing.T) {
	ctx, local, _ := setup1017(t)
	withClusterListCollapseEnabledForTest(t)
	ResetClusterListAsyncStateForTest()
	t.Cleanup(ResetClusterListAsyncStateForTest)
	read := &templates.API{Name: "own", Path: `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets" }`,
		Verb: ptr.To(http.MethodGet), DependsOn: &templates.Dependency{Iterator: ptr.To(`[{"ns":"team-a"}]`)},
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("ownErr")}
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	call := buildClusterListCall(read, endpoints.Endpoint{}, f1WidgetsGVR)
	if !populateClusterListCellSync(ctx, clusterListLogger(t), read, f1WidgetsGVR, contentKey, call, cache.ResolvedCache()) {
		t.Fatal("setup: the cluster-list cell could not be populated")
	}
	warm0, _ := cache.ClusterListCellCounters()
	resolve1017(t, ctx, local.srv.URL, read)
	if warm, _ := cache.ClusterListCellCounters(); warm != warm0+1 {
		t.Fatalf("#1017 R4 RED: an own-cluster GET iterator did not collapse on a warm cell (cell-warm %d → %d)", warm0, warm)
	}
}

// TestIssue1017_R5_AnOwnClusterStageStillLazyRegisters: on a background
// driver, a stage with no endpointRef still runs the lazy-register discovery
// hop for its group on this cluster.
func TestIssue1017_R5_AnOwnClusterStageStillLazyRegisters(t *testing.T) {
	ctx, local, _ := setup1017(t)
	var hops []string
	var mu sync.Mutex
	prev := discoverGroupResourcesFn
	discoverGroupResourcesFn = func(_ context.Context, _ *rest.Config, group string) (int, error) {
		mu.Lock()
		hops = append(hops, group)
		mu.Unlock()
		return 0, nil
	}
	t.Cleanup(func() { discoverGroupResourcesFn = prev })
	bg := cache.WithInternalRESTConfig(cache.WithBackgroundResolve(ctx),
		&rest.Config{Host: local.srv.URL, BearerToken: "local-sa-token"})
	resolve1017(t, bg, local.srv.URL, &templates.API{Name: "own", Path: path1017,
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("ownErr")})
	mu.Lock()
	defer mu.Unlock()
	if len(hops) != 1 || hops[0] != "widgets.krateo.io" {
		t.Fatalf("#1017 R5 RED: an own-cluster stage did not run the lazy-register discovery hop for its group (hops %v)", hops)
	}
}

// --- #1019 review F1/F2: the per-stage marks are set, never inherited -------

// TestR1019_N1_NestedRemoteStageInheritsNoOwnClusterMark (reviewer's arm N1,
// adopted): an own-cluster resolve:true stage hands its ctx to the nested
// resolve; a nested stage whose endpointRef names another cluster must still
// be answered by that cluster. RED when runStage only ADDS the own-cluster
// mark (the inherited true survives the nested named-ref stage).
func TestR1019_N1_NestedRemoteStageInheritsNoOwnClusterMark(t *testing.T) {
	ctx, local, remote := setup1017(t)
	var inner string
	prev := nestedCallResolver
	nestedCallResolver = func(nctx context.Context, _ templates.ObjectReference, _, _ int, _ map[string]any) ([]byte, error) {
		// what ResolveNestedCall does: resolve the inner RA under a ctx derived from nctx
		inner = resolve1017(t, nctx, local.srv.URL, remoteStage1017("innerRemote", path1017))
		b, _ := json.Marshal(map[string]any{"kind": "RESTAction", "status": map[string]any{"inner": json.RawMessage(inner)}})
		return b, nil
	}
	t.Cleanup(func() { nestedCallResolver = prev })
	r0 := remote.n()
	_ = resolve1017(t, ctx, local.srv.URL, &templates.API{Name: "outer", Path: path1017 + "/widget-team-a",
		Resolve: ptr.To(true), ContinueOnError: ptr.To(true), ErrorKey: ptr.To("outerErr")})
	if inner == "" {
		t.Fatal("setup: the outer own-cluster stage did not resolve in process")
	}
	wantRemote1017(t, "N1 (nested resolve inherits the outer own-cluster mark)", inner, remote, r0)
}

// TestR1019_N1c_TopLevelRemoteStageControl (reviewer's control N1c, adopted):
// the same inner stage at the top level is answered by the remote.
func TestR1019_N1c_TopLevelRemoteStageControl(t *testing.T) {
	ctx, local, remote := setup1017(t)
	r0 := remote.n()
	got := resolve1017(t, ctx, local.srv.URL, remoteStage1017("innerRemote", path1017))
	wantRemote1017(t, "N1c", got, remote, r0)
}

// TestR1019_N2_NestedRemoteStageInheritsNoServiceAccountDialMark: an outer
// stage that dials snowplow's ServiceAccount (a background driver's
// no-endpointRef stage) marks its dispatch ctx as an SA dial, and a
// resolve:true nested resolve runs its stages on a ctx derived from it. The
// arm hands a stage exactly that inherited mark: a stage with its own named
// endpointRef must still dial its endpoint with ITS credential, so the remote
// receives one request carrying the remote endpoint's own token. RED when
// runStage only ADDS the SA-dial mark (the inherited true survives: the
// stage's credential is replaced by the ServiceAccount's file token path and
// its own Authorization dropped).
func TestR1019_N2_NestedRemoteStageInheritsNoServiceAccountDialMark(t *testing.T) {
	ctx, local, remote := setup1017(t)
	inherited := cache.WithServiceAccountDial(ctx) // what an outer SA stage's gctx carries
	// The remote API expects a header the stage declares; on an SA dial
	// snowplow drops a stage's Impersonate-* and Authorization headers
	// (#615/#725), so it survives only if the stage is NOT read as an SA dial.
	stage := remoteStage1017("innerRemote", path1017)
	stage.Headers = []string{"Impersonate-User: remote-operator"}
	r0 := remote.n()
	got := resolve1017(t, inherited, local.srv.URL, stage)
	remote.mu.Lock()
	reqs := append([]string(nil), remote.reqs[r0:]...)
	remote.mu.Unlock()
	if len(reqs) != 1 || !strings.HasSuffix(reqs[0], "auth=Bearer remote-tok acting=remote-operator") {
		t.Fatalf("#1019 N2 RED: a stage on a ctx carrying an inherited SA-dial mark was dialled as the SA (its own declared headers dropped): remote requests %v (result %s)", reqs, got)
	}
}
