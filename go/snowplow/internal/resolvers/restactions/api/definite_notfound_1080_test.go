package api

// definite_notfound_1080_test.go — #1080 resolver arms: a named object's
// NotFound under continueOnError is a per-identity definite answer, counted on
// the #681 sink, when the watcher confirms the object absent. Driven through
// the REAL api.Resolve against a REAL ResourceWatcher (dynamic fake) and the
// REAL in-process RBAC snapshot.
//
//   N1   the requester's OWN dial (branch E, the /call shape) 404s a named
//        object the watcher does not hold: stage AND external on the sink,
//        GenuineExternal 0, DenialOnly true, the canonical NotFound body.
//   N2   branch C (SA dial, background identity) after the pre-dial
//        authorization allowed the read: stage on the sink, not external.
//   N12  N1 and N2 for the same coordinate record BYTE-IDENTICAL bodies.
//   NS   branch C after a pre-dial SCOPE FALLBACK (a namespaced GVR addressed
//        without a namespace): the SA's NotFound says nothing about the
//        identity — not definite.
//   NC   no continueOnError: not definite (branch E and branch C).
//   NU   unit: every N1 condition is load-bearing — endpointRef, SA dial, a
//        non-GET, a LIST, a subresource, a GVR the watcher does not serve, an
//        object the watcher holds, a 404 without reason NotFound, a 5xx.

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const ghost1080 = "/api/v1/namespaces/team-a/configmaps/ghost"

// watcher1080 is newWatcher677's watcher (rbacSeed677: u677 may get any
// configmap in team-a) with the configmaps informer registered and synced, and
// ONE configmap the watcher holds (team-a/present).
func watcher1080(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	kinds := dispatchTestListKinds()
	kinds[cmGVR677] = "ConfigMapList"
	kinds[ktGVR677] = "KubernetesTargetList"
	present := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": "team-a", "name": "present"},
		"data":     map[string]any{"k": "v"},
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(dispatchTestScheme(), kinds, append(rbacSeed677(), runtime.Object(present))...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if _, ch := rw.EnsureResourceType(cmGVR677); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("configmaps informer did not sync")
		}
	}
	if !rw.IsServable(cmGVR677) {
		t.Fatal("PRE: configmaps must be servable")
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
}

func canonicalGhost1080() any {
	return canonicalNotFoundBody(apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "ghost").Error())
}

// resolveBranchC1080 resolves stages under u677 with the SA credential on the
// ctx (the refresher / seed shape: branch C).
func resolveBranchC1080(t *testing.T, stages ...*templates.API) (map[string]any, sinks681, *fakeAPI677) {
	t.Helper()
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	f.missing["ghost"] = true
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
	ctx, s := withSinks681(ctx)
	dict := Resolve(context.WithoutCancel(ctx), ResolveOptions{
		RC: rc, Items: stages, RESTActionNamespace: "krateo-system", RESTActionName: "ra1080",
	})
	return dict, s, f
}

func TestIssue1080_N1_OwnDial404_IsDefinite_CanonicalBody(t *testing.T) {
	srv, dials := userAPI681(t)
	dict, s := resolveAsUser1080(t, srv.URL, stage681("ghost", ghost1080))
	if dials.Load() != 1 {
		t.Fatalf("PRE: the 404 must come from the requester's own dial (dials=%d)", dials.Load())
	}
	if s.stage.Count() != 1 || s.forb.Stage() != 1 || s.forb.External() != 1 || s.ext.Count() != 1 {
		t.Fatalf("N1 RED: stage=%d forb.stage=%d forb.external=%d ext=%d, want 1/1/1/1",
			s.stage.Count(), s.forb.Stage(), s.forb.External(), s.ext.Count())
	}
	if g := cache.GenuineExternal(s.ext, s.forb); g != 0 {
		t.Fatalf("N1 RED: GenuineExternal = %d, want 0 (an own-credential apiserver 404 is not an external touch)", g)
	}
	if !cache.DenialOnly(s.stage, s.forb, s.ext, "restactions") {
		t.Fatal("N1 RED: a definite-NotFound-only resolve does not read as storable")
	}
	if cache.DenialOnly(s.stage, s.forb, s.ext, cache.CacheEntryClassWidgetContent) {
		t.Fatal("N1: an identity-free class admitted a per-identity definite answer")
	}
	got := jsonOf(t, accumulatedFirst(dict["ghostErr"]))
	if want := jsonOf(t, canonicalGhost1080()); got != want {
		t.Fatalf("N1: the 404 body is not the canonical one:\n got %s\nwant %s", got, want)
	}
}

// resolveAsUser1080 is resolveAsUser681 (the /call shape: the requester's own
// clientconfig, no SA credential) with watcher1080 as the process watcher —
// resolveAsUser681 clears it for the #681 arms.
func resolveAsUser1080(t *testing.T, url string, stages ...*templates.API) (map[string]any, sinks681) {
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	watcher1080(t)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	secret := kubeutil.MakeDNS1123Compatible(snt681User) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		secret: {ObjectMeta: authnMeta("krateo-system", secret, snt681User),
			Data: map[string][]byte{"server-url": []byte(url), "token": []byte("tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest("krateo-system")
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	ctx, s := withSinks681(ctxWithUser(snt681User, snt681Group))
	dict := Resolve(ctx, ResolveOptions{RC: &rest.Config{Host: url}, AuthnNS: "krateo-system", Items: stages,
		RESTActionNamespace: "krateo-system", RESTActionName: "ra1080"})
	return dict, s
}

func TestIssue1080_N2_BranchC_AuthorizedNotFound_IsDefinite_SameBodyAsN1(t *testing.T) {
	watcher1080(t)
	dict, s, f := resolveBranchC1080(t, stage681("ghost", ghost1080))
	if f.dials.Load() != 1 {
		t.Fatalf("PRE: branch C must dial the SA client once (dials=%d)", f.dials.Load())
	}
	if s.stage.Count() != 1 || s.forb.Stage() != 1 || s.forb.External() != 0 {
		t.Fatalf("N2 RED: stage=%d forb.stage=%d forb.external=%d, want 1/1/0", s.stage.Count(), s.forb.Stage(), s.forb.External())
	}
	if !cache.DenialOnly(s.stage, s.forb, s.ext, "restactions") {
		t.Fatal("N2 RED: an authorized branch C NotFound does not read as storable")
	}
	got := jsonOf(t, accumulatedFirst(dict["ghostErr"]))
	if want := jsonOf(t, canonicalGhost1080()); got != want {
		t.Fatalf("N12 RED: the branch C body differs from the canonical (= N1) body:\n got %s\nwant %s", got, want)
	}
}

func TestIssue1080_NS_BranchC_ScopeFallbackNotFound_NotDefinite(t *testing.T) {
	watcher1080(t)
	// A namespaced GVR addressed without a namespace: the pre-dial step cannot
	// decide (preDialAuthzNamespace falls back), so nothing authorized u677.
	_, s, f := resolveBranchC1080(t, stage681("ghost", "/api/v1/configmaps/ghost"))
	if f.dials.Load() != 1 {
		t.Fatalf("PRE: the SA client must dial once (dials=%d)", f.dials.Load())
	}
	if !cache.WatchedAbsent(cmGVR677, "", "ghost") {
		t.Fatal("PRE: the watcher must report the coordinate absent, so only the authorization can decline")
	}
	if s.stage.Count() != 1 || s.forb.Stage() != 0 {
		t.Fatalf("NS RED: stage=%d forb.stage=%d, want 1/0 — an unauthorized SA NotFound was taken as this identity's answer",
			s.stage.Count(), s.forb.Stage())
	}
}

func TestIssue1080_NC_NoContinueOnError_NotDefinite(t *testing.T) {
	strict := &templates.API{Name: "ghost", Path: ghost1080, Verb: ptr.To(http.MethodGet), ErrorKey: ptr.To("ghostErr")}
	t.Run("branch_c", func(t *testing.T) {
		watcher1080(t)
		_, s, _ := resolveBranchC1080(t, strict)
		if s.stage.Count() != 1 || s.forb.Stage() != 0 {
			t.Fatalf("NC RED: stage=%d forb.stage=%d, want 1/0", s.stage.Count(), s.forb.Stage())
		}
	})
	t.Run("branch_e", func(t *testing.T) {
		srv, _ := userAPI681(t)
		_, s := resolveAsUser1080(t, srv.URL, strict)
		if s.stage.Count() != 1 || s.forb.Stage() != 0 {
			t.Fatalf("NC RED: stage=%d forb.stage=%d, want 1/0", s.stage.Count(), s.forb.Stage())
		}
	})
}

func TestIssue1080_NU_ClassifyOwnDialNotFound_Conditions(t *testing.T) {
	watcher1080(t)
	notFound := &response.Status{Kind: "Status", Status: response.StatusFailure, Code: 404, Reason: response.StatusReasonNotFound}
	call := func(path, verb string, coe bool) httpcall.RequestOptions {
		return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(verb)}, ContinueOnError: coe}
	}
	get := call(ghost1080, http.MethodGet, true)
	plain := &templates.API{Name: "s"}
	ctx := context.Background()

	msg, ok := classifyOwnDialNotFound(ctx, plain, get, notFound)
	if !ok || !strings.Contains(msg, `configmaps "ghost" not found`) {
		t.Fatalf("precondition: the own-dial 404 of a watched-absent named object must classify (ok=%v msg=%q)", ok, msg)
	}
	cases := []struct {
		name string
		ctx  context.Context
		api  *templates.API
		call httpcall.RequestOptions
		res  *response.Status
	}{
		{"no_continue_on_error", ctx, plain, call(ghost1080, http.MethodGet, false), notFound},
		{"endpointRef", ctx, &templates.API{Name: "s", EndpointRef: &templates.Reference{Name: "github"}}, get, notFound},
		{"sa_dial", cache.WithServiceAccountDial(ctx), plain, get, notFound},
		{"post", ctx, plain, call(ghost1080, http.MethodPost, true), notFound},
		{"list", ctx, plain, call("/api/v1/namespaces/team-a/configmaps", http.MethodGet, true), notFound},
		{"subresource", ctx, plain, call(ghost1080+"/status", http.MethodGet, true), notFound},
		{"unwatched_gvr", ctx, plain, call("/apis/core.krateo.io/v1alpha1/kubernetestargets/ghost", http.MethodGet, true), notFound},
		{"watcher_holds_it", ctx, plain, call("/api/v1/namespaces/team-a/configmaps/present", http.MethodGet, true), notFound},
		{"unparseable", ctx, plain, call("/charts/index.yaml", http.MethodGet, true), notFound},
		{"no_reason", ctx, plain, get, &response.Status{Code: 404}},
		{"5xx", ctx, plain, get, &response.Status{Code: 500, Reason: response.StatusReasonInternalError}},
		{"403", ctx, plain, get, &response.Status{Code: 403, Reason: response.StatusReasonForbidden}},
	}
	for _, c := range cases {
		if _, ok := classifyOwnDialNotFound(c.ctx, c.api, c.call, c.res); ok {
			t.Errorf("NU RED (%s): classified as a definite NotFound", c.name)
		}
	}
	// WatchedAbsent itself: a collection is never "absent", a held object is
	// not, a missing one on a served GVR is.
	if cache.WatchedAbsent(cmGVR677, "team-a", "") {
		t.Error("NU RED (WatchedAbsent collection): a LIST coordinate read as an absent object")
	}
	if cache.WatchedAbsent(cmGVR677, "team-a", "present") || !cache.WatchedAbsent(cmGVR677, "team-a", "ghost") {
		t.Error("NU RED (WatchedAbsent): present/ghost misclassified")
	}
	// No watcher at all: nothing is watched-absent.
	cache.SetGlobal(nil)
	if _, ok := classifyOwnDialNotFound(ctx, plain, get, notFound); ok {
		t.Error("NU RED (no_watcher): classified without a watcher to see the object's creation")
	}
}
