package api

// per_identity_forbidden_681_test.go — #681 resolver arms, driven through the
// REAL api.Resolve.
//
//   A1  F1: a branch C re-gate deny is a per-identity forbidden (stage, not
//       external), recorded as the canonical identity-free body.
//   A2  F2: the apiserver's typed 403 on the requester's OWN dial is a
//       per-identity forbidden (stage AND external), and the identity text the
//       apiserver put in its message (username + groups) NEVER reaches the
//       result: the errorKey value is the canonical body (S1 / H5b).
//   A3  F1 and F2 for the same coordinate produce BYTE-IDENTICAL error bodies,
//       so a seeded (F1) cell and a /call (F2) cell agree.
//   A4  mixed: a denial plus another error (404) is not all-forbidden.
//   H3  unit: an endpointRef stage, an SA dial, a non-GET, an unparseable path,
//       a 403 without the Forbidden reason (a rate limit), a 401 — none is F2.
//   H7  the branch-E SA refusal is a stage error but NOT a per-identity
//       forbidden.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// The identity text an apiserver 403 carries. None of it may reach a result.
const (
	snt681User  = "u-alice-SNT681"
	snt681Group = "g-team-SNT681"
)

type sinks681 struct {
	stage *cache.StageErrorSink
	ext   *cache.ExternalTouchedSink
	forb  *cache.PerIdentityForbiddenSink
}

func withSinks681(ctx context.Context) (context.Context, sinks681) {
	var s sinks681
	ctx, s.stage = cache.WithStageErrorSink(ctx)
	ctx, s.ext = cache.WithExternalTouchedSink(ctx)
	ctx, s.forb = cache.WithPerIdentityForbiddenSink(ctx)
	return ctx, s
}

// userAPI681 is the requester's OWN apiserver (/call shape): it 403s
// team-b/catalog with an apiserver-style message naming the requester, 404s
// "ghost", 403s "ratelimited" WITHOUT a Forbidden reason, and serves the rest.
func userAPI681(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var dials atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		w.Header().Set("Content-Type", "application/json")
		seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		name := seg[len(seg)-1]
		switch {
		case strings.Contains(r.URL.Path, "/team-b/"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,`+
				`"message":"configmaps \"%s\" is forbidden: User \"%s\" cannot get resource \"configmaps\" in API group \"\" in the namespace \"team-b\": groups [\"%s\" \"system:authenticated\"]"}`,
				name, snt681User, snt681Group)
		case name == "ratelimited":
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprintf(w, `{"message":"API rate limit exceeded for %s"}`, snt681User)
		case name == "ghost":
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		default:
			_, _ = fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"team-a","name":%q}}`, name)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &dials
}

func stage681(id, path string) *templates.API {
	return &templates.API{Name: id, Path: path, Verb: ptr.To(http.MethodGet), ContinueOnError: ptr.To(true), ErrorKey: ptr.To(id + "Err")}
}

// resolveAsUser681 drives api.Resolve the way a cache-off /call does: the
// requester's own clientconfig is the UserConfig, no SA credential on the ctx.
func resolveAsUser681(t *testing.T, url string, stages ...*templates.API) (map[string]any, sinks681) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	// The requester's own clientconfig (the /call shape): <user>-clientconfig.
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	secret := kubeutil.MakeDNS1123Compatible(snt681User) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		secret: {ObjectMeta: metav1.ObjectMeta{Namespace: "krateo-system", Name: secret},
			Data: map[string][]byte{"server-url": []byte(url), "token": []byte("tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest("krateo-system")
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })
	ctx, s := withSinks681(ctxWithUser(snt681User, snt681Group))
	dict := Resolve(ctx, ResolveOptions{RC: &rest.Config{Host: url}, AuthnNS: "krateo-system", Items: stages,
		RESTActionNamespace: "krateo-system", RESTActionName: "ra681"})
	return dict, s
}

func canonicalCatalog681() any {
	return canonicalForbiddenBody(canonicalForbiddenMessage(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "team-b", "catalog", "get"))
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestIssue681_A2_OwnDial403_IsForbidden_AndCarriesNoIdentity(t *testing.T) {
	srv, _ := userAPI681(t)
	dict, s := resolveAsUser681(t, srv.URL,
		stage681("catalog", "/api/v1/namespaces/team-b/configmaps/catalog"),
		stage681("ok", "/api/v1/namespaces/team-a/configmaps/other"))
	if s.stage.Count() != 1 || s.forb.Stage() != 1 || s.forb.External() != 1 || s.ext.Count() != 2 {
		t.Fatalf("stage=%d forb.stage=%d forb.external=%d ext=%d, want 1/1/1/2", s.stage.Count(), s.forb.Stage(), s.forb.External(), s.ext.Count())
	}
	if g := cache.GenuineExternal(s.ext, s.forb); g != 1 {
		t.Fatalf("GenuineExternal = %d, want 1 (the allowed step's own dial; the 403 is not counted)", g)
	}
	whole := jsonOf(t, dict)
	for _, snt := range []string{snt681User, snt681Group, "SNT681"} {
		if strings.Contains(whole, snt) {
			t.Fatalf("S1 RED: identity text %q from the apiserver's 403 reached the result: %s", snt, whole)
		}
	}
	got := jsonOf(t, accumulatedFirst(dict["catalogErr"]))
	if want := jsonOf(t, canonicalCatalog681()); got != want {
		t.Fatalf("the F2 error body is not the canonical one:\n got %s\nwant %s", got, want)
	}
}

// accumulatedFirst unwraps the errorKey accumulating slice (#313 W-A) to its
// single entry.
func accumulatedFirst(v any) any {
	if sl, ok := v.([]any); ok && len(sl) == 1 {
		return sl[0]
	}
	return v
}

func TestIssue681_A4_MixedDenialAndOtherError_NotAllForbidden(t *testing.T) {
	srv, _ := userAPI681(t)
	_, s := resolveAsUser681(t, srv.URL,
		stage681("catalog", "/api/v1/namespaces/team-b/configmaps/catalog"),
		stage681("ghost", "/api/v1/namespaces/team-a/configmaps/ghost"))
	if s.stage.Count() != 2 || s.forb.Stage() != 1 {
		t.Fatalf("stage=%d forb.stage=%d, want 2/1", s.stage.Count(), s.forb.Stage())
	}
	if cache.DenialOnly(s.stage, s.forb, s.ext, "restactions") {
		t.Fatal("a mixed denial + 404 result reads as denial-only")
	}
}

func TestIssue681_A4b_RateLimit403WithoutForbiddenReason_NotForbidden(t *testing.T) {
	srv, _ := userAPI681(t)
	dict, s := resolveAsUser681(t, srv.URL, stage681("rl", "/api/v1/namespaces/team-a/configmaps/ratelimited"))
	if s.stage.Count() != 1 || s.forb.Stage() != 0 {
		t.Fatalf("stage=%d forb.stage=%d, want 1/0: a 403 without reason=Forbidden is not a per-identity forbidden", s.stage.Count(), s.forb.Stage())
	}
	_ = dict
}

// A1 + A3 — F1 (branch C, SA credential, background identity) for the same
// coordinate yields the byte-identical canonical body F2 yields.
func TestIssue681_A1_A3_RegateDeny_IsForbidden_SameBodyAsF2(t *testing.T) {
	newWatcher677(t)
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	_ = f
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
	ctx, s := withSinks681(ctx)
	// u677 may not get configmaps in team-b (rbacSeed677).
	dict := Resolve(context.WithoutCancel(ctx), ResolveOptions{
		RC: rc, Items: []*templates.API{stage681("catalog", "/api/v1/namespaces/team-b/configmaps/catalog")},
		RESTActionNamespace: "krateo-system", RESTActionName: "ra681",
	})
	if s.stage.Count() != 1 || s.forb.Stage() != 1 || s.forb.External() != 0 {
		t.Fatalf("stage=%d forb.stage=%d forb.external=%d, want 1/1/0", s.stage.Count(), s.forb.Stage(), s.forb.External())
	}
	got := jsonOf(t, accumulatedFirst(dict["catalogErr"]))
	if want := jsonOf(t, canonicalCatalog681()); got != want {
		t.Fatalf("F1 body differs from the canonical (= F2) body:\n got %s\nwant %s", got, want)
	}
	if !cache.DenialOnly(s.stage, s.forb, s.ext, "restactions") {
		t.Fatal("an F1-only resolve does not read as denial-only")
	}
	if cache.DenialOnly(s.stage, s.forb, s.ext, cache.CacheEntryClassWidgetContent) {
		t.Fatal("H6: an identity-free class read as denial-only — a shared shell must never hold a denial")
	}
}

func TestIssue681_H3_ClassifyOwnDialForbidden_Conditions(t *testing.T) {
	forbidden := &response.Status{Kind: "Status", Status: response.StatusFailure, Code: 403, Reason: response.StatusReasonForbidden}
	path := "/api/v1/namespaces/team-b/configmaps/catalog"
	get := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet)}}
	plain := &templates.API{Name: "s"}
	ctx := context.Background()

	if _, ok := classifyOwnDialForbidden(ctx, plain, get, forbidden); !ok {
		t.Fatal("precondition: the own-dial apiserver 403 must classify")
	}
	cases := []struct {
		name string
		ctx  context.Context
		api  *templates.API
		call httpcall.RequestOptions
		res  *response.Status
	}{
		{"endpointRef", ctx, &templates.API{Name: "s", EndpointRef: &templates.Reference{Name: "github"}}, get, forbidden},
		{"sa_dial", cache.WithServiceAccountDial(ctx), plain, get, forbidden},
		{"post", ctx, plain, httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodPost)}}, forbidden},
		{"unparseable", ctx, plain, httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: "/charts/index.yaml", Verb: ptr.To(http.MethodGet)}}, forbidden},
		{"no_reason", ctx, plain, get, &response.Status{Code: 403}},
		{"unauthorized", ctx, plain, get, &response.Status{Code: 401, Reason: response.StatusReasonUnauthorized}},
	}
	for _, c := range cases {
		if _, ok := classifyOwnDialForbidden(c.ctx, c.api, c.call, c.res); ok {
			t.Errorf("H3 RED (%s): classified as a per-identity forbidden", c.name)
		}
	}
}

// H7 — the branch-E SA refusal (snowplow declining to dial an SA endpoint it
// cannot re-gate: here a subresource, which branch C never takes) is a stage
// error but NOT a per-identity forbidden: the requester's own dial might
// succeed, so it is not "what this identity reads".
func TestIssue681_H7_BranchERefusal_NotForbidden(t *testing.T) {
	newWatcher677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: rc.Host})
	ctx = cache.WithBackgroundResolve(ctx)
	ctx, s := withSinks681(ctx)
	_ = Resolve(context.WithoutCancel(ctx), ResolveOptions{
		RC: rc, Items: []*templates.API{stage681("st", "/api/v1/namespaces/team-b/configmaps/catalog/status")},
		RESTActionNamespace: "krateo-system", RESTActionName: "ra681",
	})
	if s.stage.Count() != 1 || s.forb.Stage() != 0 {
		t.Fatalf("stage=%d forb.stage=%d, want 1/0 (a branch-E refusal is not a per-identity forbidden)", s.stage.Count(), s.forb.Stage())
	}
	if f.dials.Load() != 0 {
		t.Fatalf("precondition: branch E must refuse BEFORE the dial (dials=%d)", f.dials.Load())
	}
}
