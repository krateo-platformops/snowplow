package api

// branche_impersonation_615_test.go — #615 / #725 / #721-gate arms for
// snowplow's ServiceAccount-endpoint dials at branch E. Each drives the REAL
// api.Resolve: the refresher's ctx shape (background, the cohort, the SA
// endpoint as the ctx internal endpoint, an impersonating *rest.Config, the
// #574 marker), or a live UAF stage (which dials the SA endpoint by design).
//
//	FailClosed_NoImpersonatingConfig   marker + a plain config → 0 dials.
//	Impersonates_DNSEndpoint_IPConfig  F2: the endpoint is a DNS name, the
//	                                   config an IP literal, one server — the
//	                                   production shape — and it impersonates.
//	FailClosed_Writes                  POST/PUT/PATCH/DELETE of a non-review
//	                                   resource → 0 dials (refused by #903's
//	                                   background-write rule, before branch E).
//	AccessReview_NotSentInBackground   #903: an SSAR / SAR POST is a write too →
//	                                   0 dials (it was dialed as the cohort).
//	AccessReview_PathTricks_FailClosed a "review" path with dot segments or
//	                                   %-encoding, or a PUT on a review → 0 dials.
//	Redirect_*                         F1: a 302 / 307 from the apiserver to
//	                                   another host — that host receives NOTHING
//	                                   (no SA bearer, no Impersonate-*), on an
//	                                   impersonated dial and on a plain UAF dial.
//	StageHeaders_*                     #725: a stage's Impersonate-* (any case,
//	                                   duplicated) and Authorization never reach
//	                                   an SA dial; the cohort and the SA bearer win.
//	EndpointRef_NeverImpersonated      an endpointRef stage keeps its own
//	                                   credential and gets no Impersonate-*.
//	BranchEDecide                      the decision table.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	u615       = "u615-cohort"
	g615       = "g615-cohort"
	saToken615 = "sa-615"
)

type rec615 struct {
	method, path, impUser string
	auth, impGroups       []string
}

type server615 struct {
	srv          *httptest.Server
	mu           sync.Mutex
	reqs         []rec615
	redirectTo   string // when set, answer every request with redirectCode → redirectTo+path
	redirectCode int
}

func newServer615(t *testing.T) *server615 {
	t.Helper()
	s := &server615{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.reqs = append(s.reqs, rec615{r.Method, r.URL.Path, r.Header.Get("Impersonate-User"),
			r.Header.Values("Authorization"), r.Header.Values("Impersonate-Group")})
		to, code := s.redirectTo, s.redirectCode
		s.mu.Unlock()
		if to != "" {
			http.Redirect(w, r, to+r.URL.Path, code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"kind":"SubjectAccessReview","status":{"allowed":true}}`))
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"List","items":[]}`))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *server615) requests() []rec615 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rec615(nil), s.reqs...)
}

// impRC615 is the refresher's impersonating config for the cohort on host.
func impRC615(host string) *rest.Config {
	return &rest.Config{Host: host, BearerToken: saToken615,
		Impersonate: rest.ImpersonationConfig{UserName: u615, Groups: []string{g615}}}
}

// resolve615 runs one stage under the refresher's ctx shape: the cohort, the SA
// endpoint saURL as the internal endpoint, rc as the internal config, the marker.
func resolve615(t *testing.T, saURL string, rc *rest.Config, stage *templates.API) (map[string]any, sinks681) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	ctx := cache.WithInternalRESTConfig(ctxWithUser(u615, g615), rc)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: saURL, Token: saToken615})
	ctx = cache.WithBackgroundResolve(ctx)
	ctx = cache.WithImpersonatedDial(ctx)
	ctx, s := withSinks681(ctx)
	dict := Resolve(context.WithoutCancel(ctx), ResolveOptions{RC: rc, AuthnNS: "krateo-system",
		Items: []*templates.API{stage}, RESTActionNamespace: "krateo-system", RESTActionName: "ra615"})
	return dict, s
}

// resolveUAF615 runs one UAF stage on a LIVE request: it dials the SA endpoint
// at saURL by design, with no marker.
func resolveUAF615(t *testing.T, saURL string, stage *templates.API) (map[string]any, sinks681) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	cache.SetGlobal(nil)
	t.Cleanup(SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: saURL, Token: saToken615}, nil
	}))
	stage.UserAccessFilter = &templates.UserAccessFilterSpec{Verb: "get", Resource: "namespaces", NamespaceFrom: ".metadata.name"}
	ctx, s := withSinks681(ctxWithUser(u615, g615))
	dict := Resolve(context.WithoutCancel(ctx), ResolveOptions{RC: &rest.Config{Host: saURL}, AuthnNS: "krateo-system",
		Items: []*templates.API{stage}, RESTActionNamespace: "krateo-system", RESTActionName: "ra615"})
	return dict, s
}

func stage615(verb, path string) *templates.API {
	return &templates.API{Name: "s615", Path: path, Verb: ptr.To(verb),
		ContinueOnError: ptr.To(true), ErrorKey: ptr.To("s615Err")}
}

func assertRefused615(t *testing.T, dict map[string]any, s sinks681, wantReason string) {
	t.Helper()
	got := jsonOf(t, dict["s615Err"])
	if s.stage.Count() != 1 || !strings.Contains(got, "refused under impersonation") || !strings.Contains(got, wantReason) {
		t.Fatalf("want the refusal recorded as a stage error naming %q; stage sink=%d errorKey=%s", wantReason, s.stage.Count(), got)
	}
}

func assertCohort615(t *testing.T, r rec615) {
	t.Helper()
	if r.impUser != u615 || len(r.impGroups) != 1 || r.impGroups[0] != g615 {
		t.Fatalf("#615 RED: %s %s went out as Impersonate-User=%q Impersonate-Group=%v, want the cohort %q/%q",
			r.method, r.path, r.impUser, r.impGroups, u615, g615)
	}
	if len(r.auth) != 1 || r.auth[0] != "Bearer "+saToken615 {
		t.Fatalf("#725 RED: %s %s carried Authorization=%q, want exactly the SA bearer", r.method, r.path, r.auth)
	}
}

func TestIssue615_FailClosed_NoImpersonatingConfig(t *testing.T) {
	srv := newServer615(t)
	dict, s := resolve615(t, srv.srv.URL, &rest.Config{Host: srv.srv.URL, BearerToken: saToken615},
		stage615(http.MethodGet, "/api/v1/namespaces/team-a/configmaps/c615/status"))
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("#615 RED: marker without an impersonating config dialed the SA endpoint %d time(s) as plain SA", n)
	}
	assertRefused615(t, dict, s, "no impersonating config")
}

// F2 — production: dynamic.ServiceAccountEndpoint is https://kubernetes.default.svc
// while rest.InClusterConfig is https://<service IP>:443. Two host strings, one
// server. The impersonated dial must still happen (it used to fail closed).
func TestIssue615_Impersonates_DNSEndpoint_IPConfig(t *testing.T) {
	srv := newServer615(t) // listens on 127.0.0.1:<port>
	port := srv.srv.URL[strings.LastIndex(srv.srv.URL, ":")+1:]
	dnsEndpoint := "http://localhost:" + port // the endpoint: a DNS name
	ipConfig := "http://127.0.0.1:" + port    // the config: an IP literal
	_, s := resolve615(t, dnsEndpoint, impRC615(ipConfig),
		stage615(http.MethodGet, "/api/v1/namespaces/team-a/configmaps/c615/status"))
	reqs := srv.requests()
	if len(reqs) != 1 {
		t.Fatalf("#721-gate F2 RED: with a DNS endpoint and an IP config the impersonated dial was not made "+
			"(%d requests, stage errors %d) — every production access review would fail closed", len(reqs), s.stage.Count())
	}
	assertCohort615(t, reqs[0])
}

func TestIssue615_FailClosed_Writes(t *testing.T) {
	// A create targets the collection; an update, patch or delete the object.
	for verb, path := range map[string]string{
		http.MethodPost:   "/api/v1/namespaces/team-a/configmaps",
		http.MethodPut:    "/api/v1/namespaces/team-a/configmaps/c615",
		http.MethodPatch:  "/api/v1/namespaces/team-a/configmaps/c615",
		http.MethodDelete: "/api/v1/namespaces/team-a/configmaps/c615",
	} {
		t.Run(verb, func(t *testing.T) {
			srv := newServer615(t)
			dict, s := resolve615(t, srv.srv.URL, impRC615(srv.srv.URL), stage615(verb, path))
			for _, r := range srv.requests() {
				t.Fatalf("#615 RED: a background refresh performed %s %s (Impersonate-User=%q) — a write is never "+
					"performed as the cohort", r.method, r.path, r.impUser)
			}
			assertBackgroundWriteRefused615(t, dict, s, verb)
		})
	}
}

// assertBackgroundWriteRefused615 — the #903 refusal (dispatchOneCall, before
// branch E) recorded as a stage error, so the refresher's Put declines.
func assertBackgroundWriteRefused615(t *testing.T, dict map[string]any, s sinks681, verb string) {
	t.Helper()
	got := jsonOf(t, dict["s615Err"])
	if s.stage.Count() != 1 || !strings.Contains(got, "background work never writes") || !strings.Contains(got, "verb "+verb) {
		t.Fatalf("want the #903 refusal recorded as a stage error for %s; stage sink=%d errorKey=%s", verb, s.stage.Count(), got)
	}
}

// #903 supersedes #615's access-review carve-out: background work never
// writes, and creating an access review is a POST.
func TestIssue615_AccessReview_NotSentInBackground(t *testing.T) {
	for _, res := range []string{"selfsubjectaccessreviews", "subjectaccessreviews"} {
		t.Run(res, func(t *testing.T) {
			srv := newServer615(t)
			dict, s := resolve615(t, srv.srv.URL, impRC615(srv.srv.URL),
				stage615(http.MethodPost, "/apis/authorization.k8s.io/v1/"+res))
			if reqs := srv.requests(); len(reqs) != 0 {
				t.Fatalf("#903 RED: a background refresh created an access review (%d request(s))", len(reqs))
			}
			assertBackgroundWriteRefused615(t, dict, s, http.MethodPost)
		})
	}
}

// Hardening (#721 gate): a path that only LOOKS like an access review, or a
// non-POST on a review resource, is a write and fails closed.
func TestIssue615_AccessReview_PathTricks_FailClosed(t *testing.T) {
	cases := map[string]struct{ verb, path string }{
		"encoded traversal": {http.MethodPost, "/apis/authorization.k8s.io/v1/..%2F..%2F..%2Fapi%2Fv1%2Fnamespaces%2Fteam-a%2Fconfigmaps"},
		"dot segments":      {http.MethodPost, "/apis/authorization.k8s.io/v1/../../../api/v1/namespaces/team-a/configmaps"},
		"dot segment":       {http.MethodPost, "/apis/authorization.k8s.io/v1/./subjectaccessreviews"},
		"PUT on a review":   {http.MethodPut, "/apis/authorization.k8s.io/v1/subjectaccessreviews/x"},
		// Only POST creates a review: a DELETE on the review collection is a
		// write that must not ride the access-review carve-out.
		"DELETE on reviews": {http.MethodDelete, "/apis/authorization.k8s.io/v1/subjectaccessreviews"},
		"non-review in grp": {http.MethodPost, "/apis/authorization.k8s.io/v1/notareview"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			srv := newServer615(t)
			_, _ = resolve615(t, srv.srv.URL, impRC615(srv.srv.URL), stage615(c.verb, c.path))
			for _, r := range srv.requests() {
				t.Fatalf("#721-gate RED: %s %s was performed as the cohort (Impersonate-User=%q) although it is not an "+
					"access-review create", r.method, r.path, r.impUser)
			}
		})
	}
}

// F1 — a redirect from the apiserver to another host: that host must receive
// nothing at all, on an impersonated dial and on a plain (UAF) SA dial.
func TestIssue615_Redirect_ImpersonatedDial_NeverLeavesTheHost(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			other := newServer615(t)
			api := newServer615(t)
			api.redirectTo, api.redirectCode = other.srv.URL, code
			dict, s := resolve615(t, api.srv.URL, impRC615(api.srv.URL),
				stage615(http.MethodGet, "/api/v1/namespaces/team-a/configmaps/c615/status"))
			for _, r := range other.requests() {
				t.Fatalf("#721-gate F1 RED: a %d redirect carried the dial to another host with Impersonate-User=%q "+
					"Impersonate-Group=%v Authorization=%q", code, r.impUser, r.impGroups, r.auth)
			}
			reqs := api.requests()
			if len(reqs) != 1 {
				t.Fatalf("precondition: the apiserver must be dialed once, got %d", len(reqs))
			}
			assertCohort615(t, reqs[0])
			if s.stage.Count() != 1 || dict["s615Err"] == nil {
				t.Fatalf("the refused redirect must be a recorded stage error (sink %d)", s.stage.Count())
			}
		})
	}
}

func TestIssue615_Redirect_PlainUAFSADial_NeverLeavesTheHost(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			other := newServer615(t)
			api := newServer615(t)
			api.redirectTo, api.redirectCode = other.srv.URL, code
			_, _ = resolveUAF615(t, api.srv.URL, stage615(http.MethodGet, "/api/v1/namespaces"))
			for _, r := range other.requests() {
				t.Fatalf("#721-gate F1 RED: a %d redirect carried the SA dial to another host with Authorization=%q",
					code, r.auth)
			}
			if len(api.requests()) != 1 {
				t.Fatalf("precondition: the SA endpoint must be dialed once, got %d", len(api.requests()))
			}
		})
	}
}

// #725 — under the marker the cohort and the SA bearer win over anything a
// stage sets, in any case, duplicated.
func TestIssue615_StageHeaders_CannotOverrideTheCohortOrTheCredential(t *testing.T) {
	srv := newServer615(t)
	st := stage615(http.MethodGet, "/api/v1/namespaces/team-a/configmaps/c615/status")
	st.Headers = []string{"Impersonate-User: evil615", "IMPERSONATE-USER: evil615b", "impersonate-group: system:masters",
		"Impersonate-Uid: evil-uid", "Impersonate-Extra-Scopes: all", "Authorization: Bearer author-615",
		" authorization : Bearer author-615b"}
	_, _ = resolve615(t, srv.srv.URL, impRC615(srv.srv.URL), st)
	reqs := srv.requests()
	if len(reqs) != 1 {
		t.Fatalf("precondition: the GET must be dialed once, got %d", len(reqs))
	}
	assertCohort615(t, reqs[0])
}

// #725 — on a plain SA dial (a live UAF stage, no marker) a stage's
// Impersonate-* and Authorization headers never reach the wire.
func TestIssue615_StageHeaders_StrippedOnUAFSADial(t *testing.T) {
	srv := newServer615(t)
	st := stage615(http.MethodGet, "/api/v1/namespaces")
	st.Headers = []string{"Impersonate-User: evil615", "impersonate-group: system:masters", "Impersonate-Extra-X: y",
		"AUTHORIZATION: Bearer author-615"}
	_, _ = resolveUAF615(t, srv.srv.URL, st)
	reqs := srv.requests()
	if len(reqs) == 0 {
		t.Fatal("precondition: the UAF stage must dial the ServiceAccount endpoint")
	}
	for _, r := range reqs {
		if r.impUser != "" || len(r.impGroups) != 0 {
			t.Fatalf("#725 RED: a stage header made snowplow's ServiceAccount impersonate %q %v", r.impUser, r.impGroups)
		}
		if len(r.auth) != 1 || r.auth[0] != "Bearer "+saToken615 {
			t.Fatalf("#725 RED: a stage Authorization reached the SA dial: %q", r.auth)
		}
	}
}

func TestIssue615_EndpointRef_NeverImpersonated(t *testing.T) {
	ext := newServer615(t)       // a third-party host behind an endpointRef
	apiserver := newServer615(t) // where the cohort is impersonated
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		"ext615": {ObjectMeta: metav1.ObjectMeta{Namespace: "krateo-system", Name: "ext615"},
			Data: map[string][]byte{"server-url": []byte(ext.srv.URL), "token": []byte("ext-token")}},
	}})
	cache.ForceSecretsCacheReadyForTest("krateo-system")
	t.Cleanup(func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() })

	for _, verb := range []string{http.MethodGet, http.MethodPost} {
		st := stage615(verb, "/render")
		st.EndpointRef = &templates.Reference{Name: "ext615", Namespace: "krateo-system"}
		_, _ = resolve615(t, apiserver.srv.URL, impRC615(apiserver.srv.URL), st)
	}
	reqs := ext.requests()
	if len(reqs) == 0 {
		t.Fatal("precondition: the endpointRef stages must reach their host")
	}
	for _, r := range reqs {
		if r.impUser != "" || len(r.impGroups) != 0 {
			t.Fatalf("#615 RED: the cohort's identity was sent to an endpointRef host (%s %s Impersonate-User=%q "+
				"Impersonate-Group=%v)", r.method, r.path, r.impUser, r.impGroups)
		}
		if len(r.auth) != 1 || r.auth[0] != "Bearer ext-token" {
			t.Fatalf("an endpointRef dial must use the endpoint's own credential, got %q", r.auth)
		}
	}
	if n := len(apiserver.requests()); n != 0 {
		t.Fatalf("an endpointRef stage must not be redirected to the apiserver (%d requests)", n)
	}
}

func TestIssue615_BranchEDecide(t *testing.T) {
	const ep = "https://kubernetes.default.svc"
	imp := impRC615("https://10.96.0.1:443") // production: the config's host is an IP literal
	plain := &rest.Config{Host: "https://10.96.0.1:443"}
	call := func(url, verb, path string) httpcall.RequestOptions {
		return httpcall.RequestOptions{Endpoint: &endpoints.Endpoint{ServerURL: url},
			RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(verb)}}
	}
	marked := func(rc *rest.Config) context.Context {
		ctx := cache.WithInternalEndpoint(context.Background(), &endpoints.Endpoint{ServerURL: ep})
		return cache.WithImpersonatedDial(cache.WithInternalRESTConfig(ctx, rc))
	}
	cases := []struct {
		name string
		ctx  context.Context
		call httpcall.RequestOptions
		want branchEDecision
	}{
		{"no marker", cache.WithInternalRESTConfig(context.Background(), imp), call(ep, "GET", "/api/v1/pods"), branchEUnmarked},
		{"GET, DNS endpoint vs IP config (F2)", marked(imp), call(ep, "GET", "/api/v1/namespaces/n/pods/p/status"), branchEImpersonate},
		// #903 — the access-review POST carve-out is gone: a background
		// refresh never writes, an access review included.
		{"SAR POST", marked(imp), call(ep, "POST", "/apis/authorization.k8s.io/v1/subjectaccessreviews"), branchEFailClosed},
		{"SSRR POST", marked(imp), call(ep, "POST", "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews"), branchEFailClosed},
		{"LSAR POST", marked(imp), call(ep, "POST", "/apis/authorization.k8s.io/v1/namespaces/n/localsubjectaccessreviews"), branchEFailClosed},
		{"plain rc", marked(plain), call(ep, "GET", "/api/v1/pods"), branchEFailClosed},
		{"no rc", cache.WithImpersonatedDial(context.Background()), call(ep, "GET", "/api/v1/pods"), branchEFailClosed},
		{"not the internal endpoint", marked(imp), call("https://evil.example", "GET", "/api/v1/pods"), branchEFailClosed},
		{"no internal endpoint", cache.WithImpersonatedDial(cache.WithInternalRESTConfig(context.Background(), imp)), call(ep, "GET", "/api/v1/pods"), branchEFailClosed},
		{"POST create", marked(imp), call(ep, "POST", "/api/v1/namespaces/n/configmaps"), branchEFailClosed},
		{"DELETE", marked(imp), call(ep, "DELETE", "/api/v1/namespaces/n/configmaps/c"), branchEFailClosed},
		{"PUT on a review", marked(imp), call(ep, "PUT", "/apis/authorization.k8s.io/v1/subjectaccessreviews/x"), branchEFailClosed},
		{"DELETE on the review collection", marked(imp), call(ep, "DELETE", "/apis/authorization.k8s.io/v1/subjectaccessreviews"), branchEFailClosed},
		{"POST, other group's review", marked(imp), call(ep, "POST", "/apis/authentication.k8s.io/v1/tokenreviews"), branchEFailClosed},
		{"POST, encoded traversal", marked(imp), call(ep, "POST", "/apis/authorization.k8s.io/v1/..%2F..%2Fapi%2Fv1%2Fnamespaces%2Fn%2Fconfigmaps"), branchEFailClosed},
		{"POST, dot segments", marked(imp), call(ep, "POST", "/apis/authorization.k8s.io/v1/../../api/v1/namespaces/n/configmaps"), branchEFailClosed},
		{"POST, query", marked(imp), call(ep, "POST", "/apis/authorization.k8s.io/v1/subjectaccessreviews?x=y"), branchEFailClosed},
	}
	for _, c := range cases {
		if _, got, _ := branchEDecide(c.ctx, c.call); got != c.want {
			t.Fatalf("%s: branchEDecide = %v, want %v", c.name, got, c.want)
		}
	}
	for _, h := range []string{"Impersonate-User", "impersonate-group", " IMPERSONATE-EXTRA-x ", "Impersonate-Uid", "Authorization", "AUTHORIZATION "} {
		if !isSADialControlledHeader(h) {
			t.Fatalf("isSADialControlledHeader(%q) = false", h)
		}
	}
	for _, h := range []string{"Accept", "Content-Type", "X-Impersonate-User", "Authorization-Extra"} {
		if isSADialControlledHeader(h) {
			t.Fatalf("isSADialControlledHeader(%q) = true", h)
		}
	}
}
