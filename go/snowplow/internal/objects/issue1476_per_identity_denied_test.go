package objects

// issue1476_per_identity_denied_test.go — #1476: objects.Get says whether its
// Forbidden is the ctx identity's (an expected per-identity deny, logged at
// WARN once per cell generation by the caller) or snowplow's own credential's
// (a fault, logged at ERROR). Decided by whose credential the read presented,
// never by the message.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

func TestIssue1476_ForbiddenIsPerIdentityOnlyForTheCtxIdentitysCredential(t *testing.T) {
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
		case "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`))
		case "/api/v1":
			_, _ = w.Write([]byte(`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[` +
				`{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]}]}`))
		case "/api/v1/namespaces/ns/configmaps/x":
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"ns"}}`))
		case "/api/v1/namespaces/ns/configmaps/refused":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	ref := func(name string) templatesv1.ObjectReference {
		return templatesv1.ObjectReference{
			Reference: templatesv1.Reference{Name: name, Namespace: "ns"}, APIVersion: "v1", Resource: "configmaps",
		}
	}
	for _, c := range []struct {
		name   string
		ctx    func() context.Context
		object string
		want   bool
	}{
		{name: "the_requesters_own_credential", object: "refused", want: true, ctx: func() context.Context {
			return context.Background()
		}},
		{name: "the_service_accounts_own_credential", object: "refused", want: false, ctx: func() context.Context {
			return cache.WithInternalRESTConfig(context.Background(), &rest.Config{Host: srv.URL})
		}},
		{name: "a_credential_impersonating_the_ctx_identity", object: "refused", want: true, ctx: func() context.Context {
			return cache.WithInternalRESTConfig(context.Background(),
				&rest.Config{Host: srv.URL, Impersonate: rest.ImpersonationConfig{UserName: "u1476"}})
		}},
		// The ServiceAccount read under a background driver's real identity is
		// re-gated for that identity; with no RBAC snapshot the gate fails
		// closed: the deny is the identity's (the #711 class).
		{name: "the_sa_read_re_gated_for_the_ctx_identity", object: "x", want: true, ctx: func() context.Context {
			ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: "u1476"}))
			return cache.WithBackgroundResolve(cache.WithInternalRESTConfig(ctx, &rest.Config{Host: srv.URL}))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := xcontext.BuildContext(c.ctx(),
				xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "t"}))
			res := getFromAPIServer(ctx, ref(c.object))
			if res.Err == nil || res.Err.Code != http.StatusForbidden {
				t.Fatalf("PRECONDITION: the read must be refused with 403 (err=%v)", res.Err)
			}
			if res.PerIdentityDenied != c.want {
				t.Fatalf("PerIdentityDenied=%v, want %v", res.PerIdentityDenied, c.want)
			}
		})
	}
}
