// issue1429_nested_resolve_test.go — #1438 review, N3: a RESTAction stage with
// `resolve: true` whose TARGET's type is not installed.
//
// The stage resolves its target in-process (ResolveNestedCall → objects.Get).
// objects.Get decides the type is not installed and the nested call returns
// that error; this arm records what the stage, and then a widget that depends
// on the RESTAction, make of it.
package dispatchers

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	jwtutil "github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	"k8s.io/client-go/rest"
)

func TestIssue1429_AResolveTrueStageWhoseTargetsTypeIsNotInstalled(t *testing.T) {
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("DEBUG", "false")
	restactionsapi.RegisterNestedCallResolver(ResolveNestedCall)

	a := &unservedapi.Server{Token: i1414Token}
	srv := httptest.NewTLSServer(a)
	t.Cleanup(srv.Close)
	ca := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "requester", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: ca, Username: "requester", Token: i1414Token}))

	ep := endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: ca, Username: "requester", Token: i1414Token}
	ctx = cache.WithInternalEndpoint(ctx, &ep)

	stage := &templates.API{
		Name:    "target",
		Path:    "/apis/templates.krateo.io/v1/namespaces/ns/restactions/inner",
		Verb:    ptr.To("GET"),
		Resolve: ptr.To(true),
	}
	dict, failure := restactionsapi.ResolveWithFailure(ctx, restactionsapi.ResolveOptions{
		RC:                  &rest.Config{Host: srv.URL},
		Items:               []*templates.API{stage},
		RESTActionNamespace: "ns",
		RESTActionName:      "outer",
	})
	if failure == nil {
		t.Fatalf("the stage did not fail; dict=%#v", dict)
	}
	t.Logf("stage failure: stage=%q code=%d reason=%q message=%q", failure.Stage, failure.Code, failure.Reason, failure.Message)

	// PRECONDITION: this is the in-process route. The target was never dialled
	// as a stage would dial it; objects.Get read discovery and decided.
	for _, p := range a.Paths() {
		if strings.Contains(p, "/restactions/") {
			t.Fatalf("precondition: the stage dialled its target (%s); this arm must take the in-process resolve", p)
		}
	}
	if len(a.Paths()) == 0 {
		t.Fatalf("precondition: nothing reached the apiserver")
	}
	const want = "The type restactions (templates.krateo.io/v1) is not installed on this cluster."
	if failure.Code != http.StatusFailedDependency || failure.Reason != "FailedDependency" || failure.Message != want {
		t.Errorf("stage failure = %d %q %q, want 424 FailedDependency %q (no upstream answered: never \"upstream answered 424\")",
			failure.Code, failure.Reason, failure.Message, want)
	}

	// What the stage's error makes of it at each handler: the same
	// type-not-installed answer as every other route, naming the target's type.
	err := failure.Err("ns", "outer")
	rec := httptest.NewRecorder()
	encodeWidgetResolveError(h1ReqCtx(h1User), rec, err)
	t.Logf("a widget whose apiRef is this RESTAction answers: status=%d body=%s", rec.Code, strings.TrimSpace(rec.Body.String()))
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("status = %d, want 424", rec.Code)
	}
	if st := i1414Status(t, rec); st.Reason != "FailedDependency" || st.Message != want {
		t.Errorf("want FailedDependency/%q, got %q/%q", want, st.Reason, st.Message)
	}
	i1429Details(t, rec, "templates.krateo.io", "templates.krateo.io/v1", "restactions")
	for _, leak := range []string{"inner", "outer", "requester", i1414Token} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("the body carries %q: %s", leak, rec.Body.String())
		}
	}
}
