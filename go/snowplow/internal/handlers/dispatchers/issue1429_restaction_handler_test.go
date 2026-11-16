// issue1429_restaction_handler_test.go — #1438 review: the RESTAction /call
// handler's own type-not-installed branch, at the handler.
//
// A RESTAction with a `resolve: true` stage whose target's TYPE is not
// installed. The real RESTAction() handler runs the real restactions.Resolve;
// the stage resolves its target in-process (ResolveNestedCall → objects.Get),
// objects.Get decides the type is not installed, and the handler must answer
// the same 424 as every other route: reason, message and the target's type in
// the details.
//
// The one seam is the read of the RESTAction CR itself (fetchObjectFn): the
// test apiserver serves no RESTAction type, which is exactly what the stage's
// target needs.
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
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func TestIssue1429_RESTActionHandler_AResolveTrueTargetOfAnUninstalledType(t *testing.T) {
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("DEBUG", "false")
	restactionsapi.RegisterNestedCallResolver(ResolveNestedCall)

	a := &unservedapi.Server{Token: i1414Token}
	srv := httptest.NewTLSServer(a)
	t.Cleanup(srv.Close)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	ca := base64.StdEncoding.EncodeToString(caPEM)
	// The ServiceAccount config the handler resolves with, as in a pod.
	saRC := &rest.Config{Host: srv.URL, BearerToken: i1414Token, TLSClientConfig: rest.TLSClientConfig{CAData: caPEM}}
	ep := endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: ca, Username: "requester", Token: i1414Token}

	// The RESTAction CR the handler dispatches: one stage, resolved in-process,
	// whose target is a RESTAction of the type this apiserver does not serve.
	outer := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": "outer", "namespace": "ns"},
		// The filter reads the stage: with the stage failed it has no value, and
		// the stage's failure is the answer (#709).
		"spec": map[string]any{"filter": ".target.spec.api[]", "api": []any{map[string]any{
			"name":    "target",
			"path":    "/apis/templates.krateo.io/v1/namespaces/ns/restactions/inner",
			"verb":    "GET",
			"resolve": true,
		}}},
	}}
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{Unstructured: outer,
			GVR: schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}}
	})
	t.Cleanup(restore)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "requester", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(ep))
	ctx = cache.WithInternalEndpoint(ctx, &ep)

	rec := httptest.NewRecorder()
	q := i1414Query("templates.krateo.io/v1", "restactions", "ns", "outer")
	RESTActionWithServiceAccountConfig(saRC).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/call?"+q.Encode(), nil).WithContext(ctx))
	t.Logf("requests: %v", a.Paths())
	t.Logf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), strings.TrimSpace(rec.Body.String()))

	// PRECONDITION: the in-process route. objects.Get read discovery for the
	// target's group/version and the target itself was never dialled.
	sawDoc := false
	for _, p := range a.Paths() {
		if strings.Contains(p, "/restactions/") {
			t.Fatalf("precondition: the stage dialled its target (%s)", p)
		}
		if p == "/apis/templates.krateo.io/v1" {
			sawDoc = true
		}
	}
	if !sawDoc {
		t.Fatalf("precondition: the read never asked discovery about the target's type; requests: %v", a.Paths())
	}

	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("status = %d, want 424; body: %s", rec.Code, rec.Body.String())
	}
	st := i1414Status(t, rec)
	const want = "The type restactions (templates.krateo.io/v1) is not installed on this cluster."
	if st.Status != "Failure" || st.Reason != "FailedDependency" || st.Message != want {
		t.Errorf("want Failure/FailedDependency/%q, got %q/%q/%q", want, st.Status, st.Reason, st.Message)
	}
	i1429Details(t, rec, "templates.krateo.io", "templates.krateo.io/v1", "restactions")
	for _, leak := range []string{"inner", "outer", "requester", i1414Token} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("the body carries %q: %s", leak, rec.Body.String())
		}
	}
}
