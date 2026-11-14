// issue1414_dependency_type_test.go — #1414: a widget whose DEPENDENCY's type
// is not installed.
//
// The widget exists; the object its apiRef names is of a type the apiserver
// does not serve. The real apiRef resolve (apiref.Resolve → objects.Get)
// produces the error against internal/testsupport/unservedapi, and the real
// encodeWidgetResolveError answers it: 424 Failed Dependency, a JSON Status
// whose message names the DEPENDENCY's type (never the widget's, never the
// object), with the reason kept.
package dispatchers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"github.com/krateo-platformops/snowplow/internal/testsupport/unservedapi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestIssue1414_AWidgetWhoseDependencysTypeIsNotInstalledIs424NamingThatType(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	a := &unservedapi.Server{}
	srv := httptest.NewServer(a)
	t.Cleanup(srv.Close)
	ctx := f6cSACtx(srv.URL)

	// The dependency: an object of the unserved type. The widget would be a served one.
	dep := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "header-context", Namespace: "ns"},
		APIVersion: unservedapi.GV, Resource: "flexes",
	}
	_, err := apiref.Resolve(ctx, apiref.ResolveOptions{RC: &rest.Config{Host: srv.URL}, ApiRef: dep})
	if err == nil {
		t.Fatalf("premise: the apiRef resolve of an unserved type succeeded")
	}
	if len(a.Paths()) == 0 {
		t.Fatalf("premise: the resolve never reached the apiserver")
	}

	rec := httptest.NewRecorder()
	encodeWidgetResolveError(h1ReqCtx(h1User), rec, err)
	t.Logf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("status = %d, want 424; body: %s", rec.Code, rec.Body.String())
	}
	st := i1414Status(t, rec)
	const want = "The type flexes (widgets.templates.krateo.io/v1beta1) is not installed on this cluster."
	if st.Status != metav1.StatusFailure || st.Reason != "FailedDependency" || st.Message != want {
		t.Errorf("want Failure/FailedDependency/%q, got %q/%q/%q", want, st.Status, st.Reason, st.Message)
	}
	for _, leak := range []string{"header-context", h1User, "apiref resolve"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Errorf("the body carries %q: %s", leak, rec.Body.String())
		}
	}
}
