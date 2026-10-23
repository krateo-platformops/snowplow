package dispatchers

// issue853_identity_path_test.go — the #856 gate's identity leak, end to end.
//
// A RESTAction stage path templated from the caller's identity lands the
// caller's name in the stage's object name. When the upstream answers 404, the
// stage-failure builder copies that name into its canonical message
// (`<resource> "<name>" not found`) and the widget /call answers with it. The
// 424 is product copy (the SPA renders it verbatim under "Data source
// unavailable"), so the identity must not reach it.
//
// The real chain: widgetsHandler.ServeHTTP → apiref.Resolve (the RESTAction
// informer-served) → restactions.Resolve → the stage GET on the caller's own
// apiserver (the #709 agent-formdef shape: a required `crd` stage and a filter
// that binds it).
//
// Which forms are reachable: #288 refuses to dial a path whose name segment is
// not DNS-1123-valid, so a name with "@" or "%40" is never requested, never
// fails and never reaches a message. The DNS-valid forms of an identity are
// what reach it: a lower-cased group, the username's local part, the
// username's DNS-1123 form.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// i853ServeIdentityPath serves the widget /call as i853Carol for a widget
// whose apiRef RESTAction's required stage GETs customresourcedefinitions/
// <objectName> on the caller's apiserver, which answers 404. It returns the
// response and whether the stage was dialed.
func i853ServeIdentityPath(t *testing.T, objectName string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	stagePath := "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/" + objectName
	ra := &unstructured.Unstructured{Object: in443RA("ident-ra", i709Filter,
		map[string]any{"name": "crd", "verb": "GET", "path": stagePath})}
	// The caller may read the RESTAction (the harness's ra-reader role).
	bind := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "i853-carol-ra-reader", UID: types.UID("uid-i853-carol")},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: i853Carol}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader"},
	}
	f := in443Setup(t, ra, bind)
	// The caller's clientconfig, under the DNS-1123 name authn writes, next to
	// the harness users' (the stage dials the caller's own apiserver).
	secrets := map[string]*corev1.Secret{}
	for _, u := range append(append([]string{}, psUsers...), kubeutil.MakeDNS1123Compatible(i853Carol)) {
		secrets[u+"-clientconfig"] = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: psAuthnNS, Name: u + "-clientconfig"},
			Data:       map[string][]byte{"server-url": []byte(f.srv.URL), "token": []byte("tok-i853")},
		}
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: secrets})
	var dialed atomic.Bool
	notFound := func(w http.ResponseWriter, _ *http.Request) {
		dialed.Store(true)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"not found"}`)
	}
	f.extra["GET "+stagePath] = notFound
	if decoded, err := url.PathUnescape(stagePath); err == nil {
		f.extra["GET "+decoded] = notFound
	}
	_, raSync := cache.Global().EnsureResourceType(h1RAGVR)
	select {
	case <-raSync:
	case <-time.After(5 * time.Second):
		t.Fatalf("restactions informer did not sync")
	}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: i853Carol, Groups: i853CarolGroups}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: f.srv.URL, Token: "tok-i853"}))

	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version, "kind": "Panel",
			"metadata": map[string]any{"name": "ident-widget", "namespace": h1NS},
			"spec":     map[string]any{"apiRef": map[string]any{"name": "ident-ra", "namespace": h1NS}},
		}}}
	})
	r2 := setWidgetsResolveForTest(func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		_, err := apiref.Resolve(ctx, apiref.ResolveOptions{
			RC: &rest.Config{Host: f.srv.URL}, ApiRef: i431ApiRef("ident-ra"), AuthnNS: psAuthnNS,
		})
		return nil, cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef "+h1NS+"/ident-ra", err)
	})
	r3 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	defer func() { r3(); r2(); r1() }()
	rec := httptest.NewRecorder()
	(&widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}}).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx))
	return rec, dialed.Load()
}

// i853IdentityForms is every form of i853Carol's identity a reader could
// recognise, case-folded.
func i853IdentityForms() []string {
	return []string{"carol.smith", "carol.smith%40example.org", "carol.smith@example.org",
		strings.ToLower(kubeutil.MakeDNS1123Compatible(i853Carol)), "team-blue", "tok-i853"}
}

func i853AssertNoIdentity(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	body := strings.ToLower(rec.Body.String())
	for _, form := range i853IdentityForms() {
		if strings.Contains(body, form) {
			t.Fatalf("#856 RED: the caller's identity %q reached the answer (%d): %s", form, rec.Code, rec.Body.String())
		}
	}
}

// TestIssue853_IdentityTemplatedPath_EndToEnd: the DNS-valid identity forms
// are dialed, fail 404 and answer 424 with the fixed sentence and no identity.
func TestIssue853_IdentityTemplatedPath_EndToEnd(t *testing.T) {
	for _, c := range []struct{ name, objectName string }{
		{"lowercase_group", "team-blue"},
		{"username_local_part", "carol.smith"},
		{"username_dns_form", kubeutil.MakeDNS1123Compatible(i853Carol)},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec, dialed := i853ServeIdentityPath(t, c.objectName)
			// Preconditions: the stage really was dialed and is the failure
			// (else this proves nothing).
			if !dialed || rec.Code != http.StatusFailedDependency {
				t.Fatalf("PRECONDITION: dialed=%v code=%d, want a dialed stage and 424: %s", dialed, rec.Code, rec.Body.String())
			}
			i853AssertNoIdentity(t, rec)
			if st := i853Body(t, rec); st.Message != dependencyFailureFallback {
				t.Fatalf("message=%q, want the fixed sentence", st.Message)
			}
		})
	}
}

// TestIssue853_IdentityTemplatedPath_RefusedForms: a name with "@" or "%40" is
// refused by #288 before any dial, so it never fails and never reaches a
// message; the answer carries no identity either. This pins the scope of the
// end-to-end arm above: the escaped forms are guarded (the message arm covers
// them) but not reachable through a stage path today.
func TestIssue853_IdentityTemplatedPath_RefusedForms(t *testing.T) {
	for _, c := range []struct{ name, objectName string }{
		{"lowercase_username", "carol.smith@example.org"},
		{"query_escaped_username", "carol.smith%40example.org"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec, dialed := i853ServeIdentityPath(t, c.objectName)
			if dialed {
				t.Fatalf("the stage dialed a DNS-1123-invalid name; #288 no longer refuses it — the message arm now has a reachable path for this form: %d %s",
					rec.Code, rec.Body.String())
			}
			i853AssertNoIdentity(t, rec)
		})
	}
}
