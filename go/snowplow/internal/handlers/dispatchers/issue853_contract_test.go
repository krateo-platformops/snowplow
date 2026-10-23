package dispatchers

// issue853_contract_test.go — #853, the two promises the SPA's matching
// branch builds on:
//
//  1. A DELETE-evicted widget answers 404, never 424. The SPA reads a 404 on a
//     refresh-frame refetch as a confirmed delete and stops retrying (#256,
//     refreshSse.wasRefetchEventTriggered + useWidgetQuery shouldRetryWidgetFetch).
//     If a deleted widget reached the dependency path, a bulk delete would go
//     back to 4 requests per widget. Driven with the REAL CR read (fetchObject →
//     objects.Get → informer, then the apiserver fallthrough) against a widget
//     that is really deleted.
//  2. The 424 body is a Status whose message is product copy: the SPA renders
//     it verbatim under "Data source unavailable". It is non-empty and names no
//     identity.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// i853WidgetURL is the /call URL the SPA sends for the h1 widget.
func i853WidgetURL() string {
	q := url.Values{}
	q.Set("apiVersion", h1WidgetGVR.Group+"/"+h1WidgetGVR.Version)
	q.Set("resource", h1WidgetGVR.Resource)
	q.Set("namespace", h1NS)
	q.Set("name", h1WName)
	return "/call?" + q.Encode()
}

// TestIssue853_DeleteEvictedWidget_Is404_NeverTheDependencyPath: the same
// widget, whose resolve answers a dependency 404, is 424 while its CR exists
// and 404 once the CR is deleted; after the delete the resolve never runs. The
// healthy shape (served 200 and cached, then deleted) is 404 too.
func TestIssue853_DeleteEvictedWidget_Is404_NeverTheDependencyPath(t *testing.T) {
	for _, c := range []struct {
		name      string
		before    int
		beforeErr error
	}{
		{"healthy_then_deleted", http.StatusOK, nil},
		{"dependency_missing_then_deleted", http.StatusFailedDependency,
			cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef "+h1NS+"/agent-formdef",
				i853StatusErr(http.StatusNotFound, metav1.StatusReasonNotFound, `restactions.templates.krateo.io "agent-formdef" not found`))},
	} {
		t.Run(c.name, func(t *testing.T) {
			dyn := h1BuildWatcherWithRA(t)
			if _, err := dyn.Resource(h1WidgetGVR).Namespace(h1NS).Create(context.Background(), h1WidgetUnstructured(nil), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create widget: %v", err)
			}
			_, syncCh := cache.Global().EnsureResourceType(h1WidgetGVR)
			select {
			case <-syncCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("widget informer did not sync")
			}
			// After the delete the informer misses and objects.Get falls
			// through to the caller's apiserver, which answers what a real
			// apiserver answers for a deleted object.
			apiserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"%s.%s \"%s\" not found"}`,
					h1WidgetGVR.Resource, h1WidgetGVR.Group, h1WName)
			}))
			defer apiserver.Close()
			// Test mode keeps the caller's endpoint as given (outside it the
			// client is rebuilt for in-cluster).
			prevTM := env.TestMode()
			env.SetTestMode(true)
			defer env.SetTestMode(prevTM)
			reqCtx := xcontext.BuildContext(h1ReqCtx(h1User),
				xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: apiserver.URL, Token: "tok-" + h1User}))

			resolves := 0
			resolveErr := c.beforeErr
			r1 := setWidgetsResolveForTest(func(_ context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
				resolves++
				if resolveErr != nil {
					return nil, resolveErr
				}
				return opts.In, nil
			})
			r2 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
			defer func() { r2(); r1() }()
			serve := func() *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				Widgets().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, i853WidgetURL(), nil).WithContext(reqCtx))
				return rec
			}

			if rec := serve(); rec.Code != c.before {
				t.Fatalf("PRECONDITION: the existing widget answered %d, want %d: %s", rec.Code, c.before, rec.Body.String())
			}
			if resolves != 1 {
				t.Fatalf("PRECONDITION: the existing widget resolved %d times, want 1", resolves)
			}

			if err := dyn.Resource(h1WidgetGVR).Namespace(h1NS).Delete(context.Background(), h1WName, metav1.DeleteOptions{}); err != nil {
				t.Fatalf("delete widget: %v", err)
			}
			for end := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
				if _, hit := cache.Global().GetObject(h1WidgetGVR, h1NS, h1WName); !hit {
					break
				}
				if time.Now().After(end) {
					t.Fatalf("the DELETE never reached the informer")
				}
			}

			// The refetch the eviction frame triggers. Every resolve from here
			// on would answer a dependency 404, so any route into the resolve
			// shows as a 424.
			resolveErr = cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef",
				i853StatusErr(http.StatusNotFound, metav1.StatusReasonNotFound, "missing"))
			rec := serve()
			if rec.Code != http.StatusNotFound {
				t.Fatalf("#853/#256 RED: a DELETE-evicted widget answered %d, want 404 — the SPA would no longer see a confirmed delete: %s",
					rec.Code, rec.Body.String())
			}
			if st := i853Body(t, rec); st.Reason != response.StatusReasonNotFound || st.Code != http.StatusNotFound {
				t.Fatalf("want a NotFound Status, got %+v", st)
			}
			if resolves != 1 {
				t.Fatalf("#853/#256 RED: the deleted widget was resolved (%d resolves); a deleted CR must stop at the CR read", resolves)
			}
		})
	}
}

// TestIssue853_424Body_IsAnIdentityFreeStatus_EndToEnd: the /agents/new shape
// on the real chain (apiref.Resolve → restactions.Resolve → the caller's
// apiserver), with the upstream 404 message stuffed with the caller's
// identity. The 424 body is a Failure Status whose message names the stage and
// the missing resource and carries none of it.
func TestIssue853_424Body_IsAnIdentityFreeStatus_EndToEnd(t *testing.T) {
	const group = "g-team-SNT853"
	secret := psAlice + "-clientconfig"
	cn := "CN=" + psAlice + "-SNT853CN"
	ra := &unstructured.Unstructured{Object: in443RA("agent-formdef", i709Filter, i709CRDStage(false), targetStage())}
	f := in443Setup(t, ra)
	f.extra["GET "+i709CRDPath] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"agents.kagent.dev not found for User \"%s\" groups [\"%s\"] via secret %s cert %s"}`,
			psAlice, group, secret, cn)
	}
	ctx := xcontext.BuildContext(context.Background(),
		// The harness's own groups (the RESTAction read is RBAC-checked) plus a
		// sentinel group.
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: psAlice, Groups: append(psGroups(in443Arm), group)}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: f.srv.URL, Token: "tok-" + psAlice}))

	// The RESTAction informer serves the apiRef read, as it does once warm.
	_, raSync := cache.Global().EnsureResourceType(h1RAGVR)
	select {
	case <-raSync:
	case <-time.After(5 * time.Second):
		t.Fatalf("restactions informer did not sync")
	}

	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: i853Widget()}
	})
	r2 := setWidgetsResolveForTest(func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		_, err := apiref.Resolve(ctx, apiref.ResolveOptions{
			RC: &rest.Config{Host: f.srv.URL}, ApiRef: i431ApiRef("agent-formdef"), AuthnNS: psAuthnNS,
		})
		return nil, cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef "+h1NS+"/agent-formdef", err)
	})
	r3 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	defer func() { r3(); r2(); r1() }()
	rec := httptest.NewRecorder()
	(&widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}}).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx))
	i853AssertCopy(t, rec, []string{psAlice, group, "SNT853", secret, cn, "tok-"})
	st := i853Body(t, rec)
	if !strings.Contains(st.Message, `stage "crd"`) || !strings.Contains(st.Message, "agents.kagent.dev") {
		t.Fatalf("the message must name the stage and the missing resource, got %q", st.Message)
	}
	t.Logf("424 message (the SPA subtitle): %q", st.Message)
}

// TestIssue853_424Message_NeverNamesAnIdentity: whatever a dependency's
// message carries, the 424 never shows a requester, a clientconfig Secret,
// or the caller's username or groups; nor is it empty. A message without any
// of them is shown as it is.
func TestIssue853_424Message_NeverNamesAnIdentity(t *testing.T) {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "carol@example.org", Groups: []string{"system:authenticated", "team-blue"}}))
	cases := []struct {
		name, msg string
		shown     bool
	}{
		{"plain_not_found", `restactions.templates.krateo.io "agent-formdef" not found`, true},
		{"caller_username", `widgets "x" not found for carol@example.org`, false},
		{"caller_dns_name", `configmaps "` + kubeutil.MakeDNS1123Compatible("carol@example.org") + `" not found`, false},
		{"caller_group", `rolebindings "team-blue" not found`, false},
		{"clientconfig_secret", `secrets "dave-clientconfig" not found`, false},
		{"apiserver_requester", `widgets is forbidden: User "erin" cannot get`, false},
		{"empty", "", false},
		{"blank", "   ", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			encodeWidgetResolveError(ctx, rec, i853StatusErr(http.StatusNotFound, metav1.StatusReasonNotFound, c.msg))
			if rec.Code != http.StatusFailedDependency {
				t.Fatalf("code=%d, want 424", rec.Code)
			}
			st := i853Body(t, rec)
			want := dependencyFailureFallback
			if c.shown {
				want = c.msg
			}
			if st.Message != want {
				t.Fatalf("message=%q, want %q", st.Message, want)
			}
		})
	}
}

// i853AssertCopy asserts the 424 body is a Failure Status whose message is
// non-empty and carries none of forbidden.
func i853AssertCopy(t *testing.T, rec *httptest.ResponseRecorder, forbidden []string) {
	t.Helper()
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("code=%d, want 424: %s", rec.Code, rec.Body.String())
	}
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("body is not JSON: %v: %s", err, rec.Body.String())
	}
	for k, want := range map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "FailedDependency", "code": float64(424)} {
		if raw[k] != want {
			t.Fatalf("body[%q]=%v, want %v: %s", k, raw[k], want, rec.Body.String())
		}
	}
	msg, _ := raw["message"].(string)
	if strings.TrimSpace(msg) == "" {
		t.Fatalf("the message is empty: %s", rec.Body.String())
	}
	for _, s := range forbidden {
		if strings.Contains(rec.Body.String(), s) {
			t.Fatalf("identity text %q reached the 424 body: %s", s, rec.Body.String())
		}
	}
}
