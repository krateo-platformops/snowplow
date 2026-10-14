package dispatchers

// issue709_stage_upstream_status_test.go — #709: a stage whose upstream answers
// 404 must not become `500 unexpected end of JSON input`.
//
// Driven through the REAL chain on the #443 ps harness (cache ON, a real
// watcher, the requester's own clientconfig pointing at a fake apiserver): the
// RESTAction /call handler → restactions.Resolve → api.Resolve → the per-user
// external fetch, and for the widget shape the real apiref.Resolve with the
// RESTAction informer-served.
//
// The #709 shape is reproduced from portal-agents' restaction.agent-formdef:
// the FIRST stage `crd` GETs the agents.kagent.dev CRD with NO continueOnError,
// and the top-level filter binds `(.crd.spec.versions[]? | …) as $spec` — on an
// install without kagent the CRD is absent, the stage records the 404, and the
// filter yields no value.
//
//	A1  stored RESTAction /call: 404 NotFound naming the CRD (was 500).
//	A2  the widget apiRef frame (apiref.Resolve) returns a typed 404, and the
//	    widget /call handler answers 404 with it (was 500 "resolve apiRef …:
//	    unexpected end of JSON input").
//	A3  continueOnError: the stage's 404 is recorded under errorKey and the
//	    later stage still ran — 200, unchanged.
//	A4  an empty 2xx body is "no content": the stage's value is null, no stage
//	    error, 200.
//	A5  a non-2xx with an EMPTY body keeps its status (404, not 500), and a
//	    non-2xx JSON body that is not a Status keeps its code too.
//	A6  a filter that yields no value with no failed stage: status null, 200.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

const (
	i709CRDPath = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/agents.kagent.dev"
	// The agent-formdef filter's first binding, verbatim in shape.
	i709Filter = `(.crd.spec.versions[]? | select(.name == "v1alpha2") | .schema.openAPIV3Schema.properties.spec) as $spec
	  | { schemaString: ($spec | tojson), namespaces: (.target // null) }`
)

// i709Serve404 makes the fake apiserver answer the CRD GET the way the
// apiserver does when the CRD does not exist.
func i709Serve404(f *in443API) {
	f.extra["GET "+i709CRDPath] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",`+
			`"message":"customresourcedefinitions.apiextensions.k8s.io \"agents.kagent.dev\" not found",`+
			`"reason":"NotFound","details":{"name":"agents.kagent.dev","group":"apiextensions.k8s.io","kind":"customresourcedefinitions"},"code":404}`)
	}
}

func i709CRDStage(continueOnError bool) map[string]any {
	s := map[string]any{"name": "crd", "verb": "GET", "path": i709CRDPath}
	if continueOnError {
		s["continueOnError"] = true
		s["errorKey"] = "crdError"
	}
	return s
}

// i709Status decodes a Status response body.
func i709Status(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("response is not JSON: %v: %s", err, rec.Body.String())
	}
	return m
}

func i709AssertNotFound(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	body := rec.Body.String()
	if strings.Contains(body, "unexpected end of JSON input") {
		t.Fatalf("#709 RED: the JSON decoder's error reached the response: %d %s", rec.Code, body)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("#709 RED: code=%d, want 404: %s", rec.Code, body)
	}
	st := i709Status(t, rec)
	if st["reason"] != "NotFound" || st["kind"] != "Status" {
		t.Fatalf("want a NotFound Status, got %s", body)
	}
	msg, _ := st["message"].(string)
	if !strings.Contains(msg, `"agents.kagent.dev" not found`) || !strings.Contains(msg, `stage "crd"`) {
		t.Fatalf("the message must name the stage and the missing resource, got %q", msg)
	}
}

func TestIssue709_A1_StoredRESTAction_StageGet404_NoContinueOnError_Is404(t *testing.T) {
	f := in443Setup(t)
	i709Serve404(f)
	rec := serveStored(t, f, in443Ctx(f, psAlice),
		in443RA("agent-formdef", i709Filter, i709CRDStage(false), targetStage()))
	i709AssertNotFound(t, rec)
}

func TestIssue709_A2_WidgetApiRef_StageGet404_Is404(t *testing.T) {
	ra := &unstructured.Unstructured{Object: in443RA("agent-formdef", i709Filter, i709CRDStage(false), targetStage())}
	f := in443Setup(t, ra)
	i709Serve404(f)
	alice := in443Ctx(f, psAlice)

	// The apiRef frame itself (the "resolve apiRef …" error in the issue).
	_, err := apiref.Resolve(alice, apiref.ResolveOptions{
		RC: &rest.Config{Host: f.srv.URL}, ApiRef: i431ApiRef("agent-formdef"), AuthnNS: psAuthnNS,
	})
	if err == nil {
		t.Fatal("#709: apiref.Resolve returned no error for a RESTAction whose required stage 404ed")
	}
	if strings.Contains(err.Error(), "unexpected end of JSON input") {
		t.Fatalf("#709 RED: %v", err)
	}
	var se *apierrors.StatusError
	if !errors.As(err, &se) || !apierrors.IsNotFound(err) {
		t.Fatalf("#709 RED: want a typed 404 NotFound, got %T: %v", err, err)
	}

	// The widget /call handler answers with it. widgets.Resolve wraps an apiRef
	// error exactly like this (resolve.go: WrapResolveStage + "resolve apiRef").
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version, "kind": "Panel",
			"metadata": map[string]any{"name": "agent-create", "namespace": h1NS},
			"spec":     map[string]any{"apiRef": map[string]any{"name": "agent-formdef", "namespace": h1NS}},
		}}}
	})
	r2 := setWidgetsResolveForTest(func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		_, err := apiref.Resolve(ctx, apiref.ResolveOptions{
			RC: &rest.Config{Host: f.srv.URL}, ApiRef: i431ApiRef("agent-formdef"), AuthnNS: psAuthnNS,
		})
		return nil, cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef "+h1NS+"/agent-formdef", err)
	})
	// The caller may read the widget itself; only the apiRef stage fails.
	r3 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	defer func() { r3(); r2(); r1() }()
	rec := httptest.NewRecorder()
	(&widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}}).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(alice))
	i709AssertNotFound(t, rec)
}

func TestIssue709_A3_ContinueOnError_RecordsTheStageErrorAndContinues(t *testing.T) {
	f := in443Setup(t)
	i709Serve404(f)
	rec := serveStored(t, f, in443Ctx(f, psAlice),
		in443RA("agent-formdef", `{crdError: .crdError, target: .target}`, i709CRDStage(true), targetStage()))
	if rec.Code != http.StatusOK {
		t.Fatalf("continueOnError: code=%d, want 200: %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Status struct {
			CrdError []map[string]any `json:"crdError"`
			Target   map[string]any   `json:"target"`
		} `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Status.CrdError) != 1 || out.Status.CrdError[0]["reason"] != "NotFound" || out.Status.CrdError[0]["code"] != float64(404) {
		t.Fatalf("the stage's 404 must be recorded under errorKey, got %s", rec.Body.String())
	}
	if out.Status.Target == nil {
		t.Fatalf("resolution must continue past a continueOnError failure, got %s", rec.Body.String())
	}

	// A filter that yields no value under continueOnError is not a failure:
	// status null, 200.
	rec = serveStored(t, f, in443Ctx(f, psAlice), in443RA("agent-formdef", i709Filter, i709CRDStage(true), targetStage()))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":null`) {
		t.Fatalf("zero-yield filter after a continueOnError failure: want 200 status null, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestIssue709_A4_Empty2xxBody_IsNoContent(t *testing.T) {
	f := in443Setup(t)
	f.extra["GET /apis/apiextensions.k8s.io/v1/customresourcedefinitions/empties.example.io"] = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}
	rec := serveStored(t, f, in443Ctx(f, psAlice), in443RA("empty-body", `{v: .empty, err: .error}`,
		map[string]any{"name": "empty", "verb": "GET", "path": "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/empties.example.io"}))
	if rec.Code != http.StatusOK {
		t.Fatalf("empty 2xx body: code=%d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":{"err":null,"v":null}`) {
		t.Fatalf("empty 2xx body: want the stage value null and no stage error, got %s", rec.Body.String())
	}
}

func TestIssue709_A5_NonOKWithoutAStatusBody_KeepsTheCode(t *testing.T) {
	t.Run("EmptyBody", func(t *testing.T) {
		f := in443Setup(t)
		f.extra["GET "+i709CRDPath] = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound) // no body at all
		}
		rec := serveStored(t, f, in443Ctx(f, psAlice), in443RA("agent-formdef", i709Filter, i709CRDStage(false)))
		i709AssertNotFound(t, rec)
		// What the stage recorded under errorKey says what the upstream did.
		rec = serveStored(t, f, in443Ctx(f, psAlice), in443RA("agent-formdef", `{e: .crdError}`, i709CRDStage(true)))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "upstream answered 404 Not Found with an empty body") ||
			!strings.Contains(rec.Body.String(), `"code":404`) {
			t.Fatalf("empty-body 404 errorKey: got %d %s", rec.Code, rec.Body.String())
		}
	})
	t.Run("JSONBodyThatIsNotAStatus", func(t *testing.T) {
		f := in443Setup(t)
		f.extra["GET "+i709CRDPath] = func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `{"error":"no such thing"}`)
		}
		rec := serveStored(t, f, in443Ctx(f, psAlice), in443RA("agent-formdef", i709Filter, i709CRDStage(false)))
		i709AssertNotFound(t, rec)
		// Under continueOnError the upstream body is recorded, and as a failure.
		rec = serveStored(t, f, in443Ctx(f, psAlice), in443RA("agent-formdef", `{e: .crdError}`, i709CRDStage(true)))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "no such thing") ||
			!strings.Contains(rec.Body.String(), `"code":404`) || !strings.Contains(rec.Body.String(), `"status":"Failure"`) {
			t.Fatalf("non-Status JSON 404 errorKey: got %d %s", rec.Code, rec.Body.String())
		}
	})
}

func TestIssue709_A6_ZeroYieldFilter_NoFailedStage_IsNull(t *testing.T) {
	f := in443Setup(t)
	rec := serveStored(t, f, in443Ctx(f, psAlice), in443RA("zero-yield", `.target | empty`, targetStage()))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":null`) {
		t.Fatalf("zero-yield filter: want 200 status null, got %d %s", rec.Code, rec.Body.String())
	}
}

// A7 — the error answer carries NO identity text for ANY status. The upstream
// is asked with the requester's own clientconfig and echoes who asked (user,
// groups, the clientconfig Secret's name, a certificate CN) in its message —
// for a 404 Status, a 403 without the Forbidden reason (not #681 F2), a 401
// and a 409. The answer keeps the upstream code and names only the request's
// own coordinates.
func TestIssue709_A7_ErrorAnswerCarriesNoIdentity_AnyStatus(t *testing.T) {
	const group = "g-team-SNT709"
	secret := psAlice + "-clientconfig"
	cn := "CN=" + psAlice + "-SNT709CN"
	cases := []struct {
		code       int
		reason     string
		wantReason string
	}{
		{http.StatusNotFound, "NotFound", "NotFound"},
		{http.StatusForbidden, "", "Forbidden"},
		{http.StatusUnauthorized, "Unauthorized", "Unauthorized"},
		{http.StatusConflict, "Conflict", "Conflict"},
	}
	for _, c := range cases {
		t.Run(http.StatusText(c.code), func(t *testing.T) {
			f := in443Setup(t)
			f.extra["GET "+i709CRDPath] = func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(c.code)
				msg := fmt.Sprintf(`agents.kagent.dev is not here for User \"%s\" groups [\"%s\"] via secret %s cert %s`, psAlice, group, secret, cn)
				if c.reason == "" {
					_, _ = fmt.Fprintf(w, `{"message":"%s"}`, msg)
					return
				}
				_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":%q,"code":%d,"message":"%s"}`, c.reason, c.code, msg)
			}
			ctx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: psAlice, Groups: []string{group}}),
				xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: f.srv.URL, Token: "tok-" + psAlice}))
			rec := serveStored(t, f, ctx, in443RA("agent-formdef", i709Filter, i709CRDStage(false)))
			body := rec.Body.String()
			if rec.Code != c.code {
				t.Fatalf("code=%d, want %d: %s", rec.Code, c.code, body)
			}
			for _, snt := range []string{psAlice, group, "SNT709", secret, cn} {
				if strings.Contains(body, snt) {
					t.Fatalf("identity text %q from the upstream reached the error answer: %s", snt, body)
				}
			}
			st := i709Status(t, rec)
			msg, _ := st["message"].(string)
			if st["reason"] != c.wantReason || !strings.Contains(msg, `stage "crd"`) || !strings.Contains(msg, "agents.kagent.dev") {
				t.Fatalf("want reason %s naming the stage and the resource, got %s", c.wantReason, body)
			}
		})
	}
}
