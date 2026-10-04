//go:build unit
// +build unit

// call_443_kind_test.go — #443 kind-backed arms (design §6), against the real
// apiserver on kind cluster "krateo", each request made with the CALLER's own
// x509 (e2e.SignUp), so the apiserver is the authority on RBAC and on what is
// persisted:
//
//   - TestS443_Kind_DryRun/b_NothingPersisted: a dry-run create returns 200
//     with X-Snowplow-Dry-Run: All, then the object does NOT exist.
//   - TestS443_Kind_DryRun/b_PlainCallDryRunRefused: POST /call?dryRun=All is
//     a 400 and the object does NOT exist. RED on main: the dryRun parameter
//     is dropped, so it is a REAL create and the object exists afterwards.
//   - TestS443_Kind_DryRun/f_Strict: an unknown field under
//     fieldValidation=Strict is the apiserver's rejection naming the field
//     (echoes present, so it is the apiserver's answer, not a snowplow 400);
//     the same body under Ignore is accepted with the field pruned.
//   - TestS443_Kind_Raw/c_AsCaller: raw=true returns the stored RESTAction
//     (no resolver), a caller without get on restactions gets the apiserver's
//     403, and a missing object is the apiserver's 404.
//
// The RESTAction kind is used because the devs group's existing grant
// (testdata/rbac.restactions.yaml) is create/get on restactions.

package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/e2e"
	"github.com/krateo-platformops/snowplow/apis"
	v1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const dryName443 = "dryrun-443"

func raBody443(extraSpec string) string {
	return `{"apiVersion":"templates.krateo.io/v1","kind":"RESTAction","metadata":{"name":"` + dryName443 +
		`","namespace":"demo-system"},"spec":{` + extraSpec + `"api":[{"name":"ns","path":"/api/v1/namespaces"}]}}`
}

const raPostQuery443 = "?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=demo-system&name=" + dryName443

func adminRes443(t *testing.T, cfg *envconf.Config) *resources.Resources {
	t.Helper()
	r, err := resources.New(cfg.Client().RESTConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apis.AddToScheme(r.GetScheme()); err != nil {
		t.Fatal(err)
	}
	return r
}

// assertAbsent443 fails unless the RESTAction dryName443 does NOT exist.
func assertAbsent443(ctx context.Context, t *testing.T, cfg *envconf.Config, why string) {
	t.Helper()
	err := adminRes443(t, cfg).Get(ctx, dryName443, namespace, &v1.RESTAction{})
	if err == nil {
		t.Fatalf("%s: RESTAction %s/%s EXISTS — the request persisted an object", why, namespace, dryName443)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("%s: admin GET: %v", why, err)
	}
}

func cleanup443(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
	ra := &v1.RESTAction{}
	ra.Name, ra.Namespace = dryName443, namespace
	if err := adminRes443(t, cfg).Delete(ctx, ra); err != nil && !apierrors.IsNotFound(err) {
		t.Logf("#443 cleanup: %v", err)
	}
	return ctx
}

func serveKind443(ctx context.Context, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequestWithContext(ctx, method, target, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestS443_Kind_DryRun(t *testing.T) {
	os.Setenv("DEBUG", "0")
	f := features.New("S443-dry-run").
		Setup(e2e.Logger("test")).
		Setup(cleanup443).
		Setup(e2e.SignUp(e2e.SignUpOptions{
			Username: "dryrunner443", Groups: []string{"devs"}, Namespace: namespace,
			JWTSignKey: signKeyPEM282(t), JWTKeyID: "test-kid-443d",
		})).
		Assess("b_NothingPersisted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			rec := serveKind443(ctx, handlers.CallDryRun(), http.MethodPost, "/call/dry-run"+raPostQuery443, raBody443(""))
			if rec.Code != http.StatusOK {
				t.Fatalf("dry-run create: code=%d body=%s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get(handlers.HeaderDryRun) != "All" {
				t.Fatalf("dry-run create: %s echo missing", handlers.HeaderDryRun)
			}
			// The apiserver ran the create (it returns the would-be object)…
			var got map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &got)
			if md, _ := got["metadata"].(map[string]any); md == nil || md["name"] != dryName443 {
				t.Fatalf("dry-run create: reply is not the would-be object: %s", rec.Body.String())
			}
			// …and persisted nothing.
			assertAbsent443(ctx, t, cfg, "after POST /call/dry-run")
			return ctx
		}).
		Assess("b_PlainCallDryRunRefused", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			rec := serveKind443(ctx, handlers.Call(), http.MethodPost, "/call"+raPostQuery443+"&dryRun=All", raBody443(""))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("POST /call?dryRun=All: code=%d, want 400 (body=%s)", rec.Code, rec.Body.String())
			}
			assertAbsent443(ctx, t, cfg, "after POST /call?dryRun=All")
			return ctx
		}).
		Assess("f_Strict", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			rec := serveKind443(ctx, handlers.CallDryRun(), http.MethodPost,
				"/call/dry-run"+raPostQuery443+"&fieldValidation=Strict", raBody443(`"bogusField443":"x",`))
			if rec.Code < 400 || rec.Code >= 500 {
				t.Fatalf("Strict + unknown field: code=%d, want the apiserver's 4xx (body=%s)", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "bogusField443") {
				t.Errorf("Strict rejection does not name the field: %s", rec.Body.String())
			}
			if rec.Header().Get(handlers.HeaderDryRun) != "All" || rec.Header().Get(handlers.HeaderFieldValidation) != "Strict" {
				t.Errorf("Strict rejection: echoes %q/%q, want All/Strict — the rejection must be the apiserver's, not a snowplow 400",
					rec.Header().Get(handlers.HeaderDryRun), rec.Header().Get(handlers.HeaderFieldValidation))
			}
			t.Logf("Strict rejection: code=%d body=%s", rec.Code, strings.TrimSpace(rec.Body.String()))

			// Control: the same body under Ignore is accepted, field pruned.
			rec = serveKind443(ctx, handlers.CallDryRun(), http.MethodPost,
				"/call/dry-run"+raPostQuery443+"&fieldValidation=Ignore", raBody443(`"bogusField443":"x",`))
			if rec.Code != http.StatusOK {
				t.Fatalf("Ignore control: code=%d body=%s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "bogusField443") {
				t.Errorf("Ignore control: the unknown field was not pruned: %s", rec.Body.String())
			}
			assertAbsent443(ctx, t, cfg, "after the Strict/Ignore dry runs")
			return ctx
		}).
		Teardown(cleanup443).
		Feature()
	testenv.Test(t, f)
}

// rawSpy443 is a resolve handler that must never be reached by a raw read.
type rawSpy443 struct{ called bool }

func (s *rawSpy443) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.called = true
	w.WriteHeader(http.StatusTeapot)
}

func TestS443_Kind_Raw(t *testing.T) {
	os.Setenv("DEBUG", "0")
	rawGet := func(ctx context.Context, name string) (*httptest.ResponseRecorder, *rawSpy443) {
		spy := &rawSpy443{}
		h := handlers.Dispatcher(map[string]http.Handler{"restactions.templates.krateo.io": spy})(handlers.Call())
		return serveKind443(ctx, h, http.MethodGet,
			"/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=demo-system&name="+name+"&raw=true", ""), spy
	}
	allowed := features.New("S443-raw-allowed").
		Setup(e2e.Logger("test")).
		Setup(createKubeGetRESTAction).
		Setup(e2e.SignUp(e2e.SignUpOptions{
			Username: "rawreader443", Groups: []string{"devs"}, Namespace: namespace,
			JWTSignKey: signKeyPEM282(t), JWTKeyID: "test-kid-443r",
		})).
		Assess("c_AsCaller_stored_and_404", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			rec, spy := rawGet(ctx, "kube-get")
			if spy.called {
				t.Fatal("raw=true reached the resolver")
			}
			if rec.Code != http.StatusOK || rec.Header().Get(handlers.HeaderRaw) != "true" {
				t.Fatalf("raw GET: code=%d echo=%q body=%s", rec.Code, rec.Header().Get(handlers.HeaderRaw), rec.Body.String())
			}
			var got map[string]any
			_ = json.Unmarshal(rec.Body.Bytes(), &got)
			spec, _ := got["spec"].(map[string]any)
			if got["kind"] != "RESTAction" || spec == nil || spec["api"] == nil {
				t.Fatalf("raw GET did not return the stored RESTAction: %s", rec.Body.String())
			}
			rec, _ = rawGet(ctx, "no-such-ra-443")
			if rec.Code != http.StatusNotFound {
				t.Errorf("raw GET of a missing object: code=%d, want the apiserver's 404", rec.Code)
			}
			return ctx
		}).
		Feature()
	denied := features.New("S443-raw-denied").
		Setup(e2e.Logger("test")).
		Setup(createKubeGetRESTAction).
		Setup(e2e.SignUp(e2e.SignUpOptions{
			Username: "outsider443", Groups: []string{"no-grants-443"}, Namespace: namespace,
			JWTSignKey: signKeyPEM282(t), JWTKeyID: "test-kid-443o",
		})).
		Assess("c_AsCaller_403", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			rec, spy := rawGet(ctx, "kube-get")
			if spy.called {
				t.Fatal("raw=true reached the resolver")
			}
			if rec.Code != http.StatusForbidden {
				t.Errorf("raw GET without get on restactions: code=%d, want the apiserver's 403 (body=%s)", rec.Code, rec.Body.String())
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, allowed, denied)
}
