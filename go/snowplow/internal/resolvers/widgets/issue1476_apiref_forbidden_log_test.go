package widgets

// issue1476_apiref_forbidden_log_test.go — #1476: the widget's "unable to
// resolve api reference" line. A Forbidden for the CALLER's identity on its
// own credential (the apiRef RESTAction read, driven through the real
// apiref.Resolve and objects.Get against a fake apiserver) is an expected
// per-identity deny: WARN once in the widget cell's generation, then DEBUG.
// The ServiceAccount's own 403 is a fault: ERROR once, then DEBUG.

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

type rec1476 struct {
	mu     sync.Mutex
	levels []slog.Level
}

func (r *rec1476) Enabled(context.Context, slog.Level) bool { return true }
func (r *rec1476) Handle(_ context.Context, rec slog.Record) error {
	if rec.Message == "unable to resolve api reference" {
		r.mu.Lock()
		r.levels = append(r.levels, rec.Level)
		r.mu.Unlock()
	}
	return nil
}
func (r *rec1476) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *rec1476) WithGroup(string) slog.Handler      { return r }

func (r *rec1476) byLevel() map[slog.Level]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	by := map[slog.Level]int{}
	for _, l := range r.levels {
		by[l]++
	}
	return by
}

// apiserver1476 answers discovery for templates.krateo.io/v1 and 403s the
// apiRef RESTAction read.
func apiserver1476(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
		case "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"templates.krateo.io",` +
				`"versions":[{"groupVersion":"templates.krateo.io/v1","version":"v1"}],` +
				`"preferredVersion":{"groupVersion":"templates.krateo.io/v1","version":"v1"}}]}`))
		case "/apis/templates.krateo.io/v1":
			_, _ = w.Write([]byte(`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"templates.krateo.io/v1","resources":[` +
				`{"name":"restactions","singularName":"restaction","namespaced":true,"kind":"RESTAction","verbs":["get","list"]}]}`))
		case "/apis/templates.krateo.io/v1/namespaces/ns1476/restactions/nav":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,` +
				`"message":"restactions.templates.krateo.io \"nav\" is forbidden"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func widget1476() *Widget {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Menu",
		"metadata":   map[string]any{"name": "nav", "namespace": "ns1476"},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": "nav", "namespace": "ns1476"},
		},
	}}
}

func TestIssue1476_WidgetApiRefForbidden_LevelByWhoseCredential(t *testing.T) {
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	t.Cleanup(api.SetFailureLogResidentForTest(func(string) bool { return true }))
	srv := apiserver1476(t)

	for _, c := range []struct {
		name  string
		ctx   func(context.Context) context.Context
		first slog.Level
	}{
		// The caller's own credential: the 403 is the caller's.
		{"the_callers_own_credential", func(ctx context.Context) context.Context { return ctx }, slog.LevelWarn},
		// snowplow's ServiceAccount credential, no identity to re-gate for: the
		// 403 is snowplow's own fault.
		{"the_service_accounts_own_credential", func(ctx context.Context) context.Context {
			return cache.WithInternalRESTConfig(ctx, &rest.Config{Host: srv.URL})
		}, slog.LevelError},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := &rec1476{}
			const n = 3
			for i := 0; i < n; i++ {
				ctx := xcontext.BuildContext(context.Background(),
					xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "t"}),
					xcontext.WithLogger(slog.New(rec)))
				ctx = cache.WithL1KeyContext(c.ctx(ctx), "widget-cell-1476-"+c.name)
				_, err := Resolve(ctx, ResolveOptions{In: widget1476(), RC: &rest.Config{Host: srv.URL}})
				if !apierrors.IsForbidden(err) {
					t.Fatalf("PRECONDITION: the apiRef read must fail with the apiserver's 403, got %v", err)
				}
			}
			by := rec.byLevel()
			if by[c.first] != 1 || by[slog.LevelDebug] != n-1 || len(by) != 2 {
				t.Fatalf("want 1 %v then %d DEBUG, got %v", c.first, n-1, by)
			}
		})
	}
}
