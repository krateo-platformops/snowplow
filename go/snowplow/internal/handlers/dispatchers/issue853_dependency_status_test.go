package dispatchers

// issue853_dependency_status_test.go — #853: a widget /call answers 404 only
// when the widget CR itself is missing. When the CR exists and a dependency
// (the apiRef RESTAction, one of its stages, a nested resolve) is not found,
// the answer is 424 Failed Dependency with the dependency's message, which
// the SPA renders as the widget's error state without retrying.
//
// Driven through the real widgetsHandler.ServeHTTP on the #443 harness
// (cache ON, a real watcher), with the CR read and the resolve swapped at
// their seams.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func i853Widget() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version, "kind": "Panel",
		"metadata": map[string]any{"name": "agent-create", "namespace": h1NS},
		"spec":     map[string]any{"apiRef": map[string]any{"name": "agent-formdef", "namespace": h1NS}},
	}}
}

// i853StatusErr is a typed apiserver failure as the resolve chain carries it.
func i853StatusErr(code int, reason metav1.StatusReason, msg string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Code: int32(code), Reason: reason, Message: msg,
	}}
}

// i853Serve runs one widget /call with the CR read answering got and the
// resolve answering resolve; it returns the response and how many times the
// resolve ran.
func i853Serve(t *testing.T, got objects.Result, resolve func() (*widgets.Widget, error)) (*httptest.ResponseRecorder, int) {
	t.Helper()
	f := in443Setup(t)
	calls := 0
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result { return got })
	r2 := setWidgetsResolveForTest(func(context.Context, widgets.ResolveOptions) (*widgets.Widget, error) {
		calls++
		return resolve()
	})
	r3 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	defer func() { r3(); r2(); r1() }()
	rec := httptest.NewRecorder()
	(&widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}}).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(in443Ctx(f, psAlice)))
	return rec, calls
}

func i853Body(t *testing.T, rec *httptest.ResponseRecorder) response.Status {
	t.Helper()
	var st response.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("response is not a Status: %v: %s", err, rec.Body.String())
	}
	return st
}

func i853AssertFailedDependency(t *testing.T, rec *httptest.ResponseRecorder, wantMsg string) {
	t.Helper()
	if rec.Code != http.StatusFailedDependency {
		t.Fatalf("#853 RED: code=%d, want 424: %s", rec.Code, rec.Body.String())
	}
	st := i853Body(t, rec)
	if st.Code != http.StatusFailedDependency || st.Status != response.StatusFailure ||
		st.Reason != reasonFailedDependency || st.Kind != "Status" {
		t.Fatalf("want a Failure Status with reason FailedDependency and code 424, got %+v", st)
	}
	if !strings.Contains(st.Message, wantMsg) {
		t.Fatalf("the message must carry the dependency's own message %q, got %q", wantMsg, st.Message)
	}
}

// The widget CR is missing: 404, and nothing is resolved.
func TestIssue853_WidgetCRMissing_Is404(t *testing.T) {
	got := objects.Result{Err: response.New(http.StatusNotFound,
		fmt.Errorf(`panels.widgets.templates.krateo.io "agent-create" not found`))}
	rec, calls := i853Serve(t, got, func() (*widgets.Widget, error) {
		t.Fatal("the resolve ran for a widget whose CR is missing")
		return nil, nil
	})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("#853 RED: a missing widget CR answered %d, want 404: %s", rec.Code, rec.Body.String())
	}
	if st := i853Body(t, rec); st.Reason != response.StatusReasonNotFound {
		t.Fatalf("want reason NotFound, got %+v", st)
	}
	if calls != 0 {
		t.Fatalf("resolve ran %d times", calls)
	}
}

// The CR exists; its apiRef RESTAction is missing (the apiref frame's typed
// 404, wrapped by widgets.Resolve exactly as resolve.go wraps it).
func TestIssue853_ApiRefMissing_Is424(t *testing.T) {
	const msg = `restactions.templates.krateo.io "agent-formdef" not found`
	rec, _ := i853Serve(t, objects.Result{GVR: h1WidgetGVR, Unstructured: i853Widget()}, func() (*widgets.Widget, error) {
		return nil, cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef "+h1NS+"/agent-formdef",
			fmt.Errorf("apiref resolve: %w", i853StatusErr(http.StatusNotFound, metav1.StatusReasonNotFound, msg)))
	})
	i853AssertFailedDependency(t, rec, msg)
}

// The CR exists; a nested resolve (a resourcesRefs child) is gone.
func TestIssue853_NestedResolveGone_Is424(t *testing.T) {
	const msg = `panels.widgets.templates.krateo.io "child" is gone`
	rec, _ := i853Serve(t, objects.Result{GVR: h1WidgetGVR, Unstructured: i853Widget()}, func() (*widgets.Widget, error) {
		return nil, cache.WrapResolveStage(cache.ResolveStageResourcesRefs, "resolve resourcesRefs",
			i853StatusErr(http.StatusGone, metav1.StatusReasonGone, msg))
	})
	i853AssertFailedDependency(t, rec, msg)
}

// A failed dependency is not cached, and a later request resolves again. On
// the h1 harness a clean resolve of the same widget Puts a cell under the
// dispatch key and the next request is served from it (the control arm), so
// "no cell" below is a real absence, not a harness that never caches.
func TestIssue853_FailureIsNotCached(t *testing.T) {
	cr := h1WidgetUnstructured(map[string]any{})
	serve := func(t *testing.T, resolveErr error) (int, int, bool) {
		h1BuildWatcher(t)
		reqCtx := h1ReqCtx(h1User)
		key, handle, _ := a548WidgetKey(reqCtx, cr, nil)
		if handle == nil || key == "" {
			t.Fatalf("PRECONDITION: live widgets key/handle expected")
		}
		calls := 0
		restore := installWidgetFakes(t, cr, func() bool { return true },
			func(context.Context, widgets.ResolveOptions) (*widgets.Widget, error) {
				calls++
				if resolveErr != nil {
					return nil, resolveErr
				}
				return h1WidgetUnstructured(map[string]any{}), nil
			})
		defer restore()
		code := 0
		for i := 0; i < 2; i++ {
			rec := httptest.NewRecorder()
			Widgets().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(reqCtx))
			code = rec.Code
		}
		_, cached := handle.Get(key)
		return code, calls, cached
	}

	t.Run("control_clean_resolve_is_cached", func(t *testing.T) {
		code, calls, cached := serve(t, nil)
		if code != http.StatusOK || !cached || calls != 1 {
			t.Fatalf("PRECONDITION: a clean resolve must be cached and serve the second request: code=%d cached=%v resolves=%d", code, cached, calls)
		}
	})
	t.Run("failed_dependency_is_not_cached", func(t *testing.T) {
		code, calls, cached := serve(t, cache.WrapResolveStage(cache.ResolveStageApiRef, "resolve apiRef",
			i853StatusErr(http.StatusNotFound, metav1.StatusReasonNotFound, "missing")))
		if code != http.StatusFailedDependency {
			t.Fatalf("code=%d, want 424", code)
		}
		if cached || calls != 2 {
			t.Fatalf("#853: a failed dependency was cached (cell present=%v, resolves=%d, want no cell and 2 resolves)", cached, calls)
		}
	})
}

// Only "not here" codes from a dependency become 424. Every other code keeps
// its meaning for the SPA: 401 resumes the session, 403 is the calm forbidden
// state, 429/5xx are retried, 503/504 show the timeout state, 400 is a status
// that fails its schema, and a non-Status error stays 500.
func TestIssue853_OtherDependencyCodesPassThrough(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"NotFound", i853StatusErr(http.StatusNotFound, metav1.StatusReasonNotFound, "m"), http.StatusFailedDependency},
		{"Gone", i853StatusErr(http.StatusGone, metav1.StatusReasonGone, "m"), http.StatusFailedDependency},
		{"Unauthorized", i853StatusErr(http.StatusUnauthorized, metav1.StatusReasonUnauthorized, "m"), http.StatusUnauthorized},
		{"Forbidden", i853StatusErr(http.StatusForbidden, metav1.StatusReasonForbidden, "m"), http.StatusForbidden},
		{"BadRequest", i853StatusErr(http.StatusBadRequest, metav1.StatusReasonBadRequest, "m"), http.StatusBadRequest},
		{"Conflict", i853StatusErr(http.StatusConflict, metav1.StatusReasonConflict, "m"), http.StatusConflict},
		{"TooManyRequests", i853StatusErr(http.StatusTooManyRequests, metav1.StatusReasonTooManyRequests, "m"), http.StatusTooManyRequests},
		{"InternalError", i853StatusErr(http.StatusInternalServerError, metav1.StatusReasonInternalError, "m"), http.StatusInternalServerError},
		{"ServiceUnavailable", i853StatusErr(http.StatusServiceUnavailable, metav1.StatusReasonServiceUnavailable, "m"), http.StatusServiceUnavailable},
		{"GatewayTimeout", i853StatusErr(http.StatusGatewayTimeout, metav1.StatusReasonTimeout, "m"), http.StatusGatewayTimeout},
		{"PlainError", errors.New("plain"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			encodeWidgetResolveError(rec, fmt.Errorf("resolve apiRef x/y: %w", c.err))
			if rec.Code != c.want {
				t.Fatalf("code=%d, want %d: %s", rec.Code, c.want, rec.Body.String())
			}
		})
	}
}
