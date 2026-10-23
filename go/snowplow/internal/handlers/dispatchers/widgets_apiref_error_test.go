// widgets_apiref_error_test.go — Task #272 / 0.30.251 dispatcher-side
// regression guard for the apiref error-type preservation fix.
//
// Companion to internal/resolvers/widgets/apiref/resolve_error_test.go
// (which tests the wrapping helper in isolation). This file tests the
// CONSUMER side: the dispatcher's `errors.As(err, *apierrors.StatusError)`
// check at widgets.go:228-234 must recover Code=403 from an
// apiref-wrapped error and emit HTTP 403 instead of HTTP 500.
//
// We do NOT spin the full widgetsHandler.ServeHTTP (which requires
// REST configs, cache, RBAC). Instead, we feed the exact wrapped error
// shape `apiref.Resolve` now returns into the dispatcher's response-
// encoding sub-block via httptest.ResponseRecorder + `response.Encode`,
// asserting the wire shape is correct (HTTP 403, content-type JSON,
// body carries the upstream message).
//
// Pre-fix wire shape (TRACED in
// docs/task-262-s8-cj-tablist-trace-2026-06-09.md §3.3): HTTP 500 +
// generic "...forbidden: ..." string body. Post-fix: HTTP 403 + Status
// body with Code=403 + Reason="Forbidden".
package dispatchers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// runWidgetErrorBlock runs the dispatcher's resolve-error encoding.
// #853 — it used to mirror the widgets.go block; the block is now
// encodeWidgetResolveError, so the test calls production directly and
// cannot drift from it.
func runWidgetErrorBlock(wri http.ResponseWriter, err error) {
	encodeWidgetResolveError(wri, err)
}

// makeApiRefForbidden builds the exact error shape `apiref.Resolve`
// emits post-fix (Task #272): a StatusError-wrapped chain carrying
// the upstream 403. This is the input the dispatcher sees on the
// widgets.Resolve return path under cj's narrow RBAC at S8.
func makeApiRefForbidden() error {
	statusErr := &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status:  response.StatusFailure,
			Message: `restactions.templates.krateo.io "bench-app-01-02-composition-values" is forbidden: User "cyberjoker" cannot get resource "restactions" in API group "templates.krateo.io" in the namespace "bench-ns-01"`,
			Reason:  metav1.StatusReasonForbidden,
			Code:    http.StatusForbidden,
			Details: &metav1.StatusDetails{
				Name: "bench-app-01-02-composition-values",
				Kind: "restactions",
			},
		},
	}
	return fmt.Errorf("apiref resolve %s/%s/%s: %w",
		"templates.krateo.io", "restactions",
		"bench-app-01-02-composition-values", statusErr)
}

// TestDispatcherWidgetError_ForbiddenChainEmits403 — primary
// falsifier. Pre-fix, this would fail (the apiref-stripped error
// would fall through to response.InternalError → HTTP 500).
// Post-fix, the dispatcher's `errors.As` recovers Code=403 and emits
// a wire shape carrying the apiserver's real status.
func TestDispatcherWidgetError_ForbiddenChainEmits403(t *testing.T) {
	wri := httptest.NewRecorder()
	runWidgetErrorBlock(wri, makeApiRefForbidden())

	resp := wri.Result()
	defer resp.Body.Close()

	if got, want := resp.StatusCode, http.StatusForbidden; got != want {
		t.Errorf("HTTP status: got %d, want %d", got, want)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type: got %q, want %q", got, "application/json")
	}

	// Decode the JSON body and assert the inner Status carries
	// Code=403 + Reason=Forbidden. This is the wire-side contract
	// the SPA reads.
	var body response.Status
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if got, want := body.Code, http.StatusForbidden; got != want {
		t.Errorf("body.Code: got %d, want %d", got, want)
	}
	if got, want := string(body.Reason), string(response.StatusReasonForbidden); got != want {
		t.Errorf("body.Reason: got %q, want %q", got, want)
	}
	// The upstream message must still be observable in the body —
	// the SPA / observability stack rely on it to distinguish
	// permission denials from generic failures.
	if body.Message == "" {
		t.Errorf("body.Message is empty; upstream message lost on wire")
	}
}

// TestDispatcherWidgetError_NotFoundChainEmits424 — sister case for
// objects.Get's IsNotFound branch. #853: the widget CR was read, so a
// missing apiRef RESTAction is a failed dependency (424), not a missing
// widget (404, answered only by the CR read).
func TestDispatcherWidgetError_NotFoundChainEmits424(t *testing.T) {
	statusErr := &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status:  response.StatusFailure,
			Message: `restactions.templates.krateo.io "does-not-exist" not found`,
			Reason:  metav1.StatusReasonNotFound,
			Code:    http.StatusNotFound,
		},
	}
	wrapped := fmt.Errorf("apiref resolve %s/%s/%s: %w",
		"templates.krateo.io", "restactions", "does-not-exist", statusErr)

	wri := httptest.NewRecorder()
	runWidgetErrorBlock(wri, wrapped)

	if got, want := wri.Result().StatusCode, http.StatusFailedDependency; got != want {
		t.Errorf("HTTP status: got %d, want %d", got, want)
	}
}

// TestDispatcherWidgetError_PlainErrorStillEmits500 — negative
// control. Errors WITHOUT a StatusError in the chain must still
// emit HTTP 500 (no regression in the response.InternalError
// fallthrough branch).
//
// This guards against an over-broad fix accidentally widening the
// errors.As recovery to non-StatusError errors.
func TestDispatcherWidgetError_PlainErrorStillEmits500(t *testing.T) {
	wri := httptest.NewRecorder()
	runWidgetErrorBlock(wri, fmt.Errorf("plain non-StatusError failure"))

	if got, want := wri.Result().StatusCode, http.StatusInternalServerError; got != want {
		t.Errorf("HTTP status: got %d, want %d (plain-error fallthrough)", got, want)
	}
}
