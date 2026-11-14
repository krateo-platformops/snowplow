// issue1426_resolve_error_branches_test.go — #1426 F4: encodeWidgetResolveError
// has two branches that both answer 424 Failed Dependency, and nothing told
// them apart.
//
//   - a dependency's 404 or 410 (an OBJECT the widget reads is missing, #853):
//     the message is the dependency's, shown only when it carries no identity,
//     else the fixed sentence;
//   - a dependency's own 424 (its TYPE is not installed, #1414): the message
//     was built by objects.Get from the type's coordinates alone, and is kept
//     as it is.
//
// The same text through each branch gives a different body, which is what this
// pins: routing a 424 through the 404/410 branch would run the identity filter
// over a message that is not an upstream's.
package dispatchers

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/statusdetails"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue1426_TheTwo424BranchesAreNotTheSameBranch(t *testing.T) {
	// A type whose resource is spelled like the caller's username: text the
	// identity filter of the 404/410 branch refuses to show.
	msg := "The type " + h1User + " (g.example.org/v1) is not installed on this cluster."
	missingType := schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: h1User}
	status := func(code int32, reason metav1.StatusReason) error {
		st := metav1.Status{Status: metav1.StatusFailure, Code: code, Reason: reason, Message: msg}
		if code == http.StatusFailedDependency {
			st.Details = statusdetails.TypeNotInstalledMeta(missingType) // objects.Get's cause (#1429)
		}
		return fmt.Errorf("apiref resolve g.example.org/x/y: %w", &apierrors.StatusError{ErrStatus: st})
	}

	// PRECONDITION: the filter does refuse this text on the object-missing branch.
	rec := httptest.NewRecorder()
	encodeWidgetResolveError(h1ReqCtx(h1User), rec, status(http.StatusNotFound, metav1.StatusReasonNotFound))
	missing := i1414Status(t, rec)
	if rec.Code != http.StatusFailedDependency || missing.Message != dependencyFailureFallback {
		t.Fatalf("precondition: a dependency's 404 carrying the caller's name must answer 424 with the fixed sentence; code=%d message=%q", rec.Code, missing.Message)
	}
	if missing.Details != nil {
		t.Fatalf("the object-missing branch must carry no details, got %+v", missing.Details)
	}

	// The type-not-installed branch keeps its message.
	rec = httptest.NewRecorder()
	encodeWidgetResolveError(h1ReqCtx(h1User), rec, status(http.StatusFailedDependency, "FailedDependency"))
	notInstalled := i1414Status(t, rec)
	if rec.Code != http.StatusFailedDependency || notInstalled.Reason != "FailedDependency" || notInstalled.Status != metav1.StatusFailure {
		t.Fatalf("want 424 Failure/FailedDependency; code=%d status=%q reason=%q", rec.Code, notInstalled.Status, notInstalled.Reason)
	}
	if notInstalled.Message != msg {
		t.Fatalf("a dependency's type-not-installed 424 went through the object-missing branch:\n got  %q\n want %q", notInstalled.Message, msg)
	}
	if got, ok := statusdetails.MissingTypeMeta(notInstalled); !ok || got != missingType {
		t.Fatalf("the type-not-installed branch must name the type in its details, got %v (ok=%v); body: %s", got, ok, rec.Body.String())
	}
}
