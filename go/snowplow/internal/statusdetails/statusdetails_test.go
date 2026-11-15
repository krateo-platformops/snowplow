package statusdetails

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The wire body, pinned as literals: this is what a reader is written against.
func TestTypeNotInstalledError_WireBody(t *testing.T) {
	for _, c := range []struct {
		name string
		gvr  schema.GroupVersionResource
		want string
	}{
		{"named_group", schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"},
			`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"m","reason":"FailedDependency","details":{"group":"widgets.templates.krateo.io","kind":"flexes","causes":[{"reason":"TypeNotInstalled","message":"widgets.templates.krateo.io/v1beta1","field":"apiVersion"}]},"code":424}`},
		// The core group has no name: `group` is left out and the apiVersion is the version alone.
		{"core_group", schema.GroupVersionResource{Version: "v1", Resource: "things"},
			`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"m","reason":"FailedDependency","details":{"kind":"things","causes":[{"reason":"TypeNotInstalled","message":"v1","field":"apiVersion"}]},"code":424}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := json.Marshal((&TypeNotInstalledError{GVR: c.gvr, Message: "m"}).Status())
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != c.want {
				t.Fatalf("body\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

// As a plain status error it is a 424 with the reason and the message, and NO
// details: nothing downstream can read the type off a status.
func TestTypeNotInstalledError_UnwrapsToAStatusWithoutDetails(t *testing.T) {
	e := &TypeNotInstalledError{GVR: schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: "things"}, Message: "m"}
	wrapped := fmt.Errorf("re-fetch things/x: %w", e)

	var se *apierrors.StatusError
	if !errors.As(wrapped, &se) {
		t.Fatalf("not a status error")
	}
	want := metav1.Status{Status: metav1.StatusFailure, Code: 424, Reason: "FailedDependency", Message: "m"}
	if se.ErrStatus.Details != nil || se.ErrStatus.Code != want.Code || se.ErrStatus.Reason != want.Reason ||
		se.ErrStatus.Message != want.Message || se.ErrStatus.Status != want.Status {
		t.Fatalf("status = %+v, want %+v", se.ErrStatus, want)
	}
	var got *TypeNotInstalledError
	if !errors.As(wrapped, &got) || got != e {
		t.Fatalf("the type is lost through a %%w wrap")
	}
	// A status that only looks like it is not it.
	lookalike := &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: 424, Reason: "FailedDependency", Message: "m",
		Details: &metav1.StatusDetails{Group: "g.example.org", Kind: "things",
			Causes: []metav1.StatusCause{{Type: CauseTypeNotInstalled, Field: "apiVersion", Message: "g.example.org/v1"}}}}}
	if errors.As(error(lookalike), &got) {
		t.Fatalf("a status with the same details was taken for snowplow's own decision")
	}
}
