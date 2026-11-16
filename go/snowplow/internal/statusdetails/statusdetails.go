// Package statusdetails is the one answer snowplow gives for a type that is not
// installed (#1414, #1429): a 424 Failed Dependency whose body says which type
// is missing, for a reader that must not parse the message.
//
// The 424 for a missing type is not the only 424 snowplow answers: a widget
// whose dependency's OBJECT is missing is one too (#853), with the same
// top-level reason. What tells them apart is the body's details:
//
//	"details": {
//	  "group":  "<group>",            // left out for the core group
//	  "kind":   "<resource>",         // the resource as the reference gave it
//	  "causes": [{
//	    "reason":  "TypeNotInstalled",
//	    "field":   "apiVersion",
//	    "message": "<group>/<version>" // "v1" for the core group
//	  }]
//	}
//
// For this cause the `message` is a value with exactly that grammar (an
// apiVersion), not prose, and exactly one such cause is present. There is no
// object name, namespace or identity in any of it.
//
// ONLY SNOWPLOW'S OWN DECISION PRODUCES IT, and that is enforced by a Go type,
// not by looking at a body. objects.Get creates a *TypeNotInstalledError when
// the apiserver's discovery says the type is not served, and nothing else in
// the module creates one (typenotinstalled_fence_test.go). The widget encoder
// and the refresher's classifier recognise the answer by errors.As on that
// type. A Status that merely looks like it, such as one client-go built from
// an upstream's body with the same details, is an ordinary error: it is never
// given the reason or the details.
//
// It is a leaf package: objects creates the error, the Status → error
// converters carry it, the widget encoder writes it and the refresher's
// classifier reads it, without any of those importing another.
package statusdetails

import (
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CauseTypeNotInstalled is the cause's reason. The one definition.
const CauseTypeNotInstalled = "TypeNotInstalled"

// ReasonFailedDependency is the Status reason of a 424 answer. Neither plumbing
// nor apimachinery names one for 424.
const ReasonFailedDependency response.StatusReason = "FailedDependency"

// TypeNotInstalledError is snowplow's own decision that a read's type is not
// installed on the cluster. Created only by objects.Get.
//
// It unwraps to an *apierrors.StatusError (424, FailedDependency, the
// message, and NO details), so every consumer that maps a status error to an
// HTTP code or a class keeps working; the ones that give the type-not-installed
// answer look for this type instead.
type TypeNotInstalledError struct {
	// GVR is the missing type, as the read's reference gave it.
	GVR schema.GroupVersionResource
	// Message is the reader-facing sentence. It names the type and nothing
	// else: no object, no namespace, no identity.
	Message string
}

func (e *TypeNotInstalledError) Error() string { return e.Message }

// Unwrap is the error as a plain apiserver status.
func (e *TypeNotInstalledError) Unwrap() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusFailedDependency,
		Reason:  metav1.StatusReason(ReasonFailedDependency),
		Message: e.Message,
	}}
}

// Status is the answer on the wire: the one place its body is built.
func (e *TypeNotInstalledError) Status() *response.Status {
	st := response.New(http.StatusFailedDependency, nil)
	st.Status = response.StatusFailure
	st.Reason = ReasonFailedDependency
	st.Message = e.Message
	st.Details = &response.StatusDetails{
		Group: e.GVR.Group,
		Kind:  e.GVR.Resource,
		Causes: []response.StatusCause{{
			Type:    CauseTypeNotInstalled,
			Field:   "apiVersion",
			Message: e.GVR.GroupVersion().String(),
		}},
	}
	return st
}
