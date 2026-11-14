// Package statusdetails is the structured part of the one answer snowplow gives
// for a type that is not installed (#1414, #1429): which type is missing, in a
// Status body's `details`, for a reader that must not parse the message.
//
// The 424 Failed Dependency for a missing type is not the only 424 snowplow
// answers: a widget whose dependency's OBJECT is missing is one too (#853),
// with the same top-level reason. What tells them apart is here:
//
//	"details": {
//	  "group":  "<group>",            // left out for the core group
//	  "kind":   "<resource>",         // the resource as it was asked for
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
// Only snowplow produces this cause. An upstream's `details` never reach a
// snowplow answer (credhttp.DecodeUpstreamStatus drops them), so a body that
// carries the cause carries it because objects.Get found the type missing.
//
// It is a leaf package: objects builds the details, the two Status → error
// converters and the widget encoder carry them, and the refresher's classifier
// reads them, without any of those importing another.
package statusdetails

import (
	"github.com/krateo-platformops/plumbing/http/response"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CauseTypeNotInstalled is the cause's reason. The one definition.
const CauseTypeNotInstalled = "TypeNotInstalled"

// causeField is the cause's field: the cause's message is the missing type's
// apiVersion.
const causeField = "apiVersion"

// TypeNotInstalled is the details of a Status that says gvr's type is not
// installed, for a plumbing Status (what snowplow writes).
func TypeNotInstalled(gvr schema.GroupVersionResource) *response.StatusDetails {
	return &response.StatusDetails{
		Group: gvr.Group,
		Kind:  gvr.Resource,
		Causes: []response.StatusCause{{
			Type:    CauseTypeNotInstalled,
			Field:   causeField,
			Message: gvr.GroupVersion().String(),
		}},
	}
}

// TypeNotInstalledMeta is TypeNotInstalled for a metav1.Status (what an
// *apierrors.StatusError carries between a read and the answer).
func TypeNotInstalledMeta(gvr schema.GroupVersionResource) *metav1.StatusDetails {
	return &metav1.StatusDetails{
		Group: gvr.Group,
		Kind:  gvr.Resource,
		Causes: []metav1.StatusCause{{
			Type:    CauseTypeNotInstalled,
			Field:   causeField,
			Message: gvr.GroupVersion().String(),
		}},
	}
}

// MissingType reads the type a plumbing Status says is not installed. ok is
// false for any Status that does not carry exactly the shape TypeNotInstalled
// builds: one cause, that reason, that field, a message that parses as the
// apiVersion of details.group, and a resource.
func MissingType(st *response.Status) (gvr schema.GroupVersionResource, ok bool) {
	if st == nil || st.Details == nil || len(st.Details.Causes) != 1 {
		return schema.GroupVersionResource{}, false
	}
	c := st.Details.Causes[0]
	return missingType(st.Details.Group, st.Details.Kind, string(c.Type), c.Field, c.Message)
}

// MissingTypeMeta is MissingType for a metav1.Status.
func MissingTypeMeta(st metav1.Status) (gvr schema.GroupVersionResource, ok bool) {
	if st.Details == nil || len(st.Details.Causes) != 1 {
		return schema.GroupVersionResource{}, false
	}
	c := st.Details.Causes[0]
	return missingType(st.Details.Group, st.Details.Kind, string(c.Type), c.Field, c.Message)
}

func missingType(group, resource, reason, field, apiVersion string) (schema.GroupVersionResource, bool) {
	if reason != CauseTypeNotInstalled || field != causeField || resource == "" {
		return schema.GroupVersionResource{}, false
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil || gv.Version == "" || gv.Group != group {
		return schema.GroupVersionResource{}, false
	}
	return gv.WithResource(resource), true
}
