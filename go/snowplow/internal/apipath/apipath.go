// Package apipath holds the one rule for a value that becomes a segment of an
// apiserver request path (#1257): it is exactly one segment, or it is refused
// before anything is dialled.
//
// Every place a group, version, resource, namespace or name coming from a
// request or a reference turns into a path uses it: the /call path builder,
// the query's GVR parser, objects.Get and the dynamic client. There is no
// second copy of the rule.
package apipath

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// BadSegmentError is the refusal of a value that is not one apiserver path
// segment. It names the parameter and the rule, never the value.
type BadSegmentError struct{ Param, Reason string }

func (e *BadSegmentError) Error() string {
	return fmt.Sprintf("invalid %q query parameter: %s", e.Param, e.Reason)
}

// Segment checks that v is exactly one path segment as the apiserver accepts
// one: not empty, never "." or "..", never a "/" or a "%" (which the apiserver
// decodes back into a "/"). param names the value in the error.
func Segment(param, v string) error {
	if v == "" {
		return &BadSegmentError{Param: param, Reason: "may not be empty"}
	}
	if errs := content.IsPathSegmentName(v); len(errs) > 0 {
		return &BadSegmentError{Param: param, Reason: strings.Join(errs, ", ")}
	}
	return nil
}

// CheckGVR checks that gvr's group, version and resource are each one path
// segment. An empty group is the core group; an empty version or resource is
// refused.
func CheckGVR(gvr schema.GroupVersionResource) error {
	if gvr.Group != "" {
		if err := Segment("apiVersion", gvr.Group); err != nil {
			return err
		}
	}
	if err := Segment("apiVersion", gvr.Version); err != nil {
		return err
	}
	return Segment("resource", gvr.Resource)
}
