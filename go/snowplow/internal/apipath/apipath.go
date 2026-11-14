// Package apipath holds the one rule for a value that becomes a segment of an
// apiserver request path (#1257): it is exactly one segment, or it is refused
// before anything is dialled.
//
// Every place a group, version, resource, namespace or name coming from a
// request or a reference turns into a path uses it: the /call path builder,
// the query's GVR parser, objects.Get and the dynamic client. There is no
// second copy of the rule.
//
// The refusal names where the value came from, because the same rule is
// applied to values of different origin: a query parameter of the request, a
// field of an object reference (from CR content, a nested call or a cache
// entry), an option of the dynamic client. An operator reading it must be sent
// to the right place.
package apipath

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Source says what a checked value is, given the name of its field: the words
// that follow "invalid " in the refusal.
type Source func(field string) string

// Query is a query parameter of the request: `"resource" query parameter`.
func Query(field string) string { return fmt.Sprintf("%q query parameter", field) }

// Reference is a field of an object reference: `reference field "resource"`.
func Reference(field string) string { return fmt.Sprintf("reference field %q", field) }

// ClientOption is a part of the resource the dynamic client was asked for:
// `client resource "version"`.
func ClientOption(field string) string { return fmt.Sprintf("client resource %q", field) }

// BadSegmentError is the refusal of a value that is not one apiserver path
// segment. It names what the value is (What, built by a Source) and the rule,
// never the value.
type BadSegmentError struct{ What, Reason string }

func (e *BadSegmentError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.What, e.Reason)
}

// Segment checks that v is exactly one path segment as the apiserver accepts
// one: not empty, never "." or "..", never a "/" or a "%" (which the apiserver
// decodes back into a "/"). what names the value in the error (a Source's
// result).
func Segment(what, v string) error {
	if v == "" {
		return &BadSegmentError{What: what, Reason: "may not be empty"}
	}
	if errs := content.IsPathSegmentName(v); len(errs) > 0 {
		return &BadSegmentError{What: what, Reason: strings.Join(errs, ", ")}
	}
	return nil
}

// CheckGVR checks that gvr's group, version and resource are each one path
// segment. An empty group is the core group; an empty version or resource is
// refused. The group and the version are the two halves of the "apiVersion"
// field, the resource is the "resource" field, of src.
func CheckGVR(gvr schema.GroupVersionResource, src Source) error {
	if gvr.Group != "" {
		if err := Segment(src("apiVersion"), gvr.Group); err != nil {
			return err
		}
	}
	if err := Segment(src("apiVersion"), gvr.Version); err != nil {
		return err
	}
	return Segment(src("resource"), gvr.Resource)
}
