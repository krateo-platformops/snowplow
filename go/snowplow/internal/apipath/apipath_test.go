package apipath

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The refusal names the field and where it came from, one row per source, and
// never the value.
func TestCheckGVR_TheRefusalNamesTheFieldAndItsSource(t *testing.T) {
	const escaping = "../../../../../api/v1/namespaces/other/configmaps"
	for _, c := range []struct {
		name string
		src  Source
		gvr  schema.GroupVersionResource
		want string
	}{
		{"query_resource", Query, schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: escaping},
			`invalid "resource" query parameter: may not contain '/'`},
		{"query_version", Query, schema.GroupVersionResource{Group: "g.example.org", Version: "..", Resource: "things"},
			`invalid "apiVersion" query parameter: may not be '..'`},
		{"query_group", Query, schema.GroupVersionResource{Group: "..", Version: "v1", Resource: "things"},
			`invalid "apiVersion" query parameter: may not be '..'`},
		{"query_empty_resource", Query, schema.GroupVersionResource{Version: "v1"},
			`invalid "resource" query parameter: may not be empty`},
		{"reference_resource", Reference, schema.GroupVersionResource{Group: "g.example.org", Version: "v1", Resource: escaping},
			`invalid reference field "resource": may not contain '/'`},
		{"reference_version", Reference, schema.GroupVersionResource{Group: "g.example.org", Version: "..", Resource: "things"},
			`invalid reference field "apiVersion": may not be '..'`},
		{"reference_percent", Reference, schema.GroupVersionResource{Version: "v1", Resource: "pods%2Fp"},
			`invalid reference field "resource": may not contain '%'`},
		{"client_resource", ClientOption, schema.GroupVersionResource{Version: "v1", Resource: "pods/p"},
			`invalid client resource "resource": may not contain '/'`},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := CheckGVR(c.gvr, c.src)
			var bad *BadSegmentError
			if !errors.As(err, &bad) {
				t.Fatalf("want a BadSegmentError, got %T: %v", err, err)
			}
			if err.Error() != c.want {
				t.Errorf("refusal = %q, want %q", err.Error(), c.want)
			}
			for _, part := range []string{"configmaps", "other", "pods", "g.example.org", "things"} {
				if strings.Contains(err.Error(), part) {
					t.Errorf("the refusal carries the value (%q): %q", part, err.Error())
				}
			}
		})
	}
}

func TestCheckGVR_AcceptsOneSegmentEach(t *testing.T) {
	for _, gvr := range []schema.GroupVersionResource{
		{Version: "v1", Resource: "configmaps"}, // the core group has no name
		{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"},
		{Group: "g.example.org", Version: "v1", Resource: "Thing"},
	} {
		if err := CheckGVR(gvr, Reference); err != nil {
			t.Errorf("%s: refused: %v", gvr, err)
		}
	}
}
