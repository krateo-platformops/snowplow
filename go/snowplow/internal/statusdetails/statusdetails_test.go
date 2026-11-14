package statusdetails

import (
	"encoding/json"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The wire shape, pinned as literals: this is what a reader is written against.
func TestTypeNotInstalled_WireShape(t *testing.T) {
	for _, c := range []struct {
		name string
		gvr  schema.GroupVersionResource
		want string
	}{
		{"named_group", schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"},
			`{"group":"widgets.templates.krateo.io","kind":"flexes","causes":[{"reason":"TypeNotInstalled","message":"widgets.templates.krateo.io/v1beta1","field":"apiVersion"}]}`},
		// The core group has no name: `group` is left out and the apiVersion is the version alone.
		{"core_group", schema.GroupVersionResource{Version: "v1", Resource: "things"},
			`{"kind":"things","causes":[{"reason":"TypeNotInstalled","message":"v1","field":"apiVersion"}]}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			ours, err := json.Marshal(TypeNotInstalled(c.gvr))
			if err != nil {
				t.Fatal(err)
			}
			meta, err := json.Marshal(TypeNotInstalledMeta(c.gvr))
			if err != nil {
				t.Fatal(err)
			}
			if string(ours) != c.want || string(meta) != c.want {
				t.Fatalf("details\n plumbing: %s\n metav1:   %s\n want:     %s", ours, meta, c.want)
			}
			// And each reads back to the type it was built from.
			if got, ok := MissingType(&response.Status{Details: TypeNotInstalled(c.gvr)}); !ok || got != c.gvr {
				t.Errorf("MissingType = %v, %v; want %v", got, ok, c.gvr)
			}
			if got, ok := MissingTypeMeta(metav1.Status{Details: TypeNotInstalledMeta(c.gvr)}); !ok || got != c.gvr {
				t.Errorf("MissingTypeMeta = %v, %v; want %v", got, ok, c.gvr)
			}
		})
	}
}

// The cause is recognised only in the exact shape TypeNotInstalled builds.
func TestMissingType_OnlyTheExactShape(t *testing.T) {
	good := metav1.StatusCause{Type: CauseTypeNotInstalled, Field: "apiVersion", Message: "g.example.org/v1"}
	for name, d := range map[string]*metav1.StatusDetails{
		"nil":                           nil,
		"no_causes":                     {Group: "g.example.org", Kind: "things"},
		"another_reason":                {Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{{Type: "FieldValueInvalid", Field: "apiVersion", Message: "g.example.org/v1"}}},
		"another_field":                 {Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{{Type: CauseTypeNotInstalled, Field: "kind", Message: "g.example.org/v1"}}},
		"two_causes":                    {Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{good, good}},
		"no_resource":                   {Group: "g.example.org", Causes: []metav1.StatusCause{good}},
		"apiVersion_of_another_group":   {Group: "other.example.org", Kind: "things", Causes: []metav1.StatusCause{good}},
		"core_details_group_apiVersion": {Kind: "things", Causes: []metav1.StatusCause{good}},
		"empty_apiVersion":              {Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{{Type: CauseTypeNotInstalled, Field: "apiVersion"}}},
		"prose":                         {Group: "g.example.org", Kind: "things", Causes: []metav1.StatusCause{{Type: CauseTypeNotInstalled, Field: "apiVersion", Message: "not/there/at all"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if gvr, ok := MissingTypeMeta(metav1.Status{Details: d}); ok {
				t.Fatalf("recognised %v from %+v", gvr, d)
			}
		})
	}
	if _, ok := MissingType(nil); ok {
		t.Fatal("recognised a nil Status")
	}
}
