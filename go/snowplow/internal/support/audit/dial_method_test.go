package audit

// dial_method_test.go — #1379 review F1: the falsifier every handler arm runs
// (DialMethodMismatch) says no to a record that names the method of the
// request snowplow served instead of the one it sent, and yes to every verb's
// own method.

import (
	"net/http"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"go.opentelemetry.io/otel/attribute"
)

func TestDialMethod_F1_TheMethodIsTheVerbs(t *testing.T) {
	rec := func(method, verb string) []attribute.KeyValue {
		return []attribute.KeyValue{
			attribute.String(krateoaudit.KeyHTTPMethod, method),
			attribute.String(krateoaudit.KeyVerb, verb),
		}
	}
	for _, c := range []struct {
		method, verb string
		mismatch     bool
	}{
		{http.MethodGet, "get", false},
		{http.MethodGet, "list", false},
		{http.MethodPost, "create", false},
		{http.MethodPut, "update", false},
		{http.MethodPatch, "patch", false},
		{http.MethodDelete, "delete", false},
		{http.MethodDelete, "deletecollection", false},
		{http.MethodPost, "post", false},
		{"_OTHER", "get", false}, // not judged
		// The served method on an export's record: the defect F1 names.
		{http.MethodPost, "get", true},
		{http.MethodGet, "create", true},
		{http.MethodPut, "delete", true},
	} {
		if _, _, got := DialMethodMismatch(rec(c.method, c.verb)); got != c.mismatch {
			t.Errorf("DialMethodMismatch(%s, %s) = %t, want %t", c.method, c.verb, got, c.mismatch)
		}
	}
	// Every verb the schema lists that names a method maps back to one the
	// schema lists: the falsifier has the vocabulary to say yes.
	k, _ := krateoaudit.LookupKey(krateoaudit.KeyVerb)
	m, _ := krateoaudit.LookupKey(krateoaudit.KeyHTTPMethod)
	for _, v := range k.Values {
		method := MethodOfVerb(v)
		if method == "" {
			t.Errorf("verb %q maps to no method", v)
			continue
		}
		found := false
		for _, mv := range m.Values {
			found = found || mv == method
		}
		if !found {
			t.Errorf("verb %q maps to %q, which http.request.method does not list", v, method)
		}
	}
}
