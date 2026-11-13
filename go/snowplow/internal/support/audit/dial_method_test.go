package audit

// dial_method_test.go — #1379 review F1: the falsifier every handler arm runs
// (DialMethodMismatch) says no to a record that names the method of the
// request snowplow served instead of the one it sent, and yes to every verb's
// own method.

import (
	"net/http"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"go.opentelemetry.io/otel/attribute"
)

// The emitter's own check (checkDialMethod, run on every record) can say yes:
// a record whose method is not its verb's moves the counter and reaches the
// test guard; one whose method is its verb's does neither.
func TestDialMethod_F1_TheEmitterChecksEveryRecord(t *testing.T) {
	var caught []string
	SetDialMethodGuardForTest(func(method, verb string) { caught = append(caught, method+" "+verb) })
	t.Cleanup(func() {
		SetDialMethodGuardForTest(func(method, verb string) {
			panic("F1 falsifier: a record names http.request.method " + method + ", not the method of its verb " + verb)
		})
	})
	rec := audittest.NewRecorder(t)
	e := New(rec.Logger(Scope))
	before := DialMethodMismatchCount()
	ctx := actorCtx()
	e.Emit(ctx, Event{Action: "export", HTTPMethod: http.MethodGet, Verb: "get", Outcome: OutcomeSuccess, ServerAddress: "x.test"})
	if len(caught) != 0 || DialMethodMismatchCount() != before {
		t.Fatalf("a record whose method is its verb's was judged a mismatch: %v", caught)
	}
	e.Emit(ctx, Event{Action: "export", HTTPMethod: http.MethodPost, Verb: "get", Outcome: OutcomeSuccess, ServerAddress: "x.test"})
	if len(caught) != 1 || caught[0] != "POST get" {
		t.Errorf("the guard saw %v, want [POST get]", caught)
	}
	if d := DialMethodMismatchCount() - before; d != 1 {
		t.Errorf("%s moved by %d, want 1", DialMethodMismatchVar, d)
	}
	if n := len(rec.Records()); n != 2 {
		t.Errorf("%d records, want 2: the check never drops one", n)
	}
}

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
