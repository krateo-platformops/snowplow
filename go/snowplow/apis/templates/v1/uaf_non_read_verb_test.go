// uaf_non_read_verb_test.go — #1281: API.UserAccessFilterOnNonReadVerb is the
// CRD's first stage rule in Go, verb for verb.
//
// The resolver refuses a stage the predicate reports (restactions/api
// uaf_non_read.go). If the predicate and the rule disagree, either the
// resolver refuses a stage admission accepts (a live RESTAction breaks) or it
// sends one admission rejects (the ServiceAccount write #1281 closes). So the
// oracle is the apiserver's own CEL validator over the COMMITTED generated CRD
// (validateCEL, uaf_verb_cel_test.go), never a copy of the rule.

package v1

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"
)

// TestUAFNonReadVerb_MatchesTheCRDRule drives every verb through admission and
// through the predicate: the predicate reports exactly the stages the rule
// rejects.
func TestUAFNonReadVerb_MatchesTheCRDRule(t *testing.T) {
	verbs := []*string{
		nil, ptr.To(""),
		ptr.To("GET"), ptr.To("get"), ptr.To("Get"),
		ptr.To("HEAD"), ptr.To("head"), ptr.To("Head"),
		ptr.To("POST"), ptr.To("post"), ptr.To("PUT"), ptr.To("put"),
		ptr.To("PATCH"), ptr.To("patch"), ptr.To("DELETE"), ptr.To("delete"),
		ptr.To("OPTIONS"), ptr.To("TRACE"), ptr.To("CONNECT"),
		ptr.To(" GET"), ptr.To("GET "), ptr.To("LIST"),
	}
	rejected := 0
	for _, v := range verbs {
		label := "<absent>"
		if v != nil {
			label = "\"" + *v + "\""
		}
		obj := uafStageWithVerb(v)
		admissionRejects := strings.Contains(validateCEL(t, obj), "userAccessFilter is only allowed on read-verb HTTP stages")
		api := &API{Name: "s", Verb: v, UserAccessFilter: &UserAccessFilterSpec{Verb: "list", Resource: "namespaces"}}
		if got := api.UserAccessFilterOnNonReadVerb(); got != admissionRejects {
			t.Errorf("verb %s: UserAccessFilterOnNonReadVerb() = %v, the CRD rule rejects = %v", label, got, admissionRejects)
		}
		if admissionRejects {
			rejected++
		}
	}
	// Non-exercise guard: the corpus must reach both sides of the rule.
	if rejected == 0 || rejected == len(verbs) {
		t.Fatalf("the verb corpus reached one side of the rule only (%d of %d rejected): the arm proves nothing", rejected, len(verbs))
	}
}

// TestUAFNonReadVerb_NoFilterNoRefusal — a stage without userAccessFilter is
// never reported, whatever its verb; nil is guarded.
func TestUAFNonReadVerb_NoFilterNoRefusal(t *testing.T) {
	for _, v := range []string{"POST", "PUT", "PATCH", "DELETE", "GET"} {
		if (&API{Name: "s", Verb: ptr.To(v)}).UserAccessFilterOnNonReadVerb() {
			t.Errorf("verb %s without userAccessFilter reported", v)
		}
	}
	var nilAPI *API
	if nilAPI.UserAccessFilterOnNonReadVerb() {
		t.Error("nil API reported")
	}
}

// uafStageWithVerb is a RESTAction whose one stage carries a well-formed
// userAccessFilter (so only the verb rule can reject it) and verb v (absent
// when nil).
func uafStageWithVerb(v *string) map[string]any {
	stage := map[string]any{
		"name": "s",
		"path": "/api/v1/namespaces",
		"userAccessFilter": map[string]any{
			"verb": "list", "group": "", "resource": "namespaces", "namespaceFrom": ".metadata.name",
		},
	}
	if v != nil {
		stage["verb"] = *v
	}
	return map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": "uaf-1281", "namespace": "krateo-system"},
		"spec":       map[string]any{"api": []any{stage}},
	}
}
