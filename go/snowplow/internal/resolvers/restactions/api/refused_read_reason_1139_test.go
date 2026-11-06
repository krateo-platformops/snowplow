package api

// #1139 — an iterator read whose refused items have different causes: the one
// WARN line names the same reason the outcome header does (the highest-ranked
// one, reasonRank), whatever order the items come in, and keeps
// refused_items. Before, the WARN named the first refused item's reason.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

func TestRefusedRead1139_WarnNamesTheHeadersReason(t *testing.T) {
	// ns 1 is a jq error in the path (string + number): TemplateError.
	// ns "" collapses the namespace segment: InvalidPath, which outranks it.
	for _, tc := range []struct {
		name string
		nss  []any
	}{
		{"template_error_first", []any{float64(1), "", "team-a"}},
		{"invalid_path_first", []any{"", float64(1), "team-a"}},
		{"template_error_last", []any{"team-a", "", float64(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newX1106Srv(t, false, x1106OK)
			x1106Env(t, api.srv.URL, map[string]map[string]string{})
			it := &templates.API{Name: "each", Verb: ptr.To(http.MethodGet), ErrorKey: ptr.To("eachErr"), ContinueOnError: ptr.To(true),
				DependsOn: &templates.Dependency{Name: "", Iterator: ptr.To(`[ .nss[] | {ns: .} ]`)},
				Path:      `${ "/api/v1/namespaces/" + .ns + "/configmaps/cm" }`}
			o := x1106Resolve(t, map[string]any{"nss": tc.nss}, it)

			// PRE: the two refusals are recorded with their own reasons.
			errs, _ := o.dict["eachErr"].([]any)
			reasons := map[any]int{}
			for _, e := range errs {
				if m, ok := e.(map[string]any); ok {
					reasons[m["reason"]]++
				}
			}
			if len(errs) != 2 || reasons[StageReasonTemplateError] != 1 || reasons[StageReasonInvalidPath] != 1 {
				t.Fatalf("PRE: one TemplateError and one InvalidPath refusal under the errorKey, got %v", errs)
			}
			x1106WantReason(t, o.outcomes, "each", StageReasonInvalidPath)

			var warn map[string]any
			n := 0
			for _, l := range strings.Split(o.logs, "\n") {
				if strings.Contains(l, `"msg":"read stage not sent"`) {
					n++
					if err := json.Unmarshal([]byte(l), &warn); err != nil {
						t.Fatalf("PRE: the WARN is JSON: %v", err)
					}
				}
			}
			if n != 1 {
				t.Fatalf("PRE: exactly one 'read stage not sent' WARN, got %d:\n%s", n, o.logs)
			}
			if warn["reason"] != StageReasonInvalidPath {
				t.Errorf("RED: the WARN names %v, the outcome header %s (%s)", warn["reason"], StageReasonInvalidPath, o.outcomes)
			}
			if rc, _ := warn["refused_items"].(float64); rc != 2 {
				t.Errorf("the WARN must keep refused_items=2, got %v", warn["refused_items"])
			}
		})
	}
}
