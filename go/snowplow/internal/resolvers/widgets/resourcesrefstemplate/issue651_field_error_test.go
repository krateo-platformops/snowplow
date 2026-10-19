//go:build unit || integration

package resourcesrefstemplate

// issue651_field_error_test.go — #651 (a) RED-first arms.
//
// THE DEFECT (main 080ac5e6): a resourcesRefsTemplate field expression that
// fails to parse, compile or evaluate was rendered as its (sanitized) error
// TEXT into the ref's id / apiVersion / name / namespace / resource, and the
// garbage ref went on to resourcesrefs.Resolve as if it were real. Nothing
// failed, so the widget resolve succeeded and its Put was accepted. The
// template's iterator, separately, compiled without the module loader, so an
// iterator that includes a shipped module could not compile.
//
// WHAT THE ARMS PIN:
//   - a failing field FAILS its template: Resolve returns an error, no ref is
//     emitted for that template (iterated or not), and a sibling valid template
//     still expands;
//   - the error carries the class only, never the data (#638);
//   - an iterator that includes a shipped module resolves.
// A valid-template control proves the arms are not vacuous.

import (
	"context"
	"strings"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/utils/ptr"
)

const a651Sentinel = "withheld-sentinel-651-zz"

func a651DS() map[string]any {
	return map[string]any{"items": []any{
		map[string]any{"name": "cm-a", "secret": a651Sentinel},
		map[string]any{"name": "cm-b", "secret": a651Sentinel},
	}}
}

func a651Good() templatesv1.ResourceRefTemplate {
	return templatesv1.ResourceRefTemplate{
		Iterator: ptr.To("${ .items }"),
		Template: templatesv1.ResourceRef{
			ID: "${ .name }", Verb: "get", Resource: "configmaps", APIVersion: "v1",
			Name: "${ .name }", Namespace: "ns-651",
		},
	}
}

func TestIssue651a_FailingFieldFailsItsTemplate(t *testing.T) {
	bad := []struct{ name, q string }{
		{"runtime_error", "${ .secret + 1 }"},
		{"parse_error", "${ .name | ( }"},
		{"unknown_module", `${ include "no-such-module-651"; .name }`},
	}
	fields := []string{"id", "apiVersion", "name", "namespace", "resource"}
	for _, b := range bad {
		for _, field := range fields {
			for _, iterated := range []bool{true, false} {
				mode := "single"
				if iterated {
					mode = "iterated"
				}
				t.Run(b.name+"/"+field+"/"+mode, func(t *testing.T) {
					tpl := templatesv1.ResourceRefTemplate{Template: templatesv1.ResourceRef{
						ID: "bad-id", Verb: "get", Resource: "configmaps", APIVersion: "v1",
						Name: "bad-name", Namespace: "ns-651",
					}}
					ds := map[string]any(a651DS())
					if iterated {
						tpl.Iterator = ptr.To("${ .items }")
					} else {
						// The non-iterated mode renders against the whole dict.
						ds["name"], ds["secret"] = "cm-x", a651Sentinel
					}
					switch field {
					case "id":
						tpl.Template.ID = b.q
					case "apiVersion":
						tpl.Template.APIVersion = b.q
					case "name":
						tpl.Template.Name = b.q
					case "namespace":
						tpl.Template.Namespace = b.q
					case "resource":
						tpl.Template.Resource = b.q
					}
					refs, err := Resolve(context.Background(), []templatesv1.ResourceRefTemplate{tpl, a651Good()}, ds)
					if err == nil {
						t.Fatalf("#651 RED: a failing %s expression did not fail its template — refs=%+v", field, refs)
					}
					if strings.Contains(err.Error(), a651Sentinel) {
						t.Errorf("the template error carries response bytes: %v", err)
					}
					for _, r := range refs {
						if r.Name != "cm-a" && r.Name != "cm-b" {
							t.Errorf("a ref from the failing template was emitted: %+v", r)
						}
						for _, v := range []string{r.ID, r.APIVersion, r.Name, r.Namespace, r.Resource} {
							if strings.Contains(v, "jq ") || strings.Contains(v, a651Sentinel) {
								t.Errorf("a ref field carries error text or data: %+v", r)
							}
						}
					}
					if len(refs) != 2 {
						t.Errorf("the sibling valid template did not expand: got %d refs, want 2 (%+v)", len(refs), refs)
					}
				})
			}
		}
	}
}

func TestIssue651a_IteratorIncludesAShippedModule(t *testing.T) {
	tpl := a651Good()
	tpl.Iterator = ptr.To(`${ include "health"; .items }`)
	refs, err := Resolve(context.Background(), []templatesv1.ResourceRefTemplate{tpl}, a651DS())
	if err != nil {
		t.Fatalf("#651 RED: an iterator that includes a shipped module failed: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("got %d refs, want 2: %+v", len(refs), refs)
	}
}

func TestIssue651a_ValidTemplate_Control(t *testing.T) {
	refs, err := Resolve(context.Background(), []templatesv1.ResourceRefTemplate{a651Good()}, a651DS())
	if err != nil {
		t.Fatalf("control errored: %v", err)
	}
	if len(refs) != 2 || refs[0].Name != "cm-a" || refs[1].Name != "cm-b" || refs[0].ID != "cm-a" {
		t.Fatalf("control refs = %+v", refs)
	}
}
