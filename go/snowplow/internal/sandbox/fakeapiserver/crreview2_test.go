//go:build js && wasm

package fakeapiserver

// #1372 re-review at 4c739cd5, the LOWs: L-a the name a create's 422 carries, L-b the summary a
// decoding 400 carries, L-c the kind-changing PUT (R12) and the CEL-breaking create (R15).

import (
	"strings"
	"testing"
)

// L-a: a create's 422 names the object by its body's metadata.name, as k8s.io/apiserver v0.36.5
// pkg/registry/rest/create.go:130 does (the URL of a create has no name).
func TestCRCreate_InvalidNamesTheObject(t *testing.T) {
	h := crHarness(t)
	cr := h.do("POST", inc, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident", "metadata": map[string]any{"name": "fresh"}, "spec": map[string]any{"level": "mid"}}})
	eq(t, cr.code, 422, "an invalid create")
	if !strings.HasPrefix(msg(cr), `Incident.observability.krateo.io "fresh" is invalid: `) {
		t.Fatalf("the 422 names the object by its metadata.name: %q", msg(cr))
	}
}

// L-b: a body that does not decode, and names no kind, is 400 with upstream's trailing summary of
// the body (pkg/endpoints/handlers/rest.go:253, summarizeData: hex unless it starts with "{", cut at
// 30 bytes with " ...").
func TestDecodeError_SummarisesTheBody(t *testing.T) {
	h := crHarness(t)
	cm := h.do("POST", "/api/v1/namespaces/krateo-system/configmaps", req{as: "sa", ct: "application/json", body: "not json"})
	eq(t, cm.code, 400, "a built-in body that is not JSON")
	if m := msg(cm); !strings.HasPrefix(m, "the object provided is unrecognized (must be of type ConfigMap): ") || !strings.HasSuffix(m, " (6e6f74206a736f6e)") {
		t.Fatalf("built-in: %q", m)
	}
	cr := h.do("POST", inc, req{as: "sa", ct: "application/json", body: "[1,2]"})
	eq(t, cr.code, 400, "a custom resource body that is not an object")
	if m := msg(cr); !strings.HasPrefix(m, "the object provided is unrecognized (must be of type Incident): ") || !strings.HasSuffix(m, " (5b312c325d)") {
		t.Fatalf("custom resource: %q", m)
	}
	long := h.do("POST", inc, req{as: "sa", ct: "application/json", body: `{"metadata":{"name":"x"},"spec":` + strings.Repeat(" ", 40)})
	eq(t, long.code, 400, "a truncated object")
	if m := msg(long); !strings.HasSuffix(m, ` ({"metadata":{"name":"x"},"spec ...)`) {
		t.Fatalf("an object body over 30 bytes is cut at 30 with \" ...\": %q", m)
	}
}

// L-c, R12: a PUT that changes the object's kind is 422 (validator.go ValidateTypeMeta), and nothing
// is stored.
func TestCRUpdate_PutChangingKindIs422(t *testing.T) {
	h := crHarness(t)
	cur := h.do("GET", inc+"/"+incident, req{as: "sa"})
	before := rv(cur)
	body := cur.body
	body["kind"] = "Other"
	r := h.do("PUT", inc+"/"+incident, req{as: "sa", ct: "application/json", body: body})
	answers(t, r, 422, `kind: Invalid value: "Other": must be Incident`, "a PUT changing the kind")
	eq(t, incidentRV(h), before, "a 422 commits nothing")
}

// L-c, R15: a create that breaks a CEL rule is 422 (strategy.go Validate runs CEL), and nothing is
// stored.
func TestCRCreate_CELRuleIs422(t *testing.T) {
	h := crHarness(t)
	cr := h.do("POST", inc, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident", "metadata": map[string]any{"name": "cel"}, "spec": map[string]any{"level": "low", "count": 11}}})
	answers(t, cr, 422, "count under 10", "a create that breaks the CEL rule")
	eq(t, h.do("GET", inc+"/cel", req{as: "sa"}).code, 404, "nothing is stored")
}
