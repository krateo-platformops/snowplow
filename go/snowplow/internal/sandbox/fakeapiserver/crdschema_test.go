//go:build js && wasm

package fakeapiserver

// M2: a custom resource's writes go through its CRD's schema, with apiextensions-apiserver's own
// pruning, defaulting, OpenAPI validation and CEL rules, and fieldValidation as the apiserver
// applies it.

import (
	"fmt"
	"strings"
	"testing"
)

// withIncidentSchema gives the incidents CRD a real structural schema.
func withIncidentSchema(objs []map[string]any) {
	schemaJSON := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"spec": map[string]any{
				"type":     "object",
				"required": []any{"applied"},
				"properties": map[string]any{
					"applied": map[string]any{"type": "boolean"},
					"note":    map[string]any{"type": "string"},
					"level":   map[string]any{"type": "string", "enum": []any{"low", "high"}, "default": "low"},
				},
				"x-kubernetes-validations": []any{map[string]any{"rule": "self.applied || !has(self.note)", "message": "a note needs applied"}},
			},
			"status": map[string]any{"type": "object", "x-kubernetes-preserve-unknown-fields": true},
		},
	}
	for _, o := range objs {
		if o["kind"] == "CustomResourceDefinition" && get(o, "metadata", "name") == "incidents.observability.krateo.io" {
			for _, v := range get(o, "spec", "versions").([]any) {
				v.(map[string]any)["schema"] = map[string]any{"openAPIV3Schema": schemaJSON}
			}
		}
		if o["kind"] == "Incident" {
			o["spec"] = map[string]any{"applied": false}
		}
	}
}

func schemaHarness(t *testing.T) *harness { return newHarness(t, opts{pageEdit: withIncidentSchema}) }

func patchSpec(h *harness, query string, spec map[string]any) resp {
	return h.do("PATCH", inc+"/"+incident+query, req{as: "visitor", ct: merge, body: map[string]any{"spec": spec}})
}

func TestCRDSchema_UnknownFieldsPerFieldValidation(t *testing.T) {
	h := schemaHarness(t)
	warn := patchSpec(h, "", map[string]any{"applied": true, "unknown": 1})
	eq(t, warn.code, 200, "Warn (the default)")
	eq(t, get(warn.body, "spec", "unknown"), nil, "pruned")
	eq(t, warn.hdr.Get("Warning"), `299 - "unknown field \"spec.unknown\""`, "the warning")
	strict := patchSpec(h, "?fieldValidation=Strict", map[string]any{"applied": true, "other": 1})
	eq(t, strict.code, 400, "Strict")
	eq(t, strict.body["message"], `strict decoding error: unknown field "spec.other"`, "message")
	ignore := patchSpec(h, "?fieldValidation=Ignore", map[string]any{"applied": true, "other": 1})
	eq(t, ignore.code, 200, "Ignore")
	eq(t, ignore.hdr.Get("Warning"), "", "no warning")
	eq(t, get(ignore.body, "spec", "other"), nil, "still pruned")
	eq(t, patchSpec(h, "?fieldValidation=Loud", map[string]any{"applied": true}).code, 422, "an unsupported fieldValidation")
}

func TestCRDSchema_Defaulting(t *testing.T) {
	h := schemaHarness(t)
	r := patchSpec(h, "", map[string]any{"applied": true})
	eq(t, get(r.body, "spec", "level"), "low", "the schema default is applied")
}

func TestCRDSchema_ValidationIs422(t *testing.T) {
	h := schemaHarness(t)
	for _, c := range []struct {
		name string
		spec map[string]any
		want string
	}{
		{"type", map[string]any{"applied": "yes"}, `spec.applied: Invalid value: "string": spec.applied in body must be of type boolean: "string"`},
		{"enum", map[string]any{"applied": true, "level": "mid"}, `spec.level: Unsupported value: "mid": supported values: "low", "high"`},
		{"CEL", map[string]any{"applied": false, "note": "x"}, `a note needs applied`},
	} {
		r := patchSpec(h, "", c.spec)
		eq(t, r.code, 422, c.name)
		eq(t, r.body["reason"], "Invalid", c.name)
		msg := fmt.Sprint(r.body["message"])
		if !strings.HasPrefix(msg, `Incident.observability.krateo.io "`+incident+`" is invalid: `) || !strings.Contains(msg, c.want) {
			t.Fatalf("%s: message %q, want it to contain %q", c.name, msg, c.want)
		}
	}
	rm := h.do("PATCH", inc+"/"+incident, req{as: "visitor", ct: "application/json-patch+json", body: []any{map[string]any{"op": "remove", "path": "/spec/applied"}}})
	eq(t, rm.code, 422, "required")
	if !strings.Contains(fmt.Sprint(rm.body["message"]), "spec.applied: Required value") {
		t.Fatalf("required: %v", rm.body["message"])
	}
}

func TestCRDSchema_ARefusedWriteStoresNothing(t *testing.T) {
	h := schemaHarness(t)
	before := incidentRV(h)
	patchSpec(h, "", map[string]any{"applied": "yes"})
	eq(t, incidentRV(h), before, "a 422 commits nothing")
}
