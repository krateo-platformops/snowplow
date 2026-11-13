//go:build js && wasm

package fakeapiserver

// #1372 re-review (274ab837): a custom resource's update check is upstream's whole one
// (apiextensions-apiserver v0.36.5 pkg/registry/customresource strategy.go, status_strategy.go,
// validator.go), and fieldValidation applies to built-in kinds through the apiserver's own strict
// decoding. One test per upstream behaviour; each would fail on the head the review read.

import (
	"fmt"
	"strings"
	"testing"
)

var crSAAll = []map[string]any{
	{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "t-all"}, "rules": []any{map[string]any{"apiGroups": []any{"*"}, "resources": []any{"*"}, "verbs": []any{"*"}}}},
	{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": map[string]any{"name": "t-all"}, "roleRef": map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "t-all"}, "subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "snowplow", "namespace": "krateo-system"}}},
}

// crUpdateSchema: an enum, an integer under a CEL rule, a list-type map and set, an embedded
// resource and a typed status. The stored incident holds a spec.level, a spec.count (against its
// CEL rule) and a status.state the schema no longer allows (captured before the schema tightened),
// and duplicate tags.
func crUpdateSchema(objs []map[string]any) {
	sch := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"spec": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"applied": map[string]any{"type": "boolean"},
					"level":   map[string]any{"type": "string", "enum": []any{"low", "high"}},
					"count":   map[string]any{"type": "integer", "minimum": 0, "x-kubernetes-validations": []any{map[string]any{"rule": "self < 10", "message": "count under 10"}}},
					"items": map[string]any{"type": "array", "x-kubernetes-list-type": "map", "x-kubernetes-list-map-keys": []any{"name"},
						"items": map[string]any{"type": "object", "required": []any{"name"}, "properties": map[string]any{"name": map[string]any{"type": "string"}, "v": map[string]any{"type": "string"}}}},
					"tags":     map[string]any{"type": "array", "x-kubernetes-list-type": "set", "items": map[string]any{"type": "string"}},
					"template": map[string]any{"type": "object", "x-kubernetes-embedded-resource": true, "x-kubernetes-preserve-unknown-fields": true},
				},
			},
			"status": map[string]any{"type": "object", "properties": map[string]any{"state": map[string]any{"type": "string", "enum": []any{"open", "closed"}}, "note": map[string]any{"type": "string"}}},
		},
	}
	for _, o := range objs {
		if o["kind"] == "CustomResourceDefinition" && get(o, "metadata", "name") == "incidents.observability.krateo.io" {
			for _, v := range get(o, "spec", "versions").([]any) {
				v.(map[string]any)["schema"] = map[string]any{"openAPIV3Schema": sch}
			}
		}
		if o["kind"] == "Incident" {
			o["spec"] = map[string]any{"applied": false, "level": "mid", "count": 12, "tags": []any{"x", "x"}}
			o["status"] = map[string]any{"state": "weird"}
		}
	}
}

// withStoredTemplate also stores an embedded resource without its kind, which no update of the
// main resource can pass (the embedded-resource check is not ratcheted).
func withStoredTemplate(objs []map[string]any) {
	crUpdateSchema(objs)
	for _, o := range objs {
		if o["kind"] == "Incident" {
			o["spec"].(map[string]any)["template"] = map[string]any{"apiVersion": "v1"}
		}
	}
}

func crHarness(t *testing.T) *harness {
	return newHarness(t, opts{pageEdit: crUpdateSchema, extra: crSAAll})
}

func crPatch(h *harness, path, query string, body map[string]any) resp {
	return h.do("PATCH", inc+"/"+incident+path+query, req{as: "sa", ct: merge, body: body})
}

func msg(r resp) string { return fmt.Sprint(r.body["message"]) }

func answers(t *testing.T, r resp, code int, want, what string) {
	t.Helper()
	if r.code != code || !strings.Contains(msg(r), want) {
		t.Fatalf("%s: %d %q, want %d containing %q", what, r.code, msg(r), code, want)
	}
}

// Ratcheting: an update leaves stored invalid fields unchanged (spec.level against its enum,
// spec.count against its CEL rule), so they do not fail it
// (validation.WithRatcheting, cel.WithRatcheting); changing one to another invalid value does.
func TestCRUpdate_Ratcheting(t *testing.T) {
	h := crHarness(t)
	eq(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"applied": true}}).code, 200, "an unrelated change while spec.level is stale-invalid")
	answers(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"level": "mid2"}}), 422, `spec.level: Unsupported value: "mid2"`, "a changed invalid value")
	cr := h.do("POST", inc, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident", "metadata": map[string]any{"name": "fresh"}, "spec": map[string]any{"level": "mid"}}})
	answers(t, cr, 422, `spec.level: Unsupported value: "mid"`, "a create is never ratcheted")
}

// A /status write validates the status schema only (status_strategy.go ValidateUpdate,
// validator.go ValidateStatusUpdate), ratcheted from status.
func TestCRUpdate_StatusValidatesTheStatusSchemaOnly(t *testing.T) {
	h := crHarness(t)
	eq(t, crPatch(h, "/status", "", map[string]any{"status": map[string]any{"note": "seen"}}).code, 200, "status ratcheting: the stored status.state is left unchanged")
	ok := crPatch(h, "/status", "", map[string]any{"status": map[string]any{"state": "open"}})
	eq(t, ok.code, 200, "a status write while the spec is stale-invalid")
	eq(t, get(ok.body, "status", "state"), "open", "stored")
	answers(t, crPatch(h, "/status", "", map[string]any{"status": map[string]any{"state": "bogus"}}), 422, `status.state: Unsupported value: "bogus"`, "an invalid status")
	// The main resource keeps the stored status, so its own write is not judged on one.
	eq(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"applied": true}, "status": map[string]any{"state": "bogus"}}).code, 200, "status on the main resource is ignored")
}

// A /status write runs the status strategy only: none of the main resource's checks, so a stored
// spec that fails one (an embedded resource without its kind) does not fail it.
func TestCRUpdate_StatusWriteRunsNoMainResourceCheck(t *testing.T) {
	h := newHarness(t, opts{pageEdit: withStoredTemplate, extra: crSAAll})
	eq(t, crPatch(h, "/status", "", map[string]any{"status": map[string]any{"state": "open"}}).code, 200, "a status write")
	answers(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"applied": true}}), 422, "spec.template.kind: Required value", "a main-resource write")
}

// x-kubernetes-list-type map and set (structurallisttype.ValidateListSetsAndMaps), checked on an
// update only when the stored object passes them.
func TestCRUpdate_ListSetsAndMaps(t *testing.T) {
	h := crHarness(t)
	eq(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"count": 2}}).code, 200, "the stored tags already repeat, so they are not judged")
	eq(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"tags": []any{"x"}}}).code, 200, "now the stored tags are a set")
	answers(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"tags": []any{"y", "y"}}}), 422, `spec.tags[1]: Duplicate value: "y"`, "set duplicates")
	cr := h.do("POST", inc, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident", "metadata": map[string]any{"name": "dup"}, "spec": map[string]any{"tags": []any{"z", "z"}}}})
	answers(t, cr, 422, `spec.tags[1]: Duplicate value: "z"`, "a create is always checked")
	answers(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"items": []any{map[string]any{"name": "a"}, map[string]any{"name": "a"}}}}), 422, `spec.items[1]: Duplicate value: {"name":"a"}`, "map duplicates")
}

// Object metadata (validation.ValidateObjectMetaAccessor and its Update form).
func TestCRUpdate_ObjectMeta(t *testing.T) {
	h := crHarness(t)
	answers(t, crPatch(h, "", "", map[string]any{"metadata": map[string]any{"labels": map[string]any{"a": "not a valid value!"}}}), 422, `metadata.labels: Invalid value: "not a valid value!"`, "a label value")
	cr := h.do("POST", inc, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident", "metadata": map[string]any{"name": "Bad_Name"}, "spec": map[string]any{"level": "low"}}})
	answers(t, cr, 422, `metadata.name: Invalid value: "Bad_Name"`, "a name that is not a DNS subdomain")
}

// Malformed metadata in a request body is a decoding error: the request decoder does not drop it
// (customresource_handler.go:1189-1200, dropInvalidMetadata false), so it is 400, not stored.
func TestCRUpdate_MalformedMetadataIsADecodingError(t *testing.T) {
	h := crHarness(t)
	cr := h.do("POST", inc, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident", "metadata": map[string]any{"name": "m", "labels": "not-a-map"}, "spec": map[string]any{"level": "low"}}})
	answers(t, cr, 400, `Incident in version "v1alpha1" cannot be handled as a Incident: `, "labels that are not a map")
}

// An embedded resource (schemaobjectmeta.Validate, validation.go validateEmbeddedResource): its
// apiVersion and kind are required, and its name is validated as a path segment, not as a DNS
// subdomain, so "Bad_Name" passes there and "a/b" does not.
func TestCRUpdate_EmbeddedResource(t *testing.T) {
	h := crHarness(t)
	tmpl := func(t map[string]any) map[string]any { return map[string]any{"spec": map[string]any{"template": t}} }
	answers(t, crPatch(h, "", "", tmpl(map[string]any{"apiVersion": "v1", "metadata": map[string]any{"name": "x"}})), 422, "spec.template.kind: Required value", "no kind")
	answers(t, crPatch(h, "", "", tmpl(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "a/b"}})), 422, `spec.template.metadata.name: Invalid value: "a/b": may not contain '/'`, "a name that is not a path segment")
	eq(t, crPatch(h, "", "", tmpl(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "Bad_Name"}})).code, 200, "a path-segment name")
}

// CEL is not run after a blocking error, and says so (strategy.go hasBlockingErr).
func TestCRUpdate_CELSkippedAfterABlockingError(t *testing.T) {
	h := crHarness(t)
	r := crPatch(h, "", "", map[string]any{"spec": map[string]any{"count": "two"}})
	answers(t, r, 422, "some validation rules were not checked because the object was invalid; correct the existing errors to complete validation", "a type error")
	if strings.Contains(msg(r), "count under 10") {
		t.Fatalf("CEL ran after a blocking error: %s", msg(r))
	}
	answers(t, crPatch(h, "", "", map[string]any{"spec": map[string]any{"count": 11}}), 422, "count under 10", "CEL without a blocking error")
}

// fieldValidation on a custom resource: a body's unknown field is a decoding error (400, through
// transformDecodeError); a patched object's is 422 on "patch" (patch.go:348-356).
func TestCRUpdate_StrictUnknownFields(t *testing.T) {
	h := crHarness(t)
	cr := h.do("POST", inc+"?fieldValidation=Strict", req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "observability.krateo.io/v1alpha1", "kind": "Incident", "metadata": map[string]any{"name": "s"}, "spec": map[string]any{"level": "low", "other": 1}}})
	answers(t, cr, 400, `Incident in version "v1alpha1" cannot be handled as a Incident: strict decoding error: unknown field "spec.other"`, "create")
	p := crPatch(h, "", "?fieldValidation=Strict", map[string]any{"spec": map[string]any{"other": 1}})
	answers(t, p, 422, `strict decoding error: unknown field "spec.other"`, "patch")
	if !strings.Contains(msg(p), "patch: Invalid value: ") {
		t.Fatalf("a strict patch error is on the patch: %s", msg(p))
	}
	dup := h.do("PATCH", inc+"/"+incident+"?fieldValidation=Strict", req{as: "sa", ct: merge, body: `{"spec":{"count":2,"count":3}}`})
	answers(t, dup, 422, `strict decoding error: duplicate field "spec.count"`, "a duplicate field in the patch")
}

// fieldValidation on a built-in kind: the codec factory's strict serializer.
func TestFieldValidation_BuiltIn(t *testing.T) {
	h := crHarness(t)
	cm := func(q, name string) resp {
		return h.do("POST", "/api/v1/namespaces/krateo-system/configmaps"+q, req{as: "sa", ct: "application/json", body: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name}, "bogus": 1}})
	}
	answers(t, cm("?fieldValidation=Strict", "s"), 400, `ConfigMap in version "v1" cannot be handled as a ConfigMap: strict decoding error: unknown field "bogus"`, "Strict create")
	w := cm("", "w")
	eq(t, w.code, 201, "Warn (the default) create")
	eq(t, w.hdr.Get("Warning"), `299 - "unknown field \"bogus\""`, "the warning")
	ig := cm("?fieldValidation=Ignore", "i")
	eq(t, ig.code, 201, "Ignore create")
	eq(t, ig.hdr.Get("Warning"), "", "no warning")
	sp := h.do("PATCH", "/api/v1/namespaces/krateo-system/configmaps/w?fieldValidation=Strict", req{as: "sa", ct: merge, body: map[string]any{"bogus2": 1}})
	answers(t, sp, 422, `strict decoding error: unknown field "bogus2"`, "Strict patch")
	wp := h.do("PATCH", "/api/v1/namespaces/krateo-system/configmaps/w", req{as: "sa", ct: merge, body: map[string]any{"data": map[string]any{"k": "v"}, "bogus2": 1}})
	eq(t, wp.code, 200, "Warn patch")
	eq(t, wp.hdr.Get("Warning"), `299 - "unknown field \"bogus2\""`, "the patch warning")
	eq(t, get(wp.body, "data", "k"), "v", "the patch applied")
}
