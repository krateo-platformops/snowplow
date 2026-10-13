package restactions

// #623: the built-in module krateo/architecture/v1 resolves through the REAL
// spec.filter path (Resolve → jqutil.Eval with jqsupport.ModuleLoader()), and
// its output there is byte-identical to the inlined filter the frontend
// generator emits. The module's full fixture matrix lives in
// internal/support/jq/architecture_v1_test.go; this pins the wiring.

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

type refChart623 struct {
	Name  string `json:"name"`
	Steps []struct {
		IDs  []string `json:"ids"`
		Step string   `json:"step"`
	} `json:"steps"`
	SpecFilter string `json:"specFilter"`
}

func loadEventSite623(t *testing.T) refChart623 {
	t.Helper()
	raw, err := os.ReadFile("../../support/jq/testdata/architecture/reference_filters.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct{ Charts []refChart623 }
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Charts {
		if c.Name == "event-site" {
			return c
		}
	}
	t.Fatal("event-site chart missing from reference filters")
	return refChart623{}
}

// The post-stage dict of an event-site resolve: the graph from the arch stage,
// two LIST stages' projected items, a denied LIST, and a condition-form
// readyWhen that is Ready. Spec.filter receives it as a literal so Resolve runs
// with no stages (no apiserver) and the filter is the only moving part.
const dict623 = `{
  "compositionName": "es1", "compositionNamespace": "es-ns",
  "arch": {"compositionId": "uid", "graph": {"v": 1, "chart": "event-site", "composition": {"name": "es1"},
    "states": ["Provisioning", "Configuring", "Running"],
    "nodes": [
      {"id": "deployment", "kind": "Deployment", "apiVersion": "apps/v1", "class": "native", "level": 0, "present": true, "names": ["es1-web"], "readyWhen": ".status.readyReplicas", "dependsOn": []},
      {"id": "service", "kind": "Service", "apiVersion": "v1", "class": "native", "level": 0, "present": true, "names": ["es1-web"], "dependsOn": []},
      {"id": "form", "kind": "Form", "apiVersion": "acmefeedback.example.io/v1alpha1", "class": "native", "level": 1, "present": true, "names": ["es1-form"],
       "readyWhen": ".status.conditions[] | select(.type == \"Ready\") | .status == \"True\"", "dependsOn": [{"ref": "deployment", "active": true, "ready": true}]},
      {"id": "computeinstance", "kind": "ComputeInstance", "apiVersion": "compute.cnrm.cloud.google.com/v1beta1", "class": "native", "level": 2, "present": true, "names": ["es1-vm"],
       "readyWhen": ".status.conditions[] | select(.type == \"Ready\") | .status == \"True\"", "dependsOn": [{"ref": "form", "active": true, "ready": true}]}
    ]}},
  "o0": [{"apiVersion": "apps/v1", "metadata": {"name": "es1-web", "namespace": "es-ns", "creationTimestamp": "2026-10-06T10:00:00Z"}, "status": {"readyReplicas": 1}}],
  "o1": [{"apiVersion": "v1", "metadata": {"name": "es1-web", "namespace": "es-ns", "creationTimestamp": "2026-10-06T10:00:00Z"}, "status": {}}],
  "o2": [{"apiVersion": "acmefeedback.example.io/v1alpha1", "metadata": {"name": "es1-form", "namespace": "es-ns", "creationTimestamp": "2026-10-06T10:01:00Z"},
          "status": {"conditions": [{"type": "Ready", "status": "True"}]}}],
  "o3Err": ["computeinstances.compute.cnrm.cloud.google.com is forbidden: User \"u\" cannot list resource \"computeinstances\""]
}`

func resolveFilter623(t *testing.T, filter string) string {
	t.Helper()
	ra := &templates.RESTAction{Spec: templates.RESTActionSpec{Filter: ptr.To(filter)}}
	ra.SetName("event-site-status")
	ra.SetNamespace("krateo-system")
	out, err := Resolve(context.Background(), ResolveOptions{In: ra})
	if err != nil {
		t.Fatalf("Resolve: %v\nfilter:\n%s", err, filter)
	}
	if out.Status == nil {
		t.Fatal("Resolve left no status")
	}
	return string(out.Status.Raw)
}

func TestArchitectureModule623_SpecFilterThroughResolve(t *testing.T) {
	ch := loadEventSite623(t)
	steps, err := json.Marshal(ch.Steps)
	if err != nil {
		t.Fatal(err)
	}
	inlined := resolveFilter623(t, dict623+" | (\n"+ch.SpecFilter+")")
	module := resolveFilter623(t, `include "krateo/architecture/v1"; `+dict623+` | fold_steps(`+string(steps)+`) | architecture_state`)
	if module != inlined {
		t.Fatalf("module output differs from the inlined spec.filter through Resolve\n inlined: %s\n  module: %s", inlined, module)
	}
	// Not vacuous: the condition-form node resolved, the denied LIST is unreadable.
	for _, want := range []string{`"id":"form"`, `"phase":"done"`, `"value":"True"`, `"read":"unreadable"`, `"level":2`} {
		if !strings.Contains(module, want) {
			t.Fatalf("resolved state lacks %s: %s", want, module)
		}
	}
}
