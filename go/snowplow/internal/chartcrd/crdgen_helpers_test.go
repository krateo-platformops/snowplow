//go:build unit || integration

package chartcrd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/crdgen"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

const (
	// The chart this guard protects, from go/snowplow/internal/chartcrd.
	chartSchemaPath     = "../../../../helm/snowplow/values.schema.json"
	goModPath           = "../../go.mod"
	crdsChartSchemaPath = "../../../../helm/snowplow-crds/values.schema.json"

	// THE INVARIANT: the crdgen this module builds is byte-identical to the one
	// core-provider compiles snowplow's chart schemas into the
	// CompositionDefinition CRD with. A guard run against another crdgen proves
	// nothing about the one that installs us.
	//
	// core-provider 2.13.71 (whose status schema is in testdata) and 2.13.76,
	// which installer 0.3.546 deploys, both require plumbing v1.28.1:
	// coreProviderCrdgenVersion. This module requires plumbing v1.36.0
	// (installer#716, then plumbing#107): crdgenVersion. The crdgen code of the
	// two is byte-identical: crdgen/ and crdgen/schemas/, the only plumbing
	// packages crdgen imports, match under `diff -r` of the module trees
	// (v1.28.1 → v1.34.0, and v1.34.0 → v1.36.0, which changes only docs/,
	// helm/, jwtutil/ and a server/use test). That audit is what lets the two
	// constants differ. requirePinnedCrdgen fails when go.mod moves off
	// crdgenVersion, so a bump repeats it.
	coreProviderVersion = "2.13.71"
	crdgenModule        = "github.com/krateo-platformops/plumbing"
	crdgenVersion       = "v1.36.0"
	// coreProviderCrdgenVersion is the plumbing core-provider compiles with.
	coreProviderCrdgenVersion = "v1.28.1"

	// core-provider's statics/status.schema.json at coreProviderVersion, copied
	// verbatim. crdgen compiles it into the same CRD, so it counts toward the
	// CRD's total CEL cost.
	statusSchemaPath = "testdata/core-provider-" + coreProviderVersion + ".status.schema.json"

	// What the apiserver (and crdgen's in-process copy of its validator) says
	// when a rule's estimated cost exceeds the per-rule budget.
	costOverBudget = "estimated rule cost exceeds budget"
)

// The two allowlists whose items carry a constraint. #657: each stays bounded.
var constrainedItemFields = []string{ruleOnlyField, nativeNotField}

const (
	// ruleOnlyField is the item node whose constraint exists ONLY as a CEL
	// rule: crdgen drops this schema's `not`, so the rule is the enforcement,
	// and the bounds are what keep its cost under the budget.
	ruleOnlyField = "impersonateGroups"

	// nativeNotField is the item node whose `not: {enum}` crdgen emits as a
	// native `not` since plumbing#81 (v1.28.0). It has no CEL cost. Before, crdgen
	// emitted costlyItemRule for it.
	nativeNotField = "impersonateServiceAccounts"

	// costlyItemRule broke the cost budget unbounded (1.363149x), which is why
	// the negative controls put it back: they prove the cost check still runs.
	costlyItemRule = `!(self in ["*"])`
)

// requirePinnedCrdgen reads go.mod, because a `go test` binary carries no
// module dependency list in its build info. The module has no replace
// directives, so the require line is the version that gets built.
func requirePinnedCrdgen(t *testing.T) {
	t.Helper()
	got := ""
	for _, line := range strings.Split(string(readFile(t, goModPath)), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == "replace" && strings.HasPrefix(line, "replace "+crdgenModule) {
			t.Fatalf("go.mod replaces %s: the guard no longer runs core-provider's crdgen", crdgenModule)
		}
		if len(f) >= 2 && f[0] == crdgenModule {
			got = f[1]
		}
	}
	if got != crdgenVersion {
		t.Fatalf("this module requires %s %q, but only %s is audited: its crdgen/ and crdgen/schemas/ are "+
			"byte-identical to %s, which core-provider %s compiles the chart with. Check which core-provider "+
			"the installer deploys and the plumbing it requires, diff crdgen/ and crdgen/schemas/ of %q against "+
			"that version, and re-pin crdgenVersion (and coreProviderCrdgenVersion) only if they match",
			crdgenModule, got, crdgenVersion, coreProviderCrdgenVersion, coreProviderVersion, got)
	}
}

func readFile(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(p))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// requireNoCoreProviderRewrite fails if the schema holds a construct that
// core-provider rewrites before calling crdgen (normalizeIntOrStringUnions: a
// bare `type: number`, or a oneOf/anyOf of exactly {number|integer, string}).
// The guard passes the schema to crdgen unmodified, which matches core-provider
// only while neither construct appears. If one is added, port the rewrite here.
func requireNoCoreProviderRewrite(t *testing.T, schema []byte) {
	t.Helper()
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		t.Fatal(err)
	}
	var walk func(n any, path string)
	walk = func(n any, path string) {
		switch v := n.(type) {
		case map[string]any:
			if ty, _ := v["type"].(string); ty == "number" {
				t.Fatalf("%s is `type: number`, which core-provider %s rewrites before crdgen; port that rewrite into this guard", path, coreProviderVersion)
			}
			for _, k := range []string{"oneOf", "anyOf"} {
				if isIntOrStringUnion(v[k]) {
					t.Fatalf("%s.%s is an int-or-string union, which core-provider %s rewrites before crdgen; port that rewrite into this guard", path, k, coreProviderVersion)
				}
			}
			for k, c := range v {
				walk(c, path+"."+k)
			}
		case []any:
			for _, c := range v {
				walk(c, path+"[]")
			}
		}
	}
	walk(root, "$")
}

func isIntOrStringUnion(raw any) bool {
	arr, ok := raw.([]any)
	if !ok || len(arr) != 2 {
		return false
	}
	types := map[string]bool{}
	for _, e := range arr {
		m, ok := e.(map[string]any)
		if !ok || len(m) != 1 {
			return false
		}
		ty, ok := m["type"].(string)
		if !ok {
			return false
		}
		types[ty] = true
	}
	return types["string"] && (types["number"] || types["integer"])
}

// generate runs crdgen exactly as core-provider's generation.generateCRD does.
// crdgen.Generate also runs the apiserver's ValidateCustomResourceDefinition
// in-process, at this module's apiextensions-apiserver version, and returns an
// error rather than a CRD the validator rejects.
func generate(t *testing.T, schema []byte) (*apiextensionsv1.CustomResourceDefinition, error) {
	t.Helper()
	requirePinnedCrdgen(t)
	out, err := crdgen.Generate(crdgen.Options{
		Group:        "composition.krateo.io",
		Version:      "v0-0-0",
		Kind:         "Snowplow",
		Managed:      true,
		Categories:   []string{"compositions", "comps"},
		SpecSchema:   schema,
		StatusSchema: readFile(t, statusSchemaPath),
	})
	if err != nil {
		return nil, err
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(out, &crd); err != nil {
		t.Fatalf("crdgen output does not parse as a CRD: %v", err)
	}
	return &crd, nil
}

// stripBounds returns a copy of the schema with every maxItems, maxLength and
// maxProperties removed: the shape 1.12.45 shipped. With withCostlyItemRule it
// is the guard's negative control: if crdgen stops running the cost estimator,
// that copy is accepted and the guard fails instead of passing for no reason.
func stripBounds(t *testing.T, schema []byte) []byte {
	t.Helper()
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		t.Fatal(err)
	}
	var walk func(n any)
	walk = func(n any) {
		switch v := n.(type) {
		case map[string]any:
			delete(v, "maxItems")
			delete(v, "maxLength")
			delete(v, "maxProperties")
			for _, c := range v {
				walk(c)
			}
		case []any:
			for _, c := range v {
				walk(c)
			}
		}
	}
	walk(root)
	out, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// withCostlyItemRule returns a copy of the schema whose nativeNotField items
// carry costlyItemRule, the rule crdgen emitted for them before plumbing#81.
func withCostlyItemRule(t *testing.T, schema []byte) []byte {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(schema, &root); err != nil {
		t.Fatal(err)
	}
	var cur any = root
	for _, k := range []string{"properties", "rbac", "properties", nativeNotField, "items"} {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("schema has no %s on the way to %s.items", k, nativeNotField)
		}
		cur = m[k]
	}
	items, ok := cur.(map[string]any)
	if !ok {
		t.Fatalf("schema has no %s.items", nativeNotField)
	}
	items["x-kubernetes-validations"] = []any{map[string]any{"rule": costlyItemRule, "message": "must not be one of the forbidden values"}}
	out, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// rbacField returns spec.rbac.<field> from the generated CRD's served version.
func rbacField(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition, field string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	for _, v := range crd.Spec.Versions {
		if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			continue
		}
		f, ok := v.Schema.OpenAPIV3Schema.Properties["spec"].Properties["rbac"].Properties[field]
		if !ok {
			t.Fatalf("generated CRD has no spec.rbac.%s", field)
		}
		return f
	}
	t.Fatal("generated CRD has no version with a schema")
	return apiextensionsv1.JSONSchemaProps{}
}

// Shared with the kind arm. The names are the schema's literal enums plus the
// #631 prefix family; none is a real tenant group.
var rejectCases = []struct{ field, name string }{
	{"impersonateGroups", "system:masters"},
	{"impersonateGroups", "system:nodes"},
	{"impersonateGroups", "system:foo"},
	{"impersonateGroups", "system:"},
	{"impersonateGroups", "*"},
	{"impersonateServiceAccounts", "*"},
}

var acceptCases = []struct {
	field string
	names []string
}{
	{"impersonateGroups", []string{"devs", "team:system:ops", "System:Ops"}},
	{"impersonateServiceAccounts", []string{"snowplow", "system-reader"}},
}
