//go:build unit || integration

package chartcrd

import (
	"encoding/json"
	"sort"
	"strconv"
	"testing"
)

// crdgen's only difference between plumbing v1.28.1 (the crdgen core-provider
// 2.13.71 and 2.13.76 compile with) and v1.52.0 (this module's; code-identical
// to v1.42.0, which core-provider 2.13.84 compiles with) is emitNot
// (crdgen/transpile_keywords.go, plumbing#102): a `not` whose subschema is
// scalar-only used to be emitted only when it held const/enum, and DROPPED
// otherwise; v1.52.0 also emits pattern, format, length, numeric bounds,
// multipleOf and required. Every other `not` is handled the same by both: a
// non-scalar-only subschema (properties, items, a junctor, $ref,
// additionalProperties, if, a nested not) is dropped by both.
//
// So the two crdgens compile a chart schema to the same CRD exactly when the
// schema has no `not` that is scalar-only AND carries anything but const and
// enum. This test is that condition. Measured at the v1.52.0 bump, the two CRDs
// for both chart schemas were byte-identical (PR #1359). Should a chart schema
// gain such a `not`, this test fails, because the CRD core-provider installs
// would then depend on which core-provider is deployed.
func TestCrdgenPin_NoNotShapeWhereTheTwoCoreProviderCrdgensDiffer(t *testing.T) {
	// The keys that make a subschema not scalar-only (crdgen isScalarOnly).
	composite := map[string]bool{"properties": true, "items": true, "allOf": true, "anyOf": true, "oneOf": true,
		"$ref": true, "additionalProperties": true, "if": true, "not": true}
	// The keys both crdgens carry, or that do not validate.
	same := map[string]bool{"const": true, "enum": true, "description": true, "title": true, "$comment": true}

	seen := 0
	for _, p := range []string{chartSchemaPath, crdsChartSchemaPath} {
		var root any
		if err := json.Unmarshal(readFile(t, p), &root); err != nil {
			t.Fatal(err)
		}
		var walk func(n any, path string)
		walk = func(n any, path string) {
			switch v := n.(type) {
			case map[string]any:
				if sub, ok := v["not"].(map[string]any); ok {
					seen++
					var keys, differ []string
					isComposite := false
					for k := range sub {
						keys = append(keys, k)
						if composite[k] {
							isComposite = true
						}
						if !same[k] {
							differ = append(differ, k)
						}
					}
					sort.Strings(keys)
					sort.Strings(differ)
					if !isComposite && len(differ) > 0 {
						t.Errorf("%s: %s.not carries %v (keys %v): crdgen v1.28.1 drops it and v1.52.0 emits it, so the CRD "+
							"core-provider installs depends on its version; see this test's comment", p, path, differ, keys)
					}
				}
				for k, c := range v {
					walk(c, path+"."+k)
				}
			case []any:
				for i, c := range v {
					walk(c, path+"["+strconv.Itoa(i)+"]")
				}
			}
		}
		walk(root, "$")
	}
	// Non-vacuous: the chart schema has `not` nodes today (one enum-only, one anyOf).
	if seen == 0 {
		t.Fatal("no `not` found in the chart schemas: the walk is broken, or the schemas changed and this test should be re-read")
	}
}
