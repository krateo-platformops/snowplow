//go:build unit

package chartcrd

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	schemacel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
)

// crdgen_installable_test.go — #657, the in-process half of the guard. It runs
// on every PR under -tags unit and needs no cluster.
//
// It runs crdgen, the version core-provider pins, over helm/snowplow/values.schema.json.
// crdgen gates its own output on the apiserver's ValidateCustomResourceDefinition,
// so a schema whose CRD the apiserver would refuse fails here. 1.12.45 failed
// with the exact 057 message: impersonateGroups "factor of 1.468006x",
// impersonateServiceAccounts "factor of 1.363149x". The kind-backed half
// (crdgen_kind_admission_test.go, -tags integration) submits the same CRD to a
// v1.36.4 apiserver, which is 057's version.
//
// The guard asserts BOTH directions. The chart's schema generates, and a copy
// with the bounds stripped and costlyItemRule put back on every constrained
// item is refused for cost. If crdgen ever stopped running the cost estimator,
// the refusal would disappear and this test would fail. It would not pass for
// no reason.

// TestIssue657_CrdgenCompilesChartSchema: each chart, as committed, compiles
// to a CRD that the apiserver's validator accepts. compositiondefinition.yaml
// installs only helm/snowplow through core-provider. helm/snowplow-crds is
// checked too, so that installing it the same way stays possible.
func TestIssue657_CrdgenCompilesChartSchema(t *testing.T) {
	for _, p := range []string{chartSchemaPath, crdsChartSchemaPath} {
		schema := readFile(t, p)
		requireNoCoreProviderRewrite(t, schema)
		if _, err := generate(t, schema); err != nil {
			t.Errorf("crdgen (core-provider %s) refuses %s, so the chart cannot be installed on Krateo (#657):\n%v",
				coreProviderVersion, p, err)
		}
	}
}

// TestIssue657_UnboundedSchemaIsRefusedForCost is the negative control. With
// every maxItems/maxLength stripped and the item rule crdgen emitted before
// plumbing#81 put back on every constrained item (the 1.12.45 shape), crdgen
// must refuse the schema for cost, at each of those items. That proves the
// positive arm exercised the cost check and did not merely skip it.
func TestIssue657_UnboundedSchemaIsRefusedForCost(t *testing.T) {
	_, err := generate(t, stripBounds(t, withCostlyItemRule(t, readFile(t, chartSchemaPath))))
	if err == nil {
		t.Fatal("crdgen accepted an unbounded CEL rule known to be over budget: crdgen stopped running the cost " +
			"estimator, or the estimator changed. Either way the positive arm no longer proves anything")
	}
	// The refusal must be the cost check, at the rule this control put back,
	// and nothing else.
	for _, f := range constrainedItemFields {
		if !strings.Contains(err.Error(), "properties["+f+"].items.x-kubernetes-validations") {
			t.Errorf("unbounded schema: no %q error on spec.rbac.%s's items; got:\n%v", costOverBudget, f, err)
		}
	}
	for _, part := range strings.Split(err.Error(), "; ") {
		if !strings.Contains(part, costOverBudget) {
			t.Errorf("unbounded schema refused for a reason other than cost, so this control does not isolate the bounds:\n%s", part)
		}
	}
}

// TestIssue657_GeneratedCRDCarriesTheConstraints: the CRD keeps the bounds, and
// each constrained item carries the chart's `not` as its own native `not`,
// whole, and no CEL rule. crdgen drops a `not` it cannot carry, and only
// warns: #640's `not: {anyOf: [enum, pattern]}` was dropped that way until
// plumbing#203 (v1.88.0), so the CRD stopped rejecting system:masters. Should a
// crdgen drop or narrow a `not` again, this fails, and the constraint must not
// be restated in CEL beside a `not` that carries it.
func TestIssue657_GeneratedCRDCarriesTheConstraints(t *testing.T) {
	schema := readFile(t, chartSchemaPath)
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		t.Fatal(err)
	}
	crd, err := generate(t, schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range constrainedItemFields {
		arr := rbacField(t, crd, f)
		if arr.MaxItems == nil {
			t.Errorf("spec.rbac.%s has no maxItems in the CRD: an item rule would be costed as unbounded (#657)", f)
		}
		if arr.Items == nil || arr.Items.Schema == nil {
			t.Fatalf("spec.rbac.%s has no item schema", f)
		}
		it := arr.Items.Schema
		if it.MaxLength == nil {
			t.Errorf("spec.rbac.%s.items has no maxLength in the CRD", f)
		}
		want := schemaNode(root, "properties", "rbac", "properties", f, "items", "not")
		if want == nil {
			t.Fatalf("the chart's rbac.%s.items has no `not`: this test's premise is gone, re-read it", f)
		}
		var got any
		if it.Not != nil {
			raw, err := json.Marshal(it.Not)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("spec.rbac.%s.items' native `not` is %v, the chart's is %v: the CRD does not refuse exactly what helm refuses", f, got, want)
		}
		if len(it.XValidations) != 0 {
			t.Errorf("spec.rbac.%s.items carries a CEL rule (%v) beside its native `not`: one constraint, stated twice", f, it.XValidations)
		}
	}
}

// TestIssue657_CRDRuleRejectsPrivilegedNames runs the generated CRD's constraints
// through the apiserver's own validators, over whole spec objects: the schema
// validator (a native `not`) and the CEL validator (any x-kubernetes-validations
// rule), as admission does. They must reject what the helm schema rejects, at
// the item, and admit ordinary names. The kind-backed twin does the same with
// real CRs against a v1.36.4 apiserver.
func TestIssue657_CRDRuleRejectsPrivilegedNames(t *testing.T) {
	crd, err := generate(t, readFile(t, chartSchemaPath))
	if err != nil {
		t.Fatal(err)
	}
	var internal apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
		crd.Spec.Versions[0].Schema.OpenAPIV3Schema, &internal, nil); err != nil {
		t.Fatal(err)
	}
	structural, err := structuralschema.NewStructural(&internal)
	if err != nil {
		t.Fatal(err)
	}
	cel := schemacel.NewValidator(structural, true, 1_000_000)
	schemaValidator, _, err := apiservervalidation.NewSchemaValidator(&internal)
	if err != nil {
		t.Fatal(err)
	}

	admissionErrs := func(field_ string, names ...string) string {
		list := make([]any, len(names))
		for i, n := range names {
			list[i] = n
		}
		obj := map[string]any{
			"apiVersion": "composition.krateo.io/v0-0-0",
			"kind":       "Snowplow",
			"metadata":   map[string]any{"name": "x", "namespace": "default"},
			"spec":       map[string]any{"rbac": map[string]any{field_: list}},
		}
		errs := apiservervalidation.ValidateCustomResource(nil, obj, schemaValidator)
		celErrs, _ := cel.Validate(context.Background(), nil, structural, obj, nil, 10_000_000)
		errs = append(errs, celErrs...)
		if len(errs) == 0 {
			return ""
		}
		return errs.ToAggregate().Error()
	}

	for _, c := range rejectCases {
		t.Run("reject/"+c.field+"/"+c.name, func(t *testing.T) {
			// After an allowed name: the constraint must apply per item.
			got := admissionErrs(c.field, "devs", c.name)
			if got == "" {
				t.Fatalf("the CRD admits spec.rbac.%s=[devs, %s]", c.field, c.name)
			}
			// Refused by the item's constraint, not by something incidental.
			if !strings.Contains(got, "spec.rbac."+c.field+"[1]") {
				t.Fatalf("spec.rbac.%s=[devs, %s] refused, but not at the item: %s", c.field, c.name, got)
			}
		})
	}
	for _, c := range acceptCases {
		t.Run("accept/"+c.field, func(t *testing.T) {
			if got := admissionErrs(c.field, c.names...); got != "" {
				t.Fatalf("the CRD rejects spec.rbac.%s=%v: %s", c.field, c.names, got)
			}
		})
	}
}
