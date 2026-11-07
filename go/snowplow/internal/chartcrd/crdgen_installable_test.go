//go:build unit

package chartcrd

import (
	"context"
	"strings"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	schemacel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	celmodel "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/environment"
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
// with the bounds stripped is refused for cost. If crdgen ever stopped emitting
// the CEL, the refusal would disappear and this test would fail. It would not
// pass for no reason.

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
// plumbing#81 put back (the 1.12.45 shape), crdgen must refuse the schema for
// cost. That proves the positive arm exercised the cost check and did not
// merely skip it.
func TestIssue657_UnboundedSchemaIsRefusedForCost(t *testing.T) {
	_, err := generate(t, stripBounds(t, withCostlyItemRule(t, readFile(t, chartSchemaPath))))
	if err == nil {
		t.Fatal("crdgen accepted an unbounded CEL rule known to be over budget: crdgen stopped running the cost " +
			"estimator, or the estimator changed. Either way the positive arm no longer proves anything")
	}
	// The refusal must be the cost check and nothing else. The `in [...]` rule
	// is the one that breaks the budget unbounded (1.363149x). The explicit
	// impersonateGroups rule (`!=` and startsWith against constants) stays under
	// it even unbounded, and TestIssue657_RuleCostMargin pins its cost.
	if !strings.Contains(err.Error(), "properties["+nativeNotField+"].items.x-kubernetes-validations") ||
		!strings.Contains(err.Error(), costOverBudget) {
		t.Errorf("unbounded schema: no %q error on impersonateServiceAccounts; got:\n%v", costOverBudget, err)
	}
	for _, part := range strings.Split(err.Error(), "; ") {
		if !strings.Contains(part, costOverBudget) {
			t.Errorf("unbounded schema refused for a reason other than cost, so this control does not isolate the bounds:\n%s", part)
		}
	}
}

// TestIssue657_GeneratedCRDCarriesTheRules: the CRD keeps the bounds and each
// item node's constraint. crdgen drops a `not` it cannot translate, and only
// warns. #640's `not: {anyOf: [enum, pattern]}` was dropped that way, so the CRD
// stopped rejecting system:masters. That is why impersonateGroups carries an
// explicit x-kubernetes-validations rule, and must keep it: nothing else
// enforces that constraint. impersonateServiceAccounts' `not: {enum}` is
// translated, natively since plumbing#81 (#1135), and must stay.
func TestIssue657_GeneratedCRDCarriesTheRules(t *testing.T) {
	crd, err := generate(t, readFile(t, chartSchemaPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range constrainedItemFields {
		arr := rbacField(t, crd, f)
		if arr.MaxItems == nil {
			t.Errorf("spec.rbac.%s has no maxItems in the CRD: its item rule is costed as unbounded (#657)", f)
		}
		if arr.Items == nil || arr.Items.Schema == nil {
			t.Fatalf("spec.rbac.%s has no item schema", f)
		}
		it := arr.Items.Schema
		if it.MaxLength == nil {
			t.Errorf("spec.rbac.%s.items has no maxLength in the CRD", f)
		}
		switch f {
		case ruleOnlyField:
			if len(it.XValidations) == 0 {
				t.Errorf("spec.rbac.%s.items carries NO CEL rule: crdgen drops this schema's `not`, so the CRD admits the values helm refuses", f)
			}
		case nativeNotField:
			if it.Not == nil || len(it.Not.Enum) != 1 || string(it.Not.Enum[0].Raw) != `"*"` {
				t.Errorf("spec.rbac.%s.items carries no native `not: {enum: [*]}` (got %+v): the CRD admits the value helm refuses", f, it.Not)
			}
			if len(it.XValidations) != 0 {
				t.Errorf("spec.rbac.%s.items carries a CEL rule again (%v): a rule here is costed against the budget, which plumbing#81 ended", f, it.XValidations)
			}
		default:
			t.Errorf("spec.rbac.%s: no expected constraint declared for this field", f)
		}
	}
}

// TestIssue657_CRDRuleRejectsPrivilegedNames runs the generated CRD's constraints
// through the apiserver's own validators, over whole spec objects: the schema
// validator (a native `not`) and the CEL validator (the x-kubernetes-validations
// rules), as admission does. They must reject what the helm schema rejects, at
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

// TestIssue657_RuleCostMargin reports each item rule's estimated cost against
// the apiserver's per-rule budget (validation.StaticEstimatedCostLimit), computed
// as the apiserver computes it: the rule's compiled MaxCost times the number of
// times it can run (maxItems, since spec and rbac are single objects). It
// requires 100x headroom, so a later rule or a raised maxItems that eats into
// the budget fails here before it fails an install.
func TestIssue657_RuleCostMargin(t *testing.T) {
	const minHeadroom = 100
	crd, err := generate(t, readFile(t, chartSchemaPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range constrainedItemFields {
		arr := rbacField(t, crd, f)
		if arr.MaxItems == nil {
			t.Errorf("spec.rbac.%s has no maxItems: its item rules run an unbounded number of times", f)
			continue
		}
		var internal apiextensions.JSONSchemaProps
		if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(arr.Items.Schema, &internal, nil); err != nil {
			t.Fatal(err)
		}
		s, err := structuralschema.NewStructural(&internal)
		if err != nil {
			t.Fatal(err)
		}
		results, err := schemacel.Compile(s, celmodel.SchemaDeclType(s, false), celconfig.PerCallLimit,
			environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()), schemacel.NewExpressionsEnvLoader())
		if err != nil {
			t.Fatal(err)
		}
		for i, r := range results {
			if r.Error != nil {
				t.Fatalf("spec.rbac.%s.items rule %d does not compile: %v", f, i, r.Error)
			}
			cost := r.MaxCost * uint64(*arr.MaxItems)
			t.Logf("spec.rbac.%s.items rule %d %q: MaxCost/item=%d x maxItems=%d = %d of budget %d (%.4f%%, %.0fx headroom); unbounded it would be x%d = %d",
				f, i, rule(arr.Items.Schema, i), r.MaxCost, *arr.MaxItems, cost, validation.StaticEstimatedCostLimit,
				100*float64(cost)/float64(validation.StaticEstimatedCostLimit), float64(validation.StaticEstimatedCostLimit)/float64(cost),
				r.MaxCardinality, r.MaxCost*r.MaxCardinality)
			if cost*minHeadroom > validation.StaticEstimatedCostLimit {
				t.Errorf("spec.rbac.%s.items rule %d costs %d: less than %dx headroom under the per-rule budget %d",
					f, i, cost, minHeadroom, validation.StaticEstimatedCostLimit)
			}
		}
	}
}

func rule(s *apiextensionsv1.JSONSchemaProps, i int) string { return s.XValidations[i].Rule }
