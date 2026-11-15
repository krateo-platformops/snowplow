//go:build js && wasm

package fakeapiserver

// SNAPSHOT.md §3's one snapshot rule at load (#1464): New refuses a snapshot whose discovery lists a
// resource of a group that is not built in with no CRD for it among the objects. agentiko-sandbox
// keeps the same rule in snowplow-wasm/snapshot-rules.js and capture/pipeline/common.mjs; the
// groups below are its own test's, and the words are checked against the source.

import (
	"os"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	crdRuleWords  = "Built-in (needs no CRD): the core group, groups without a dot, and *.k8s.io. Every other group's resource needs its CRD, named <plural>.<group>."
	crdRulePrefix = "snapshot.json discovery.resources: custom types (their group is not built in) with no CRD among the objects: "
	widgetsGroup  = "widgets.templates.krateo.io"
	widgetsGV     = widgetsGroup + "/v1beta1"
)

// listing adds plain resources to one group version's discovery list.
func listing(gv string, names ...string) func(map[string]any) {
	var res []map[string]any
	for _, n := range names {
		res = append(res, apires(n, "Thing", true))
	}
	return withResources(gv, res...)
}

// namedCRD is a real CRD (057's zendeskrequests) under another metadata.name: the rule reads the
// name alone, so that is all a case changes.
func namedCRD(t *testing.T, name string) map[string]any {
	t.Helper()
	crd := zrCRD(t, "zendeskrequests")
	crd["metadata"].(map[string]any)["name"] = name
	return crd
}

// refusal loads the fixture with the edit and the extra objects and returns New's error text, or
// "" when it loads.
func refusal(t *testing.T, edit func(map[string]any), extra ...map[string]any) string {
	t.Helper()
	if _, err := loadHarness(t, opts{edit: edit, extra: extra}); err != nil {
		return err.Error()
	}
	return ""
}

func TestCRDRule_TheWordsAreTheSandboxs(t *testing.T) {
	src, err := os.ReadFile("crdrule.go")
	if err != nil {
		t.Fatal(err)
	}
	_, after, ok := strings.Cut(string(src), "\n//\n// Built-in")
	doc, _, ok2 := strings.Cut("Built-in"+after, "\nfunc isBuiltinGroup(")
	if !ok || !ok2 {
		t.Fatal("isBuiltinGroup's doc comment does not end with the rule")
	}
	eq(t, strings.Join(strings.Fields(strings.ReplaceAll(doc, "\n//", " ")), " "), crdRuleWords, "isBuiltinGroup's doc comment is the rule, word for word")
}

func TestCRDRule_BuiltinGroups(t *testing.T) {
	for _, g := range []string{"", "apps", "batch", "rbac.authorization.k8s.io", "apiextensions.k8s.io", "metrics.k8s.io", "gateway.networking.k8s.io"} {
		eq(t, isBuiltinGroup(g), true, g+" is built in")
	}
	// k8s.io itself has a dot and does not end with ".k8s.io": by the predicate it needs its CRDs.
	for _, g := range []string{"observability.krateo.io", "zendesk.ogen.krateo.io", "k8s.io", "k8s.io.example", "x-k8s.io.evil", "apps.example", "xk8s.io", "example.K8S.IO"} {
		eq(t, isBuiltinGroup(g), false, g+" needs its CRDs")
	}
}

func TestCRDRule_RefusesAListedCustomTypeWithNoCRD(t *testing.T) {
	// The issue's case: a sibling type added to a custom group version's list, no CRD, no object.
	eq(t, refusal(t, listing(widgetsGV, "panels")),
		crdRulePrefix+widgetsGV+" panels (no CustomResourceDefinition panels."+widgetsGroup+")", "the type is named, with the CRD it lacks")
	// A sibling of a type that has its CRD is still refused: the fixture's own group.
	eq(t, refusal(t, listing("observability.krateo.io/v1alpha1", "tickets")),
		crdRulePrefix+"observability.krateo.io/v1alpha1 tickets (no CustomResourceDefinition tickets.observability.krateo.io)", "a sibling of a type with a CRD")
	// With the CRD among the objects the same listing loads.
	eq(t, refusal(t, listing(widgetsGV, "panels"), namedCRD(t, "panels."+widgetsGroup)), "", "the same listing, with the CRD")
}

func TestCRDRule_BuiltinGroupVersionsNeedNoCRD(t *testing.T) {
	for _, gv := range []string{"v1", "apps/v1", "batch/v1", "rbac.authorization.k8s.io/v1", "metrics.k8s.io/v1beta1", "gateway.networking.k8s.io/v1"} {
		eq(t, refusal(t, listing(gv, "things", "things/status", "others/scale")), "", gv+": built in, no CRD needed")
	}
	for _, gv := range []string{"k8s.io/v1", "k8s.io.example/v1", "x-k8s.io.evil/v1", "apps.example/v1"} {
		g, _ := splitGV(gv)
		eq(t, refusal(t, listing(gv, "things")), crdRulePrefix+gv+" things (no CustomResourceDefinition things."+g+")", gv+": not built in")
	}
}

func TestCRDRule_ASubresourceCountsAsItsResource(t *testing.T) {
	crd := namedCRD(t, "panels."+widgetsGroup)
	eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status", "panels/scale"), crd), "", "a subresource asks for its resource's CRD, not one of its own")
	// Its resource not listed: it is still that resource's, so the CRD is asked for, once.
	eq(t, refusal(t, listing(widgetsGV, "panels/status", "panels/scale"), crd), "", "a subresource alone, its resource's CRD present")
	eq(t, refusal(t, listing(widgetsGV, "panels/status", "panels/scale")),
		crdRulePrefix+widgetsGV+" panels (no CustomResourceDefinition panels."+widgetsGroup+")", "a subresource alone, no CRD: its resource is named, once")
	eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status")),
		crdRulePrefix+widgetsGV+" panels (no CustomResourceDefinition panels."+widgetsGroup+")", "a resource and its subresource are one offender")
}

func TestCRDRule_OnlyTheExactCRDNameCounts(t *testing.T) {
	want := crdRulePrefix + widgetsGV + " panels (no CustomResourceDefinition panels." + widgetsGroup + ")"
	for what, name := range map[string]string{
		"a name that only ends like it":   "subpanels." + widgetsGroup,
		"a name that only starts like it": "panels." + widgetsGroup + ".example",
		"another plural of the group":     "panelsets." + widgetsGroup,
		"the plural in another case":      "Panels." + widgetsGroup,
		"the group in another case":       "panels.Widgets.templates.krateo.io",
		"the plural alone":                "panels",
		"another group's same plural":     "panels.templates.krateo.io",
	} {
		eq(t, refusal(t, listing(widgetsGV, "panels"), namedCRD(t, name)), want, what+" ("+name+")")
	}
}

func TestCRDRule_OnlyAnApiextensionsCustomResourceDefinitionCounts(t *testing.T) {
	want := crdRulePrefix + widgetsGV + " panels (no CustomResourceDefinition panels." + widgetsGroup + ")"
	for what, change := range map[string]func(map[string]any){
		"a CustomResourceDefinition of another group": func(o map[string]any) { o["apiVersion"] = "example.io/v1" },
		"a group that only ends like it":              func(o map[string]any) { o["apiVersion"] = "x.apiextensions.k8s.io/v1" },
		"an apiVersion with no group":                 func(o map[string]any) { o["apiVersion"] = "v1" },
		"another kind of apiextensions.k8s.io":        func(o map[string]any) { o["kind"] = "CustomResourceDefinitionList" },
	} {
		crd := namedCRD(t, "panels."+widgetsGroup)
		change(crd)
		eq(t, refusal(t, listing(widgetsGV, "panels"), crd), want, what)
	}
	// Any version of apiextensions.k8s.io is that group's.
	rule := func(av string) []string {
		return crdlessTypes(map[string]metav1.APIResourceList{widgetsGV: {APIResources: []metav1.APIResource{{Name: "panels"}}}},
			[]map[string]any{{"apiVersion": av, "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "panels." + widgetsGroup}}})
	}
	eq(t, len(rule("apiextensions.k8s.io/v1beta1")), 0, "apiextensions.k8s.io/v1beta1 is the group's")
	eq(t, len(rule("apiextensions.k8s.io")), 1, "an apiVersion that is the group's name alone has no group")
}

func TestCRDRule_NamesEveryOffenderInOrder(t *testing.T) {
	edit := func(s map[string]any) {
		listing(widgetsGV, "panels", "layouts/status", "flexes", "panels/status")(s)
		listing("a.example.io/v1", "gadgets")(s)
		listing(widgetsGroup+"/v1alpha1", "panels")(s)
		listing("apps/v1", "statefulsets")(s)
	}
	// flexes has its CRD; the built-in apps/v1 asks none. Every other type is named, each group
	// version's own, in the order of the group versions and then of the plurals, whatever order the
	// snapshot lists them in.
	want := crdRulePrefix + strings.Join([]string{
		"a.example.io/v1 gadgets (no CustomResourceDefinition gadgets.a.example.io)",
		widgetsGroup + "/v1alpha1 panels (no CustomResourceDefinition panels." + widgetsGroup + ")",
		widgetsGV + " layouts (no CustomResourceDefinition layouts." + widgetsGroup + ")",
		widgetsGV + " panels (no CustomResourceDefinition panels." + widgetsGroup + ")",
	}, "; ")
	for i := 0; i < 20; i++ { // a map's order differs from one load to the next
		eq(t, refusal(t, edit, namedCRD(t, "flexes."+widgetsGroup)), want, "every offender, sorted")
	}
}

// The rule asks for the CRD by name, per group; discovery is per group version. A group version the
// CRD does not serve is not this rule's to refuse: 057's zendeskrequests CRD serves v2 alone.
func TestCRDRule_TheCRDIsPerGroupNotPerGroupVersion(t *testing.T) {
	crd := zrCRD(t, "zendeskrequests")
	edit := func(s map[string]any) {
		listing(zrGroup+"/v2", "zendeskrequests", "zendeskrequests/status")(s)
		listing(zrGroup+"/v1alpha1", "zendeskrequests")(s)
	}
	eq(t, refusal(t, edit, crd), "", "one CRD answers for every group version that lists its plural")
	eq(t, refusal(t, edit), crdRulePrefix+strings.Join([]string{
		zrGroup + "/v1alpha1 zendeskrequests (no CustomResourceDefinition zendeskrequests." + zrGroup + ")",
		zrGroup + "/v2 zendeskrequests (no CustomResourceDefinition zendeskrequests." + zrGroup + ")",
	}, "; "), "without it, each group version's listing is named")
}

func TestCRDRule_NothingListedAsksNothing(t *testing.T) {
	eq(t, len(crdlessTypes(nil, nil)), 0, "no discovery at all")
	eq(t, len(crdlessTypes(map[string]metav1.APIResourceList{}, []map[string]any{{"kind": "CustomResourceDefinition"}})), 0, "an empty discovery")
	eq(t, refusal(t, listing(widgetsGV)), "", "a custom group version that lists nothing")
	// The fixture itself: its custom types (incidents, alerts and their status) have their CRDs.
	eq(t, refusal(t, nil), "", "testdata loads")
}
