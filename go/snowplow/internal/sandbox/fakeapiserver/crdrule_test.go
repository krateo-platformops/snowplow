//go:build js && wasm

package fakeapiserver

// SNAPSHOT.md §3's one snapshot rule at load (#1464): New refuses a snapshot whose discovery lists a
// resource of a group that is not built in with no CRD for it among the objects. agentiko-sandbox
// keeps the same rule in snowplow-wasm/snapshot-rules.js and capture/pipeline/common.mjs; the
// groups below are its own test's, and the words are checked against the source.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	crdRuleWords  = "Built-in (needs no CRD): the core group, groups without a dot, and *.k8s.io. Every other group's resource needs its CRD, named <plural>.<group>, and that CRD must serve the listed version (spec.versions[].name with served: true)."
	crdRulePrefix = "snapshot.json discovery.resources: custom types (their group is not built in) with no CRD among the objects that serves the listed version: "
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

// sv is one spec.versions entry of a test CRD: its name and whether it is served.
type sv struct {
	name   string
	served bool
}

// versionedCRD is a real CRD (057's zendeskrequests) under another metadata.name and with these
// spec.versions, each the real one's schema: the rule reads the name and the versions alone, so
// that is all a case changes.
func versionedCRD(t *testing.T, name string, vs ...sv) map[string]any {
	t.Helper()
	crd := zrCRD(t, "zendeskrequests")
	crd["metadata"].(map[string]any)["name"] = name
	spec := crd["spec"].(map[string]any)
	real := spec["versions"].([]any)[0].(map[string]any)
	versions := []any{}
	for i, v := range vs {
		c := deepCopy(real).(map[string]any)
		c["name"], c["served"], c["storage"] = v.name, v.served, i == 0
		versions = append(versions, c)
	}
	spec["versions"] = versions
	return crd
}

// namedCRD is versionedCRD serving v1beta1, widgetsGV's version.
func namedCRD(t *testing.T, name string) map[string]any {
	t.Helper()
	return versionedCRD(t, name, sv{"v1beta1", true})
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
			[]map[string]any{{"apiVersion": av, "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "panels." + widgetsGroup},
				"spec": map[string]any{"versions": []any{map[string]any{"name": "v1beta1", "served": true}}}}})
	}
	eq(t, len(rule("apiextensions.k8s.io/v1beta1")), 0, "apiextensions.k8s.io/v1beta1 is the group's")
	eq(t, len(rule("apiextensions.k8s.io")), 1, "an apiVersion that is the group's name alone has no group")
}

func TestCRDRule_NamesEveryOffenderInOrder(t *testing.T) {
	edit := func(s map[string]any) {
		listing(widgetsGV, "panels", "layouts/status", "flexes", "panels/status")(s)
		listing("a.example.io/v1", "gadgets")(s)
		listing(widgetsGroup+"/v1alpha1", "panels", "flexes/status")(s)
		listing("apps/v1", "statefulsets")(s)
	}
	// flexes has its CRD, which serves v1beta1 alone; the built-in apps/v1 asks none. Every other
	// type is named, each group version's own, in the order of the group versions and then of the
	// plurals, whatever order the snapshot lists them in.
	want := crdRulePrefix + strings.Join([]string{
		"a.example.io/v1 gadgets (no CustomResourceDefinition gadgets.a.example.io)",
		widgetsGroup + "/v1alpha1 flexes (CustomResourceDefinition flexes." + widgetsGroup + " does not serve v1alpha1)",
		widgetsGroup + "/v1alpha1 panels (no CustomResourceDefinition panels." + widgetsGroup + ")",
		widgetsGV + " layouts (no CustomResourceDefinition layouts." + widgetsGroup + ")",
		widgetsGV + " panels (no CustomResourceDefinition panels." + widgetsGroup + ")",
	}, "; ")
	for i := 0; i < 20; i++ { // a map's order differs from one load to the next
		eq(t, refusal(t, edit, namedCRD(t, "flexes."+widgetsGroup)), want, "every offender, sorted")
	}
}

// unserved is the refusal of one listed type whose CRD is among the objects and does not serve it.
func unserved(gv, plural string) string {
	g, v := splitGV(gv)
	return gv + " " + plural + " (CustomResourceDefinition " + plural + "." + g + " does not serve " + v + ")"
}

func TestCRDRule_TheCRDMustServeTheListedVersion(t *testing.T) {
	name := "panels." + widgetsGroup
	list := listing(widgetsGV, "panels")
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1beta1", true})), "", "served: true")
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1beta1", false})), crdRulePrefix+unserved(widgetsGV, "panels"), "served: false: the type, the version and the CRD are named")
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1", true}, sv{"v1beta1", false})), crdRulePrefix+unserved(widgetsGV, "panels"), "another version served, this one not")
}

func TestCRDRule_AVersionTheCRDLacksIsNotServed(t *testing.T) {
	name := "panels." + widgetsGroup
	eq(t, refusal(t, listing(widgetsGV, "panels"), versionedCRD(t, name, sv{"v1alpha1", true})), crdRulePrefix+unserved(widgetsGV, "panels"), "the version is absent from spec.versions")
	// By exact name: a version that only starts like the listed one, or that the listed one starts like.
	eq(t, refusal(t, listing(widgetsGV, "panels"), versionedCRD(t, name, sv{"v1beta10", true}, sv{"v1", true}, sv{"V1beta1", true})),
		crdRulePrefix+unserved(widgetsGV, "panels"), "v1beta10, v1 and V1beta1 are not v1beta1")
	eq(t, refusal(t, listing(widgetsGroup+"/v1", "panels"), versionedCRD(t, name, sv{"v1beta1", true})),
		crdRulePrefix+unserved(widgetsGroup+"/v1", "panels"), "v1beta1 is not v1")
}

func TestCRDRule_TwoVersionsListedOneServed(t *testing.T) {
	edit := func(s map[string]any) {
		listing(widgetsGV, "panels", "panels/status")(s)
		listing(widgetsGroup+"/v1alpha1", "panels")(s)
		listing(widgetsGroup+"/v2", "panels")(s)
	}
	name := "panels." + widgetsGroup
	eq(t, refusal(t, edit, versionedCRD(t, name, sv{"v1beta1", true}, sv{"v1alpha1", true}, sv{"v2", true})), "", "one CRD serves every listed version")
	eq(t, refusal(t, edit, versionedCRD(t, name, sv{"v1beta1", true}, sv{"v1alpha1", false})),
		crdRulePrefix+unserved(widgetsGroup+"/v1alpha1", "panels")+"; "+unserved(widgetsGroup+"/v2", "panels"), "the served one loads; the unserved and the absent one are named")
	// With no CRD at all the message says so, for each group version: the two refusals differ.
	eq(t, refusal(t, edit), crdRulePrefix+strings.Join([]string{
		widgetsGroup + "/v1alpha1 panels (no CustomResourceDefinition " + name + ")",
		widgetsGroup + "/v1beta1 panels (no CustomResourceDefinition " + name + ")",
		widgetsGroup + "/v2 panels (no CustomResourceDefinition " + name + ")",
	}, "; "), "no CRD is not an unserved version")
}

func TestCRDRule_ASubresourceUnderAnUnservedVersion(t *testing.T) {
	crd := func() map[string]any {
		return versionedCRD(t, "panels."+widgetsGroup, sv{"v1", true}, sv{"v1beta1", false})
	}
	eq(t, refusal(t, listing(widgetsGV, "panels/status", "panels/scale"), crd()), crdRulePrefix+unserved(widgetsGV, "panels"), "a subresource alone: its resource is named, once")
	eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status"), crd()), crdRulePrefix+unserved(widgetsGV, "panels"), "a resource and its subresource are one offender")
	eq(t, refusal(t, listing(widgetsGroup+"/v1", "panels/status"), crd()), "", "a subresource under the served version")
}

// A real apiserver rejects a CRD that names a version twice (spec.versions[i].name: Duplicate value),
// so neither entry is taken: the listed type is refused, whatever the entries' served say and in
// whichever order, and the refusal says why.
func TestCRDRule_AVersionNamedTwiceIsRefused(t *testing.T) {
	name := "panels." + widgetsGroup
	want := crdRulePrefix + widgetsGV + " panels (CustomResourceDefinition " + name + " names v1beta1 more than once)"
	list := listing(widgetsGV, "panels", "panels/status")
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1beta1", true}, sv{"v1beta1", false})), want, "served, then not")
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1beta1", false}, sv{"v1beta1", true})), want, "not served, then served")
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1beta1", true}, sv{"v1beta1", true})), want, "served twice")
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1beta1", false}, sv{"v1", true}, sv{"v1beta1", false})), want, "never served: still named twice")
	// Another version named twice is not the listed one's concern.
	eq(t, refusal(t, list, versionedCRD(t, name, sv{"v1beta1", true}, sv{"v1", true}, sv{"v1", false})), "", "the listed version once, served")
}

// A spec.versions that is missing or not the API's shape serves nothing: refused, never a panic.
func TestCRDRule_MalformedVersionsServeNothing(t *testing.T) {
	name := "panels." + widgetsGroup
	rule := func(crd map[string]any) []string {
		crd["apiVersion"], crd["kind"] = "apiextensions.k8s.io/v1", "CustomResourceDefinition"
		crd["metadata"] = map[string]any{"name": name}
		return crdlessTypes(map[string]metav1.APIResourceList{widgetsGV: {APIResources: []metav1.APIResource{{Name: "panels"}, {Name: "panels/status"}}}}, []map[string]any{crd})
	}
	versions := func(v any) map[string]any { return map[string]any{"spec": map[string]any{"versions": v}} }
	eq(t, len(rule(versions([]any{map[string]any{"name": "v1beta1", "served": true}}))), 0, "the API's shape, served")
	for what, crd := range map[string]map[string]any{
		"no spec":                         {},
		"a spec that is not an object":    {"spec": "v1beta1"},
		"no spec.versions":                {"spec": map[string]any{}},
		"spec.versions null":              versions(nil),
		"spec.versions empty":             versions([]any{}),
		"spec.versions a string":          versions("v1beta1"),
		"spec.versions an object":         versions(map[string]any{"v1beta1": map[string]any{"served": true}}),
		"an entry that is a string":       versions([]any{"v1beta1"}),
		"an entry that is null":           versions([]any{nil}),
		"an entry with no name":           versions([]any{map[string]any{"served": true}}),
		"a name that is not a string":     versions([]any{map[string]any{"name": 1, "served": true}}),
		"an entry with no served":         versions([]any{map[string]any{"name": "v1beta1"}}),
		"served the string true":          versions([]any{map[string]any{"name": "v1beta1", "served": "true"}}),
		"served the number 1 as stored":   versions([]any{map[string]any{"name": "v1beta1", "served": json.Number("1")}}),
		"served an object":                versions([]any{map[string]any{"name": "v1beta1", "served": map[string]any{}}}),
		"served only under another name":  versions([]any{map[string]any{"name": "v1beta1"}, map[string]any{"name": "v1", "served": true}}),
		"served 1":                        versions([]any{map[string]any{"name": "v1beta1", "served": 1}}),
		"served null":                     versions([]any{map[string]any{"name": "v1beta1", "served": nil}}),
		"served: false":                   versions([]any{map[string]any{"name": "v1beta1", "served": false}}),
		"storage: true and served absent": versions([]any{map[string]any{"name": "v1beta1", "storage": true}}),
	} {
		eq(t, strings.Join(rule(crd), "; "), unserved(widgetsGV, "panels"), what)
	}
	// Through New: a CRD with no spec.versions, and one whose spec.versions is empty.
	for what, change := range map[string]func(spec map[string]any){
		"no spec.versions":    func(spec map[string]any) { delete(spec, "versions") },
		"spec.versions empty": func(spec map[string]any) { spec["versions"] = []any{} },
	} {
		crd := namedCRD(t, name)
		change(crd["spec"].(map[string]any))
		eq(t, refusal(t, listing(widgetsGV, "panels"), crd), crdRulePrefix+unserved(widgetsGV, "panels"), what+", through New")
	}
}

func TestCRDRule_NothingListedAsksNothing(t *testing.T) {
	eq(t, len(crdlessTypes(nil, nil)), 0, "no discovery at all")
	eq(t, len(crdlessTypes(map[string]metav1.APIResourceList{}, []map[string]any{{"kind": "CustomResourceDefinition"}})), 0, "an empty discovery")
	eq(t, refusal(t, listing(widgetsGV)), "", "a custom group version that lists nothing")
	// The fixture itself: its custom types (incidents, alerts and their status) have their CRDs.
	eq(t, refusal(t, nil), "", "testdata loads")
}
