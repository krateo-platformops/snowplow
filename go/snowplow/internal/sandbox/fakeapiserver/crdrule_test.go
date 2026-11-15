//go:build js && wasm

package fakeapiserver

// SNAPSHOT.md §3's one snapshot rule at load (#1464): New refuses a snapshot whose discovery lists a
// resource of a group that is not built in without its CRD among the objects: one of that name,
// group and plural that serves the listed version. agentiko-sandbox keeps the same rule in
// snowplow-wasm/snapshot-rules.js and capture/pipeline/common.mjs; the groups below are its own
// test's, the words are checked against the source, and TestCRDRule_TheSandboxsEdges is its
// reviewer's list of what its validator does, row for row.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const (
	crdRuleWords  = "Built-in (needs no CRD): the core group, groups without a dot, and *.k8s.io. Every other group's resource needs its CRD, named <plural>.<group>, and that CRD must serve the listed version (spec.versions[].name with served: true), and its spec.group and spec.names.plural must be that group and plural."
	crdRulePrefix = "snapshot.json discovery.resources: custom types (their group is not built in) without their CRD among the objects: "
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

// versionedCRD is a real CRD (057's zendeskrequests) made the CRD of that name: its metadata.name,
// its spec.group and spec.names.plural (the name's two parts) and these spec.versions, each with the
// real one's schema. The rule reads nothing else of a CRD.
func versionedCRD(t *testing.T, name string, vs ...sv) map[string]any {
	t.Helper()
	crd := zrCRD(t, "zendeskrequests")
	crd["metadata"].(map[string]any)["name"] = name
	spec := crd["spec"].(map[string]any)
	plural, group, _ := strings.Cut(name, ".")
	spec["group"] = group
	spec["names"].(map[string]any)["plural"] = plural
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

// noCRD, unserved and wrongSpec are the three refusals of one listed type.
func noCRD(gv, plural string) string {
	g, _ := splitGV(gv)
	return gv + " " + plural + " (no CustomResourceDefinition " + plural + "." + g + ")"
}

func unserved(gv, plural string) string {
	g, v := splitGV(gv)
	return gv + " " + plural + " (CustomResourceDefinition " + plural + "." + g + " does not serve " + v + ")"
}

func wrongSpec(gv, plural, what string) string {
	g, _ := splitGV(gv)
	return gv + " " + plural + " (CustomResourceDefinition " + plural + "." + g + ": " + what + ")"
}

// The rule on JSON as a snapshot holds it: discovery.resources and the objects. The answer is the
// offenders joined by "; ", "" for none, or "error: " and listedResources' refusal.
func crdRule(t *testing.T, resources, objects string) string {
	t.Helper()
	listed, err := listedResources([]byte(`{"discovery":{"resources":` + resources + `}}`))
	if err != nil {
		return "error: " + err.Error()
	}
	var objs []map[string]any
	if err := json.Unmarshal([]byte(objects), &objs); err != nil {
		t.Fatalf("the test's objects %s: %v", objects, err)
	}
	return strings.Join(crdlessTypes(listed, objs), "; ")
}

const (
	eG  = "w.example.io"
	eGV = eG + "/v1"
)

// crdJ is a CRD as JSON: its name, its spec, and (rest) what else the object holds, which replaces
// the usual apiVersion and kind when it names them.
func crdJ(name, spec string, rest ...string) string {
	head := `"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition"`
	if len(rest) > 0 {
		head = rest[0]
	}
	return `{` + head + `,"metadata":{"name":"` + name + `"},"spec":` + spec + `}`
}

// specJ is the spec of the CRD of that group and plural with these spec.versions.
func specJ(group, plural, versions string) string {
	return `{"group":"` + group + `","names":{"plural":"` + plural + `"},"versions":` + versions + `}`
}

// panelsJ is panels.w.example.io with these spec.versions; served is the one that serves v1.
func panelsJ(versions string) string { return crdJ("panels."+eG, specJ(eG, "panels", versions)) }

const served = `[{"name":"v1","served":true}]`

// listJ is one group version's list of these entries, each a JSON value.
func listJ(gv string, entries ...string) string {
	return `{"` + gv + `":{"resources":[` + strings.Join(entries, ",") + `]}}`
}

// namesJ is listJ of plain entries with these names.
func namesJ(gv string, names ...string) string {
	var es []string
	for _, n := range names {
		es = append(es, `{"name":"`+n+`"}`)
	}
	return listJ(gv, es...)
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
	eq(t, refusal(t, listing(widgetsGV, "panels")), crdRulePrefix+noCRD(widgetsGV, "panels"), "the type is named, with the CRD it lacks")
	// A sibling of a type that has its CRD is still refused: the fixture's own group.
	eq(t, refusal(t, listing("observability.krateo.io/v1alpha1", "tickets")), crdRulePrefix+noCRD("observability.krateo.io/v1alpha1", "tickets"), "a sibling of a type with a CRD")
	// With the CRD among the objects the same listing loads.
	eq(t, refusal(t, listing(widgetsGV, "panels"), namedCRD(t, "panels."+widgetsGroup)), "", "the same listing, with the CRD")
}

func TestCRDRule_BuiltinGroupVersionsNeedNoCRD(t *testing.T) {
	for _, gv := range []string{"v1", "apps/v1", "batch/v1", "rbac.authorization.k8s.io/v1", "metrics.k8s.io/v1beta1", "gateway.networking.k8s.io/v1"} {
		eq(t, refusal(t, listing(gv, "things", "things/status", "others/scale")), "", gv+": built in, no CRD needed")
	}
	for _, gv := range []string{"k8s.io/v1", "k8s.io.example/v1", "x-k8s.io.evil/v1", "apps.example/v1"} {
		eq(t, refusal(t, listing(gv, "things")), crdRulePrefix+noCRD(gv, "things"), gv+": not built in")
	}
}

func TestCRDRule_ASubresourceCountsAsItsResource(t *testing.T) {
	crd := func() map[string]any { return namedCRD(t, "panels."+widgetsGroup) }
	eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status", "panels/scale"), crd()), "", "a subresource asks for its resource's CRD, not one of its own")
	// Its resource not listed: it is still that resource's, so the CRD is asked for, once.
	eq(t, refusal(t, listing(widgetsGV, "panels/status", "panels/scale"), crd()), "", "a subresource alone, its resource's CRD present")
	eq(t, refusal(t, listing(widgetsGV, "panels/status", "panels/scale")), crdRulePrefix+noCRD(widgetsGV, "panels"), "a subresource alone, no CRD: its resource is named, once")
	eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status")), crdRulePrefix+noCRD(widgetsGV, "panels"), "a resource and its subresource are one offender")
}

func TestCRDRule_OnlyTheExactCRDNameCounts(t *testing.T) {
	for what, name := range map[string]string{
		"a name that only ends like it":   "subpanels." + widgetsGroup,
		"a name that only starts like it": "panels." + widgetsGroup + ".example",
		"another plural of the group":     "panelsets." + widgetsGroup,
		"the plural in another case":      "Panels." + widgetsGroup,
		"the group in another case":       "panels.Widgets.templates.krateo.io",
		"the plural alone":                "panels",
		"another group's same plural":     "panels.templates.krateo.io",
	} {
		// Each is a CRD the rule would take for its own type: only the name is not this type's.
		eq(t, refusal(t, listing(widgetsGV, "panels"), namedCRD(t, name)), crdRulePrefix+noCRD(widgetsGV, "panels"), what+" ("+name+")")
		// The same with this type's spec.group and spec.names.plural: the name still decides.
		crd := namedCRD(t, "panels."+widgetsGroup)
		crd["metadata"].(map[string]any)["name"] = name
		eq(t, refusal(t, listing(widgetsGV, "panels"), crd), crdRulePrefix+noCRD(widgetsGV, "panels"), what+", the type's own spec ("+name+")")
	}
}

func TestCRDRule_OnlyAnApiextensionsCustomResourceDefinitionCounts(t *testing.T) {
	for what, change := range map[string]func(map[string]any){
		"a CustomResourceDefinition of another group": func(o map[string]any) { o["apiVersion"] = "example.io/v1" },
		"a group that only ends like it":              func(o map[string]any) { o["apiVersion"] = "x.apiextensions.k8s.io/v1" },
		"an apiVersion with no group":                 func(o map[string]any) { o["apiVersion"] = "v1" },
		"another kind of apiextensions.k8s.io":        func(o map[string]any) { o["kind"] = "CustomResourceDefinitionList" },
	} {
		crd := namedCRD(t, "panels."+widgetsGroup)
		change(crd)
		eq(t, refusal(t, listing(widgetsGV, "panels"), crd), crdRulePrefix+noCRD(widgetsGV, "panels"), what)
	}
}

func TestCRDRule_NamesEveryOffenderInOrder(t *testing.T) {
	edit := func(s map[string]any) {
		listing(widgetsGV, "panels", "layouts/status", "flexes", "panels/status", "grids")(s)
		listing("a.example.io/v1", "gadgets")(s)
		listing(widgetsGroup+"/v1alpha1", "panels", "flexes/status")(s)
		listing("apps/v1", "statefulsets")(s)
	}
	// flexes has its CRD, which serves v1beta1 alone; grids' CRD is of another group; the built-in
	// apps/v1 asks none. Every other type is named, each group version's own, in the order of the
	// group versions and then of the plurals, whatever order the snapshot lists them in.
	want := crdRulePrefix + strings.Join([]string{
		noCRD("a.example.io/v1", "gadgets"),
		unserved(widgetsGroup+"/v1alpha1", "flexes"),
		noCRD(widgetsGroup+"/v1alpha1", "panels"),
		wrongSpec(widgetsGV, "grids", `spec.group is "templates.krateo.io", expected "`+widgetsGroup+`"`),
		noCRD(widgetsGV, "layouts"),
		noCRD(widgetsGV, "panels"),
	}, "; ")
	for i := 0; i < 20; i++ { // a map's order differs from one load to the next
		grids := namedCRD(t, "grids."+widgetsGroup)
		grids["spec"].(map[string]any)["group"] = "templates.krateo.io"
		eq(t, refusal(t, edit, namedCRD(t, "flexes."+widgetsGroup), grids), want, "every offender, sorted")
	}
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
	eq(t, refusal(t, edit), crdRulePrefix+strings.Join([]string{noCRD(widgetsGroup+"/v1alpha1", "panels"), noCRD(widgetsGV, "panels"), noCRD(widgetsGroup+"/v2", "panels")}, "; "),
		"no CRD is not an unserved version")
}

func TestCRDRule_ASubresourceUnderAnUnservedVersion(t *testing.T) {
	crd := func() map[string]any {
		return versionedCRD(t, "panels."+widgetsGroup, sv{"v1", true}, sv{"v1beta1", false})
	}
	eq(t, refusal(t, listing(widgetsGV, "panels/status", "panels/scale"), crd()), crdRulePrefix+unserved(widgetsGV, "panels"), "a subresource alone: its resource is named, once")
	eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status"), crd()), crdRulePrefix+unserved(widgetsGV, "panels"), "a resource and its subresource are one offender")
	eq(t, refusal(t, listing(widgetsGroup+"/v1", "panels/status"), crd()), "", "a subresource under the served version")
}

// The last clause: the CRD found by its name counts only if its spec.group and spec.names.plural are
// the listed type's. The fake keys a CRD's schema by those two, so one of another spec leaves the
// listed type with no schema at all, as if it had no CRD.
func TestCRDRule_TheCRDsSpecMustBeThatGroupAndPlural(t *testing.T) {
	name := "panels." + eG
	list := namesJ(eGV, "panels", "panels/status")
	group := func(found string) string { return "spec.group is " + found + `, expected "` + eG + `"` }
	plural := func(found string) string { return "spec.names.plural is " + found + `, expected "panels"` }
	for _, c := range []struct{ what, spec, want string }{
		{"that group and plural", specJ(eG, "panels", served), ""},
		{"spec.group wrong", specJ("x.example.io", "panels", served), group(`"x.example.io"`)},
		{"spec.group in another case", specJ("W.example.io", "panels", served), group(`"W.example.io"`)},
		{"spec.names.plural wrong", specJ(eG, "panelsets", served), plural(`"panelsets"`)},
		{"spec.names.plural the singular", specJ(eG, "panel", served), plural(`"panel"`)},
		{"both wrong", specJ("x.example.io", "boards", served), group(`"x.example.io"`) + " and " + plural(`"boards"`)},
		{"both wrong and the version unserved: the spec is what is said", specJ("x.example.io", "boards", `[]`), group(`"x.example.io"`) + " and " + plural(`"boards"`)},
		{"spec.names missing", `{"group":"` + eG + `","versions":` + served + `}`, plural("not a string")},
		{"spec.names null", `{"group":"` + eG + `","names":null,"versions":` + served + `}`, plural("not a string")},
		{"spec.names a string", `{"group":"` + eG + `","names":"panels","versions":` + served + `}`, plural("not a string")},
		{"spec.names a list", `{"group":"` + eG + `","names":["panels"],"versions":` + served + `}`, plural("not a string")},
		{"spec.names without plural", `{"group":"` + eG + `","names":{"kind":"Panel"},"versions":` + served + `}`, plural("not a string")},
		{"spec.names.plural not a string", `{"group":"` + eG + `","names":{"plural":1},"versions":` + served + `}`, plural("not a string")},
		{"spec.names.plural empty", specJ(eG, "", served), plural(`""`)},
		{"spec.group missing", `{"names":{"plural":"panels"},"versions":` + served + `}`, group("not a string")},
		{"spec.group null", `{"group":null,"names":{"plural":"panels"},"versions":` + served + `}`, group("not a string")},
		{"spec.group not a string", `{"group":1,"names":{"plural":"panels"},"versions":` + served + `}`, group("not a string")},
		{"spec.group empty", specJ("", "panels", served), group(`""`)},
		{"no spec", `null`, group("not a string") + " and " + plural("not a string")},
		{"a spec that is a string", `"panels"`, group("not a string") + " and " + plural("not a string")},
	} {
		want := c.want
		if want != "" {
			want = wrongSpec(eGV, "panels", want)
		}
		eq(t, crdRule(t, list, `[`+crdJ(name, c.spec)+`]`), want, c.what)
	}
	// Of two CRDs with the name, the last among the objects decides.
	good, bad := crdJ(name, specJ(eG, "panels", served)), crdJ(name, specJ("x.example.io", "panels", served))
	eq(t, crdRule(t, list, `[`+good+`,`+bad+`]`), wrongSpec(eGV, "panels", group(`"x.example.io"`)), "a consistent CRD shadowed by a later inconsistent one")
	eq(t, crdRule(t, list, `[`+bad+`,`+good+`]`), "", "an inconsistent CRD shadowed by a later consistent one")
	// Each group version that lists the type is refused, the same way.
	eq(t, crdRule(t, `{"`+eGV+`":{"resources":[{"name":"panels"}]},"`+eG+`/v2":{"resources":[{"name":"panels/status"}]}}`, `[`+bad+`]`),
		wrongSpec(eGV, "panels", group(`"x.example.io"`))+"; "+wrongSpec(eG+"/v2", "panels", group(`"x.example.io"`)), "every group version's listing")
}

// The same clause through New, on a real CRD.
func TestCRDRule_ACRDOfAnotherSpecIsRefusedAtLoad(t *testing.T) {
	list := listing(widgetsGV, "panels", "panels/status")
	spec := func(crd map[string]any) map[string]any { return crd["spec"].(map[string]any) }

	crd := namedCRD(t, "panels."+widgetsGroup)
	spec(crd)["group"] = zrGroup
	eq(t, refusal(t, list, crd), crdRulePrefix+wrongSpec(widgetsGV, "panels", `spec.group is "`+zrGroup+`", expected "`+widgetsGroup+`"`), "spec.group wrong")

	crd = namedCRD(t, "panels."+widgetsGroup)
	spec(crd)["names"].(map[string]any)["plural"] = "zendeskrequests"
	eq(t, refusal(t, list, crd), crdRulePrefix+wrongSpec(widgetsGV, "panels", `spec.names.plural is "zendeskrequests", expected "panels"`), "spec.names.plural wrong")

	// 057's CRD as it is, under the listed type's name: what the fake took for the type's CRD
	// before this clause, and served the type with no schema.
	crd = zrCRD(t, "zendeskrequests")
	crd["metadata"].(map[string]any)["name"] = "panels." + widgetsGroup
	eq(t, refusal(t, listing(widgetsGroup+"/v2", "panels"), crd),
		crdRulePrefix+wrongSpec(widgetsGroup+"/v2", "panels", `spec.group is "`+zrGroup+`", expected "`+widgetsGroup+`" and spec.names.plural is "zendeskrequests", expected "panels"`), "both wrong")

	crd = namedCRD(t, "panels."+widgetsGroup)
	delete(spec(crd), "names")
	eq(t, refusal(t, list, crd), crdRulePrefix+wrongSpec(widgetsGV, "panels", `spec.names.plural is not a string, expected "panels"`), "spec.names missing")

	crd = namedCRD(t, "panels."+widgetsGroup)
	delete(spec(crd), "group")
	eq(t, refusal(t, list, crd), crdRulePrefix+wrongSpec(widgetsGV, "panels", `spec.group is not a string, expected "`+widgetsGroup+`"`), "spec.group missing")
}

// agentiko-sandbox publishes CRDs with spec.conversion set to {strategy: None}, a redaction. The
// fake reads nothing of spec.conversion (it does no version conversion: doc.go), so the rule's
// answer and the load are the same whatever it holds.
func TestCRDRule_SpecConversionIsNotRead(t *testing.T) {
	for what, conversion := range map[string]any{
		"none":    nil,
		"None":    map[string]any{"strategy": "None"},
		"Webhook": map[string]any{"strategy": "Webhook", "webhook": map[string]any{"conversionReviewVersions": []any{"v1"}, "clientConfig": map[string]any{"url": "https://conversion.invalid/convert"}}},
	} {
		crd := namedCRD(t, "panels."+widgetsGroup)
		if conversion == nil {
			delete(crd["spec"].(map[string]any), "conversion")
		} else {
			crd["spec"].(map[string]any)["conversion"] = conversion
		}
		eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status"), crd), "", "spec.conversion "+what+": loads")
		crd["spec"].(map[string]any)["versions"].([]any)[0].(map[string]any)["served"] = false
		eq(t, refusal(t, listing(widgetsGV, "panels"), crd), crdRulePrefix+unserved(widgetsGV, "panels"), "spec.conversion "+what+": unserved is still refused")
	}
}

// What agentiko-sandbox's validator does at the rule's edges (the review of its #77, each line RUN
// there), one row per line: the fake answers each the same way.
func TestCRDRule_TheSandboxsEdges(t *testing.T) {
	panels := namesJ(eGV, "panels")
	refusedNoCRD, refusedUnserved := noCRD(eGV, "panels"), unserved(eGV, "panels")
	for _, c := range []struct{ what, resources, objects, want string }{
		// spec.versions empty, null, a string or an object: not served, refused.
		{"spec.versions empty", panels, `[` + panelsJ(`[]`) + `]`, refusedUnserved},
		{"spec.versions null", panels, `[` + panelsJ(`null`) + `]`, refusedUnserved},
		{"spec.versions a string", panels, `[` + panelsJ(`"v1"`) + `]`, refusedUnserved},
		{"spec.versions an object", panels, `[` + panelsJ(`{"v1":{"name":"v1","served":true}}`) + `]`, refusedUnserved},
		{"spec.versions absent", panels, `[` + crdJ("panels."+eG, `{"group":"`+eG+`","names":{"plural":"panels"}}`) + `]`, refusedUnserved},

		// An entry without `name`, or a null entry: ignored; other entries still count.
		{"an entry without name, alone", panels, `[` + panelsJ(`[{"served":true}]`) + `]`, refusedUnserved},
		{"a null entry, alone", panels, `[` + panelsJ(`[null]`) + `]`, refusedUnserved},
		{"an entry without name, then the version", panels, `[` + panelsJ(`[{"served":true},{"name":"v1","served":true}]`) + `]`, ""},
		{"a null entry, then the version", panels, `[` + panelsJ(`[null,{"name":"v1","served":true}]`) + `]`, ""},
		{"the version, then a null entry and one without name", panels, `[` + panelsJ(`[{"name":"v1","served":true},null,{"served":false}]`) + `]`, ""},
		{"an entry that is a string, then the version", panels, `[` + panelsJ(`["v1",{"name":"v1","served":true}]`) + `]`, ""},
		{"a name that is not a string", panels, `[` + panelsJ(`[{"name":1,"served":true}]`) + `]`, refusedUnserved},

		// `served` must be the JSON boolean true; missing, "true", 1 and null are refused.
		{"served true", panels, `[` + panelsJ(served) + `]`, ""},
		{"served missing", panels, `[` + panelsJ(`[{"name":"v1"}]`) + `]`, refusedUnserved},
		{`served "true"`, panels, `[` + panelsJ(`[{"name":"v1","served":"true"}]`) + `]`, refusedUnserved},
		{"served 1", panels, `[` + panelsJ(`[{"name":"v1","served":1}]`) + `]`, refusedUnserved},
		{"served null", panels, `[` + panelsJ(`[{"name":"v1","served":null}]`) + `]`, refusedUnserved},
		{"served false", panels, `[` + panelsJ(`[{"name":"v1","served":false}]`) + `]`, refusedUnserved},
		{"served an object", panels, `[` + panelsJ(`[{"name":"v1","served":{}}]`) + `]`, refusedUnserved},
		{"storage true and served missing", panels, `[` + panelsJ(`[{"name":"v1","storage":true}]`) + `]`, refusedUnserved},

		// Version name exact and case-sensitive; the listed version is everything after the FIRST `/`.
		{"the version in another case", panels, `[` + panelsJ(`[{"name":"V1","served":true}]`) + `]`, refusedUnserved},
		{"a version that starts like it", panels, `[` + panelsJ(`[{"name":"v1beta1","served":true}]`) + `]`, refusedUnserved},
		{"a version it starts like", namesJ(eG+"/v1beta1", "panels"), `[` + panelsJ(served) + `]`, unserved(eG+"/v1beta1", "panels")},
		{"a key with two slashes: the version is the rest", namesJ(eG+"/v1/x", "panels"), `[` + panelsJ(`[{"name":"v1/x","served":true}]`) + `]`, ""},
		{"a key with two slashes, v1 served", namesJ(eG+"/v1/x", "panels"), `[` + panelsJ(served) + `]`, unserved(eG+"/v1/x", "panels")},
		{"a key with two slashes, x served", namesJ(eG+"/v1/x", "panels"), `[` + panelsJ(`[{"name":"x","served":true}]`) + `]`, unserved(eG+"/v1/x", "panels")},

		// Duplicate version names with different served values: accepted if ANY entry has served true.
		{"the version twice, served then not", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":"v1","served":false}]`) + `]`, ""},
		{"the version twice, not served then served", panels, `[` + panelsJ(`[{"name":"v1","served":false},{"name":"v1","served":true}]`) + `]`, ""},
		{"the version twice, never served", panels, `[` + panelsJ(`[{"name":"v1","served":false},{"name":"v1"}]`) + `]`, refusedUnserved},

		// Two CRDs with the same name: the LAST one in the object list decides.
		{"two CRDs of the name, the last serves", panels, `[` + panelsJ(`[]`) + `,` + panelsJ(served) + `]`, ""},
		{"two CRDs of the name, the last does not serve", panels, `[` + panelsJ(served) + `,` + panelsJ(`[]`) + `]`, refusedUnserved},

		// Subresource mapped to the part before the FIRST `/`.
		{"panels/status/extra is panels'", namesJ(eGV, "panels/status/extra"), `[` + panelsJ(served) + `]`, ""},
		{"panels/status/extra, no CRD", namesJ(eGV, "panels/status/extra"), `[]`, refusedNoCRD},
		{"panels//status is panels'", namesJ(eGV, "panels//status"), `[` + panelsJ(served) + `]`, ""},
		{"panels//status, no CRD", namesJ(eGV, "panels//status"), `[]`, refusedNoCRD},
		{"/status, no CRD matches", namesJ(eGV, "/status"), `[` + panelsJ(served) + `]`, noCRD(eGV, "")},
		{"an empty name, no CRD matches", namesJ(eGV, ""), `[` + panelsJ(served) + `]`, noCRD(eGV, "")},

		// A subresource whose CRD is present but whose parent entry is absent from the list: accepted.
		{"a subresource alone, its CRD present", namesJ(eGV, "panels/status"), `[` + panelsJ(served) + `]`, ""},
		{"a subresource alone, no CRD", namesJ(eGV, "panels/status"), `[]`, refusedNoCRD},

		// A CRD counts only if kind is CustomResourceDefinition and its apiVersion's GROUP is
		// apiextensions.k8s.io (any version of that group); found by exact metadata.name.
		{"apiextensions.k8s.io/v1beta1", panels, `[` + crdJ("panels."+eG, specJ(eG, "panels", served), `"apiVersion":"apiextensions.k8s.io/v1beta1","kind":"CustomResourceDefinition"`) + `]`, ""},
		{"another group's CustomResourceDefinition", panels, `[` + crdJ("panels."+eG, specJ(eG, "panels", served), `"apiVersion":"example.io/v1","kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"a group that ends like it", panels, `[` + crdJ("panels."+eG, specJ(eG, "panels", served), `"apiVersion":"x.apiextensions.k8s.io/v1","kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"another kind", panels, `[` + crdJ("panels."+eG, specJ(eG, "panels", served), `"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinitionList"`) + `]`, refusedNoCRD},
		{"no kind", panels, `[` + crdJ("panels."+eG, specJ(eG, "panels", served), `"apiVersion":"apiextensions.k8s.io/v1"`) + `]`, refusedNoCRD},
		{"a name that ends like it", panels, `[` + crdJ("subpanels."+eG, specJ(eG, "panels", served)) + `]`, refusedNoCRD},
		{"a name that starts like it", panels, `[` + crdJ("panels."+eG+".x", specJ(eG, "panels", served)) + `]`, refusedNoCRD},
		{"the name in another case", panels, `[` + crdJ("Panels."+eG, specJ(eG, "panels", served)) + `]`, refusedNoCRD},
		{"a CRD whose name is not a string", panels, `[{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":1},"spec":` + specJ(eG, "panels", served) + `}]`, refusedNoCRD},
		{"a CRD with no metadata", panels, `[{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","spec":` + specJ(eG, "panels", served) + `}]`, refusedNoCRD},

		// A resource entry whose name is not a string, or a null entry: skipped.
		{"an entry whose name is a number", listJ(eGV, `{"name":1}`), `[]`, ""},
		{"an entry whose name is null", listJ(eGV, `{"name":null}`), `[]`, ""},
		{"an entry with no name", listJ(eGV, `{"kind":"Panel"}`), `[]`, ""},
		{"a null entry", listJ(eGV, `null`), `[]`, ""},
		{"skipped entries beside a listed one", listJ(eGV, `null`, `{"name":1}`, `{"name":"panels"}`, `{"name":["panels"]}`), `[]`, refusedNoCRD},
		{"a group version whose list is null", `{"` + eGV + `":null}`, `[]`, ""},
		{"a list with no resources", `{"` + eGV + `":{}}`, `[]`, ""},
		{"a list whose resources is null", `{"` + eGV + `":{"resources":null}}`, `[]`, ""},
		{"an empty list", listJ(eGV), `[]`, ""},
		{"no discovery.resources", `{}`, `[` + panelsJ(served) + `]`, ""},

		// A list whose `resources` is not a list: refused cleanly, naming the group version.
		{"resources an object", `{"` + eGV + `":{"resources":{"name":"panels"}}}`, `[]`, `error: discovery.resources["` + eGV + `"]: resources is not a list`},
		{"resources a string", `{"` + eGV + `":{"resources":"panels"}}`, `[]`, `error: discovery.resources["` + eGV + `"]: resources is not a list`},
		{"resources a number", `{"` + eGV + `":{"resources":1}}`, `[]`, `error: discovery.resources["` + eGV + `"]: resources is not a list`},
		{"resources true", `{"` + eGV + `":{"resources":true}}`, `[]`, `error: discovery.resources["` + eGV + `"]: resources is not a list`},
	} {
		eq(t, crdRule(t, c.resources, c.objects), c.want, c.what)
	}
}

// The edges that reach New: a list that is not one is refused by name, and a null entry is skipped.
func TestCRDRule_TheEdgesAtLoad(t *testing.T) {
	resources := func(gv string, v any) func(map[string]any) {
		return func(s map[string]any) {
			s["discovery"].(map[string]any)["resources"].(map[string]any)[gv] = map[string]any{"kind": "APIResourceList", "groupVersion": gv, "resources": v}
		}
	}
	for what, v := range map[string]any{"an object": map[string]any{"name": "panels"}, "a string": "panels", "a number": 1} {
		eq(t, refusal(t, resources(widgetsGV, v)), `snapshot.json: discovery.resources["`+widgetsGV+`"]: resources is not a list`, "resources "+what)
	}
	eq(t, refusal(t, resources(widgetsGV, nil)), "", "resources null")
	eq(t, refusal(t, resources(widgetsGV, []any{nil})), "", "a null entry is skipped")
	eq(t, refusal(t, resources(widgetsGV, []any{nil, apires("panels", "Panel", true)})), crdRulePrefix+noCRD(widgetsGV, "panels"), "a null entry beside a listed type")
	eq(t, refusal(t, resources(widgetsGV, []any{apires("", "Panel", true)})), crdRulePrefix+noCRD(widgetsGV, ""), "an empty name")
}

func TestCRDRule_NothingListedAsksNothing(t *testing.T) {
	eq(t, len(crdlessTypes(nil, nil)), 0, "no discovery at all")
	eq(t, len(crdlessTypes(map[string][]string{}, []map[string]any{{"kind": "CustomResourceDefinition"}})), 0, "an empty discovery")
	eq(t, refusal(t, listing(widgetsGV)), "", "a custom group version that lists nothing")
	// The fixture itself: its custom types (incidents, alerts and their status) have their CRDs.
	eq(t, refusal(t, nil), "", "testdata loads")
}
