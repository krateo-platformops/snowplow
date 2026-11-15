//go:build js && wasm

package fakeapiserver

// SNAPSHOT.md §3's one snapshot rule at load (#1464): New refuses a snapshot whose discovery lists a
// resource of a group that is not built in without its CRD among the objects: one of that name,
// group and plural that serves the listed version. agentiko-sandbox keeps the same rule in
// snowplow-wasm/snapshot-rules.js and capture/pipeline/common.mjs; the groups below are its own
// test's, the words are checked against the source, and TestCRDRule_TheSandboxsEdges is what its
// validator does at the rule's edges, row for row.

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
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

// noCRD is the refusal of a listed type with no CRD; wrongCRD, of one whose CRD is there and
// wrong, with what is wrong with it; unserved is wrongCRD for a CRD that only does not serve the
// version.
func noCRD(gv, plural string) string {
	g, _ := splitGV(gv)
	return gv + " " + plural + " (no CustomResourceDefinition " + plural + "." + g + ")"
}

func wrongCRD(gv, plural, what string) string {
	g, _ := splitGV(gv)
	return gv + " " + plural + " (CustomResourceDefinition " + plural + "." + g + ": " + what + ")"
}

func unserved(gv, plural string) string {
	_, v := splitGV(gv)
	return wrongCRD(gv, plural, "does not serve "+v)
}

// misnamed is snapshotCRDs' refusal of a CRD whose name is not its spec's.
func misnamed(name, spec string) string {
	return `CustomResourceDefinition "` + name + `": metadata.name is not <spec.names.plural>.<spec.group>, "` + spec + `"`
}

// The three steps on JSON as a snapshot holds it: discovery.resources and the objects. The answer
// is "" when the snapshot keeps the rule, or the refusal of the first step that has one, as New
// gives it without the file's name.
func crdRule(t *testing.T, resources, objects string) string {
	t.Helper()
	read, err := strictJSON([]byte(`{"discovery":{"resources":`+resources+`}}`), reflect.TypeOf(Snapshot{}))
	if err != nil {
		return "strict: " + err.Error()
	}
	listed, problems := listedResources(read)
	if len(problems) > 0 {
		return strings.Join(problems, "; ")
	}
	var objs []map[string]any
	if err := json.Unmarshal([]byte(objects), &objs); err != nil {
		t.Fatalf("the test's objects %s: %v", objects, err)
	}
	_, crds, problems := snapshotCRDs(objs)
	if len(problems) > 0 {
		return "objects: " + strings.Join(problems, "; ")
	}
	return strings.Join(crdlessTypes(listed, crds), "; ")
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

// goodJ is the CRD of that name (its spec the name's two parts) with these spec.versions.
func goodJ(name, versions string, rest ...string) string {
	plural, group, _ := strings.Cut(name, ".")
	return crdJ(name, specJ(group, plural, versions), rest...)
}

// panelsJ is panels.w.example.io with these spec.versions; served is the one that serves v1.
func panelsJ(versions string) string { return goodJ("panels."+eG, versions) }

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
	// So does an aggregated API's group that is not *.k8s.io: the sentence knows no other built-in.
	for _, g := range []string{"observability.krateo.io", "zendesk.ogen.krateo.io", "k8s.io", "k8s.io.example", "x-k8s.io.evil", "apps.example", "xk8s.io", "example.K8S.IO", "metrics.example.com"} {
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
		"another group's same plural":     "panels.templates.krateo.io",
	} {
		// Each is a well-formed CRD, of the type its name says: not this type's.
		eq(t, refusal(t, listing(widgetsGV, "panels"), namedCRD(t, name)), crdRulePrefix+noCRD(widgetsGV, "panels"), what+" ("+name+")")
	}
}

func TestCRDRule_OnlyAnApiextensionsV1CustomResourceDefinitionCounts(t *testing.T) {
	for what, change := range map[string]func(map[string]any){
		"a CustomResourceDefinition of another group": func(o map[string]any) { o["apiVersion"] = "example.io/v1" },
		"a group that only ends like it":              func(o map[string]any) { o["apiVersion"] = "x.apiextensions.k8s.io/v1" },
		"an apiVersion with no group":                 func(o map[string]any) { o["apiVersion"] = "v1" },
		"another version of apiextensions.k8s.io":     func(o map[string]any) { o["apiVersion"] = "apiextensions.k8s.io/v1beta1" },
		"the bare group":                              func(o map[string]any) { o["apiVersion"] = "apiextensions.k8s.io" },
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
	// flexes has its CRD, which serves v1beta1 alone; grids' names v1 twice; the built-in apps/v1
	// asks none. Every other type is named, each group version's own, in the order of the group
	// versions and then of the plurals, whatever order the snapshot lists them in.
	want := crdRulePrefix + strings.Join([]string{
		noCRD("a.example.io/v1", "gadgets"),
		unserved(widgetsGroup+"/v1alpha1", "flexes"),
		noCRD(widgetsGroup+"/v1alpha1", "panels"),
		wrongCRD(widgetsGV, "grids", `spec.versions names "v1" more than once`),
		noCRD(widgetsGV, "layouts"),
		noCRD(widgetsGV, "panels"),
	}, "; ")
	for i := 0; i < 20; i++ { // a map's order differs from one load to the next
		grids := versionedCRD(t, "grids."+widgetsGroup, sv{"v1beta1", true}, sv{"v1", true}, sv{"v1", false})
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

// Every CustomResourceDefinition among the objects, listed or not, is named for its spec:
// metadata.name is <spec.names.plural>.<spec.group>, and no two have one name. The fake keys a
// CRD's schema by spec.group and spec.names.plural, so one named otherwise would give its schema to
// a type that is not the one it is named for, and leave that one with none.
func TestCRDRule_ACRDIsNamedForItsSpec(t *testing.T) {
	name := "panels." + eG
	list := namesJ(eGV, "panels", "panels/status")
	notAString := func(what string) string { return `objects: CustomResourceDefinition "` + name + `": ` + what }
	for _, c := range []struct{ what, spec, want string }{
		{"named for its spec", specJ(eG, "panels", served), ""},
		{"spec.group wrong", specJ("x.example.io", "panels", served), "objects: " + misnamed(name, "panels.x.example.io")},
		{"spec.group in another case", specJ("W.example.io", "panels", served), "objects: " + misnamed(name, "panels.W.example.io")},
		{"spec.names.plural wrong", specJ(eG, "panelsets", served), "objects: " + misnamed(name, "panelsets."+eG)},
		{"spec.names.plural the singular", specJ(eG, "panel", served), "objects: " + misnamed(name, "panel."+eG)},
		{"both wrong", specJ("x.example.io", "boards", served), "objects: " + misnamed(name, "boards.x.example.io")},
		{"both wrong and no version", specJ("x.example.io", "boards", `[]`), "objects: " + misnamed(name, "boards.x.example.io")},
		{"spec.names.plural empty", specJ(eG, "", served), "objects: " + misnamed(name, "."+eG)},
		{"spec.group empty", specJ("", "panels", served), "objects: " + misnamed(name, "panels.")},
		{"spec.names missing", `{"group":"` + eG + `","versions":` + served + `}`, notAString("spec.names.plural is not a string")},
		{"spec.names null", `{"group":"` + eG + `","names":null,"versions":` + served + `}`, notAString("spec.names.plural is not a string")},
		{"spec.names a string", `{"group":"` + eG + `","names":"panels","versions":` + served + `}`, notAString("spec.names.plural is not a string")},
		{"spec.names a list", `{"group":"` + eG + `","names":["panels"],"versions":` + served + `}`, notAString("spec.names.plural is not a string")},
		{"spec.names without plural", `{"group":"` + eG + `","names":{"kind":"Panel"},"versions":` + served + `}`, notAString("spec.names.plural is not a string")},
		{"spec.names.plural not a string", `{"group":"` + eG + `","names":{"plural":1},"versions":` + served + `}`, notAString("spec.names.plural is not a string")},
		{"spec.group missing", `{"names":{"plural":"panels"},"versions":` + served + `}`, notAString("spec.group is not a string")},
		{"spec.group null", `{"group":null,"names":{"plural":"panels"},"versions":` + served + `}`, notAString("spec.group is not a string")},
		{"spec.group not a string", `{"group":1,"names":{"plural":"panels"},"versions":` + served + `}`, notAString("spec.group is not a string")},
		{"no spec", `null`, notAString("spec.group is not a string and spec.names.plural is not a string")},
		{"a spec that is a string", `"panels"`, notAString("spec.group is not a string and spec.names.plural is not a string")},
	} {
		eq(t, crdRule(t, list, `[`+crdJ(name, c.spec)+`]`), c.want, c.what)
		// The same CRD when nothing lists its type, and when a built-in group version does.
		eq(t, crdRule(t, `{}`, `[`+crdJ(name, c.spec)+`]`), c.want, c.what+", listed by nothing")
		eq(t, crdRule(t, namesJ("apps/v1", "panels"), `[`+crdJ(name, c.spec)+`]`), c.want, c.what+", a built-in group version beside it")
	}
	crd := `"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition"`
	for _, c := range []struct{ what, object string }{
		{"no metadata", `{` + crd + `,"spec":` + specJ(eG, "panels", served) + `}`},
		{"metadata null", `{` + crd + `,"metadata":null,"spec":` + specJ(eG, "panels", served) + `}`},
		{"metadata a string", `{` + crd + `,"metadata":"` + name + `","spec":` + specJ(eG, "panels", served) + `}`},
		{"no metadata.name", `{` + crd + `,"metadata":{},"spec":` + specJ(eG, "panels", served) + `}`},
		{"metadata.name a number", `{` + crd + `,"metadata":{"name":1},"spec":` + specJ(eG, "panels", served) + `}`},
		{"metadata.name null", `{` + crd + `,"metadata":{"name":null},"spec":` + specJ(eG, "panels", served) + `}`},
	} {
		eq(t, crdRule(t, `{}`, `[`+c.object+`]`), "objects: a CustomResourceDefinition: metadata.name is not a string", c.what)
	}

	// No two CRDs have one name: none decides for the other.
	good, bad, unserving := crdJ(name, specJ(eG, "panels", served)), crdJ(name, specJ("x.example.io", "panels", served)), crdJ(name, specJ(eG, "panels", `[]`))
	twice := `objects: CustomResourceDefinition "` + name + `": two objects have this name`
	eq(t, crdRule(t, list, `[`+good+`,`+good+`]`), twice, "the same CRD twice")
	eq(t, crdRule(t, list, `[`+unserving+`,`+good+`]`), twice, "one that does not serve the version, then one that does")
	eq(t, crdRule(t, list, `[`+good+`,`+unserving+`]`), twice, "one that serves the version, then one that does not")
	eq(t, crdRule(t, `{}`, `[`+good+`,`+good+`]`), twice, "the same CRD twice, listed by nothing")
	eq(t, crdRule(t, list, `[`+good+`,`+bad+`]`), "objects: "+misnamed(name, "panels.x.example.io")+" and two objects have this name", "a well-named CRD, then a misnamed one of its name")
	eq(t, crdRule(t, list, `[`+bad+`,`+good+`]`), "objects: "+misnamed(name, "panels.x.example.io")+"; "+twice[len("objects: "):], "a misnamed CRD, then a well-named one of its name")
	eq(t, crdRule(t, list, `[`+good+`,`+goodJ("boards."+eG, served)+`,`+good+`,`+goodJ("boards."+eG, served)+`]`),
		`objects: CustomResourceDefinition "boards.`+eG+`": two objects have this name; `+twice[len("objects: "):], "two names, each twice: both are said, sorted")

	// An object that is not a CRD (another apiVersion, another kind) is not asked this.
	for what, head := range map[string]string{
		"apiextensions.k8s.io/v1beta1": `"apiVersion":"apiextensions.k8s.io/v1beta1","kind":"CustomResourceDefinition"`,
		"another group":                `"apiVersion":"example.io/v1","kind":"CustomResourceDefinition"`,
		"another kind":                 `"apiVersion":"apiextensions.k8s.io/v1","kind":"Panel"`,
	} {
		eq(t, crdRule(t, `{}`, `[`+crdJ(name, specJ("x.example.io", "boards", served), head)+`,`+crdJ(name, specJ("x.example.io", "boards", served), head)+`]`), "", what+": not a CRD")
	}

	// One name cut in two places: a.b.c.d is a's CRD in group b.c.d and a.b's in group c.d. A CRD
	// named for its spec can still be of another group and plural than the listed type's, and is
	// refused with everything that is wrong with it.
	cut := crdJ("a.b.c.d", specJ("c.d", "a.b", `[{"name":"v1","served":true}]`))
	spec := `spec.group is "c.d", expected "b.c.d" and spec.names.plural is "a.b", expected "a"`
	eq(t, crdRule(t, namesJ("c.d/v1", "a.b"), `[`+cut+`]`), "", "a.b of group c.d: its CRD")
	eq(t, crdRule(t, namesJ("c.d/v2", "a.b"), `[`+cut+`]`), unserved("c.d/v2", "a.b"), "a.b of group c.d, a version it does not serve")
	eq(t, crdRule(t, namesJ("b.c.d/v1", "a", "a/status"), `[`+cut+`]`), wrongCRD("b.c.d/v1", "a", spec), "a of group b.c.d: the CRD of that name is another type's")
	eq(t, crdRule(t, namesJ("b.c.d/v2", "a"), `[`+cut+`]`), wrongCRD("b.c.d/v2", "a", spec+" and does not serve v2"), "and it does not serve the version: both are said")
	eq(t, crdRule(t, `{"b.c.d/v1":{"resources":[{"name":"a"}]},"b.c.d/v2":{"resources":[{"name":"a/status"}]}}`, `[`+cut+`]`),
		wrongCRD("b.c.d/v1", "a", spec)+"; "+wrongCRD("b.c.d/v2", "a", spec+" and does not serve v2"), "every group version's listing")
	eq(t, crdRule(t, namesJ("b.c.d/v2", "a"), `[`+crdJ("a.b.c.d", specJ("c.d", "a.b", `[{"name":"v1"},{"name":"v1"}]`))+`]`),
		wrongCRD("b.c.d/v2", "a", `spec.versions names "v1" more than once`), "and it names a version twice: that alone is said")
}

// The same through New, on a real CRD.
func TestCRDRule_AMisnamedCRDIsRefusedAtLoad(t *testing.T) {
	list := listing(widgetsGV, "panels", "panels/status")
	spec := func(crd map[string]any) map[string]any { return crd["spec"].(map[string]any) }
	name := "panels." + widgetsGroup

	crd := namedCRD(t, name)
	spec(crd)["group"] = zrGroup
	eq(t, refusal(t, list, crd), "objects: "+misnamed(name, "panels."+zrGroup), "spec.group wrong")

	crd = namedCRD(t, name)
	spec(crd)["names"].(map[string]any)["plural"] = "zendeskrequests"
	eq(t, refusal(t, list, crd), "objects: "+misnamed(name, "zendeskrequests."+widgetsGroup), "spec.names.plural wrong")

	// 057's CRD as it is, under the listed type's name: what the fake took for the type's CRD
	// before this rule, and served the type with no schema.
	crd = zrCRD(t, "zendeskrequests")
	crd["metadata"].(map[string]any)["name"] = name
	eq(t, refusal(t, listing(widgetsGroup+"/v2", "panels"), crd), "objects: "+misnamed(name, "zendeskrequests."+zrGroup), "both wrong")
	// The same when nothing lists the type its name says.
	eq(t, refusal(t, nil, crd), "objects: "+misnamed(name, "zendeskrequests."+zrGroup), "both wrong, listed by nothing")

	crd = namedCRD(t, name)
	delete(spec(crd), "names")
	eq(t, refusal(t, list, crd), `objects: CustomResourceDefinition "`+name+`": spec.names.plural is not a string`, "spec.names missing")

	crd = namedCRD(t, name)
	delete(spec(crd), "group")
	eq(t, refusal(t, list, crd), `objects: CustomResourceDefinition "`+name+`": spec.group is not a string`, "spec.group missing")

	eq(t, refusal(t, list, namedCRD(t, name), namedCRD(t, name)), `objects: CustomResourceDefinition "`+name+`": two objects have this name`, "two CRDs of one name")
	// The fixture's own CRD a second time, on another page.
	eq(t, refusal(t, nil, map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "incidents.observability.krateo.io"},
		"spec": map[string]any{"group": "observability.krateo.io", "names": map[string]any{"plural": "incidents"}}}),
		`objects: CustomResourceDefinition "incidents.observability.krateo.io": two objects have this name`, "a fixture CRD's name a second time")
}

// The loader registers a CRD's schema under its spec.group and spec.names.plural. Only a CRD the
// rule's steps passed reaches that: an object of the kind and another apiVersion is no CRD to the
// rule (isCRD) and none to the loader.
func TestCRDRule_OnlyACheckedCRDIsRegistered(t *testing.T) {
	registered := func(h *harness) []string {
		var grs []string
		for gr, info := range h.s.crds {
			grs = append(grs, gr+"="+info.kind)
		}
		for gr := range h.s.custom {
			if h.s.crds[gr] == nil {
				grs = append(grs, gr+" (custom, no schema)")
			}
		}
		for gr := range h.s.selectable {
			if !h.s.custom[gr] {
				grs = append(grs, gr+" (selectable fields, not custom)")
			}
		}
		return sortedCopy(grs)
	}
	fixture := registered(newHarness(t, opts{}))
	eq(t, strings.Join(fixture, " "), "observability.krateo.io/alerts=Alert observability.krateo.io/incidents=Incident", "the fixture's CRDs, by group and plural, each with its kind")

	// A group without a dot is built in, so its list needs no CRD and may hold any kind.
	legacy := withResources("legacy/v1", apires("customresourcedefinitions", "CustomResourceDefinition", false))
	impostor := namedCRD(t, "panels."+widgetsGroup)
	impostor["apiVersion"] = "legacy/v1"
	impostor["spec"].(map[string]any)["group"] = "observability.krateo.io"
	impostor["spec"].(map[string]any)["names"].(map[string]any)["plural"] = "incidents"
	h, err := loadHarness(t, opts{edit: legacy, extra: []map[string]any{impostor}})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, strings.Join(registered(h), " "), strings.Join(fixture, " "), "an object of the kind and another apiVersion, of a registered CRD's group and plural, replaces nothing (its kind is ZendeskRequest)")
	impostor = namedCRD(t, "things.impostor.example.io")
	impostor["apiVersion"] = "legacy/v1"
	if h, err = loadHarness(t, opts{edit: legacy, extra: []map[string]any{impostor}}); err != nil {
		t.Fatal(err)
	}
	eq(t, strings.Join(registered(h), " "), strings.Join(fixture, " "), "an object of the kind and another apiVersion, of a group and plural of its own, registers nothing")
	_, stored := h.s.store["legacy/customresourcedefinitions"]["/things.impostor.example.io"]
	eq(t, stored, true, "it is stored, as the plain object it is")

	// A checked CRD is registered under the type its name says.
	h, err = loadHarness(t, opts{edit: listing(widgetsGV, "panels"), extra: []map[string]any{namedCRD(t, "panels."+widgetsGroup)}})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, strings.Join(registered(h), " "), strings.Join(append(append([]string{}, fixture...), widgetsGroup+"/panels=ZendeskRequest"), " "), "a CRD is registered under its name's group and plural")
}

func sortedCopy(in []string) []string {
	out := append([]string{}, in...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
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

// What agentiko-sandbox's validator does at the rule's edges, one row per line: the fake answers
// each the same way (refused or not; the words are its own).
func TestCRDRule_TheSandboxsEdges(t *testing.T) {
	panels := namesJ(eGV, "panels")
	refusedNoCRD, refusedUnserved := noCRD(eGV, "panels"), unserved(eGV, "panels")
	twice := func(name string) string {
		return wrongCRD(eGV, "panels", "spec.versions names "+name+" more than once")
	}
	notAList := func(gv string) string {
		return `discovery.resources["` + gv + `"] is not an APIResourceList with a resources array`
	}
	noName := func(gv string, i int) string {
		return `discovery.resources["` + gv + `"]: resources[` + strconv.Itoa(i) + `]: name is not a non-empty string`
	}
	v1beta1 := `"apiVersion":"apiextensions.k8s.io/v1beta1","kind":"CustomResourceDefinition"`
	for _, c := range []struct{ what, resources, objects, want string }{
		// spec.versions empty, null, a string or an object: not served, refused.
		{"spec.versions empty", panels, `[` + panelsJ(`[]`) + `]`, refusedUnserved},
		{"spec.versions null", panels, `[` + panelsJ(`null`) + `]`, refusedUnserved},
		{"spec.versions a string", panels, `[` + panelsJ(`"v1"`) + `]`, refusedUnserved},
		{"spec.versions an object", panels, `[` + panelsJ(`{"v1":{"name":"v1","served":true}}`) + `]`, refusedUnserved},
		{"spec.versions absent", panels, `[` + crdJ("panels."+eG, `{"group":"`+eG+`","names":{"plural":"panels"}}`) + `]`, refusedUnserved},

		// A spec.versions entry without `name`, or a null entry: ignored; other entries still count.
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

		// Two spec.versions entries with one name: the CRD a listed type needs is malformed, and that
		// alone is said. Any name two entries share (JavaScript's ===), the listed version's or not, a
		// string or not; entries with no name share none.
		{"the version twice, served then not", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":"v1","served":false}]`) + `]`, twice(`"v1"`)},
		{"the version twice, not served then served", panels, `[` + panelsJ(`[{"name":"v1","served":false},{"name":"v1","served":true}]`) + `]`, twice(`"v1"`)},
		{"the version twice, served twice", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":"v1","served":true}]`) + `]`, twice(`"v1"`)},
		{"the version three times", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":"v1","served":true},{"name":"v1","served":true}]`) + `]`, twice(`"v1"`)},
		{"the version twice, never served: that alone is said", panels, `[` + panelsJ(`[{"name":"v1","served":false},{"name":"v1"}]`) + `]`, twice(`"v1"`)},
		{"another version twice, the listed one served", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":"v2","served":true},{"name":"v2","served":false}]`) + `]`, twice(`"v2"`)},
		{"two versions twice: the first found is said", panels, `[` + panelsJ(`[{"name":"v2"},{"name":"v1","served":true},{"name":"v2"},{"name":"v1"}]`) + `]`, twice(`"v2"`)},
		{"two entries named 1", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":1},{"name":1}]`) + `]`, twice(`1`)},
		{"entries named 1 and 1.0: one number", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":1},{"name":1.0}]`) + `]`, twice(`1`)},
		{"entries named 0 and -0: one number", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":0},{"name":-0}]`) + `]`, twice(`0`)},
		{"two entries named null", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":null},{"name":null}]`) + `]`, twice(`null`)},
		{"two entries named true", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":true},{"name":true}]`) + `]`, twice(`true`)},
		{`entries named 1 and "1": two names`, panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":1},{"name":"1"}]`) + `]`, ""},
		{"entries named true and false: two names", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":true},{"name":false}]`) + `]`, ""},
		{"two entries with no name", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"served":true},{"served":false}]`) + `]`, ""},
		{"two null entries", panels, `[` + panelsJ(`[{"name":"v1","served":true},null,null]`) + `]`, ""},
		{"two entries that are the same string", panels, `[` + panelsJ(`[{"name":"v1","served":true},"v1","v1"]`) + `]`, ""},
		{"two entries named by equal objects", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":{}},{"name":{}}]`) + `]`, ""},
		{"two entries named by equal lists", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":["v2"]},{"name":["v2"]}]`) + `]`, ""},
		{"a CRD no listed type needs names a version twice", panels, `[` + panelsJ(served) + `,` + goodJ("boards."+eG, `[{"name":"v1","served":true},{"name":"v1","served":true}]`) + `]`, ""},
		{"a built-in group's type's CRD names a version twice", namesJ("apps/v1", "boards"), `[` + goodJ("boards.apps", `[{"name":"v1"},{"name":"v1"}]`) + `]`, ""},

		// Subresource mapped to the part before the FIRST `/`.
		{"panels/status/extra is panels'", namesJ(eGV, "panels/status/extra"), `[` + panelsJ(served) + `]`, ""},
		{"panels/status/extra, no CRD", namesJ(eGV, "panels/status/extra"), `[]`, refusedNoCRD},
		{"panels//status is panels'", namesJ(eGV, "panels//status"), `[` + panelsJ(served) + `]`, ""},
		{"panels//status, no CRD", namesJ(eGV, "panels//status"), `[]`, refusedNoCRD},
		{"/status, no CRD matches", namesJ(eGV, "/status"), `[` + panelsJ(served) + `]`, noCRD(eGV, "")},

		// A subresource whose CRD is present but whose parent entry is absent from the list: accepted.
		{"a subresource alone, its CRD present", namesJ(eGV, "panels/status"), `[` + panelsJ(served) + `]`, ""},
		{"a subresource alone, no CRD", namesJ(eGV, "panels/status"), `[]`, refusedNoCRD},

		// A CRD counts only if kind is CustomResourceDefinition and its apiVersion is exactly
		// apiextensions.k8s.io/v1; found by exact metadata.name.
		{"apiextensions.k8s.io/v1", panels, `[` + panelsJ(served) + `]`, ""},
		{"apiextensions.k8s.io/v1beta1", panels, `[` + goodJ("panels."+eG, served, v1beta1) + `]`, refusedNoCRD},
		{"the bare group apiextensions.k8s.io", panels, `[` + goodJ("panels."+eG, served, `"apiVersion":"apiextensions.k8s.io","kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"apiextensions.k8s.io/v1/x", panels, `[` + goodJ("panels."+eG, served, `"apiVersion":"apiextensions.k8s.io/v1/x","kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"apiextensions.k8s.io/V1", panels, `[` + goodJ("panels."+eG, served, `"apiVersion":"apiextensions.k8s.io/V1","kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"no apiVersion", panels, `[` + goodJ("panels."+eG, served, `"kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"another group's CustomResourceDefinition", panels, `[` + goodJ("panels."+eG, served, `"apiVersion":"example.io/v1","kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"a group that ends like it", panels, `[` + goodJ("panels."+eG, served, `"apiVersion":"x.apiextensions.k8s.io/v1","kind":"CustomResourceDefinition"`) + `]`, refusedNoCRD},
		{"another kind", panels, `[` + goodJ("panels."+eG, served, `"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinitionList"`) + `]`, refusedNoCRD},
		{"no kind", panels, `[` + goodJ("panels."+eG, served, `"apiVersion":"apiextensions.k8s.io/v1"`) + `]`, refusedNoCRD},
		{"a name that ends like it", panels, `[` + goodJ("subpanels."+eG, served) + `]`, refusedNoCRD},
		{"a name that starts like it", panels, `[` + goodJ("panels."+eG+".x", served) + `]`, refusedNoCRD},
		{"the name in another case", panels, `[` + goodJ("Panels."+eG, served) + `]`, refusedNoCRD},

		// Every CRD, listed or not, is named <spec.names.plural>.<spec.group>, and no two have a name.
		{"a misnamed CRD nothing lists", panels, `[` + panelsJ(served) + `,` + crdJ("boards."+eG, specJ(eG, "cards", served)) + `]`, "objects: " + misnamed("boards."+eG, "cards."+eG)},
		{"two CRDs of the name, the last serves", panels, `[` + panelsJ(`[]`) + `,` + panelsJ(served) + `]`, `objects: CustomResourceDefinition "panels.` + eG + `": two objects have this name`},
		{"two CRDs of the name, the last does not serve", panels, `[` + panelsJ(served) + `,` + panelsJ(`[]`) + `]`, `objects: CustomResourceDefinition "panels.` + eG + `": two objects have this name`},
		{"two CRDs of a name nothing lists", panels, `[` + panelsJ(served) + `,` + goodJ("boards."+eG, served) + `,` + goodJ("boards."+eG, served) + `]`, `objects: CustomResourceDefinition "boards.` + eG + `": two objects have this name`},

		// A resource entry whose name is not a non-empty string refuses the snapshot, naming the
		// group version and the entry, in every group version, the built-in ones too.
		{"an entry whose name is a number", listJ(eGV, `{"name":1}`), `[]`, noName(eGV, 0)},
		{"an entry whose name is null", listJ(eGV, `{"name":null}`), `[]`, noName(eGV, 0)},
		{"an entry whose name is a list", listJ(eGV, `{"name":["panels"]}`), `[]`, noName(eGV, 0)},
		{"an entry whose name is empty", listJ(eGV, `{"name":""}`), `[]`, noName(eGV, 0)},
		{"an entry with no name", listJ(eGV, `{"kind":"Panel"}`), `[]`, noName(eGV, 0)},
		{"a null entry", listJ(eGV, `null`), `[]`, noName(eGV, 0)},
		{"an entry that is a string", listJ(eGV, `"panels"`), `[]`, noName(eGV, 0)},
		{"an entry that is a list", listJ(eGV, `["panels"]`), `[]`, noName(eGV, 0)},
		{"a null entry after two listed ones, their CRD present", listJ(eGV, `{"name":"panels"}`, `{"name":"panels/status"}`, `null`), `[` + panelsJ(served) + `]`, noName(eGV, 2)},
		{"two entries with no name: both are said", listJ(eGV, `null`, `{"name":"panels"}`, `{}`), `[]`, noName(eGV, 0) + "; " + noName(eGV, 2)},
		{"an empty name in the core group", listJ("v1", `{"name":"pods"}`, `{"name":""}`), `[]`, noName("v1", 1)},
		{"a null entry in a built-in group", listJ("apps/v1", `null`), `[]`, noName("apps/v1", 0)},
		{"a name that is only a slash is a string", namesJ("apps/v1", "/"), `[]`, ""},

		// A group version that is not an APIResourceList with a resources array: refused by name, in
		// every group version, the built-in ones too.
		{"resources an object", `{"` + eGV + `":{"resources":{"name":"panels"}}}`, `[]`, notAList(eGV)},
		{"resources a string", `{"` + eGV + `":{"resources":"panels"}}`, `[]`, notAList(eGV)},
		{"resources a number", `{"` + eGV + `":{"resources":1}}`, `[]`, notAList(eGV)},
		{"resources true", `{"` + eGV + `":{"resources":true}}`, `[]`, notAList(eGV)},
		{"resources null", `{"` + eGV + `":{"resources":null}}`, `[]`, notAList(eGV)},
		{"no resources", `{"` + eGV + `":{}}`, `[]`, notAList(eGV)},
		{"no resources, the list's other fields there", `{"` + eGV + `":{"kind":"APIResourceList","groupVersion":"` + eGV + `"}}`, `[]`, notAList(eGV)},
		{"a group version that is null", `{"` + eGV + `":null}`, `[]`, notAList(eGV)},
		{"a group version that is a string", `{"` + eGV + `":"panels"}`, `[]`, notAList(eGV)},
		{"a group version that is a list", `{"` + eGV + `":[{"name":"panels"}]}`, `[]`, notAList(eGV)},
		{"a group version that is a number", `{"` + eGV + `":1}`, `[]`, notAList(eGV)},
		{"resources an object, in the core group", `{"v1":{"resources":{"name":"pods"}}}`, `[]`, notAList("v1")},
		{"resources a string, in a built-in group", `{"apps/v1":{"resources":"deployments"}}`, `[]`, notAList("apps/v1")},
		{"resources null, in a built-in group", `{"apps/v1":{"resources":null}}`, `[]`, notAList("apps/v1")},
		{"no resources, in the core group", `{"v1":{}}`, `[]`, notAList("v1")},
		{"a built-in group version that is null", `{"batch/v1":null}`, `[]`, notAList("batch/v1")},
		{"two group versions that are not lists: both are said", `{"v1":null,"` + eGV + `":{}}`, `[]`, notAList("v1") + "; " + notAList(eGV)},
		{"an empty list", listJ(eGV), `[]`, ""},
		{"an empty list in a built-in group", listJ("apps/v1"), `[]`, ""},
		{"no group version at all", `{}`, `[` + panelsJ(served) + `]`, ""},
		{"discovery.resources null", `null`, `[` + panelsJ(served) + `]`, ""},
		{"discovery.resources a list", `[]`, `[]`, "discovery.resources is not an object"},
		{"discovery.resources a string", `"v1"`, `[]`, "discovery.resources is not an object"},

		// A name that is not well-formed Unicode: an unpaired surrogate, which Go reads as U+FFFD and
		// JavaScript keeps, so two names could be one here and two there.
		{"a group version with an unpaired surrogate", namesJ(eG+`/v1\ud800`, "panels"), `[]`, `discovery.resources["` + eG + `/v1` + "\uFFFD" + `"] is not well-formed Unicode`},
		{"a resource name with an unpaired surrogate", namesJ(eGV, `panels\udc00`), `[` + goodJ(`panels\udc00.`+eG, served) + `]`, `discovery.resources["` + eGV + `"]: resources[0]: name "panels` + "\uFFFD" + `" is not well-formed Unicode`},
		{"a resource name with U+FFFD itself", namesJ(eGV, "panels\uFFFD"), `[]`, `discovery.resources["` + eGV + `"]: resources[0]: name "panels` + "\uFFFD" + `" is not well-formed Unicode`},
		{"a resource name with an unpaired surrogate, in a built-in group", namesJ("apps/v1", `pods\ud800`), `[]`, `discovery.resources["apps/v1"]: resources[0]: name "pods` + "\uFFFD" + `" is not well-formed Unicode`},
		{"a surrogate pair is a character", namesJ("apps/v1", `pods😀`), `[]`, ""},
		{"a CRD name with an unpaired surrogate", `{}`, `[` + goodJ(`boards\ud800.`+eG, served) + `]`, `objects: CustomResourceDefinition "boards` + "\uFFFD" + `.` + eG + `": a name in it is not well-formed Unicode`},
		{"a CRD's spec.group with an unpaired surrogate", `{}`, `[` + crdJ("boards."+eG, specJ(eG+`\ud800`, "boards", served)) + `]`,
			`objects: CustomResourceDefinition "boards.` + eG + `": a name in it is not well-formed Unicode and metadata.name is not <spec.names.plural>.<spec.group>, "boards.` + eG + "\uFFFD" + `"`},
		{"a CRD's version name with an unpaired surrogate", `{}`, `[` + goodJ("boards."+eG, `[{"name":"v1\udfff","served":true}]`) + `]`, `objects: CustomResourceDefinition "boards.` + eG + `": a name in it is not well-formed Unicode`},
		{"two version names that differ only in their unpaired surrogates", panels, `[` + panelsJ(`[{"name":"v1","served":true},{"name":"v\ud800"},{"name":"v\udc00"}]`) + `]`, `objects: CustomResourceDefinition "panels.` + eG + `": a name in it is not well-formed Unicode`},
	} {
		eq(t, crdRule(t, c.resources, c.objects), c.want, c.what)
	}
}

// The edges through New: each refusal is the rule's own, by name, never the decoder's.
func TestCRDRule_TheEdgesAtLoad(t *testing.T) {
	resources := func(gv string, v any) func(map[string]any) {
		return func(s map[string]any) {
			s["discovery"].(map[string]any)["resources"].(map[string]any)[gv] = map[string]any{"kind": "APIResourceList", "groupVersion": gv, "resources": v}
		}
	}
	groupVersion := func(gv string, v any) func(map[string]any) {
		return func(s map[string]any) { s["discovery"].(map[string]any)["resources"].(map[string]any)[gv] = v }
	}
	notAList := func(gv string) string {
		return `snapshot.json: discovery.resources["` + gv + `"] is not an APIResourceList with a resources array`
	}
	noName := func(gv string, i int) string {
		return `snapshot.json: discovery.resources["` + gv + `"]: resources[` + strconv.Itoa(i) + `]: name is not a non-empty string`
	}
	for what, v := range map[string]any{"an object": map[string]any{"name": "panels"}, "a string": "panels", "a number": 1, "null": nil} {
		eq(t, refusal(t, resources(widgetsGV, v)), notAList(widgetsGV), "resources "+what)
		eq(t, refusal(t, resources("apps/v1", v)), notAList("apps/v1"), "resources "+what+", in a built-in group")
	}
	for what, v := range map[string]any{"null": nil, "a string": "panels", "a list": []any{apires("panels", "Panel", true)}, "an object with no resources": map[string]any{"kind": "APIResourceList"}} {
		eq(t, refusal(t, groupVersion(widgetsGV, v)), notAList(widgetsGV), "a group version that is "+what)
		eq(t, refusal(t, groupVersion("v1", v)), notAList("v1"), "the core group version "+what)
	}
	eq(t, refusal(t, resources(widgetsGV, []any{})), "", "an empty list")

	eq(t, refusal(t, resources(widgetsGV, []any{nil})), noName(widgetsGV, 0), "a null entry")
	eq(t, refusal(t, resources(widgetsGV, []any{apires("panels", "Panel", true), nil}), namedCRD(t, "panels."+widgetsGroup)), noName(widgetsGV, 1), "a null entry after a listed type")
	eq(t, refusal(t, resources(widgetsGV, []any{apires("", "Panel", true)})), noName(widgetsGV, 0), "an empty name")
	eq(t, refusal(t, resources(widgetsGV, []any{map[string]any{"name": 1, "kind": "Panel"}})), noName(widgetsGV, 0), "a name that is a number: this refusal, not the decoder's")
	eq(t, refusal(t, resources(widgetsGV, []any{map[string]any{"kind": "Panel"}})), noName(widgetsGV, 0), "no name")
	eq(t, refusal(t, resources(widgetsGV, []any{"panels"})), noName(widgetsGV, 0), "an entry that is a string")
	eq(t, refusal(t, withResources("apps/v1", apires("", "Deployment", true))), noName("apps/v1", 1), "an empty name in a built-in group")
	eq(t, refusal(t, resources(widgetsGV, []any{apires("panels\uFFFD", "Panel", true)})),
		`snapshot.json: discovery.resources["`+widgetsGV+`"]: resources[0]: name "panels`+"\uFFFD"+`" is not well-formed Unicode`, "a name that is not well-formed Unicode")

	// A CRD that names a version twice, through New: the refusal is this rule's.
	crd := versionedCRD(t, "panels."+widgetsGroup, sv{"v1beta1", true}, sv{"v1beta1", false})
	eq(t, refusal(t, listing(widgetsGV, "panels", "panels/status"), crd), crdRulePrefix+wrongCRD(widgetsGV, "panels", `spec.versions names "v1beta1" more than once`), "the listed version named twice")
	// And nothing lists its type: the CRD is not read for this, and loads.
	eq(t, refusal(t, nil, versionedCRD(t, "panels."+widgetsGroup, sv{"v1beta1", true}, sv{"v1beta1", false})), "", "a version named twice in a CRD no listed type needs")
	// The snapshot is not a JSON object.
	for _, text := range []string{`[]`, `"snapshot"`, `null`, `1`} {
		_, err := loadHarness(t, opts{rawSnap: func(string) string { return text }})
		eq(t, err != nil && err.Error() == "snapshot.json: is not a JSON object", true, "snapshot.json that is "+text)
	}
}

func TestCRDRule_NothingListedAsksNothing(t *testing.T) {
	eq(t, len(crdlessTypes(nil, nil)), 0, "no discovery at all")
	eq(t, crdRule(t, `{}`, `[{"kind":"CustomResourceDefinition"}]`), "", "an empty discovery")
	eq(t, refusal(t, listing(widgetsGV)), "", "a custom group version that lists nothing")
	// The fixture itself: its custom types (incidents, alerts and their status) have their CRDs.
	eq(t, refusal(t, nil), "", "testdata loads")
}

// A loaded object holds its numbers as json.Number: two version names that are one number in
// JavaScript are one name, however each is written.
func TestCRDRule_VersionNamesAreComparedAsJavaScriptDoes(t *testing.T) {
	named := func(names ...any) []any {
		var vs []any
		for _, n := range names {
			vs = append(vs, map[string]any{"name": n})
		}
		return vs
	}
	for _, c := range []struct {
		what   string
		names  []any
		shared string
	}{
		{"1 and 1.0", []any{json.Number("1"), json.Number("1.0")}, "1"},
		{"1 and 1e0", []any{json.Number("1"), json.Number("1e0")}, "1"},
		{"0 and -0", []any{json.Number("0"), json.Number("-0")}, "0"},
		{"two numbers too large for a float: both Infinity", []any{json.Number("1e400"), json.Number("2e400")}, "+Inf"},
		{"two integers a float cannot tell apart", []any{json.Number("9007199254740992"), json.Number("9007199254740993")}, "9.007199254740992e+15"},
		{"1 and 2", []any{json.Number("1"), json.Number("2")}, ""},
		{`1 and "1"`, []any{json.Number("1"), "1"}, ""},
		{"a string twice", []any{"v1", "v2", "v1"}, `"v1"`},
		{"null twice", []any{nil, nil}, "null"},
		{"false twice", []any{false, true, false}, "false"},
		{"an object twice", []any{map[string]any{}, map[string]any{}}, ""},
		{"a list twice", []any{[]any{"v1"}, []any{"v1"}}, ""},
	} {
		got, _ := sharedVersionName(named(c.names...))
		eq(t, got, c.shared, c.what)
	}
	got, ok := sharedVersionName([]any{map[string]any{}, map[string]any{"served": true}, nil, "v1", "v1"})
	eq(t, got+strconv.FormatBool(ok), "false", "entries with no name share none")
}
