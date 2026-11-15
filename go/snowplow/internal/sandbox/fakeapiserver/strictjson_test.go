//go:build js && wasm

package fakeapiserver

// The one reading of a snapshot file (strictJSON, #1464 review B1): a key twice, and a key that
// encoding/json would take for a field only by ignoring case, are refused, so the rule (which reads
// keys exactly, as agentiko-sandbox's JavaScript does) and the loader (encoding/json) cannot read
// one file two ways.

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func strictSnapshot(text string) string {
	if _, err := strictJSON([]byte(text), reflect.TypeOf(Snapshot{})); err != nil {
		return err.Error()
	}
	return ""
}

// What encoding/json alone does with the keys strictJSON refuses: the reason it is there.
func TestStrictJSON_EncodingJSONFoldsCaseAndKeepsTheLastKey(t *testing.T) {
	var s Snapshot
	if err := json.Unmarshal([]byte(`{"discovery":{"reſources":{"g.io/v1":{"Resources":[{"NAME":"first"},{"name":"a","Name":"last"}]}}}}`), &s); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range s.Discovery.Resources["g.io/v1"].APIResources {
		names = append(names, r.Name)
	}
	eq(t, strings.Join(names, " "), "first last", "encoding/json reads reſources, Resources, NAME and Name as the fields, and the last of two")
}

func TestStrictJSON_RefusesAKeyTwiceAndAKeyByAnotherCase(t *testing.T) {
	list := func(inner string) string { return `{"discovery":{"resources":{"g.io/v1":` + inner + `}}}` }
	entry := func(inner string) string { return list(`{"resources":[{"name":"a"},` + inner + `]}`) }
	folded := func(where, key, field string) string {
		return where + ` holds the key "` + key + `", which is "` + field + `" only by ignoring case`
	}
	twice := func(where, key string) string { return where + ` holds the key "` + key + `" twice` }
	for _, c := range []struct{ what, text, want string }{
		{"the keys as written", `{"schema":1,"objects":["objects/000.json"],"resourceVersion":"7","discovery":{"api":{"Kind":"x","kind":"y"},"resources":{"g.io/v1":{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"g.io/v1","resources":[{"name":"a","singularName":"","namespaced":true,"kind":"A","verbs":["get"],"shortNames":["x"]}]}}}}`, ""},
		{"a key the fake does not read", `{"objects":[],"anything":{"a":1,"A":2},"discovery":{"resources":{"g.io/v1":{"resources":[{"name":"a","unknown":1,"Unknown":2}]}}}}`, ""},
		{"free-form JSON may differ by case alone", `{"discovery":{"api":{"versions":["v1"],"Versions":["v2"]},"apis":{"Groups":[]}},"scripted":{"applyStatus":[{"id":"r","status":{"phase":"a","Phase":"b"}}]}}`, ""},

		// A key by another case, at every level of discovery.
		{"Discovery", `{"Discovery":{}}`, folded("the top level", "Discovery", "discovery")},
		{"DISCOVERY", `{"DISCOVERY":{}}`, folded("the top level", "DISCOVERY", "discovery")},
		{"discovery.Resources", `{"discovery":{"Resources":{}}}`, folded("discovery", "Resources", "resources")},
		{"discovery.reſources (a long s)", `{"discovery":{"reſources":{}}}`, folded("discovery", "reſources", "resources")},
		{"discovery.API", `{"discovery":{"API":{}}}`, folded("discovery", "API", "api")},
		{"a list's Resources", list(`{"Resources":[{"name":"a"}]}`), folded("discovery.resources.g.io/v1", "Resources", "resources")},
		{"a list's RESOURCES", list(`{"RESOURCES":[]}`), folded("discovery.resources.g.io/v1", "RESOURCES", "resources")},
		{"a list's reſourceſ (long s)", list(`{"reſourceſ":[]}`), folded("discovery.resources.g.io/v1", "reſourceſ", "resources")},
		{"a list's resources and Resources", list(`{"resources":[],"Resources":[{"name":"a"}]}`), folded("discovery.resources.g.io/v1", "Resources", "resources")},
		{"a list's Resources and resources", list(`{"Resources":[{"name":"a"}],"resources":[]}`), folded("discovery.resources.g.io/v1", "Resources", "resources")},
		{"a list's Kind (an embedded field)", list(`{"Kind":"APIResourceList","resources":[]}`), folded("discovery.resources.g.io/v1", "Kind", "kind")},
		{"a list's KIND with a Kelvin sign", list(`{"\u212aind":"APIResourceList","resources":[]}`), folded("discovery.resources.g.io/v1", "\u212aind", "kind")},
		{"a list's APIVersion", list(`{"APIVersion":"v1","resources":[]}`), folded("discovery.resources.g.io/v1", "APIVersion", "apiVersion")},
		{"a list's GroupVersion", list(`{"GroupVersion":"g.io/v1","resources":[]}`), folded("discovery.resources.g.io/v1", "GroupVersion", "groupVersion")},
		{"an entry's Name", entry(`{"Name":"b"}`), folded("discovery.resources.g.io/v1.resources[1]", "Name", "name")},
		{"an entry's NAME", entry(`{"NAME":"b"}`), folded("discovery.resources.g.io/v1.resources[1]", "NAME", "name")},
		{"an entry's name and Name", entry(`{"name":"b","Name":"c"}`), folded("discovery.resources.g.io/v1.resources[1]", "Name", "name")},
		{"an entry's Name and name", entry(`{"Name":"c","name":"b"}`), folded("discovery.resources.g.io/v1.resources[1]", "Name", "name")},
		{"an entry's Kind", entry(`{"name":"b","Kind":"B"}`), folded("discovery.resources.g.io/v1.resources[1]", "Kind", "kind")},
		{"an entry's Namespaced", entry(`{"name":"b","Namespaced":true}`), folded("discovery.resources.g.io/v1.resources[1]", "Namespaced", "namespaced")},
		{"an entry's Verbs", entry(`{"name":"b","Verbs":["get"]}`), folded("discovery.resources.g.io/v1.resources[1]", "Verbs", "verbs")},
		{"an entry's singularname", entry(`{"name":"b","singularname":"b"}`), folded("discovery.resources.g.io/v1.resources[1]", "singularname", "singularName")},

		// The rest of snapshot.json.
		{"Schema", `{"Schema":1}`, folded("the top level", "Schema", "schema")},
		{"ResourceVersion", `{"ResourceVersion":"7"}`, folded("the top level", "ResourceVersion", "resourceVersion")},
		{"capturedFrom.Server", `{"capturedFrom":{"Server":"x"}}`, folded("capturedFrom", "Server", "server")},
		{"capturedFrom.selfSubjectRulesReview.Incomplete", `{"capturedFrom":{"selfSubjectRulesReview":{"Incomplete":true}}}`, folded("capturedFrom.selfSubjectRulesReview", "Incomplete", "incomplete")},
		{"identity.visitor.Username", `{"identity":{"visitor":{"Username":"x"}}}`, folded("identity.visitor", "Username", "username")},
		{"identity.serviceaccount", `{"identity":{"serviceaccount":{}}}`, folded("identity", "serviceaccount", "serviceAccount")},
		{"a rule's ID", `{"scripted":{"applyStatus":[{"ID":"r"}]}}`, folded("scripted.applyStatus[0]", "ID", "id")},
		{"a rule's on.Verb", `{"scripted":{"applyStatus":[{"id":"r"},{"on":{"Verb":"create"}}]}}`, folded("scripted.applyStatus[1].on", "Verb", "verb")},
		{"a rule's AfterMs", `{"scripted":{"applyStatus":[{"afterms":1}]}}`, folded("scripted.applyStatus[0]", "afterms", "afterMs")},

		// A key twice, at every level of discovery and anywhere else.
		{"discovery twice", `{"discovery":{},"discovery":{"resources":{}}}`, twice("the top level", "discovery")},
		{"discovery.resources twice", `{"discovery":{"resources":{},"resources":{"g.io/v1":{"resources":[]}}}}`, twice("discovery", "resources")},
		{"a group version twice", `{"discovery":{"resources":{"g.io/v1":{"resources":[]},"g.io/v1":{"resources":[{"name":"a"}]}}}}`, twice("discovery.resources", "g.io/v1")},
		{"a list's resources twice", list(`{"resources":[],"resources":[{"name":"a"}]}`), twice("discovery.resources.g.io/v1", "resources")},
		{"a list's kind twice", list(`{"kind":"a","resources":[],"kind":"b"}`), twice("discovery.resources.g.io/v1", "kind")},
		{"an entry's name twice", entry(`{"name":"b","name":"c"}`), twice("discovery.resources.g.io/v1.resources[1]", "name")},
		{"an entry's name twice, the same value", entry(`{"name":"b","kind":"B","name":"b"}`), twice("discovery.resources.g.io/v1.resources[1]", "name")},
		{"an entry's kind twice", entry(`{"name":"b","kind":"B","kind":"C"}`), twice("discovery.resources.g.io/v1.resources[1]", "kind")},
		{"a key the fake does not read, twice", `{"objects":[],"objects":["a"]}`, twice("the top level", "objects")},
		{"a key twice in free-form JSON", `{"discovery":{"api":{"versions":["v1"],"versions":["v2"]}}}`, twice("discovery.api", "versions")},
		{"a key twice in a rule's status", `{"scripted":{"applyStatus":[{"id":"r","status":{"conditions":[{"type":"a","type":"b"}]}}]}}`, twice("scripted.applyStatus[0].status.conditions[0]", "type")},
		{"an empty key twice", `{"":1,"":2}`, twice("the top level", "")},
		{"two keys that are one after decoding", `{"a\u0062c":1,"abc":2}`, twice("the top level", "abc")},

		// Not one JSON value.
		{"data after the value", `{} {}`, "data after the JSON value"},
		{"a value cut short", `{"discovery":{"resources":`, "EOF"},
	} {
		eq(t, strictSnapshot(c.text), c.want, c.what)
	}
}

// Through New: the review's cases. A custom type with no CRD is refused under `name` and
// `resources`; it was loaded, and listed, under `Name` and `Resources`.
func TestStrictJSON_NewReadsEveryKeyExactly(t *testing.T) {
	entry := func(key string) func(map[string]any) {
		return func(s map[string]any) {
			r := apires("panels", "Panel", true)
			r[key] = r["name"]
			if key != "name" {
				delete(r, "name")
			}
			s["discovery"].(map[string]any)["resources"].(map[string]any)[widgetsGV] = map[string]any{"kind": "APIResourceList", "groupVersion": widgetsGV, "resources": []any{r}}
		}
	}
	list := func(key string) func(map[string]any) {
		return func(s map[string]any) {
			s["discovery"].(map[string]any)["resources"].(map[string]any)[widgetsGV] = map[string]any{"kind": "APIResourceList", "groupVersion": widgetsGV, key: []any{apires("panels", "Panel", true)}}
		}
	}
	at := "snapshot.json: discovery.resources." + widgetsGV
	eq(t, refusal(t, entry("name")), crdRulePrefix+noCRD(widgetsGV, "panels"), "name: the rule's refusal")
	eq(t, refusal(t, entry("Name")), at+`.resources[0] holds the key "Name", which is "name" only by ignoring case`, "Name")
	eq(t, refusal(t, entry("NAME")), at+`.resources[0] holds the key "NAME", which is "name" only by ignoring case`, "NAME")
	eq(t, refusal(t, list("resources")), crdRulePrefix+noCRD(widgetsGV, "panels"), "resources: the rule's refusal")
	eq(t, refusal(t, list("Resources")), at+` holds the key "Resources", which is "resources" only by ignoring case`, "Resources")
	eq(t, refusal(t, list("reſources")), at+` holds the key "reſources", which is "resources" only by ignoring case`, "reſources, a long s")

	// A key twice cannot be held by a Go map: the text is rewritten.
	raw := func(old, new string) string {
		_, err := loadHarness(t, opts{edit: entry("name"), rawSnap: func(text string) string {
			if strings.Count(text, old) != 1 {
				t.Fatalf("the snapshot holds %s %d times", old, strings.Count(text, old))
			}
			return strings.Replace(text, old, new, 1)
		}})
		if err != nil {
			return err.Error()
		}
		return ""
	}
	eq(t, raw(`"name":"panels"`, `"name":"panels","name":"layouts"`), at+`.resources[0] holds the key "name" twice`, "name twice")
	eq(t, raw(`"name":"panels"`, `"name":"panels","Name":"layouts"`), at+`.resources[0] holds the key "Name", which is "name" only by ignoring case`, "name and Name")
	eq(t, raw(`"groupVersion":"`+widgetsGV+`"`, `"groupVersion":"`+widgetsGV+`","resources":[],"resources":[]`), at+` holds the key "resources" twice`, "resources twice")
	eq(t, raw(`"resourceVersion"`, `"resourceVersion":"1","resourceVersion"`), `snapshot.json: the top level holds the key "resourceVersion" twice`, "a top-level key twice")

	// A rule's target is decoded on its own, later: its keys are read exactly too.
	target := func(v any) func(map[string]any) {
		return func(s map[string]any) {
			rules := s["scripted"].(map[string]any)["applyStatus"].([]any)
			rules[0].(map[string]any)["target"] = v
		}
	}
	eq(t, refusal(t, target(map[string]any{"group": "observability.krateo.io", "resource": "incidents", "namespace": "krateo-system", "name": incident})), "", "a target's keys as written")
	eq(t, refusal(t, target(map[string]any{"Group": "observability.krateo.io", "resource": "incidents", "namespace": "krateo-system", "name": incident})),
		`snapshot.json: scripted.applyStatus[0].target: the top level holds the key "Group", which is "group" only by ignoring case`, "a target's Group")

	// An object page: the items key, and a key twice in an object.
	page := func(old, new string) string {
		_, err := loadHarness(t, opts{rawPage: func(text string) string {
			if !strings.Contains(text, old) {
				t.Fatalf("the page does not hold %s", old)
			}
			return strings.Replace(text, old, new, 1)
		}})
		if err != nil {
			return err.Error()
		}
		return ""
	}
	eq(t, page(`"items"`, `"Items"`), `object page 0: the top level holds the key "Items", which is "items" only by ignoring case`, "a page's Items")
	eq(t, page(`"items"`, `"items":[],"items"`), `object page 0: the top level holds the key "items" twice`, "a page's items twice")
	eq(t, page(`"kind"`, `"kind":"Secret","kind"`), `object page 0: items[0] holds the key "kind" twice`, "a key twice in an object")
}
