//go:build js && wasm

package fakeapiserver

// The one reading of a snapshot file (strictJSON, #1464 review B1): a key twice, and a key that
// encoding/json would take for a field only by ignoring case, are refused, so the rule (which reads
// keys exactly, as agentiko-sandbox's JavaScript does) and the loader (encoding/json) cannot read
// one file two ways.

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"syscall/js"
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

// The fold is encoding/json's own: simple case folding, one code point for one. A key it folds to
// a field is refused; one it does not is a key the loader does not know, and is left alone.
// agentiko-sandbox keeps the same sixteen rows.
func TestStrictJSON_FoldsAsEncodingJSONDoes(t *testing.T) {
	type fields struct {
		Kind      string `json:"kind"`
		Resources string `json:"resources"`
		Class     string `json:"class"`
		ID        string `json:"id"`
		Fi        string `json:"fi"`
		Ring      string `json:"å"`
		Sigma     string `json:"σ"`
	}
	for _, c := range []struct {
		key, field, what string
		folds            bool
	}{
		{"KIND", "kind", "ASCII upper case", true},
		{"ID", "id", "ASCII upper case", true},
		{"Kind", "kind", "the Kelvin sign U+212A for k", true},
		{"reſources", "resources", "the long s U+017F for s", true},
		{"Å", "å", "the Angstrom sign U+212B", true},
		{"Å", "å", "capital A with ring U+00C5", true},
		{"Σ", "σ", "capital sigma U+03A3", true},
		{"ς", "σ", "final sigma U+03C2", true},
		{"claß", "class", "sharp s U+00DF for ss: full folding", false},
		{"ﬁ", "fi", "the ligature U+FB01 for fi: full folding", false},
		{"İd", "id", "dotted capital I U+0130: Turkish", false},
		{"ıd", "id", "dotless i U+0131: Turkish", false},
		{"kınd", "kind", "dotless i inside", false},
		{"ｋｉｎｄ", "kind", "fullwidth letters", false},
		{"Å", "å", "A and a combining ring: a combining sequence", false},
		{"å", "å", "a and a combining ring: not normalised", false},
	} {
		key, _ := json.Marshal(c.key)
		_, err := strictJSON([]byte(`{`+string(key)+`:"v"}`), reflect.TypeOf(fields{}))
		want := ""
		if c.folds {
			want = `the top level holds the key ` + strconv.Quote(c.key) + `, which is ` + strconv.Quote(c.field) + ` only by ignoring case`
		}
		got := ""
		if err != nil {
			got = err.Error()
		}
		eq(t, got, want, fmt.Sprintf("%s (%U)", c.what, []rune(c.key)))
		// And encoding/json itself: it takes the key for the field exactly when strictJSON refuses it.
		var f fields
		if err := json.Unmarshal([]byte(`{`+string(key)+`:"v"}`), &f); err != nil {
			t.Fatal(err)
		}
		eq(t, f != fields{}, c.folds, fmt.Sprintf("encoding/json on %s (%U)", c.what, []rune(c.key)))
	}
}

func nestedJSON(open, close string, n int, inner string) string {
	return strings.Repeat(open, n) + inner + strings.Repeat(close, n)
}

const tooDeep = "the JSON is nested deeper than 1000 containers"

// maxDepth: containers from the file's top level, objects and arrays alike, the top-level one at
// depth 1. 1000 is read, 1001 is refused, by name.
func TestStrictJSON_NestingBound(t *testing.T) {
	eq(t, maxDepth, 1000, "maxDepth, as agentiko-sandbox's parsers hold it")
	bare := func(text string) string {
		if _, err := strictJSON([]byte(text), nil); err != nil {
			return err.Error()
		}
		return ""
	}
	for what, mk := range map[string]func(n int) string{
		"arrays":            func(n int) string { return nestedJSON("[", "]", n, "") },
		"objects":           func(n int) string { return nestedJSON(`{"a":`, "}", n-1, "{}") },
		"arrays in objects": func(n int) string { return nestedJSON(`{"a":[`, "]}", n/2, "") + strings.Repeat(" ", n%2) },
	} {
		eq(t, bare(mk(1000)), "", what+": 1000 deep")
		eq(t, bare(mk(1002)), tooDeep, what+": 1002 deep")
	}
	eq(t, bare(nestedJSON("[", "]", 999, "")), "", "arrays: 999 deep")
	eq(t, bare(nestedJSON("[", "]", 1001, "")), tooDeep, "arrays: 1001 deep")
	eq(t, bare(nestedJSON(`{"a":`, "}", 1000, "{}")), tooDeep, "objects: 1001 deep")
	eq(t, bare(nestedJSON("[", "]", 1000, `{}`)), tooDeep, "an object under 1000 arrays")
	eq(t, bare(nestedJSON("[", "]", 1000, `1`)), "", "a number under 1000 arrays is no container")
	eq(t, bare(`[`+nestedJSON("[", "]", 999, "")+`,`+nestedJSON("[", "]", 999, "")+`]`), "", "two siblings, each 1000 deep in all")

	// Through New: the enclosing containers count. own is the value's own depth.
	snap := func(where string, own int) string {
		v := nestedJSON("[", "]", own, "")
		_, err := loadHarness(t, opts{edit: func(s map[string]any) {
			s["scripted"].(map[string]any)["applyStatus"].([]any)[0].(map[string]any)["target"] = map[string]any{"group": "g", "resource": "r", "namespace": "n", "name": "x"}
		}, rawSnap: func(s string) string {
			old := map[string]string{"top": `{`, "status": `"status":{`, "target": `"target":{`}[where]
			if !strings.Contains(s, old) {
				t.Fatalf("no %s in the snapshot", old)
			}
			return strings.Replace(s, old, old+`"zz":`+v+`,`, 1)
		}})
		if err != nil {
			return err.Error()
		}
		return ""
	}
	page := func(inItem bool, own int) string {
		v := nestedJSON(`{"a":`, "}", own-1, "{}")
		_, err := loadHarness(t, opts{rawPage: func(s string) string {
			i := strings.Index(s, "{")
			if inItem {
				i = strings.Index(s, "[")
				i += strings.Index(s[i:], "{")
			}
			return s[:i+1] + `"zz":` + v + `,` + s[i+1:]
		}})
		if err != nil {
			return err.Error()
		}
		return ""
	}
	eq(t, snap("top", 999), "", "snapshot.json, a top-level key: 999 under 1")
	eq(t, snap("top", 1000), "snapshot.json: "+tooDeep, "snapshot.json, a top-level key: 1000 under 1")
	eq(t, snap("status", 995), "", "snapshot.json, a rule's status: 995 under 5")
	eq(t, snap("status", 996), "snapshot.json: "+tooDeep, "snapshot.json, a rule's status: 996 under 5")
	eq(t, snap("target", 995), "", "snapshot.json, a rule's target: 995 under 5")
	eq(t, snap("target", 996), "snapshot.json: "+tooDeep, "snapshot.json, a rule's target: 996 under 5")
	eq(t, page(false, 999), "", "an object page, a top-level key: 999 under 1")
	eq(t, page(false, 1000), "object page 0: "+tooDeep, "an object page, a top-level key: 1000 under 1")
	eq(t, page(true, 997), "", "an object page, inside items[0]: 997 under 3")
	eq(t, page(true, 998), "object page 0: "+tooDeep, "an object page, inside items[0]: 998 under 3")
	// A rule's target read on its own (its second reading, after snapshot.json's).
	target := func(own int) string {
		if _, err := strictJSON([]byte(nestedJSON(`{"zz":`, "}", own-1, "{}")), reflect.TypeOf(ruleTarget{})); err != nil {
			return err.Error()
		}
		return ""
	}
	eq(t, target(1000), "", "a target alone: 1000 deep")
	eq(t, target(1001), tooDeep, "a target alone: 1001 deep")
}

// The forms of a number: an integer (no fraction, no exponent) is read when a double holds it
// exactly; any other literal is a float and only has to be finite as a double. The integer rows are
// the table agentiko-sandbox keeps too.
func TestStrictJSON_NumberBounds(t *testing.T) {
	inexact := func(at, lit string) string {
		return at + " is the integer " + lit + ", which names no double: it is neither one exactly nor one's shortest spelling"
	}
	infinite := func(at, lit string) string { return at + " is the number " + lit + ", which is not finite as a double" }
	pow := func(n int64) string { return new(big.Int).Exp(big.NewInt(2), big.NewInt(n), nil).String() }
	rows := []struct{ lit, want string }{
		// The shared table: integers that name a double, exactly or as its shortest spelling.
		{"0", ""}, {"-0", ""}, {"1", ""}, {"-1", ""},
		{"9007199254740991", ""}, {"9007199254740992", ""}, {"-9007199254740992", ""}, {"9007199254740994", ""}, {"-9007199254740994", ""},
		{"9223372036854775808", ""}, {"-9223372036854775808", ""}, // ±2^63, exactly
		{"9223372036854776000", ""}, {"-9223372036854776000", ""}, // ±2^63, as String writes them (kube-apiserver's int64 maximum)
		{"18446744073709551616", ""}, {"18446744073709552000", ""}, {"36028797018963968", ""}, // 2^64 both ways, 2^55
		{"999999999999999868928", ""}, {"999999999999999900000", ""}, // the largest double below 1e21, both ways
		{"92233720368547760000", ""},                  // 10 × 2^63's shortest digits: the shortest spelling of the double 9.223372036854776e19
		{"1000000000000000000000", ""}, {pow(70), ""}, // 1e21 and 2^70: exactly only, String writes them with an exponent
		{pow(1000), ""}, {"-" + pow(1000), ""}, {pow(1023), ""},
		{"179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368", ""}, // the largest double
		// Integers that name no double: neither one exactly nor one's shortest spelling.
		{"9007199254740993", "inexact"}, {"-9007199254740993", "inexact"}, {"9007199254740995", "inexact"},
		{"9223372036854775807", "inexact"}, {"-9223372036854775807", "inexact"}, {"9223372036854775809", "inexact"},
		{"9223372036854776001", "inexact"}, {"9223372036854775999", "inexact"}, {"92233720368547760001", "inexact"},
		{"18446744073709551615", "inexact"}, {"18446744073709552001", "inexact"},
		{"999999999999999999999", "inexact"},  // its double is 1e21, which String writes 1e+21
		{"1000000000000000000001", "inexact"}, // 1e21+1
		{"99999999999999999999", "inexact"},   // its double is 1e20, which String writes 100000000000000000000
		{"100000000000000000000", ""},
		{new(big.Int).Add(new(big.Int).Exp(big.NewInt(2), big.NewInt(70), nil), big.NewInt(1)).String(), "inexact"}, // 2^70+1
		{"1180591620717411300000", "inexact"}, // 2^70's shortest digits, written out: String writes 1.1805916207174113e+21
		{"123456789012345678901234567890123456789012345678901234567890", "inexact"},
		{pow(1000) + "1", "inexact"}, // 2^1000 with a digit more: ten times it, and one
		// Integers beyond the largest double: the nearest is an infinity.
		{strings.Repeat("9", 400), "inexact"}, {"1" + strings.Repeat("0", 399), "inexact"}, {"-" + strings.Repeat("9", 400), "inexact"}, {pow(1024), "inexact"},
		// A fraction or an exponent makes a float, whatever its value.
		{"9007199254740993.0", ""}, {"9007199254740993e0", ""}, {"1e20", ""}, {"1E20", ""}, {"1e+20", ""}, {"1.5e300", ""}, {"-1.5e300", ""},
		{"1.7976931348623157e308", ""}, {"0.1", ""}, {"-0.0", ""}, {"0e0", ""}, {"1e-400", ""}, {"5e-324", ""},
		{"1e400", "infinite"}, {"-1e400", "infinite"}, {"1.8e308", "infinite"}, {"-1.8e308", "infinite"},
		{"123456789012345678901234567890123456789012345678901234567890.0e300", "infinite"},
	}
	for _, c := range rows {
		want := map[string]func(at string) string{
			"":         func(string) string { return "" },
			"inexact":  func(at string) string { return inexact(at, c.lit) },
			"infinite": func(at string) string { return infinite(at, c.lit) },
		}[c.want]
		strict := func(text string, typ reflect.Type) string {
			if _, err := strictJSON([]byte(text), typ); err != nil {
				return err.Error()
			}
			return ""
		}
		eq(t, strict(c.lit, nil), want("the top level"), c.lit+" alone")
		eq(t, strict(`{"a":[1,{"b":`+c.lit+`}]}`, nil), want("a[1].b"), c.lit+" in free-form JSON")
		eq(t, strict(`{"group":"g","zz":`+c.lit+`}`, reflect.TypeOf(ruleTarget{})), want("zz"), c.lit+" in a rule's target")

		// Through New: a rule's status and its target in snapshot.json, and a field of an object.
		prefix := func(file, at string) string {
			if w := want(at); w != "" {
				return file + ": " + w
			}
			return ""
		}
		_, err := loadHarness(t, opts{edit: func(s map[string]any) {
			s["scripted"].(map[string]any)["applyStatus"].([]any)[0].(map[string]any)["target"] = map[string]any{"group": "g", "resource": "r", "namespace": "n", "name": "x"}
		}, rawSnap: func(s string) string {
			s = strings.Replace(s, `"status":{`, `"status":{"zz":`+c.lit+`,`, 1)
			return strings.Replace(s, `"target":{`, `"target":{"zy":`+c.lit+`,`, 1)
		}})
		got := ""
		if err != nil {
			got = err.Error()
		}
		// target sorts before status in the marshalled rule? Neither: the first of the two in the text is said.
		if got != prefix("snapshot.json", "scripted.applyStatus[0].status.zz") && got != prefix("snapshot.json", "scripted.applyStatus[0].target.zy") {
			t.Errorf("%s in snapshot.json: got %q", c.lit, got)
		}
		_, err = loadHarness(t, opts{rawPage: func(s string) string {
			i := strings.Index(s, "[")
			i += strings.Index(s[i:], "{")
			return s[:i+1] + `"zz":` + c.lit + `,` + s[i+1:]
		}})
		got = ""
		if err != nil {
			got = err.Error()
		}
		eq(t, got, prefix("object page 0", "items[0].zz"), c.lit+" in an object page")
	}

	// A number as read is the number written: nothing is rounded.
	h, err := loadHarness(t, opts{edit: func(map[string]any) {}, rawSnap: func(s string) string {
		return strings.Replace(s, `"status":{`, `"status":{"zz":9007199254740992,`, 1)
	}, rawPage: func(s string) string {
		i := strings.Index(s, "[")
		i += strings.Index(s[i:], "{")
		return s[:i+1] + `"zz":-9007199254740992,"zy":9223372036854776000,` + s[i+1:]
	}})
	if err != nil {
		t.Fatal(err)
	}
	held, _ := json.Marshal(h.s.rules[0].Status["zz"])
	eq(t, string(held), "9007199254740992", "2^53 in a rule's status is held exactly")
	for _, bucket := range h.s.store {
		for _, obj := range bucket {
			if v, ok := obj["zz"]; ok {
				held, _ = json.Marshal(v)
			}
		}
	}
	eq(t, string(held), "-9007199254740992", "-2^53 in an object is held exactly")
	for _, bucket := range h.s.store {
		for _, obj := range bucket {
			if v, ok := obj["zy"]; ok {
				held, _ = json.Marshal(v)
			}
		}
	}
	eq(t, string(held), "9223372036854776000", "an int64 maximum as kube-apiserver writes it is held as the double it names, and written back the same")

	// Where the loader reads a Go integer, a float is refused by name, before the decoder.
	typed := func(old, lit string) string {
		_, err := loadHarness(t, opts{edit: func(map[string]any) {}, rawSnap: func(s string) string {
			i := strings.Index(s, old)
			if i < 0 {
				t.Fatalf("no %s in the snapshot", old)
			}
			j := i + len(old) + strings.IndexAny(s[i+len(old):], ",}")
			return s[:i] + old + lit + s[j:]
		}})
		if err != nil {
			return err.Error()
		}
		return ""
	}
	eq(t, typed(`"afterMs":`, "1500"), "", "afterMs 1500")
	eq(t, typed(`"afterMs":`, "9007199254740992"), "", "afterMs 2^53")
	eq(t, typed(`"afterMs":`, "9007199254740993"), "snapshot.json: "+inexact("scripted.applyStatus[0].afterMs", "9007199254740993"), "afterMs 2^53+1")
	eq(t, typed(`"afterMs":`, "9223372036854775807"), "snapshot.json: "+inexact("scripted.applyStatus[0].afterMs", "9223372036854775807"), "afterMs the int64 maximum: a double would round it")
	eq(t, typed(`"afterMs":`, "-9223372036854776000"), "snapshot.json: scripted.applyStatus[0].afterMs is the integer -9223372036854776000, which does not fit the int64 read there", "afterMs -2^63 as String writes it: the integer written does not fit")
	eq(t, typed(`"afterMs":`, "-9223372036854775808"), "", "afterMs the int64 minimum: exact, and it fits")
	// Exact as a double, and too large for the int64 the loader reads: this refusal, not the decoder's.
	eq(t, typed(`"afterMs":`, "9223372036854776000"), "snapshot.json: scripted.applyStatus[0].afterMs is the integer 9223372036854776000, which does not fit the int64 read there", "afterMs 9223372036854776000")
	eq(t, typed(`"afterMs":`, "9223372036854775808"), "snapshot.json: scripted.applyStatus[0].afterMs is the integer 9223372036854775808, which does not fit the int64 read there", "afterMs 2^63")
	eq(t, typed(`"afterMs":`, "-18446744073709551616"), "snapshot.json: scripted.applyStatus[0].afterMs is the integer -18446744073709551616, which does not fit the int64 read there", "afterMs -2^64")
	eq(t, typed(`"schema":`, "9223372036854776000"), "snapshot.json: schema is the integer 9223372036854776000, which does not fit the int read there", "schema 9223372036854776000")
	eq(t, typed(`"afterMs":`, "1.5"), "snapshot.json: scripted.applyStatus[0].afterMs is the number 1.5, where an integer is read", "afterMs 1.5")
	eq(t, typed(`"afterMs":`, "1e3"), "snapshot.json: scripted.applyStatus[0].afterMs is the number 1e3, where an integer is read", "afterMs 1e3")
	eq(t, typed(`"afterMs":`, "1500.0"), "snapshot.json: scripted.applyStatus[0].afterMs is the number 1500.0, where an integer is read", "afterMs 1500.0")
	eq(t, typed(`"afterMs":`, "1e400"), "snapshot.json: "+infinite("scripted.applyStatus[0].afterMs", "1e400"), "afterMs 1e400")
	eq(t, typed(`"schema":`, "1.0"), "snapshot.json: schema is the number 1.0, where an integer is read", "schema 1.0")
	eq(t, typed(`"schema":`, "9007199254740993"), "snapshot.json: "+inexact("schema", "9007199254740993"), "schema 2^53+1")

	// Forms JSON forbids are the tokenizer's to refuse, as before.
	for _, lit := range []string{"01", "-01", "+1", ".5", "1.", "1e", "1e+", "--1", "0x10", "NaN", "Infinity", "-Infinity", "1_000", "١"} {
		if _, err := strictJSON([]byte(`{"a":`+lit+`}`), nil); err == nil {
			t.Errorf("%s was read", lit)
		}
	}
}

// closureSeeds are the doubles agentiko-sandbox's closure test seeds, the same here: random bit
// patterns, integers from 2^49 to 2^65, and the neighbours of 2^53, 2^63, 2^64, 1e20, 1e21 and 2^70
// (the four doubles on each side, and the value ±1 and ±3), each also negated.
func closureSeeds(n int) []float64 {
	r := rand.New(rand.NewPCG(1464, 81))
	var out []float64
	add := func(f float64) {
		if !math.IsNaN(f) && !math.IsInf(f, 0) {
			out = append(out, f, -f)
		}
	}
	for _, x := range []float64{1 << 53, 1 << 63, 1 << 64, 1e20, 1e21, 1 << 70} {
		add(x)
		for _, d := range []float64{1, -1, 3, -3} {
			add(x + d)
		}
		up, down := x, x
		for k := 0; k < 4; k++ {
			up, down = math.Nextafter(up, math.Inf(1)), math.Nextafter(down, 0)
			add(up)
			add(down)
		}
	}
	for len(out) < n {
		switch len(out) % 3 {
		case 0:
			add(math.Float64frombits(r.Uint64())) // any double
		case 1:
			e := 49 + r.IntN(65-49+1) // an integer in [2^e, 2^(e+1))
			add(math.Ldexp(float64(r.Uint64N(1<<52)|1<<52), e-52))
		default:
			e := r.IntN(70) // an integer-valued double below 2^70, every binade alike
			add(math.Trunc(math.Ldexp(float64(r.Uint64N(1<<52)|1<<52), e-52)))
		}
	}
	return out
}

// Below 1e21, ECMAScript's String(f) of an integer-valued double is strconv.FormatFloat(f, 'f', -1,
// 64): checked against Node's own String, in which this test runs.
func TestStrictJSON_ShortestSpellingIsECMAScripts(t *testing.T) {
	esString := js.Global().Get("String")
	checked := 0
	for _, f := range closureSeeds(1_000_000) {
		if f != math.Trunc(f) || math.Abs(f) >= 1e21 {
			continue
		}
		checked++
		if got, want := strconv.FormatFloat(f, 'f', -1, 64), esString.Invoke(f).String(); got != want && !(f == 0 && want == "0") {
			t.Fatalf("FormatFloat(%v) is %s, String is %s", f, got, want)
		}
	}
	if checked < 600_000 {
		t.Fatalf("only %d integer-valued doubles below 1e21 were checked", checked)
	}
	t.Logf("%d integer-valued doubles below 1e21: FormatFloat equals Node's String for each", checked)
}

// The rule is closed under the writer: every double, written as ECMAScript's String(f) writes it
// (the sandbox's canonical writer), is read, and read back as the same double.
func TestStrictJSON_TheNumberRuleIsClosedUnderTheWriter(t *testing.T) {
	esString := js.Global().Get("String")
	for _, f := range closureSeeds(1_000_000) {
		lit := esString.Invoke(f).String()
		v, err := strictJSON([]byte(lit), nil)
		if err != nil {
			t.Fatalf("%v written %s is refused: %v", f, lit, err)
		}
		back, err := v.(json.Number).Float64()
		if err != nil || back != f && !(f == 0 && back == 0) {
			t.Fatalf("%v written %s is read back as %v (%v)", f, lit, back, err)
		}
	}
}

// Every integer literal the rule reads names one double, and is held as that double: its
// spelling as String writes it, or its exact value.
func TestStrictJSON_AnIntegerReadNamesOneDouble(t *testing.T) {
	for _, f := range closureSeeds(200_000) {
		if f != math.Trunc(f) {
			continue
		}
		exact, _ := new(big.Float).SetFloat64(f).Int(nil)
		for _, lit := range []string{exact.String(), strconv.FormatFloat(f, 'f', -1, 64)} {
			i, _ := new(big.Int).SetString(lit, 10)
			ok := integerLiteralOk(lit, i)
			nearest, _ := new(big.Float).SetInt(i).Float64()
			switch {
			case lit == exact.String() && !ok:
				t.Fatalf("%s, exactly %v, is refused", lit, f)
			case ok && nearest != f:
				t.Fatalf("%s is read, and names %v, not %v", lit, nearest, f)
			case lit != exact.String() && ok != (math.Abs(f) < 1e21):
				t.Fatalf("%s, the shortest spelling of %v: read %v", lit, f, ok)
			}
		}
	}
}

// kube-apiserver renders an int64 bound in a CRD schema as String writes its double: the maximum
// as 9223372036854776000, the minimum as -9223372036854776000. Such a CRD is read.
func TestStrictJSON_ACRDWithInt64BoundsAsTheApiserverWritesThem(t *testing.T) {
	load := func(min, max string) string {
		crd := namedCRD(t, "panels."+widgetsGroup)
		schema := crd["spec"].(map[string]any)["versions"].([]any)[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
		props, _ := schema["properties"].(map[string]any)
		if props == nil {
			props = map[string]any{}
			schema["properties"] = props
		}
		props["count"] = map[string]any{"type": "integer", "format": "int64", "minimum": "MIN-BOUND", "maximum": "MAX-BOUND"}
		_, err := loadHarness(t, opts{edit: listing(widgetsGV, "panels"), extra: []map[string]any{crd}, rawExtra: func(text string) string {
			if strings.Count(text, `"MIN-BOUND"`) != 1 || strings.Count(text, `"MAX-BOUND"`) != 1 {
				t.Fatal("the bounds are not in the CRD's text once each")
			}
			return strings.Replace(strings.Replace(text, `"MIN-BOUND"`, min, 1), `"MAX-BOUND"`, max, 1)
		}})
		if err != nil {
			return err.Error()
		}
		return ""
	}
	at := "object page 1: items[0].spec.versions[0].schema.openAPIV3Schema.properties.count."
	eq(t, load("-9223372036854776000", "9223372036854776000"), "", "int64 bounds as kube-apiserver writes them")
	eq(t, load("-9223372036854775808", "9223372036854775808"), "", "±2^63 written exactly")
	eq(t, load("-9223372036854775808", "9223372036854775807"), at+"maximum is the integer 9223372036854775807, which names no double: it is neither one exactly nor one's shortest spelling", "the int64 maximum written exactly names no double")
	eq(t, load("-9223372036854775807", "9223372036854776000"), at+"minimum is the integer -9223372036854775807, which names no double: it is neither one exactly nor one's shortest spelling", "-(2^63-1) names no double")
}

// An unpaired surrogate escape is refused anywhere, key or value; a byte that is not UTF-8 is read
// as U+FFFD, refused only in a name the rule matches (the sandbox's readers now refuse such a file
// outright, SNAPSHOT.md V25; matching it is #1496); a byte order
// mark and content after the value are refused.
func TestStrictJSON_UnpairedSurrogates(t *testing.T) {
	read := func(text string) string {
		if _, err := strictJSON([]byte(text), reflect.TypeOf(Snapshot{})); err != nil {
			return err.Error()
		}
		return ""
	}
	at := func(esc string, i int) string {
		return "the JSON holds an unpaired surrogate, " + esc + ", at byte " + strconv.Itoa(i)
	}
	for _, c := range []struct{ what, text, want string }{
		{"a high surrogate alone, in a value", `{"a":"x\ud800y"}`, at(`\ud800`, 7)},
		{"a low surrogate alone, in a value", `{"a":"x\udc00"}`, at(`\udc00`, 7)},
		{"a high surrogate at the end of a string", `{"a":"\udbff"}`, at(`\udbff`, 6)},
		{"a high surrogate in a key", `{"k\ud800":1}`, at(`\ud800`, 3)},
		{"a low surrogate in a key the loader reads", `{"discovery":{"resources":{"apps/v1\udc00":{"resources":[]}}}}`, at(`\udc00`, 35)},
		{"two high surrogates", `["\ud800\ud800"]`, at(`\ud800`, 2)},
		{"a low then a high", `["\udc00\ud800"]`, at(`\udc00`, 2)},
		{"a high then a non-surrogate escape", `["\ud800\u0041"]`, at(`\ud800`, 2)},
		{"a high then a raw character", `["\ud800A"]`, at(`\ud800`, 2)},
		{"upper-case hex", `["\uD800"]`, at(`\uD800`, 2)},
		{"mixed-case hex", `["\uDbFf"]`, at(`\uDbFf`, 2)},
		{"mixed-case hex, a pair", `["􏿿"]`, ""},
		{"a pair, then a low alone", `["\ud83d\ude00\ude00"]`, at(`\ude00`, 14)},
		{"a pair", `["\ud83d\ude00"]`, ""},
		{"a pair in upper case", `["\uD83D\uDE00"]`, ""},
		{"the last pair", `["\udbff\udfff"]`, ""},
		{"an escaped backslash, then text like an escape", `["\\ud800"]`, ""},
		{"an escaped quote before it", `["\"","\ud800"]`, at(`\ud800`, 7)},
		{"outside a string it is the tokenizer's", `[1]`, ""},
		{"a non-surrogate escape", `["\u00e9\u20ac"]`, ""},
		{"U+FFFD written as an escape", `["\ufffd"]`, ""},
	} {
		eq(t, read(c.text), c.want, c.what)
	}
	// Bytes that are not UTF-8: read as U+FFFD here (the sandbox refuses the file, V25: #1496).
	v, err := strictJSON([]byte("[\"a\xffb\",\"\xed\xa0\x80\"]"), nil)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, v.([]any)[0], "a�b", "a byte that is not UTF-8 is read as U+FFFD")
	eq(t, strings.Count(v.([]any)[1].(string), "�") > 0, true, "a surrogate's raw bytes are not UTF-8, and read as U+FFFD")
	// Through New: in a resource name it is refused; in data it is read.
	_, err = loadHarness(t, opts{edit: listing("apps/v1", "pods"), rawSnap: func(s string) string {
		return strings.Replace(s, `"name":"pods"`, "\"name\":\"po\xffds\"", 1)
	}})
	eq(t, err != nil && strings.Contains(err.Error(), `name "po`+"�"+`ds" is not well-formed Unicode`), true, "a byte that is not UTF-8 in a resource name")
	_, err = loadHarness(t, opts{rawPage: func(s string) string {
		i := strings.Index(s, "[")
		i += strings.Index(s[i:], "{")
		return s[:i+1] + "\"zz\":\"da\xfft\xe9a\"," + s[i+1:]
	}})
	eq(t, err, error(nil), "bytes that are not UTF-8 in data")
	_, err = loadHarness(t, opts{rawPage: func(s string) string { return strings.Replace(s, `"items"`, `"it\ud800ems"`, 1) }})
	eq(t, err != nil && strings.HasPrefix(err.Error(), `object page 0: the JSON holds an unpaired surrogate, \ud800, at byte`), true, "an unpaired surrogate in a page")
	_, err = loadHarness(t, opts{edit: func(map[string]any) {}, rawSnap: func(s string) string {
		return strings.Replace(s, `"resourceVersion"`, `"x\udfff":1,"resourceVersion"`, 1)
	}})
	eq(t, err != nil && strings.HasPrefix(err.Error(), `snapshot.json: the JSON holds an unpaired surrogate, \udfff, at byte`), true, "an unpaired surrogate in snapshot.json")

	// A byte order mark, and content after the value, are refused.
	eq(t, read("\xef\xbb\xbf{}") != "", true, "a byte order mark")
	eq(t, read(`{} x`) != "", true, "content after the value")
	eq(t, read(`{"a":1,"\u0061":2}`), `the top level holds the key "a" twice`, "a key twice after its escapes are read")
}
