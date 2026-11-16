package jq

import (
	"reflect"
	"testing"
)

// TestRootKeys_1477 pins what RootKeys reports for the shapes an endpointRef
// template takes, and that every shape it cannot follow reports ok=false.
func TestRootKeys_1477(t *testing.T) {
	cases := []struct {
		q    string
		keys []string
		ok   bool
	}{
		{`.name`, []string{"name"}, true},
		{`.name + "-endpoint"`, []string{"name"}, true},
		{`(.name // "") + "-clientconfig"`, []string{"name"}, true},
		// A read under an alternative is a read (file comment): the live
		// portal spoke endpointRef, and a hub default.
		{`((.name // "") | gsub("[^a-zA-Z0-9.-]"; "")) + "-endpoint"`, []string{"name"}, true},
		{`(.name // "hub") + "-endpoint"`, []string{"name"}, true},
		{`"\(.name)-\(.namespace)"`, []string{"name", "namespace"}, true},
		{`."name"`, []string{"name"}, true},
		{`.["name"]`, []string{"name"}, true},
		{`.target.items[0].name`, []string{"target"}, true},
		{`.target | .items[0].name`, []string{"target"}, true},
		{`.target.items | map(.name) | first`, []string{"target"}, true},
		{`. | .name`, []string{"name"}, true},
		{`if .a then .b else "x" end`, []string{"a", "b"}, true},
		{`.a as $x | $x + .b`, []string{"a", "b"}, true},
		{`{name} | .name`, []string{"name"}, true},
		{`.a[.b]`, []string{"a", "b"}, true},
		{`"static"`, []string{}, true},
		{`.name | ascii_downcase`, []string{"name"}, true},

		// The root as a whole, a computed index, a call or an assignment on
		// the root: cannot tell.
		{`.`, nil, false},
		{`..`, nil, false},
		{`.[]`, nil, false},
		{`.[$k]`, nil, false},
		{`.[.k]`, nil, false},
		{`keys[0]`, nil, false},
		{`to_entries[0].value`, nil, false},
		{`getpath(["name"])`, nil, false},
		{`tojson`, nil, false},
		{`@base64`, nil, false},
		{`. as $r | $r.name`, nil, false},
		{`(.)`, nil, false},
		{`.name // .`, nil, false},
		{`.a = 1 | .a`, nil, false},
		{`def f: .x; f`, nil, false},
		{`select(.a) | .b`, nil, false},
		{`not a query (`, nil, false},
	}
	for _, c := range cases {
		keys, ok := RootKeys(c.q)
		if ok != c.ok {
			t.Errorf("RootKeys(%q) ok = %v; want %v (keys %v)", c.q, ok, c.ok, keys)
			continue
		}
		if ok && !reflect.DeepEqual(keys, c.keys) {
			t.Errorf("RootKeys(%q) = %v; want %v", c.q, keys, c.keys)
		}
	}
}

// TestRootKeysUnder_1477 pins the reads beneath one key (a stage filter's
// `extras`): constant indexes are sub keys; any other use of the key makes
// every sub key possible (subOK=false).
func TestRootKeysUnder_1477(t *testing.T) {
	cases := []struct {
		q     string
		sub   []string
		subOK bool
	}{
		{`{ ep: ((.extras.name // "") + "-endpoint") }`, []string{"name"}, true},
		{`.extras["name"] + .extras."ns"`, []string{"name", "ns"}, true},
		{`.A.items`, []string{}, true},
		{`.extras`, []string{}, false},
		{`.extras | .name`, []string{}, false},
		{`.extras[]`, []string{}, false},
		{`.extras[.k]`, []string{}, false},
		{`{extras} | .extras.name`, []string{}, false},
	}
	for _, c := range cases {
		_, sub, subOK, ok := RootKeysUnder(c.q, "extras")
		if !ok {
			t.Errorf("RootKeysUnder(%q): ok=false", c.q)
			continue
		}
		if subOK != c.subOK || subOK && !reflect.DeepEqual(sub, c.sub) {
			t.Errorf("RootKeysUnder(%q) = %v subOK=%v; want %v subOK=%v", c.q, sub, subOK, c.sub, c.subOK)
		}
	}
}
