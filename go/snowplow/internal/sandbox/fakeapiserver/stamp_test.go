//go:build js && wasm

package fakeapiserver

import (
	"reflect"
	"testing"
)

// stamp is applyStatus's stamp of one pointer: parsed, then set.
func stamp(doc map[string]any, pointer string) string {
	toks, r := pointerTokens(pointer)
	if r != "" {
		return r
	}
	return stampAt(doc, toks, "T")
}

// RFC 6901's reference tokens, and the strings that are not pointers.
func TestPointerTokens_RFC6901(t *testing.T) {
	for _, tc := range []struct {
		pointer string
		want    []string
	}{
		{"", nil},
		{"/", []string{""}},
		{"/status", []string{"status"}},
		{"/a//b", []string{"a", "", "b"}},
		{"/a~1b", []string{"a/b"}},
		{"/m~0n", []string{"m~n"}},
		{"/~01", []string{"~1"}},
		{"/~10", []string{"/0"}},
		{"/c%d", []string{"c%d"}},
		{"/ ", []string{" "}},
	} {
		got, r := pointerTokens(tc.pointer)
		if r != "" || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%q: got %q (%s), want %q", tc.pointer, got, r, tc.want)
		}
	}
	for _, p := range []string{"status", "#/status", "/a~", "/a~2", "/~x/b"} {
		if got, r := pointerTokens(p); r == "" {
			t.Errorf("%q was taken as a pointer: %q", p, got)
		}
	}
}

// A stamp's pointer is RFC 6901: objects by key, arrays by index, "~1" is "/" and "~0" is "~".
func TestStampAt_RFC6901(t *testing.T) {
	doc := func() map[string]any {
		return map[string]any{"status": map[string]any{
			"conditions": []any{map[string]any{"type": "Ready"}, map[string]any{"type": "Synced"}},
			"a/b":        map[string]any{},
			"m~n":        map[string]any{},
			"m~1n":       map[string]any{},
			"list":       []any{"x", "y"},
		}}
	}
	ok := []struct {
		name, pointer string
		want          func(d map[string]any) any
	}{
		{"an object member is added", "/status/created_at", func(d map[string]any) any { return get(d, "status", "created_at") }},
		{"through an array by index", "/status/conditions/1/lastTransitionTime", func(d map[string]any) any {
			return get(d, "status", "conditions").([]any)[1].(map[string]any)["lastTransitionTime"]
		}},
		{"index 0", "/status/conditions/0/lastTransitionTime", func(d map[string]any) any {
			return get(d, "status", "conditions").([]any)[0].(map[string]any)["lastTransitionTime"]
		}},
		{"an existing array element is replaced", "/status/list/1", func(d map[string]any) any { return get(d, "status", "list").([]any)[1] }},
		{"~1 is /", "/status/a~1b/at", func(d map[string]any) any { return get(d, "status", "a/b", "at") }},
		{"~0 is ~", "/status/m~0n/at", func(d map[string]any) any { return get(d, "status", "m~n", "at") }},
		{"~01 is ~1, not /", "/status/m~01n/at", func(d map[string]any) any { return get(d, "status", "m~1n", "at") }},
		{`"-" is a member name in an object`, "/status/-", func(d map[string]any) any { return get(d, "status", "-") }},
	}
	for _, tc := range ok {
		d := doc()
		if r := stamp(d, tc.pointer); r != "" {
			t.Errorf("%s: %s refused: %s", tc.name, tc.pointer, r)
			continue
		}
		if got := tc.want(d); got != "T" {
			t.Errorf("%s: %s: got %v at the location", tc.name, tc.pointer, got)
		}
	}
	// Only the named location changed.
	d := doc()
	_ = stamp(d, "/status/conditions/1/lastTransitionTime")
	want := doc()
	want["status"].(map[string]any)["conditions"].([]any)[1].(map[string]any)["lastTransitionTime"] = "T"
	if !reflect.DeepEqual(d, want) {
		t.Errorf("a stamp changed more than its location: %v", d)
	}

	refused := []struct{ name, pointer string }{
		{`"-" (append)`, "/status/conditions/-"},
		{`"-" mid-pointer`, "/status/conditions/-/lastTransitionTime"},
		{"an index past the end", "/status/conditions/2/lastTransitionTime"},
		{"a leading zero", "/status/conditions/01/lastTransitionTime"},
		{"a non-number into an array", "/status/conditions/ready/lastTransitionTime"},
		{"a negative index", "/status/conditions/-1/lastTransitionTime"},
		{"through a scalar", "/status/list/0/at"},
		{"through a missing member", "/status/missing/at"},
		{"the whole document", ""},
		{`no leading "/"`, "status/created_at"},
		{`"~" before neither 0 nor 1`, "/status/a~2b/at"},
		{`a trailing "~"`, "/status/a~"},
	}
	for _, tc := range refused {
		d := doc()
		if r := stamp(d, tc.pointer); r == "" {
			t.Errorf("%s: %s was accepted", tc.name, tc.pointer)
		}
		if !reflect.DeepEqual(d, doc()) {
			t.Errorf("%s: a refused stamp changed the document", tc.name)
		}
	}
}
