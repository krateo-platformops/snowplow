package jqinterest

// Adversarial differential cases from reviewer-603 (#603 PR-1 gate), adopted
// as arms: every pair the analyzer calls projection-equal must give
// byte-identical real output.

import (
	"bytes"
	"fmt"
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

func advItem(name, ns string, spec map[string]any) map[string]any {
	it := map[string]any{"apiVersion": "v1", "kind": "Thing",
		"metadata": map[string]any{"name": name, "namespace": ns}}
	if spec != nil {
		it["spec"] = spec
	}
	return it
}

type advPair struct {
	tag           string
	before, after []any
	env           emuEnv
}

// advCheck: for each pair whose ONLY difference is one UPDATE that the
// analyzer calls projection-equal, the real output must be byte-identical.
// Returns a description of each counterexample.
func advCheck(t *testing.T, name string, c Chain, pairs []advPair) []string {
	in := Analyze(c, jqsupport.ModuleLoader())
	t.Logf("%-28s interest: %s", name, in)
	if in.All {
		return nil
	}
	var out []string
	for _, p := range pairs {
		if len(p.before) != len(p.after) {
			continue
		}
		eq := true
		for i := range p.before {
			if !bytes.Equal(Project(p.before[i], in.Reads), Project(p.after[i], in.Reads)) {
				eq = false
			}
		}
		a, b := emulate(c, p.before, p.env), emulate(c, p.after, p.env)
		t.Logf("   pair %-14s projEqual=%v  A=%s  B=%s", p.tag, eq, trunc(a.String()), trunc(b.String()))
		if eq && (okOutcome(a) || okOutcome(b)) && !sameOutcome(a, b) {
			out = append(out, fmt.Sprintf("%s/%s: projection unchanged, output changed: %s -> %s", name, p.tag, a, b))
		}
	}
	return out
}

func trunc(s string) string {
	if len(s) > 160 {
		return s[:160] + "…"
	}
	return s
}

func TestReviewerAdv_ReadSetCounterexamples(t *testing.T) {
	sec := func(v string) []any { return []any{advItem("a", "ns1", map[string]any{"secret": v, "x": 1.0})} }
	env := emuEnv{identity: 1, extras: map[string]any{"q": "z"}}
	var bad []string
	cases := []struct {
		name  string
		chain Chain
		pairs []advPair
	}{
		{"ENV-shadow-as", oneStage(`[.s.items[] | .spec.secret] as $ENV | $ENV`, ""),
			[]advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"ENV-shadow-param", oneStage(`def f($ENV): $ENV; [.s.items[] | f(.spec.secret)]`, ""),
			[]advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"ENV-shadow-def", oneStage(`def env: .spec.secret; [.s.items[] | env]`, ""),
			[]advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"loc-shadow", oneStage(`[.s.items[] | .spec.secret] as $__loc__ | $__loc__`, ""),
			[]advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"stage-named-extras", Chain{RA: RESTAction{Stages: []Stage{{Name: "extras", Filter: strp(`[.extras.items[] | .spec.secret]`)}}}, Tracked: "extras"},
			[]advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"stage-named-slice", Chain{RA: RESTAction{Stages: []Stage{{Name: "slice", Filter: strp(`[.slice.items[] | .spec.secret]`)}}}, Tracked: "slice"},
			[]advPair{{"secret a->b", sec("a"), sec("b"), emuEnv{identity: 1}}}},
		// gojq builtins with special shapes
		{"path(..)", oneStage(`[.s.items[] | .spec | [paths]]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"to_entries", oneStage(`[.s.items[] | .spec | to_entries | map(.key)]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"keys", oneStage(`[.s.items[] | .spec | keys]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"has", oneStage(`[.s.items[] | .spec | has("secret")]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"length-str", oneStage(`[.s.items[] | .spec.secret | length]`, ""), []advPair{{"secret a->bb", sec("a"), sec("bb"), env}}},
		{"getpath", oneStage(`[.s.items[] | getpath(["spec","secret"])]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"tojson", oneStage(`[.s.items[] | .spec | tojson]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"@base64", oneStage(`[.s.items[] | .spec | @base64 "\(.secret)"]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"ltrimstr", oneStage(`[.s.items[] | .spec.secret | ltrimstr("x")]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"recurse", oneStage(`[.s.items[] | [.. | strings]]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"reduce", oneStage(`reduce .s.items[] as $i (""; . + $i.spec.secret)`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"foreach", oneStage(`[foreach .s.items[] as $i (0; . + 1; $i.spec.secret)]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"try-catch-err", oneStage(`[.s.items[] | try error(.spec.secret) catch .]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"try-catch-nav", oneStage(`[.s.items[] | try (.spec.secret | .z) catch .]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"try-catch-tonum", oneStage(`[.s.items[] | try (.spec.secret | tonumber) catch .]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"try-catch-keys", oneStage(`[.s.items[] | try (.spec.secret | keys) catch .]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"try-catch-len", oneStage(`[.s.items[] | try (.spec.secret | length) catch . ]`, ""), []advPair{{"bool t->f", []any{advItem("a", "ns1", map[string]any{"secret": true})}, []any{advItem("a", "ns1", map[string]any{"secret": false})}, env}}},
		{"try-catch-obj", oneStage(`[.s.items[] | try ({k: .spec.secret} | .[0]) catch .]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"try-catch-arr", oneStage(`[.s.items[] | try ([.spec.secret] | .k) catch .]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"alt-missing", oneStage(`[.s.items[] | .spec.missing // .spec.secret]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"alt-err", oneStage(`[.s.items[] | (.spec.secret.k // "d")]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"sort_by-unread", oneStage(`.s.items | sort_by(.spec.secret) | map(.metadata.name)`, ""), []advPair{{"order swap",
			[]any{advItem("a", "ns1", map[string]any{"secret": "1"}), advItem("b", "ns1", map[string]any{"secret": "2"})},
			[]any{advItem("a", "ns1", map[string]any{"secret": "3"}), advItem("b", "ns1", map[string]any{"secret": "2"})}, env}}},
		{"unique_by-unread", oneStage(`.s.items | unique_by(.spec.k) | map(.metadata.name)`, ""), []advPair{{"group change",
			[]any{advItem("a", "ns1", map[string]any{"k": "1"}), advItem("b", "ns1", map[string]any{"k": "1"})},
			[]any{advItem("a", "ns1", map[string]any{"k": "1"}), advItem("b", "ns1", map[string]any{"k": "2"})}, env}}},
		{"group_by-len", oneStage(`.s.items | group_by(.spec.k) | map(length)`, ""), []advPair{{"group change",
			[]any{advItem("a", "ns1", map[string]any{"k": "1"}), advItem("b", "ns1", map[string]any{"k": "1"})},
			[]any{advItem("a", "ns1", map[string]any{"k": "1"}), advItem("b", "ns1", map[string]any{"k": "2"})}, env}}},
		{"limit-first-select", oneStage(`[limit(1; .s.items[] | select(.spec.on)) | .metadata.name]`, ""), []advPair{{"on flips",
			[]any{advItem("a", "ns1", map[string]any{"on": true}), advItem("b", "ns1", map[string]any{"on": true})},
			[]any{advItem("a", "ns1", map[string]any{"on": false}), advItem("b", "ns1", map[string]any{"on": true})}, env}}},
		{"first-obj-values", oneStage(`[.s.items[] | first(.spec[])]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"keys[0]", oneStage(`[.s.items[] | .spec | keys[0]]`, ""), []advPair{{"rename key",
			[]any{advItem("a", "ns1", map[string]any{"b": 1.0, "c": 1.0})}, []any{advItem("a", "ns1", map[string]any{"a": 1.0, "c": 1.0})}, env}}},
		{"var-parent", oneStage(`. as $r | [.s.items[] | $r.s.items | map(.spec.secret)]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"del-then-keys", oneStage(`[.s.items[] | .spec | del(.x) | keys]`, ""), []advPair{{"x 1->2", sec("a"), []any{advItem("a", "ns1", map[string]any{"secret": "a", "x": 2.0})}, env}}},
		{"del-then-iter", oneStage(`[.s.items[] | .spec | del(.x) | [.[]]]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"del-nested-err", oneStage(`[.s.items[] | .spec | try del(.secret.k) catch "E"]`, ""), []advPair{{"secret str->obj",
			sec("a"), []any{advItem("a", "ns1", map[string]any{"secret": map[string]any{"k": "v"}, "x": 1.0})}, env}}},
		{"isempty", oneStage(`[.s.items[] | isempty(.spec.list[]?)]`, ""), []advPair{{"list []->[1]",
			[]any{advItem("a", "ns1", map[string]any{"list": []any{}})}, []any{advItem("a", "ns1", map[string]any{"list": []any{1.0}})}, env}}},
		{"label-break", oneStage(`[label $f | .s.items[] | .spec.secret, break $f]`, ""), nil},
		{"input", oneStage(`[.s.items[] | input]`, ""), nil},
		{"env-const", oneStage(`[.s.items[] | env | length]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"num-int-vs-float", oneStage(`[.s.items[] | .spec.x | tostring]`, ""), []advPair{{"x 1.0->1.5",
			sec("a"), []any{advItem("a", "ns1", map[string]any{"secret": "a", "x": 1.5})}, env}}},
		{"splits", oneStage(`[.s.items[] | .spec.secret | [splits(",")]]`, ""), []advPair{{"secret a->a,b", sec("a"), sec("a,b"), env}}},
		{"string-mult", oneStage(`[.s.items[] | .spec.secret * 2]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"obj-key-computed", oneStage(`[.s.items[] | {(.spec.secret): 1}]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"destructure-arr", oneStage(`[.s.items[] | .spec.l as [$a, $b] | $b]`, ""), []advPair{{"l[1]",
			[]any{advItem("a", "ns1", map[string]any{"l": []any{"p", "q"}})}, []any{advItem("a", "ns1", map[string]any{"l": []any{"p", "r"}})}, env}}},
		{"destructure-obj", oneStage(`[.s.items[] | .spec as {secret: $s} | $s]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"recursive-def", oneStage(`def r: if type == "object" then [.[] | r] else . end; [.s.items[] | .spec | r]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"closure-def", oneStage(`def f(g): g | g; [.s.items[] | .spec | f(.)]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"update-assign", oneStage(`[.s.items[] | .spec | .x += 1]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"with_entries", oneStage(`[.s.items[] | .spec | with_entries(select(.key == "x"))]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"min_by", oneStage(`.s.items | min_by(.spec.x) | .spec.secret`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"index-dyn", oneStage(`[.s.items[] | .spec[.spec.k]]`, ""), []advPair{{"secret a->b",
			[]any{advItem("a", "ns1", map[string]any{"k": "secret", "secret": "a"})}, []any{advItem("a", "ns1", map[string]any{"k": "secret", "secret": "b"})}, env}}},
		{"if-noelse", oneStage(`[.s.items[] | .spec | if .x == 1 then "one" end]`, ""), []advPair{{"secret a->b", sec("a"), sec("b"), env}}},
		{"and-stream", oneStage(`[.s.items[] | (.spec.l[]? and true)]`, ""), []advPair{{"l [1]->[1,2]",
			[]any{advItem("a", "ns1", map[string]any{"l": []any{1.0}})}, []any{advItem("a", "ns1", map[string]any{"l": []any{1.0, 2.0}})}, env}}},
	}
	for _, tc := range cases {
		bad = append(bad, advCheck(t, tc.name, tc.chain, tc.pairs)...)
	}
	for _, b := range bad {
		t.Errorf("COUNTEREXAMPLE %s", b)
	}
}

// An extras key equal to the tracked stage name: api/resolve.go seeds the
// dict with the extras at TOP level, and jsonHandlerCore then ACCUMULATES the
// stage response into an array [extrasValue, response].
func TestReviewerAdv_ExtrasKeyCollidesWithStage(t *testing.T) {
	c := oneStage("", `.s | if type == "array" then .[1].items[0].spec.secret else "n" end`)
	sec := func(v string) []any { return []any{advItem("a", "ns1", map[string]any{"secret": v})} }
	env := emuEnv{identity: 1, extras: map[string]any{"s": "collide"}}
	for _, b := range advCheck(t, "extras-key==stage", c, []advPair{{"secret a->b", sec("a"), sec("b"), env}}) {
		t.Errorf("COUNTEREXAMPLE %s", b)
	}
}
