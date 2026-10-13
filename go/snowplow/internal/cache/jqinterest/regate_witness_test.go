package jqinterest

import (
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// reviewer-603 re-gate witnesses N2/N3/N4, adopted as arms (green with every
// rule in place; N4 is also the RED-first witness of literal-values-skip).
func TestRegateWitness_N2N3N4(t *testing.T) {
	// N2/N4: a claimed non-member (by the Values oracle) must not change the output.
	for _, tc := range []struct {
		name, prog, member string
		env              emuEnv
	}{
		{"N2-stream-second-value", `[.s.items[] | select(.metadata.name == ("a", "b"))]`, "b", emuEnv{identity: 1}},
		{"N4-mixed-literal-extras", `.extras.name as $n | [.s.items[] | select(.metadata.name == ("zzz", $n))]`, "a",
			emuEnv{identity: 1, extras: map[string]any{"name": "a"}}},
	} {
		c := oneStage(tc.prog, "")
		in := Analyze(c, jqsupport.ModuleLoader())
		t.Logf("%s: %s", tc.name, in)
		if in.Membership == nil {
			continue
		}
		before := advItem(tc.member, "ns1", map[string]any{"v": "1"})
		after := advItem(tc.member, "ns1", map[string]any{"v": "2"})
		m, known := claimedMember(in.Membership, before)
		x, y := emulate(c, []any{before}, tc.env), emulate(c, []any{after}, tc.env)
		if known && !m && (okOutcome(x) || okOutcome(y)) && !sameOutcome(x, y) {
			t.Errorf("COUNTEREXAMPLE %s: claimed non-member %q changed output %s -> %s", tc.name, tc.member, x, y)
		}
	}
	// N3: extras collision with a filter-produced array (spliced elements).
	c := oneStage(`[.s.items[] | .spec.secret]`, `.s | if type == "array" then (.[1] | length) else 0 end`)
	env := emuEnv{identity: 1, extras: map[string]any{"s": "c"}}
	sec := func(v string) []any { return []any{advItem("a", "ns1", map[string]any{"secret": v})} }
	for _, b := range advCheck(t, "N3-splice", c, []advPair{{"a->bb", sec("a"), sec("bb"), env}}) {
		t.Errorf("COUNTEREXAMPLE %s", b)
	}
}
